"""Forward-only edge report and the pre-registered ``prune_rule_v1`` (S16).

The rule is frozen in ``docs/research/prune-rule-v1-preregistration.md``. It
was committed before any number this module produces was computed. This
module implements it:

* per-unit hit rate (direction right) with a naive and a date-clustered
  Wilson 95% interval;
* effective N clustered by date (cluster-robust variance of the hit rate);
* a circular block bootstrap over date-ordered ``(hits, n)`` pairs for a
  two-sided p against the 0.5 baseline, with Benjamini–Hochberg within each
  family;
* Brier score where a probability exists (informational only);
* the verdict ``EDGE`` / ``NO_EDGE`` / ``ANTI_EDGE`` / ``INSUFFICIENT``.

Families: ``signal_source_type`` and ``signal_feed`` (``signal_source_outcomes``
version ``sig-eval-4-sessions``), ``oracle_model`` (official
``oracle_predictions`` verdicts) and ``astrogrid_model`` (effective
``astrogrid.prediction_score`` rows; ``grid-crypto-v1`` is one unit).
Rows rescored in hindsight (``oracle_prediction_rescores``,
``astrogrid.prediction_score_legacy_rescore``) and rows that fail the
recorded-before-knowable check go to the separate *exploratory* column. The
verdict never sees them.

Everything here is read-only. ``load_*`` functions take a connection on which
the caller has opened a read-only, time-bounded transaction (see
``scripts/edge_report.py``). The pure statistics take plain Python data.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from typing import Any

import numpy as np

from analysis.offline_research_proof import autocorrelation_block, bh_adjusted
from evals.e2.scoring import seed_for, splitmix64, wilson_interval

RULE_VERSION = "prune_rule_v1"
PREREGISTRATION_DOC = "docs/research/prune-rule-v1-preregistration.md"
BASELINE = 0.5
MIN_EFFECTIVE_N = 30
MIN_DATES = 20
ALPHA = 0.05
N_BOOT = 1999
LIVE_SCORED_GRACE_DAYS = 9
NOT_FORWARD_SUFFIX = " [not forward]"  # evaluation.signal_outcomes.FINAL_GRACE_DAYS (7) + 2

FAMILIES = ("signal_source_type", "signal_feed", "oracle_model", "astrogrid_model")
VERDICTS = ("EDGE", "NO_EDGE", "ANTI_EDGE", "INSUFFICIENT")
PRUNE_VERDICTS = ("NO_EDGE", "ANTI_EDGE")


# ---------------------------------------------------------------------------
# Data shapes
# ---------------------------------------------------------------------------


@dataclass
class DateCell:
    """Aggregates of one unit's outcomes on one cluster date."""

    hits: int = 0
    n: int = 0
    no_move: int = 0
    long_calls: int = 0
    live: int = 0
    target_hits: int = 0
    prob_n: int = 0
    prob_sq: float = 0.0
    prob_y: int = 0

    def add(self, other: DateCell) -> None:
        for name in ("hits", "n", "no_move", "long_calls", "live", "target_hits", "prob_n", "prob_y"):
            setattr(self, name, getattr(self, name) + getattr(other, name))
        self.prob_sq += other.prob_sq


@dataclass
class UnitData:
    """One unit's per-date cells, for one column (forward or exploratory)."""

    family: str
    unit: str
    horizon_days: float = 1.0
    cells: dict[date, DateCell] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict)

    def cell(self, d: date) -> DateCell:
        if d not in self.cells:
            self.cells[d] = DateCell()
        return self.cells[d]


@dataclass
class UnitStats:
    family: str
    unit: str
    n: int
    hits: int
    dates: int
    hit_rate: float | None
    n_eff: float
    wilson_naive: tuple[float, float] | None
    wilson_clustered: tuple[float, float] | None
    block: int
    boot_p: float
    bh_p: float = 1.0
    verdict: str = "INSUFFICIENT"
    brier: float | None = None
    brier_climatology: float | None = None
    prob_n: int = 0
    long_share: float | None = None
    no_move: int = 0
    no_move_share: float | None = None
    live_share: float | None = None
    target_hit_rate: float | None = None
    horizon_days: float = 1.0
    meta: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        out = asdict(self)
        for key in ("wilson_naive", "wilson_clustered"):
            if out[key] is not None:
                out[key] = [round(float(x), 6) for x in out[key]]
        return out


# ---------------------------------------------------------------------------
# Pure statistics
# ---------------------------------------------------------------------------


def clustered_effective_n(per_date: Sequence[tuple[int, int]]) -> float:
    """``p(1-p) / Var_CR(p̂)`` with date clusters, capped at N.

    ``Var_CR = D/(D-1) · Σ_d (h_d − p̂·n_d)² / N²``. When p̂ is 0 or 1 the
    variance is degenerate and the number of dates is used.
    """
    cells = [(int(h), int(n)) for h, n in per_date if n > 0]
    total = sum(n for _, n in cells)
    d = len(cells)
    if total == 0:
        return 0.0
    if d < 2:
        return float(min(total, d))
    p = sum(h for h, _ in cells) / total
    if p <= 0.0 or p >= 1.0:
        return float(min(total, d))
    var = d / (d - 1) * sum((h - p * n) ** 2 for h, n in cells) / (total * total)
    if var <= 0.0:
        return float(total)
    return float(min(total, p * (1.0 - p) / var))


def choose_block(per_date: Sequence[tuple[int, int]], horizon_days: float) -> int:
    """Bootstrap block length: autocorrelation-sized, overlap floor, cap D // 4."""
    rates = [h / n for h, n in per_date if n > 0]
    d = len(rates)
    if d < 2:
        return 1
    depth = max(0, math.ceil(horizon_days) - 1)
    block, _ = autocorrelation_block(rates, depth)
    return int(max(1, min(block, max(1, d // 4))))


def block_bootstrap_p(
    per_date: Sequence[tuple[int, int]],
    *,
    baseline: float,
    block: int,
    seed: int,
    n_boot: int = N_BOOT,
) -> float:
    """Two-sided circular-block-bootstrap p for ``hit_rate != baseline``.

    ``(1 + #{|p*_b − p̂| ≥ |p̂ − baseline|}) / (n_boot + 1)``; never 0.
    Deterministic: draws come from ``evals.e2.scoring.splitmix64``.
    """
    cells = [(int(h), int(n)) for h, n in per_date if n > 0]
    d = len(cells)
    if d == 0:
        return 1.0
    hits = np.array([h for h, _ in cells], dtype=float)
    ns = np.array([n for _, n in cells], dtype=float)
    p_hat = hits.sum() / ns.sum()
    gap = abs(p_hat - baseline)
    if gap == 0.0:
        return 1.0
    block = max(1, min(int(block), d))
    k = -(-d // block)
    starts = (splitmix64(seed, n_boot * k) % np.uint64(d)).astype(np.int64).reshape(n_boot, k)
    idx = (starts[:, :, None] + np.arange(block)[None, None, :]) % d
    idx = idx.reshape(n_boot, k * block)[:, :d]
    boot = hits[idx].sum(axis=1) / ns[idx].sum(axis=1)
    extreme = int(np.sum(np.abs(boot - p_hat) >= gap - 1e-12))
    return (1 + extreme) / (n_boot + 1)


def classify(
    n_eff: float,
    dates: int,
    wilson_clustered: tuple[float, float] | None,
    bh_p: float,
    *,
    baseline: float = BASELINE,
) -> str:
    """The pre-registered verdict (forward column only)."""
    if n_eff < MIN_EFFECTIVE_N or dates < MIN_DATES or wilson_clustered is None:
        return "INSUFFICIENT"
    lo, hi = wilson_clustered
    if lo > baseline and bh_p < ALPHA:
        return "EDGE"
    if hi < baseline and bh_p < ALPHA:
        return "ANTI_EDGE"
    return "NO_EDGE"


def unit_stats(data: UnitData, *, baseline: float = BASELINE, n_boot: int = N_BOOT) -> UnitStats:
    """Everything but the BH-adjusted p and the verdict (they need the family)."""
    ordered = [data.cells[d] for d in sorted(data.cells)]
    per_date = [(c.hits, c.n) for c in ordered if c.n > 0]
    n = sum(c.n for c in ordered)
    hits = sum(c.hits for c in ordered)
    no_move = sum(c.no_move for c in ordered)
    dates = len(per_date)
    hit_rate = hits / n if n else None
    n_eff = clustered_effective_n(per_date)
    naive = wilson_interval(hits, n) if n else None
    clustered = wilson_interval(hit_rate * n_eff, n_eff) if n and n_eff > 0 else None
    sufficient = n_eff >= MIN_EFFECTIVE_N and dates >= MIN_DATES
    block = choose_block(per_date, data.horizon_days) if dates >= 2 else 1
    boot_p = (
        block_bootstrap_p(per_date, baseline=baseline, block=block,
                          seed=seed_for(data.family, data.unit), n_boot=n_boot)
        if sufficient else 1.0
    )
    prob_n = sum(c.prob_n for c in ordered)
    prob_sq = sum(c.prob_sq for c in ordered)
    prob_y = sum(c.prob_y for c in ordered)
    brier = prob_sq / prob_n if prob_n else None
    ybar = prob_y / prob_n if prob_n else None
    calls = n + no_move
    return UnitStats(
        family=data.family,
        unit=data.unit,
        n=n,
        hits=hits,
        dates=dates,
        hit_rate=hit_rate,
        n_eff=round(n_eff, 2),
        wilson_naive=naive,
        wilson_clustered=clustered,
        block=block,
        boot_p=boot_p,
        brier=brier,
        brier_climatology=(ybar * (1 - ybar)) if ybar is not None else None,
        prob_n=prob_n,
        long_share=(sum(c.long_calls for c in ordered) / calls) if calls else None,
        no_move=no_move,
        no_move_share=(no_move / calls) if calls else None,
        live_share=(sum(c.live for c in ordered) / calls) if calls else None,
        target_hit_rate=(sum(c.target_hits for c in ordered) / n) if n else None,
        horizon_days=data.horizon_days,
        meta=dict(data.meta),
    )


def evaluate_family(
    units: Iterable[UnitData],
    *,
    baseline: float = BASELINE,
    n_boot: int = N_BOOT,
) -> list[UnitStats]:
    """Stats, BH within the family (insufficient units enter as p = 1) and verdicts."""
    stats = [unit_stats(u, baseline=baseline, n_boot=n_boot) for u in units]
    if not stats:
        return []
    pvals = [s.boot_p if (s.n_eff >= MIN_EFFECTIVE_N and s.dates >= MIN_DATES) else 1.0 for s in stats]
    adjusted = bh_adjusted(pvals)
    for s, adj in zip(stats, adjusted):
        s.bh_p = float(adj)
        s.verdict = classify(s.n_eff, s.dates, s.wilson_clustered, s.bh_p, baseline=baseline)
    stats.sort(key=lambda s: (-s.n, s.unit))
    return stats


def exploratory_stats(units: Iterable[UnitData]) -> list[UnitStats]:
    """Exploratory column: descriptive statistics only, never a verdict."""
    out = []
    for u in units:
        s = unit_stats(u, n_boot=1)
        s.boot_p = float("nan")
        s.bh_p = float("nan")
        s.verdict = "EXPLORATORY"
        out.append(s)
    out.sort(key=lambda s: (-s.n, s.unit))
    return out


# ---------------------------------------------------------------------------
# Read-only loaders (PostgreSQL)
# ---------------------------------------------------------------------------

# Forward: the signal row existed before 16:00 ET on the entry-bar date (the
# evaluator anchors entry at or after the known date, so this holds by
# construction; the predicate keeps a violation visible). Live: scored within
# FINAL_GRACE_DAYS + 2 of the exit, i.e. by the forward scorer, not the replay.
_SIGNAL_CELLS_SQL = """
    SELECT s.source_type,
           {feed_id} AS feed_id,
           o.signal_date,
           (s.created_at IS NOT NULL AND o.entry_price_date IS NOT NULL
            AND s.created_at < ((o.entry_price_date + TIME '16:00') AT TIME ZONE 'America/New_York'))
               AS forward,
           SUM(CASE WHEN o.outcome = 'CORRECT' THEN 1 ELSE 0 END) AS hits,
           SUM(CASE WHEN o.outcome IN ('CORRECT', 'WRONG') THEN 1 ELSE 0 END) AS n,
           SUM(CASE WHEN o.outcome = 'NO_MOVE' THEN 1 ELSE 0 END) AS no_move,
           SUM(CASE WHEN o.direction = 'BUY' THEN 1 ELSE 0 END) AS long_calls,
           SUM(CASE WHEN o.exit_price_date IS NOT NULL
                     AND o.evaluated_at <= ((o.exit_price_date + :live_grace) + TIME '23:59')
                                           AT TIME ZONE 'America/New_York'
                    THEN 1 ELSE 0 END) AS live
      FROM signal_source_outcomes o
      JOIN signal_sources s ON s.id = o.signal_source_id
     WHERE o.evaluation_version = :version
       AND o.outcome IN ('CORRECT', 'WRONG', 'NO_MOVE')
     GROUP BY 1, 2, 3, 4
"""

_SIGNAL_OUTCOME_MIX_SQL = """
    SELECT outcome, COUNT(*) FROM signal_source_outcomes
     WHERE evaluation_version = :version GROUP BY outcome
"""

# Official verdicts only. Forward: recorded before the expiry close and not
# scored before it. hit/partial = expiry close on the called side of entry.
_ORACLE_CELLS_SQL = """
    SELECT p.model_name,
           CAST((p.created_at AT TIME ZONE 'UTC') AS DATE) AS cdate,
           (p.created_at IS NOT NULL
            AND p.created_at < ((p.expiry + TIME '16:00') AT TIME ZONE 'America/New_York')
            AND (p.scored_at IS NULL
                 OR p.scored_at >= ((p.expiry + TIME '16:00') AT TIME ZONE 'America/New_York')))
               AS forward,
           SUM(CASE WHEN p.verdict IN ('hit', 'partial') THEN 1 ELSE 0 END) AS hits,
           COUNT(*) AS n,
           SUM(CASE WHEN p.verdict = 'hit' THEN 1 ELSE 0 END) AS target_hits,
           SUM(CASE WHEN p.direction = 'CALL' THEN 1 ELSE 0 END) AS long_calls,
           SUM(CASE WHEN p.confidence >= 0 AND p.confidence <= 1 THEN 1 ELSE 0 END) AS prob_n,
           SUM(CASE WHEN p.confidence >= 0 AND p.confidence <= 1
                    THEN POWER(p.confidence - CASE WHEN p.verdict IN ('hit', 'partial') THEN 1 ELSE 0 END, 2)
                    ELSE 0 END) AS prob_sq,
           SUM(CASE WHEN p.confidence >= 0 AND p.confidence <= 1 AND p.verdict IN ('hit', 'partial')
                    THEN 1 ELSE 0 END) AS prob_y,
           AVG(p.expiry - CAST((p.created_at AT TIME ZONE 'UTC') AS DATE)) AS horizon
      FROM oracle_predictions p
     WHERE p.dedup_keep
       AND p.verdict IN ('hit', 'partial', 'miss')
       AND p.direction IN ('CALL', 'PUT')
     GROUP BY 1, 2, 3
"""

_ORACLE_RESCORE_CELLS_SQL = """
    SELECT p.model_name,
           CAST((p.created_at AT TIME ZONE 'UTC') AS DATE) AS cdate,
           SUM(CASE WHEN r.verdict IN ('hit', 'partial') THEN 1 ELSE 0 END) AS hits,
           COUNT(*) AS n,
           SUM(CASE WHEN r.verdict = 'hit' THEN 1 ELSE 0 END) AS target_hits,
           SUM(CASE WHEN p.direction = 'CALL' THEN 1 ELSE 0 END) AS long_calls,
           AVG(p.expiry - CAST((p.created_at AT TIME ZONE 'UTC') AS DATE)) AS horizon
      FROM oracle_prediction_rescores r
      JOIN oracle_predictions p ON p.id = r.prediction_id
     WHERE p.dedup_keep
       AND r.verdict IN ('hit', 'partial', 'miss')
     GROUP BY 1, 2
"""

# The SPY-graded legacy crypto exclusion, identical to
# store.astrogrid._legacy_spy_score_predicate("ps", "pr") (a test pins it).
ASTROGRID_LEGACY_SPY_PREDICATE = (
    "COALESCE(pr.model_version = :legacy_model_version"
    " AND pr.created_at < :legacy_cutoff"
    " AND ps.raw_payload->'symbols' = CAST(:legacy_spy_symbols AS jsonb), FALSE)"
)

_ASTROGRID_CELLS_SQL = """
    SELECT pr.model_version,
           CAST((pr.created_at AT TIME ZONE 'UTC') AS DATE) AS cdate,
           (pr.created_at < ps.scored_at) AS forward,
           SUM(CASE WHEN CAST(ps.raw_payload->>'signed_return' AS DOUBLE PRECISION) > 0 THEN 1 ELSE 0 END) AS hits,
           SUM(CASE WHEN CAST(ps.raw_payload->>'signed_return' AS DOUBLE PRECISION) <> 0 THEN 1 ELSE 0 END) AS n,
           SUM(CASE WHEN ps.verdict = 'hit'
                     AND CAST(ps.raw_payload->>'signed_return' AS DOUBLE PRECISION) <> 0
                    THEN 1 ELSE 0 END) AS target_hits,
           SUM(CASE WHEN ps.raw_payload->>'direction' = 'bullish'
                     AND CAST(ps.raw_payload->>'signed_return' AS DOUBLE PRECISION) <> 0
                    THEN 1 ELSE 0 END) AS long_calls,
           AVG(CASE WHEN pr.horizon_label = 'macro' THEN 30 ELSE 7 END) AS horizon
      FROM {ag}.prediction_score ps
      JOIN {ag}.prediction_run pr ON pr.id = ps.prediction_run_id
     WHERE NOT {legacy_spy}
       AND ps.raw_payload->>'direction' IN ('bullish', 'bearish')
       AND ps.raw_payload->>'signed_return' IS NOT NULL
     GROUP BY 1, 2, 3
"""

_ASTROGRID_RESCORE_CELLS_SQL = """
    SELECT pr.model_version,
           CAST((pr.created_at AT TIME ZONE 'UTC') AS DATE) AS cdate,
           SUM(CASE WHEN CAST(lr.raw_payload->>'signed_return' AS DOUBLE PRECISION) > 0 THEN 1 ELSE 0 END) AS hits,
           SUM(CASE WHEN CAST(lr.raw_payload->>'signed_return' AS DOUBLE PRECISION) <> 0 THEN 1 ELSE 0 END) AS n,
           SUM(CASE WHEN lr.verdict = 'hit'
                     AND CAST(lr.raw_payload->>'signed_return' AS DOUBLE PRECISION) <> 0
                    THEN 1 ELSE 0 END) AS target_hits,
           SUM(CASE WHEN lr.raw_payload->>'direction' = 'bullish'
                     AND CAST(lr.raw_payload->>'signed_return' AS DOUBLE PRECISION) <> 0
                    THEN 1 ELSE 0 END) AS long_calls,
           AVG(CASE WHEN pr.horizon_label = 'macro' THEN 30 ELSE 7 END) AS horizon
      FROM {ag}.prediction_score_legacy_rescore lr
      JOIN {ag}.prediction_run pr ON pr.id = lr.prediction_run_id
     WHERE lr.raw_payload->>'direction' IN ('bullish', 'bearish')
       AND lr.raw_payload->>'signed_return' IS NOT NULL
     GROUP BY 1, 2
"""

# Verdict-only counts (the "~10% crypto-v1" style baseline the plan quotes).
_ASTROGRID_VERDICT_MIX_SQL = """
    SELECT pr.model_version, ps.verdict, COUNT(*)
      FROM {ag}.prediction_score ps
      JOIN {ag}.prediction_run pr ON pr.id = ps.prediction_run_id
     WHERE NOT {legacy_spy}
     GROUP BY 1, 2
"""


def _to_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _f(value: Any) -> float:
    return float(value) if value is not None else 0.0


def _i(value: Any) -> int:
    return int(value) if value is not None else 0


@dataclass
class FamilyColumns:
    """Forward and exploratory per-unit data for one family."""

    forward: dict[str, UnitData] = field(default_factory=dict)
    exploratory: dict[str, UnitData] = field(default_factory=dict)

    def unit(self, column: str, family: str, unit: str, horizon: float) -> UnitData:
        bucket = self.forward if column == "forward" else self.exploratory
        if unit not in bucket:
            bucket[unit] = UnitData(family=family, unit=unit, horizon_days=horizon)
        return bucket[unit]


def feed_unit_key(source_type: str, feed_id: str) -> str:
    """Display key of a ``signal_feed`` unit (``source_type`` / feed id)."""
    return f"{source_type} / {feed_id}"


def build_signal_families(rows: Iterable[Sequence[Any]]) -> tuple[FamilyColumns, FamilyColumns]:
    """``(source_type family, feed family)`` from ``_SIGNAL_CELLS_SQL`` rows."""
    from intelligence.trust_scorer import EVALUATION_WINDOWS

    by_type, by_feed = FamilyColumns(), FamilyColumns()
    for src_type, feed_id, sig_date, forward, hits, n, no_move, long_calls, live in rows:
        column = "forward" if forward else "exploratory"
        suffix = "" if forward else NOT_FORWARD_SUFFIX
        horizon = float(EVALUATION_WINDOWS.get(src_type, 7))
        cell = DateCell(hits=_i(hits), n=_i(n), no_move=_i(no_move), long_calls=_i(long_calls), live=_i(live))
        d = _to_date(sig_date)
        by_type.unit(column, "signal_source_type", src_type + suffix, horizon).cell(d).add(cell)
        feed = by_feed.unit(column, "signal_feed", feed_unit_key(src_type, feed_id) + suffix, horizon)
        feed.meta.setdefault("source_type", src_type)
        feed.meta.setdefault("source_id", feed_id)
        feed.cell(d).add(cell)
    return by_type, by_feed


def _weighted_horizon(unit: UnitData, horizon: Any, n: int) -> None:
    total = unit.meta.get("_h_n", 0) + n
    if total and horizon is not None:
        unit.horizon_days = (unit.horizon_days * unit.meta.get("_h_n", 0) + float(horizon) * n) / total
    unit.meta["_h_n"] = total


def build_oracle_family(rows: Iterable[Sequence[Any]], rescore_rows: Iterable[Sequence[Any]]) -> FamilyColumns:
    fam = FamilyColumns()
    for (model, cdate, forward, hits, n, target_hits, long_calls,
         prob_n, prob_sq, prob_y, horizon) in rows:
        column = "forward" if forward else "exploratory"
        unit = fam.unit(column, "oracle_model", model if forward else model + NOT_FORWARD_SUFFIX, 1.0)
        _weighted_horizon(unit, horizon, _i(n))
        unit.cell(_to_date(cdate)).add(DateCell(
            hits=_i(hits), n=_i(n), target_hits=_i(target_hits), long_calls=_i(long_calls),
            prob_n=_i(prob_n), prob_sq=_f(prob_sq), prob_y=_i(prob_y),
        ))
        if not forward:
            unit.meta["reason"] = "recorded_or_scored_after_expiry_close"
    for model, cdate, hits, n, target_hits, long_calls, horizon in rescore_rows:
        unit = fam.unit("exploratory", "oracle_model", f"{model} [rescore:hindsight]", 1.0)
        _weighted_horizon(unit, horizon, _i(n))
        unit.cell(_to_date(cdate)).add(DateCell(
            hits=_i(hits), n=_i(n), target_hits=_i(target_hits), long_calls=_i(long_calls),
        ))
    for bucket in (fam.forward, fam.exploratory):
        for unit in bucket.values():
            unit.meta.pop("_h_n", None)
    return fam


def build_astrogrid_family(rows: Iterable[Sequence[Any]], rescore_rows: Iterable[Sequence[Any]]) -> FamilyColumns:
    fam = FamilyColumns()
    for model, cdate, forward, hits, n, target_hits, long_calls, horizon in rows:
        column = "forward" if forward else "exploratory"
        unit = fam.unit(column, "astrogrid_model", model if forward else model + NOT_FORWARD_SUFFIX, 7.0)
        _weighted_horizon(unit, horizon, _i(n))
        unit.cell(_to_date(cdate)).add(DateCell(
            hits=_i(hits), n=_i(n), target_hits=_i(target_hits), long_calls=_i(long_calls),
        ))
    for model, cdate, hits, n, target_hits, long_calls, horizon in rescore_rows:
        unit = fam.unit("exploratory", "astrogrid_model", f"{model} [legacy_rescore]", 7.0)
        _weighted_horizon(unit, horizon, _i(n))
        unit.cell(_to_date(cdate)).add(DateCell(
            hits=_i(hits), n=_i(n), target_hits=_i(target_hits), long_calls=_i(long_calls),
        ))
    for bucket in (fam.forward, fam.exploratory):
        for unit in bucket.values():
            unit.meta.pop("_h_n", None)
    return fam


def _table_exists(conn: Any, name: str) -> bool:
    from sqlalchemy import text

    return conn.execute(text("SELECT to_regclass(:n)"), {"n": name}).scalar() is not None


_SCHEMA_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


def _ag_sql(sql: str, schema: str) -> str:
    """Substitute the (validated) AstroGrid schema name and the legacy predicate."""
    if not _SCHEMA_RE.fullmatch(schema):
        raise ValueError(f"invalid astrogrid schema name: {schema!r}")
    return sql.replace("{ag}", schema).replace("{legacy_spy}", ASTROGRID_LEGACY_SPY_PREDICATE)


def load_all(conn: Any, *, astrogrid_schema: str = "astrogrid") -> dict[str, Any]:
    """Run every read-only query; returns family columns plus context counts.

    ``signal_source_outcomes`` and the oracle tables resolve through the
    connection's ``search_path``; ``astrogrid_schema`` is validated against a
    strict identifier pattern before it is placed in the SQL.
    """
    from sqlalchemy import text

    from evaluation.signal_outcomes import SESSION_EVALUATION_VERSION
    from ingestion.altdata.quiverquant_identity import feed_source_id_sql
    from store.astrogrid import _legacy_score_params

    out: dict[str, Any] = {"context": {"signal_evaluation_version": SESSION_EVALUATION_VERSION}}
    version = {"version": SESSION_EVALUATION_VERSION}
    if _table_exists(conn, "signal_source_outcomes"):
        out["context"]["signal_outcome_mix"] = {
            str(k): int(v) for k, v in conn.execute(text(_SIGNAL_OUTCOME_MIX_SQL), version).fetchall()
        }
        rows = conn.execute(
            text(_SIGNAL_CELLS_SQL.replace("{feed_id}", feed_source_id_sql("s.source_id"))),
            {**version, "live_grace": LIVE_SCORED_GRACE_DAYS},
        ).fetchall()
        out["signal_source_type"], out["signal_feed"] = build_signal_families(rows)
    else:
        out["signal_source_type"], out["signal_feed"] = FamilyColumns(), FamilyColumns()

    oracle_rows = conn.execute(text(_ORACLE_CELLS_SQL)).fetchall()
    rescore_rows = (
        conn.execute(text(_ORACLE_RESCORE_CELLS_SQL)).fetchall()
        if _table_exists(conn, "oracle_prediction_rescores") else []
    )
    out["oracle_model"] = build_oracle_family(oracle_rows, rescore_rows)

    legacy = _legacy_score_params()
    ag_rows = conn.execute(text(_ag_sql(_ASTROGRID_CELLS_SQL, astrogrid_schema)), legacy).fetchall()
    ag_rescore = (
        conn.execute(text(_ag_sql(_ASTROGRID_RESCORE_CELLS_SQL, astrogrid_schema))).fetchall()
        if _table_exists(conn, f"{astrogrid_schema}.prediction_score_legacy_rescore") else []
    )
    out["astrogrid_model"] = build_astrogrid_family(ag_rows, ag_rescore)
    mix: dict[str, dict[str, int]] = {}
    for model, verdict, count in conn.execute(
        text(_ag_sql(_ASTROGRID_VERDICT_MIX_SQL, astrogrid_schema)), legacy,
    ).fetchall():
        mix.setdefault(str(model), {})[str(verdict)] = int(count)
    out["context"]["astrogrid_verdict_mix"] = mix
    return out


# ---------------------------------------------------------------------------
# Report assembly
# ---------------------------------------------------------------------------


def build_report(loaded: Mapping[str, Any], *, n_boot: int = N_BOOT) -> dict[str, Any]:
    """Evaluate every family (forward: verdicts; exploratory: descriptive)."""
    families: dict[str, Any] = {}
    for family in FAMILIES:
        cols: FamilyColumns = loaded.get(family) or FamilyColumns()
        forward = evaluate_family(cols.forward.values(), n_boot=n_boot)
        exploratory = exploratory_stats(cols.exploratory.values())
        counts = {v: sum(1 for s in forward if s.verdict == v) for v in VERDICTS}
        families[family] = {"forward": forward, "exploratory": exploratory, "verdict_counts": counts}
    return {
        "rule_version": RULE_VERSION,
        "preregistration": PREREGISTRATION_DOC,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "thresholds": {
            "baseline": BASELINE, "min_effective_n": MIN_EFFECTIVE_N,
            "min_dates": MIN_DATES, "alpha_bh": ALPHA, "n_boot": n_boot,
        },
        "context": dict(loaded.get("context") or {}),
        "families": families,
    }


def candidate_prune_list(report: Mapping[str, Any]) -> dict[str, Any]:
    """The *candidate* prune list (NO_EDGE / ANTI_EDGE forward units).

    It takes effect only if an operator copies it to
    ``GRID_TRUST_PRUNE_LIST_PATH`` and sets ``GRID_TRUST_PRUNE_NO_EDGE``.
    """
    units: list[dict[str, Any]] = []
    for family, body in report["families"].items():
        for s in body["forward"]:
            if s.verdict not in PRUNE_VERDICTS:
                continue
            entry: dict[str, Any] = {
                "family": family, "unit": s.unit, "verdict": s.verdict,
                "n": s.n, "n_eff": s.n_eff, "hit_rate": round(s.hit_rate, 6) if s.hit_rate is not None else None,
                "wilson_clustered": [round(x, 6) for x in s.wilson_clustered] if s.wilson_clustered else None,
                "bh_p": round(s.bh_p, 6),
            }
            if family == "signal_source_type":
                entry["source_type"] = s.unit
            elif family == "signal_feed":
                entry["source_type"] = s.meta.get("source_type")
                entry["source_id"] = s.meta.get("source_id")
            units.append(entry)
    return {
        "rule_version": RULE_VERSION,
        "status": "candidate -- not active until an operator copies it to GRID_TRUST_PRUNE_LIST_PATH",
        "generated_at": report["generated_at"],
        "preregistration": PREREGISTRATION_DOC,
        "units": units,
    }


def report_json(report: Mapping[str, Any]) -> str:
    """JSON with NaN (exploratory p-values) written as null."""
    return json.dumps(_scrub(report), default=str, indent=1, sort_keys=True, allow_nan=False)


def _scrub(obj: Any) -> Any:
    if isinstance(obj, UnitStats):
        return _scrub(obj.as_dict())
    if isinstance(obj, dict):
        return {k: _scrub(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_scrub(v) for v in obj]
    if isinstance(obj, float) and not math.isfinite(obj):
        return None
    return obj


def _pct(x: float | None, digits: int = 1) -> str:
    return "—" if x is None else f"{100 * x:.{digits}f}%"


def _ci(ci: Sequence[float] | None) -> str:
    return "—" if not ci else f"{100 * ci[0]:.1f}–{100 * ci[1]:.1f}"


def _p(x: float) -> str:
    if x is None or not math.isfinite(x):
        return "—"
    return f"{x:.4f}" if x >= 0.0001 else f"{x:.1e}"


def _forward_table(stats: Sequence[UnitStats], *, show_brier: bool, show_live: bool, limit: int | None) -> list[str]:
    head = ["Unit", "N", "Dates", "N_eff", "Hit", "Wilson (naive)", "Wilson (clustered)",
            "Block", "Boot p", "BH p", "Long", "Verdict"]
    if show_live:
        head[-1:-1] = ["No-move", "Live-scored"]
    if show_brier:
        head[-1:-1] = ["Brier (clim.)"]
    lines = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    for s in stats[:limit] if limit else stats:
        row = [f"`{s.unit}`", str(s.n), str(s.dates), f"{s.n_eff:.0f}", _pct(s.hit_rate),
               _ci(s.wilson_naive), _ci(s.wilson_clustered), str(s.block), _p(s.boot_p), _p(s.bh_p),
               _pct(s.long_share, 0), f"**{s.verdict}**"]
        if show_live:
            row[-1:-1] = [_pct(s.no_move_share, 0), _pct(s.live_share, 0)]
        if show_brier:
            brier = "—" if s.brier is None else f"{s.brier:.3f} ({s.brier_climatology:.3f})"
            row[-1:-1] = [brier]
        lines.append("| " + " | ".join(row) + " |")
    return lines


def _exploratory_table(stats: Sequence[UnitStats], limit: int | None = 15) -> list[str]:
    if not stats:
        return ["_No exploratory rows._"]
    lines = ["| Unit | N | Dates | Hit | Wilson (naive) | Target-hit |", "|---|---|---|---|---|---|"]
    for s in stats[:limit] if limit else stats:
        lines.append(f"| `{s.unit}` | {s.n} | {s.dates} | {_pct(s.hit_rate)} | {_ci(s.wilson_naive)} | "
                     f"{_pct(s.target_hit_rate)} |")
    return lines


def _pooled(stats: Sequence[UnitStats]) -> tuple[int, float | None]:
    n = sum(s.n for s in stats)
    return n, (sum(s.hits for s in stats) / n if n else None)


def render_markdown(report: Mapping[str, Any], *, title_date: str, feed_limit: int = 40) -> str:
    fam = report["families"]
    ctx = report.get("context", {})
    th = report["thresholds"]
    out: list[str] = [
        f"# Forward edge report — {title_date}",
        "",
        (f"Rule: `{report['rule_version']}`, pre-registered in `{report['preregistration']}` before these "
         f"numbers were computed. Generated {report['generated_at']} (read-only, production). "
         "Reproduce: `python scripts/edge_report.py report` (see `--help`)."),
        "",
        (f"Baseline {th['baseline']:.2f} (a coin flip on direction). A unit needs N_eff ≥ "
         f"{th['min_effective_n']} and ≥ {th['min_dates']} dates for a verdict. EDGE / ANTI_EDGE need the "
         f"clustered Wilson 95% CI to exclude 0.5 **and** a Benjamini–Hochberg p < {th['alpha_bh']} (circular "
         f"block bootstrap, B = {th['n_boot']}, BH within each family). Hit = direction right. Signals: "
         "CORRECT/(CORRECT+WRONG), with NO_MOVE excluded. Oracle: (hit+partial)/(hit+partial+miss). "
         "AstroGrid: sign(direction)·return > 0."),
        "",
        ("Only the **forward** tables feed the verdict. Hindsight rescores (#976 oracle, #973 crypto) and rows "
         "recorded after the outcome was knowable appear only in the exploratory tables."),
        "",
        "## Headline",
        "",
    ]
    for family, label in (("signal_source_type", "Signal sources"), ("oracle_model", "Oracle models"),
                          ("astrogrid_model", "AstroGrid models")):
        n, rate = _pooled(fam[family]["forward"])
        en, erate = _pooled(fam[family]["exploratory"])
        vc = fam[family]["verdict_counts"]
        out.append(
            f"- **{label}:** forward N = {n:,}, pooled hit {_pct(rate)}; verdicts "
            + ", ".join(f"{k} {v}" for k, v in vc.items())
            + f". Exploratory N = {en:,}, hit {_pct(erate)}."
        )
    vc = fam["signal_feed"]["verdict_counts"]
    out.append("- **Signal feeds:** verdicts " + ", ".join(f"{k} {v}" for k, v in vc.items()) + ".")
    mix = ctx.get("signal_outcome_mix")
    if mix:
        out += ["", "Corrected signal outcome mix (`" + str(ctx.get("signal_evaluation_version")) + "`): "
                + ", ".join(f"{k} {v:,}" for k, v in sorted(mix.items())) + "."]
    ag_mix = ctx.get("astrogrid_verdict_mix")
    if ag_mix:
        parts = []
        for model, counts in sorted(ag_mix.items()):
            scored = sum(v for k, v in counts.items() if k in ("hit", "partial", "miss"))
            parts.append(f"`{model}` stored-verdict hit {_pct(counts.get('hit', 0) / scored if scored else None)}"
                         f" of {scored:,} hit/partial/miss")
        out += ["", "AstroGrid stored verdicts (effective scores, legacy SPY-graded rows excluded): "
                + "; ".join(parts) + "."]

    sections = (
        ("signal_source_type", "Signal sources by source type", False, True, None),
        ("oracle_model", "Oracle by model", True, False, None),
        ("astrogrid_model", "AstroGrid by model version (crypto-v1 = `grid-crypto-v1`)", False, False, None),
    )
    for family, title, brier, live, limit in sections:
        out += ["", f"## {title}", "", "### Forward", ""]
        out += _forward_table(fam[family]["forward"], show_brier=brier, show_live=live, limit=limit) \
            if fam[family]["forward"] else ["_No forward rows._"]
        if family == "oracle_model":
            out += ["", "Target-hit rate (`hit` only, the legacy headline): "
                    + ", ".join(f"`{s.unit}` {_pct(s.target_hit_rate)}" for s in fam[family]["forward"][:12]) + "."]
        out += ["", "### Exploratory (not used by the rule)", ""]
        out += _exploratory_table(fam[family]["exploratory"])

    feeds = fam["signal_feed"]["forward"]
    ranked = [s for s in feeds if s.verdict != "INSUFFICIENT"]
    out += ["", "## Signal feeds (source_type / feed id)", "",
            (f"{len(feeds):,} feeds have forward rows; {len(ranked):,} clear the N_eff/date floor. "
             f"Top {min(feed_limit, len(ranked))} by N:"), ""]
    out += _forward_table(ranked, show_brier=False, show_live=True, limit=feed_limit) if ranked \
        else ["_No feed clears the floor._"]
    out += ["", "## What happens next", "",
            ("`scripts/edge_report.py report --candidate-out PATH` writes the NO_EDGE / ANTI_EDGE units as a "
             "*candidate* prune list. Nothing changes until an operator copies it to "
             "`GRID_TRUST_PRUNE_LIST_PATH` and sets `GRID_TRUST_PRUNE_NO_EDGE=true`. Those units then get "
             "weight 0 in the convergence conviction layer, `detect_convergence` and `get_trusted_sources`. "
             "Nothing is up-weighted, and no unit is flipped to contrarian."), ""]
    return "\n".join(out)


# ---------------------------------------------------------------------------
# Trust-score before/after (S16 item 1)
# ---------------------------------------------------------------------------


def trust_diff(legacy: Mapping[str, Any], corrected: Mapping[str, Any], *, top: int = 25) -> dict[str, Any]:
    """Distribution and biggest movers between two ``compute_trust_scores`` results."""
    def _index(result: Mapping[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
        return {(s["source_type"], s["source_id"]): s for s in result.get("sources", [])}

    old, new = _index(legacy), _index(corrected)

    def _dist(values: list[float]) -> dict[str, Any]:
        if not values:
            return {"n": 0}
        arr = np.asarray(values, dtype=float)
        q = np.quantile(arr, [0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0])
        hist, edges = np.histogram(arr, bins=10, range=(0.0, 1.0))
        return {
            "n": int(arr.size), "mean": round(float(arr.mean()), 4),
            "quantiles": dict(zip(("min", "p10", "p25", "p50", "p75", "p90", "max"),
                                  (round(float(x), 4) for x in q))),
            "histogram": {f"{edges[i]:.1f}-{edges[i + 1]:.1f}": int(hist[i]) for i in range(len(hist))},
            "share_ge_0_6": round(float((arr >= 0.6).mean()), 4),
        }

    movers = []
    for key in set(old) | set(new):
        a, b = old.get(key), new.get(key)
        ta = a["trust_score"] if a else None
        tb = b["trust_score"] if b else None
        movers.append({
            "source_type": key[0], "source_id": key[1],
            "legacy_trust": ta, "corrected_trust": tb,
            "delta": round(tb - ta, 4) if ta is not None and tb is not None else None,
            "legacy_hits_misses": [a["hit_count"], a["miss_count"]] if a else None,
            "corrected_hits_misses": [b["hit_count"], b["miss_count"]] if b else None,
            "no_move": b.get("no_move_count") if b else None,
            "ineligible": b.get("ineligible_count") if b else None,
        })
    both = [m for m in movers if m["delta"] is not None]
    both.sort(key=lambda m: (-abs(m["delta"]), m["source_type"], m["source_id"]))
    by_type: dict[str, dict[str, Any]] = {}
    for m in both:
        t = by_type.setdefault(m["source_type"], {"n": 0, "legacy_sum": 0.0, "corrected_sum": 0.0})
        t["n"] += 1
        t["legacy_sum"] += m["legacy_trust"]
        t["corrected_sum"] += m["corrected_trust"]
    type_summary = {
        k: {"sources": v["n"], "legacy_mean": round(v["legacy_sum"] / v["n"], 4),
            "corrected_mean": round(v["corrected_sum"] / v["n"], 4),
            "delta_mean": round((v["corrected_sum"] - v["legacy_sum"]) / v["n"], 4)}
        for k, v in sorted(by_type.items())
    }
    return {
        "legacy_basis": legacy.get("outcome_basis"),
        "corrected_basis": corrected.get("outcome_basis"),
        "legacy": _dist([s["trust_score"] for s in old.values()]),
        "corrected": _dist([s["trust_score"] for s in new.values()]),
        "sources_in_both": len(both),
        "only_legacy": sum(1 for m in movers if m["corrected_trust"] is None),
        "only_corrected": sum(1 for m in movers if m["legacy_trust"] is None),
        "top_up": sorted(both, key=lambda m: -m["delta"])[:top],
        "top_down": sorted(both, key=lambda m: m["delta"])[:top],
        "by_source_type": type_summary,
    }
