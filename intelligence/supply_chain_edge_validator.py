"""
GRID Supply-Chain Edge Validator.

Weekly sanity check for every row in ``supply_chain_edges``. For each edge
where BOTH the upstream and downstream resolve to a price series (via
``intelligence.cross_lens.resolve_price_series_id``), compute a rolling
180-day Pearson correlation on daily log returns and persist the result
on the edge row itself.

Why bother
----------

Edges land in the graph from five different confidence tiers (confirmed,
derived, estimated, rumored, inferred) — some seeded, some LLM-extracted,
some scraped. The graph is only as honest as the weakest edge. If HSY is
"dependent" on cocoa but their returns correlate at 0.02 for half a year,
either (a) the edge is wrong, (b) the hedge book is dominating, or (c) the
exposure is too small to matter for trading purposes. In every case the
edge should be flagged for human review before it drives a prediction.

Note: this is a diagnostic / data-quality layer, not an inference path.
It intentionally reads ``raw_series`` directly the same way
``intelligence.cross_lens`` does — see the PIT note in that module for
why historical, post-hoc correlation scans are fine here. Anything that
feeds forward into trading MUST still go through ``store/pit.py``.

State machine
-------------

Each weekly pass updates one edge like so::

    1. corr = pearson(upstream_log_returns, downstream_log_returns) over 180d
    2. last_validation_at = NOW()
    3. validation_correlation = corr
    4. if corr >= WEAK_CORRELATION_FLOOR:
           weak_since       = NULL
           relationship_weak = FALSE
    5. elif corr <  WEAK_CORRELATION_FLOOR:
           if weak_since is NULL:
               weak_since = today
           elif (today - weak_since) >= WEAK_MIN_DURATION_DAYS:
               relationship_weak = TRUE

So the FIRST time an edge dips, we only start the clock — we don't flag.
Flagging happens once the dip persists for six months. This matches the
"6+ consecutive months" requirement in the mission spec and avoids
flapping on noise.

Idempotent: rerunning on the same day is a no-op; weak_since is only set
once per weak streak and is cleared the moment correlation recovers.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.cross_lens import (
    compute_log_returns,
    fetch_close_series,
    resolve_price_series_id,
)


# ── Constants ─────────────────────────────────────────────────────────────

#: Rolling window (trading days of calendar days worth of history pulled).
DEFAULT_LOOKBACK_DAYS: int = 180

#: Correlation magnitude below which we consider the edge "not co-moving".
#: Uses absolute value because inverse relationships (commodity up -> maker
#: down) are still meaningful. If |corr| < 0.1 we treat the edge as noise.
WEAK_CORRELATION_FLOOR: float = 0.1

#: How long the correlation has to stay below the floor before we flag.
#: Mission spec: 6+ consecutive months. 180 days is the normalised form.
WEAK_MIN_DURATION_DAYS: int = 180

#: Minimum number of overlapping return observations required before we
#: trust a correlation enough to update the edge. Below this we leave the
#: edge alone (no update) to avoid false weak flags on thin data.
MIN_OBSERVATIONS: int = 30


# ── Data classes ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class EdgeRow:
    """Minimal subset of ``supply_chain_edges`` the validator needs."""

    edge_id: int
    upstream_id: str
    downstream_id: str
    weak_since: date | None
    relationship_weak: bool
    relationship: str = ""
    pct_downstream_cogs: float | None = None


@dataclass(frozen=True)
class ValidationResult:
    """Immutable result of running the validator on a single edge.

    ``action`` is one of:

        * ``"updated"``     — correlation computed and persisted
        * ``"skipped_up"``  — upstream has no resolvable price series
        * ``"skipped_down"``— downstream has no resolvable price series
        * ``"skipped_data"``— not enough overlapping observations
        * ``"error"``       — unexpected exception during evaluation
    """

    edge_id: int
    upstream_id: str
    downstream_id: str
    correlation: float | None
    action: str
    weak_since: date | None
    relationship_weak: bool
    detail: str | None = None


# ── Edge enumeration ──────────────────────────────────────────────────────


def list_edges(engine: Engine, limit: int | None = None) -> list[EdgeRow]:
    """Return every supply_chain_edges row the validator should consider.

    Intentionally pulls ALL edges (not just ones with COGS data), because
    the validator is agnostic to exposure percentages — it just wants to
    know whether the two endpoints move together. Ordering by id gives
    deterministic test runs.
    """
    sql = text(
        """
        SELECT id, upstream_id, downstream_id, weak_since,
               relationship_weak, relationship, pct_downstream_cogs
        FROM supply_chain_edges
        ORDER BY id ASC
        """
        + (" LIMIT :lim" if limit else "")
    )
    params: dict[str, Any] = {}
    if limit:
        params["lim"] = int(limit)
    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [
        EdgeRow(
            edge_id=int(r[0]),
            upstream_id=r[1],
            downstream_id=r[2],
            weak_since=r[3],
            relationship_weak=bool(r[4]) if r[4] is not None else False,
            relationship=str(r[5] or ""),
            pct_downstream_cogs=(
                float(r[6]) if r[6] is not None else None
            ),
        )
        for r in rows
    ]


# ── Correlation math ──────────────────────────────────────────────────────


def compute_edge_correlation(
    engine: Engine,
    upstream_id: str,
    downstream_id: str,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> tuple[float | None, int, str]:
    """Return ``(corr, n_obs, detail)`` for a single edge.

    ``corr`` is ``None`` when the edge cannot be evaluated. ``detail`` is a
    short status string identifying why the correlation is missing, used
    downstream to classify each ValidationResult.

    Implementation notes:

    * We reuse ``cross_lens.fetch_close_series`` + ``compute_log_returns``
      so the series-resolution heuristics stay in one place.
    * Pearson correlation is computed on the inner-join of the two return
      series by observation date — no lag, no shifting. The validator is
      not trying to detect lead/lag; it only wants the answer to "do these
      two things actually move together over 180d?".
    """
    up_sid = resolve_price_series_id(upstream_id)
    if up_sid is None:
        return None, 0, "no_upstream_series"
    down_sid = resolve_price_series_id(downstream_id)
    if down_sid is None:
        return None, 0, "no_downstream_series"

    up_df = fetch_close_series(engine, up_sid, lookback_days)
    if up_df.empty:
        return None, 0, "no_upstream_data"
    down_df = fetch_close_series(engine, down_sid, lookback_days)
    if down_df.empty:
        return None, 0, "no_downstream_data"

    up_returns = compute_log_returns(up_df)
    down_returns = compute_log_returns(down_df)
    if up_returns.empty or down_returns.empty:
        return None, 0, "empty_returns"

    up_returns.index = pd.to_datetime(up_returns.index)
    down_returns.index = pd.to_datetime(down_returns.index)
    joined = pd.concat(
        [up_returns.rename("up"), down_returns.rename("down")],
        axis=1,
    ).dropna()
    n_obs = len(joined)
    if n_obs < MIN_OBSERVATIONS:
        return None, n_obs, "insufficient_overlap"

    corr = float(joined["up"].corr(joined["down"]))
    if not np.isfinite(corr):
        return None, n_obs, "nan_correlation"
    return corr, n_obs, "ok"


# ── State transition ──────────────────────────────────────────────────────


def next_edge_state(
    correlation: float,
    prior_weak_since: date | None,
    today: date,
    floor: float = WEAK_CORRELATION_FLOOR,
    min_duration_days: int = WEAK_MIN_DURATION_DAYS,
) -> tuple[date | None, bool]:
    """Return ``(weak_since, relationship_weak)`` after observing ``correlation``.

    Pure function — no DB, no clock. Easy to unit test.

    Logic:

        * |corr| >= floor -> clear weak state
        * |corr| <  floor AND no prior weak_since -> set weak_since = today
        * |corr| <  floor AND prior weak_since >= min_duration_days old ->
          weak_since unchanged, relationship_weak = TRUE
        * |corr| <  floor AND prior weak_since too recent -> weak_since
          unchanged, relationship_weak still FALSE (clock is running)
    """
    if abs(correlation) >= floor:
        return None, False
    if prior_weak_since is None:
        return today, False
    days_weak = (today - prior_weak_since).days
    if days_weak >= min_duration_days:
        return prior_weak_since, True
    return prior_weak_since, False


# ── Persistence ───────────────────────────────────────────────────────────


def persist_result(
    engine: Engine,
    edge_id: int,
    correlation: float,
    weak_since: date | None,
    relationship_weak: bool,
) -> None:
    """Write a single validation result back to ``supply_chain_edges``."""
    stmt = text(
        """
        UPDATE supply_chain_edges
        SET validation_correlation = :corr,
            last_validation_at     = NOW(),
            weak_since             = :weak_since,
            relationship_weak      = :weak
        WHERE id = :id
        """
    )
    with engine.begin() as conn:
        conn.execute(
            stmt,
            {
                "corr": round(correlation, 4),
                "weak_since": weak_since,
                "weak": bool(relationship_weak),
                "id": int(edge_id),
            },
        )


_PRODUCER_MODULE = "intelligence.supply_chain_edge_validator"


def _emit_edge_validated(
    *,
    edge: EdgeRow,
    correlation: float,
    new_weak_since: date | None,
    new_weak_flag: bool,
) -> None:
    """Emit an ``EdgeValidated`` contract. Non-fatal on any failure."""
    try:
        from contracts.correlation import (
            get_current_correlation_id,
            new_correlation_id,
        )
        from contracts.emit import emit as _emit
        from contracts.schemas import EdgeValidated
    except Exception as exc:  # pragma: no cover — defensive import guard
        log.debug("edge_validator: contracts import failed: {e}", e=str(exc))
        return

    try:
        corr_id = get_current_correlation_id() or new_correlation_id()
    except Exception:
        return

    weak_since_dt: datetime | None = None
    if new_weak_since is not None:
        try:
            weak_since_dt = datetime(
                new_weak_since.year,
                new_weak_since.month,
                new_weak_since.day,
                tzinfo=timezone.utc,
            )
        except Exception:
            weak_since_dt = None

    try:
        _emit(
            EdgeValidated(
                producer_module=_PRODUCER_MODULE,
                correlation_id=corr_id,
                edge_id=int(edge.edge_id),
                upstream_id=str(edge.upstream_id),
                downstream_id=str(edge.downstream_id),
                relationship=str(edge.relationship or ""),
                validation_correlation=float(round(correlation, 4)),
                weak_since=weak_since_dt,
                relationship_weak=bool(new_weak_flag),
                implied_pct_cogs=(
                    float(edge.pct_downstream_cogs)
                    if edge.pct_downstream_cogs is not None
                    else None
                ),
            )
        )
    except Exception as exc:  # non-fatal per SYNTH-C contract
        log.debug(
            "edge_validator emit failed for edge {i}: {e}",
            i=edge.edge_id, e=str(exc),
        )


# ── Main orchestrator ─────────────────────────────────────────────────────


def validate_edge(
    engine: Engine,
    edge: EdgeRow,
    today: date,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> ValidationResult:
    """Validate a single edge and (on success) persist the new state."""
    try:
        corr, n_obs, detail = compute_edge_correlation(
            engine, edge.upstream_id, edge.downstream_id, lookback_days
        )
    except Exception as exc:
        log.warning(
            "edge_validator: {u}->{d} correlation failed: {e}",
            u=edge.upstream_id,
            d=edge.downstream_id,
            e=str(exc),
        )
        return ValidationResult(
            edge_id=edge.edge_id,
            upstream_id=edge.upstream_id,
            downstream_id=edge.downstream_id,
            correlation=None,
            action="error",
            weak_since=edge.weak_since,
            relationship_weak=edge.relationship_weak,
            detail=str(exc)[:200],
        )

    if corr is None:
        # Map the cross_lens detail string to a validator action code.
        if detail in ("no_upstream_series", "no_upstream_data"):
            action = "skipped_up"
        elif detail in ("no_downstream_series", "no_downstream_data"):
            action = "skipped_down"
        else:
            action = "skipped_data"
        return ValidationResult(
            edge_id=edge.edge_id,
            upstream_id=edge.upstream_id,
            downstream_id=edge.downstream_id,
            correlation=None,
            action=action,
            weak_since=edge.weak_since,
            relationship_weak=edge.relationship_weak,
            detail=f"{detail} (n_obs={n_obs})",
        )

    new_weak_since, new_weak_flag = next_edge_state(
        correlation=corr,
        prior_weak_since=edge.weak_since,
        today=today,
    )
    persist_result(
        engine=engine,
        edge_id=edge.edge_id,
        correlation=corr,
        weak_since=new_weak_since,
        relationship_weak=new_weak_flag,
    )
    # SYNTH-C / SYNTH-39 — non-fatal EdgeValidated emit. Downgrades
    # cross_lens trust whenever an edge flips weak.
    _emit_edge_validated(
        edge=edge,
        correlation=corr,
        new_weak_since=new_weak_since,
        new_weak_flag=new_weak_flag,
    )
    return ValidationResult(
        edge_id=edge.edge_id,
        upstream_id=edge.upstream_id,
        downstream_id=edge.downstream_id,
        correlation=round(corr, 4),
        action="updated",
        weak_since=new_weak_since,
        relationship_weak=new_weak_flag,
        detail=f"n_obs={n_obs}",
    )


def validate_all_edges(
    engine: Engine,
    limit: int | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    today: date | None = None,
) -> list[ValidationResult]:
    """Walk every supply_chain_edges row and validate it.

    Returns the list of ValidationResult rows so callers (runner script,
    tests) can summarise without re-reading from the DB.
    """
    as_of = today or date.today()
    edges = list_edges(engine, limit=limit)
    log.info(
        "edge_validator: examining {n} edges (lookback={l}d, as_of={d})",
        n=len(edges),
        l=lookback_days,
        d=as_of,
    )
    results: list[ValidationResult] = []
    for edge in edges:
        result = validate_edge(engine, edge, as_of, lookback_days)
        results.append(result)
    log.info(
        "edge_validator: {u} updated, {s_up} skipped_up, {s_dn} skipped_down, "
        "{s_dt} skipped_data, {er} errors",
        u=sum(1 for r in results if r.action == "updated"),
        s_up=sum(1 for r in results if r.action == "skipped_up"),
        s_dn=sum(1 for r in results if r.action == "skipped_down"),
        s_dt=sum(1 for r in results if r.action == "skipped_data"),
        er=sum(1 for r in results if r.action == "error"),
    )
    return results


def summarise_results(results: list[ValidationResult]) -> dict[str, Any]:
    """Return a small dict for logging / scheduler telemetry."""
    updated = [r for r in results if r.action == "updated"]
    corr_values = [r.correlation for r in updated if r.correlation is not None]
    hist: dict[str, int] = {
        "abs_ge_0.7": 0,
        "abs_0.4_0.7": 0,
        "abs_0.1_0.4": 0,
        "abs_lt_0.1": 0,
    }
    for c in corr_values:
        abs_c = abs(c)
        if abs_c >= 0.7:
            hist["abs_ge_0.7"] += 1
        elif abs_c >= 0.4:
            hist["abs_0.4_0.7"] += 1
        elif abs_c >= 0.1:
            hist["abs_0.1_0.4"] += 1
        else:
            hist["abs_lt_0.1"] += 1
    return {
        "total": len(results),
        "validated": len(updated),
        "skipped_upstream_no_series": sum(1 for r in results if r.action == "skipped_up"),
        "skipped_downstream_no_series": sum(1 for r in results if r.action == "skipped_down"),
        "skipped_insufficient_data": sum(1 for r in results if r.action == "skipped_data"),
        "errors": sum(1 for r in results if r.action == "error"),
        "flagged_weak": sum(1 for r in updated if r.relationship_weak),
        "weak_clock_running": sum(
            1 for r in updated if r.weak_since is not None and not r.relationship_weak
        ),
        "correlation_histogram": hist,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


# ── Hermes entry point ────────────────────────────────────────────────────


def run_weekly(engine: Engine | None = None) -> dict[str, Any]:
    """Hermes-scheduler entry point. Validates every edge and returns a summary.

    Matches the signature used by other weekly intelligence tasks in
    ``scripts/hermes_operator.py`` so it can be registered the same way.
    """
    if engine is None:
        from db import get_engine
        engine = get_engine()
    results = validate_all_edges(engine)
    return summarise_results(results)
