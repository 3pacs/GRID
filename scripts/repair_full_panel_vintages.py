#!/usr/bin/env python3
"""Repair plan for ``{ticker}_full`` resolved vintages written on the wrong price basis.

Dry run by default: reads only, reports counts, writes nothing. ``--apply``
performs the repair and is refused without ``--run-tag`` and outside the
operator's explicit go-ahead (this script cannot check that; the person
running it can). Nothing here ever UPDATEs or DELETEs a row.

Background (docs/reference/PRICE_PANEL_BASIS.md, 2026-11-08). Until
normalization/entity_map.py's PRICE BASIS POLICY both ``YF:<T>:adj_close``
and ``YF:<T>:close`` fed ``<t>_full``, so its vintages alternate between
dividend/split-adjusted and raw closes. ``build_price_panel`` read the
latest vintage per date and the universal split adjuster compounded the
flips (XLP first-to-last ratio 8e201, GLD 2,071x, NVDA 366x). The code fix
stops new wrong-basis vintages; the rows already written are still served
for every ``as_of`` that reaches them. This script plans and, on request,
performs the repair of those rows.

What the repair does, per feature (one transaction each):

1. Classifies every resolved row of the feature by matching its value
   against the raw rows of the two price fields for that obs_date:
   ON_BASIS (equals the target field -- ``adj_close``, or ``close`` for
   RAW_CLOSE_FULL_FEATURES), OFF_BASIS (equals the other field only),
   AMBIGUOUS (equals both; harmless, kept), UNMATCHED (equals neither;
   kept, reported). An ON_BASIS/UNMATCHED row more than 50 % away from
   the median of its neighbouring on-basis cells is an OUTLIER (the
   ~0.6 holiday prints a yfinance writer stamped into XLP/XLB/XLRE
   adj_close).
2. ``--apply`` retracts OFF_BASIS and OUTLIER rows through
   ``resolved_series_retractions`` (append-only, PIT-safe: reads with
   ``as_of`` before the retraction day still reproduce exactly what they
   served; reads from today on no longer see the row). The row itself is
   not touched.
3. A cell left with no visible vintage gets ONE new vintage dated the
   repair day, carrying the latest on-basis raw value (ON CONFLICT DO
   NOTHING on the unique key). A cell with no on-basis raw row stays dark
   and is counted.

Reads are bounded and indexed: ``resolved_series`` by feature_id,
``raw_series`` by (series_id, obs_date range) in one-year chunks, never an
IN-list over raw_series. Every read-only statement runs in a READ ONLY
transaction with a statement_timeout. Like scripts/audit_backdated_vintages.py
it refuses to run between 03:30 and 10:30 UTC (backup/DB window) unless
``--allow-db-window`` is passed.

Usage::

    python scripts/repair_full_panel_vintages.py --feature xlp_full --feature gld_full
    python scripts/repair_full_panel_vintages.py --all-full-features --max-features 40 --out plan.json
    python scripts/repair_full_panel_vintages.py --feature xlp_full --apply --run-tag full-basis-20261108

Exit status: 0 complete (dry run or apply); 1 any feature reported an error
or the run stopped at the DB window; 2 usage refused (apply without run-tag,
retractions table missing).

Output: JSON on stdout (machine-readable first), one summary line per
feature on stderr. The ``downstream`` block names the records that read the
corrupted panel and must be re-labelled as invalid by their owners; this
script does not touch them.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text  # noqa: E402
from sqlalchemy.engine import Connection, Engine  # noqa: E402

from normalization.entity_map import RAW_CLOSE_FULL_FEATURES  # noqa: E402

DB_WINDOW_START = time(3, 30)
DB_WINDOW_END = time(10, 30)
DEFAULT_STATEMENT_TIMEOUT_MS = 60_000
DEFAULT_START = date(2010, 1, 1)
VALUE_TOL_REL = 1e-6
VALUE_TOL_ABS = 1e-6
OUTLIER_REL = 0.50
OUTLIER_NEIGHBOURS = 5
RETRACTION_REASON = (
    "wrong price basis for {feature}: value matches YF:{ticker}:{field}, "
    "basis is YF:{ticker}:{target} (PRICE BASIS POLICY 2026-11-08)"
)
OUTLIER_REASON = (
    "outlier vintage for {feature}: {pct:+.0%} vs neighbouring on-basis cells "
    "(junk print, PRICE BASIS POLICY 2026-11-08)"
)

ON_BASIS = "ON_BASIS"
OFF_BASIS = "OFF_BASIS"
AMBIGUOUS = "AMBIGUOUS"
UNMATCHED = "UNMATCHED"
OUTLIER = "OUTLIER"
ALREADY_RETRACTED = "ALREADY_RETRACTED"

#: Records produced from the corrupted panel. Owners re-label; this script
#: reads none of them and writes none of them.
DOWNSTREAM_RECORDS: list[dict[str, str]] = [
    {
        "record": "astrogrid.backtest_run / backtest_result, run_key rotation_variant_*",
        "producer": "alpha_research/strategies/rotation_variant_backtest.py (fleet job rotation_variant)",
        "why": "242 runs 2026-05-08..05-19 read build_price_panel over the mixed-basis panel; "
               "Sharpe 4.5-4.7 and +9,600 % on every config are the vintage-flip artefact",
        "action": "mark invalid; re-run only after this repair and with the gate in "
                  "docs/paper_log/high-sharpe-ensemble-v1-preregistration.md",
    },
    {
        "record": "paper_trades strategy adaptive_rotation_live (wallet grid_rotation_paper_7913a6f5)",
        "producer": "alpha_research/strategies/adaptive_rotation.py via scripts/rotation_paper_trader.py",
        "why": "signals from build_price_panel; 19 of 23 trades at a flat price (stale-price bug)",
        "action": "no usable record; keep as history, exclude from any performance table",
    },
    {
        "record": "QuantaAlpha / LightGBM rank signals and their logged scores (2026)",
        "producer": "alpha_research/signals/quanta_alpha.py over build_price_panel",
        "why": "equity ranks computed on the mixed-basis panel",
        "action": "owner to re-label the 2026 runs invalid; recompute after repair",
    },
    {
        "record": "realized_alpha_daily rows with spy_feature = spy_full",
        "producer": "alpha_research/realized_alpha.py load_spy_path (PITStore, LATEST_AS_OF)",
        "why": "spy_full carried adjusted vintages (2026-04-24/29) next to the receipted raw close",
        "action": "re-score the affected windows after repair; keep the old rows (table is a log)",
    },
]


class DbWindowRefused(RuntimeError):
    pass


def check_window(now: datetime, allow: bool) -> None:
    t = now.astimezone(timezone.utc).time()
    if not allow and DB_WINDOW_START <= t <= DB_WINDOW_END:
        raise DbWindowRefused(
            f"{now.astimezone(timezone.utc).isoformat()} is inside the 03:30-10:30 UTC DB window; "
            "pass --allow-db-window for a non-production database"
        )


def _readonly(conn: Connection, timeout_ms: int) -> None:
    conn.execute(text("SET TRANSACTION READ ONLY"))
    conn.execute(text("SELECT set_config('statement_timeout', :ms, true)"), {"ms": str(int(timeout_ms))})


def _timeout(conn: Connection, timeout_ms: int) -> None:
    conn.execute(text("SELECT set_config('statement_timeout', :ms, true)"), {"ms": str(int(timeout_ms))})


# ---------------------------------------------------------------------------
# pure planning
# ---------------------------------------------------------------------------

def ticker_for_feature(feature: str) -> str:
    """``xlp_full`` -> ``XLP``, ``brk-b_full`` -> ``BRK-B``, ``btc_usd_full`` -> ``BTC-USD``."""
    if not feature.endswith("_full"):
        raise ValueError(f"not a _full feature: {feature}")
    return feature[: -len("_full")].upper().replace("_", "-")


def target_field(feature: str) -> str:
    return "close" if feature in RAW_CLOSE_FULL_FEATURES else "adj_close"


def _close(a: float, b: float) -> bool:
    return abs(a - b) <= max(VALUE_TOL_ABS, VALUE_TOL_REL * max(abs(a), abs(b)))


@dataclass
class ResolvedRow:
    id: int
    obs_date: date
    vintage_date: date
    value: float
    source_id: int | None
    retracted: bool = False
    cls: str = ""
    matched_field: str | None = None
    outlier_pct: float | None = None


@dataclass
class RawCandidate:
    value: float
    pull_date: date
    source_id: int


def classify_rows(
    rows: list[ResolvedRow],
    raw: dict[str, dict[date, list[RawCandidate]]],
    target: str,
) -> None:
    """Set ``cls`` / ``matched_field`` on every row (in place).

    A row matches a field when a SUCCESS raw row of that field on the same
    obs_date carries the same value (relative 1e-6). Same-vintage matches
    are preferred but any vintage counts: the resolver's vintage is the pull
    date, yet a re-pulled unchanged value is the same observation.
    """
    other = "close" if target == "adj_close" else "adj_close"
    for r in rows:
        if r.retracted:
            r.cls = ALREADY_RETRACTED
            continue
        hit_target = any(_close(r.value, c.value) for c in raw.get(target, {}).get(r.obs_date, []))
        hit_other = any(_close(r.value, c.value) for c in raw.get(other, {}).get(r.obs_date, []))
        if hit_target and hit_other:
            r.cls, r.matched_field = AMBIGUOUS, "both"
        elif hit_target:
            r.cls, r.matched_field = ON_BASIS, target
        elif hit_other:
            r.cls, r.matched_field = OFF_BASIS, other
        else:
            r.cls, r.matched_field = UNMATCHED, None


def latest_per_cell(rows: Iterable[ResolvedRow], keep: Callable[[ResolvedRow], bool]) -> dict[date, ResolvedRow]:
    """LATEST_AS_OF as of today over the rows ``keep`` admits."""
    best: dict[date, ResolvedRow] = {}
    for r in rows:
        if not keep(r):
            continue
        cur = best.get(r.obs_date)
        if cur is None or r.vintage_date > cur.vintage_date:
            best[r.obs_date] = r
    return best


def flag_outliers(rows: list[ResolvedRow], neighbours: int = OUTLIER_NEIGHBOURS, rel: float = OUTLIER_REL) -> int:
    """Mark ON_BASIS / UNMATCHED / AMBIGUOUS rows far from their neighbours as OUTLIER.

    The reference for a row is the median of the latest on-basis values of
    the ``neighbours`` nearest cells on each side (its own cell excluded).
    Returns the number of rows flagged.
    """
    ref = latest_per_cell(rows, lambda r: r.cls in (ON_BASIS, AMBIGUOUS))
    dates = sorted(ref)
    if len(dates) < 3:
        return 0
    pos = {d: i for i, d in enumerate(dates)}
    flagged = 0
    for r in rows:
        if r.cls not in (ON_BASIS, UNMATCHED, AMBIGUOUS):
            continue
        i = pos.get(r.obs_date)
        if i is None:
            # Not a reference cell itself: anchor on the nearest earlier cell.
            i = sum(1 for d in dates if d < r.obs_date)
            window = dates[max(0, i - neighbours): i] + dates[i: i + neighbours]
        else:
            window = dates[max(0, i - neighbours): i] + dates[i + 1: i + 1 + neighbours]
        vals = [ref[d].value for d in window if ref[d].value > 0]
        if len(vals) < 3:
            continue
        med = statistics.median(vals)
        if med <= 0:
            continue
        pct = r.value / med - 1.0
        if abs(pct) > rel:
            r.cls, r.outlier_pct = OUTLIER, pct
            flagged += 1
    return flagged


def ratio_and_jumps(series: dict[date, float]) -> dict[str, Any]:
    """First-to-last ratio and count of day moves above 10 % for a date->value map."""
    if len(series) < 2:
        return {"n": len(series), "ratio": None, "jumps_gt_10pct": 0}
    ds = sorted(series)
    vals = [series[d] for d in ds]
    jumps = sum(1 for a, b in zip(vals, vals[1:]) if a and abs(b / a - 1.0) > 0.10)
    return {
        "n": len(ds), "first": [ds[0].isoformat(), vals[0]], "last": [ds[-1].isoformat(), vals[-1]],
        "ratio": (vals[-1] / vals[0]) if vals[0] else None, "jumps_gt_10pct": jumps,
    }


@dataclass
class FeaturePlan:
    feature: str
    ticker: str
    target_field: str
    feature_id: int | None = None
    rows: int = 0
    cells: int = 0
    by_class: dict[str, int] = field(default_factory=dict)
    retract: list[dict[str, Any]] = field(default_factory=list)
    revintage: list[dict[str, Any]] = field(default_factory=list)
    cells_dark_after: int = 0
    cells_revintaged: int = 0
    cells_left_dark: int = 0
    raw_rows_read: dict[str, int] = field(default_factory=dict)
    served_before: dict[str, Any] = field(default_factory=dict)
    served_after: dict[str, Any] = field(default_factory=dict)
    examples: list[dict[str, Any]] = field(default_factory=list)
    applied: dict[str, int] | None = None
    error: str | None = None

    def summary(self) -> dict[str, Any]:
        d = asdict(self)
        d["retract"] = len(self.retract)
        d["revintage"] = len(self.revintage)
        return d


def plan_feature(
    feature: str,
    rows: list[ResolvedRow],
    raw: dict[str, dict[date, list[RawCandidate]]],
    *,
    repair_day: date,
    feature_id: int | None = None,
) -> FeaturePlan:
    """Pure: classify, flag, simulate the served series before/after, list actions."""
    target = target_field(feature)
    plan = FeaturePlan(feature=feature, ticker=ticker_for_feature(feature), target_field=target,
                       feature_id=feature_id, rows=len(rows), cells=len({r.obs_date for r in rows}))
    classify_rows(rows, raw, target)
    flag_outliers(rows)
    plan.by_class = {}
    for r in rows:
        plan.by_class[r.cls] = plan.by_class.get(r.cls, 0) + 1
    plan.raw_rows_read = {f: sum(len(v) for v in by_date.values()) for f, by_date in raw.items()}

    before = latest_per_cell(rows, lambda r: not r.retracted)
    plan.served_before = ratio_and_jumps({d: r.value for d, r in before.items()})

    to_retract = [r for r in rows if r.cls in (OFF_BASIS, OUTLIER)]
    other = "close" if target == "adj_close" else "adj_close"
    for r in to_retract:
        if r.cls == OFF_BASIS:
            reason = RETRACTION_REASON.format(feature=feature, ticker=plan.ticker, field=other, target=target)
        else:
            reason = OUTLIER_REASON.format(feature=feature, pct=r.outlier_pct or 0.0)
        plan.retract.append({"resolved_id": r.id, "obs_date": r.obs_date.isoformat(),
                             "vintage_date": r.vintage_date.isoformat(), "value": r.value,
                             "cls": r.cls, "reason": reason})
    retract_keys = {(r.obs_date, r.vintage_date) for r in to_retract}

    after = latest_per_cell(rows, lambda r: not r.retracted and (r.obs_date, r.vintage_date) not in retract_keys)
    dark = sorted({r.obs_date for r in rows} - set(after))
    plan.cells_dark_after = len(dark)
    for d in dark:
        cands = raw.get(target, {}).get(d, [])
        if not cands:
            continue
        best = max(cands, key=lambda c: c.pull_date)
        if any(r.obs_date == d and r.vintage_date == repair_day for r in rows):
            # Today's key is taken by a row we are retracting; cannot re-vintage today.
            continue
        plan.revintage.append({"obs_date": d.isoformat(), "value": best.value, "source_id": best.source_id,
                               "from_pull_date": best.pull_date.isoformat(), "vintage_date": repair_day.isoformat()})
        after[d] = ResolvedRow(id=-1, obs_date=d, vintage_date=repair_day, value=best.value, source_id=best.source_id)
    plan.cells_revintaged = len(plan.revintage)
    plan.cells_left_dark = plan.cells_dark_after - plan.cells_revintaged
    plan.served_after = ratio_and_jumps({d: r.value for d, r in after.items()})

    for r in to_retract[:6]:
        plan.examples.append({"obs_date": r.obs_date.isoformat(), "vintage_date": r.vintage_date.isoformat(),
                              "value": r.value, "cls": r.cls, "matched_field": r.matched_field,
                              "outlier_pct": r.outlier_pct})
    return plan


# ---------------------------------------------------------------------------
# bounded reads
# ---------------------------------------------------------------------------

_RESOLVED_SQL = text("""
    SELECT rs.id, rs.obs_date, rs.vintage_date, rs.value, rs.source_priority_used,
           EXISTS (
               SELECT 1 FROM resolved_series_retractions rr
               WHERE rr.feature_id = rs.feature_id AND rr.obs_date = rs.obs_date
                 AND rr.vintage_date = rs.vintage_date
           ) AS retracted
    FROM resolved_series rs
    WHERE rs.feature_id = :fid AND rs.obs_date >= :start
    ORDER BY rs.obs_date, rs.vintage_date
""")
_RESOLVED_SQL_NO_RETRACTIONS = text("""
    SELECT rs.id, rs.obs_date, rs.vintage_date, rs.value, rs.source_priority_used, FALSE AS retracted
    FROM resolved_series rs
    WHERE rs.feature_id = :fid AND rs.obs_date >= :start
    ORDER BY rs.obs_date, rs.vintage_date
""")
_RAW_CHUNK_SQL = text("""
    SELECT obs_date, value, pull_timestamp, source_id
    FROM raw_series
    WHERE series_id = :sid AND pull_status = 'SUCCESS'
      AND obs_date BETWEEN :lo AND :hi
""")


def _as_date(v: Any) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return date.fromisoformat(str(v)[:10])


def read_resolved(conn: Connection, feature_id: int, start: date, has_retractions: bool) -> list[ResolvedRow]:
    sql = _RESOLVED_SQL if has_retractions else _RESOLVED_SQL_NO_RETRACTIONS
    out: list[ResolvedRow] = []
    for rid, od, vd, val, src, retracted in conn.execute(sql, {"fid": feature_id, "start": start}):
        out.append(ResolvedRow(id=int(rid), obs_date=_as_date(od), vintage_date=_as_date(vd), value=float(val),
                               source_id=int(src) if src is not None else None, retracted=bool(retracted)))
    return out


def read_raw_by_field(conn: Connection, ticker: str, start: date, end: date) -> dict[str, dict[date, list[RawCandidate]]]:
    """SUCCESS raw rows of ``YF:<ticker>:close`` and ``:adj_close`` in yearly obs_date chunks."""
    out: dict[str, dict[date, list[RawCandidate]]] = {"close": {}, "adj_close": {}}
    for fld in out:
        sid = f"YF:{ticker}:{fld}"
        lo = start
        while lo <= end:
            hi = min(date(lo.year, 12, 31), end)
            for od, val, ts, src in conn.execute(_RAW_CHUNK_SQL, {"sid": sid, "lo": lo, "hi": hi}):
                if val is None:
                    continue
                pulled = ts.astimezone(timezone.utc).date() if isinstance(ts, datetime) and ts.tzinfo else _as_date(ts)
                out[fld].setdefault(_as_date(od), []).append(RawCandidate(float(val), pulled, int(src)))
            lo = hi + timedelta(days=1)
    return out


def retractions_table_exists(conn: Connection) -> bool:
    return bool(conn.execute(text("SELECT to_regclass(:t) IS NOT NULL"), {"t": "resolved_series_retractions"}).scalar())


def plan_feature_db(engine: Engine, feature: str, *, start: date, repair_day: date,
                    timeout_ms: int) -> FeaturePlan:
    try:
        with engine.connect() as conn, conn.begin():
            _readonly(conn, timeout_ms)
            row = conn.execute(text("SELECT id FROM feature_registry WHERE name = :n"), {"n": feature}).fetchone()
            if row is None:
                p = FeaturePlan(feature=feature, ticker=ticker_for_feature(feature), target_field=target_field(feature))
                p.error = "feature not in feature_registry"
                return p
            fid = int(row[0])
            rows = read_resolved(conn, fid, start, retractions_table_exists(conn))
            end = max((r.obs_date for r in rows), default=start)
            raw = read_raw_by_field(conn, ticker_for_feature(feature), start, end)
        return plan_feature(feature, rows, raw, repair_day=repair_day, feature_id=fid)
    except Exception as exc:  # one feature's timeout must not hide the others
        p = FeaturePlan(feature=feature, ticker=ticker_for_feature(feature), target_field=target_field(feature))
        p.error = f"{type(exc).__name__}: {str(exc).splitlines()[0][:200]}"
        return p


# ---------------------------------------------------------------------------
# apply (explicit flag only)
# ---------------------------------------------------------------------------

_RETRACT_SQL = text("""
    INSERT INTO resolved_series_retractions (feature_id, obs_date, vintage_date, reason, run_tag)
    VALUES (:fid, :od, :vd, :reason, :tag)
    ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING
""")
_REVINTAGE_SQL = text("""
    INSERT INTO resolved_series
        (feature_id, obs_date, release_date, vintage_date, value, source_priority_used, conflict_flag)
    VALUES (:fid, :od, :vd, :vd, :val, :src, FALSE)
    ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING
""")


def apply_plan(engine: Engine, plan: FeaturePlan, *, run_tag: str, timeout_ms: int) -> dict[str, int]:
    """Write the plan's retractions and re-vintages in one transaction. Appends only."""
    if plan.feature_id is None or plan.error:
        raise RuntimeError(f"{plan.feature}: nothing to apply ({plan.error})")
    done = {"retracted": 0, "revintaged": 0}
    with engine.begin() as conn:
        _timeout(conn, timeout_ms)
        if not retractions_table_exists(conn):
            raise RuntimeError("resolved_series_retractions does not exist; apply the migration first")
        for r in plan.retract:
            res = conn.execute(_RETRACT_SQL, {"fid": plan.feature_id, "od": date.fromisoformat(r["obs_date"]),
                                              "vd": date.fromisoformat(r["vintage_date"]),
                                              "reason": r["reason"], "tag": run_tag})
            done["retracted"] += max(0, int(res.rowcount or 0))
        for v in plan.revintage:
            res = conn.execute(_REVINTAGE_SQL, {"fid": plan.feature_id, "od": date.fromisoformat(v["obs_date"]),
                                                "vd": date.fromisoformat(v["vintage_date"]),
                                                "val": v["value"], "src": v["source_id"]})
            done["revintaged"] += max(0, int(res.rowcount or 0))
    return done


# ---------------------------------------------------------------------------
# driver
# ---------------------------------------------------------------------------

def list_full_features(engine: Engine, limit: int, timeout_ms: int) -> list[str]:
    with engine.connect() as conn, conn.begin():
        _readonly(conn, timeout_ms)
        rows = conn.execute(text("SELECT name FROM feature_registry WHERE name LIKE :pat ORDER BY name LIMIT :n"),
                            {"pat": "%\\_full", "n": limit})
        return [r[0] for r in rows]


def run(engine: Engine, features: list[str], *, start: date, apply: bool, run_tag: str | None,
        timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS, allow_db_window: bool = False,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> dict[str, Any]:
    started = clock()
    check_window(started, allow_db_window)
    repair_day = started.astimezone(timezone.utc).date()
    plans: list[dict[str, Any]] = []
    stopped = None
    for f in features:
        try:
            check_window(clock(), allow_db_window)
        except DbWindowRefused as exc:
            stopped = f"db_window: {exc}"
            break
        plan = plan_feature_db(engine, f, start=start, repair_day=repair_day, timeout_ms=timeout_ms)
        if apply and plan.error is None and (plan.retract or plan.revintage):
            try:
                plan.applied = apply_plan(engine, plan, run_tag=run_tag or "", timeout_ms=timeout_ms)
            except Exception as exc:  # noqa: BLE001
                plan.error = f"apply failed: {type(exc).__name__}: {str(exc).splitlines()[0][:200]}"
        print(
            f"{plan.feature:>14} rows={plan.rows:>7} cells={plan.cells:>6} "
            f"retract={len(plan.retract):>6} revintage={len(plan.revintage):>5} "
            f"left_dark={plan.cells_left_dark:>5} ratio {plan.served_before.get('ratio')!s:>12} -> "
            f"{plan.served_after.get('ratio')!s:>12} jumps {plan.served_before.get('jumps_gt_10pct')} -> "
            f"{plan.served_after.get('jumps_gt_10pct')}"
            + (f"  ERROR {plan.error}" if plan.error else "")
            + (f"  APPLIED {plan.applied}" if plan.applied else ""),
            file=sys.stderr,
        )
        plans.append(plan.summary())
    return {
        "generated_at": started.astimezone(timezone.utc).isoformat(), "mode": "apply" if apply else "dry_run",
        "run_tag": run_tag, "repair_day": repair_day.isoformat(), "start": start.isoformat(),
        "statement_timeout_ms": timeout_ms, "features": plans, "stopped": stopped,
        "complete": stopped is None and len(plans) == len(features),
        "errors": sum(1 for p in plans if p["error"]),
        "totals": {
            "rows": sum(p["rows"] for p in plans), "retract": sum(p["retract"] for p in plans),
            "revintage": sum(p["revintage"] for p in plans), "left_dark": sum(p["cells_left_dark"] for p in plans),
        },
        "downstream": DOWNSTREAM_RECORDS,
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--feature", action="append", default=[], help="a _full feature name (repeatable)")
    p.add_argument("--all-full-features", action="store_true", help="every feature_registry name ending in _full")
    p.add_argument("--max-features", type=int, default=100)
    p.add_argument("--start", type=date.fromisoformat, default=DEFAULT_START, help="first obs_date (default 2010-01-01)")
    p.add_argument("--apply", action="store_true", help="WRITE retractions and re-vintages (default: dry run)")
    p.add_argument("--run-tag", help="required with --apply; stored on every retraction")
    p.add_argument("--allow-db-window", action="store_true", help="run inside 03:30-10:30 UTC (non-production only)")
    p.add_argument("--statement-timeout-ms", type=int, default=DEFAULT_STATEMENT_TIMEOUT_MS)
    p.add_argument("--out", type=Path, help="write the JSON report here as well as stdout")
    return p


def main(argv: list[str] | None = None, engine: Engine | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.apply and not args.run_tag:
        print("--apply requires --run-tag", file=sys.stderr)
        return 2
    if not args.feature and not args.all_full_features:
        print("name at least one --feature or pass --all-full-features", file=sys.stderr)
        return 2
    if engine is None:
        from db import get_engine  # noqa: PLC0415 -- deferred: config needs the service env

        engine = get_engine()
    features = list(args.feature)
    if args.all_full_features:
        features += [f for f in list_full_features(engine, args.max_features, args.statement_timeout_ms)
                     if f not in features]
    try:
        report = run(engine, features, start=args.start, apply=args.apply, run_tag=args.run_tag,
                     timeout_ms=args.statement_timeout_ms, allow_db_window=args.allow_db_window)
    except DbWindowRefused as exc:
        print(str(exc), file=sys.stderr)
        return 1
    payload = json.dumps(report, indent=1, default=str)
    print(payload)
    if args.out:
        args.out.write_text(payload)
    return 0 if report["complete"] and report["errors"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
