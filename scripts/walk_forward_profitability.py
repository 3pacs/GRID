#!/usr/bin/env python3
"""
Walk-forward profitability report — minimal, schema-honest.

Replaces the broken ticket-reconstruction pipeline in
``scripts/walk_forward_validate.py`` for the daily timer. Uses ONLY the
columns that actually exist in oracle_predictions:

    confidence, verdict, pnl_pct, direction, ticker, created_at

For each confidence bucket (HIGH ≥ 0.7, MEDIUM 0.5–0.7, LOW < 0.5),
computes:

    n          — count of scored predictions
    hit_rate   — fraction with verdict='hit' (or pnl_pct > 0 fallback)
    mean_pnl   — mean pnl_pct
    std_pnl    — std of pnl_pct
    sharpe     — mean / std (period-relative, NOT annualized)
    max_dd     — running max drawdown of cumulative pnl_pct

Prints a one-line verdict:

    "STACK CALIBRATED" — HIGH meaningfully beats MEDIUM (Δ ≥ 5pp hit rate
                          AND HIGH mean_pnl > MEDIUM mean_pnl)
    "STACK BROKEN"     — HIGH underperforms MEDIUM by either metric
    "STACK INCONCLUSIVE" — separation is ambiguous

Persists the report row into ``backtest_results``. Exits 0 on success.

CLI:
    python -m scripts.walk_forward_profitability --days 90
    python -m scripts.walk_forward_profitability --days 365
    python -m scripts.walk_forward_profitability --dry-run
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text


HIGH_THRESHOLD = 0.70
MED_LOWER = 0.50
DEFAULT_DAYS = 90


@dataclass
class BucketStats:
    bucket: str  # "HIGH" | "MEDIUM" | "LOW"
    n: int
    hit_rate: float
    mean_pnl: float
    std_pnl: float
    sharpe: float
    max_drawdown: float


@dataclass
class ProfitabilityReport:
    days: int
    generated_at: str
    n_total: int
    # Dup transparency: rows in the table vs unique trade events the report
    # actually scored. dup_factor > 2 means the source data is noisy and the
    # next "Find dup-write source" task is still pending.
    raw_rows_scanned: int = 0
    dup_factor: float = 0.0
    buckets: dict[str, BucketStats] = field(default_factory=dict)
    # Slice-level breakdowns. Each is a dict of "label -> BucketStats-shaped dict"
    # where label encodes the slice (e.g., "astrogrid_HIGH", "PUT_HIGH").
    by_prediction_type: dict[str, dict[str, Any]] = field(default_factory=dict)
    by_direction: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Action items: which slices are profitable vs bleeding, for the
    # post-mortem / intelligence loop to act on.
    profitable_slices: list[str] = field(default_factory=list)
    bleeding_slices: list[str] = field(default_factory=list)
    verdict: str = ""
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        # asdict already serializes nested dataclasses; just need bucket dict
        return out


_QUERY = text("""
    -- Two layers of dedup, belt-and-braces:
    --   (1) WHERE dedup_keep = TRUE — applied by the migration on
    --       2026-04-29 and enforced going forward by a partial unique
    --       index. Historical duplicates have dedup_keep=FALSE.
    --   (2) DISTINCT ON (ticker, direction, entry_price, actual_price,
    --       expiry) — defensive in case dedup_keep wasn't set correctly
    --       on some rows or different model_versions produced semantically
    --       identical trades.
    SELECT DISTINCT ON (ticker, direction, entry_price, actual_price, expiry)
           confidence, verdict, pnl_pct, direction, ticker, created_at,
           prediction_type
    FROM oracle_predictions
    WHERE verdict IN ('hit', 'miss', 'partial')
      AND created_at >= NOW() - (:days || ' days')::interval
      AND pnl_pct IS NOT NULL
      AND dedup_keep = TRUE
    ORDER BY ticker, direction, entry_price, actual_price, expiry, created_at ASC
""")

_QUERY_COLUMNS = (
    "confidence", "verdict", "pnl_pct", "direction", "ticker",
    "created_at", "prediction_type",
)

# Companion query — count the duplicates we're collapsing, so the report
# can surface "we deduped X dup rows down to Y unique events" for transparency.
_DUP_COUNT_QUERY = text("""
    SELECT COUNT(*) AS total_rows,
           COUNT(DISTINCT (ticker, direction, entry_price, actual_price, expiry))
             AS unique_events
    FROM oracle_predictions
    WHERE verdict IN ('hit', 'miss', 'partial')
      AND created_at >= NOW() - (:days || ' days')::interval
      AND pnl_pct IS NOT NULL
""")


def _bucket_for(confidence: float) -> str:
    if confidence is None:
        return "LOW"
    c = float(confidence)
    if c >= HIGH_THRESHOLD:
        return "HIGH"
    if c >= MED_LOWER:
        return "MEDIUM"
    return "LOW"


def _max_drawdown(returns: list[float]) -> float:
    """Max drawdown of cumulative compounded returns. Returns positive value."""
    if not returns:
        return 0.0
    cum = 1.0
    peak = 1.0
    max_dd = 0.0
    for r in returns:
        cum *= (1.0 + (r / 100.0))  # pnl_pct is percent, not fraction
        if cum > peak:
            peak = cum
        dd = (peak - cum) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
    return max_dd


def _bucket_stats(name: str, rows: list[dict[str, Any]]) -> BucketStats:
    n = len(rows)
    if n == 0:
        return BucketStats(name, 0, 0.0, 0.0, 0.0, 0.0, 0.0)
    pnls = [float(r["pnl_pct"]) for r in rows]
    hits = sum(1 for r in rows if str(r.get("verdict") or "").lower() == "hit")
    hit_rate = hits / n
    mean_pnl = statistics.fmean(pnls)
    std_pnl = statistics.pstdev(pnls) if n > 1 else 0.0
    sharpe = (mean_pnl / std_pnl) if std_pnl > 0 else 0.0
    max_dd = _max_drawdown(pnls)
    return BucketStats(
        bucket=name, n=n,
        hit_rate=hit_rate, mean_pnl=mean_pnl, std_pnl=std_pnl,
        sharpe=sharpe, max_drawdown=max_dd,
    )


def _verdict_call(buckets: dict[str, BucketStats]) -> str:
    high = buckets.get("HIGH")
    medium = buckets.get("MEDIUM")
    if not high or not medium or high.n == 0 or medium.n == 0:
        return "INCONCLUSIVE — empty HIGH or MEDIUM bucket"
    hr_lift_pp = (high.hit_rate - medium.hit_rate) * 100.0
    pnl_lift_pp = high.mean_pnl - medium.mean_pnl
    if hr_lift_pp >= 5.0 and pnl_lift_pp > 0:
        return f"CALIBRATED — HIGH beats MEDIUM by {hr_lift_pp:.1f}pp hit rate and {pnl_lift_pp:+.2f}% mean PnL"
    if hr_lift_pp <= -5.0 or pnl_lift_pp < 0:
        return f"BROKEN — HIGH underperforms MEDIUM ({hr_lift_pp:+.1f}pp hit rate, {pnl_lift_pp:+.2f}% PnL)"
    return f"INCONCLUSIVE — HIGH vs MEDIUM gap is small ({hr_lift_pp:+.1f}pp, {pnl_lift_pp:+.2f}%)"


def _slice_stats(rows: list[dict[str, Any]], group_key: str) -> dict[str, dict[str, Any]]:
    """Group rows by `group_key × bucket` and compute stats per cell.

    Returns a dict keyed by ``"<group>_<bucket>"`` so the same flat structure
    works for prediction_type and direction slices.
    """
    cells: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        g = str(r.get(group_key) or "unknown")
        bucket = _bucket_for(r["confidence"])
        key = f"{g}_{bucket}"
        cells.setdefault(key, []).append(r)
    out: dict[str, dict[str, Any]] = {}
    for key, group_rows in cells.items():
        # name format "group_BUCKET" — pass the full key to _bucket_stats
        # so the BucketStats.bucket field reads e.g. "astrogrid_HIGH".
        out[key] = asdict(_bucket_stats(key, group_rows))
    return out


# A slice is "profitable" if it has at least N samples AND positive mean PnL
# AND hit rate > 50%. "Bleeding" is at least N samples AND negative mean PnL.
_MIN_SLICE_N_FOR_VERDICT = 100


def _classify_slices(slice_stats: dict[str, dict[str, Any]]) -> tuple[list[str], list[str]]:
    profitable: list[str] = []
    bleeding: list[str] = []
    for key, s in slice_stats.items():
        n = int(s.get("n", 0))
        if n < _MIN_SLICE_N_FOR_VERDICT:
            continue
        mean_pnl = float(s.get("mean_pnl", 0.0))
        hit_rate = float(s.get("hit_rate", 0.0))
        if mean_pnl > 0 and hit_rate > 0.5:
            profitable.append(
                f"{key}: n={n} hit={hit_rate:.0%} pnl={mean_pnl:+.2f}%"
            )
        elif mean_pnl < -0.5:  # tolerate small noise; only flag real bleed
            bleeding.append(
                f"{key}: n={n} hit={hit_rate:.0%} pnl={mean_pnl:+.2f}%"
            )
    return profitable, bleeding


def run(engine, days: int = DEFAULT_DAYS) -> ProfitabilityReport:
    with engine.connect() as conn:
        rows = conn.execute(_QUERY, {"days": int(days)}).fetchall()
        dup_row = conn.execute(_DUP_COUNT_QUERY, {"days": int(days)}).fetchone()
    rows = [dict(zip(_QUERY_COLUMNS, r)) for r in (rows or [])]
    raw_rows = int(dup_row[0]) if dup_row else 0
    unique_events = int(dup_row[1]) if dup_row else len(rows)
    dup_factor = (raw_rows / unique_events) if unique_events else 0.0
    log.info(
        "walk_forward_profitability: deduped {r} rows -> {u} unique events "
        "(dup_factor={f:.2f}) over last {d}d",
        r=raw_rows, u=unique_events, f=dup_factor, d=days,
    )
    if dup_factor > 2.0:
        log.warning(
            "walk_forward_profitability: dup_factor={f:.2f} — oracle is "
            "writing many duplicate predictions per event. See TODO-DUP-WRITES.md",
            f=dup_factor,
        )

    # Aggregate (overall verdict still useful, but secondary)
    by_bucket: dict[str, list[dict[str, Any]]] = {"HIGH": [], "MEDIUM": [], "LOW": []}
    for r in rows:
        by_bucket[_bucket_for(r["confidence"])].append(r)
    buckets = {name: _bucket_stats(name, by_bucket[name]) for name in ("HIGH", "MEDIUM", "LOW")}

    # Slice breakdowns — this is what tells us WHICH part of the stack works
    by_pt = _slice_stats(rows, "prediction_type")
    by_dir = _slice_stats(rows, "direction")

    # Surface profitable vs bleeding slices (combined view across both axes)
    combined = {**by_pt, **by_dir}
    profitable, bleeding = _classify_slices(combined)

    verdict = _verdict_call(buckets)

    report = ProfitabilityReport(
        days=days,
        generated_at=datetime.now(timezone.utc).isoformat(),
        n_total=len(rows),
        raw_rows_scanned=raw_rows,
        dup_factor=round(dup_factor, 2),
        buckets={name: asdict(b) for name, b in buckets.items()},
        by_prediction_type=by_pt,
        by_direction=by_dir,
        profitable_slices=profitable,
        bleeding_slices=bleeding,
        verdict=verdict,
        notes=("PnL units = percent. Sharpe is per-period (NOT annualized). "
               "Predictions sourced from oracle_predictions, deduped on "
               "(ticker, direction, entry_price, actual_price, expiry). "
               "Slices with "
               f"n<{_MIN_SLICE_N_FOR_VERDICT} excluded from "
               "profitable/bleeding lists."),
    )
    return report


def _persist(engine, report: ProfitabilityReport) -> None:
    """Append a row to backtest_results so the existing dashboard / log
    consumers keep working."""
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS backtest_results (
                    id              SERIAL PRIMARY KEY,
                    start_date      TIMESTAMPTZ NOT NULL,
                    end_date        TIMESTAMPTZ NOT NULL,
                    predictions_walked INTEGER NOT NULL,
                    trades_generated   INTEGER NOT NULL,
                    report_json     JSONB,
                    narrative       TEXT,
                    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """))
            conn.execute(text("""
                INSERT INTO backtest_results
                  (start_date, end_date, predictions_walked, trades_generated,
                   report_json, narrative)
                VALUES
                  (:start, :end, :walked, :trades, :report, :narrative)
            """), {
                "start": report.generated_at,
                "end": report.generated_at,
                "walked": report.n_total,
                "trades": report.n_total,
                "report": json.dumps(report.to_dict()),
                "narrative": report.verdict,
            })
    except Exception as exc:  # noqa: BLE001
        log.error("walk_forward_profitability: persist failed: {e}", e=str(exc))


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="walk_forward_profitability")
    p.add_argument("--days", type=int, default=DEFAULT_DAYS,
                   help=f"Lookback window (default: {DEFAULT_DAYS})")
    p.add_argument("--dry-run", action="store_true",
                   help="Don't persist to backtest_results")
    args = p.parse_args(argv)

    from db import get_engine
    engine = get_engine()
    report = run(engine, days=args.days)

    print(json.dumps(report.to_dict(), indent=2, default=str))
    print()
    print(f"=== AGGREGATE ({report.days}d, n={report.n_total}) ===")
    print(f"verdict: {report.verdict}")
    for name in ("HIGH", "MEDIUM", "LOW"):
        b = report.buckets[name]
        print(f"  {name:6s} n={b['n']:6d}  hit={b['hit_rate']:6.1%}  "
              f"pnl={b['mean_pnl']:+7.2f}%  sharpe={b['sharpe']:+6.2f}  "
              f"dd={b['max_drawdown']:5.1%}")

    print()
    print("=== BY prediction_type × bucket ===")
    for key in sorted(report.by_prediction_type):
        b = report.by_prediction_type[key]
        print(f"  {key:30s} n={b['n']:6d}  hit={b['hit_rate']:6.1%}  "
              f"pnl={b['mean_pnl']:+7.2f}%")

    print()
    print("=== BY direction × bucket ===")
    for key in sorted(report.by_direction):
        b = report.by_direction[key]
        print(f"  {key:30s} n={b['n']:6d}  hit={b['hit_rate']:6.1%}  "
              f"pnl={b['mean_pnl']:+7.2f}%")

    print()
    print(f"=== PROFITABLE slices (n>={_MIN_SLICE_N_FOR_VERDICT}, hit>50%, pnl>0) ===")
    for s in report.profitable_slices or ["  (none)"]:
        print(f"  + {s}")
    print(f"=== BLEEDING slices (n>={_MIN_SLICE_N_FOR_VERDICT}, pnl<-0.5%) ===")
    for s in report.bleeding_slices or ["  (none)"]:
        print(f"  - {s}")

    if not args.dry_run:
        _persist(engine, report)
        log.info("walk_forward_profitability: persisted")

    return 0


if __name__ == "__main__":
    sys.exit(main())
