#!/usr/bin/env python3
"""
Signal Replay Backtester — same-day verdict on whether a signal's
direction mapping is informative.

Why this exists: changing signal direction logic and waiting 30 days
for new predictions to expire is too slow an iteration loop. Every
signal we publish writes a row into signal_sources with `signal_type`
(BUY/SELL) AND `outcome_return` (the realized N-day return after the
signal). That data alone tells us if the direction was informative —
no need to wait.

For each (source_type, source_id):
    avg_return_when_BUY   = mean(outcome_return | signal_type='BUY')
    avg_return_when_SELL  = mean(outcome_return | signal_type='SELL')
    lift                  = avg_return_when_BUY - avg_return_when_SELL

    If lift > +0.5%, BUY/SELL direction is informative as-published
    If lift < -0.5%, direction is INVERTED (signal works if we flip BUY<->SELL)
    If |lift| < 0.5%, direction is noise (publish as NEUTRAL)

Usage:
    python -m scripts.signal_replay_backtest                       # all sources
    python -m scripts.signal_replay_backtest --source alpha_research  # filter
    python -m scripts.signal_replay_backtest --min-n 100              # noise floor
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text


_QUERY = text("""
    SELECT
        source_type,
        source_id,
        signal_type,
        COUNT(*) AS n,
        AVG(outcome_return)::float AS avg_return,
        STDDEV(outcome_return)::float AS std_return,
        SUM(CASE WHEN outcome = 'CORRECT' THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0) AS hit_rate
    FROM signal_sources
    WHERE outcome IN ('CORRECT', 'WRONG')
      AND outcome_return IS NOT NULL
      AND signal_date >= NOW() - (:days || ' days')::interval
    GROUP BY source_type, source_id, signal_type
""")


def _verdict(lift: float, n_buy: int, n_sell: int, min_n: int) -> str:
    if n_buy < min_n or n_sell < min_n:
        return f"INSUFFICIENT (n_buy={n_buy}, n_sell={n_sell}, need ≥{min_n} each)"
    if lift > 0.5:
        return f"INFORMATIVE (lift={lift:+.2f}%, keep direction)"
    if lift < -0.5:
        return f"INVERTED (lift={lift:+.2f}%, flip BUY↔SELL)"
    return f"NOISE (lift={lift:+.2f}%, publish NEUTRAL)"


def run(engine, days: int = 90, source_filter: str | None = None,
        min_n: int = 30) -> dict[str, Any]:
    with engine.connect() as conn:
        rows = conn.execute(_QUERY, {"days": days}).fetchall()

    # Group by (source_type, source_id) → {BUY: stats, SELL: stats}
    groups: dict[tuple[str, str], dict[str, dict]] = {}
    for r in rows:
        key = (r[0], r[1])
        if source_filter and source_filter not in str(key[0]) and source_filter not in str(key[1]):
            continue
        groups.setdefault(key, {})[r[2]] = {
            "n": int(r[3]),
            "avg_return": float(r[4] or 0),
            "std_return": float(r[5] or 0),
            "hit_rate": float(r[6] or 0),
        }

    results = []
    for (src_type, src_id), by_dir in groups.items():
        buy = by_dir.get("BUY", {})
        sell = by_dir.get("SELL", {})
        n_buy, n_sell = buy.get("n", 0), sell.get("n", 0)
        if n_buy + n_sell < 5:
            continue
        avg_buy = buy.get("avg_return", 0.0)
        avg_sell = sell.get("avg_return", 0.0)
        lift = avg_buy - avg_sell
        results.append({
            "source_type": src_type,
            "source_id": src_id,
            "n_buy": n_buy,
            "n_sell": n_sell,
            "avg_return_buy": avg_buy,
            "avg_return_sell": avg_sell,
            "lift_pct": lift,
            "hit_rate_buy": buy.get("hit_rate", 0.0),
            "hit_rate_sell": sell.get("hit_rate", 0.0),
            "verdict": _verdict(lift, n_buy, n_sell, min_n),
        })

    # Sort by absolute lift descending — biggest signals (positive or
    # negative) at the top so the user sees what to flip / keep / kill.
    results.sort(key=lambda r: abs(r["lift_pct"]), reverse=True)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lookback_days": days,
        "min_n_per_direction": min_n,
        "n_sources": len(results),
        "sources": results,
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="signal_replay_backtest")
    p.add_argument("--days", type=int, default=90,
                   help="Lookback window in days (default: 90)")
    p.add_argument("--source", type=str, default=None,
                   help="Filter by substring of source_type or source_id")
    p.add_argument("--min-n", type=int, default=30,
                   help="Minimum samples per direction for a verdict (default: 30)")
    p.add_argument("--limit", type=int, default=40,
                   help="Cap rows printed (default: 40)")
    args = p.parse_args(argv)

    from db import get_engine
    engine = get_engine()
    report = run(engine, days=args.days, source_filter=args.source, min_n=args.min_n)

    print(f"=== Signal Replay Backtest ({report['lookback_days']}d, "
          f"{report['n_sources']} sources analysed) ===\n")
    print(f"{'source_type':<32} {'source_id':<28} {'n_buy':>6} {'n_sell':>7} "
          f"{'avg_buy':>8} {'avg_sell':>9} {'lift':>8} verdict")
    print("-" * 140)
    for s in report["sources"][:args.limit]:
        print(f"{s['source_type'][:32]:<32} {str(s['source_id'])[:28]:<28} "
              f"{s['n_buy']:>6} {s['n_sell']:>7} "
              f"{s['avg_return_buy']:>+7.2f}% {s['avg_return_sell']:>+8.2f}% "
              f"{s['lift_pct']:>+7.2f}% {s['verdict']}")

    # Summary buckets
    buckets = {"INFORMATIVE": 0, "INVERTED": 0, "NOISE": 0, "INSUFFICIENT": 0}
    for s in report["sources"]:
        for k in buckets:
            if s["verdict"].startswith(k):
                buckets[k] += 1
                break
    print(f"\n=== Summary ===")
    for k, v in buckets.items():
        print(f"  {k:<14} {v:>4} sources")
    print(f"\nINVERTED sources should be flipped (BUY↔SELL).")
    print(f"NOISE sources should be published as NEUTRAL.")
    print(f"INFORMATIVE sources are working as published — keep them.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
