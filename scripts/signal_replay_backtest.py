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


_QUERY_BY_TYPE = text("""
    SELECT
        source_type AS group_key1,
        ''::text    AS group_key2,
        signal_type,
        COUNT(*) AS n,
        AVG(outcome_return)::float AS avg_return,
        STDDEV(outcome_return)::float AS std_return,
        SUM(CASE WHEN outcome = 'CORRECT' THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0) AS hit_rate
    FROM signal_sources
    WHERE outcome IN ('CORRECT', 'WRONG')
      AND outcome_return IS NOT NULL
      AND signal_date >= NOW() - (:days || ' days')::interval
    GROUP BY source_type, signal_type
""")

_QUERY_BY_ID = text("""
    SELECT
        source_type AS group_key1,
        source_id   AS group_key2,
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


# signal_registry has no pre-computed outcome_return — we join to raw_series
# (close prices via the YF:<TICKER>:close series_id convention) to compute
# the N-day forward return per signal. BULLISH treated as BUY, BEARISH as SELL,
# NEUTRAL excluded (nothing to backtest).
_QUERY_REGISTRY_BY_MODULE = text("""
    WITH signals AS (
        SELECT
            sr.source_module,
            sr.ticker,
            sr.direction,
            sr.valid_from::date AS sig_date
        FROM signal_registry sr
        WHERE sr.ticker IS NOT NULL
          AND sr.direction IN ('bullish', 'bearish')
          AND sr.valid_from >= NOW() - (:days || ' days')::interval
    ),
    -- Price at signal date (close that day or prior trading day)
    p_at AS (
        SELECT s.source_module, s.ticker, s.direction, s.sig_date,
               (
                   SELECT value FROM raw_series
                   WHERE series_id = 'YF:' || s.ticker || ':close'
                     AND obs_date <= s.sig_date
                     AND pull_status = 'SUCCESS'
                   ORDER BY obs_date DESC LIMIT 1
               ) AS p_now,
               (
                   SELECT value FROM raw_series
                   WHERE series_id = 'YF:' || s.ticker || ':close'
                     AND obs_date >= s.sig_date + (:horizon || ' days')::interval
                     AND pull_status = 'SUCCESS'
                   ORDER BY obs_date ASC LIMIT 1
               ) AS p_fwd
        FROM signals s
    ),
    returns AS (
        SELECT
            source_module,
            -- direction acts like signal_type: bullish == BUY, bearish == SELL.
            CASE direction WHEN 'bullish' THEN 'BUY' ELSE 'SELL' END AS signal_type,
            CASE WHEN p_now > 0 THEN ((p_fwd / p_now) - 1.0) * 100.0 END AS return_pct
        FROM p_at
        WHERE p_now IS NOT NULL AND p_fwd IS NOT NULL AND p_now > 0
    )
    SELECT
        source_module AS group_key1,
        ''::text      AS group_key2,
        signal_type,
        COUNT(*) AS n,
        AVG(return_pct)::float  AS avg_return,
        STDDEV(return_pct)::float AS std_return,
        -- "hit rate" = directional success: BUY hits when return > 0, SELL hits when return < 0
        SUM(CASE WHEN signal_type = 'BUY'  AND return_pct > 0 THEN 1
                 WHEN signal_type = 'SELL' AND return_pct < 0 THEN 1
                 ELSE 0 END)::float / NULLIF(COUNT(*), 0) AS hit_rate
    FROM returns
    GROUP BY source_module, signal_type
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
        min_n: int = 30, by_id: bool = False, horizon: int = 5) -> dict[str, Any]:
    query = _QUERY_BY_ID if by_id else _QUERY_BY_TYPE
    with engine.connect() as conn:
        rows = list(conn.execute(query, {"days": days}).fetchall())
        # Also include signal_registry sources (alpha_research, news_intel,
        # feature:*, etc.) — these don't have pre-computed outcome_return,
        # so we join to raw_series for an N-day forward return.
        try:
            rows.extend(conn.execute(
                _QUERY_REGISTRY_BY_MODULE,
                {"days": days, "horizon": horizon},
            ).fetchall())
        except Exception as exc:  # noqa: BLE001
            log.warning("signal_registry replay failed: {e}", e=str(exc))

    # Group by (key1, key2) → {BUY: stats, SELL: stats}. With by_id=False,
    # key2 is empty so groups collapse to just source_type, giving us
    # enough samples for a verdict.
    groups: dict[tuple[str, str], dict[str, dict]] = {}
    for r in rows:
        key = (r[0] or "", r[1] or "")
        if source_filter and source_filter not in str(key[0]) and source_filter not in str(key[1]):
            continue
        groups.setdefault(key, {})[r[2]] = {
            "n": int(r[3]),
            "avg_return": float(r[4] or 0),
            "std_return": float(r[5] or 0),
            "hit_rate": float(r[6] or 0),
        }

    results = []
    for (key1, key2), by_dir in groups.items():
        buy = by_dir.get("BUY", {})
        sell = by_dir.get("SELL", {})
        n_buy, n_sell = buy.get("n", 0), sell.get("n", 0)
        if n_buy + n_sell < 5:
            continue
        avg_buy = buy.get("avg_return", 0.0)
        avg_sell = sell.get("avg_return", 0.0)
        lift = avg_buy - avg_sell
        results.append({
            "source_type": key1,
            "source_id": key2,
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
    p.add_argument("--by-id", action="store_true",
                   help="Group by source_type+source_id (per-person granularity); "
                        "default is by source_type only for sufficient sample size")
    p.add_argument("--horizon", type=int, default=5,
                   help="Forward-return horizon (days) for signal_registry replay; "
                        "default 5")
    args = p.parse_args(argv)

    from db import get_engine
    engine = get_engine()
    report = run(engine, days=args.days, source_filter=args.source,
                 min_n=args.min_n, by_id=args.by_id, horizon=args.horizon)

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
