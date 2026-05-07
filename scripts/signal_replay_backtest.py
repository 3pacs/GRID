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
# to compute forward returns at MULTIPLE horizons in a single sweep.
# BULLISH = BUY, BEARISH = SELL, NEUTRAL excluded.
#
# 2026-04-29: multi-horizon. Different signals have alpha at different
# timescales. Credit cycle is noise at 5d but may be informative at 90d-180d.
# This query returns one row per (source_module, signal_type, horizon_days)
# so the caller can produce a horizon-vs-lift heatmap per signal.
_HORIZONS_DAYS = (1, 5, 30, 90, 180)

_QUERY_REGISTRY_MULTI_HORIZON = text("""
    -- Multi-horizon replay via UNION ALL (avoids CROSS JOIN LATERAL VALUES
    -- which is fragile across PG versions).
    WITH signals AS (
        SELECT
            sr.source_module,
            sr.ticker,
            CASE sr.direction WHEN 'bullish' THEN 'BUY' ELSE 'SELL' END AS signal_type,
            sr.valid_from::date AS sig_date
        FROM signal_registry sr
        WHERE sr.ticker IS NOT NULL
          AND sr.direction IN ('bullish', 'bearish')
          AND sr.valid_from >= NOW() - ((:days)::text || ' days')::interval
    ),
    -- 2026-04-29: dropped pull_status='SUCCESS' filter — the value column is
    -- the source of truth and NULL handling downstream skips bad rows.
    -- Date arithmetic uses INTERVAL for clarity (DATE + INT works in PG but
    -- can confuse the planner).
    p_at AS (
        SELECT s.source_module, s.ticker, s.signal_type, s.sig_date,
               (SELECT value FROM raw_series
                  WHERE series_id = 'YF:' || s.ticker || ':close'
                    AND obs_date <= s.sig_date
                  ORDER BY obs_date DESC LIMIT 1) AS p_now,
               (SELECT value FROM raw_series
                  WHERE series_id = 'YF:' || s.ticker || ':close'
                    AND obs_date >= s.sig_date + INTERVAL '1 day'
                  ORDER BY obs_date ASC LIMIT 1) AS p_1d,
               (SELECT value FROM raw_series
                  WHERE series_id = 'YF:' || s.ticker || ':close'
                    AND obs_date >= s.sig_date + INTERVAL '5 days'
                  ORDER BY obs_date ASC LIMIT 1) AS p_5d,
               (SELECT value FROM raw_series
                  WHERE series_id = 'YF:' || s.ticker || ':close'
                    AND obs_date >= s.sig_date + INTERVAL '30 days'
                  ORDER BY obs_date ASC LIMIT 1) AS p_30d,
               (SELECT value FROM raw_series
                  WHERE series_id = 'YF:' || s.ticker || ':close'
                    AND obs_date >= s.sig_date + INTERVAL '90 days'
                  ORDER BY obs_date ASC LIMIT 1) AS p_90d,
               (SELECT value FROM raw_series
                  WHERE series_id = 'YF:' || s.ticker || ':close'
                    AND obs_date >= s.sig_date + INTERVAL '180 days'
                  ORDER BY obs_date ASC LIMIT 1) AS p_180d
        FROM signals s
    ),
    -- One row per (signal, horizon) instead of LATERAL VALUES — friendlier to PG.
    expanded AS (
        SELECT source_module, signal_type, 1 AS h, ((p_1d   / p_now) - 1.0) * 100.0 AS rp FROM p_at WHERE p_now > 0 AND p_1d   IS NOT NULL
        UNION ALL
        SELECT source_module, signal_type, 5,    ((p_5d   / p_now) - 1.0) * 100.0     FROM p_at WHERE p_now > 0 AND p_5d   IS NOT NULL
        UNION ALL
        SELECT source_module, signal_type, 30,   ((p_30d  / p_now) - 1.0) * 100.0     FROM p_at WHERE p_now > 0 AND p_30d  IS NOT NULL
        UNION ALL
        SELECT source_module, signal_type, 90,   ((p_90d  / p_now) - 1.0) * 100.0     FROM p_at WHERE p_now > 0 AND p_90d  IS NOT NULL
        UNION ALL
        SELECT source_module, signal_type, 180,  ((p_180d / p_now) - 1.0) * 100.0     FROM p_at WHERE p_now > 0 AND p_180d IS NOT NULL
    )
    SELECT
        source_module AS group_key1,
        h::text       AS group_key2,
        signal_type,
        COUNT(*) AS n,
        AVG(rp)::float  AS avg_return,
        STDDEV(rp)::float AS std_return,
        SUM(CASE WHEN signal_type = 'BUY'  AND rp > 0 THEN 1
                 WHEN signal_type = 'SELL' AND rp < 0 THEN 1
                 ELSE 0 END)::float / NULLIF(COUNT(*), 0) AS hit_rate
    FROM expanded
    GROUP BY source_module, h, signal_type
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
    """`horizon` is retained for backwards-compat but the registry sweep now
    evaluates 1d/5d/30d/90d/180d in a single pass."""
    query = _QUERY_BY_ID if by_id else _QUERY_BY_TYPE
    with engine.connect() as conn:
        rows = list(conn.execute(query, {"days": days}).fetchall())
        # signal_sources rows have group_key2='' (single horizon, baked into
        # outcome_return). Mark them with the synthetic horizon "outcome" so
        # the report can keep them separate from the multi-horizon registry data.
        # group_key2 is unused for source_type rows; reuse it as horizon label.
        rows = [tuple(r[:1]) + ("outcome",) + tuple(r[2:]) for r in rows]

        # signal_registry: 1d/5d/30d/90d/180d in one query.
        # Loud on failure — silent swallowing was hiding a SQL bug.
        try:
            registry_rows = conn.execute(
                _QUERY_REGISTRY_MULTI_HORIZON, {"days": days},
            ).fetchall()
            rows.extend(registry_rows)
            log.info("signal_registry replay: {n} (source × horizon × dir) cells",
                     n=len(registry_rows))
        except Exception as exc:  # noqa: BLE001
            import traceback
            log.error("signal_registry replay FAILED: {e}\n{tb}",
                      e=str(exc), tb=traceback.format_exc())

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
        # key2 carries the horizon label ("outcome" for signal_sources or
        # "1"/"5"/"30"/"90"/"180" for signal_registry).
        results.append({
            "source": key1,
            "horizon": key2 or "outcome",
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

    print(f"=== Signal Replay Backtest ({report['lookback_days']}d lookback, "
          f"{report['n_sources']} (source × horizon) cells analysed) ===\n")

    # Horizon heatmap per source: shows lift at each timescale so you can
    # see "this signal is noise at 5d but alpha at 90d" cases.
    by_source: dict[str, dict[str, dict[str, Any]]] = {}
    for s in report["sources"]:
        by_source.setdefault(s["source"], {})[s["horizon"]] = s

    horizon_order = ["1", "5", "30", "90", "180", "outcome"]
    print("=== Horizon × Lift heatmap (lift = avg_return_buy − avg_return_sell) ===")
    print(f"{'source':<40} " + " ".join(f"{h:>10}" for h in horizon_order))
    print("-" * (40 + 11 * len(horizon_order)))
    # Sort sources by max-abs-lift across horizons so the most-actionable ones rise.
    def _max_lift(src_name: str) -> float:
        return max((abs(c.get("lift_pct", 0.0)) for c in by_source[src_name].values()), default=0.0)
    for src_name in sorted(by_source, key=_max_lift, reverse=True)[:args.limit]:
        cells = []
        for h in horizon_order:
            c = by_source[src_name].get(h)
            if c is None:
                cells.append(f"{'—':>10}")
            else:
                lift = c["lift_pct"]
                n = c["n_buy"] + c["n_sell"]
                marker = ""
                if c["verdict"].startswith("INFORMATIVE"):
                    marker = "✓"
                elif c["verdict"].startswith("INVERTED"):
                    marker = "↯"
                elif c["verdict"].startswith("NOISE"):
                    marker = "·"
                cells.append(f"{lift:>+7.2f}%{marker}({n})")
        print(f"{src_name[:40]:<40} " + " ".join(f"{c:>10}" for c in cells))

    # Detailed top-N rows
    print(f"\n=== Top {min(args.limit, len(report['sources']))} (source, horizon) rows by |lift| ===")
    print(f"{'source':<32} {'horizon':>8} {'n_buy':>6} {'n_sell':>7} "
          f"{'avg_buy':>9} {'avg_sell':>10} {'lift':>9} verdict")
    print("-" * 140)
    for s in report["sources"][:args.limit]:
        print(f"{s['source'][:32]:<32} {str(s['horizon'])[:8]:>8} "
              f"{s['n_buy']:>6} {s['n_sell']:>7} "
              f"{s['avg_return_buy']:>+8.2f}% {s['avg_return_sell']:>+9.2f}% "
              f"{s['lift_pct']:>+8.2f}% {s['verdict']}")

    # Summary buckets
    buckets = {"INFORMATIVE": 0, "INVERTED": 0, "NOISE": 0, "INSUFFICIENT": 0}
    for s in report["sources"]:
        for k in buckets:
            if s["verdict"].startswith(k):
                buckets[k] += 1
                break
    print(f"\n=== Summary (across {report['n_sources']} cells) ===")
    for k, v in buckets.items():
        print(f"  {k:<14} {v:>4} cells")
    print("\n✓ INFORMATIVE — keep direction at this horizon")
    print("↯ INVERTED   — flip BUY↔SELL at this horizon")
    print("· NOISE      — publish NEUTRAL or skip at this horizon")
    print("\nA signal can be NOISE at 5d and INFORMATIVE at 90d — that's the heatmap.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
