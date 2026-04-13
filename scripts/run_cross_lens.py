#!/usr/bin/env python3
"""Run the cross-lens supply-shock attribution detector and print a summary.

Usage::

    python3 scripts/run_cross_lens.py --lookback 180 --min-corr 0.5

Writes rows to ``supply_shock_attributions`` via ON CONFLICT upsert, so
reruns are idempotent.
"""

from __future__ import annotations

import argparse
import sys

from loguru import logger as log
from sqlalchemy import text

# Ensure the repo root is on sys.path when invoked from either
# ``/data/grid_v4/grid_repo`` (server) or local dev.
import os
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from db import get_engine  # noqa: E402
from intelligence.cross_lens import (  # noqa: E402
    DEFAULT_LAG_WINDOW,
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_MIN_CORRELATION,
    detect_attributions,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run cross-lens supply-shock detector")
    p.add_argument("--lookback", type=int, default=DEFAULT_LOOKBACK_DAYS)
    p.add_argument("--min-corr", type=float, default=DEFAULT_MIN_CORRELATION)
    p.add_argument(
        "--min-lag", type=int, default=DEFAULT_LAG_WINDOW[0],
        help="Minimum lag (days) to test for downstream response",
    )
    p.add_argument(
        "--max-lag", type=int, default=DEFAULT_LAG_WINDOW[1],
        help="Maximum lag (days) to test for downstream response",
    )
    return p.parse_args()


def summarise(engine, written: int) -> None:
    """Print per-method counts and the top-10 strongest rows from the DB."""
    print(f"\nAttribution rows written or updated: {written}")
    with engine.connect() as conn:
        method_rows = conn.execute(
            text(
                "SELECT method, COUNT(*) FROM supply_shock_attributions "
                "GROUP BY method ORDER BY method"
            )
        ).fetchall()
        print("\nCounts by method:")
        for method, cnt in method_rows:
            print(f"  {method:20s} {cnt}")

        top = conn.execute(
            text(
                """
                SELECT upstream_id, downstream_id, correlation, lag_days,
                       shock_magnitude, method
                FROM supply_shock_attributions
                ORDER BY ABS(COALESCE(correlation, 0)) DESC,
                         ABS(COALESCE(shock_magnitude, 0)) DESC
                LIMIT 10
                """
            )
        ).fetchall()
        print("\nTop 10 strongest attributions:")
        for r in top:
            corr = r[2] if r[2] is not None else 0.0
            lag = r[3] if r[3] is not None else "-"
            mag = f"{r[4]:+.2%}" if r[4] is not None else "-"
            print(
                f"  {r[0]:<18s} -> {r[1]:<10s}  "
                f"corr={corr:+.3f}  lag={lag}  shock={mag}  [{r[5]}]"
            )


def main() -> int:
    args = parse_args()
    engine = get_engine()
    log.info(
        "cross_lens run — lookback={l}d min_corr={c} lag_window=({a}, {b})",
        l=args.lookback,
        c=args.min_corr,
        a=args.min_lag,
        b=args.max_lag,
    )
    rows = detect_attributions(
        engine=engine,
        lookback_days=args.lookback,
        min_correlation=args.min_corr,
        lag_window=(args.min_lag, args.max_lag),
    )
    summarise(engine, len(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
