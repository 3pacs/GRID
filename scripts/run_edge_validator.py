#!/usr/bin/env python3
"""Run the supply-chain edge validator and print a summary.

Usage::

    python3 scripts/run_edge_validator.py --limit 500
    python3 scripts/run_edge_validator.py            # all edges

Writes ``validation_correlation``, ``last_validation_at``, ``weak_since``,
and ``relationship_weak`` columns on ``supply_chain_edges``. Safe to run
repeatedly — the underlying state machine is idempotent (see
``intelligence/supply_chain_edge_validator.py``).
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from loguru import logger as log
from sqlalchemy import text

# Ensure the repo root is on sys.path when invoked from either
# ``/data/grid_v4/grid_repo`` (server) or local dev.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from db import get_engine  # noqa: E402
from intelligence.supply_chain_edge_validator import (  # noqa: E402
    DEFAULT_LOOKBACK_DAYS,
    summarise_results,
    validate_all_edges,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run supply-chain edge validator")
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of edges to validate (default: all)",
    )
    p.add_argument(
        "--lookback",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help=f"Lookback window in days (default: {DEFAULT_LOOKBACK_DAYS})",
    )
    return p.parse_args()


def _print_distribution(engine) -> None:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT
                    COUNT(*)                                                AS total,
                    COUNT(*) FILTER (WHERE validation_correlation IS NOT NULL) AS validated,
                    COUNT(*) FILTER (WHERE relationship_weak)               AS weak,
                    COUNT(*) FILTER (WHERE weak_since IS NOT NULL)          AS clock_running,
                    COUNT(*) FILTER (WHERE ABS(validation_correlation) >= 0.7)            AS strong,
                    COUNT(*) FILTER (WHERE ABS(validation_correlation) >= 0.4
                                       AND ABS(validation_correlation) <  0.7)            AS medium,
                    COUNT(*) FILTER (WHERE ABS(validation_correlation) >= 0.1
                                       AND ABS(validation_correlation) <  0.4)            AS light,
                    COUNT(*) FILTER (WHERE validation_correlation IS NOT NULL
                                       AND ABS(validation_correlation) <  0.1)            AS noise
                FROM supply_chain_edges
                """
            )
        ).fetchone()
    if not row:
        return
    print("\nDatabase-wide edge validation state:")
    print(f"  total edges              {row[0]}")
    print(f"  validated                {row[1]}")
    print(f"  flagged weak             {row[2]}")
    print(f"  weak clock running (pending) {row[3]}")
    print("\nCorrelation distribution (|corr|):")
    print(f"  >= 0.70  strong     {row[4]}")
    print(f"  >= 0.40  medium     {row[5]}")
    print(f"  >= 0.10  light      {row[6]}")
    print(f"   < 0.10  noise      {row[7]}")


def main() -> int:
    args = parse_args()
    engine = get_engine()
    log.info(
        "edge_validator run — lookback={l}d limit={lim}",
        l=args.lookback,
        lim=args.limit,
    )
    results = validate_all_edges(
        engine=engine,
        limit=args.limit,
        lookback_days=args.lookback,
    )
    summary = summarise_results(results)
    print("\nValidation summary:")
    print(json.dumps(summary, indent=2, default=str))
    _print_distribution(engine)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
