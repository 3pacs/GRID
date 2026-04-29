#!/usr/bin/env python3
"""Runner for the contagion -> supply-chain edge feedback loop.

Walks every ``contagion_backtest_results`` row scored in the last
``--since-hours`` window and applies decay or validation to the
implicated ``supply_chain_edges.pct_downstream_cogs`` values.

Usage:
    python scripts/run_postmortem_feedback.py                  # write mode, 24h
    python scripts/run_postmortem_feedback.py --dry-run        # preview only
    python scripts/run_postmortem_feedback.py --dry-run --since-hours 720
    python scripts/run_postmortem_feedback.py --verbose
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# Ensure repo root is importable.
_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
os.chdir(_REPO_ROOT)

from loguru import logger as log  # noqa: E402

from db import get_engine  # noqa: E402
from intelligence.postmortem import apply_contagion_feedback  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Apply contagion backtest feedback to supply_chain_edges."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute adjustments without writing to the database.",
    )
    parser.add_argument(
        "--since-hours",
        type=int,
        default=24,
        help="Only consider backtest rows scored in the last N hours.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    if args.verbose:
        log.remove()
        log.add(sys.stderr, level="DEBUG")

    if args.since_hours <= 0:
        log.error("--since-hours must be > 0")
        return 2

    engine = get_engine()
    summary = apply_contagion_feedback(
        engine,
        since_hours=args.since_hours,
        dry_run=args.dry_run,
    )

    print("postmortem_feedback summary:")
    print(json.dumps(summary, indent=2, default=str))

    if summary.get("errors", 0) > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
