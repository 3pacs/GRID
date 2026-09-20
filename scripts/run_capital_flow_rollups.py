#!/usr/bin/env python3
"""Runner for the capital_flows rollup derivations.

Builds two derived views off the base ``capital_flows`` table:

1. ``period_type='ttm'`` rows from trailing four quarters
2. ``period_type='annual'`` rows tagged ``announcement_rolled`` from
   ``period_type='announcement'`` events

Both are idempotent — re-running refreshes existing rows in place.

Usage:
    python scripts/run_capital_flow_rollups.py
    python scripts/run_capital_flow_rollups.py --ttm-only
    python scripts/run_capital_flow_rollups.py --rollup-only
    python scripts/run_capital_flow_rollups.py --verbose
"""

from __future__ import annotations

import argparse
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
from intelligence.company_financial_rollups import (  # noqa: E402
    compute_ttm,
    fold_announcements,
    run_all,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build TTM + announcement-folded capital_flows rows.",
    )
    parser.add_argument(
        "--ttm-only",
        action="store_true",
        help="Only compute TTM rows.",
    )
    parser.add_argument(
        "--rollup-only",
        action="store_true",
        help="Only fold announcements into annual rows.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    parser.add_argument(
        "--watermark",
        default=None,
        help=(
            "Vestigial (fable-daily-intel-sql-tasks, 2026-09-20 SECOND "
            "follow-up): compute_ttm's dirty-actor recompute set is now "
            "decided entirely by a durable per-actor content fingerprint "
            "in capital_flows_ttm_state, not by any as_of cursor. This "
            "flag is accepted and passed through for backward-compatible "
            "call signatures only and has NO effect on which actors are "
            "recomputed — see intelligence/company_financial_rollups.py's "
            "module docstring."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.verbose:
        log.remove()
        log.add(sys.stderr, level="DEBUG")

    engine = get_engine()

    if args.ttm_only and args.rollup_only:
        log.error("--ttm-only and --rollup-only are mutually exclusive")
        return 2

    if args.ttm_only:
        result = compute_ttm(engine, args.watermark)
        print(
            f"capital_flow_rollups: ttm rows={result.rows_written} "
            f"watermark={args.watermark!r} -> {result.watermark!r}"
        )
        return 0

    if args.rollup_only:
        n = fold_announcements(engine)
        print(f"capital_flow_rollups: rolled rows={n}")
        return 0

    stats = run_all(engine, ttm_watermark=args.watermark)
    print("capital_flow_rollups summary:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    if not stats.get("ok", False):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
