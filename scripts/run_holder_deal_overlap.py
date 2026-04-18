#!/usr/bin/env python3
"""Runner for the holder_deal_overlap detector.

Scans ``capital_flows`` announcement rows for acquisitions and
cross-references ``institutional_holdings`` 13F snapshots to find
filers that held both the acquirer and the target before the deal
was announced. See ``intelligence/holder_deal_overlap.py`` for logic.

Usage:
    python scripts/run_holder_deal_overlap.py
    python scripts/run_holder_deal_overlap.py --min-position-usd 1000000
    python scripts/run_holder_deal_overlap.py --verbose
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
from intelligence.holder_deal_overlap import MIN_POSITION_USD, run  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scan capital_flows acquisitions + institutional_holdings "
            "for pre-positioning cross-overlaps."
        ),
    )
    parser.add_argument(
        "--min-position-usd",
        type=float,
        default=MIN_POSITION_USD,
        help=(
            "Material-position floor (USD). Filer must hold at least "
            "this much on the weaker leg for the pre_position_flag to "
            "be set. Default: %(default)s"
        ),
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

    engine = get_engine()
    stats = run(engine, min_position_usd=args.min_position_usd)

    print("holder_deal_overlap summary:")
    print(f"  deals_scanned:    {stats.get('deals_scanned', 0)}")
    print(f"  overlaps_written: {stats.get('overlaps_written', 0)}")
    print(f"  pre_positioned:   {stats.get('pre_positioned', 0)}")
    print(f"  quick_exits:      {stats.get('quick_exits', 0)}")
    if stats.get("errors"):
        print(f"  errors: {len(stats['errors'])}")
        for e in stats["errors"][:5]:
            print(f"    - {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
