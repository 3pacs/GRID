#!/usr/bin/env python3
"""Runner for the agricultural + industrial commodity futures ingestor.

Usage:
    python scripts/run_ag_commodity_futures.py                 # incremental pull
    python scripts/run_ag_commodity_futures.py --backfill 1825  # 5 year backfill
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Ensure grid/ root is on sys.path so ``ingestion.*`` imports resolve.
_GRID_DIR = str(Path(__file__).resolve().parent.parent)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)
os.chdir(_GRID_DIR)

from loguru import logger as log  # noqa: E402

from db import get_engine  # noqa: E402
from ingestion.altdata.ag_commodity_futures import (  # noqa: E402
    AgCommodityFuturesPuller,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run agricultural + industrial commodity futures ingestion.",
    )
    parser.add_argument(
        "--backfill",
        type=int,
        default=None,
        help=(
            "Number of days to backfill. Default: 5 years (1825) on first run, "
            "10 days on subsequent runs."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    engine = get_engine()
    puller = AgCommodityFuturesPuller(db_engine=engine)
    results = puller.pull_all(backfill_days=args.backfill)

    success = sum(1 for r in results if r.get("status") == "SUCCESS")
    rows = sum(int(r.get("rows_inserted") or 0) for r in results)
    log.info(
        "run_ag_commodity_futures done — {s} tickers SUCCESS, {r} rows written",
        s=success,
        r=rows,
    )
    for r in results:
        print(
            f"  {r.get('feature', '?')}: {r['status']} "
            f"({r.get('rows_inserted', 0)} inserted, "
            f"{r.get('rows_skipped', 0)} skipped)"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
