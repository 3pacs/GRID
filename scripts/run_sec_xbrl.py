#!/usr/bin/env python3
"""Runner for the SEC XBRL Company Facts ingestor.

Usage:
    python scripts/run_sec_xbrl.py                        # full run, resume from checkpoint
    python scripts/run_sec_xbrl.py --limit 20             # cap at 20 tickers (testing)
    python scripts/run_sec_xbrl.py --no-resume            # ignore checkpoint, start fresh
    python scripts/run_sec_xbrl.py --tickers TSM,ASML,BP  # restrict to a list
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
from ingestion.altdata.sec_xbrl_financials import (  # noqa: E402
    SECXBRLFinancialsPuller,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run SEC XBRL Company Facts capital_flows ingestion.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum tickers to process (None = all).",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Ignore checkpoint and start from the top of SECTOR_MAP.",
    )
    parser.add_argument(
        "--tickers",
        type=str,
        default=None,
        help=(
            "Comma-separated ticker list to restrict the run to "
            "(e.g. 'TSM,ASML,BP'). Useful for re-running just foreign "
            "IFRS issuers after a taxonomy change."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    engine = get_engine()
    puller = SECXBRLFinancialsPuller(db_engine=engine)

    tickers_list: list[str] | None = None
    if args.tickers:
        tickers_list = [t.strip() for t in args.tickers.split(",") if t.strip()]

    results = puller.pull_all(
        limit=args.limit,
        resume=not args.no_resume,
        tickers=tickers_list,
    )

    success = sum(1 for r in results if r.get("status") == "SUCCESS")
    rows = sum(int(r.get("rows") or 0) for r in results)
    log.info(
        "run_sec_xbrl done — {s} tickers SUCCESS, {r} rows written",
        s=success, r=rows,
    )
    # Print the summary row for easy shell-side scraping.
    for r in results:
        if r.get("status") == "SUMMARY":
            print(r)
            break
    return 0


if __name__ == "__main__":
    sys.exit(main())
