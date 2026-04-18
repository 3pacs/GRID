#!/usr/bin/env python3
"""GRID — Corporate actions 8-K mining CLI.

Runs ``ingestion.altdata.corporate_actions_parser.CorporateActionsParser``
against a ticker universe and writes extracted events into
``capital_flows`` with ``period_type='announcement'``.

Usage:
    python3 scripts/run_corporate_actions.py                 # default universe
    python3 scripts/run_corporate_actions.py --limit 20      # first 20 tickers
    python3 scripts/run_corporate_actions.py --days-back 2500
    python3 scripts/run_corporate_actions.py --tickers MSFT,CSCO,AAPL
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

_GRID_DIR = str(Path(__file__).resolve().parent.parent)
os.chdir(_GRID_DIR)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

from loguru import logger as log


def main() -> None:
    parser = argparse.ArgumentParser(
        description="GRID corporate actions 8-K parser",
    )
    parser.add_argument(
        "--tickers",
        type=str,
        default=None,
        help="Comma-separated tickers. Defaults to DEFAULT_TICKERS in parser.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process the first N tickers from the universe.",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=1500,
        help="Days of 8-K history to scan (default 1500 ≈ 4 years).",
    )
    args = parser.parse_args()

    from db import get_engine
    from ingestion.altdata.corporate_actions_parser import (
        CorporateActionsParser,
        DEFAULT_TICKERS,
    )

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        tickers = list(DEFAULT_TICKERS)

    if args.limit is not None and args.limit > 0:
        tickers = tickers[: args.limit]

    log.info(
        "corporate_actions CLI starting: {n} tickers, {d} days back",
        n=len(tickers), d=args.days_back,
    )
    t0 = time.time()

    engine = get_engine()
    cap = CorporateActionsParser(engine)
    try:
        result = cap.pull(tickers=tickers, days_back=args.days_back)
    finally:
        cap.close()

    elapsed = time.time() - t0
    log.info("=" * 60)
    log.info(
        "corporate_actions DONE in {t:.1f}s — {r} rows, {f} filings scanned",
        t=elapsed,
        r=result.get("rows_inserted", 0),
        f=result.get("filings_scanned", 0),
    )
    by_type = result.get("by_flow_type", {}) or {}
    for ft, n in sorted(by_type.items(), key=lambda kv: -kv[1]):
        log.info("  {ft:<20} {n}", ft=ft, n=n)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
