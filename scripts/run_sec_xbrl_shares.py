#!/usr/bin/env python3
"""Runner for the SEC XBRL shares-outstanding / market-cap ingestor.

Usage:
    python scripts/run_sec_xbrl_shares.py                      # default 90d, all tickers
    python scripts/run_sec_xbrl_shares.py --backfill-days 30   # 30-day window
    python scripts/run_sec_xbrl_shares.py --limit 200          # cap at 200 tickers
    python scripts/run_sec_xbrl_shares.py --tickers AAPL,MSFT  # only specific tickers
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
from ingestion.altdata.sec_xbrl_shares import (  # noqa: E402
    FOREIGN_ISSUER_TICKERS,
    SECXBRLSharesPuller,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run SEC XBRL shares ingestion → ticker_metrics_daily.",
    )
    parser.add_argument(
        "--backfill-days",
        type=int,
        default=90,
        help="Number of days back from today to compute daily rows.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum tickers to process this run (None = all).",
    )
    parser.add_argument(
        "--tickers",
        type=str,
        default=None,
        help="Comma-separated explicit ticker list (overrides sector map).",
    )
    parser.add_argument(
        "--foreign-issuers",
        action="store_true",
        help=(
            "Include the known 20-F/6-K foreign-issuer tickers (TSM, ASML, "
            "BHP, RIO, NVO, AZN, BP, ...) which report shares under IFRS and "
            "are usually absent from the domestic sector map. Merged with "
            "--tickers when both are given."
        ),
    )
    return parser.parse_args()


def _split_tickers(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    return [tk.strip().upper() for tk in raw.split(",") if tk.strip()]


def _resolve_universe(
    raw_tickers: str | None, include_foreign: bool
) -> list[str] | None:
    """Build the explicit ticker list from --tickers and --foreign-issuers.

    Returns None when neither is supplied (puller falls back to sector map).
    Deduplicates while preserving order (explicit tickers first).
    """
    explicit = _split_tickers(raw_tickers) or []
    if include_foreign:
        explicit = list(explicit) + list(FOREIGN_ISSUER_TICKERS)
    if not explicit:
        return None
    seen: set[str] = set()
    ordered: list[str] = []
    for tk in explicit:
        if tk not in seen:
            seen.add(tk)
            ordered.append(tk)
    return ordered


def main() -> int:
    args = _parse_args()
    engine = get_engine()
    puller = SECXBRLSharesPuller(db_engine=engine)
    tickers = _resolve_universe(args.tickers, args.foreign_issuers)
    results = puller.pull_all(
        limit=args.limit,
        backfill_days=args.backfill_days,
        tickers=tickers,
    )

    success = sum(1 for r in results if r.get("status") == "SUCCESS")
    rows = sum(int(r.get("rows") or 0) for r in results)
    log.info(
        "run_sec_xbrl_shares done — {s} tickers SUCCESS, {r} rows written",
        s=success, r=rows,
    )
    for r in results:
        if r.get("status") == "SUMMARY":
            print(r)
            break
    return 0


if __name__ == "__main__":
    sys.exit(main())
