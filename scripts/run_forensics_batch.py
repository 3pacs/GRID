#!/usr/bin/env python3
"""Run batch forensics for SPY, BTC, ETH, QQQ, AAPL with threshold=0.015."""

import sys
sys.path.insert(0, "/data/grid_v4/grid_repo")

from sqlalchemy import create_engine
from intelligence.forensics import batch_forensics
from loguru import logger as log

DB_URL = "postgresql://grid:gridmaster2026@localhost:5432/griddb"
TICKERS = ["SPY", "BTC", "ETH", "QQQ", "AAPL"]
THRESHOLD = 0.015

def main() -> None:
    engine = create_engine(DB_URL)
    log.info("Running batch_forensics with threshold={} (1.5%)\n", THRESHOLD)

    total_all = 0
    for ticker in TICKERS:
        reports = batch_forensics(engine, ticker, days=90, threshold=THRESHOLD)
        count = len(reports)
        total_all += count
        log.info("  {:5s}: {} reports generated", ticker, count)
        for r in reports:
            log.info("         {}  {:4s} {:+.2f}%  confidence={:.3f}  signals={}",
                     r.move_date, r.move_direction, r.move_pct, r.confidence, r.warning_signals)

    log.info("\nTotal reports generated: {}", total_all)

if __name__ == "__main__":
    main()
