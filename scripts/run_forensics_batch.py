#!/usr/bin/env python3
"""Run batch forensics for SPY, BTC, ETH, QQQ, AAPL with threshold=0.015."""

import sys
sys.path.insert(0, "/data/grid_v4/grid_repo")

from sqlalchemy import create_engine
from intelligence.forensics import batch_forensics

DB_URL = "postgresql://grid:gridmaster2026@localhost:5432/griddb"
TICKERS = ["SPY", "BTC", "ETH", "QQQ", "AAPL"]
THRESHOLD = 0.015

def main() -> None:
    engine = create_engine(DB_URL)
    print(f"Running batch_forensics with threshold={THRESHOLD} (1.5%)\n")

    total_all = 0
    for ticker in TICKERS:
        reports = batch_forensics(engine, ticker, days=90, threshold=THRESHOLD)
        count = len(reports)
        total_all += count
        print(f"  {ticker:5s}: {count} reports generated")
        for r in reports:
            print(f"         {r.move_date}  {r.move_direction:4s} {r.move_pct:+.2f}%  "
                  f"confidence={r.confidence:.3f}  signals={r.warning_signals}")

    print(f"\nTotal reports generated: {total_all}")

if __name__ == "__main__":
    main()
