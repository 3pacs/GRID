#!/usr/bin/env python3
"""Run one Oracle cycle: score → evolve → predict → report."""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.chdir(os.path.join(os.path.dirname(__file__), ".."))

from loguru import logger as log

from db import get_engine
from oracle.engine import OracleEngine
from oracle.report import send_oracle_report


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Run Oracle prediction cycle")
    parser.add_argument("--no-email", action="store_true", help="Skip sending email")
    parser.add_argument("--tickers", type=str, default="", help="Comma-separated tickers (empty=all)")
    args = parser.parse_args()

    engine = get_engine()
    oracle = OracleEngine(db_engine=engine)

    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()] if args.tickers else None

    result = oracle.run_cycle(tickers=tickers)

    print(json.dumps({
        "new_predictions": result["new_predictions"],
        "scoring": result["scoring"],
        "evolution": result["evolution"],
        "leaderboard": result["leaderboard"],
    }, indent=2, default=str))

    if not args.no_email and result["new_predictions"] > 0:
        send_oracle_report(result)
    elif result["new_predictions"] == 0:
        log.info("No predictions generated — skipping email")


if __name__ == "__main__":
    main()
