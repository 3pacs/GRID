#!/usr/bin/env python3
"""Runner for the SEC Item 1C cybersecurity puller.

Usage::

    python scripts/run_sec_item_1c.py --limit 50
    python scripts/run_sec_item_1c.py --limit 500 --budget 3600
    python scripts/run_sec_item_1c.py --tickers AAPL,MSFT,ORCL --reset

Extracts the Item 1C (Cybersecurity) section of the most recent 10-K
for every ticker in the GRID universe, finds named third-party
software / platform providers, and writes a
``component`` edge from provider → issuer in ``supply_chain_edges``
with ``confidence='derived'`` and source ``10-K Item 1C <TICKER>``.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from db import get_engine  # noqa: E402
from ingestion.altdata.sec_item_1c_cyber import (  # noqa: E402
    SECItem1CCyberPuller,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract Item 1C cybersecurity dependencies from 10-Ks"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max tickers to process this run (default: no limit)",
    )
    parser.add_argument(
        "--budget", type=int, default=3600,
        help="Wall-clock budget in seconds (default: 3600)",
    )
    parser.add_argument(
        "--tickers", type=str, default=None,
        help="Comma-separated ticker list (overrides sector_map universe)",
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="Delete the checkpoint file before starting",
    )
    args = parser.parse_args()

    tickers = None
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    engine = get_engine()
    runner = SECItem1CCyberPuller(db_engine=engine)
    summary = runner.run(
        limit=args.limit,
        tickers=tickers,
        budget_seconds=args.budget,
        reset_checkpoint=args.reset,
    )
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
