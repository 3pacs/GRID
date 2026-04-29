#!/usr/bin/env python3
"""Runner for the 10-K supply chain parser.

Usage::

    python scripts/run_supply_chain_parser.py --limit 20
    python scripts/run_supply_chain_parser.py --limit 200 --budget 3600
    python scripts/run_supply_chain_parser.py --tickers AAPL,HSY,MSFT --reset

Writes derived edges to supply_chain_edges (confidence='derived') and new
private-company nodes to supply_chain_nodes (ON CONFLICT DO NOTHING — the
hand-curated seed is never overwritten).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Make the grid root importable when run as a script
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from db import get_engine  # noqa: E402
from ingestion.altdata.supply_chain_parser import (  # noqa: E402
    SupplyChain10KParser,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Parse 10-K filings for supplier/customer relationships"
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
    runner = SupplyChain10KParser(db_engine=engine)
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
