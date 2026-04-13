#!/usr/bin/env python3
"""CLI runner for the LLM-driven pct_downstream_cogs enrichment.

Usage::

    python scripts/run_pct_cogs_enrichment.py --limit 30
    python scripts/run_pct_cogs_enrichment.py --tickers AAPL,WMT,HSY
    python scripts/run_pct_cogs_enrichment.py --limit 300

The default ticker set is the top-30 US mega-caps that anchor most supply
chain edges. Pass ``--tickers`` to override.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Make grid root importable when run as a script
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from db import get_engine  # noqa: E402
from intelligence.pct_cogs_enrichment import (  # noqa: E402
    LLMUnavailableError,
    PctCogsEnricher,
)


DEFAULT_TICKERS: list[str] = [
    "WMT", "AAPL", "MSFT", "GOOGL", "META", "AMZN", "TSLA", "NVDA",
    "JPM",  "V",    "MA",   "XOM",   "CVX",  "JNJ",  "PG",   "KO",
    "PEP",  "HD",   "BAC",  "LLY",   "UNH",  "MRK",  "PFE",  "ABBV",
    "ADBE", "CRM",  "CSCO", "ACN",   "INTC",
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="LLM-extract pct_downstream_cogs values from 10-K text",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum candidate edges to process this run (default: no cap)",
    )
    parser.add_argument(
        "--tickers",
        type=str,
        default=None,
        help=(
            "Comma-separated downstream tickers to scope to "
            "(default: top-30 US mega-caps)"
        ),
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Ignore the default ticker scope and run on every missing edge",
    )
    parser.add_argument(
        "--mode",
        choices=("per_edge", "harvest", "both"),
        default="both",
        help=(
            "Which pass(es) to run. 'harvest' scans full 10-Ks for every "
            "counterparty disclosure and matches back to edges (high yield); "
            "'per_edge' only reasons about a single edge at a time; "
            "'both' (default) runs harvest first then per_edge cleanup."
        ),
    )
    args = parser.parse_args()

    if args.all:
        tickers: list[str] | None = None
    elif args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        tickers = DEFAULT_TICKERS

    engine = get_engine()
    enricher = PctCogsEnricher(engine=engine)
    try:
        summary = enricher.run(
            tickers=tickers, limit=args.limit, mode=args.mode
        )
    except LLMUnavailableError as exc:
        print(json.dumps({"status": "blocked", "error": str(exc)}))
        return 2

    payload = {
        "status": "ok",
        "mode": args.mode,
        "tickers": tickers,
        "limit": args.limit,
        "summary": summary.as_dict(),
    }
    print(json.dumps(payload, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
