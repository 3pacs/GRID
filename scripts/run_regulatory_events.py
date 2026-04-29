#!/usr/bin/env python3
"""Runner for the regulatory events puller.

Usage::

    python scripts/run_regulatory_events.py --limit 50
    python scripts/run_regulatory_events.py --limit 25 --sources fda,ftc,sec
    python scripts/run_regulatory_events.py --sources doj

Polls FDA / FTC / SEC / DOJ / USDA / CFPB / EPA enforcement feeds,
writes to ``regulatory_events`` and emits
``relationship='regulatory_threat'`` edges into ``supply_chain_edges``.
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
from ingestion.altdata.regulatory_events import (  # noqa: E402
    RegulatoryEventsPuller,
    SOURCES,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Pull regulatory enforcement actions and emit "
                    "regulatory_threat edges."
    )
    parser.add_argument(
        "--limit", type=int, default=50,
        help="Max events to parse per source (default: 50)",
    )
    parser.add_argument(
        "--sources", type=str, default=None,
        help="Comma-separated source slugs. Default: all. "
             "Valid: " + ",".join(sorted(SOURCES.keys())),
    )
    args = parser.parse_args()

    selected: list[str] | None = None
    if args.sources:
        selected = [s.strip().lower() for s in args.sources.split(",") if s.strip()]
        unknown = [s for s in selected if s not in SOURCES]
        if unknown:
            parser.error(f"unknown sources: {unknown}. Valid: {sorted(SOURCES)}")

    engine = get_engine()
    puller = RegulatoryEventsPuller(db_engine=engine)
    summary = puller.pull(sources=selected, limit_per_source=args.limit)
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
