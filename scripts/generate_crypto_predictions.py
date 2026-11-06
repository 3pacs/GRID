#!/usr/bin/env python3
"""Publish evidence-backed crypto insights; never manufacture a trading call."""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from oracle.astrogrid_insights import produce_insights
from store.astrogrid import AstroGridStore


def main() -> int:
    from db import get_engine

    engine = get_engine()
    result = produce_insights(engine, AstroGridStore(engine))
    print(json.dumps(result, default=str))
    # Missing evidence is an explicit abstention, not a made-up forecast.
    # A completely stale feed or failed persistence is an operational failure.
    return int(bool(result["errors"]) or not any(o["available"] for o in result["observations"]))


if __name__ == "__main__":
    raise SystemExit(main())
