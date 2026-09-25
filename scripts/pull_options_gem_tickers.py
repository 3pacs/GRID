#!/usr/bin/env python3
"""Compatibility entrypoint for the nine-ticker GEM options timer.

The retired installed script wrote options_snapshots directly, with no capture
batch metadata. All chain publication now belongs to ingestion.options.
"""

from __future__ import annotations

import sys
from pathlib import Path

from loguru import logger as log

# Keep `python3 scripts/pull_options_gem_tickers.py` callable from the repo.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

GEM_TICKERS = (
    "OPCH", "SPGI", "FLUT", "GEHC", "GLND", "BHRB",
    "SPY", "QQQ", "IWM",
)
GEM_MAX_EXPIRATIONS = 6


def main() -> int:
    """Run one complete canonical capture per legacy ticker; expose failures."""
    from db import get_engine
    from ingestion.options import OptionsPuller

    try:
        results = OptionsPuller(db_engine=get_engine()).pull_all(
            tickers=list(GEM_TICKERS),
            include_catalyst_universe=False,
            max_expirations=GEM_MAX_EXPIRATIONS,
        )
    except Exception as exc:  # noqa: BLE001 - process boundary, no secret-bearing error text
        log.error("GEM options pull failed during {kind}", kind=type(exc).__name__)
        return 1

    expected = list(GEM_TICKERS)
    if (not isinstance(results, list) or len(results) != len(expected)
            or any(not isinstance(row, dict) for row in results)
            or [row.get("ticker") for row in results] != expected):
        log.error("GEM options pull returned an incomplete ticker result set")
        return 1

    failures = [(row["ticker"], row.get("status")) for row in results
                if row.get("status") != "SUCCESS"]
    if failures:
        log.warning("GEM options pull incomplete: {statuses}", statuses=failures)
        return 1

    log.info("GEM options pull completed for {count} tickers", count=len(expected))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
