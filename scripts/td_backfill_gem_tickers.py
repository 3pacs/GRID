#!/usr/bin/env python3
"""Entry point kept for ``grid-td-backfill.service`` on grid-svr.

No Twelve Data request is made here any more (owner decision, 2026-11-23).
The unit's ExecStart still names this file, so it now runs
``scripts/close_backfill_universe.py gem``, which fills
``ticker_metrics_daily`` closes for the gem-alert tickers from GRID's own stored
prices. Once the unit points at that script, delete this file.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.close_backfill_universe import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main(["gem"]))
