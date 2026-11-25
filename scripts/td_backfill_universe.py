#!/usr/bin/env python3
"""Entry point of ``grid-td-universe-backfill.service``: fill the universe's
missing closes from GRID's own stored prices. No Twelve Data call.

Owner decision 2026-11-23: "drop the twelve stuff, get it from our other
sources instead". The file keeps its name because the systemd unit's
``ExecStart`` names it (this change edits no unit and removes no key); the
work is ``scripts/close_backfill_universe.py universe``, which writes only a
missing close, from completed closes of the ticker's own instrument (see
that module). The gem cohort is NOT cut over: ``scripts/td_backfill_gem_tickers.py``
still calls Twelve Data.

What this stops doing, on purpose:

* it no longer fetches Twelve Data or reads ``TWELVEDATA_API_KEY``;
* it no longer stamps ``source_catalog`` ``TWELVEDATA`` as pulled (the gem
  backfill still reads that source);
* it no longer refreshes a close it wrote earlier (the old upsert replaced
  ``close_price`` where ``shares_outstanding`` was NULL; the stored-close
  fill never replaces an existing close, whoever wrote it).

``DRY_RUN=1``, ``ONLY_BUCKET`` and ``LIMIT`` work as in the module it calls.
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts import close_backfill_universe  # noqa: E402


def main() -> int:
    return close_backfill_universe.main(["universe"])


if __name__ == "__main__":
    sys.exit(main())
