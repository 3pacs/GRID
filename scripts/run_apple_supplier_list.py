#!/usr/bin/env python3
"""Runner for the Apple Supplier List puller.

Usage::

    python scripts/run_apple_supplier_list.py

Fetches the latest Apple Supplier List PDF, parses every supplier, and
upserts ``supply_chain_edges`` rows with ``downstream_id='aapl'`` and
``confidence='confirmed'``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from db import get_engine  # noqa: E402
from ingestion.altdata.apple_supplier_list import (  # noqa: E402
    AppleSupplierListPuller,
)


def main() -> int:
    engine = get_engine()
    puller = AppleSupplierListPuller(db_engine=engine)
    summary = puller.run()
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
