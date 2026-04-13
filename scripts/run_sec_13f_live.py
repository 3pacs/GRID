#!/usr/bin/env python3
"""Runner for the SEC 13F live ingestor.

Pulls the latest 13F-HR filing for a curated set of institutional
investment managers and upserts the positions into
``institutional_holdings``.

Usage:
    python scripts/run_sec_13f_live.py                     # all filers
    python scripts/run_sec_13f_live.py --limit 10          # first 10
    python scripts/run_sec_13f_live.py --filers berkshire_hathaway,soros_fund
    python scripts/run_sec_13f_live.py --verbose
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Ensure repo root is importable.
_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
os.chdir(_REPO_ROOT)

from loguru import logger as log  # noqa: E402

from db import get_engine  # noqa: E402
from ingestion.altdata.sec_13f_live import (  # noqa: E402
    FILERS,
    SEC13FLiveIngestor,
    filer_by_key,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Live SEC 13F-HR ingestor for institutional_holdings.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process at most N filers (default: all).",
    )
    parser.add_argument(
        "--filers",
        type=str,
        default=None,
        help="Comma-separated filer keys to process (default: all). "
             "See FILERS in ingestion/altdata/sec_13f_live.py.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Log per-filer position detail.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    engine = get_engine()

    filters = None
    if args.filers:
        filters = []
        for key in args.filers.split(","):
            key = key.strip()
            if not key:
                continue
            f = filer_by_key(key)
            if f is None:
                log.error("Unknown filer key: {k}", k=key)
                return 2
            filters.append(f)

    ingestor = SEC13FLiveIngestor(engine=engine)
    results = ingestor.run(filers=filters, limit=args.limit, verbose=args.verbose)

    ok = sum(1 for r in results if r.status == "ok")
    rows = sum(r.rows_written for r in results)
    total_pos = sum(r.positions_total for r in results)
    match_pos = sum(r.positions_matched for r in results)
    coverage = (100.0 * match_pos / total_pos) if total_pos else 0.0

    print()
    print(f"SEC 13F live ingest summary:")
    print(f"  filers ok   : {ok}/{len(results)}")
    print(f"  rows written: {rows}")
    print(f"  positions   : {match_pos}/{total_pos} matched ({coverage:.1f}% CUSIP coverage)")

    # Per-filer breakdown
    for r in results:
        tag = {
            "ok": "OK  ",
            "no_filing": "NOFL",
            "no_positions": "NOPO",
            "error": "ERR ",
        }.get(r.status, "??? ")
        rep = r.filing.report_date if r.filing else "-"
        print(
            f"  [{tag}] {r.filer.key:24s} report={rep} "
            f"rows={r.rows_written} matched={r.positions_matched}/{r.positions_total}"
            + (f"  error={r.error}" if r.error else "")
        )

    if not ok:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
