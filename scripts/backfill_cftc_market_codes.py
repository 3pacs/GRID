"""Backfill CFTC COT history under the code-keyed series ids (owner-gated, A1).

Background
----------
``ingestion/altdata/cftc_cot.py`` now stores each market under
``cftc.<cftc_contract_market_code>.<metric>`` (see
``ingestion/altdata/cftc_markets.py``). Scheduled runs only fetch forward
(plus an 8-week bootstrap window), so the full history for the new ids is a
one-time ``raw_series`` write that needs the owner's approval (god-view plan
step A1). The legacy name-matched ids (``cftc.SP500.*`` ...) are not read or
modified by this script.

Safety
------
* **Dry run is the default.** It fetches from the public CFTC Socrata API,
  reads the existing dates for each new series id (read-only connection),
  and prints per-market counts, the report-date range and every market name
  seen under the code. It writes nothing.
* ``--execute`` performs the write, one transaction per market code.
  Re-running is a no-op for dates already stored (per-series date dedup).

Usage
-----
    python3 scripts/backfill_cftc_market_codes.py                 # dry run, all codes
    python3 scripts/backfill_cftc_market_codes.py --codes 13874A 043602
    python3 scripts/backfill_cftc_market_codes.py --start 2006-01-01 --execute
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from typing import Any

from ingestion.altdata.cftc_cot import BACKFILL_DEFAULT_START, CFTCCOTPuller
from ingestion.altdata.cftc_markets import MARKETS


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--codes", nargs="*", default=None,
                   help="market codes to backfill (default: every tracked code)")
    p.add_argument("--start", type=date.fromisoformat, default=BACKFILL_DEFAULT_START,
                   help="earliest report date (default %(default)s)")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", dest="dry_run", action="store_true", default=True,
                      help="fetch and count only, write nothing (default)")
    mode.add_argument("--execute", dest="dry_run", action="store_false",
                      help="write rows to raw_series (owner-approved step A1 only)")
    args = p.parse_args(argv)
    if args.codes:
        unknown = [c for c in args.codes if c not in MARKETS]
        if unknown:
            p.error(f"unknown market code(s): {unknown}; tracked: {sorted(MARKETS)}")
    return args


def run(puller: Any, args: argparse.Namespace) -> list[dict[str, Any]]:
    return puller.pull_all(market_codes=args.codes, start_date=args.start, dry_run=args.dry_run)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    from db import get_engine

    puller = CFTCCOTPuller(get_engine())
    results = run(puller, args)
    for r in results:
        print(json.dumps({k: r.get(k) for k in (
            "market_code", "root", "status", "dry_run", "start_date",
            "first_report_date", "last_report_date", "reports",
            "rows_would_insert", "rows_inserted", "market_names_seen", "errors",
        )}, default=str))
        for s in r.get("skipped") or []:
            print("   skipped:", json.dumps(s, default=str))
    bad = [r for r in results if r["status"] in ("FAILED", "SKIPPED")]
    print(f"{'DRY RUN' if args.dry_run else 'EXECUTED'}: {len(results)} markets, "
          f"{len(bad)} failed/skipped", file=sys.stderr)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
