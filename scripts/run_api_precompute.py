#!/usr/bin/env python3
"""Run GRID's API precompute jobs by hand (W3-2).

The intelligence scheduler already ticks these every 5 minutes
(``intelligence/api_precompute.py``). Use this to list the jobs, fill the
table right after the migration, force one job, or run them from another
LAN node (gridz4) against griddb. Each job holds a PostgreSQL advisory lock,
so this never computes a job the scheduler is computing at the same moment.

Usage::

    python scripts/run_api_precompute.py --list
    python scripts/run_api_precompute.py                 # every due job
    python scripts/run_api_precompute.py --job globe --force
    python scripts/run_api_precompute.py --json
"""

from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--list", action="store_true", help="list jobs and exit")
    parser.add_argument("--job", action="append", default=None, help="run only this job (repeatable)")
    parser.add_argument("--force", action="store_true", help="run even if not due")
    parser.add_argument("--json", action="store_true", help="print the run summary as JSON")
    args = parser.parse_args(argv)

    from intelligence import api_precompute as ap

    if args.list:
        print(json.dumps(ap.describe(), indent=2))
        return 0
    unknown = [j for j in (args.job or []) if j not in ap.JOBS_BY_NAME]
    if unknown:
        parser.error(f"unknown job(s): {', '.join(unknown)}; see --list")

    from db import get_engine

    results = ap.run_due(get_engine(), only=args.job, force=args.force)
    if args.json:
        print(json.dumps(results, indent=2, default=str))
    else:
        for r in results:
            print(r)
    return 1 if any(r.get("error") for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
