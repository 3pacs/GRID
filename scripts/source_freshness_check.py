#!/usr/bin/env python3
"""Print source-level freshness for the SmartScheduler registry (read-only).

    python3 scripts/source_freshness_check.py               # problems only, table
    python3 scripts/source_freshness_check.py --all         # every registry source
    python3 scripts/source_freshness_check.py --json        # machine-readable
    python3 scripts/source_freshness_check.py --fail-on STALE,NEVER   # exit 1 if any

Verdicts and why UNKNOWN is not STALE: see ingestion/source_freshness.py.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.source_freshness import FRESH, VERDICT_ORDER, check


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--all", action="store_true", help="include FRESH sources")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--lookback-days", type=int, default=45)
    ap.add_argument("--fail-on", default="", help="comma-separated verdicts that make the exit code 1")
    args = ap.parse_args(argv)

    from db import get_engine

    results = check(get_engine(), lookback_days=args.lookback_days)
    counts = Counter(r.verdict for r in results)
    shown = results if args.all else [r for r in results if r.verdict != FRESH]

    if args.json:
        print(json.dumps({"counts": dict(counts), "sources": [r.as_dict() for r in shown]}, indent=1))
    else:
        print("  ".join(f"{v}={counts.get(v, 0)}" for v in VERDICT_ORDER), f"(of {len(results)})")
        for r in shown:
            last_ok = r.last_success.strftime("%Y-%m-%d %H:%M") if r.last_success else "-"
            rows = "?" if r.rows_reported_since_success is None else str(r.rows_reported_since_success)
            err = (r.last_error or "")[:90].replace("\n", " ")
            print(f"{r.verdict:8} {r.name:24} freq={r.freq_h:g}h last_ok={last_ok:16} "
                  f"tries={r.attempts_since_success:<3} rows={rows:>8} {r.last_error_class or '':8} {err}")

    fail_on = {v.strip().upper() for v in args.fail_on.split(",") if v.strip()}
    return 1 if fail_on and any(counts.get(v) for v in fail_on) else 0


if __name__ == "__main__":
    sys.exit(main())
