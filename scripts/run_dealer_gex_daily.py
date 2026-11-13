#!/usr/bin/env python3
"""Write ``dealer_gex_daily`` (SPY) for the prior completed session, or a range (``physics/dealer_gex_daily.py``).

    python3 scripts/run_dealer_gex_daily.py                         # prior session
    python3 scripts/run_dealer_gex_daily.py --from 2026-09-26 --to 2026-11-12
    python3 scripts/run_dealer_gex_daily.py --dry-run --json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _d(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


def main(argv: list[str] | None = None, engine=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--from", dest="start", type=_d)
    ap.add_argument("--to", dest="end", type=_d)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    from ingestion.market_calendar import last_trading_day, trading_days_between

    today = date.today()
    if a.start or a.end:
        if not (a.start and a.end):
            print("dealer-gex-daily: --from and --to go together", file=sys.stderr)
            return 2
        days = trading_days_between(a.start, a.end)
    else:
        days = [last_trading_day(today - timedelta(days=1))]
    if any(d >= today for d in days):
        print(f"dealer-gex-daily: refusing a date on or after today ({today})", file=sys.stderr)
        return 2
    from physics.dealer_gex_daily import run

    if engine is None:
        from db import get_engine

        engine = get_engine()
    out = run(engine, days, dry_run=a.dry_run)
    print(json.dumps(out, default=str) if a.json else
          f"dealer-gex-daily: {out['written']} written, {out['already_present']} present, {len(out['skipped'])} skipped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
