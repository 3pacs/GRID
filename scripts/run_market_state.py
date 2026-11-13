#!/usr/bin/env python3
"""Writer for ``market_state_daily``: the standardized market-state record (``intelligence/regime/market_state.py``).

One row per session, insert-only: a date that already has a row is left as it is unless ``--replace`` is given.
Computes the prior completed trading session by default (never today's), the same rule as
``scripts/run_regime_state_vectors.py``. ``--from/--to`` backfills a range of sessions in date order; every record is
computed only from what was known by the end of its own date, so a backfilled row equals what the nightly run would
have written then (apart from the revised-vintage caveat in the module docstring).

Usage:
    python3 scripts/run_market_state.py                         # prior session
    python3 scripts/run_market_state.py --dry-run --json        # print the record, write nothing
    python3 scripts/run_market_state.py --from 2019-01-02 --to 2026-11-12   # backfill sessions

Before computing, a real run fetches the FRED vintages published since the last stored one for the news indexes
(``ingestion/fred_vintages.py``; ``--no-refresh`` skips it). First time: ``python -m ingestion.fred_vintages backfill``.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _d(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--as-of", type=_d, default=None, help="one session (YYYY-MM-DD), strictly before today")
    p.add_argument("--from", dest="start", type=_d, default=None, help="backfill start (inclusive)")
    p.add_argument("--to", dest="end", type=_d, default=None, help="backfill end (inclusive), before today")
    p.add_argument("--dry-run", action="store_true", help="compute without writing")
    p.add_argument("--json", action="store_true", help="print each record (dry run) or summary as JSON")
    p.add_argument("--replace", action="store_true", help="overwrite an existing row for the same date")
    p.add_argument("--no-refresh", action="store_true",
                   help="skip fetching new FRED vintages of the news indexes first (a dry run never fetches)")
    return p


def sessions(start: date, end: date) -> list[date]:
    from ingestion.market_calendar import trading_days_between

    return [d for d in trading_days_between(start, end) if start <= d <= end]


def main(argv: list[str] | None = None, engine=None) -> int:
    a = build_parser().parse_args(argv)
    today = date.today()
    if a.start or a.end:
        if not (a.start and a.end):
            print("market-state: --from and --to go together", file=sys.stderr)
            return 2
        targets = sessions(a.start, a.end)
    elif a.as_of:
        targets = [a.as_of]
    else:
        from ingestion.market_calendar import last_trading_day

        targets = [last_trading_day(today - timedelta(days=1))]
    if any(t >= today for t in targets):
        print(f"market-state: refusing a date on or after today ({today})", file=sys.stderr)
        return 2

    from intelligence.regime.market_state import compute_market_state, load_calendar, store_market_state

    if engine is None:
        from db import get_engine

        engine = get_engine()
    calendar = load_calendar()
    refresh: list[dict] = []
    if not a.dry_run and not a.no_refresh:
        from ingestion.fred_vintages import SERIES, refresh as refresh_vintages

        refresh = [refresh_vintages(engine, s) for s in SERIES]
    written = skipped = 0
    for t in targets:
        state = compute_market_state(engine, t, calendar=calendar)
        rec = state.to_dict()
        if a.dry_run:
            if a.json:
                print(json.dumps(rec, sort_keys=True, default=str))
            else:
                print(f"market-state {t}: {rec['coverage']['ok']}/{rec['coverage']['total']} variables ok (dry run)")
            continue
        if store_market_state(engine, state, replace=a.replace):
            written += 1
        else:
            skipped += 1
    if not a.dry_run:
        summary = {"job": "run_market_state", "sessions": len(targets), "written": written,
                   "already_present": skipped, "vintage_refresh": [{k: r.get(k) for k in ("series", "status", "rows_sent")}
                                                                   for r in refresh],
                   "generated_at": datetime.now(timezone.utc).isoformat()}
        print(json.dumps(summary) if a.json else
              f"market-state: {written} written, {skipped} already present, {len(targets)} sessions")
    return 0


if __name__ == "__main__":
    sys.exit(main())
