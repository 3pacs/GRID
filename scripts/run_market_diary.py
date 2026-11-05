#!/usr/bin/env python3
"""Scheduled writer for ``market_diary`` (Wave 3 slice W3.1).

Writes the day's market diary entry: rule-based market-move / active-actor
sections plus an LLM narrative over them, and a pre-open thesis verdict
(read from the last ``thesis_snapshots`` row before 13:30Z that day — never
computed at write time; see ``intelligence/market_diary.py``'s
``_gather_thesis_accuracy`` for the look-ahead this replaced).

Price sections are held behind ``GRID_MARKET_DIARY_PRICES_ENABLED`` (see
``intelligence/market_diary.py``) until the operator confirms the YF
quarantine has run; every price read that does clear the flag still
requires the newest accepted observation to be dated exactly the target
date, or it is reported as "no close for date" rather than a stale value.

Usage:
    python3 scripts/run_market_diary.py                    # today, write
    python3 scripts/run_market_diary.py --date 2026-09-26   # a specific date
    python3 scripts/run_market_diary.py --dry-run --json    # compute only
    python3 scripts/run_market_diary.py --previous-session  # last completed US session

``--previous-session`` is what the 05:30Z timer uses: yfinance closes only
become policy-eligible after the next New York midnight (04:00Z EDT / 05:00Z
EST), so the diary runs the next morning for the previous session rather than
in the evening for a close that is not yet eligible. ``--date`` wins if both
are given.

Timer template (not installed): deploy/systemd/grid-market-diary.timer.template
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from loguru import logger as log

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def previous_session_date(today: date | None = None) -> date:
    """Last US trading day strictly before ``today`` (UTC date by default)."""
    from ingestion.market_calendar import last_trading_day

    if today is None:
        today = datetime.now(timezone.utc).date()
    return last_trading_day(today - timedelta(days=1))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--date", default=None, help="ISO date (YYYY-MM-DD); default today")
    p.add_argument(
        "--previous-session",
        action="store_true",
        help="target the last US trading day strictly before today (UTC); ignored if --date is given",
    )
    p.add_argument("--dry-run", action="store_true", help="compute and render without writing")
    p.add_argument("--json", action="store_true", help="print the full result as JSON")
    return p


def main(argv: list[str] | None = None, engine=None) -> int:
    args = build_parser().parse_args(argv)

    from intelligence.market_diary import write_diary_entry

    if engine is None:
        from db import get_engine

        engine = get_engine()

    if args.date:
        target = date.fromisoformat(args.date)
    elif args.previous_session:
        target = previous_session_date()
        log.info("market-diary: --previous-session chose target_date={}", target)
    else:
        target = date.today()

    result = write_diary_entry(engine, target_date=target, dry_run=args.dry_run)

    if args.json:
        print(json.dumps(result, sort_keys=True, default=str))
    else:
        acc = result.get("thesis_accuracy", {})
        moves = result.get("market_moves", {})
        print(
            f"market-diary {result['date']}: "
            f"prices_enabled={moves.get('prices_enabled')} "
            f"verdict={acc.get('verdict')} reason={acc.get('reason')} "
            f"narrative_model={result.get('narrative_model')} "
            f"narrative_fallback={result.get('narrative_fallback')}"
            + (" [dry-run]" if result.get("dry_run") else "")
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
