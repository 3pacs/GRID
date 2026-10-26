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

Timer template (not installed): deploy/systemd/grid-market-diary.timer.template
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--date", default=None, help="ISO date (YYYY-MM-DD); default today")
    p.add_argument("--dry-run", action="store_true", help="compute and render without writing")
    p.add_argument("--json", action="store_true", help="print the full result as JSON")
    return p


def main(argv: list[str] | None = None, engine=None) -> int:
    args = build_parser().parse_args(argv)

    from intelligence.market_diary import write_diary_entry

    if engine is None:
        from db import get_engine

        engine = get_engine()

    target = date.fromisoformat(args.date) if args.date else date.today()

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
