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

``--previous-session`` skips (exit 0, nothing written) when the previous New York
day was a weekday NYSE holiday -- the 05:15Z capture slot skips it too -- and
reports ``capture_ready`` (advisory: is a policy-qualified ``YF:^GSPC:close``
for the target readable now?) without ever blocking the write.

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


_ANCHOR_SERIES = "YF:^GSPC:close"
_ANCHOR_LOOKBACK_DAYS = 14


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def new_york_date(now_utc: datetime | None = None) -> date:
    """The America/New_York calendar date at ``now_utc`` (default: now).

    A yfinance daily close is policy-eligible only after the next New York
    local midnight, so this date -- not the UTC date -- is the one that
    decides which session is complete. At the 05:30Z timer slot the two are
    equal (01:30 EDT / 00:30 EST); they differ only between 00:00Z and the
    midnight boundary, i.e. for a late ``Persistent=true`` catch-up.
    """
    from zoneinfo import ZoneInfo

    return (now_utc or _utc_now()).astimezone(ZoneInfo("America/New_York")).date()


def previous_session_date(today: date | None = None) -> date:
    """Last US trading day strictly before ``today`` (New York date by default)."""
    from ingestion.market_calendar import last_trading_day

    if today is None:
        today = new_york_date()
    return last_trading_day(today - timedelta(days=1))


def previous_session_skip_reason(today: date | None = None) -> str | None:
    """Why a ``--previous-session`` run has nothing new to write, else ``None``.

    The 05:15Z capture slot captures nothing when the previous day was an NYSE
    holiday on a weekday, and the session before it was already written by the
    run that followed *its* capture. Re-running would only regenerate that
    entry (new narrative, ``generated_at`` and insight-archive row). Weekend
    previous days are not skipped: that is the Friday-session run on Saturday.
    """
    from ingestion.market_calendar import is_market_holiday

    if today is None:
        today = new_york_date()
    prior = today - timedelta(days=1)
    if prior.weekday() < 5 and is_market_holiday(prior):
        return (
            f"{prior} was an NYSE holiday: the capture slot skips it and the "
            "previous session was already written by an earlier run"
        )
    return None


def qualified_close_ready(engine, target: date, now_utc: datetime | None = None) -> bool | None:
    """Advisory: is a policy-qualified ``YF:^GSPC:close`` for ``target`` readable now?

    Same reader and point-in-time bounds the diary's price section uses
    (``read_daily_policy_n``, ``as_of_ts=now``). ``False`` means the 05:15Z
    capture has not produced a qualified close (failed, late or skipped) and
    the diary's price section will say "no close for date"; ``None`` means the
    probe itself failed. It never blocks the write.
    """
    from store.observations import read_daily_policy_n

    now = now_utc or _utc_now()
    try:
        with engine.connect() as conn:
            rows = read_daily_policy_n(
                conn, _ANCHOR_SERIES, 1, source="yfinance",
                start=target - timedelta(days=_ANCHOR_LOOKBACK_DAYS), as_of=target, as_of_ts=now,
            )
    except Exception as exc:  # noqa: BLE001 - advisory only
        log.warning("market-diary: capture readiness probe failed: {e}", e=str(exc))
        return None
    return bool(rows) and rows[0].obs_date == target


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--date", default=None, help="ISO date (YYYY-MM-DD); default today")
    p.add_argument(
        "--previous-session",
        action="store_true",
        help="target the last US trading day strictly before today (New York date); skipped when the previous day was an NYSE weekday holiday; ignored if --date is given",
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

    capture_ready: bool | None = None
    probed = False
    if args.date:
        target = date.fromisoformat(args.date)
    elif args.previous_session:
        reason = previous_session_skip_reason()
        if reason:
            log.info("market-diary: --previous-session skipped: {r}", r=reason)
            print(json.dumps({"skipped": True, "reason": reason}) if args.json
                  else f"market-diary: skipped ({reason})")
            return 0
        target = previous_session_date()
        log.info("market-diary: --previous-session chose target_date={}", target)
        capture_ready = qualified_close_ready(engine, target)
        probed = True
        if capture_ready is False:
            log.warning(
                "market-diary: no policy-qualified {s} for {d} yet (05:15Z capture failed, "
                "late or skipped); the price section will report 'no close for date'; "
                "re-run with --date {d} once the capture lands",
                s=_ANCHOR_SERIES, d=target,
            )
    else:
        target = date.today()

    result = write_diary_entry(engine, target_date=target, dry_run=args.dry_run)

    if probed:
        result["capture_ready"] = capture_ready

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
            + (f" capture_ready={capture_ready}" if probed else "")
            + (" [dry-run]" if result.get("dry_run") else "")
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
