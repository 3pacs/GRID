#!/usr/bin/env python3
"""One gated GEM options capture; intended only for the held timer template.

This program never installs or starts its timer. A claim file makes every
calendar day single-attempt, including days skipped by a failed gate.
"""

from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import sys
from datetime import date, datetime, time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.market_calendar import is_market_open
from scripts.pull_options_gem_tickers import (
    GEM_MAX_EXPIRATIONS,
    GEM_TICKERS,
)

_NY = ZoneInfo("America/New_York")
_FIRST_DAY = date(2026, 10, 1)
_ATTEMPTS = Path("/data/grid_v4/gem_daily/attempts")
_ACTIVATED = Path("/data/grid_v4/gem_daily/ACTIVATED")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _hard_stop(_signum, _frame) -> None:
    raise TimeoutError("GEM absolute 10:20 New York containment")


def _session_deadline(day: date) -> datetime:
    return datetime.combine(day, time(10, 20), _NY).astimezone(timezone.utc)


def _scheduler_gate(lines: list[str], day: date) -> bool:
    """Require one NYSE-open daily start and a positive options completion."""
    start = []
    complete = []
    failed = []
    previous = None
    for line in lines:
        try:
            record = json.loads(line)
            stamp = datetime.fromtimestamp(
                int(record["__REALTIME_TIMESTAMP"]) / 1_000_000, timezone.utc
            )
            message = str(record.get("MESSAGE", ""))
        except (ValueError, KeyError, TypeError, OverflowError):
            return False
        if previous is not None and stamp < previous:
            return False
        previous = stamp
        local = stamp.astimezone(_NY)
        if local.date() != day or local.time() < time(9, 29):
            continue
        if "Starting daily pulls" in message:
            start.append((stamp, "market_open=True" in message))
        match = re.fullmatch(
            r"Options daily pull complete — (\d+)/(\d+) tickers, (\d+) snapshots",
            message,
        )
        if match:
            ok, total, snapshots = map(int, match.groups())
            complete.append((stamp, 0 < ok <= total and snapshots > 0))
        elif "Options daily pull complete" in message:
            return False
        if "Options daily pull failed" in message or "Options pull skipped" in message:
            failed.append(stamp)
    return (len(start) == len(complete) == 1 and start[0][1]
            and complete[0][1] and complete[0][0] > start[0][0]
            and complete[0][0] <= _session_deadline(day) and not failed)


def _read_scheduler_journal(day: date, now: datetime) -> list[str]:
    result = subprocess.run(
        ["journalctl", "-u", "grid-scheduler.service", "--since",
         (datetime.combine(day, time(9, 29), _NY).astimezone(timezone.utc)
          .strftime("%Y-%m-%d %H:%M:%S UTC")), "--until",
         now.strftime("%Y-%m-%d %H:%M:%S UTC"), "-o", "json", "--no-pager"],
        text=True, capture_output=True, timeout=15, check=True,
    )
    # A bounded day window should be small. Refuse truncated or unusual output.
    if len(result.stdout) > 2_000_000:
        raise ValueError("scheduler journal window too large")
    return result.stdout.splitlines()


def _claim_day(day: date) -> bool:
    _ATTEMPTS.mkdir(mode=0o750, parents=True, exist_ok=True)
    try:
        fd = os.open(_ATTEMPTS / day.isoformat(), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
    except FileExistsError:
        return False
    with os.fdopen(fd, "w", encoding="ascii") as receipt:
        receipt.write(f"{_utc_now().isoformat()} STARTED\n")
    return True


def _activation_identity_ok() -> bool:
    pin = os.environ.get("GEM_DAILY_PIN_SHA", "")
    root = Path(__file__).resolve().parents[1]
    if (not _ACTIVATED.is_file() or len(pin) != 40
            or any(c not in "0123456789abcdef" for c in pin)
            or root != Path("/data/grid_v4/grid-options-puller-pins") / pin):
        return False
    try:
        head = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True,
            text=True, check=True, timeout=5,
        ).stdout.strip()
        ancestor = subprocess.run(
            ["git", "-C", str(root), "merge-base", "--is-ancestor", "01b19194", pin],
            capture_output=True, check=False, timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return head == pin and ancestor.returncode == 0 and _checkout_clean(root)


def _checkout_clean(root: Path) -> bool:
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "status", "--porcelain=v1", "--untracked-files=all"],
            capture_output=True, text=True, check=True, timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return not result.stdout


def _append_only_schema_ok(engine) -> bool:
    with engine.connect() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        conn.execute(text("SET LOCAL statement_timeout = '10s'"))
        row = conn.execute(text("""
            SELECT to_regclass('options_snapshots_batch_strike_key') IS NOT NULL,
                   to_regclass('options_snapshots_legacy_strike_key') IS NOT NULL,
                   to_regclass('options_snapshots_ticker_snap_date_expiry_opt_type_strike_key') IS NULL
        """)).one()
    return all(row)


def _validate_batch(conn, ticker: str, day: date, result: dict) -> bool:
    batch_id = result.get("capture_batch_id")
    ordinal = result.get("capture_ordinal")
    if not batch_id or not isinstance(ordinal, int) or ordinal <= 0:
        return False
    row = conn.execute(text("""
        SELECT COUNT(*) AS n,
               COUNT(*) FILTER (WHERE opt_type = 'call') AS calls,
               COUNT(*) FILTER (WHERE opt_type = 'put') AS puts,
               COUNT(DISTINCT expiry) AS expiries,
               COUNT(*) FILTER (WHERE capture_ordinal IS NULL OR capture_ordinal <> :ordinal
                 OR capture_started_at IS NULL OR capture_completed_at IS NULL
                 OR provider_regular_market_at IS NULL
                 OR DATE(provider_regular_market_at AT TIME ZONE 'UTC') <> :day
                 OR DATE(provider_regular_market_at AT TIME ZONE 'America/New_York') <> :day
                 OR provider_regular_market_at > capture_completed_at) AS bad,
               MIN(capture_started_at) AS first_start,
               MAX(capture_started_at) AS last_start,
               MIN(capture_completed_at) AS first_complete,
               MAX(capture_completed_at) AS last_complete
        FROM options_snapshots
        WHERE ticker = :ticker AND snap_date = :day AND capture_batch_id = :batch
    """), {"ticker": ticker, "day": day, "batch": batch_id, "ordinal": ordinal}).mappings().one()
    start = row["first_start"]
    completed = row["first_complete"]
    if (not start or not completed or start != row["last_start"]
            or completed != row["last_complete"]):
        return False
    return (row["n"] == result.get("snapshots_inserted")
            and row["n"] > 0 and row["calls"] > 0 and row["puts"] > 0
            and 1 <= row["expiries"] <= GEM_MAX_EXPIRATIONS and row["bad"] == 0
            and start <= completed <= _utc_now()
            and start.astimezone(_NY).date() == day
            and completed.astimezone(_NY).date() == day
            and time(9, 30) <= start.astimezone(_NY).time() <= time(16, 0)
            and time(9, 30) <= completed.astimezone(_NY).time() <= time(16, 0))


def main() -> int:
    now = _utc_now()
    day = now.date()
    if not _activation_identity_ok():
        print("GEM_SKIP activation identity gate", flush=True)
        return 1
    if not _claim_day(day):
        print("GEM_SKIP same-day attempt already recorded", flush=True)
        return 0
    if (day < _FIRST_DAY or day.weekday() >= 5 or not is_market_open(day)
            or now >= _session_deadline(day)
            or now.astimezone(_NY).date() != day
            or not time(9, 30) <= now.astimezone(_NY).time() <= time(16, 0)):
        print("GEM_SKIP date/session/time gate", flush=True)
        return 0
    try:
        if not _scheduler_gate(_read_scheduler_journal(day, now), day):
            print("GEM_SKIP daily options journal gate", flush=True)
            return 0
    except (OSError, subprocess.SubprocessError, ValueError):
        print("GEM_SKIP scheduler journal unavailable", flush=True)
        return 0

    from db import get_engine
    from ingestion.options import OptionsPuller

    try:
        # The systemd 15-minute timeout is a second safety net. This one is
        # absolute, so a delayed 10:05 NY timer start cannot run past 10:20 NY.
        if sys.platform == "linux":
            remaining = (_session_deadline(day) - _utc_now()).total_seconds()
            if remaining <= 0:
                print("GEM_SKIP absolute deadline", flush=True)
                return 0
            signal.signal(signal.SIGALRM, _hard_stop)
            signal.setitimer(signal.ITIMER_REAL, remaining)
        engine = get_engine()
        if not _append_only_schema_ok(engine):
            print("GEM_SKIP append-only schema gate", flush=True)
            return 0
        results = OptionsPuller(db_engine=engine).pull_all(
            tickers=list(GEM_TICKERS), include_catalyst_universe=False,
            max_expirations=GEM_MAX_EXPIRATIONS,
            should_continue=lambda: _utc_now() < _session_deadline(day),
        )
        if (len(results) == 1 and results[0].get("status") == "SKIPPED"
                and results[0].get("reason") == "another options writer active"):
            print("GEM_SKIP another options writer active", flush=True)
            return 0
        if len(results) != len(GEM_TICKERS) or [r.get("ticker") for r in results] != list(GEM_TICKERS):
            print("GEM_FAILED incomplete result set", flush=True)
            return 1
        passed = []
        with engine.connect() as conn:
            conn.execute(text("SET statement_timeout = '20s'"))
            for result in results:
                ticker = result["ticker"]
                good = (result.get("status") == "SUCCESS"
                        and _validate_batch(conn, ticker, day, result))
                passed.append(good)
                print(f"GEM_VALIDATE {ticker} {'PASS' if good else 'FAIL'}", flush=True)
        return 0 if all(passed) else 1
    except Exception as exc:  # noqa: BLE001 - process boundary; no secret-bearing text
        print(f"GEM_FAILED {type(exc).__name__}", flush=True)
        return 1
    finally:
        if sys.platform == "linux":
            signal.setitimer(signal.ITIMER_REAL, 0)


if __name__ == "__main__":
    raise SystemExit(main())
