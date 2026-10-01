#!/usr/bin/env python3
"""One gated GEM options capture per NYSE session; run only by its timer.

This program never installs, enables or starts a timer. Every gate fails
closed and prints one ``GEM_SKIP``/``GEM_FAILED``/``GEM_VALIDATE`` line:

* activation identity: the ACTIVATED marker exists and this file runs from
  a clean Git checkout at ``/data/grid_v4/grid-options-puller-pins/<pin>``
  whose HEAD is ``GEM_DAILY_PIN_SHA`` and descends from #653 (01b19194);
* single use: a same-day claim file makes each UTC day one attempt, even
  when a later gate skips it;
* session: on/after 2026-10-01, an NYSE session, 09:30-16:00 New York,
  before the absolute 10:20 New York deadline;
* scheduler: since 13:29 UTC the grid-scheduler journal shows exactly one
  ``Starting daily pulls ... market_open=True`` and exactly one positive
  ``Options daily pull complete`` line, the completion after the start,
  both from the SAME non-empty systemd invocation ID and PID, and no
  options failure/skip line (review finding 2);
* schema: the append-only store (options_append_only_20260930) is live;
* VALIDATE: every GEM ticker's batch is registered with source ``gem``,
  its stored rows equal the registered and reported counts, and its capture
  lies inside the New York session.

GEM batches are appended; nothing deletes or replaces another batch.
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
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.market_calendar import is_market_open  # noqa: E402
from scripts.pull_options_gem_tickers import (  # noqa: E402
    GEM_CAPTURE_SOURCE,
    GEM_MAX_EXPIRATIONS,
    GEM_TICKERS,
)

_NY = ZoneInfo("America/New_York")
_FIRST_DAY = date(2026, 10, 1)
_SCHEDULER_WINDOW_UTC = time(13, 29)
_ATTEMPTS = Path("/data/grid_v4/gem_daily/attempts")
_ACTIVATED = Path("/data/grid_v4/gem_daily/ACTIVATED")
_PIN_ROOT = Path("/data/grid_v4/grid-options-puller-pins")
_SESSION_GUARD_COMMIT = "01b19194"  # #653: fail closed on non-session captures

_START_RE = re.compile(
    r"Starting daily pulls — start_date=[0-9-]+, market_open=(True|False)$")
_COMPLETE_RE = re.compile(
    r"Options daily pull complete — (\d+)/(\d+) tickers, (\d+) snapshots$")
_FAILURE_MARKERS = ("Options daily pull failed", "Options pull skipped")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _hard_stop(_signum, _frame) -> None:
    raise TimeoutError("GEM absolute 10:20 New York containment")


def _session_deadline(day: date) -> datetime:
    return datetime.combine(day, time(10, 20), _NY).astimezone(timezone.utc)


def _scheduler_window_start(day: date) -> datetime:
    return datetime.combine(day, _SCHEDULER_WINDOW_UTC, timezone.utc)


def _message(record: dict[str, Any]) -> str | None:
    raw = record.get("MESSAGE")
    if isinstance(raw, str):
        return raw
    if isinstance(raw, list) and all(isinstance(b, int) and 0 <= b < 256 for b in raw):
        return bytes(raw).decode("utf-8", errors="replace")
    return None


def _scheduler_gate(lines: list[str], day: date) -> bool:
    """Exactly one scheduler options pull since 13:29 UTC, one invocation.

    The start and its positive completion must carry the same non-empty
    ``_SYSTEMD_INVOCATION_ID`` and ``_PID``; a restart between them, a
    missing identity, a second pull, a failure/skip line, a zero-row or
    malformed completion, or out-of-order/unparseable journal output all
    fail closed.
    """
    window = _scheduler_window_start(day)
    starts: list[tuple[datetime, bool, str, str]] = []
    completes: list[tuple[datetime, bool, str, str]] = []
    previous = None
    for line in lines:
        try:
            record = json.loads(line)
            stamp = datetime.fromtimestamp(
                int(record["__REALTIME_TIMESTAMP"]) / 1_000_000, timezone.utc)
        except (ValueError, KeyError, TypeError, OverflowError):
            return False
        if not isinstance(record, dict):
            return False
        if previous is not None and stamp < previous:
            return False
        previous = stamp
        if stamp < window or stamp.date() != day:
            continue
        message = _message(record)
        if message is None:
            return False
        invocation = record.get("_SYSTEMD_INVOCATION_ID")
        pid = record.get("_PID")
        invocation = invocation if isinstance(invocation, str) else ""
        pid = pid if isinstance(pid, str) else ""
        if any(marker in message for marker in _FAILURE_MARKERS):
            return False
        start = _START_RE.search(message)
        if start:
            starts.append((stamp, start.group(1) == "True", invocation, pid))
            continue
        complete = _COMPLETE_RE.search(message)
        if complete:
            ok, total, snapshots = map(int, complete.groups())
            completes.append((stamp, 0 < ok <= total and snapshots > 0, invocation, pid))
        elif "Options daily pull complete" in message:
            return False
    if len(starts) != 1 or len(completes) != 1:
        return False
    s_at, market_open, s_inv, s_pid = starts[0]
    c_at, positive, c_inv, c_pid = completes[0]
    return (market_open and positive and c_at > s_at
            and c_at <= _session_deadline(day)
            and bool(s_inv) and s_inv == c_inv
            and bool(s_pid) and s_pid == c_pid)


def _read_scheduler_journal(day: date, now: datetime) -> list[str]:
    result = subprocess.run(
        ["journalctl", "-u", "grid-scheduler.service",
         "--since", _scheduler_window_start(day).strftime("%Y-%m-%d %H:%M:%S UTC"),
         "--until", now.strftime("%Y-%m-%d %H:%M:%S UTC"),
         "-o", "json", "--no-pager"],
        text=True, capture_output=True, timeout=15, check=True,
    )
    # A bounded day window should be small. Refuse truncated or unusual output.
    if len(result.stdout) > 5_000_000:
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
            or root != _PIN_ROOT / pin):
        return False
    try:
        head = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True,
            text=True, check=True, timeout=5,
        ).stdout.strip()
        ancestor = subprocess.run(
            ["git", "-C", str(root), "merge-base", "--is-ancestor", _SESSION_GUARD_COMMIT, pin],
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
            SELECT
              (SELECT c.relkind FROM pg_class c
                WHERE c.oid = to_regclass('options_snapshots')) = 'v',
              to_regclass('options_snapshots_all') IS NOT NULL,
              to_regclass('options_capture_batches') IS NOT NULL,
              (SELECT COUNT(*) FROM pg_trigger
                WHERE NOT tgisinternal
                  AND tgrelid IN (to_regclass('options_snapshots_all'),
                                  to_regclass('options_capture_batches'))
                  AND tgname IN (
                  'options_snapshots_all_no_row_mutation',
                  'options_snapshots_all_no_truncate',
                  'options_capture_batches_no_row_mutation',
                  'options_capture_batches_no_truncate')) = 4,
              (SELECT COUNT(*) FROM pg_constraint
                WHERE conrelid = to_regclass('options_snapshots_all')
                  AND conname IN ('options_snapshots_all_batch_fk',
                                  'options_snapshots_all_batch_required')) = 2
        """)).one()
    return all(value is True for value in row)


_VALIDATE_SQL = text("""
    SELECT b.ticker, b.snap_date, b.capture_ordinal, b.capture_source,
           b.capture_started_at, b.capture_completed_at, b.row_count,
           COUNT(s.id) AS n,
           COUNT(s.id) FILTER (WHERE s.opt_type = 'call') AS calls,
           COUNT(s.id) FILTER (WHERE s.opt_type = 'put') AS puts,
           COUNT(DISTINCT s.expiry) AS expiries,
           COUNT(s.id) FILTER (WHERE s.capture_ordinal <> b.capture_ordinal
             OR s.capture_started_at <> b.capture_started_at
             OR s.capture_completed_at <> b.capture_completed_at
             OR s.provider_regular_market_at IS NULL
             OR DATE(s.provider_regular_market_at AT TIME ZONE 'UTC') <> b.snap_date
             OR DATE(s.provider_regular_market_at AT TIME ZONE 'America/New_York') <> b.snap_date
             OR s.provider_regular_market_at > b.capture_completed_at) AS bad
    FROM options_capture_batches b
    LEFT JOIN options_snapshots_all s ON s.capture_batch_id = b.capture_batch_id
    WHERE b.capture_batch_id = :batch
    GROUP BY b.capture_batch_id
""")


def _validate_batch(conn, ticker: str, day: date, result: dict) -> bool:
    batch_id = result.get("capture_batch_id")
    ordinal = result.get("capture_ordinal")
    if (not isinstance(batch_id, str) or not batch_id or not isinstance(ordinal, int)
            or isinstance(ordinal, bool) or ordinal <= 0):
        return False
    row = conn.execute(_VALIDATE_SQL, {"batch": batch_id}).mappings().first()
    if row is None:
        return False
    start = row["capture_started_at"]
    completed = row["capture_completed_at"]
    if start is None or completed is None:
        return False
    in_session = all(
        ts.astimezone(_NY).date() == day and time(9, 30) <= ts.astimezone(_NY).time() <= time(16, 0)
        for ts in (start, completed))
    return (row["ticker"] == ticker and row["snap_date"] == day
            and row["capture_ordinal"] == ordinal
            and row["capture_source"] == GEM_CAPTURE_SOURCE
            and row["n"] == row["row_count"] == result.get("snapshots_inserted")
            and row["n"] > 0 and row["calls"] > 0 and row["puts"] > 0
            and 1 <= row["expiries"] <= GEM_MAX_EXPIRATIONS and row["bad"] == 0
            and start <= completed <= _utc_now() and in_session)


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
            print("GEM_SKIP scheduler options journal gate", flush=True)
            return 0
    except (OSError, subprocess.SubprocessError, ValueError):
        print("GEM_SKIP scheduler journal unavailable", flush=True)
        return 0

    from db import get_engine
    from ingestion.options import OptionsPuller

    try:
        # The systemd timeout is a second safety net. This one is absolute,
        # so a delayed 10:05 New York start cannot run past 10:20 New York.
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
            capture_source=GEM_CAPTURE_SOURCE,
        )
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
                print(f"GEM_VALIDATE {ticker} {'PASS' if good else 'FAIL'} "
                      f"batch={result.get('capture_batch_id')} "
                      f"ordinal={result.get('capture_ordinal')}", flush=True)
        return 0 if all(passed) else 1
    except Exception as exc:  # noqa: BLE001 - process boundary; no secret-bearing text
        print(f"GEM_FAILED {type(exc).__name__}", flush=True)
        return 1
    finally:
        if sys.platform == "linux":
            signal.setitimer(signal.ITIMER_REAL, 0)


if __name__ == "__main__":
    raise SystemExit(main())
