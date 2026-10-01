"""Offline gates for the GEM daily capture runner; no provider or database.

Journal lines use the exact production shape (loguru prefix inside MESSAGE,
em-dash separators, journald ``_SYSTEMD_INVOCATION_ID``/``_PID`` fields), as
read from grid-scheduler on 2026-09-30.
"""

import json
import subprocess
from datetime import date, datetime, timezone

import pytest

from scripts import gem_daily_capture as daily

DAY = date(2026, 10, 1)
INV_A = "ab0d462dfc8b4c9586c7726a7cbbe8d6"
INV_B = "0123456789abcdef0123456789abcdef"

START_MSG = ("2026-10-01 13:30:17 | INFO     | __main__:run_daily_pulls:1208 — "
             "Starting daily pulls — start_date=2026-09-27, market_open=True")
DONE_MSG = ("2026-10-01 13:57:20 | INFO     | __main__:_run_equity_pulls:1544 — "
            "Options daily pull complete — 122/209 tickers, 66562 snapshots")


def _event(hour: int, minute: int, message: str, *, invocation: str | None = INV_A,
           pid: str | None = "2795500", day: date = DAY) -> str:
    stamp = datetime(day.year, day.month, day.day, hour, minute, tzinfo=timezone.utc)
    record = {"__REALTIME_TIMESTAMP": str(int(stamp.timestamp() * 1_000_000)),
              "MESSAGE": message}
    if invocation is not None:
        record["_SYSTEMD_INVOCATION_ID"] = invocation
    if pid is not None:
        record["_PID"] = pid
    return json.dumps(record)


def test_real_production_line_shape_passes() -> None:
    assert daily._scheduler_gate([
        _event(13, 30, START_MSG),
        _event(13, 36, "2026-10-01 13:36:31 | INFO     | __main__:_run_equity_pulls:1376 — "
                       "FRED daily pull complete — 26/87 series, 0 rows"),
        _event(13, 57, "2026-10-01 13:57:20 | INFO     | ingestion.options:pull_all:327 — "
                       "Options pull complete — 122/209 tickers, 66562 snapshots"),
        _event(13, 57, DONE_MSG),
    ], DAY)


@pytest.mark.parametrize("start_inv,start_pid,done_inv,done_pid", [
    (INV_A, "2795500", INV_B, "2795500"),   # restart between start and completion
    (INV_A, "2795500", INV_A, "2799999"),   # same invocation id, different process
    (None, "2795500", INV_A, "2795500"),    # start has no invocation identity
    (INV_A, "2795500", None, "2795500"),    # completion has no invocation identity
    ("", "2795500", "", "2795500"),         # empty identities never match
    (INV_A, None, INV_A, None),             # missing PIDs
])
def test_mixed_or_missing_invocation_identity_fails_closed(
    start_inv, start_pid, done_inv, done_pid,
) -> None:
    assert not daily._scheduler_gate([
        _event(13, 30, START_MSG, invocation=start_inv, pid=start_pid),
        _event(13, 57, DONE_MSG, invocation=done_inv, pid=done_pid),
    ], DAY)


def test_duplicate_failed_skipped_zero_and_out_of_order_refuse() -> None:
    start = _event(13, 30, START_MSG)
    done = _event(13, 57, DONE_MSG)
    assert not daily._scheduler_gate([start, start, done], DAY)
    assert not daily._scheduler_gate([start, done, done], DAY)
    assert not daily._scheduler_gate([done, start], DAY)
    assert not daily._scheduler_gate([start, done, _event(13, 58, "x — Options daily pull failed: boom")], DAY)
    assert not daily._scheduler_gate(
        [start, _event(13, 40, "x — Options pull skipped: 2026-10-01 is not a scheduled equity session"),
         done], DAY)
    assert not daily._scheduler_gate(
        [_event(13, 30, START_MSG.replace("market_open=True", "market_open=False")), done], DAY)
    assert not daily._scheduler_gate(
        [start, _event(13, 57, DONE_MSG.replace("122/209 tickers, 66562", "0/209 tickers, 0"))], DAY)
    assert not daily._scheduler_gate(
        [start, _event(13, 57, "x — Options daily pull complete — garbled")], DAY)
    assert not daily._scheduler_gate(["broken-json"], DAY)
    assert not daily._scheduler_gate([], DAY)


def test_only_pulls_after_1329_utc_count() -> None:
    early_start = _event(13, 10, START_MSG)
    early_done = _event(13, 20, DONE_MSG)
    assert not daily._scheduler_gate([early_start, early_done], DAY)
    assert daily._scheduler_gate(
        [early_start, early_done, _event(13, 30, START_MSG), _event(13, 57, DONE_MSG)], DAY)


def test_completion_after_absolute_deadline_refuses() -> None:
    assert not daily._scheduler_gate(
        [_event(13, 30, START_MSG), _event(14, 25, DONE_MSG)], DAY)
    winter = date(2026, 11, 2)
    assert daily._session_deadline(winter).hour == 15
    assert daily._session_deadline(DAY).hour == 14
    assert daily._scheduler_gate(
        [_event(13, 30, START_MSG, day=winter), _event(14, 25, DONE_MSG, day=winter)], winter)


def test_same_day_attempt_is_single_use(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(daily, "_ATTEMPTS", tmp_path)
    assert daily._claim_day(DAY)
    assert not daily._claim_day(DAY)
    assert len(list(tmp_path.iterdir())) == 1


def test_unactivated_code_never_claims_or_calls_provider(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(daily, "_ACTIVATED", tmp_path / "absent")
    monkeypatch.setattr(daily, "_claim_day", lambda _day: (_ for _ in ()).throw(
        AssertionError("attempt claimed before activation")))
    assert daily.main() == 1


def test_pinned_checkout_refuses_dirty_executable_or_untracked_code(tmp_path) -> None:
    def git(*args):
        subprocess.run(["git", "-C", str(tmp_path), *args], check=True,
                       capture_output=True, text=True)
    git("init")
    git("-c", "user.name=Test", "-c", "user.email=test@example.invalid",
        "commit", "--allow-empty", "-m", "init")
    executable = tmp_path / "scripts" / "gem_daily_capture.py"
    executable.parent.mkdir()
    executable.write_text("reviewed\n", encoding="utf-8")
    git("add", "scripts/gem_daily_capture.py")
    git("-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-m", "pin")
    assert daily._checkout_clean(tmp_path)
    executable.write_text("modified\n", encoding="utf-8")
    assert not daily._checkout_clean(tmp_path)
    git("checkout", "--", "scripts/gem_daily_capture.py")
    (tmp_path / "ingestion").mkdir()
    (tmp_path / "ingestion" / "options.py").write_text("new code\n", encoding="utf-8")
    assert not daily._checkout_clean(tmp_path)


class _Rows:
    def __init__(self, row):
        self.row = row

    def mappings(self):
        return self

    def first(self):
        return self.row


class _Conn:
    def __init__(self, row):
        self.row = row

    def execute(self, *_args, **_kwargs):
        return _Rows(self.row)


def _batch_row(**overrides):
    start = datetime(2026, 10, 1, 14, 6, tzinfo=timezone.utc)
    row = {"ticker": "SPY", "snap_date": DAY, "capture_ordinal": 42, "capture_source": "gem",
           "capture_started_at": start, "capture_completed_at": start.replace(minute=8),
           "row_count": 100, "n": 100, "calls": 50, "puts": 50, "expiries": 6, "bad": 0}
    row.update(overrides)
    return row


@pytest.mark.parametrize("overrides,ok", [
    ({}, True),
    ({"capture_source": "options_puller"}, False),   # not a GEM batch
    ({"n": 99}, False),                              # stored rows != registered
    ({"row_count": 99, "n": 99}, False),             # != reported inserted count
    ({"capture_ordinal": 41}, False),                # ordinal pair mismatch
    ({"ticker": "QQQ"}, False),
    ({"bad": 1}, False),
    ({"expiries": 7}, False),
    ({"capture_started_at": datetime(2026, 10, 1, 13, 20, tzinfo=timezone.utc)}, False),  # pre-open
])
def test_validate_batch(monkeypatch, overrides, ok) -> None:
    monkeypatch.setattr(daily, "_utc_now", lambda: datetime(2026, 10, 1, 14, 10, tzinfo=timezone.utc))
    result = {"capture_batch_id": "b", "capture_ordinal": 42, "snapshots_inserted": 100}
    assert daily._validate_batch(_Conn(_batch_row(**overrides)), "SPY", DAY, result) is ok
    assert daily._validate_batch(_Conn(None), "SPY", DAY, result) is False
