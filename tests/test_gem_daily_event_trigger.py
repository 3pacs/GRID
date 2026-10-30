"""GEM-EV: the capture starts on the scheduler's options completion, not a clock.

Offline: journal lines use the production shape (loguru prefix in MESSAGE,
``_SYSTEMD_INVOCATION_ID``/``_PID``); no provider, no database.
"""

import json
import sys
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from scripts import gem_daily_capture as daily

DAY = date(2026, 10, 1)  # Thursday, EDT
INV = "ab0d462dfc8b4c9586c7726a7cbbe8d6"
START_MSG = ("2026-10-01 13:30:17 | INFO     | __main__:run_daily_pulls:1208 — "
             "Starting daily pulls — start_date=2026-09-27, market_open=True")
DONE_MSG = ("2026-10-01 13:57:20 | INFO     | __main__:_run_equity_pulls:1544 — "
            "Options daily pull complete — 122/209 tickers, 66562 snapshots")
OTHER_MSG = ("2026-10-01 13:36:31 | INFO     | __main__:_run_equity_pulls:1376 — "
             "FRED daily pull complete — 26/87 series, 0 rows")


def _at(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2026, 10, 1, hour, minute, second, tzinfo=timezone.utc)


def _event(stamp: datetime, message: str, *, invocation: str = INV, pid: str = "2795500") -> str:
    return json.dumps({"__REALTIME_TIMESTAMP": str(int(stamp.timestamp() * 1_000_000)),
                       "MESSAGE": message, "_SYSTEMD_INVOCATION_ID": invocation, "_PID": pid})


class _Clock:
    def __init__(self, start: datetime) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now


def _stream(clock: _Clock, items):
    """Yield journal items; ``("tick", seconds)`` advances the clock and yields None."""
    for item in items:
        if isinstance(item, tuple):
            clock.now += timedelta(seconds=item[1])
            yield None
        else:
            clock.now = max(clock.now, datetime.fromtimestamp(
                int(json.loads(item)["__REALTIME_TIMESTAMP"]) / 1e6, timezone.utc))
            yield item


def _stop_after_completion(items):
    for item in items:
        yield item
    raise AssertionError("waited past the completion line")


def test_state_machine_pending_then_pass() -> None:
    start = _event(_at(13, 30, 17), START_MSG)
    assert daily._scheduler_state([], DAY) == ("pending", None)
    assert daily._scheduler_state([start, _event(_at(13, 36), OTHER_MSG)], DAY) == ("pending", None)
    done_at = _at(13, 57, 20)
    assert daily._scheduler_state([start, _event(done_at, DONE_MSG)], DAY) == ("pass", done_at)


@pytest.mark.parametrize("lines", [
    [_event(_at(13, 57), DONE_MSG)],                                          # completion, no start
    [_event(_at(13, 30), START_MSG.replace("True", "False"))],                # market closed
    [_event(_at(13, 30), START_MSG, invocation="")],                          # no identity
    [_event(_at(13, 30), START_MSG), _event(_at(13, 31), START_MSG)],          # second start
    [_event(_at(13, 30), START_MSG), _event(_at(13, 40), "x — Options daily pull failed: boom")],
    [_event(_at(13, 30), START_MSG), _event(_at(13, 40), "x — Options pull skipped: closed")],
    [_event(_at(13, 30), START_MSG), _event(_at(13, 57), DONE_MSG, invocation="f" * 32)],
    [_event(_at(13, 30), START_MSG), _event(_at(13, 57), DONE_MSG, pid="1")],
    [_event(_at(13, 30), START_MSG),
     _event(_at(13, 57), DONE_MSG.replace("122/209 tickers, 66562", "0/209 tickers, 0"))],
    ["not json"],
])
def test_state_machine_fails_closed(lines) -> None:
    assert daily._scheduler_state(lines, DAY) == ("fail", None)


def test_trigger_fires_on_the_completion_line_without_waiting_further() -> None:
    clock = _Clock(_at(13, 31))
    items = [_event(_at(13, 30, 17), START_MSG), ("tick", 2), _event(_at(13, 36), OTHER_MSG),
             ("tick", 2), _event(_at(13, 57, 20), DONE_MSG)]
    state, done_at = daily._await_scheduler(DAY, _stop_after_completion(_stream(clock, items)), clock)
    assert (state, done_at) == ("pass", _at(13, 57, 20))
    assert clock.now - done_at < timedelta(seconds=5)  # started within seconds, not at 14:05


def test_failure_line_skips_immediately() -> None:
    clock = _Clock(_at(13, 31))
    items = [_event(_at(13, 30), START_MSG), _event(_at(13, 50), "x — Options daily pull failed: boom")]
    assert daily._await_scheduler(DAY, _stop_after_completion(_stream(clock, items)), clock) == ("fail", None)


def test_no_completion_by_latest_start_is_a_timeout_skip() -> None:
    clock = _Clock(_at(13, 31))
    items = [_event(_at(13, 30), START_MSG)] + [("tick", 60)] * 40  # 40 min of silence
    state, _ = daily._await_scheduler(DAY, _stream(clock, items), clock)
    assert state == "timeout"
    assert clock.now >= daily._latest_start(DAY)


def test_late_completion_after_latest_start_does_not_trigger() -> None:
    clock = _Clock(_at(13, 31))
    items = [_event(_at(13, 30), START_MSG), ("tick", 34 * 60 + 1), _event(_at(14, 6), DONE_MSG)]
    assert daily._await_scheduler(DAY, _stream(clock, items), clock)[0] == "timeout"


def test_stream_end_and_oversize_fail_closed(monkeypatch) -> None:
    clock = _Clock(_at(13, 31))
    assert daily._await_scheduler(DAY, iter([_event(_at(13, 30), START_MSG)]), clock) == ("fail", None)
    monkeypatch.setattr(daily, "_JOURNAL_LIMIT_BYTES", 10)
    assert daily._await_scheduler(DAY, iter([_event(_at(13, 30), START_MSG)]), clock) == ("fail", None)


@pytest.fixture
def armed(tmp_path, monkeypatch):
    """main() past activation, with an offline journal, clock and receipt."""
    monkeypatch.setattr(daily, "_ATTEMPTS", tmp_path)
    monkeypatch.setattr(daily, "_activation_identity_ok", lambda: True)
    monkeypatch.setattr(daily, "_arm_deadline", lambda *_a: None)
    monkeypatch.setattr(daily, "_disarm_deadline", lambda: None)
    clock = _Clock(_at(13, 31))
    monkeypatch.setattr(daily, "_utc_now", clock)
    state = SimpleNamespace(clock=clock, items=[], confirm=None, tmp=tmp_path)
    monkeypatch.setattr(daily, "_follow_scheduler_journal", lambda _day: _stream(clock, state.items))
    monkeypatch.setattr(daily, "_read_scheduler_journal",
                        lambda _day, _now: state.confirm if state.confirm is not None else [
                            i for i in state.items if not isinstance(i, tuple)])
    return state


def _receipt(state) -> str:
    return (state.tmp / DAY.isoformat()).read_text()


def test_main_triggers_on_completion_and_records_lag(armed, monkeypatch) -> None:
    armed.items = [_event(_at(13, 30, 17), START_MSG), ("tick", 2), _event(_at(13, 57, 20), DONE_MSG)]
    monkeypatch.setitem(sys.modules, "db", SimpleNamespace(get_engine=lambda: object()))
    monkeypatch.setattr(daily, "_append_only_schema_ok", lambda _e: False)  # stop before provider
    assert daily.main() == 0
    receipt = _receipt(armed)
    assert "GEM_TRIGGER scheduler_options_complete=2026-10-01T13:57:20+00:00 lag_s=0.0" in receipt
    assert "GEM_SKIP append-only schema gate" in receipt


def test_main_timeout_records_skip_and_never_reaches_provider(armed, monkeypatch) -> None:
    armed.items = [_event(_at(13, 30), START_MSG)] + [("tick", 60)] * 40
    monkeypatch.setitem(sys.modules, "db", SimpleNamespace(
        get_engine=lambda: pytest.fail("no capture after a timeout")))
    assert daily.main() == 0
    assert "GEM_SKIP scheduler options not complete by latest start 10:05 New York" in _receipt(armed)


def test_main_confirmation_rejects_a_second_pull_seen_by_the_strict_check(armed, monkeypatch) -> None:
    armed.items = [_event(_at(13, 30), START_MSG), _event(_at(13, 57), DONE_MSG)]
    armed.confirm = armed.items + [_event(_at(13, 58), START_MSG)]
    monkeypatch.setitem(sys.modules, "db", SimpleNamespace(
        get_engine=lambda: pytest.fail("no capture when confirmation fails")))
    assert daily.main() == 0
    assert "GEM_SKIP scheduler options journal gate (confirmation)" in _receipt(armed)


def test_main_refuses_to_arm_after_latest_start(armed) -> None:
    armed.clock.now = _at(14, 5)  # 10:05 New York
    assert daily.main() == 0
    assert "GEM_SKIP date/session/time gate" in _receipt(armed)


def test_main_refuses_to_arm_before_the_open(armed) -> None:
    armed.clock.now = _at(13, 29)  # 09:29 New York
    assert daily.main() == 0
    assert "GEM_SKIP date/session/time gate" in _receipt(armed)


# --- review round 1 (B1, B2, S1-S3) -----------------------------------------

def test_confirmation_until_includes_a_same_second_completion(monkeypatch) -> None:
    """B1: --until is whole-second; it must be rounded up, not down."""
    seen = {}

    def fake_run(argv, **_kwargs):
        seen["argv"] = argv
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(daily.subprocess, "run", fake_run)
    daily._read_scheduler_journal(DAY, datetime(2026, 10, 1, 13, 57, 20, 800000, tzinfo=timezone.utc))
    until = seen["argv"][seen["argv"].index("--until") + 1]
    assert until == "2026-10-01 13:57:21 UTC"  # a 13:57:20.300 completion stays inside


def test_pass_after_latest_start_without_an_idle_tick_is_a_timeout() -> None:
    """B2: a completion processed at/after 10:05 NY never starts a capture."""
    clock = _Clock(_at(13, 31))
    items = [_event(_at(13, 30), START_MSG), _event(_at(14, 4, 59), DONE_MSG)]

    def late(stream):
        for item in stream:
            if item is not None and "Options daily pull complete" in item:
                clock.now = _at(14, 5, 1)  # decision lands after the latest start
            yield item

    assert daily._await_scheduler(DAY, late(iter(items)), clock)[0] == "timeout"
    clock = _Clock(_at(13, 31))
    items = [_event(_at(13, 30), START_MSG), _event(_at(14, 5, 0), DONE_MSG)]
    assert daily._await_scheduler(DAY, _stream(clock, items), clock)[0] == "timeout"


def test_main_rechecks_latest_start_after_confirmation(armed, monkeypatch) -> None:
    armed.items = [_event(_at(13, 30), START_MSG), _event(_at(14, 4, 50), DONE_MSG)]
    real_read = daily._read_scheduler_journal

    def slow_confirmation(day, now):
        armed.clock.now = _at(14, 5, 2)  # the confirmation read straddled 10:05 NY
        return real_read(day, now)

    monkeypatch.setattr(daily, "_read_scheduler_journal", slow_confirmation)
    monkeypatch.setitem(sys.modules, "db", SimpleNamespace(
        get_engine=lambda: pytest.fail("no capture after the latest start")))
    assert daily.main() == 0
    assert "GEM_SKIP scheduler options not complete by latest start" in _receipt(armed)


def test_alarm_during_wait_is_labelled_as_deadline(armed) -> None:
    def alarm(_day):
        raise TimeoutError("GEM absolute 10:20 New York containment")
        yield  # pragma: no cover

    daily_follow = daily._follow_scheduler_journal
    try:
        daily._follow_scheduler_journal = alarm
        assert daily.main() == 0
    finally:
        daily._follow_scheduler_journal = daily_follow
    assert "GEM_SKIP absolute deadline reached while waiting" in _receipt(armed)


def test_tracker_is_incremental_and_matches_whole_window() -> None:
    lines = [_event(_at(13, 30), START_MSG)] + [
        _event(_at(13, 31) + timedelta(milliseconds=i), OTHER_MSG) for i in range(3000)
    ] + [_event(_at(14, 2), DONE_MSG)]
    tracker = daily._SchedulerTracker(DAY)
    for line in lines:
        tracker.feed(line)
    assert tracker.state() == daily._scheduler_state(lines, DAY) == ("pass", _at(14, 2))


_FAKE_JOURNALCTL = r"""
import sys, time
out = sys.stdout.buffer
out.write(b'{"a": 1}\n{"b"'); out.flush(); time.sleep(0.3)
out.write(b': 2}\n'); out.flush()
if sys.argv[1] == "eof":
    sys.exit(0)
time.sleep(60)
"""


@pytest.mark.skipif(sys.platform == "win32", reason="select() on pipes needs POSIX")
@pytest.mark.parametrize("mode", ["eof", "hang"])
def test_follow_splits_partial_lines_and_cleans_up_the_child(monkeypatch, mode) -> None:
    import subprocess as sp

    real_popen = sp.Popen
    procs = []

    def fake_popen(argv, **kwargs):
        assert argv[:3] == ["journalctl", "-u", "grid-scheduler.service"]
        assert "-f" in argv and "--no-tail" in argv and "--since" in argv
        proc = real_popen([sys.executable, "-c", _FAKE_JOURNALCTL, mode], **kwargs)
        procs.append(proc)
        return proc

    monkeypatch.setattr(daily.subprocess, "Popen", fake_popen)
    stream = daily._follow_scheduler_journal(DAY, poll_seconds=0.1)
    got = None
    if mode == "hang":
        import time as _time
        got, deadline = [], _time.monotonic() + 10
        while len(got) < 2 and _time.monotonic() < deadline:
            item = next(stream)
            if item is not None:
                got.append(item)
    if mode == "eof":
        lines = []
        with pytest.raises(OSError):
            for item in stream:
                if item is not None:
                    lines.append(item)
        assert lines == ['{"a": 1}', '{"b": 2}']
    else:
        assert got == ['{"a": 1}', '{"b": 2}']
        stream.close()
        assert procs[0].poll() is not None  # journalctl child terminated on close
