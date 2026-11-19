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
    state = SimpleNamespace(clock=clock, items=[], confirm=None, tmp=tmp_path, slept=[])

    def _fake_sleep(seconds):
        state.slept.append(seconds)
        clock.now += timedelta(seconds=seconds)

    monkeypatch.setattr(daily, "_sleep", _fake_sleep)
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


class _CaptureConn:
    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, *_args, **_kwargs):
        return None


class _CaptureEngine:
    def connect(self):
        return _CaptureConn()


UNDATED = ("incomplete options chain response: 0 failed expiry fetch(es), 1 undated quote(s), "
           "0 empty of 6 expiries, 0 contracts, timed_out=False")


def _capture_run(armed, monkeypatch, failing, retry_failing=None, *, capture_minutes=0):
    """Run main() through capture, VALIDATE and the retry.

    ``failing`` tickers fail the first capture; ``retry_failing`` (default:
    the same set) fail the 10:00 retry. ``capture_minutes`` is how long the
    first capture takes on the fake clock.
    """
    armed.items = [_event(_at(13, 30, 17), START_MSG), _event(_at(13, 57, 20), DONE_MSG)]
    monkeypatch.setitem(sys.modules, "db", SimpleNamespace(get_engine=_CaptureEngine))
    monkeypatch.setattr(daily, "_append_only_schema_ok", lambda _e: True)
    calls = armed.calls = []
    plan = [set(failing), set(failing if retry_failing is None else retry_failing)]

    class _Puller:
        def __init__(self, db_engine):
            pass

        def pull_all(self, tickers, **_kwargs):
            bad = plan[min(len(calls), 1)]
            calls.append((list(tickers), armed.clock.now))
            if len(calls) == 1:
                armed.clock.now += timedelta(minutes=capture_minutes)
            return [{"ticker": t, "status": "FAILED" if t in bad else "SUCCESS",
                     "error": UNDATED if t in bad else None,
                     "capture_batch_id": None if t in bad else f"b{len(calls)}-{t}",
                     "capture_ordinal": None if t in bad else 7 + len(calls)}
                    for t in tickers]

    monkeypatch.setitem(sys.modules, "ingestion.options", SimpleNamespace(OptionsPuller=_Puller))
    monkeypatch.setattr(daily, "_validate_batch", lambda _c, t, _d, r: r["status"] == "SUCCESS")
    return daily.main()


def test_main_partial_capture_exits_zero_with_a_partial_record(armed, monkeypatch) -> None:
    assert _capture_run(armed, monkeypatch, {"GLND"}) == 0
    receipt = _receipt(armed)
    assert "GEM_VALIDATE GLND FAIL batch=None ordinal=None" in receipt
    assert receipt.count(" PASS batch=b1-") == len(daily.GEM_TICKERS) - 1
    assert "GEM_VALIDATE GLND FAIL batch=None ordinal=None attempt=2" in receipt
    assert (f"GEM_RESULT PARTIAL pass={len(daily.GEM_TICKERS) - 1} fail=1 failed=[GLND]"
            in receipt)


def test_main_all_tickers_failing_still_exits_non_zero(armed, monkeypatch) -> None:
    assert _capture_run(armed, monkeypatch, set(daily.GEM_TICKERS)) == 1
    assert (f"GEM_RESULT FAIL pass=0 fail={len(daily.GEM_TICKERS)} "
            f"failed=[{','.join(daily.GEM_TICKERS)}]") in _receipt(armed)


def test_main_full_pass_records_pass_and_exits_zero(armed, monkeypatch) -> None:
    assert _capture_run(armed, monkeypatch, set()) == 0
    assert f"GEM_RESULT PASS pass={len(daily.GEM_TICKERS)} fail=0 failed=[]" in _receipt(armed)


def test_main_run_error_still_exits_non_zero(armed, monkeypatch) -> None:
    def boom(*_a, **_k):
        raise RuntimeError("provider down")
    monkeypatch.setattr(daily, "_validate_batch", boom)
    armed.items = [_event(_at(13, 30, 17), START_MSG), _event(_at(13, 57, 20), DONE_MSG)]
    monkeypatch.setitem(sys.modules, "db", SimpleNamespace(get_engine=_CaptureEngine))
    monkeypatch.setattr(daily, "_append_only_schema_ok", lambda _e: True)
    puller = SimpleNamespace(pull_all=lambda tickers, **_k: [
        {"ticker": t, "status": "SUCCESS"} for t in tickers])
    monkeypatch.setitem(sys.modules, "ingestion.options",
                        SimpleNamespace(OptionsPuller=lambda db_engine: puller))
    assert daily.main() == 1
    receipt = _receipt(armed)
    assert "GEM_FAILED RuntimeError" in receipt
    assert "GEM_RESULT" not in receipt


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


def test_retry_captures_a_failed_ticker_again_at_ten_new_york(armed, monkeypatch) -> None:
    assert _capture_run(armed, monkeypatch, {"GLND"}, set()) == 0
    receipt = _receipt(armed)
    first, retry = armed.calls
    assert first[0] == list(daily.GEM_TICKERS)
    assert retry == (["GLND"], daily._retry_at(DAY))  # 10:00 New York, one ticker
    assert ("GEM_REASON GLND attempt=1 status=FAILED reason=incomplete failed_fetches=0 "
            "undated_quotes=1 empty=0/6 contracts=0 timed_out=False") in receipt
    assert "GEM_RETRY START tickers=[GLND]" in receipt
    assert "GEM_VALIDATE GLND PASS batch=b2-GLND ordinal=9 attempt=2" in receipt
    assert f"GEM_RESULT PASS pass={len(daily.GEM_TICKERS)} fail=0 failed=[]" in receipt


def test_retry_that_fails_again_stays_partial(armed, monkeypatch) -> None:
    assert _capture_run(armed, monkeypatch, {"GLND", "OPCH"}, {"GLND"}) == 0
    receipt = _receipt(armed)
    assert armed.calls[1][0] == ["OPCH", "GLND"]  # GEM order, failed tickers only
    assert "GEM_VALIDATE OPCH PASS batch=b2-OPCH ordinal=9 attempt=2" in receipt
    assert "GEM_REASON GLND attempt=2 status=FAILED reason=incomplete" in receipt
    assert (f"GEM_RESULT PARTIAL pass={len(daily.GEM_TICKERS) - 1} fail=1 failed=[GLND]"
            in receipt)


def test_retry_runs_at_once_after_ten_but_never_after_the_latest_start(armed, monkeypatch) -> None:
    # First capture ends 10:01 New York: retry immediately, no sleep.
    assert _capture_run(armed, monkeypatch, {"GLND"}, set(), capture_minutes=4) == 0
    assert armed.calls[1][1] == _at(14, 1, 20) and armed.slept == []
    assert "GEM_RESULT PASS" in _receipt(armed)


def test_no_retry_once_the_latest_start_has_passed(armed, monkeypatch) -> None:
    assert _capture_run(armed, monkeypatch, {"GLND"}, set(), capture_minutes=9) == 0
    receipt = _receipt(armed)
    assert len(armed.calls) == 1
    assert "GEM_RETRY SKIP latest start 10:05 New York reached failed=[GLND]" in receipt
    assert "GEM_RESULT PARTIAL" in receipt


@pytest.mark.parametrize("result, expected", [
    ({"status": "SUCCESS"}, "status=SUCCESS reason=validate"),
    ({"status": "FAILED", "error": UNDATED},
     "status=FAILED reason=incomplete failed_fetches=0 undated_quotes=1 empty=0/6 "
     "contracts=0 timed_out=False"),
    ({"status": "FAILED", "error": "no data from options provider"},
     "status=FAILED reason=no_provider_data"),
    ({"status": "FAILED", "error": "connection to server at 10.0.0.1 password=x failed"},
     "status=FAILED reason=error=other"),
    ({"status": "SKIPPED", "reason": "no dated regular-market price"},
     "status=SKIPPED reason=no_dated_regular-market_price"),
    ({"status": "DEFERRED", "reason": "time budget"}, "status=DEFERRED reason=time_budget"),
    ({}, "status=unknown reason=none"),
])
def test_failure_reason_is_short_and_secret_free(result, expected) -> None:
    assert daily._failure_reason(result) == expected
