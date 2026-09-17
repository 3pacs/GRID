from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from alerts import scheduler
from api import main


def test_lifespan_starts_and_stops_alert_scheduler() -> None:
    """Regression test: schedule_alerts() was added to lifespan() in bb9ab4b1
    and silently dropped by the very next commit that touched this function,
    leaving alerts/email.py::daily_digest() unreachable for ~4 months with no
    error anywhere (api/main.py:38's _load_router only guards routers, not
    this inline startup block). Assert the wiring by source inspection since
    lifespan() itself needs a live DB/event bus to exercise end-to-end.
    """
    source = inspect.getsource(main.lifespan)

    assert "schedule_alerts" in source
    assert "stop_alerts" in source


class _FakeLockConn:
    """Answers only the two advisory-lock queries; anything else is a bug
    in the code under test, not something this fake should paper over."""

    def __init__(self, lock_available: bool = True):
        self.lock_available = lock_available
        self.lock_calls = 0
        self.unlock_calls = 0
        self.closed = False

    def execute(self, clause, params=None):
        sql = str(clause)
        if "pg_try_advisory_lock" in sql:
            self.lock_calls += 1
            return SimpleNamespace(scalar=lambda: self.lock_available)
        if "pg_advisory_unlock" in sql:
            self.unlock_calls += 1
            return SimpleNamespace(scalar=lambda: True)
        raise AssertionError(f"unexpected query against the lock connection: {sql}")

    def close(self):
        self.closed = True


class _FakeEngine:
    def __init__(self, lock_available: bool = True):
        self.conn = _FakeLockConn(lock_available)

    def connect(self):
        return self.conn


class _FakeStateStore:
    """In-memory stand-in for the alert_state row, used in place of
    _read_state/_write_state so tests can drive realistic sequences
    (including a write failing on a specific call) without a real DB.
    """

    def __init__(self):
        self.state: dict | None = None
        self.write_calls: list[tuple] = []
        self.fail_write_on_call: int | None = None

    def read(self, engine):
        return self.state

    def write(self, engine, at, status):
        self.write_calls.append((at, status))
        if self.fail_write_on_call == len(self.write_calls):
            raise RuntimeError("simulated persistence failure")
        self.state = {"seen_at": at, "status": status}


def _digest(status: str, error: str | None = None):
    result = {"send_status": status, "sent": status == "sent"}
    if error:
        result["error"] = error
    return result


def _in_window(minute: int = 30) -> datetime:
    return datetime(2026, 9, 17, scheduler._DIGEST_HOUR_UTC, minute, tzinfo=timezone.utc)


def test_outside_the_digest_hour_never_touches_the_engine() -> None:
    outside = _in_window().replace(hour=(scheduler._DIGEST_HOUR_UTC + 1) % 24)
    engine = _FakeEngine()

    scheduler._send_digest_once_per_window(engine, outside)

    assert engine.conn.lock_calls == 0


def test_confirmed_send_writes_uncertain_then_sent(monkeypatch) -> None:
    engine = _FakeEngine(lock_available=True)
    store = _FakeStateStore()
    monkeypatch.setattr(scheduler, "_read_state", store.read)
    monkeypatch.setattr(scheduler, "_write_state", store.write)
    digest_calls = []
    monkeypatch.setattr(
        "alerts.email.daily_digest", lambda: (digest_calls.append(1), _digest("sent"))[1]
    )

    now = _in_window()
    scheduler._send_digest_once_per_window(engine, now)

    assert digest_calls == [1]
    # The pre-send marker and the terminal write both happened, in order.
    assert [status for _at, status in store.write_calls] == ["uncertain", "sent"]
    assert store.state == {"seen_at": now, "status": "sent"}


def test_failed_send_is_recorded_as_failed_and_immediately_retryable(monkeypatch) -> None:
    engine = _FakeEngine(lock_available=True)
    store = _FakeStateStore()
    monkeypatch.setattr(scheduler, "_read_state", store.read)
    monkeypatch.setattr(scheduler, "_write_state", store.write)
    digest_calls = []
    monkeypatch.setattr(
        "alerts.email.daily_digest",
        lambda: (digest_calls.append(1), _digest("failed", "SMTP refused"))[1],
    )

    now = _in_window()
    scheduler._send_digest_once_per_window(engine, now)
    assert store.state["status"] == "failed"

    # A clean pre-acceptance failure must not block the very next attempt.
    scheduler._send_digest_once_per_window(engine, now + timedelta(minutes=1))
    assert len(digest_calls) == 2


def test_persist_failure_after_confirmed_send_leaves_uncertain_not_silent(monkeypatch) -> None:
    """This is the exact bug the reviewer flagged: SMTP succeeds, then the
    terminal state write raises (persistence failure, or a crash at that
    exact point). The pre-send "uncertain" marker must be what's left on
    disk — not nothing, which would read as "never attempted" and permit
    an automatic resend next minute.
    """
    engine = _FakeEngine(lock_available=True)
    store = _FakeStateStore()
    store.fail_write_on_call = 2  # the terminal write, not the pre-send one
    monkeypatch.setattr(scheduler, "_read_state", store.read)
    monkeypatch.setattr(scheduler, "_write_state", store.write)
    monkeypatch.setattr("alerts.email.daily_digest", lambda: _digest("sent"))

    now = _in_window()
    scheduler._send_digest_once_per_window(engine, now)

    # The terminal write raised, so it's not in state, but the pre-send
    # write succeeded and is what's left.
    assert store.state == {"seen_at": now, "status": "uncertain"}


def test_uncertain_state_blocks_automatic_retry_while_fresh(monkeypatch) -> None:
    engine = _FakeEngine(lock_available=True)
    now = _in_window()
    store = _FakeStateStore()
    store.state = {"seen_at": now - timedelta(minutes=1), "status": "uncertain"}
    monkeypatch.setattr(scheduler, "_read_state", store.read)
    monkeypatch.setattr(scheduler, "_write_state", store.write)
    digest_calls = []
    monkeypatch.setattr(
        "alerts.email.daily_digest", lambda: (digest_calls.append(1), _digest("sent"))[1]
    )

    scheduler._send_digest_once_per_window(engine, now)

    assert digest_calls == []


def test_stale_uncertain_state_still_blocks_within_the_gap_window(monkeypatch) -> None:
    """An unresolved attempt does not get silently retried just because
    time has passed within the same day's window — that would be
    guessing it's now safe. It stays blocked (same as a confirmed "sent"
    would) until the whole gap window elapses.
    """
    engine = _FakeEngine(lock_available=True)
    now = _in_window()
    store = _FakeStateStore()
    store.state = {
        "seen_at": now - timedelta(minutes=scheduler._UNCERTAIN_STALE_MINUTES + 1),
        "status": "uncertain",
    }
    monkeypatch.setattr(scheduler, "_read_state", store.read)
    monkeypatch.setattr(scheduler, "_write_state", store.write)
    digest_calls = []
    monkeypatch.setattr(
        "alerts.email.daily_digest", lambda: (digest_calls.append(1), _digest("sent"))[1]
    )

    scheduler._send_digest_once_per_window(engine, now)

    assert digest_calls == []


def test_uncertain_state_past_the_whole_gap_window_is_eligible_again(monkeypatch) -> None:
    engine = _FakeEngine(lock_available=True)
    now = _in_window()
    store = _FakeStateStore()
    store.state = {"seen_at": now - timedelta(hours=scheduler._MIN_GAP_HOURS + 1), "status": "uncertain"}
    monkeypatch.setattr(scheduler, "_read_state", store.read)
    monkeypatch.setattr(scheduler, "_write_state", store.write)
    digest_calls = []
    monkeypatch.setattr(
        "alerts.email.daily_digest", lambda: (digest_calls.append(1), _digest("sent"))[1]
    )

    scheduler._send_digest_once_per_window(engine, now)

    assert digest_calls == [1]


def test_two_concurrent_callers_only_one_sends(monkeypatch) -> None:
    """The bug this fixes: a plain read-then-later-write let two processes
    (or two racing loop iterations) both pass the "not yet sent" check
    before either had written it. A held advisory lock means the second
    caller can't even get past the lock to check.
    """
    shared_engine = _FakeEngine(lock_available=True)
    store = _FakeStateStore()
    monkeypatch.setattr(scheduler, "_read_state", store.read)
    monkeypatch.setattr(scheduler, "_write_state", store.write)
    digest_calls = []
    monkeypatch.setattr(
        "alerts.email.daily_digest", lambda: (digest_calls.append(1), _digest("sent"))[1]
    )

    now = _in_window()
    scheduler._send_digest_once_per_window(shared_engine, now)
    # Simulate a second caller finding the lock already held (a real
    # pg_try_advisory_lock from a second session would behave this way
    # while the first caller's connection is still open with the lock).
    shared_engine.conn.lock_available = False
    scheduler._send_digest_once_per_window(shared_engine, now)

    assert digest_calls == [1]


def test_already_sent_within_the_gap_skips_without_sending(monkeypatch) -> None:
    engine = _FakeEngine(lock_available=True)
    now = _in_window()
    store = _FakeStateStore()
    store.state = {"seen_at": now - timedelta(hours=1), "status": "sent"}
    monkeypatch.setattr(scheduler, "_read_state", store.read)
    monkeypatch.setattr(scheduler, "_write_state", store.write)
    digest_calls = []
    monkeypatch.setattr(
        "alerts.email.daily_digest", lambda: (digest_calls.append(1), _digest("sent"))[1]
    )

    scheduler._send_digest_once_per_window(engine, now)

    assert digest_calls == []
    assert engine.conn.unlock_calls == 1  # lock released even on the early-return path


def test_a_state_read_error_fails_closed_not_open(monkeypatch) -> None:
    """The bug this fixes: state reads used to swallow DB errors and
    return None, which reads as "never sent" — i.e. eligible to send. An
    unreadable state table must block sending, not permit it.
    """
    engine = _FakeEngine(lock_available=True)

    def _raise(engine):
        raise RuntimeError("state table unreachable")

    monkeypatch.setattr(scheduler, "_read_state", _raise)
    digest_calls = []
    monkeypatch.setattr(
        "alerts.email.daily_digest", lambda: (digest_calls.append(1), _digest("sent"))[1]
    )

    scheduler._send_digest_once_per_window(engine, _in_window())

    assert digest_calls == []


def test_schedule_alerts_start_stop_is_safe_without_reaching_email_or_db(monkeypatch) -> None:
    """Exercises the real daemon thread's start/stop mechanics only.

    _send_digest_once_per_window is forced to a no-op so the loop can
    never reach alerts.email.daily_digest() or a DB connection,
    regardless of the real wall-clock hour when this test happens to run
    in CI.
    """
    monkeypatch.setattr(scheduler, "_send_digest_once_per_window", lambda engine, now: None)

    def _fail_if_called(*_args, **_kwargs):
        raise AssertionError("daily_digest() must not run when the scheduler is stubbed out")

    monkeypatch.setattr("alerts.email.daily_digest", _fail_if_called, raising=False)

    scheduler.schedule_alerts()
    scheduler.schedule_alerts()  # must be idempotent — safe to call twice
    scheduler.stop_alerts()
