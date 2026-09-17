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


def _in_window(minute: int = 30) -> datetime:
    return datetime(2026, 9, 17, scheduler._DIGEST_HOUR_UTC, minute, tzinfo=timezone.utc)


def test_outside_the_digest_hour_never_touches_the_engine() -> None:
    outside = _in_window().replace(hour=(scheduler._DIGEST_HOUR_UTC + 1) % 24)
    engine = _FakeEngine()

    scheduler._send_digest_once_per_window(engine, outside)

    assert engine.conn.lock_calls == 0


def test_claims_lock_then_sends_then_records_on_confirmed_success(monkeypatch) -> None:
    engine = _FakeEngine(lock_available=True)
    monkeypatch.setattr(scheduler, "_latest_sent_at", lambda engine: None)
    recorded = []
    monkeypatch.setattr(scheduler, "_record_sent", lambda engine, now: recorded.append(now))
    digest_calls = []
    monkeypatch.setattr(
        "alerts.email.daily_digest",
        lambda: (digest_calls.append(1), {"sent": True})[1],
    )

    now = _in_window()
    scheduler._send_digest_once_per_window(engine, now)

    assert engine.conn.lock_calls == 1
    assert engine.conn.unlock_calls == 1
    assert digest_calls == [1]
    assert recorded == [now]


def test_unconfirmed_send_does_not_record_so_a_retry_can_happen(monkeypatch) -> None:
    """This is the fix for the "sent" flag meaning dispatch, not delivery:
    daily_digest() now returns a real synchronous outcome, and the
    scheduler must only persist a claim when that outcome was True.
    """
    engine = _FakeEngine(lock_available=True)
    monkeypatch.setattr(scheduler, "_latest_sent_at", lambda engine: None)
    recorded = []
    monkeypatch.setattr(scheduler, "_record_sent", lambda engine, now: recorded.append(now))
    monkeypatch.setattr(
        "alerts.email.daily_digest", lambda: {"sent": False, "error": "SMTP refused"}
    )

    scheduler._send_digest_once_per_window(engine, _in_window())

    assert recorded == []


def test_two_concurrent_callers_only_one_sends(monkeypatch) -> None:
    """The bug this fixes: a plain read-then-later-write let two processes
    (or two racing loop iterations) both pass the "not yet sent" check
    before either had written it. A held advisory lock means the second
    caller can't even get past the lock to check.
    """
    shared_engine = _FakeEngine(lock_available=True)
    monkeypatch.setattr(scheduler, "_latest_sent_at", lambda engine: None)
    monkeypatch.setattr(scheduler, "_record_sent", lambda engine, now: None)
    digest_calls = []
    monkeypatch.setattr(
        "alerts.email.daily_digest",
        lambda: (digest_calls.append(1), {"sent": True})[1],
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
    monkeypatch.setattr(scheduler, "_latest_sent_at", lambda engine: now - timedelta(hours=1))
    digest_calls = []
    monkeypatch.setattr(
        "alerts.email.daily_digest",
        lambda: (digest_calls.append(1), {"sent": True})[1],
    )

    scheduler._send_digest_once_per_window(engine, now)

    assert digest_calls == []
    assert engine.conn.unlock_calls == 1  # lock released even on the early-return path


def test_a_state_read_error_fails_closed_not_open(monkeypatch) -> None:
    """The bug this fixes: _latest_sent_at used to swallow DB errors and
    return None, which reads as "never sent" — i.e. eligible to send. An
    unreadable state table must block sending, not permit it.
    """
    engine = _FakeEngine(lock_available=True)

    def _raise(engine):
        raise RuntimeError("state table unreachable")

    monkeypatch.setattr(scheduler, "_latest_sent_at", _raise)
    digest_calls = []
    monkeypatch.setattr(
        "alerts.email.daily_digest",
        lambda: (digest_calls.append(1), {"sent": True})[1],
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
