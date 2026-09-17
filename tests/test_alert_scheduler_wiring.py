from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone

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


def test_should_send_now_only_fires_in_the_digest_hour() -> None:
    in_window = datetime(2026, 9, 17, scheduler._DIGEST_HOUR_UTC, 30, tzinfo=timezone.utc)
    outside_window = in_window.replace(hour=(scheduler._DIGEST_HOUR_UTC + 1) % 24)

    assert scheduler._should_send_now(engine=None, now=in_window) is True
    assert scheduler._should_send_now(engine=None, now=outside_window) is False


def test_should_send_now_respects_persisted_min_gap(monkeypatch) -> None:
    """Regression test: the old scheduler deduped on an in-process variable,
    so a restart mid-window (or a second worker process) could resend the
    same day's digest with no record of the earlier send. Dedup must be
    readable from persisted state alone.
    """
    now = datetime(2026, 9, 17, scheduler._DIGEST_HOUR_UTC, 30, tzinfo=timezone.utc)
    recently_sent = now - timedelta(hours=1)
    long_ago = now - timedelta(hours=scheduler._MIN_GAP_HOURS + 1)

    monkeypatch.setattr(scheduler, "_latest_sent_at", lambda engine: recently_sent)
    assert scheduler._should_send_now(engine=object(), now=now) is False

    monkeypatch.setattr(scheduler, "_latest_sent_at", lambda engine: long_ago)
    assert scheduler._should_send_now(engine=object(), now=now) is True


def test_schedule_alerts_start_stop_is_safe_without_reaching_email_or_db(monkeypatch) -> None:
    """Exercises the real daemon thread's start/stop mechanics only.

    The send-decision logic is covered directly by the tests above; here
    _should_send_now is forced to False so the loop can never reach
    alerts.email.daily_digest() or a DB connection, regardless of the
    real wall-clock hour when this test happens to run in CI.
    """
    monkeypatch.setattr(scheduler, "_should_send_now", lambda engine, now: False)

    def _fail_if_called(*_args, **_kwargs):
        raise AssertionError("daily_digest() must not run when _should_send_now is False")

    monkeypatch.setattr("alerts.email.daily_digest", _fail_if_called, raising=False)

    scheduler.schedule_alerts()
    scheduler.schedule_alerts()  # must be idempotent — safe to call twice
    scheduler.stop_alerts()
