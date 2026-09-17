from __future__ import annotations

import inspect

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


def test_alert_scheduler_start_stop_is_safe_without_a_running_loop() -> None:
    from alerts.scheduler import schedule_alerts, stop_alerts

    schedule_alerts()
    schedule_alerts()  # must be idempotent — safe to call twice
    stop_alerts()
