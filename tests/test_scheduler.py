from __future__ import annotations

import pytest
import sys
from datetime import datetime
from types import SimpleNamespace

import config

try:
    import schedule  # noqa: F401
except ModuleNotFoundError:
    sys.modules["schedule"] = SimpleNamespace()

from intelligence import scheduler


class _StopScheduler(Exception):
    pass


class _FakeSchedule:
    def __init__(self) -> None:
        self.jobs: list[dict[str, object]] = []
        self.run_pending_calls = 0

    def every(self, interval: int = 1) -> "_FakeJob":
        return _FakeJob(self, interval)

    def run_pending(self) -> None:
        self.run_pending_calls += 1


class _FakeJob:
    _UNITS = {"minutes", "hours", "day", "days"}
    _DAYS = {
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    }

    def __init__(self, fake_schedule: _FakeSchedule, interval: int) -> None:
        self._schedule = fake_schedule
        self._interval = interval
        self._unit: str | None = None
        self._day: str | None = None
        self._at: str | None = None

    def __getattr__(self, name: str) -> "_FakeJob":
        if name in self._UNITS:
            self._unit = name
            return self
        if name in self._DAYS:
            self._day = name
            return self
        raise AttributeError(name)

    def at(self, when: str) -> "_FakeJob":
        self._at = when
        return self

    def do(self, func):
        self._schedule.jobs.append(
            {
                "interval": self._interval,
                "unit": self._unit,
                "day": self._day,
                "at": self._at,
                "func": func.__name__,
            }
        )
        return self


def test_intelligence_loop_registers_expected_jobs(monkeypatch):
    fake_schedule = _FakeSchedule()

    monkeypatch.setattr(config, "Settings", lambda: object())
    monkeypatch.setattr(scheduler, "_sched", fake_schedule)
    monkeypatch.setattr(
        scheduler.time,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(_StopScheduler()),
    )

    with pytest.raises(_StopScheduler):
        scheduler.run_intelligence_loop()

    jobs_by_name = {job["func"]: job for job in fake_schedule.jobs}

    assert fake_schedule.run_pending_calls == 0
    assert len(fake_schedule.jobs) >= 40
    assert jobs_by_name["_crucix_ingest"] == {
        "interval": 15,
        "unit": "minutes",
        "day": None,
        "at": None,
        "func": "_crucix_ingest",
    }
    assert jobs_by_name["_hourly_briefing"]["unit"] == "hours"
    assert jobs_by_name["_capital_flow_refresh"]["interval"] == 4
    assert jobs_by_name["_nightly_research"]["at"] == "02:45"
    assert jobs_by_name["_actor_news_weekly_tail"]["day"] == "sunday"
    assert jobs_by_name["_actor_news_weekly_tail"]["at"] == "04:00"
    assert jobs_by_name["_options_tracker"]["unit"] == "days"
    assert jobs_by_name["_fci_compute_6h"]["interval"] == 6
    assert jobs_by_name["_credit_novelty_daily"]["at"] == "04:30"


class _CapturingSchedule:
    """Minimal fake `schedule` module that captures the real job callables
    (by function name) instead of just recording their cadence, so a test
    can invoke one job's closure directly."""

    def __init__(self) -> None:
        self.callables: dict[str, object] = {}

    def every(self, interval: int = 1) -> "_CapturingJob":
        return _CapturingJob(self)

    def run_pending(self) -> None:
        pass


class _CapturingJob:
    _UNITS = {"minutes", "hours", "day", "days"}
    _DAYS = {
        "monday", "tuesday", "wednesday", "thursday", "friday",
        "saturday", "sunday",
    }

    def __init__(self, fake_schedule: _CapturingSchedule) -> None:
        self._schedule = fake_schedule

    def __getattr__(self, name: str) -> "_CapturingJob":
        if name in self._UNITS or name in self._DAYS:
            return self
        raise AttributeError(name)

    def at(self, when: str) -> "_CapturingJob":
        return self

    def do(self, func):
        self._schedule.callables[func.__name__] = func
        return self


def test_thesis_invalidation_hourly_logs_real_monitor_run_fields(monkeypatch):
    """CAT-190 regression: the hourly job's summary log line must read
    fields that actually exist on `MonitorRun`/`InvalidationEvent`
    ('MonitorRun' object has no attribute 'theses_checked' was the
    production failure). Runs the job's own summary-logging path against
    a real MonitorRun instance — not a mock — so a stale field name would
    raise AttributeError here exactly as it did in production."""
    import db
    import intelligence.thesis_invalidation_monitor as tim
    from intelligence.thesis_invalidation_monitor import (
        InvalidationEvent,
        MonitorRun,
    )

    captured = _CapturingSchedule()

    monkeypatch.setattr(config, "Settings", lambda: object())
    monkeypatch.setattr(scheduler, "_sched", captured)
    monkeypatch.setattr(
        scheduler.time,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(_StopScheduler()),
    )

    with pytest.raises(_StopScheduler):
        scheduler.run_intelligence_loop()

    thesis_job = captured.callables["_thesis_invalidation_hourly"]

    real_run = MonitorRun(
        as_of=datetime(2026, 4, 13),
        predictions_scanned=7,
        events=[
            InvalidationEvent(
                journal_id=1,
                ticker="AAPL",
                inval_type="price_level",
                triggered_at=datetime(2026, 4, 13),
                reason="close 170 < 180",
                current_value=170.0,
                threshold_value=180.0,
                auto_size_down_to=0.0,
            ),
        ],
        errors=["pred 9: malformed invalidation"],
    )

    monkeypatch.setattr(db, "get_engine", lambda: object())
    monkeypatch.setattr(tim, "run_monitor", lambda engine: real_run)

    warnings: list[str] = []
    infos: list[tuple[str, dict]] = []
    monkeypatch.setattr(
        scheduler.log, "warning", lambda msg, **kw: warnings.append(msg.format(**kw))
    )
    monkeypatch.setattr(
        scheduler.log, "info", lambda msg, **kw: infos.append((msg, kw))
    )

    thesis_job()

    # No AttributeError should have been swallowed into a warning log.
    assert warnings == []
    assert len(infos) == 1
    msg, kw = infos[0]
    assert kw["t"] == real_run.predictions_scanned == 7
    assert kw["i"] == real_run.triggered_count == 1
    assert kw["e"] == len(real_run.errors) == 1
