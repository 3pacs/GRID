from __future__ import annotations

import pytest
import sys
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
