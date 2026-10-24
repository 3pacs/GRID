"""yfinance slot gating in the domestic scheduler.

The yfinance writer only marks a daily close POLICY_ELIGIBLE once the pull
happens after the next New York midnight, so the first qualified close for
session D used to land in the 13:30Z run on D+1. The scheduler now has a 05:15Z
yfinance-only morning slot. The 13:30Z slot keeps running yfinance: that row is
the opening-time raw observation and 16:00/20:00/22:00Z are same-day
snapshots (all with the #853 writer's INCOMPLETE/UNKNOWN labels, none a close).
The 05:15Z run is the separate completed-close capture; its daily-bar Open is
the official open seen next morning, not an opening-time observation.

Everything is a local fake: no network, no database, no email.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone
from types import ModuleType
from unittest.mock import MagicMock

import pytest
import schedule

import ingestion.scheduler as sched

# Modules _run_equity_pulls / run_daily_pulls import lazily. Each is replaced by
# a MagicMock module so the "other pullers still run" claim is observable.
_OTHER_MODULES = (
    "ingestion.fred",
    "ingestion.altdata.insider_filings",
    "ingestion.options",
    "discovery.options_scanner",
    "ingestion.celestial.lunar",
    "ingestion.celestial.planetary",
    "ingestion.celestial.vedic",
    "ingestion.celestial.chinese",
    "ingestion.celestial.solar",
    "ingestion.altdata.cboe_indices",
    "ingestion.altdata.fed_speeches",
    "ingestion.altdata.repo_market",
    "ingestion.altdata.yield_curve_full",
    "ingestion.coingecko",
    "ingestion.altdata.google_trends",
    "ingestion.crucix_bridge",
    "ingestion.ml.finbert_scorer",
    "scripts.auto_regime",
    "alerts.email",
)


class _World:
    """Handles on the fakes installed by the ``world`` fixture."""

    def __init__(self, modules: dict[str, MagicMock], yf_puller: MagicMock) -> None:
        self.modules = modules
        self.yf_puller = yf_puller


@pytest.fixture
def world(monkeypatch: pytest.MonkeyPatch) -> _World:
    monkeypatch.setenv("TESTING", "true")
    monkeypatch.setenv("DB_PASSWORD", "synthetic-test-only")

    fake_db = ModuleType("db")
    fake_db.get_engine = lambda: MagicMock()
    monkeypatch.setitem(sys.modules, "db", fake_db)

    modules = {name: MagicMock(name=name) for name in _OTHER_MODULES}
    for name, mod in modules.items():
        monkeypatch.setitem(sys.modules, name, mod)
    modules["scripts.auto_regime"].run.return_value = {"regime": "TEST"}

    yf_puller = MagicMock(name="YFinancePuller()")
    yf_puller.pull_all.return_value = [
        {"ticker": "SPY", "status": "SUCCESS", "rows_inserted": 1, "outcome": "inserted"},
    ]
    yf_puller.pull_ticker.return_value = {"outcome": "inserted", "rows_inserted": 1}
    fake_yf = ModuleType("ingestion.yfinance_pull")
    fake_yf.YFinancePuller = lambda **_kwargs: yf_puller
    monkeypatch.setitem(sys.modules, "ingestion.yfinance_pull", fake_yf)
    return _World(modules, yf_puller)


def _other_equity_pullers_ran(world: _World) -> None:
    mods = world.modules
    mods["ingestion.fred"].FREDPuller.return_value.pull_all.assert_called_once()
    mods["ingestion.altdata.insider_filings"].InsiderFilingsPuller.return_value.pull_all.assert_called_once()
    mods["ingestion.options"].OptionsPuller.return_value.pull_all.assert_called_once()
    mods["discovery.options_scanner"].OptionsScanner.return_value.scan_all.assert_called_once()


def _24x7_pullers_ran(world: _World) -> None:
    mods = world.modules
    mods["ingestion.coingecko"].CoinGeckoPuller.return_value.pull_all.assert_called_once()
    mods["ingestion.altdata.google_trends"].GoogleTrendsPuller.return_value.pull_all.assert_called_once()
    mods["ingestion.crucix_bridge"].CrucixBridgePuller.return_value.pull_all.assert_called_once()
    mods["ingestion.ml.finbert_scorer"].FinBERTScorer.return_value.score_all_sources.assert_called_once()
    mods["scripts.auto_regime"].run.assert_called_once()


def _open_on_session_day(monkeypatch: pytest.MonkeyPatch) -> None:
    """run_daily_pulls decides 'market open' from the real date; pin it open
    and the slot on time so only the slot gating is under test."""
    import ingestion.market_calendar as mc

    monkeypatch.setattr(mc, "is_market_open", lambda _d: True)
    monkeypatch.setattr(sched, "_slot_lateness", lambda slot, now=None: timedelta(0))


# ── every domestic slot (13:30 opening observation included) runs yfinance ──


@pytest.mark.parametrize("slot", ["13:30", "16:00", "20:00", "22:00"])
def test_every_domestic_slot_still_runs_yfinance(world, monkeypatch, slot) -> None:
    _open_on_session_day(monkeypatch)

    sched.run_daily_pulls(start_date="2026-10-01", slot=slot)

    world.yf_puller.pull_all.assert_called_once_with(start_date="2026-10-01")
    _other_equity_pullers_ran(world)
    _24x7_pullers_ran(world)


def test_run_daily_pulls_without_a_slot_still_runs_yfinance(world, monkeypatch) -> None:
    """Manual / script callers (scripts/run_full_pipeline.py) pass no slot."""
    _open_on_session_day(monkeypatch)

    sched.run_daily_pulls(start_date="2026-10-01")

    world.yf_puller.pull_all.assert_called_once_with(start_date="2026-10-01")


def test_run_equity_pulls_runs_yfinance_and_the_other_pullers(world) -> None:
    sched._run_equity_pulls(start_date="2026-10-01")
    world.yf_puller.pull_all.assert_called_once_with(start_date="2026-10-01")
    _other_equity_pullers_ran(world)


def test_1330_slot_keeps_the_completed_spy_close_check(world, monkeypatch) -> None:
    """The 13:30 run is unchanged: pull_all plus the SPY completed-close check
    (here start_date is after the completed session, so the check fires)."""
    _open_on_session_day(monkeypatch)
    monkeypatch.setattr(sched, "_pull_completed_spy_close",
                        lambda puller, start: (date(2026, 10, 16), {"outcome": "inserted", "rows_inserted": 1}))

    sched.run_daily_pulls(start_date="2026-10-17", slot="13:30")

    world.yf_puller.pull_all.assert_called_once_with(start_date="2026-10-17")


# ── 05:15Z morning slot ───────────────────────────────────────────────────


def _at(y: int, m: int, d: int, hh: int = 5, mm: int = 15) -> datetime:
    return datetime(y, m, d, hh, mm, tzinfo=timezone.utc)


@pytest.mark.parametrize("now, label", [
    (_at(2026, 10, 17), "Saturday captures Friday"),
    (_at(2026, 10, 20), "Tuesday captures Monday"),
    (_at(2026, 9, 9), "Wednesday: the post-Labor Day Tuesday was a session"),
    (_at(2026, 11, 28), "Saturday: the day-after-Thanksgiving Friday was a session"),
])
def test_morning_slot_runs_when_the_previous_day_was_a_session(world, now, label) -> None:
    sched.run_yfinance_morning_slot(start_date="2026-10-01", now_utc=now)

    world.yf_puller.pull_all.assert_called_once_with(start_date="2026-10-01")


@pytest.mark.parametrize("now, label", [
    (_at(2026, 10, 18), "Sunday: Saturday was no session"),
    (_at(2026, 10, 19), "Monday: Sunday was no session"),
    (_at(2026, 9, 8), "Tuesday after the Labor Day holiday Monday"),
    (_at(2026, 11, 27), "Friday after the Thanksgiving holiday Thursday"),
])
def test_morning_slot_skips_when_the_previous_day_was_not_a_session(world, now, label) -> None:
    sched.run_yfinance_morning_slot(start_date="2026-10-01", now_utc=now)

    world.yf_puller.pull_all.assert_not_called()
    world.yf_puller.pull_ticker.assert_not_called()


def test_morning_slot_only_runs_yfinance(world) -> None:
    sched.run_yfinance_morning_slot(start_date="2026-10-01", now_utc=_at(2026, 10, 17))

    world.yf_puller.pull_all.assert_called_once()
    for name, mod in world.modules.items():
        assert not mod.mock_calls, f"{name} must not run in the morning slot"


def test_morning_slot_runs_the_completed_spy_close_check(world) -> None:
    """Scheduler restarted on Saturday: start_date omits Friday, so the SPY
    close-only fetch for the completed Friday session rides with the pull."""
    sched.run_yfinance_morning_slot(start_date="2026-10-17", now_utc=_at(2026, 10, 17))

    world.yf_puller.pull_all.assert_called_once_with(start_date="2026-10-17")
    args, kwargs = world.yf_puller.pull_ticker.call_args
    assert args == ("SPY",)
    assert kwargs["start_date"] == date(2026, 10, 16)
    assert kwargs["end_date"] == date(2026, 10, 17)
    assert kwargs["only_fields"] == frozenset({"close"})


def test_morning_slot_lateness_guard_skips_a_late_release(world) -> None:
    # 07:00Z is 105 min after the 05:15 slot (limit 90): an earlier job held
    # the single-threaded loop, so this is skipped, not run as a catch-up.
    sched.run_yfinance_morning_slot(start_date="2026-10-01", now_utc=_at(2026, 10, 17, 7, 0))

    world.yf_puller.pull_all.assert_not_called()


def test_morning_slot_runs_when_released_within_the_lateness_limit(world) -> None:
    sched.run_yfinance_morning_slot(start_date="2026-10-01", now_utc=_at(2026, 10, 17, 6, 40))

    world.yf_puller.pull_all.assert_called_once()


def test_morning_slot_lateness_uses_the_utc_clock(world) -> None:
    """A non-UTC aware clock is converted: 03:30-04:00 (UTC-4 wall) == 07:30Z."""
    est = timezone(timedelta(hours=-4))
    sched.run_yfinance_morning_slot(
        start_date="2026-10-01", now_utc=datetime(2026, 10, 17, 3, 30, tzinfo=est),
    )

    world.yf_puller.pull_all.assert_not_called()


def test_morning_slot_uses_the_shared_lateness_limit() -> None:
    assert sched.DOMESTIC_SLOT_MAX_LATE_MINUTES == 90


# ── registration ──────────────────────────────────────────────────────────


class _SchedEngine:
    def dispose(self) -> None:
        pass


def _registered_jobs(monkeypatch: pytest.MonkeyPatch) -> schedule.Scheduler:
    fake_db = ModuleType("db")
    fake_db.get_engine = lambda: _SchedEngine()
    monkeypatch.setitem(sys.modules, "db", fake_db)
    jobs = schedule.Scheduler()
    monkeypatch.setattr(sched, "schedule", jobs)
    monkeypatch.setattr(sched.time, "sleep", lambda _s: None)

    def _stop() -> None:
        raise KeyboardInterrupt()

    monkeypatch.setattr(jobs, "run_pending", _stop)
    sched.start_scheduler()
    return jobs


def test_morning_slot_is_registered_daily_at_0515_utc(monkeypatch) -> None:
    jobs = _registered_jobs(monkeypatch)

    morning = [j for j in jobs.jobs if j.job_func.func is sched.run_yfinance_morning_slot]
    assert len(morning) == 1  # one every-day job; the function gates on the prior session
    job = morning[0]
    assert job.unit == "days" and job.interval == 1 and job.start_day is None
    assert job.at_time.strftime("%H:%M") == "05:15"
    assert job.at_time_zone.zone == "UTC"
    assert job.job_func.keywords["slot"] == "05:15"
    assert job.job_func.keywords["start_date"] == date.today().isoformat()


def test_domestic_slots_are_unchanged_and_the_morning_slot_is_not_one_of_them(monkeypatch) -> None:
    jobs = _registered_jobs(monkeypatch)

    domestic = [j for j in jobs.jobs if j.job_func.func is sched.run_daily_pulls]
    assert len(domestic) == 28
    assert {j.job_func.keywords["slot"] for j in domestic} == {"13:30", "16:00", "20:00", "22:00"}
    assert not hasattr(sched, "YFINANCE_SKIP_SLOTS")  # 13:30 keeps yfinance
