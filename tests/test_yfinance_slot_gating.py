"""yfinance slot gating in the domestic scheduler.

The yfinance writer only marks a daily close POLICY_ELIGIBLE once the pull
happens after the next New York midnight, so the first qualified close for
session D used to land in the 13:30Z run on D+1. The scheduler now has a 05:15Z
yfinance-only morning slot. The 13:30Z slot keeps running yfinance: that row is
a same-day raw capture (09:32 ET in-progress bar, capture_state INCOMPLETE; its
open/close fields are provisional and not an exchange-opening print) and
16:00/20:00/22:00Z are later same-day snapshots (all with the #853 writer's
INCOMPLETE/UNKNOWN labels, none a close). The 05:15Z run is the separate
completed-close capture; its daily-bar Open is the official open seen next
morning, not an opening-time observation.

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
    # Monday 2026-10-19 13:30Z: the previous session was Friday 10-16 (every day
    # is "open" under the patch above, so the routine window is Sun 10-18 - 3d).
    monkeypatch.setattr(sched, "_scheduler_utc_now", lambda: _at(2026, 10, 19, 13, 30))


# ── every domestic slot (13:30Z same-day raw capture included) runs yfinance ──


@pytest.mark.parametrize("slot", ["13:30", "16:00", "20:00", "22:00"])
def test_every_domestic_slot_still_runs_yfinance(world, monkeypatch, slot) -> None:
    _open_on_session_day(monkeypatch)

    sched.run_daily_pulls(start_date="2026-10-01", slot=slot)

    # yfinance gets the call-time window, not the scheduler's start date.
    world.yf_puller.pull_all.assert_called_once_with(start_date="2026-10-15")
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
    """The 13:30 run still calls the completed-SPY-close check with its window."""
    _open_on_session_day(monkeypatch)
    seen: list = []
    monkeypatch.setattr(sched, "_pull_completed_spy_close",
                        lambda puller, start: seen.append(start) or None)

    sched.run_daily_pulls(start_date="2026-10-17", slot="13:30")

    assert seen == ["2026-10-15"]


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

    world.yf_puller.pull_all.assert_called_once()


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


def test_morning_slot_window_is_computed_at_call_time_not_from_the_restart_date(world) -> None:
    """D2 regression: scheduler (re)started Sat 10-17 -> ongoing_start=10-17.
    The 05:15Z run for Friday 10-16 must still request 10-16 (and an overlap)."""
    sched.run_yfinance_morning_slot(start_date="2026-10-17", now_utc=_at(2026, 10, 17))

    [call] = world.yf_puller.pull_all.call_args_list
    assert call.kwargs["start_date"] <= "2026-10-16"
    assert call.kwargs["start_date"] == "2026-10-13"  # 10-16 minus 3 calendar days
    # Friday is inside the main window, so the SPY close-only fetch is not needed.
    world.yf_puller.pull_ticker.assert_not_called()


@pytest.mark.parametrize("slot", ["13:30", "16:00", "20:00", "22:00"])
def test_domestic_slot_window_is_computed_at_call_time_not_from_the_restart_date(
    world, monkeypatch, slot,
) -> None:
    """D2 regression: ongoing_start=2026-10-17 on Mon 10-19 must still reach
    back to the last completed session (Fri 10-16) minus the overlap."""
    import ingestion.market_calendar as mc

    monkeypatch.setattr(sched, "_slot_lateness", lambda s, now=None: timedelta(0))
    monkeypatch.setattr(sched, "_scheduler_utc_now", lambda: _at(2026, 10, 19, 13, 30))
    # Weekday calendar (no holidays in range); "today" is always open so the
    # equity block runs whatever day the suite is executed on.
    monkeypatch.setattr(mc, "is_market_open", lambda d: d == date.today() or d.weekday() < 5)

    sched.run_daily_pulls(start_date="2026-10-17", slot=slot)

    [call] = world.yf_puller.pull_all.call_args_list
    assert call.kwargs["start_date"] <= "2026-10-16"
    assert call.kwargs["start_date"] == "2026-10-13"


def test_window_is_bounded_a_long_running_scheduler_does_not_regrow_it(world) -> None:
    sched.run_yfinance_morning_slot(start_date="2026-01-05", now_utc=_at(2026, 10, 20))

    [call] = world.yf_puller.pull_all.call_args_list
    assert call.kwargs["start_date"] == "2026-10-16"  # Mon 10-19 session minus 3 calendar days, not 01-05


def test_unslotted_manual_runs_keep_their_explicit_start_date(world, monkeypatch) -> None:
    """--historical / run_full_pipeline pass a deliberate start_date and no slot."""
    _open_on_session_day(monkeypatch)

    sched.run_daily_pulls(start_date="1990-01-01")

    world.yf_puller.pull_all.assert_called_once_with(start_date="1990-01-01")


def test_scheduled_slots_pass_both_the_scheduler_start_and_the_yfinance_window(world, monkeypatch) -> None:
    calls: list[tuple] = []
    monkeypatch.setattr(
        sched, "_run_equity_pulls",
        lambda start_date, yfinance_start=None: calls.append((start_date, yfinance_start)),
    )
    _open_on_session_day(monkeypatch)

    sched.run_daily_pulls(start_date="2026-10-17", slot="16:00")
    sched.run_daily_pulls(start_date="2026-10-17")

    # Date-floored pullers (FRED, ...) keep start_date; only yfinance gets the window.
    assert calls == [("2026-10-17", "2026-10-15"), ("2026-10-17", None)]


def test_morning_slot_lateness_guard_skips_a_late_release(world) -> None:
    # 07:00Z is 105 min after the 05:15 slot (limit 90): an earlier job held
    # the single-threaded loop, so this is skipped, not run as a catch-up.
    sched.run_yfinance_morning_slot(start_date="2026-10-01", now_utc=_at(2026, 10, 17, 7, 0))

    world.yf_puller.pull_all.assert_not_called()


def test_morning_slot_runs_when_released_within_the_lateness_limit(world) -> None:
    sched.run_yfinance_morning_slot(start_date="2026-10-01", now_utc=_at(2026, 10, 17, 6, 40))

    world.yf_puller.pull_all.assert_called_once()


def test_morning_slot_lateness_uses_the_utc_clock(world) -> None:
    """A non-UTC aware clock is converted before the lateness is computed."""
    edt = timezone(timedelta(hours=-4))
    # 03:30 wall (UTC-4) == 07:30Z: 135 min late -> skipped.
    sched.run_yfinance_morning_slot(
        start_date="2026-10-01", now_utc=datetime(2026, 10, 17, 3, 30, tzinfo=edt),
    )
    world.yf_puller.pull_all.assert_not_called()
    # 01:30 wall (UTC-4) == 05:30Z: only 15 min late -> runs. Computed on the
    # wall clock instead, this would be 20h15 after the previous 05:15 and skip.
    sched.run_yfinance_morning_slot(
        start_date="2026-10-01", now_utc=datetime(2026, 10, 17, 1, 30, tzinfo=edt),
    )
    world.yf_puller.pull_all.assert_called_once()


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


# ── D3: the expected SPY provisional-close refusal is not an item failure ──

REFUSAL = "Daily captures refused: incomplete or unknown session policy"


def _items(n_ok: int = 69, spy: dict | None = None) -> list[dict]:
    items = [
        {"ticker": f"T{i}", "status": "SUCCESS", "rows_inserted": 5, "errors": [],
         "outcome": "inserted", "daily_policy_refusals": 0, "raw_value_refusals": 0}
        for i in range(n_ok)
    ]
    items.append(spy or {
        "ticker": "SPY", "status": "PARTIAL", "rows_inserted": 2, "errors": [REFUSAL],
        "outcome": "inserted", "daily_policy_refusals": 1, "raw_value_refusals": 0})
    return items


def _daily(world, items: list[dict]):
    world.yf_puller.pull_all.return_value = items
    engine = MagicMock()
    sys.modules["db"].get_engine = lambda: engine
    sched._run_yfinance_daily("2026-10-13")
    sql = [str(c.args[0]) for c in engine.begin.return_value.__enter__.return_value.execute.call_args_list]
    return engine, any("UPDATE source_catalog SET last_pull_at" in q for q in sql)


def test_only_the_expected_spy_refusal_counts_as_success_and_advances_last_pull_at(world) -> None:
    _engine, advanced = _daily(world, _items())

    assert advanced


def test_expected_spy_refusal_with_zero_rows_is_still_classified_without_failing(world) -> None:
    spy = {"ticker": "SPY", "status": "PARTIAL", "rows_inserted": 0, "errors": [REFUSAL],
           "outcome": "no_data", "daily_policy_refusals": 1, "raw_value_refusals": 0}

    normalized = sched._normalize_expected_spy_refusal(_items(spy=spy))

    assert normalized[-1]["status"] == "SUCCESS" and normalized[-1]["outcome"] == "duplicate_only"


@pytest.mark.parametrize("spy", [
    {"errors": [REFUSAL, "Invalid raw numeric captures refused"], "raw_value_refusals": 1},
    {"errors": [REFUSAL, "boom"]},
    {"errors": ["HTTP 500"], "daily_policy_refusals": 0, "outcome": "error", "status": "SKIPPED"},
    {"ticker": "QQQ"},  # the refusal is only expected for SPY
])
def test_any_other_problem_keeps_the_run_partial(world, spy) -> None:
    base = {"ticker": "SPY", "status": "PARTIAL", "rows_inserted": 2, "errors": [REFUSAL],
            "outcome": "inserted", "daily_policy_refusals": 1, "raw_value_refusals": 0}
    _engine, advanced = _daily(world, _items(spy={**base, **spy}))

    assert not advanced


def test_another_ticker_failing_alongside_the_refusal_keeps_the_run_partial(world) -> None:
    items = _items()
    items[3] = {"ticker": "T3", "status": "SKIPPED", "rows_inserted": 0, "errors": ["HTTP 429"],
                "outcome": "error", "daily_policy_refusals": 0, "raw_value_refusals": 0}

    _engine, advanced = _daily(world, items)

    assert not advanced


def test_per_item_errors_are_logged_so_n_of_70_is_reconstructible(world, monkeypatch) -> None:
    log = MagicMock()
    monkeypatch.setattr(sched, "log", log)
    items = _items()
    items[3] = {"ticker": "T3", "status": "SKIPPED", "rows_inserted": 0, "errors": ["HTTP 429 rate limited"],
                "outcome": "error", "daily_policy_refusals": 0, "raw_value_refusals": 0}
    items[4] = {"ticker": "T4", "status": "SUCCESS", "rows_inserted": 0, "errors": [],
                "outcome": "no_data", "daily_policy_refusals": 0, "raw_value_refusals": 0}

    _daily(world, items)

    text_ = " ".join(str(c) for c in log.warning.call_args_list)
    assert "T3" in text_ and "HTTP 429 rate limited" in text_ and "T4" in text_
    [issue_call] = [c for c in log.warning.call_args_list if "not clean" in c.args[0]]
    assert (issue_call.kwargs["n"], issue_call.kwargs["total"]) == (2, 70)  # "2 of 70"
    # The expected SPY refusal is reported separately (info), not as a failure.
    assert "SPY" not in text_
    assert any("expected" in str(c) and "SPY" in str(c) for c in log.info.call_args_list)


# ── drift guard and the non-None completed-SPY-close path ─────────────────


def test_expected_refusal_string_matches_the_one_the_writer_emits() -> None:
    """If yfinance_pull.py rewords its refusal error, the normalizer would
    silently stop matching and every run would go PARTIAL again."""
    from pathlib import Path

    source = (Path(__file__).resolve().parents[1] / "ingestion" / "yfinance_pull.py").read_text()
    assert f'"{sched._YF_EXPECTED_REFUSAL}"' in source
    assert sched._YF_EXPECTED_REFUSAL == REFUSAL


def test_completed_spy_close_helper_fetches_close_only_for_the_completed_session(world) -> None:
    """A start_date after the completed session (e.g. a manual caller) still gets
    the SPY close-only request for the omitted session."""
    out = sched._pull_completed_spy_close(
        world.yf_puller, "2026-10-17", now_utc=_at(2026, 10, 17))

    assert out == (date(2026, 10, 16), {"outcome": "inserted", "rows_inserted": 1})
    args, kwargs = world.yf_puller.pull_ticker.call_args
    assert args == ("SPY",)
    assert (kwargs["start_date"], kwargs["end_date"]) == (date(2026, 10, 16), date(2026, 10, 17))
    assert kwargs["only_fields"] == frozenset({"close"})


def test_yfinance_daily_logs_the_completed_spy_close_when_the_helper_returns_one(world, monkeypatch) -> None:
    log = MagicMock()
    monkeypatch.setattr(sched, "log", log)
    monkeypatch.setattr(
        sched, "_pull_completed_spy_close",
        lambda puller, start: (date(2026, 10, 16), {"outcome": "inserted", "rows_inserted": 1}),
    )

    _daily(world, _items())

    assert any("SPY completed-close check" in c.args[0] and c.kwargs["d"] == date(2026, 10, 16)
               for c in log.info.call_args_list)
