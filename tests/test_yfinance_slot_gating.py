"""yfinance slot gating in the domestic scheduler.

Generic supported US-listed daily captures become POLICY_ELIGIBLE after the
next New York midnight. SPY close retains its post-UTC-day exception. The
05:15Z slot targets the prior day's session. The 13:30Z slot is 09:30 EDT /
08:30 EST; it and 16:00/20:00/22:00Z retain raw snapshots without promising an
opening print or provider finality. A daily-bar Open observed next morning
is not an opening-time observation. These tests also inspect receipt intent
separately from aggregate classification and the fallback's reported outcome.

Everything is a local fake: no network, no database, no email.
"""

from __future__ import annotations

import sys
import threading
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
        lambda start_date, yfinance_start=None, yfinance_slot=None:
            calls.append((start_date, yfinance_start, yfinance_slot)),
    )
    _open_on_session_day(monkeypatch)

    sched.run_daily_pulls(start_date="2026-10-17", slot="16:00")
    sched.run_daily_pulls(start_date="2026-10-17")

    # Date-floored pullers (FRED, ...) keep start_date; only yfinance gets the window.
    assert calls == [("2026-10-17", "2026-10-15", "16:00"), ("2026-10-17", None, None)]


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


@pytest.mark.parametrize("extra, runs", [
    (timedelta(0), True),
    (timedelta(microseconds=1), False),
])
def test_morning_slot_exact_lateness_boundary(world, extra, runs) -> None:
    sched.run_yfinance_morning_slot(
        now_utc=_at(2026, 10, 17, 6, 45) + extra,
    )

    assert world.yf_puller.pull_all.call_count == int(runs)
    if not runs:
        world.yf_puller.pull_ticker.assert_not_called()


@pytest.mark.parametrize("extra, runs", [
    (timedelta(0), True),
    (timedelta(microseconds=1), False),
])
def test_domestic_slot_exact_lateness_boundary(world, monkeypatch, extra, runs) -> None:
    _open_on_session_day(monkeypatch)
    monkeypatch.setattr(
        sched, "_slot_lateness", lambda slot: timedelta(minutes=90) + extra,
    )

    sched.run_daily_pulls(start_date="2026-10-01", slot="13:30")

    assert world.yf_puller.pull_all.call_count == int(runs)
    if not runs:
        world.yf_puller.pull_ticker.assert_not_called()


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


# Receipt intent is separate from qualification and from fallback write counts.
def _capture_yfinance_receipts(monkeypatch):
    contexts = []
    fake_context = ModuleType("ingestion.pull_context")

    def _context(engine, name):
        ctx = MagicMock(name=name)
        ctx.__enter__.return_value = ctx
        ctx.__exit__.return_value = False
        contexts.append((name, ctx))
        return ctx

    fake_context.PullContext = _context
    monkeypatch.setitem(sys.modules, "ingestion.pull_context", fake_context)
    fallback_receipt = MagicMock()
    monkeypatch.setattr(sched, "_write_job_pull_log", fallback_receipt)
    monkeypatch.setattr(sched, "_pull_completed_spy_close", lambda puller, start: None)
    return contexts, fallback_receipt


@pytest.mark.parametrize("slot", ["13:30", "16:00", "20:00", "22:00"])
def test_domestic_receipt_has_slot_raw_purpose_and_intended_session(world, monkeypatch, slot) -> None:
    contexts, fallback_receipt = _capture_yfinance_receipts(monkeypatch)
    _open_on_session_day(monkeypatch)

    sched.run_daily_pulls(start_date="2026-10-17", slot=slot)

    [(name, ctx)] = contexts
    assert name == "YFinance_Daily"
    [note] = ctx.set_note.call_args.args
    assert note.startswith("SUCCESS:")
    assert f"slot_utc={slot}" in note
    assert "purpose=same_day_raw_snapshot; target_session=2026-10-19" in note
    assert fallback_receipt.call_args.args[4:6] == ("SUCCESS", 0)
    fallback_note = fallback_receipt.call_args.args[6]
    assert fallback_note.startswith("SKIPPED:")
    assert f"slot_utc={slot}; purpose=completed_spy_close_fallback" in fallback_note
    assert "target_session=not_requested" in fallback_note


def test_morning_receipt_has_prior_session_purpose(world, monkeypatch) -> None:
    contexts, _ = _capture_yfinance_receipts(monkeypatch)

    sched.run_yfinance_morning_slot(now_utc=_at(2026, 10, 17))

    [(_, ctx)] = contexts
    [note] = ctx.set_note.call_args.args
    assert "slot_utc=05:15; purpose=completed_session_capture; target_session=2026-10-16" in note


@pytest.mark.parametrize("result, outcome", [
    ({"status": "SUCCESS", "rows_inserted": 0, "outcome": "duplicate_only"}, "NO_NEW_DATA"),
    ({"status": "SKIPPED", "rows_inserted": 0, "error": "HTTP 429"}, "SKIPPED"),
    ({"status": "PARTIAL", "rows_inserted": 1, "errors": ["raw refusal"]}, "PARTIAL"),
    ({"status": "FAILED", "rows_inserted": 0, "error": "HTTP 500"}, "FAILED"),
])
def test_receipt_keeps_original_classifier_marker_and_detail(world, monkeypatch, result, outcome) -> None:
    from ingestion.smart_scheduler import _classify_outcome

    contexts, _ = _capture_yfinance_receipts(monkeypatch)
    items = [{"ticker": "QQQ", **result}]
    expected_outcome, _, expected_note = _classify_outcome(items)
    assert expected_outcome == outcome

    _daily(world, items)

    [(_, ctx)] = contexts
    if outcome in {"PARTIAL", "FAILED"}:
        status, note = ctx.set_status.call_args.args
        assert status == outcome
    else:
        [note] = ctx.set_note.call_args.args
    assert note.startswith(f"{outcome}: {expected_note};")
    assert "slot_utc=manual; purpose=manual; target_session=unspecified" in note


@pytest.mark.parametrize("result, expected_outcome, expected_status", [
    ({"outcome": "inserted", "rows_inserted": 1}, "SUCCESS", "SUCCESS"),
    ({"outcome": "duplicate_only", "rows_inserted": 0}, "NO_NEW_DATA", "SUCCESS"),
    ({"status": "PARTIAL", "outcome": "error", "rows_inserted": 0,
      "errors": ["raw refusal"]}, "FAILED", "FAILED"),
])
def test_fallback_receipt_keeps_its_outcome_separate_from_main_freshness(
    world, monkeypatch, result, expected_outcome, expected_status,
) -> None:
    contexts, fallback_receipt = _capture_yfinance_receipts(monkeypatch)
    helper = MagicMock(return_value=(date(2026, 10, 16), result))
    monkeypatch.setattr(sched, "_pull_completed_spy_close", helper)

    _, advanced = _daily(world, _items())

    helper.assert_called_once_with(world.yf_puller, "2026-10-13")
    [(_, ctx)] = contexts
    ctx.record_rows.assert_called_once_with(347)  # main rows only, no fallback rows
    assert advanced  # existing main classification still controls freshness
    args = fallback_receipt.call_args.args
    assert args[1] == "YFinance_Completed_SPY_Close"
    assert args[4:6] == (expected_status, result["rows_inserted"])
    assert args[6].startswith(f"{expected_outcome}:")
    assert "purpose=completed_spy_close_fallback; target_session=2026-10-16" in args[6]


def test_fallback_exception_gets_its_own_failure_receipt_and_keeps_existing_handling(world, monkeypatch) -> None:
    _, fallback_receipt = _capture_yfinance_receipts(monkeypatch)
    helper = MagicMock(side_effect=RuntimeError("synthetic fallback failure"))
    monkeypatch.setattr(sched, "_pull_completed_spy_close", helper)

    _, advanced = _daily(world, _items())

    helper.assert_called_once()
    assert not advanced  # existing outer exception handler precedes freshness
    args = fallback_receipt.call_args.args
    assert args[4:6] == ("FAILED", None)  # exception does not prove zero committed writes
    assert "RuntimeError: synthetic fallback failure" in args[6]
    assert "purpose=completed_spy_close_fallback; target_session=unknown" in args[6]


def test_fallback_receipt_database_failure_does_not_change_fetch_or_freshness(world, monkeypatch) -> None:
    receipt_writer = sched._write_job_pull_log
    _capture_yfinance_receipts(monkeypatch)
    failed_receipt_engine = MagicMock()
    failed_receipt_engine.begin.side_effect = RuntimeError("synthetic receipt failure")
    monkeypatch.setattr(
        sched, "_write_job_pull_log",
        lambda engine, *args: receipt_writer(failed_receipt_engine, *args),
    )
    helper = MagicMock(return_value=(
        date(2026, 10, 16), {"outcome": "inserted", "rows_inserted": 1},
    ))
    monkeypatch.setattr(sched, "_pull_completed_spy_close", helper)

    _, advanced = _daily(world, _items())

    helper.assert_called_once_with(world.yf_puller, "2026-10-13")
    failed_receipt_engine.begin.assert_called_once()
    assert advanced


# Flow review corrections: synthetic counts, worker synchronization and pool calls.
def _flow_receipt_test_setup(monkeypatch, sync_all):
    engine = MagicMock()
    fake_db = ModuleType("db")
    fake_db.get_engine = lambda: engine
    monkeypatch.setitem(sys.modules, "db", fake_db)
    fake_flow = ModuleType("ingestion.flow_materializer")
    fake_flow.sync_all = sync_all
    monkeypatch.setitem(sys.modules, "ingestion.flow_materializer", fake_flow)
    lock_conn = MagicMock()
    monkeypatch.setattr(sched, "_try_flow_materialize_lock", lambda _: lock_conn)
    release = MagicMock()
    monkeypatch.setattr(sched, "_release_flow_materialize_lock", release)
    receipt = MagicMock()
    monkeypatch.setattr(sched, "_write_job_pull_log", receipt)
    monkeypatch.setattr(sched, "_flow_materialize_worker", None)
    monkeypatch.setattr(sched, "_flow_materialize_guard", threading.Lock())
    return lock_conn, release, receipt


def test_flow_timeout_rows_are_unknown_after_partial_commit_and_worker_continues(monkeypatch) -> None:
    committed = []  # synthetic separately committed table counts, not a database
    prefix_committed = threading.Event()
    continue_worker = threading.Event()

    def sync_all(_engine):
        committed.append(3)
        prefix_committed.set()
        if not continue_worker.wait(2):
            raise RuntimeError("synthetic worker release deadline")
        committed.append(4)
        return {"first": 3, "second": 4, "status": "SUCCESS", "errors": []}

    lock_conn, release, receipt = _flow_receipt_test_setup(monkeypatch, sync_all)
    real_thread = threading.Thread

    class _AfterPrefixThread(real_thread):
        def join(self, timeout=None):
            assert prefix_committed.wait(1), "synthetic prefix did not run"
            return super().join(timeout)

    monkeypatch.setattr(sched.threading, "Thread", _AfterPrefixThread)
    try:
        result = sched.run_flow_materialize_daily(
            now_utc=_at(2026, 10, 19, 23, 15), budget_s=0,
        )

        assert result["status"] == "FAILED" and "budget exceeded" in result["error"]
        assert committed == [3]
        assert sched._flow_materialize_worker.is_alive()
        release.assert_not_called()  # the lock stays with the continuing worker
        args = receipt.call_args.args
        assert args[4:6] == ("FAILED", None)
    finally:
        continue_worker.set()
        worker = sched._flow_materialize_worker
        if worker is not None:
            worker.join(1)
            assert not worker.is_alive()

    assert committed == [3, 4]
    release.assert_called_once_with(lock_conn)


def test_flow_worker_exception_rows_are_unknown_after_partial_commit(monkeypatch) -> None:
    committed = []

    def sync_all(_engine):
        committed.append(3)
        raise RuntimeError("synthetic failure after first table commit")

    lock_conn, release, receipt = _flow_receipt_test_setup(monkeypatch, sync_all)

    result = sched.run_flow_materialize_daily(
        now_utc=_at(2026, 10, 19, 23, 15), budget_s=1,
    )

    assert committed == [3]
    assert result["status"] == "FAILED" and "after first table commit" in result["error"]
    assert receipt.call_args.args[4:6] == ("FAILED", None)
    release.assert_called_once_with(lock_conn)
    assert not sched._flow_materialize_worker.is_alive()


@pytest.mark.parametrize("stage", ["execute", "scalar", "commit"])
def test_flow_uncertain_acquisition_invalidates_before_close_and_preserves_error(monkeypatch, stage) -> None:
    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = True
    failure = RuntimeError(f"synthetic acquire {stage}")
    failing_call = conn.execute.return_value.scalar if stage == "scalar" else getattr(conn, stage)
    failing_call.side_effect = failure
    engine = MagicMock()
    engine.connect.return_value = conn

    with pytest.raises(RuntimeError) as caught:
        sched._try_flow_materialize_lock(engine)

    assert caught.value is failure
    assert [call[0] for call in conn.mock_calls][-2:] == ["invalidate", "close"]
    conn.invalidate.assert_called_once_with()
    conn.close.assert_called_once_with()
    conn.detach.assert_not_called()


@pytest.mark.parametrize("stage", ["execute", "commit"])
def test_flow_uncertain_unlock_invalidates_before_close(monkeypatch, stage) -> None:
    conn = MagicMock()
    getattr(conn, stage).side_effect = RuntimeError(f"synthetic unlock {stage}")

    sched._release_flow_materialize_lock(conn)

    assert [call[0] for call in conn.mock_calls][-2:] == ["invalidate", "close"]
    conn.invalidate.assert_called_once_with()
    conn.close.assert_called_once_with()
    conn.detach.assert_not_called()


@pytest.mark.parametrize("acquired", [True, False])
def test_flow_known_acquisition_keeps_healthy_pool_behavior(acquired) -> None:
    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = acquired
    engine = MagicMock()
    engine.connect.return_value = conn

    result = sched._try_flow_materialize_lock(engine)

    assert result is (conn if acquired else None)
    conn.commit.assert_called_once_with()
    conn.invalidate.assert_not_called()
    conn.detach.assert_not_called()
    assert conn.close.call_count == int(not acquired)


def test_flow_healthy_unlock_closes_without_invalidating() -> None:
    conn = MagicMock()

    sched._release_flow_materialize_lock(conn)

    assert [call[0] for call in conn.mock_calls] == ["execute", "commit", "close"]
    conn.invalidate.assert_not_called()
    conn.detach.assert_not_called()


@pytest.mark.parametrize("phase", ["acquire", "release"])
@pytest.mark.parametrize("detach_fails", [False, True])
def test_flow_invalidation_failure_attempts_detach_and_reports_uncertain_discard(
    monkeypatch, phase, detach_fails,
) -> None:
    conn = MagicMock()
    failure = RuntimeError("synthetic lock commit failure")
    conn.commit.side_effect = failure
    conn.invalidate.side_effect = RuntimeError("synthetic invalidation failure")
    if detach_fails:
        conn.detach.side_effect = RuntimeError("synthetic detach failure")
    log = MagicMock()
    monkeypatch.setattr(sched, "log", log)
    if phase == "acquire":
        engine = MagicMock()
        engine.connect.return_value = conn
        with pytest.raises(RuntimeError) as caught:
            sched._try_flow_materialize_lock(engine)
        assert caught.value is failure
    else:
        sched._release_flow_materialize_lock(conn)

    assert [call[0] for call in conn.mock_calls][-3:] == ["invalidate", "detach", "close"]
    assert any("backend invalidation failed" in str(call) for call in log.error.call_args_list)
    assert any("discard is unconfirmed" in str(call) for call in log.error.call_args_list) == detach_fails
