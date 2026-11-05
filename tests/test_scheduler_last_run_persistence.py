"""ingestion.scheduler run dates survive a restart, and the monthly group
starts after the 05:15Z yfinance morning slot.

Regression for 2026-11-05: ``_last_run`` was in-memory only, so each of the
scheduler's frequent restarts forgot that the monthly group had already run.
The group (Comtrade + ICIJ_Full, ~2h) re-ran at 04:00Z and held the single
scheduler thread past the 05:15Z slot, so no POLICY_ELIGIBLE close was
captured for 2026-11-04 and the market diary found "no close for date".

Also: the ICIJ_Full puller in that group bypassed the offshore_leaks hold.
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime, time, timedelta
from types import ModuleType

import pytest
import schedule

import ingestion.scheduler as sched


@pytest.fixture(autouse=True)
def _fresh_run_dates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sched, "_last_run", {})


def test_mark_run_persists_and_a_restart_still_skips_the_month() -> None:
    assert sched._should_run("monthly_intl", "month")
    sched._mark_run("monthly_intl")

    stored = json.loads(sched._last_run_path().read_text())
    assert stored == {"monthly_intl": date.today().isoformat()}

    # A restarted process starts with an empty dict and reloads from disk.
    sched._last_run.clear()
    assert sched._should_run("monthly_intl", "month")
    sched._last_run.update(sched._load_last_run())
    assert not sched._should_run("monthly_intl", "month")


def test_previous_month_on_disk_still_allows_this_months_run() -> None:
    last_month = (date.today().replace(day=1) - timedelta(days=1)).isoformat()
    sched._last_run_path().write_text(json.dumps({"monthly_intl": last_month}))

    sched._last_run.update(sched._load_last_run())

    assert sched._should_run("monthly_intl", "month")


def test_missing_file_means_never_ran() -> None:
    assert not sched._last_run_path().exists()
    assert sched._load_last_run() == {}


@pytest.mark.parametrize(
    "content",
    ["{not json", "[1, 2]", '"2026-11-05"'],
    ids=["corrupt", "list", "string"],
)
def test_unreadable_file_is_ignored_not_raised(content: str) -> None:
    sched._last_run_path().write_text(content)
    assert sched._load_last_run() == {}


def test_invalid_entries_are_dropped_and_valid_ones_kept() -> None:
    sched._last_run_path().write_text(json.dumps({
        "monthly_intl": "2026-11-02",
        "monthly_bls": "not-a-date",
        "annual_intl": 2026,
    }))
    assert sched._load_last_run() == {"monthly_intl": "2026-11-02"}


def test_unwritable_path_logs_and_keeps_the_in_memory_mark(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    blocker = tmp_path / "a-file"
    blocker.write_text("")
    monkeypatch.setenv(sched._LAST_RUN_PATH_ENV, str(blocker / "state.json"))

    sched._mark_run("monthly_intl")  # parent is a regular file: OSError, swallowed

    assert sched._last_run == {"monthly_intl": date.today().isoformat()}
    assert not sched._should_run("monthly_intl", "month")


def test_write_is_atomic_and_leaves_no_temp_file() -> None:
    sched._mark_run("monthly_intl")
    sched._mark_run("monthly_bls")

    path = sched._last_run_path()
    assert sorted(p.name for p in path.parent.iterdir()) == [path.name]
    assert json.loads(path.read_text()).keys() == {"monthly_intl", "monthly_bls"}


def test_default_path_is_under_the_users_state_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(sched._LAST_RUN_PATH_ENV, raising=False)
    path = sched._last_run_path()
    assert path.parts[-4:] == (".local", "state", "grid", "scheduler_last_run.json")


# ── registration ──────────────────────────────────────────────────────────


class _SchedEngine:
    def dispose(self) -> None:
        pass


def _start(monkeypatch: pytest.MonkeyPatch) -> schedule.Scheduler:
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


def _job_named(jobs: schedule.Scheduler, name: str) -> schedule.Job:
    found = [j for j in jobs.jobs if getattr(j.job_func.func, "__name__", "") == name]
    assert len(found) == 1, f"{name}: {len(found)} registrations"
    return found[0]


def test_start_scheduler_restores_persisted_run_dates(monkeypatch: pytest.MonkeyPatch) -> None:
    today = date.today().isoformat()
    sched._last_run_path().write_text(json.dumps({"monthly_intl": today}))

    _start(monkeypatch)

    assert sched._last_run["monthly_intl"] == today
    assert not sched._should_run("monthly_intl", "month")


def test_extended_monthly_and_annual_start_after_the_morning_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    jobs = _start(monkeypatch)
    morning = _job_named(jobs, "run_yfinance_morning_slot")
    monthly = _job_named(jobs, "_monthly_extended")
    annual = _job_named(jobs, "_annual_extended")

    for job, slot in ((monthly, sched.EXTENDED_MONTHLY_SLOT_UTC),
                      (annual, sched.EXTENDED_ANNUAL_SLOT_UTC)):
        assert job.at_time.strftime("%H:%M") == slot
        assert job.at_time_zone.zone == "UTC"
        assert job.at_time > morning.at_time

    # The old 04:00 start could hold the loop past the slot's lateness limit.
    slot = time.fromisoformat(sched.YFINANCE_MORNING_SLOT_UTC)
    latest_ok = (datetime.combine(date(2026, 11, 5), slot)
                 + timedelta(minutes=sched.DOMESTIC_SLOT_MAX_LATE_MINUTES)).time()
    assert time.fromisoformat(sched.EXTENDED_MONTHLY_SLOT_UTC) > slot
    assert latest_ok > slot


# ── ICIJ_Full honours the offshore_leaks hold ─────────────────────────────


def _monthly_puller_names(monkeypatch: pytest.MonkeyPatch, hold: str | None) -> list[str]:
    import ingestion.altdata.icij_puller as icij
    import ingestion.smart_scheduler as smart
    from unittest.mock import MagicMock

    class _FakeICIJ:
        def __init__(self, _engine: object) -> None:
            pass

    monkeypatch.setattr(icij, "ICIJPuller", _FakeICIJ)
    monkeypatch.setattr(smart, "hold_reason_for", lambda name: hold if name == "offshore_leaks" else None)
    return [p[0] for p in sched._get_pullers_for_group("monthly", MagicMock(), {})]


def test_icij_full_is_not_scheduled_while_offshore_leaks_is_held(monkeypatch: pytest.MonkeyPatch) -> None:
    names = _monthly_puller_names(monkeypatch, "Match quality under review")
    assert "ICIJ_Full" not in names
    assert "ICIJ_Offshore" not in names


def test_icij_full_is_scheduled_once_the_hold_is_lifted(monkeypatch: pytest.MonkeyPatch) -> None:
    assert "ICIJ_Full" in _monthly_puller_names(monkeypatch, None)
