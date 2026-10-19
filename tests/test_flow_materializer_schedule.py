"""Daily flow_materializer.sync_all job on grid-scheduler (Hermes-independent).

All fakes: no database, no provider. The receipt writer is also exercised
against an in-memory SQLite pull_log.
"""

from __future__ import annotations

import sys
import threading
from datetime import datetime, time, timedelta, timezone
from types import ModuleType
from unittest.mock import MagicMock

import pytest
import schedule
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

import ingestion.scheduler as sched

SLOT = "23:15"


def _at(hh: int, mm: int, day: int = 20) -> datetime:
    return datetime(2026, 10, day, hh, mm, tzinfo=timezone.utc)


class _World:
    def __init__(self) -> None:
        self.engine = MagicMock(name="engine")
        self.receipts: list[tuple] = []
        self.sync_all = MagicMock(name="sync_all")


@pytest.fixture
def world(monkeypatch: pytest.MonkeyPatch) -> _World:
    w = _World()
    fake_db = ModuleType("db")
    fake_db.get_engine = lambda: w.engine
    monkeypatch.setitem(sys.modules, "db", fake_db)
    fake_fm = ModuleType("ingestion.flow_materializer")
    fake_fm.sync_all = lambda engine: w.sync_all(engine)
    monkeypatch.setitem(sys.modules, "ingestion.flow_materializer", fake_fm)
    monkeypatch.setattr(
        sched, "_write_job_pull_log",
        lambda engine, name, sid, started, status, rows, err: w.receipts.append(
            (name, status, rows, err)),
    )
    monkeypatch.setattr(sched, "_flow_materialize_worker", None)
    return w


SUCCESS_SUMMARY = {
    "status": "SUCCESS", "errors": [],
    "insider_trades": 10, "congressional_trades": 4, "dark_pool_weekly": 3,
    "etf_flows": 7, "junction_point_readings": 2,
}


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


def test_job_is_registered_weekdays_at_2315_utc(monkeypatch) -> None:
    jobs = _registered_jobs(monkeypatch)

    flow = [j for j in jobs.jobs if j.job_func.func is sched.run_flow_materialize_daily]
    assert sorted(j.start_day for j in flow) == sorted(
        ["monday", "tuesday", "wednesday", "thursday", "friday"])
    for j in flow:
        assert j.at_time.strftime("%H:%M") == SLOT == sched.FLOW_MATERIALIZE_SLOT_UTC
        assert j.at_time_zone.zone == "UTC"
        assert j.job_func.keywords == {"slot": SLOT}


def test_slot_is_after_the_post_close_pulls_and_outside_the_backup_window() -> None:
    slot = time.fromisoformat(sched.FLOW_MATERIALIZE_SLOT_UTC)
    assert slot > time(22, 0)                              # after the last domestic slot
    assert not (time(3, 30) <= slot <= time(10, 30))       # pg_dump window (UTC)
    # Even a maximally late release (90 min) stays out of it.
    late = (datetime.combine(datetime(2026, 10, 20), slot)
            + timedelta(minutes=sched.DOMESTIC_SLOT_MAX_LATE_MINUTES)).time()
    assert late < time(3, 30) or late > time(10, 30)


def test_budget_matches_the_hermes_sql_task_budget() -> None:
    assert sched.FLOW_MATERIALIZE_BUDGET_S == 60


# ── lateness ──────────────────────────────────────────────────────────────


def test_grossly_late_release_is_skipped(world) -> None:
    # 02:00Z next day: 165 min after the 23:15 slot (limit 90).
    assert sched.run_flow_materialize_daily(now_utc=_at(2, 0, 21)) is None
    world.sync_all.assert_not_called()
    assert world.receipts == []


def test_release_within_the_limit_runs(world) -> None:
    world.sync_all.return_value = dict(SUCCESS_SUMMARY)

    out = sched.run_flow_materialize_daily(now_utc=_at(23, 40))

    assert out == SUCCESS_SUMMARY
    world.sync_all.assert_called_once_with(world.engine)


# ── receipt ───────────────────────────────────────────────────────────────


def test_success_writes_a_receipt_with_per_table_counts(world) -> None:
    world.sync_all.return_value = dict(SUCCESS_SUMMARY)

    sched.run_flow_materialize_daily(now_utc=_at(23, 15))

    [(name, status, rows, detail)] = world.receipts
    assert name == "FlowMaterialize_Daily"
    assert (status, rows) == ("SUCCESS", 26)
    for part in ("insider_trades=10", "congressional_trades=4", "dark_pool_weekly=3",
                 "etf_flows=7", "junction_point_readings=2"):
        assert part in detail


def test_partial_receipt_carries_the_table_errors(world) -> None:
    world.sync_all.return_value = {
        "status": "PARTIAL", "insider_trades": 5, "congressional_trades": 0,
        "dark_pool_weekly": 3, "etf_flows": 7, "junction_point_readings": 2,
        "errors": [{"table": "congressional_trades", "error": "boom " * 40}],
    }

    sched.run_flow_materialize_daily(now_utc=_at(23, 15))

    [(_, status, rows, detail)] = world.receipts
    assert (status, rows) == ("PARTIAL", 17)
    assert "congressional_trades=0" in detail
    assert "errors: congressional_trades: boom" in detail


def test_unknown_summary_status_is_receipted_failed(world) -> None:
    world.sync_all.return_value = {"status": "WEIRD", "errors": []}

    sched.run_flow_materialize_daily(now_utc=_at(23, 15))

    assert world.receipts[0][1] == "FAILED"


def test_receipt_row_lands_in_pull_log(monkeypatch) -> None:
    engine = create_engine("sqlite://", poolclass=StaticPool,
                           connect_args={"check_same_thread": False})
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE pull_log (id INTEGER PRIMARY KEY, puller_name TEXT, "
            "source_id INTEGER, started_at TEXT, completed_at TEXT, status TEXT, "
            "rows_inserted INTEGER, error_message TEXT, node_name TEXT)"))
    fake_db = ModuleType("db")
    fake_db.get_engine = lambda: engine
    monkeypatch.setitem(sys.modules, "db", fake_db)
    fake_fm = ModuleType("ingestion.flow_materializer")
    fake_fm.sync_all = lambda _engine: dict(SUCCESS_SUMMARY)
    monkeypatch.setitem(sys.modules, "ingestion.flow_materializer", fake_fm)
    monkeypatch.setattr(sched, "_flow_materialize_worker", None)

    sched.run_flow_materialize_daily(now_utc=_at(23, 15))

    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT puller_name, status, rows_inserted, error_message FROM pull_log")).one()
    assert row[:3] == ("FlowMaterialize_Daily", "SUCCESS", 26)
    assert "etf_flows=7" in row[3]


# ── failures are logged, not raised ───────────────────────────────────────


def test_sync_all_exception_is_logged_and_receipted_not_raised(world, monkeypatch) -> None:
    world.sync_all.side_effect = RuntimeError("db down")
    log = MagicMock()
    monkeypatch.setattr(sched, "log", log)

    out = sched.run_flow_materialize_daily(now_utc=_at(23, 15))

    assert out == {"status": "FAILED", "error": "RuntimeError: db down"}
    assert world.receipts == [("FlowMaterialize_Daily", "FAILED", 0, "RuntimeError: db down")]
    assert log.error.called


def test_missing_engine_is_logged_not_raised(world, monkeypatch) -> None:
    def _boom():
        raise RuntimeError("no engine")

    sys.modules["db"].get_engine = _boom
    out = sched.run_flow_materialize_daily(now_utc=_at(23, 15))

    assert out["status"] == "FAILED"
    world.sync_all.assert_not_called()


def test_budget_overrun_is_receipted_failed_and_blocks_a_second_start(world) -> None:
    release = threading.Event()
    world.sync_all.side_effect = lambda _engine: release.wait(10) or dict(SUCCESS_SUMMARY)

    try:
        out = sched.run_flow_materialize_daily(now_utc=_at(23, 15), budget_s=0.05)
        assert out["status"] == "FAILED" and "budget exceeded" in out["error"]
        assert world.receipts[0][:3] == ("FlowMaterialize_Daily", "FAILED", 0)
        # The worker is still alive: the next slot must not stack another run.
        assert sched.run_flow_materialize_daily(now_utc=_at(23, 15), budget_s=0.05) is None
        assert world.sync_all.call_count == 1
    finally:
        release.set()
        sched._flow_materialize_worker.join(5)
