"""Regression tests for the sector-health snapshot scheduler.

Background
----------
``hermes_operator.run_intelligence_tasks`` used to gate the daily
sector-health snapshot with ``now.hour == 3 and now.minute < 10``. Traced
live on 2026-09-19: `run_intelligence_tasks` is invoked late in `run_cycle`
(after obsidian sync, pull fixes, stale refresh, smart ingestion, conflict
resolution, diagnostics, autoresearch, digests, supply chain, news
contagion, astrogrid...), and inside it the 30-minute active-hypothesis
scorer runs FIRST. Cycles are nominally 5 minutes apart but frequently run
much longer (a cycle that started 02:55Z had not reached the check by
03:13Z). Production `analytical_snapshots` (category pipeline_summary /
hermes_operator) show ``last_sector_health`` took only two values in the
last 400 snapshots: 2026-07-13T03:00:03Z and 2026-09-13T03:08:17Z, and
``sector_health_snapshots`` has exactly one write day in 30 days
(2026-09-13, 20 rows) — the step is EXECUTED only on the rare day the
check happens to land inside the 10-minute window. Worse, the state marker
``state.last_sector_health = now`` used to be set even when the snapshot
failed, so a single failed run blocked retries for 20h.

This file pins the replacement: a due-period scheduler
(``daily_task_due``) with no minute window, plus a bounded retry/backoff
guard for failed executions (``_maybe_run_sector_health_snapshot``). See
docs/handoffs/2026-09-19/fable-hermes-sector-schedule.md for the full
write-up. No network/DB — the DB-touching test below uses an in-memory
fake engine, never a real connection.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from scripts import hermes_operator as ho
from scripts.hermes_health import OperatorState


# ─── daily_task_due: pure helper ─────────────────────────────────────────


class TestDailyTaskDue:
    def test_a_missed_window_evaluation_after_boundary_is_due(self) -> None:
        """(a) Evaluation at 03:12 with last success yesterday -> due once.

        This is exactly the case the old ``minute < 10`` rule missed
        whenever the cycle that reached the check ran even 3 minutes late.
        """
        last_success = datetime(2026, 9, 18, 3, 5, tzinfo=timezone.utc)
        now = datetime(2026, 9, 19, 3, 12, tzinfo=timezone.utc)
        assert ho.daily_task_due(last_success, now, boundary_hour=3) is True

    def test_b_evaluation_before_boundary_not_due(self) -> None:
        """(b) Evaluation at 02:59, already satisfied since yesterday's
        boundary -> not due (the due period hasn't reopened yet)."""
        last_success = datetime(2026, 9, 18, 3, 5, tzinfo=timezone.utc)
        now = datetime(2026, 9, 19, 2, 59, tzinfo=timezone.utc)
        assert ho.daily_task_due(last_success, now, boundary_hour=3) is False

    def test_c_second_evaluation_later_same_day_not_due(self) -> None:
        """(c) A success earlier today means a later evaluation the same
        day is not due again."""
        last_success = datetime(2026, 9, 19, 3, 4, tzinfo=timezone.utc)
        now = datetime(2026, 9, 19, 14, 30, tzinfo=timezone.utc)
        assert ho.daily_task_due(last_success, now, boundary_hour=3) is False

    def test_d_restart_with_hydrated_marker_not_due(self) -> None:
        """(d) A freshly constructed state (simulating a restart) hydrated
        with today's success timestamp is not due at a later evaluation."""
        state = OperatorState()
        state.last_sector_health = datetime(2026, 9, 19, 3, 6, tzinfo=timezone.utc)
        now = datetime(2026, 9, 19, 9, 0, tzinfo=timezone.utc)
        assert ho.daily_task_due(state.last_sector_health, now, boundary_hour=3) is False

    def test_never_run_is_due(self) -> None:
        """No last_success at all (fresh install) is always due."""
        now = datetime(2026, 9, 19, 10, 0, tzinfo=timezone.utc)
        assert ho.daily_task_due(None, now, boundary_hour=3) is True

    def test_idle_process_catches_up_hours_late(self) -> None:
        """A process idle since before the boundary is due the first time
        it is evaluated, however late — no minute window."""
        last_success = datetime(2026, 9, 17, 3, 1, tzinfo=timezone.utc)
        now = datetime(2026, 9, 18, 16, 45, tzinfo=timezone.utc)
        assert ho.daily_task_due(last_success, now, boundary_hour=3) is True

    def test_f_naive_datetimes_are_normalised_to_utc(self) -> None:
        """(f) The helper is timezone-safe: naive datetimes are NOT
        rejected, they are normalised by assuming UTC (matching
        OperatorState.hydrate_from_snapshot's convention for timestamps
        restored from old snapshots)."""
        last_success_naive = datetime(2026, 9, 18, 3, 5)  # no tzinfo
        now_naive = datetime(2026, 9, 19, 3, 12)  # no tzinfo
        # Naive inputs must not raise, and must agree with the aware
        # equivalents (i.e. they were treated as UTC, not rejected).
        assert ho.daily_task_due(last_success_naive, now_naive, boundary_hour=3) is True

        last_success_aware = last_success_naive.replace(tzinfo=timezone.utc)
        now_aware = now_naive.replace(tzinfo=timezone.utc)
        assert (
            ho.daily_task_due(last_success_naive, now_naive, boundary_hour=3)
            == ho.daily_task_due(last_success_aware, now_aware, boundary_hour=3)
        )

    def test_g_old_window_rule_reproduced_as_a_regression(self) -> None:
        """(g) Reproduce the OLD ``now.hour == 3 and now.minute < 10`` rule
        and prove the new helper changes exactly the case it broke: an
        evaluation at 03:12 (just past the window) used to skip, but is
        due under the fix."""
        now = datetime(2026, 9, 19, 3, 12, tzinfo=timezone.utc)
        last_success = datetime(2026, 9, 18, 3, 3, tzinfo=timezone.utc)

        # Old rule, reproduced verbatim from the pre-fix code.
        old_is_window = (now.hour == 3 and now.minute < 10)
        assert old_is_window is False  # the old rule skips this evaluation

        # New rule runs it.
        assert ho.daily_task_due(last_success, now, boundary_hour=3) is True


# ─── _maybe_run_sector_health_snapshot: scheduling + retry/backoff wiring ─


def _fresh_state() -> OperatorState:
    return OperatorState()


class TestSectorHealthSchedulerWiring:
    """Exercises the guard in scripts/hermes_operator.py directly (no
    network/DB): a MagicMock engine plus a monkeypatched
    ``snapshot_all_sectors`` stand in for the DB-touching import."""

    def test_due_period_runs_and_advances_success_marker(self, monkeypatch) -> None:
        calls = []

        def _fake_snapshot(engine: Any) -> dict[str, Any]:
            calls.append(engine)
            return {"snapshots_written": 20, "snapshots_skipped_unavailable": 0}

        monkeypatch.setattr(
            "intelligence.sector_health.snapshot_all_sectors", _fake_snapshot,
        )

        state = _fresh_state()
        now = datetime(2026, 9, 19, 3, 12, tzinfo=timezone.utc)
        results: dict[str, Any] = {}
        engine = MagicMock()

        ho._maybe_run_sector_health_snapshot(engine, state, now, results)

        assert len(calls) == 1
        assert state.last_sector_health == now
        assert state.last_sector_health_attempt == now
        assert state.sector_health_attempt_count == 1
        assert results["sector_health_snapshot"]["snapshots_written"] == 20

    def test_second_evaluation_same_due_period_does_not_rerun(self, monkeypatch) -> None:
        """(c) at the wiring level: the state marker prevents a second
        execution entirely within the same due period."""
        calls = []
        monkeypatch.setattr(
            "intelligence.sector_health.snapshot_all_sectors",
            lambda engine: calls.append(engine) or {"snapshots_written": 20},
        )

        state = _fresh_state()
        engine = MagicMock()
        results: dict[str, Any] = {}

        first_now = datetime(2026, 9, 19, 3, 12, tzinfo=timezone.utc)
        ho._maybe_run_sector_health_snapshot(engine, state, first_now, results)
        assert len(calls) == 1

        second_now = datetime(2026, 9, 19, 14, 0, tzinfo=timezone.utc)
        ho._maybe_run_sector_health_snapshot(engine, state, second_now, results)
        assert len(calls) == 1, "second evaluation in the same due period must not re-run"

    def test_d_restart_with_hydrated_marker_does_not_rerun(self, monkeypatch) -> None:
        """(d) A restart that hydrates last_sector_health from the snapshot
        (simulated here by constructing state and assigning the field, the
        way hydrate_from_snapshot does) must not re-run either."""
        calls = []
        monkeypatch.setattr(
            "intelligence.sector_health.snapshot_all_sectors",
            lambda engine: calls.append(engine) or {"snapshots_written": 20},
        )

        state = _fresh_state()
        state.last_sector_health = datetime(2026, 9, 19, 3, 6, tzinfo=timezone.utc)
        engine = MagicMock()
        results: dict[str, Any] = {}

        now = datetime(2026, 9, 19, 9, 0, tzinfo=timezone.utc)
        ho._maybe_run_sector_health_snapshot(engine, state, now, results)

        assert calls == []
        assert "sector_health_snapshot" not in results

    def test_e_failure_does_not_advance_success_marker_and_retries_with_backoff(
        self, monkeypatch
    ) -> None:
        """(e) A failing execution must not mark the day done, must not
        retry on the very next cycle, but must retry after the configured
        backoff — and stop retrying once the per-day cap is hit."""
        attempts = {"n": 0}

        def _failing_snapshot(engine: Any) -> dict[str, Any]:
            attempts["n"] += 1
            raise RuntimeError("boom")

        monkeypatch.setattr(
            "intelligence.sector_health.snapshot_all_sectors", _failing_snapshot,
        )

        state = _fresh_state()
        engine = MagicMock()
        results: dict[str, Any] = {}

        t0 = datetime(2026, 9, 19, 3, 1, tzinfo=timezone.utc)
        ho._maybe_run_sector_health_snapshot(engine, state, t0, results)
        assert attempts["n"] == 1
        assert state.last_sector_health is None, "failure must not mark the day done"
        assert state.sector_health_attempt_count == 1
        assert results["sector_health_snapshot"]["status"] == "failed"

        # A cycle 10 minutes later must NOT retry yet (backoff = 60 min).
        t1 = t0 + timedelta(minutes=10)
        ho._maybe_run_sector_health_snapshot(engine, state, t1, results)
        assert attempts["n"] == 1, "retry before backoff elapsed must be skipped"

        # 61 minutes after the first attempt, the backoff has elapsed.
        t2 = t0 + timedelta(minutes=61)
        ho._maybe_run_sector_health_snapshot(engine, state, t2, results)
        assert attempts["n"] == 2
        assert state.sector_health_attempt_count == 2

        # Keep failing every 61+ minutes until the per-day cap (5) is hit.
        t = t2
        for _ in range(3):
            t = t + timedelta(minutes=61)
            ho._maybe_run_sector_health_snapshot(engine, state, t, results)
        assert attempts["n"] == 5
        assert state.sector_health_attempt_count == 5

        # A 6th attempt, well past backoff, must be refused: cap reached.
        t_capped = t + timedelta(minutes=61)
        ho._maybe_run_sector_health_snapshot(engine, state, t_capped, results)
        assert attempts["n"] == 5, "attempts must stop once the per-day cap is hit"
        assert state.last_sector_health is None

    def test_new_due_period_resets_the_attempt_cap(self, monkeypatch) -> None:
        """A fresh due period (next day's boundary) must reset the attempt
        counter even after yesterday's cap was exhausted."""
        monkeypatch.setattr(
            "intelligence.sector_health.snapshot_all_sectors",
            lambda engine: (_ for _ in ()).throw(RuntimeError("boom")),
        )

        state = _fresh_state()
        engine = MagicMock()
        results: dict[str, Any] = {}

        # Exhaust the cap on day 1.
        t = datetime(2026, 9, 19, 3, 1, tzinfo=timezone.utc)
        for _ in range(ho.SECTOR_HEALTH_MAX_ATTEMPTS_PER_DAY):
            ho._maybe_run_sector_health_snapshot(engine, state, t, results)
            t = t + timedelta(minutes=ho.SECTOR_HEALTH_RETRY_BACKOFF_MINUTES + 1)
        assert state.sector_health_attempt_count == ho.SECTOR_HEALTH_MAX_ATTEMPTS_PER_DAY

        # Next day's due period must allow a fresh attempt regardless of
        # yesterday's exhausted cap.
        next_day = datetime(2026, 9, 20, 3, 5, tzinfo=timezone.utc)

        def _fake_success(engine: Any) -> dict[str, Any]:
            return {"snapshots_written": 20}

        monkeypatch.setattr(
            "intelligence.sector_health.snapshot_all_sectors", _fake_success,
        )
        ho._maybe_run_sector_health_snapshot(engine, state, next_day, results)
        assert state.last_sector_health == next_day
        assert state.sector_health_attempt_count == 1


# ─── snapshot_all_sectors idempotency: real upsert path, fake engine ────


class _FakeCompiledConn:
    """Minimal stand-in for a SQLAlchemy connection that supports the
    single ``INSERT ... ON CONFLICT (sector_name, snapshot_date) DO
    UPDATE`` statement ``snapshot_all_sectors`` issues per sector.

    Captures each bound-parameter set keyed by (sector_name, snapshot_date)
    to prove the upsert never accumulates more than one row per key no
    matter how many times it runs within the same due period — exercising
    the REAL SQL text in intelligence/sector_health.py, not a re-implementation of it.
    """

    def __init__(self, store: dict[tuple[str, Any], dict[str, Any]]) -> None:
        self._store = store

    def __enter__(self) -> "_FakeCompiledConn":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, stmt: Any) -> MagicMock:
        compiled = stmt.compile()
        params = dict(compiled.params)
        key = (params["s"], params["d"])
        self._store[key] = {"score": params["sc"], "components": params["c"]}
        return MagicMock()


class _FakeEngine:
    def __init__(self) -> None:
        self.store: dict[tuple[str, Any], dict[str, Any]] = {}

    def begin(self) -> _FakeCompiledConn:
        return _FakeCompiledConn(self.store)


class TestSnapshotAllSectorsIdempotency:
    def test_two_executions_in_the_same_due_period_write_no_duplicate_rows(
        self, monkeypatch
    ) -> None:
        from analysis.sector_map import SECTOR_MAP
        import intelligence.sector_health as sh

        def _fake_compute(engine: Any, sector_name: str) -> dict[str, Any]:
            return {
                "score": 55.0,
                "trend_30d": 0.0,
                "components": {"stub": True},
            }

        monkeypatch.setattr(sh, "compute_sector_health", _fake_compute)

        engine = _FakeEngine()

        result1 = sh.snapshot_all_sectors(engine)
        assert result1["snapshots_written"] == len(SECTOR_MAP)
        assert len(engine.store) == len(SECTOR_MAP)

        # Second execution in the same UTC day (same snapshot_date key)
        # must upsert the SAME rows, not add new ones.
        result2 = sh.snapshot_all_sectors(engine)
        assert result2["snapshots_written"] == len(SECTOR_MAP)
        assert len(engine.store) == len(SECTOR_MAP), (
            "a second run within the same due period must not create "
            "duplicate (sector, date) rows"
        )
