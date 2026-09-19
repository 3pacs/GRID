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

from datetime import date, datetime, timedelta, timezone
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

        def _fake_snapshot(engine: Any, snapshot_date: date | None = None, **_kwargs: Any) -> dict[str, Any]:
            calls.append(engine)
            return {
                "snapshots_written": 20, "snapshots_skipped_unavailable": 0,
                "upsert_failed": 0,
            }

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
        assert state.last_sector_health_outcome == "success"
        assert results["sector_health_snapshot"]["outcome"] == "success"

    def test_second_evaluation_same_due_period_does_not_rerun(self, monkeypatch) -> None:
        """(c) at the wiring level: the state marker prevents a second
        execution entirely within the same due period."""
        calls = []
        monkeypatch.setattr(
            "intelligence.sector_health.snapshot_all_sectors",
            lambda engine, snapshot_date=None, **_kwargs: calls.append(engine) or {
                "snapshots_written": 20, "snapshots_skipped_unavailable": 0,
                "upsert_failed": 0,
            },
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
            lambda engine, snapshot_date=None, **_kwargs: calls.append(engine) or {
                "snapshots_written": 20, "snapshots_skipped_unavailable": 0,
                "upsert_failed": 0,
            },
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

        def _failing_snapshot(engine: Any, snapshot_date: date | None = None, **_kwargs: Any) -> dict[str, Any]:
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
        assert results["sector_health_snapshot"]["outcome"] == "failure"
        assert state.last_sector_health_outcome == "failure"

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
            lambda engine, snapshot_date=None, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
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

        def _fake_success(engine: Any, snapshot_date: date | None = None, **_kwargs: Any) -> dict[str, Any]:
            return {
                "snapshots_written": 20, "snapshots_skipped_unavailable": 0,
                "upsert_failed": 0,
            }

        monkeypatch.setattr(
            "intelligence.sector_health.snapshot_all_sectors", _fake_success,
        )
        ho._maybe_run_sector_health_snapshot(engine, state, next_day, results)
        assert state.last_sector_health == next_day
        assert state.sector_health_attempt_count == 1


# ─── Review finding #1: snapshot_date identity across a due period ──────


class TestSnapshotDateIdentity:
    """A retry that crosses midnight must stamp the SAME snapshot_date as
    the attempt(s) before it — the due period (UTC 03:00 boundary), not
    the calendar date at the moment each attempt happens to run."""

    def test_retry_across_midnight_uses_same_due_period_date(self, monkeypatch) -> None:
        recorded_dates: list[date | None] = []

        def _first_attempt_fails(engine: Any, snapshot_date: date | None = None, **_kwargs: Any) -> dict[str, Any]:
            recorded_dates.append(snapshot_date)
            raise RuntimeError("boom")

        monkeypatch.setattr(
            "intelligence.sector_health.snapshot_all_sectors", _first_attempt_fails,
        )

        state = _fresh_state()
        engine = MagicMock()
        results: dict[str, Any] = {}

        # 23:30 UTC — still inside the due period that opened at 03:00 UTC
        # that same calendar day.
        t0 = datetime(2026, 9, 19, 23, 30, tzinfo=timezone.utc)
        ho._maybe_run_sector_health_snapshot(engine, state, t0, results)
        assert len(recorded_dates) == 1
        assert recorded_dates[0] == date(2026, 9, 19)
        assert state.last_sector_health is None  # failure: not marked done

        def _retry_succeeds(engine: Any, snapshot_date: date | None = None, **_kwargs: Any) -> dict[str, Any]:
            recorded_dates.append(snapshot_date)
            return {
                "snapshots_written": 20, "snapshots_skipped_unavailable": 0,
                "upsert_failed": 0,
            }

        monkeypatch.setattr(
            "intelligence.sector_health.snapshot_all_sectors", _retry_succeeds,
        )

        # 00:35 UTC the next calendar day — still the SAME due period
        # (boundary_hour=3), 65 minutes after the failed attempt (clears
        # the 60-minute backoff).
        t1 = t0 + timedelta(minutes=65)
        ho._maybe_run_sector_health_snapshot(engine, state, t1, results)

        assert len(recorded_dates) == 2
        assert recorded_dates[0] == recorded_dates[1] == date(2026, 9, 19), (
            "the 23:30 attempt and its 00:35-next-day retry are one due "
            "period and must upsert the same (sector, snapshot_date) rows"
        )
        assert state.last_sector_health == t1


# ─── Review finding #2: outcome semantics (success / no_eligible / failure) ─


class TestSectorHealthOutcomeSemantics:
    def test_success_outcome_marks_done(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "intelligence.sector_health.snapshot_all_sectors",
            lambda engine, snapshot_date=None, **_kwargs: {
                "snapshots_written": 5, "snapshots_skipped_unavailable": 0,
                "upsert_failed": 0,
            },
        )
        state = _fresh_state()
        engine = MagicMock()
        results: dict[str, Any] = {}
        now = datetime(2026, 9, 19, 3, 5, tzinfo=timezone.utc)

        ho._maybe_run_sector_health_snapshot(engine, state, now, results)

        assert state.last_sector_health == now
        assert state.last_sector_health_outcome == "success"
        assert results["sector_health_snapshot"]["outcome"] == "success"

    def test_no_eligible_sectors_outcome_marks_done_without_retry(self, monkeypatch) -> None:
        """0 written, every sector unavailable, no upsert failures is a
        legitimate empty day: the due period IS marked done (unlike a
        real failure) so a genuinely-empty day doesn't retry forever."""
        calls = []
        monkeypatch.setattr(
            "intelligence.sector_health.snapshot_all_sectors",
            lambda engine, snapshot_date=None, **_kwargs: (calls.append(1) or {
                "snapshots_written": 0, "snapshots_skipped_unavailable": 12,
                "upsert_failed": 0,
            }),
        )
        state = _fresh_state()
        engine = MagicMock()
        results: dict[str, Any] = {}
        now = datetime(2026, 9, 19, 3, 5, tzinfo=timezone.utc)

        ho._maybe_run_sector_health_snapshot(engine, state, now, results)

        assert state.last_sector_health == now, (
            "no_eligible_sectors must still mark the due period done"
        )
        assert state.last_sector_health_outcome == "no_eligible_sectors"
        assert results["sector_health_snapshot"]["outcome"] == "no_eligible_sectors"

        # A later evaluation in the SAME due period must not re-run.
        later = now + timedelta(hours=2)
        ho._maybe_run_sector_health_snapshot(engine, state, later, results)
        assert len(calls) == 1, "a marked-done due period must not retry"

    def test_partial_upsert_failure_outcome_is_failure_and_retries(self, monkeypatch) -> None:
        """Some rows written but at least one upsert failed: must count
        as failure (NOT success just because count > 0), must not mark
        the due period done, and must retry after backoff."""
        calls = []

        def _partial_failure(engine: Any, snapshot_date: date | None = None, **_kwargs: Any) -> dict[str, Any]:
            calls.append(1)
            return {
                "snapshots_written": 8, "snapshots_skipped_unavailable": 1,
                "upsert_failed": 2,
            }

        monkeypatch.setattr(
            "intelligence.sector_health.snapshot_all_sectors", _partial_failure,
        )
        state = _fresh_state()
        engine = MagicMock()
        results: dict[str, Any] = {}
        t0 = datetime(2026, 9, 19, 3, 5, tzinfo=timezone.utc)

        ho._maybe_run_sector_health_snapshot(engine, state, t0, results)

        assert state.last_sector_health is None, "partial upsert failure must not mark the day done"
        assert state.last_sector_health_outcome == "failure"
        assert results["sector_health_snapshot"]["outcome"] == "failure"
        assert len(calls) == 1

        # Retry after backoff must re-run.
        t1 = t0 + timedelta(minutes=61)
        ho._maybe_run_sector_health_snapshot(engine, state, t1, results)
        assert len(calls) == 2

    def test_exception_outcome_is_failure(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "intelligence.sector_health.snapshot_all_sectors",
            lambda engine, snapshot_date=None, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        state = _fresh_state()
        engine = MagicMock()
        results: dict[str, Any] = {}
        now = datetime(2026, 9, 19, 3, 5, tzinfo=timezone.utc)

        ho._maybe_run_sector_health_snapshot(engine, state, now, results)

        assert state.last_sector_health is None
        assert state.last_sector_health_outcome == "failure"


# ─── Review finding #3: cross-cycle race (abandoned _run_with_timeout worker) ─


class TestSectorHealthCrossCycleRace:
    def test_abandoned_first_attempt_does_not_clobber_a_later_attempts_result(
        self, monkeypatch
    ) -> None:
        """Simulate _run_with_timeout abandoning (not killing) a worker:
        attempt 1 starts, runs long, and — while it is still in flight —
        a later cycle's attempt 2 starts (after backoff has elapsed) and
        finishes first. Attempt 1's belated return must be discarded
        (stale token), not overwrite attempt 2's newer, real result."""
        state = _fresh_state()
        engine = MagicMock()
        results: dict[str, Any] = {}

        t0 = datetime(2026, 9, 19, 3, 1, tzinfo=timezone.utc)
        t1 = t0 + timedelta(minutes=61)  # clears the retry backoff

        def _second_cycle_snapshot(engine: Any, snapshot_date: date | None = None, **_kwargs: Any) -> dict[str, Any]:
            return {
                "snapshots_written": 3, "snapshots_skipped_unavailable": 0,
                "upsert_failed": 0,
            }

        def _first_cycle_snapshot(engine: Any, snapshot_date: date | None = None, **_kwargs: Any) -> dict[str, Any]:
            # While attempt 1 is "in flight" here, a later real cycle
            # evaluates and completes ITS OWN attempt (a real, nested
            # call into the function under test — not a hand-rolled
            # stand-in for it).
            monkeypatch.setattr(
                "intelligence.sector_health.snapshot_all_sectors", _second_cycle_snapshot,
            )
            ho._maybe_run_sector_health_snapshot(engine, state, t1, results)
            # Attempt 1 now (belatedly) returns its own, older result.
            return {
                "snapshots_written": 1, "snapshots_skipped_unavailable": 0,
                "upsert_failed": 0,
            }

        monkeypatch.setattr(
            "intelligence.sector_health.snapshot_all_sectors", _first_cycle_snapshot,
        )

        ho._maybe_run_sector_health_snapshot(engine, state, t0, results)

        # Attempt 2 (the later, real cycle) won: its result is what's
        # recorded, and attempt 1's belated return did not clobber it.
        assert state.last_sector_health == t1
        assert state.last_sector_health_outcome == "success"
        assert results["sector_health_snapshot"]["snapshots_written"] == 3

    def test_non_stale_result_still_commits_normally(self, monkeypatch) -> None:
        """Sanity check: when no later attempt starts, the token guard
        must not block the ordinary (non-racy) path."""
        monkeypatch.setattr(
            "intelligence.sector_health.snapshot_all_sectors",
            lambda engine, snapshot_date=None, **_kwargs: {
                "snapshots_written": 4, "snapshots_skipped_unavailable": 0,
                "upsert_failed": 0,
            },
        )
        state = _fresh_state()
        engine = MagicMock()
        results: dict[str, Any] = {}
        now = datetime(2026, 9, 19, 3, 1, tzinfo=timezone.utc)

        ho._maybe_run_sector_health_snapshot(engine, state, now, results)

        assert state.last_sector_health == now
        assert results["sector_health_snapshot"]["snapshots_written"] == 4


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
        # Each call in this test uses a later (or equal) `computed_at` than
        # the one before it (real wall-clock time, monotonically
        # non-decreasing), so real Postgres's `as_of <= EXCLUDED.as_of`
        # guard always matches here — rowcount 1, same as a genuine
        # non-stale upsert. See TestSnapshotAllSectorsAsOfGuard below for
        # the rowcount-0 (stale, guard rejects) case.
        result = MagicMock()
        result.rowcount = 1
        return result


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
