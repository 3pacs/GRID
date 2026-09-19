"""Tests for the sector-health snapshot's own dispatch step and its DB-row
protections, split out of ``run_intelligence_tasks`` on 2026-09-19.

Background
----------
Production traces (journal 2026-09-19 03:48, 04:35, 05:02, 06:05 UTC, and
the May-2026 log) show ``run_intelligence_tasks`` timing out at its 900s
budget on EVERY observed cycle: the daily-due block inside it runs with
``catch_up=True`` every cycle because the step is abandoned before
``state.last_daily_intel = now`` is ever reached, so nothing placed after
that point — including the old sector-health call — was ever reached
either. This is a confirmed *current* blocker, separate from (and in
addition to) the already-merged due-period scheduler fix pinned by
tests/test_hermes_sector_schedule.py (which fixed a 10-minute evaluation
window, a different defect).

This file covers the fix: sector-health now dispatches as its own
``run_cycle`` step, with its own short timeout
(``SECTOR_HEALTH_TIMEOUT_SECONDS``), ahead of ``intelligence_tasks``, and
the DB write path (``intelligence.sector_health.snapshot_all_sectors``) now
guards against an abandoned worker overwriting a newer row.

See docs/handoffs/2026-09-19/fable-hermes-sector-schedule.md, "Parent-
timeout blocker and own-step fix", for the full write-up.

No network/DB — MagicMock/fake engines and monkeypatched
``snapshot_all_sectors``/``run_intelligence_tasks`` throughout.
"""
from __future__ import annotations

import inspect
import threading
import time
from datetime import date, datetime, timezone
from typing import Any, Self
from unittest.mock import MagicMock

from scripts import hermes_operator as ho
from scripts.hermes_health import OperatorState


def _fresh_state() -> OperatorState:
    return OperatorState()


# ─── 1. Starvation: sector step must run even when intelligence_tasks blocks ─


class TestSectorStepIsNotStarvedByIntelligenceTasks:
    def test_sector_step_runs_even_when_intelligence_tasks_blocks(self, monkeypatch) -> None:
        """The whole point of the split: a slow/hung intelligence_tasks
        step must not prevent the sector-health step from running. Exercises
        the REAL _run_with_timeout for both steps (not a monkeypatched
        stand-in for it) so the timeout plumbing itself is under test."""
        calls: list[int] = []

        def _fast_fake_snapshot(engine: Any, snapshot_date: date | None = None, **_kwargs: Any) -> dict[str, Any]:
            calls.append(1)
            return {
                "snapshots_written": 20, "snapshots_skipped_unavailable": 0,
                "upsert_failed": 0,
            }

        monkeypatch.setattr(
            "intelligence.sector_health.snapshot_all_sectors", _fast_fake_snapshot,
        )

        def _blocking_intelligence_tasks(engine: Any, state: Any, dry_run: bool = False) -> dict[str, Any]:
            # Simulates production: intelligence_tasks runs far longer than
            # its patched budget. Bounded to a couple of seconds (not the
            # real 900s) — kept short and on a daemon-safe timer so the
            # orphaned worker _run_with_timeout abandons on timeout cannot
            # meaningfully stall the test process at interpreter shutdown
            # (ThreadPoolExecutor workers are non-daemon and get joined by
            # concurrent.futures.thread._python_exit).
            threading.Event().wait(2)
            return {"skipped": "never reached — orphaned by the timeout"}

        monkeypatch.setattr(ho, "run_intelligence_tasks", _blocking_intelligence_tasks)
        monkeypatch.setattr(ho, "INTELLIGENCE_TASKS_TIMEOUT_SECONDS", 1)

        state = _fresh_state()
        engine = MagicMock()
        cycle_result: dict[str, Any] = {}

        started = time.monotonic()
        ho._run_sector_and_intelligence_steps(engine, state, False, cycle_result)
        elapsed = time.monotonic() - started

        assert len(calls) == 1, (
            "sector-health must execute even though intelligence_tasks blocks"
        )
        assert state.last_sector_health is not None
        assert elapsed < 5, (
            f"orchestration helper took {elapsed:.1f}s — intelligence_tasks "
            "starved it (this is exactly the production bug)"
        )
        assert cycle_result["intelligence"] == {"timeout": True}


class TestSectorDispatchPrecedesIntelligenceDispatch:
    def test_source_order_sector_before_intelligence(self) -> None:
        src = inspect.getsource(ho._run_sector_and_intelligence_steps)
        sector_pos = src.index('current_step = "sector_health"')
        intel_pos = src.index('current_step = "intelligence_tasks"')
        assert sector_pos < intel_pos, (
            "the sector-health dispatch must appear before the "
            "intelligence_tasks dispatch in source order so a slow/timed-"
            "out intelligence_tasks step can never again prevent sector-"
            "health from running"
        )

    def test_run_intelligence_tasks_no_longer_calls_the_sector_health_helper(self) -> None:
        src = inspect.getsource(ho.run_intelligence_tasks)
        assert "_maybe_run_sector_health_snapshot" not in src, (
            "the sector-health snapshot must be dispatched from its own "
            "step, not from inside run_intelligence_tasks"
        )


# ─── 2. Own timeout: sector step is bounded independently of intelligence ──


class TestSectorStepHasItsOwnTimeout:
    def test_timeout_and_abandoned_worker_do_not_clobber_state(self, monkeypatch) -> None:
        started_computing = threading.Event()

        def _slow_then_success(engine: Any, snapshot_date: date | None = None, **_kwargs: Any) -> dict[str, Any]:
            started_computing.set()
            time.sleep(3)
            return {
                "snapshots_written": 20, "snapshots_skipped_unavailable": 0,
                "upsert_failed": 0,
            }

        monkeypatch.setattr(
            "intelligence.sector_health.snapshot_all_sectors", _slow_then_success,
        )
        monkeypatch.setattr(ho, "SECTOR_HEALTH_TIMEOUT_SECONDS", 1)
        # This test is only about the sector-health step's own timeout;
        # stub out intelligence_tasks (fast, no-op) so this test does not
        # also invoke the real ~900s-budget run_intelligence_tasks against
        # a bare MagicMock engine.
        monkeypatch.setattr(
            ho, "run_intelligence_tasks", lambda engine, state, dry_run=False: {"skipped": "stubbed"},
        )

        state = _fresh_state()
        engine = MagicMock()
        cycle_result: dict[str, Any] = {}
        token_before = state.sector_health_attempt_token

        ho._run_sector_and_intelligence_steps(engine, state, False, cycle_result)

        assert cycle_result["sector_health"] == {"timeout": True}
        assert state.last_sector_health_attempt is not None
        assert state.sector_health_attempt_count == 1
        assert state.last_sector_health is None, "a timed-out attempt must not mark the due period done"
        assert state.sector_health_attempt_token > token_before, (
            "the token must be bumped on timeout so the abandoned worker's "
            "later _commit is discarded"
        )

        # Let the orphaned worker actually finish (it slept 3s from roughly
        # when it started; give it headroom) and prove its belated success
        # did NOT retroactively mark the due period done.
        assert started_computing.wait(timeout=2), "fake snapshot never started"
        time.sleep(3.5)

        assert state.last_sector_health is None, (
            "the orphaned worker's belated success must not clobber state "
            "after the fact"
        )
        assert state.last_sector_health_outcome != "success", (
            "the orphan must not be the one to set the outcome to success"
        )


# ─── 3. Blacklist trace pinned ──────────────────────────────────────────


class TestSectorHealthDoesNotConsultTheCooldownBlacklist:
    def test_blacklisted_after_timeout_but_backoff_still_runs_it(self, monkeypatch) -> None:
        """_run_with_timeout blacklists "sector_health" on every timeout
        (state.cooldowns.blacklist_for_timeout), same as every other named
        step. Pin that this entry is genuinely written (can_retry is False
        right after) AND genuinely never consulted by this step: a later
        evaluation, once the sector-health-specific backoff has elapsed,
        still runs the snapshot despite the blacklist saying not to."""
        monkeypatch.setattr(ho, "SECTOR_HEALTH_TIMEOUT_SECONDS", 1)

        def _times_out(engine: Any, snapshot_date: date | None = None, **_kwargs: Any) -> dict[str, Any]:
            time.sleep(2)
            return {"snapshots_written": 0, "snapshots_skipped_unavailable": 0, "upsert_failed": 0}

        monkeypatch.setattr("intelligence.sector_health.snapshot_all_sectors", _times_out)
        # Same reason as TestSectorStepHasItsOwnTimeout above: keep this
        # test's real _run_with_timeout call scoped to sector_health only.
        monkeypatch.setattr(
            ho, "run_intelligence_tasks", lambda engine, state, dry_run=False: {"skipped": "stubbed"},
        )

        state = _fresh_state()
        engine = MagicMock()
        cycle_result: dict[str, Any] = {}

        ho._run_sector_and_intelligence_steps(engine, state, False, cycle_result)

        assert state.cooldowns.can_retry("sector_health") is False, (
            "the blacklist entry must actually be written on timeout — "
            "this step just must not consult it"
        )

        # Fast-forward past the sector-health retry backoff and let the
        # orphaned worker's sleep(2) elapse; a fresh (fast) fake now
        # succeeds, proving the step ran despite the standing blacklist.
        time.sleep(2.2)

        def _fast_success(engine: Any, snapshot_date: date | None = None, **_kwargs: Any) -> dict[str, Any]:
            return {"snapshots_written": 20, "snapshots_skipped_unavailable": 0, "upsert_failed": 0}

        monkeypatch.setattr("intelligence.sector_health.snapshot_all_sectors", _fast_success)
        monkeypatch.setattr(ho, "SECTOR_HEALTH_TIMEOUT_SECONDS", 120)

        # Directly drive the retry-eligible path (bypasses real wall-clock
        # waiting for the 60-minute backoff): construct a `now` past both
        # the due-period boundary and SECTOR_HEALTH_RETRY_BACKOFF_MINUTES
        # since the timed-out attempt.
        from datetime import timedelta
        retry_now = state.last_sector_health_attempt + timedelta(
            minutes=ho.SECTOR_HEALTH_RETRY_BACKOFF_MINUTES + 1,
        )
        results: dict[str, Any] = {}
        ho._maybe_run_sector_health_snapshot(engine, state, retry_now, results)

        assert state.cooldowns.can_retry("sector_health") is False, (
            "blacklist entry is still standing — proves the step really "
            "does not check it, since it ran anyway"
        )
        assert results.get("sector_health_snapshot", {}).get("outcome") == "success"
        assert state.last_sector_health == retry_now

    def test_run_cycle_source_does_not_check_the_blacklist_for_sector_health(self) -> None:
        src = inspect.getsource(ho.run_cycle)
        assert 'can_retry("sector_health")' not in src


# ─── 4. DB-row protection: snapshot_all_sectors with a fake engine ────────


class _RowcountConn:
    """Fake connection whose upsert result's rowcount is controlled by the
    test (``next_rowcount``), exercising the REAL SQL text/bindparams in
    intelligence/sector_health.py — not a re-implementation of it."""

    def __init__(self, recorder: list[dict[str, Any]], rowcounts: list[int]) -> None:
        self._recorder = recorder
        self._rowcounts = rowcounts

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def execute(self, stmt: Any) -> MagicMock:
        compiled = stmt.compile()
        params = dict(compiled.params)
        self._recorder.append(params)
        result = MagicMock()
        result.rowcount = self._rowcounts.pop(0) if self._rowcounts else 1
        return result


class _RowcountEngine:
    def __init__(self, rowcounts: list[int]) -> None:
        self.recorded: list[dict[str, Any]] = []
        self._rowcounts = rowcounts

    def begin(self) -> _RowcountConn:
        return _RowcountConn(self.recorded, self._rowcounts)


class TestSnapshotAllSectorsAsOfGuard:
    def test_upsert_sql_has_the_as_of_guard_and_binds_as_of(self, monkeypatch) -> None:
        import intelligence.sector_health as sh

        monkeypatch.setattr(
            sh, "compute_sector_health",
            lambda _engine, name: {"score": 60.0, "trend_30d": 0.0, "components": {"stub": True}},
        )

        captured_sql: list[str] = []

        class _SqlCapturingConn:
            def __enter__(self) -> Self:
                return self

            def __exit__(self, *exc: object) -> bool:
                return False

            def execute(self, stmt: Any) -> MagicMock:
                captured_sql.append(str(stmt))
                params = dict(stmt.compile().params)
                assert "as_of" in params
                result = MagicMock()
                result.rowcount = 1
                return result

        class _SqlCapturingEngine:
            def begin(self) -> _SqlCapturingConn:
                return _SqlCapturingConn()

        computed_at = datetime(2026, 9, 19, 3, 0, tzinfo=timezone.utc)
        out = sh.snapshot_all_sectors(
            _SqlCapturingEngine(), snapshot_date=date(2026, 9, 19), computed_at=computed_at,
        )

        assert captured_sql, "no upsert executed"
        assert "as_of <= EXCLUDED.as_of" in captured_sql[0]
        assert "as_of = EXCLUDED.as_of" in captured_sql[0]
        assert out["upsert_failed"] == 0

    def test_rowcount_zero_counts_as_stale_skipped_not_written_not_failed(self, monkeypatch) -> None:
        import intelligence.sector_health as sh
        from analysis.sector_map import SECTOR_MAP

        monkeypatch.setattr(
            sh, "compute_sector_health",
            lambda _engine, name: {"score": 60.0, "trend_30d": 0.0, "components": {"stub": True}},
        )

        n = len(SECTOR_MAP)
        engine = _RowcountEngine(rowcounts=[0] * n)

        out = sh.snapshot_all_sectors(engine, snapshot_date=date(2026, 9, 19))

        assert out["snapshots_written"] == 0
        assert out["snapshots_stale_skipped"] == n
        assert out["upsert_failed"] == 0

    def test_should_continue_false_before_first_upsert_writes_nothing(self, monkeypatch) -> None:
        import intelligence.sector_health as sh

        monkeypatch.setattr(
            sh, "compute_sector_health",
            lambda _engine, name: {"score": 60.0, "trend_30d": 0.0, "components": {"stub": True}},
        )

        engine = _RowcountEngine(rowcounts=[])

        out = sh.snapshot_all_sectors(
            engine, snapshot_date=date(2026, 9, 19), should_continue=lambda: False,
        )

        assert engine.recorded == [], "no upsert statement may execute once should_continue is False"
        assert out["aborted_stale"] is True
        assert out["snapshots_written"] == 0

    def test_token_flow_end_to_end_attempt_1_writes_nothing_after_attempt_2_starts(
        self, monkeypatch,
    ) -> None:
        """Full token lifecycle against the real snapshot_all_sectors:
        attempt 1 is mid-loop (has written some sectors) when attempt 2's
        token bump happens; attempt 1's should_continue then reports False
        and its remaining sectors write nothing further."""
        import intelligence.sector_health as sh
        from analysis.sector_map import SECTOR_MAP

        sector_names = list(SECTOR_MAP.keys())
        assert len(sector_names) >= 2, "test needs at least 2 sectors to prove a mid-loop abort"

        state = _fresh_state()
        state.sector_health_attempt_token = 1
        attempt_1_token = 1

        call_count = {"n": 0}

        def _compute(_engine: Any, name: str) -> dict[str, Any]:
            call_count["n"] += 1
            if call_count["n"] == 2:
                # Simulate attempt 2 starting while attempt 1 is mid-loop.
                state.sector_health_attempt_token = 2
            return {"score": 60.0, "trend_30d": 0.0, "components": {"stub": True}}

        monkeypatch.setattr(sh, "compute_sector_health", _compute)

        engine = _RowcountEngine(rowcounts=[1] * len(sector_names))

        out = sh.snapshot_all_sectors(
            engine,
            snapshot_date=date(2026, 9, 19),
            should_continue=lambda: state.sector_health_attempt_token == attempt_1_token,
        )

        assert out["aborted_stale"] is True
        # Exactly one sector's upsert landed (the one computed before the
        # token moved) — the rest of attempt 1's sectors wrote nothing.
        assert out["snapshots_written"] == 1
        assert len(engine.recorded) == 1


# ─── 5. Existing suites stay green — see run instructions in the module
#        docstring; nothing further to assert here, this file just adds
#        coverage alongside tests/test_hermes_sector_schedule.py,
#        tests/test_sector_health.py and tests/test_hermes_timeout_budgets.py.
