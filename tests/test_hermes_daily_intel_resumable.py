"""Regression tests for the resumable daily-intel batch.

Background
----------
``scripts/hermes_operator.py::run_intelligence_tasks`` used to run its
"Daily at 2:00 AM" block as ~20 sequential tasks, each in its own bare
``try/except`` and none with its own timeout, inside the 900s
``INTELLIGENCE_TASKS_TIMEOUT_SECONDS`` step. Traced in production: the
step was abandoned at 900s on every post-02:00Z cycle before reaching
``state.last_daily_intel = now``, so the whole block restarted from the
top as catch-up every cycle and nothing after the first ~10 minutes of it
ever ran — hypothesis discovery starved since 2026-09-17.

fable-daily-intel-resumable (2026-09-20) replaced that monolithic block
with ``DAILY_INTEL_TASKS`` (an ordered task table, each entry with its own
``_run_with_timeout`` budget) executed by ``_run_daily_intel_block``, which
tracks per-task completion in a persisted per-period ledger on
``OperatorState`` (``daily_intel_done`` / ``daily_intel_skipped_for_period``
/ ``daily_intel_attempts``) and a cumulative per-cycle budget
(``DAILY_INTEL_CYCLE_BUDGET_SECONDS``). This file pins that behavior. No
DB/network — every task in these tests is a monkeypatched fake; the real
``DAILY_INTEL_TASKS`` table is only exercised by
``TestRealTaskTableSanity`` and the pure budget-pin tests, neither of
which touches a real engine.
"""
from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from scripts import hermes_operator as ho
from scripts.hermes_health import OperatorState


@pytest.fixture(autouse=True)
def _clear_daily_intel_in_flight_registry():
    """_DAILY_INTEL_IN_FLIGHT is module-level and shared across tests in
    this file — several tests reuse synthetic task names ("t0", "t1",
    ...). Clear it before and after every test so no test can observe a
    leftover in-flight entry (or stale thread ident) from a previous
    test."""
    ho._DAILY_INTEL_IN_FLIGHT.clear()
    yield
    ho._DAILY_INTEL_IN_FLIGHT.clear()


def _task(
    name: str, fn, budget_s: float = 5.0, *, reports_done_queued: bool = False,
) -> ho.DailyIntelTask:
    return ho.DailyIntelTask(name, fn, budget_s, reports_done_queued=reports_done_queued)


def _allow_all(monkeypatch, tasks: tuple[ho.DailyIntelTask, ...]) -> None:
    """Most of this file's tests predate the allow/hold gate and exercise
    synthetic task names ("t0", "blocked", "bad", ...) that are not in the
    real DAILY_INTEL_INITIAL_ALLOWLIST. Allow every synthetic task so the
    pre-existing resumability behavior under test is unaffected by the
    allow-list gate — the gate itself is covered separately by
    TestAllowlistGate/TestDailyIntelAllowlistClassification below."""
    monkeypatch.setattr(
        ho, "DAILY_INTEL_INITIAL_ALLOWLIST", frozenset(t.name for t in tasks),
    )


def _recording_fn(name: str, calls: list[str], *, sleep_s: float = 0.0, fail: bool = False):
    def _fn(engine: Any, state: OperatorState, now: datetime, results: dict[str, Any]) -> None:
        if sleep_s:
            time.sleep(sleep_s)
        calls.append(name)
        if fail:
            raise RuntimeError(f"{name} boom")

    return _fn


def _period_iso(now: datetime) -> str:
    return ho._period_boundary(now, ho.DAILY_INTEL_BOUNDARY_HOUR).date().isoformat()


NOW = datetime(2026, 9, 20, 2, 5, tzinfo=timezone.utc)


# ─── (a) a blocked first task does not prevent later tasks from running ──


class TestBlockedFirstTaskDoesNotBlockLaterTasks:
    def test_a_timeout_on_task_one_continues_to_the_rest_within_the_cycle(
        self, monkeypatch
    ) -> None:
        calls: list[str] = []
        blocked = _task("blocked", _recording_fn("blocked", calls, sleep_s=0.3), budget_s=0.05)
        t2 = _task("t2", _recording_fn("t2", calls))
        t3 = _task("t3", _recording_fn("t3", calls))
        monkeypatch.setattr(ho, "DAILY_INTEL_TASKS", (blocked, t2, t3))
        _allow_all(monkeypatch, (blocked, t2, t3))
        monkeypatch.setattr(ho, "DAILY_INTEL_CYCLE_BUDGET_SECONDS", 30)

        state = OperatorState()
        results: dict[str, Any] = {}
        ho._run_daily_intel_block(MagicMock(), state, NOW, results)

        # t2/t3 ran within the SAME cycle despite task 1 timing out.
        assert "t2" in calls
        assert "t3" in calls
        period_iso = _period_iso(NOW)
        assert state.daily_intel_attempts.get("blocked") == 1
        assert state.daily_intel_done.get("blocked") is None
        assert state.daily_intel_done.get("t2") == period_iso
        assert state.daily_intel_done.get("t3") == period_iso


# ─── (b) restart mid-period resumes from the first undone task ──────────


class TestRestartMidPeriodResumes:
    def test_b_hydrated_done_tasks_are_not_re_run(self, monkeypatch) -> None:
        calls: list[str] = []
        tasks = tuple(_task(f"t{i}", _recording_fn(f"t{i}", calls)) for i in range(3))
        monkeypatch.setattr(ho, "DAILY_INTEL_TASKS", tasks)
        _allow_all(monkeypatch, tasks)
        monkeypatch.setattr(ho, "DAILY_INTEL_CYCLE_BUDGET_SECONDS", 30)

        state = OperatorState()
        period_iso = _period_iso(NOW)
        # Simulate a restart that hydrated the ledger from a persisted
        # snapshot dict (the same shape OperatorState.hydrate_from_snapshot
        # restores) with t0 already done for the current period.
        state.daily_intel_period = period_iso
        state.daily_intel_done = {"t0": period_iso}

        results: dict[str, Any] = {}
        ho._run_daily_intel_block(MagicMock(), state, NOW, results)

        assert "t0" not in calls, "a task hydrated as done must not be re-run"
        assert "t1" in calls
        assert "t2" in calls


# ─── (c) period rollover resets the ledger and attempts ─────────────────


class TestPeriodRollover:
    def test_c_new_period_clears_done_and_attempts(self, monkeypatch) -> None:
        calls: list[str] = []
        tasks = (_task("t0", _recording_fn("t0", calls)),)
        monkeypatch.setattr(ho, "DAILY_INTEL_TASKS", tasks)
        _allow_all(monkeypatch, tasks)
        monkeypatch.setattr(ho, "DAILY_INTEL_CYCLE_BUDGET_SECONDS", 30)

        state = OperatorState()
        state.daily_intel_period = "2026-09-18"
        state.daily_intel_done = {"t0": "2026-09-18"}
        state.daily_intel_skipped_for_period = {}
        state.daily_intel_attempts = {"t0": 2}

        results: dict[str, Any] = {}
        ho._run_daily_intel_block(MagicMock(), state, NOW, results)

        new_period = _period_iso(NOW)
        assert state.daily_intel_period == new_period
        assert "t0" in calls, "stale-period done entry must not block a new period"
        assert state.daily_intel_done.get("t0") == new_period
        assert state.daily_intel_attempts.get("t0", 0) == 0, (
            "attempts must reset on rollover, not carry the old period's count"
        )


# ─── (d) a task hitting max attempts is marked skipped_for_period ───────


class TestMaxAttemptsSkipsForPeriod:
    def test_d_skipped_after_max_attempts_and_block_completes(self, monkeypatch) -> None:
        attempts: list[int] = []

        def always_fail(engine: Any, state: OperatorState, now: datetime, results: dict[str, Any]) -> None:
            attempts.append(1)
            raise RuntimeError("boom")

        tasks = (_task("bad", always_fail),)
        monkeypatch.setattr(ho, "DAILY_INTEL_TASKS", tasks)
        _allow_all(monkeypatch, tasks)
        monkeypatch.setattr(ho, "DAILY_INTEL_CYCLE_BUDGET_SECONDS", 30)
        monkeypatch.setattr(ho, "DAILY_INTEL_MAX_ATTEMPTS", 3)

        state = OperatorState()
        results: dict[str, Any] = {}
        for _ in range(3):
            ho._run_daily_intel_block(MagicMock(), state, NOW, results)

        assert len(attempts) == 3
        period_iso = _period_iso(NOW)
        assert state.daily_intel_skipped_for_period.get("bad") == period_iso
        assert state.daily_intel_done.get("bad") == period_iso, (
            "a skipped task must also be recorded done so it cannot block "
            "tasks scheduled behind it"
        )
        assert state.last_daily_intel == NOW, (
            "the block must complete (last_daily_intel set) once every task "
            "is done OR skipped_for_period, even if this one never succeeded"
        )

        # A 4th call in the same period must not attempt it again.
        ho._run_daily_intel_block(MagicMock(), state, NOW, results)
        assert len(attempts) == 3


# ─── (e) last_daily_intel is NOT set while any task is undone ───────────


class TestLastDailyIntelNotSetUntilComplete:
    def test_e_partial_completion_leaves_last_daily_intel_unset(self, monkeypatch) -> None:
        calls: list[str] = []
        good = _task("good", _recording_fn("good", calls))

        def fails_once(engine: Any, state: OperatorState, now: datetime, results: dict[str, Any]) -> None:
            raise RuntimeError("boom")

        bad = _task("bad", fails_once)
        monkeypatch.setattr(ho, "DAILY_INTEL_TASKS", (good, bad))
        _allow_all(monkeypatch, (good, bad))
        monkeypatch.setattr(ho, "DAILY_INTEL_CYCLE_BUDGET_SECONDS", 30)
        monkeypatch.setattr(ho, "DAILY_INTEL_MAX_ATTEMPTS", 3)

        state = OperatorState()
        results: dict[str, Any] = {}
        ho._run_daily_intel_block(MagicMock(), state, NOW, results)

        assert "good" in calls
        assert state.daily_intel_done.get("good") == _period_iso(NOW)
        assert state.daily_intel_done.get("bad") is None
        assert state.last_daily_intel is None, (
            "last_daily_intel must stay unset while 'bad' is neither done "
            "nor skipped_for_period"
        )


# ─── (f) cycle-budget exhaustion stops the block mid-list and resumes ────


class TestCycleBudgetExhaustion:
    def test_f_budget_cutoff_stops_mid_list_and_resumes_next_call(self, monkeypatch) -> None:
        calls: list[str] = []
        tasks = tuple(_task(f"t{i}", _recording_fn(f"t{i}", calls)) for i in range(3))
        monkeypatch.setattr(ho, "DAILY_INTEL_TASKS", tasks)
        _allow_all(monkeypatch, tasks)
        monkeypatch.setattr(ho, "DAILY_INTEL_CYCLE_BUDGET_SECONDS", 100)

        # Fake a monotonic clock so each task "costs" exactly 60 simulated
        # seconds regardless of real wall-clock time — deterministic,
        # no real sleeping needed.
        first_cycle_clock = iter([0.0, 60.0, 60.0, 120.0])
        monkeypatch.setattr(ho.time, "monotonic", lambda: next(first_cycle_clock))

        state = OperatorState()
        results: dict[str, Any] = {}
        ho._run_daily_intel_block(MagicMock(), state, NOW, results)

        period_iso = _period_iso(NOW)
        assert calls == ["t0", "t1"], "budget must run out before t2 starts (120s >= 100s cap)"
        assert state.daily_intel_done.get("t0") == period_iso
        assert state.daily_intel_done.get("t1") == period_iso
        assert "t2" not in state.daily_intel_done

        # Next cycle: budget resets (it is a per-call local, not persisted),
        # and the loop resumes at the first undone task (t2) without
        # re-running t0/t1.
        second_cycle_clock = iter([120.0, 130.0])
        monkeypatch.setattr(ho.time, "monotonic", lambda: next(second_cycle_clock))
        ho._run_daily_intel_block(MagicMock(), state, NOW, results)

        assert calls == ["t0", "t1", "t2"]
        assert state.daily_intel_done.get("t2") == period_iso


# ─── (g) an abandoned timed-out task cannot mark itself done later ──────


class TestAbandonedWorkerCannotSelfMarkDone:
    def test_g_late_return_after_timeout_does_not_mark_done(self, monkeypatch) -> None:
        finished = threading.Event()

        def slow(engine: Any, state: OperatorState, now: datetime, results: dict[str, Any]) -> None:
            time.sleep(0.3)
            finished.set()

        tasks = (_task("slow", slow, budget_s=0.05),)
        monkeypatch.setattr(ho, "DAILY_INTEL_TASKS", tasks)
        _allow_all(monkeypatch, tasks)
        monkeypatch.setattr(ho, "DAILY_INTEL_CYCLE_BUDGET_SECONDS", 30)

        state = OperatorState()
        results: dict[str, Any] = {}
        ho._run_daily_intel_block(MagicMock(), state, NOW, results)

        # _run_with_timeout already returned ok=False synchronously.
        assert state.daily_intel_done.get("slow") is None
        assert state.daily_intel_attempts.get("slow") == 1

        # Let the orphaned worker actually finish in the background.
        assert finished.wait(timeout=2.0), "orphaned worker never completed"
        # There is no callback path from the orphaned worker back into the
        # ledger — its belated completion must not have marked it done.
        assert state.daily_intel_done.get("slow") is None


# ─── (h) budget pins ──────────────────────────────────────────────────


class TestBudgetPins:
    def test_h_scorer_plus_cycle_budget_fits_inside_intelligence_step(self) -> None:
        assert (
            ho.ACTIVE_HYPO_SCORING_MAX_RUNTIME_S + ho.DAILY_INTEL_CYCLE_BUDGET_SECONDS + 60
            <= ho.INTELLIGENCE_TASKS_TIMEOUT_SECONDS
        )

    def test_h_max_attempts_and_cycle_budget_are_sane_positive_values(self) -> None:
        assert ho.DAILY_INTEL_MAX_ATTEMPTS >= 1
        assert ho.DAILY_INTEL_CYCLE_BUDGET_SECONDS > 0
        assert ho.DAILY_INTEL_CYCLE_BUDGET_SECONDS < ho.INTELLIGENCE_TASKS_TIMEOUT_SECONDS

    def test_h_every_real_task_has_a_positive_budget(self) -> None:
        assert len(ho.DAILY_INTEL_TASKS) > 0
        for task in ho.DAILY_INTEL_TASKS:
            assert task.budget_s > 0, task.name
            # No single task's budget should be able to consume the WHOLE
            # per-cycle budget on its own and still leave room for at
            # least the next task to be attempted next cycle.
            assert task.budget_s <= ho.DAILY_INTEL_CYCLE_BUDGET_SECONDS

    def test_h_task_names_are_unique(self) -> None:
        names = [t.name for t in ho.DAILY_INTEL_TASKS]
        assert len(names) == len(set(names))


# ─── Summary log line ─────────────────────────────────────────────────


class TestSummaryLogLine:
    def test_summary_line_reports_done_ran_skipped_remaining(self, monkeypatch) -> None:
        calls: list[str] = []
        good = _task("good", _recording_fn("good", calls))

        def fails(engine: Any, state: OperatorState, now: datetime, results: dict[str, Any]) -> None:
            raise RuntimeError("boom")

        bad = _task("bad", fails)
        monkeypatch.setattr(ho, "DAILY_INTEL_TASKS", (good, bad))
        _allow_all(monkeypatch, (good, bad))
        monkeypatch.setattr(ho, "DAILY_INTEL_CYCLE_BUDGET_SECONDS", 30)
        monkeypatch.setattr(ho, "DAILY_INTEL_MAX_ATTEMPTS", 3)

        logged: list[tuple[str, dict]] = []
        monkeypatch.setattr(ho.log, "info", lambda msg, **kw: logged.append((msg, kw)))

        state = OperatorState()
        results: dict[str, Any] = {}
        ho._run_daily_intel_block(MagicMock(), state, NOW, results)

        summary = next(kw for msg, kw in logged if msg.startswith("daily_intel: period="))
        assert summary["d"] == 1
        assert summary["t"] == 2
        assert "good" in summary["r"]
        assert "bad" in summary["r"]
        assert summary["s"] == []
        assert summary["rem"] == ["bad"]
        assert isinstance(summary["b"], float)


# ─── Sanity check against the REAL task table (no fakes) ────────────────


class TestRealTaskTableSanity:
    def test_task_table_matches_the_documented_ordered_list(self) -> None:
        expected_order = [
            "storage_maintenance_subagent",
            "source_audit",
            "flow_materialize",
            "backtest_scan",
            "postmortem_batch",
            "options_improvement",
            "hypothesis_review",
            "hypothesis_discovery",
            "rag_index",
            "actor_research",
            "icij_linking",
            "milestone_scoring",
            "attention_anomaly",
            "edgar_transcripts",
            "corporate_actions",
            "capital_flow_rollups",
            "fundamental_divergence",
            "holder_deal_overlap",
            "insight_cleanup",
            "briefing_cleanup",
            "errors_jsonl_cleanup",
        ]
        assert [t.name for t in ho.DAILY_INTEL_TASKS] == expected_order


# ─── Allow-list classification (review amendment, deliverable 1) ────────


class TestDailyIntelAllowlistClassification:
    def test_every_task_classified_exactly_once(self) -> None:
        all_names = {t.name for t in ho.DAILY_INTEL_TASKS}
        allow = ho.DAILY_INTEL_INITIAL_ALLOWLIST
        hold = set(ho.DAILY_INTEL_HOLD_REASONS)
        assert allow.isdisjoint(hold), "a task cannot be both allowed and held"
        assert allow | hold == all_names, (
            "every DAILY_INTEL_TASKS name must be classified exactly once "
            "(present in exactly one of DAILY_INTEL_INITIAL_ALLOWLIST / "
            "DAILY_INTEL_HOLD_REASONS)"
        )

    def test_hold_classified_tasks_are_absent_from_the_allowlist(self) -> None:
        for name in ho.DAILY_INTEL_HOLD_REASONS:
            assert name not in ho.DAILY_INTEL_INITIAL_ALLOWLIST, name

    def test_standing_hold_categories_are_represented(self) -> None:
        # Standing holds (controller instruction): scorer execution/signal
        # scoring, historical repair/backfill, and learning/research
        # writes (hypothesis registry, backtests, model registry,
        # postmortems that feed learning).
        expected_holds = {
            "hypothesis_discovery", "hypothesis_review", "backtest_scan",
            "postmortem_batch", "options_improvement", "milestone_scoring",
            "actor_research", "edgar_transcripts",
            # coordinator's initial-subset holds (non-idempotent audit
            # appends + priority_rank rewrite; delete-then-rebuild index)
            "source_audit", "rag_index",
            # controller's 2026-09-20 narrowing: cleanups held until file
            # deletion/truncation policy (directories, retention,
            # exclusions) is accepted separately.
            "insight_cleanup", "briefing_cleanup", "errors_jsonl_cleanup",
        }
        assert set(ho.DAILY_INTEL_HOLD_REASONS) == expected_holds

    def test_expected_allow_set(self) -> None:
        # Pins the exact controller-narrowed allow-list (2026-09-20, see
        # docs/handoffs/2026-09-20/fable-hermes-daily-intel-resumable.md):
        # the eight non-cleanup tasks whose late-write analysis came back
        # `safe` (sole writer, deterministic recompute from current
        # inputs, idempotent DO UPDATE/DO NOTHING). source_audit and
        # rag_index are held for the initial subset (non-idempotent audit
        # appends / delete-then-rebuild); the three cleanups are held
        # pending a separate deletion/truncation policy review.
        expected_allow = {
            "storage_maintenance_subagent", "flow_materialize",
            "icij_linking", "attention_anomaly",
            "corporate_actions", "capital_flow_rollups",
            "fundamental_divergence", "holder_deal_overlap",
        }
        assert ho.DAILY_INTEL_INITIAL_ALLOWLIST == expected_allow


# ─── Held tasks never run, never counted (deliverable 1) ────────────────


class TestHeldTasksNeverRun:
    def test_held_task_is_never_dispatched_and_never_blocks_completion(
        self, monkeypatch
    ) -> None:
        calls: list[str] = []
        allowed = _task("allowed", _recording_fn("allowed", calls))

        def should_never_run(
            engine: Any, state: OperatorState, now: datetime, results: dict[str, Any]
        ) -> None:
            raise AssertionError("a held task's fn must never be called")

        held_task = _task("held_one", should_never_run)
        tasks = (allowed, held_task)
        monkeypatch.setattr(ho, "DAILY_INTEL_TASKS", tasks)
        monkeypatch.setattr(ho, "DAILY_INTEL_INITIAL_ALLOWLIST", frozenset({"allowed"}))
        monkeypatch.setattr(ho, "DAILY_INTEL_CYCLE_BUDGET_SECONDS", 30)

        state = OperatorState()
        results: dict[str, Any] = {}
        ho._run_daily_intel_block(MagicMock(), state, NOW, results)

        assert "allowed" in calls
        assert state.daily_intel_task_outcome.get("held_one") == "held"
        assert state.daily_intel_done.get("held_one") is None
        assert state.daily_intel_skipped_for_period.get("held_one") is None
        # Period completion is decided over ENABLED tasks only — the held
        # task must not block it, and must not appear in "remaining" logic
        # either (it can never make done_count < total).
        assert state.last_daily_intel == NOW
        # part E (2026-09-20): "complete" alone is reserved for zero held
        # tasks. A held task is present here, so the outcome must be the
        # "_for_enabled_tasks" variant even though nothing was skipped.
        assert state.daily_intel_period_outcome == "complete_for_enabled_tasks"

    def test_summary_line_lists_held_tasks_explicitly(self, monkeypatch) -> None:
        calls: list[str] = []
        allowed = _task("allowed", _recording_fn("allowed", calls))
        held_task = _task("held_one", lambda *a: None)
        tasks = (allowed, held_task)
        monkeypatch.setattr(ho, "DAILY_INTEL_TASKS", tasks)
        monkeypatch.setattr(ho, "DAILY_INTEL_INITIAL_ALLOWLIST", frozenset({"allowed"}))
        monkeypatch.setattr(ho, "DAILY_INTEL_CYCLE_BUDGET_SECONDS", 30)

        logged: list[tuple[str, dict]] = []
        monkeypatch.setattr(ho.log, "info", lambda msg, **kw: logged.append((msg, kw)))

        state = OperatorState()
        results: dict[str, Any] = {}
        ho._run_daily_intel_block(MagicMock(), state, NOW, results)

        summary = next(kw for msg, kw in logged if msg.startswith("daily_intel: period="))
        assert summary["h"] == ["held_one"], (
            "a held task must be listed in the summary line's held=[...] "
            "so it can never be mistaken for a completed task"
        )
        assert "held_one" not in summary["r"]


# ─── In-flight no-overlap guard (deliverable 2) ──────────────────────────


class TestInFlightOverlapGuard:
    def test_second_run_skips_in_flight_task_and_proceeds_to_next(
        self, monkeypatch
    ) -> None:
        release = threading.Event()
        worker_finished = threading.Event()
        call_count = {"n": 0}

        def slow_then_fast(
            engine: Any, state: OperatorState, now: datetime, results: dict[str, Any]
        ) -> None:
            call_count["n"] += 1
            if call_count["n"] == 1:
                # First call blocks past its own budget, simulating a
                # worker abandoned by _run_with_timeout's timeout.
                release.wait(timeout=5.0)
                worker_finished.set()
            results["slow"] = "ran"

        calls: list[str] = []
        slow_task = _task("slow", slow_then_fast, budget_s=0.05)
        t2_task = _task("t2", _recording_fn("t2", calls))
        tasks = (slow_task, t2_task)
        monkeypatch.setattr(ho, "DAILY_INTEL_TASKS", tasks)
        _allow_all(monkeypatch, tasks)

        state = OperatorState()
        results: dict[str, Any] = {}

        # Call A: a tiny cycle budget so only "slow" is attempted this
        # call (it times out; its worker thread is left running, blocked
        # on `release`) — t2 is never reached in call A.
        monkeypatch.setattr(ho, "DAILY_INTEL_CYCLE_BUDGET_SECONDS", 0.001)
        ho._run_daily_intel_block(MagicMock(), state, NOW, results)

        assert call_count["n"] == 1
        assert state.daily_intel_attempts.get("slow") == 1
        assert state.daily_intel_done.get("slow") is None
        assert "t2" not in calls, "tiny cycle budget must stop the block before t2"

        # Call B: while the call-A worker is still blocked on `release`, a
        # second call for the SAME period must skip "slow" as in_flight —
        # not attempt it, not increment its attempt count — and proceed
        # to the next task ("t2").
        monkeypatch.setattr(ho, "DAILY_INTEL_CYCLE_BUDGET_SECONDS", 30)
        ho._run_daily_intel_block(MagicMock(), state, NOW, results)

        assert call_count["n"] == 1, "in_flight must not start a second worker"
        assert state.daily_intel_attempts.get("slow") == 1, (
            "an in_flight skip must not count as an attempt"
        )
        assert state.daily_intel_task_outcome.get("slow") == "in_flight"
        assert "t2" in calls, "the block must proceed past the in_flight task to t2"
        period_iso = _period_iso(NOW)
        assert state.daily_intel_done.get("t2") == period_iso

        # Release the call-A worker and let it actually finish.
        release.set()
        assert worker_finished.wait(timeout=2.0), "orphaned worker never completed"

        # Its belated completion must not have published anything: "slow"
        # is still not done, and the shared `results` dict was never
        # touched by that abandoned worker (it wrote into its own LOCAL
        # results dict, discarded once its token went stale at call A's
        # timeout — see _run_daily_intel_block's docstring).
        assert state.daily_intel_done.get("slow") is None
        assert "slow" not in results

        # Wait for the orphan's OS thread to actually exit (worker_finished
        # fires just before the function returns; give the thread pool a
        # moment to tear down) so the registry no longer reports it alive.
        deadline = time.monotonic() + 2.0
        entry = ho._DAILY_INTEL_IN_FLIGHT.get("slow") or {}
        while (
            ho._daily_intel_thread_alive(entry.get("thread"))
            and time.monotonic() < deadline
        ):
            time.sleep(0.01)
            entry = ho._DAILY_INTEL_IN_FLIGHT.get("slow") or {}
        assert not ho._daily_intel_thread_alive(entry.get("thread")), (
            "registry must stop reporting the task in flight once its "
            "worker thread has actually exited"
        )

        # A fresh call now retries "slow" normally and succeeds.
        ho._run_daily_intel_block(MagicMock(), state, NOW, results)
        assert call_count["n"] == 2
        assert state.daily_intel_done.get("slow") == period_iso
        assert results.get("slow") == "ran"


# ─── Outcome semantics: done vs skipped_for_period vs held (deliverable 3)


class TestOutcomeSemantics:
    def test_all_enabled_tasks_done_gives_complete_period_outcome(
        self, monkeypatch
    ) -> None:
        calls: list[str] = []
        a = _task("a", _recording_fn("a", calls))
        b = _task("b", _recording_fn("b", calls))
        tasks = (a, b)
        monkeypatch.setattr(ho, "DAILY_INTEL_TASKS", tasks)
        _allow_all(monkeypatch, tasks)
        monkeypatch.setattr(ho, "DAILY_INTEL_CYCLE_BUDGET_SECONDS", 30)

        state = OperatorState()
        results: dict[str, Any] = {}
        ho._run_daily_intel_block(MagicMock(), state, NOW, results)

        assert state.daily_intel_period_outcome == "complete"
        assert state.daily_intel_task_outcome.get("a") == "done"
        assert state.daily_intel_task_outcome.get("b") == "done"
        assert state.last_daily_intel == NOW

    def test_one_task_exhausted_gives_complete_with_skips(self, monkeypatch) -> None:
        calls: list[str] = []
        good = _task("good", _recording_fn("good", calls))

        def always_fail(
            engine: Any, state: OperatorState, now: datetime, results: dict[str, Any]
        ) -> None:
            raise RuntimeError("boom")

        bad = _task("bad", always_fail)
        tasks = (good, bad)
        monkeypatch.setattr(ho, "DAILY_INTEL_TASKS", tasks)
        _allow_all(monkeypatch, tasks)
        monkeypatch.setattr(ho, "DAILY_INTEL_CYCLE_BUDGET_SECONDS", 30)
        monkeypatch.setattr(ho, "DAILY_INTEL_MAX_ATTEMPTS", 2)

        state = OperatorState()
        results: dict[str, Any] = {}
        for _ in range(2):
            ho._run_daily_intel_block(MagicMock(), state, NOW, results)

        assert state.daily_intel_period_outcome == "complete_with_skips"
        assert state.daily_intel_task_outcome.get("bad") == "skipped_for_period"
        assert state.daily_intel_task_outcome.get("good") == "done"
        assert state.last_daily_intel == NOW

    def test_hydration_preserves_outcome_fields(self) -> None:
        period_iso = _period_iso(NOW)
        source_state = OperatorState()
        source_state.daily_intel_period = period_iso
        source_state.daily_intel_done = {"a": period_iso}
        source_state.daily_intel_task_outcome = {"a": "done", "h": "held"}
        source_state.daily_intel_period_outcome = "complete"
        payload = {"operator_state": source_state.to_dict()}

        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        conn.execute.return_value.fetchone.return_value = [payload]

        fresh = OperatorState()
        assert fresh.hydrate_from_snapshot(engine) is True
        assert fresh.daily_intel_task_outcome == {"a": "done", "h": "held"}
        assert fresh.daily_intel_period_outcome == "complete"

    def test_held_task_never_appears_as_done_or_skipped(self, monkeypatch) -> None:
        calls: list[str] = []
        allowed = _task("allowed", _recording_fn("allowed", calls))

        def should_never_run(
            engine: Any, state: OperatorState, now: datetime, results: dict[str, Any]
        ) -> None:
            raise AssertionError("held task fn must never be called")

        held_task = _task("held_one", should_never_run)
        tasks = (allowed, held_task)
        monkeypatch.setattr(ho, "DAILY_INTEL_TASKS", tasks)
        monkeypatch.setattr(ho, "DAILY_INTEL_INITIAL_ALLOWLIST", frozenset({"allowed"}))
        monkeypatch.setattr(ho, "DAILY_INTEL_CYCLE_BUDGET_SECONDS", 30)

        state = OperatorState()
        results: dict[str, Any] = {}
        ho._run_daily_intel_block(MagicMock(), state, NOW, results)

        assert state.daily_intel_task_outcome.get("held_one") == "held"
        assert state.daily_intel_done.get("held_one") is None
        assert state.daily_intel_skipped_for_period.get("held_one") is None


# ─── done_queued outcome (deliverable C) ─────────────────────────────────


class TestDoneQueuedOutcome:
    def test_reports_done_queued_task_gets_done_queued_not_done(
        self, monkeypatch
    ) -> None:
        calls: list[str] = []
        queued = _task(
            "storage_maintenance_subagent",
            _recording_fn("storage_maintenance_subagent", calls),
            reports_done_queued=True,
        )
        plain = _task("plain", _recording_fn("plain", calls))
        tasks = (queued, plain)
        monkeypatch.setattr(ho, "DAILY_INTEL_TASKS", tasks)
        _allow_all(monkeypatch, tasks)
        monkeypatch.setattr(ho, "DAILY_INTEL_CYCLE_BUDGET_SECONDS", 30)

        state = OperatorState()
        results: dict[str, Any] = {}
        ho._run_daily_intel_block(MagicMock(), state, NOW, results)

        period_iso = _period_iso(NOW)
        # done_queued still counts as "done for the period" —
        # daily_intel_done is unaffected by the distinction.
        assert state.daily_intel_done.get("storage_maintenance_subagent") == period_iso
        assert (
            state.daily_intel_task_outcome.get("storage_maintenance_subagent")
            == "done_queued"
        )
        assert state.daily_intel_task_outcome.get("plain") == "done"

    def test_real_storage_maintenance_subagent_task_reports_done_queued(self) -> None:
        # No monkeypatching — pins the REAL DAILY_INTEL_TASKS entry so an
        # accidental revert of reports_done_queued=True fails CI.
        entry = next(
            t for t in ho.DAILY_INTEL_TASKS if t.name == "storage_maintenance_subagent"
        )
        assert entry.reports_done_queued is True
        for t in ho.DAILY_INTEL_TASKS:
            if t.name != "storage_maintenance_subagent":
                assert t.reports_done_queued is False, (
                    f"{t.name}: only storage_maintenance_subagent dispatches "
                    "child work and finishes its OWN step before that work "
                    "executes; every other task's own step performs its "
                    "effects directly and should report plain 'done'"
                )


# ─── Period-outcome wording (deliverable E) ──────────────────────────────


class TestPeriodOutcomeWordingWithHeldTasks:
    def test_real_allowlist_with_13_held_reports_complete_for_enabled_tasks(
        self, monkeypatch
    ) -> None:
        """Uses the REAL DAILY_INTEL_TASKS/DAILY_INTEL_INITIAL_ALLOWLIST/
        DAILY_INTEL_HOLD_REASONS (8 allowed, 13 held) with every allow-listed
        task's fn replaced by a no-op so no real DB/network is touched.
        Held tasks keep their real (never-called) fn."""
        calls: list[str] = []

        def _noop(engine: Any, state: OperatorState, now: datetime, results: dict[str, Any]) -> None:
            calls.append("ran")

        patched_tasks = tuple(
            ho.DailyIntelTask(
                t.name,
                _noop if t.name in ho.DAILY_INTEL_INITIAL_ALLOWLIST else t.fn,
                t.budget_s,
                t.reports_done_queued,
            )
            for t in ho.DAILY_INTEL_TASKS
        )
        monkeypatch.setattr(ho, "DAILY_INTEL_TASKS", patched_tasks)
        monkeypatch.setattr(ho, "DAILY_INTEL_CYCLE_BUDGET_SECONDS", 3600)

        logged: list[tuple[str, dict]] = []
        monkeypatch.setattr(ho.log, "info", lambda msg, **kw: logged.append((msg, kw)))

        state = OperatorState()
        results: dict[str, Any] = {}
        ho._run_daily_intel_block(MagicMock(), state, NOW, results)

        assert len(calls) == len(ho.DAILY_INTEL_INITIAL_ALLOWLIST) == 8
        assert len(ho.DAILY_INTEL_HOLD_REASONS) == 13
        assert state.daily_intel_period_outcome == "complete_for_enabled_tasks"
        assert state.last_daily_intel == NOW

        summary = next(
            kw for msg, kw in logged
            if msg.startswith("daily_intel: period=") and "outcome" in kw
        )
        assert summary["outcome"] == "complete_for_enabled_tasks"
        assert summary["n"] == 8
        assert summary["d"] == 7, (
            "7 of the 8 allow-listed tasks report plain 'done' — "
            "storage_maintenance_subagent is the one exception (see below)"
        )
        assert summary["dq"] == 1, (
            "storage_maintenance_subagent's outcome is driven by "
            "DailyIntelTask.reports_done_queued (preserved from the real "
            "task table even though this test replaces its fn with the "
            "same no-op as every other allow-listed task), so it reports "
            "'done_queued' here too — see TestDoneQueuedOutcome for the "
            "dedicated assertion"
        )
        assert summary["s"] == 0, "skipped_for_period must be reported separately from held"
        assert summary["h"] == 13, "held must be reported separately from skipped_for_period"

    def test_bare_complete_only_when_zero_tasks_held(self, monkeypatch) -> None:
        calls: list[str] = []
        a = _task("a", _recording_fn("a", calls))
        b = _task("b", _recording_fn("b", calls))
        tasks = (a, b)
        monkeypatch.setattr(ho, "DAILY_INTEL_TASKS", tasks)
        _allow_all(monkeypatch, tasks)  # every task allowed -> zero held
        monkeypatch.setattr(ho, "DAILY_INTEL_CYCLE_BUDGET_SECONDS", 30)

        state = OperatorState()
        results: dict[str, Any] = {}
        ho._run_daily_intel_block(MagicMock(), state, NOW, results)

        assert state.daily_intel_period_outcome == "complete", (
            "zero held tasks is the only case where the bare 'complete' "
            "value (no '_for_enabled_tasks' suffix) is used"
        )


# ─── Abandonment truth (deliverable B) ───────────────────────────────────


class TestAbandonmentDoesNotPreventTaskEffects:
    def test_abandoned_task_performs_its_effect_but_ledger_stays_unchanged(
        self, monkeypatch
    ) -> None:
        """A task that blocks past its budget, then performs a fake write
        AFTER _run_with_timeout has already given up on it, must still be
        seen to have performed that write — the attempt-token check only
        withholds the LEDGER update, it does not, and cannot, stop the
        orphaned thread from doing its own work (see
        _run_daily_intel_block's "Abandonment truth" docstring)."""
        release = threading.Event()
        fake_table: list[str] = []  # stands in for a real DB write
        worker_finished = threading.Event()

        def slow_then_writes(
            engine: Any, state: OperatorState, now: datetime, results: dict[str, Any]
        ) -> None:
            release.wait(timeout=5.0)  # blocks well past its own budget
            fake_table.append("row-written-by-abandoned-worker")
            worker_finished.set()

        tasks = (_task("slow_writer", slow_then_writes, budget_s=0.05),)
        monkeypatch.setattr(ho, "DAILY_INTEL_TASKS", tasks)
        _allow_all(monkeypatch, tasks)
        monkeypatch.setattr(ho, "DAILY_INTEL_CYCLE_BUDGET_SECONDS", 30)

        state = OperatorState()
        results: dict[str, Any] = {}
        ho._run_daily_intel_block(MagicMock(), state, NOW, results)

        # _run_with_timeout already returned ok=False synchronously — the
        # ledger has NOT recorded this task as done, and no attempt token
        # was ever "cancelled": the worker is simply abandoned, still
        # running, blocked on `release`.
        assert state.daily_intel_done.get("slow_writer") is None
        assert fake_table == [], "the write has not happened yet — worker is still blocked"

        # Let the abandoned worker actually perform its write.
        release.set()
        assert worker_finished.wait(timeout=2.0), "abandoned worker never completed"

        # The effect happened — nothing in this design prevents an
        # abandoned task from doing its own work.
        assert fake_table == ["row-written-by-abandoned-worker"], (
            "an abandoned (timed-out) task's own effects are NOT prevented "
            "— only its ledger/result publication is suppressed"
        )
        # ...but the ledger was never updated by that late completion —
        # this is the part that IS prevented (late-publish guard).
        assert state.daily_intel_done.get("slow_writer") is None
        assert state.daily_intel_task_outcome.get("slow_writer") != "done"
        assert "slow_writer" not in results
