"""Weekly intelligence block as its own due-period, resumable Hermes step
(2026-11-22).

Production (griddb hermes_operator snapshots, read-only, 2026-11-21): the
weekly block ran only when the 900 s ``intelligence_tasks`` step STARTED
inside Sunday 03:00-03:10 UTC. Cycles start 5-30 min apart, so that window
was often missed (2026-11-08: never started). On 2026-11-15 it started at
03:02, the cross-reference check and the lever report finished, the trust
and actor reports never recorded, and ``last_weekly_intel`` stayed at
2026-11-01. The supply-chain edge validator, the last task, never ran from
Hermes (``last_validation_at`` max 2026-04-13).

These tests pin the fix:
  * a due block runs on the first cycle after the Sunday 03:00 UTC boundary,
    not only inside a 10-minute window; a block that is not due does not run;
  * an interrupted block resumes the tasks without a final outcome, also
    across a restart (the ledger is persisted on OperatorState);
  * a hung task times out, is recorded, and the validator still runs;
  * the daily block still runs first, inside intelligence_tasks, and the
    weekly tasks are no longer in that step.
"""
from __future__ import annotations

import inspect
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from scripts import hermes_operator as ho
from scripts.hermes_health import OperatorState

# Sunday 2026-11-22 03:47 UTC: after the boundary, outside the old
# 03:00-03:10 start window.
FIXED_NOW = datetime(2026, 11, 22, 3, 47, tzinfo=timezone.utc)
LAST_COMPLETE = datetime(2026, 11, 1, 3, 3, 55, tzinfo=timezone.utc)  # live value on 2026-11-21


class _FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):  # noqa: D401 — datetime API
        return FIXED_NOW if tz is not None else FIXED_NOW.replace(tzinfo=None)


def _hours(ts_: datetime | None) -> float:
    return 999.0 if ts_ is None else (FIXED_NOW - ts_).total_seconds() / 3600


def _minutes(ts_: datetime | None) -> float:
    return 99999.0 if ts_ is None else (FIXED_NOW - ts_).total_seconds() / 60


@pytest.fixture(autouse=True)
def _isolated_registries(monkeypatch):
    monkeypatch.setattr(ho, "_WEEKLY_INTEL_IN_FLIGHT", {})
    monkeypatch.setattr(ho, "_TRUST_CYCLE_WORKER", {"thread": None})
    monkeypatch.setattr(ho, "_TRUST_CYCLE_PROGRESS", {})


@pytest.fixture
def clock(monkeypatch):
    monkeypatch.setattr(ho, "datetime", _FixedDateTime)
    monkeypatch.setattr(ho, "_hours_since", _hours)
    monkeypatch.setattr(ho, "_minutes_since", _minutes)
    return FIXED_NOW


def _recording(name: str, calls: list[str], *, fail: bool = False,
               block: threading.Event | None = None, sleep_s: float = 0.0):
    def _fn(engine: Any, state: OperatorState) -> Any:
        calls.append(name)
        if sleep_s:
            time.sleep(sleep_s)
        if block is not None:
            block.wait(10)
        if fail:
            raise RuntimeError(f"{name} boom")
        return {"task": name}

    return _fn


def _tasks(monkeypatch, *specs: ho.WeeklyIntelTask) -> None:
    monkeypatch.setattr(ho, "WEEKLY_INTEL_TASKS", tuple(specs))


def _six(calls: list[str], *, budget_s: float = 1.0, sleep_s: float = 0.0,
         **overrides) -> list[ho.WeeklyIntelTask]:
    """Six fake tasks named like the real table."""
    out = []
    for t in ho.WEEKLY_INTEL_TASKS:
        fn = overrides.get(t.name) or _recording(t.name, calls, sleep_s=sleep_s)
        out.append(ho.WeeklyIntelTask(t.name, fn, budget_s, t.waits_for_trust_cycle))
    return out


def _wait_until(predicate, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return bool(predicate())


VALIDATOR = "weekly_supply_chain_edge_validator"


# ─── due rule ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize("now,last,due", [
    # old window missed: Sunday 03:47, last completed run 3 weeks ago
    (FIXED_NOW, LAST_COMPLETE, True),
    # Monday after a Sunday with no run at all
    (datetime(2026, 11, 23, 9, 0, tzinfo=timezone.utc), LAST_COMPLETE, True),
    # never completed
    (FIXED_NOW, None, True),
    # completed this period (Sunday 03:30): not due again until next Sunday
    (datetime(2026, 11, 22, 20, 0, tzinfo=timezone.utc),
     datetime(2026, 11, 22, 3, 30, tzinfo=timezone.utc), False),
    (datetime(2026, 11, 29, 2, 59, tzinfo=timezone.utc),
     datetime(2026, 11, 22, 3, 30, tzinfo=timezone.utc), False),
    (datetime(2026, 11, 29, 3, 0, tzinfo=timezone.utc),
     datetime(2026, 11, 22, 3, 30, tzinfo=timezone.utc), True),
    # completed late (Monday 05:00): the 160 h guard holds the next period
    # until 160 h have passed, then it is due
    (datetime(2026, 11, 29, 3, 30, tzinfo=timezone.utc),
     datetime(2026, 11, 23, 5, 0, tzinfo=timezone.utc), False),
    (datetime(2026, 11, 29, 21, 0, tzinfo=timezone.utc),
     datetime(2026, 11, 23, 5, 0, tzinfo=timezone.utc), True),
    # Saturday before the boundary, last run the previous Sunday
    (datetime(2026, 11, 28, 23, 0, tzinfo=timezone.utc),
     datetime(2026, 11, 22, 3, 5, tzinfo=timezone.utc), False),
])
def test_weekly_due_rule(now, last, due):
    assert ho.weekly_intel_due(last, now) is due


def test_period_start_is_most_recent_sunday_0300_utc():
    assert ho.weekly_intel_period_start(FIXED_NOW) == datetime(2026, 11, 22, 3, tzinfo=timezone.utc)
    assert (ho.weekly_intel_period_start(datetime(2026, 11, 22, 2, 59, tzinfo=timezone.utc))
            == datetime(2026, 11, 15, 3, tzinfo=timezone.utc))
    assert (ho.weekly_intel_period_start(datetime(2026, 11, 28, 23, 0, tzinfo=timezone.utc))
            == datetime(2026, 11, 22, 3, tzinfo=timezone.utc))


# ─── 1. due but the old window missed: it now runs ────────────────────────


def test_due_block_outside_old_window_runs_every_task_and_marks_completion(clock, monkeypatch):
    calls: list[str] = []
    _tasks(monkeypatch, *_six(calls))
    state = OperatorState()
    state.last_weekly_intel = LAST_COMPLETE
    cycle_result: dict[str, Any] = {}

    ho._run_weekly_intel_step(MagicMock(), state, False, cycle_result)

    assert calls == [t.name for t in ho.WEEKLY_INTEL_TASKS]
    assert VALIDATOR in calls
    summary = cycle_result["weekly_intel"]
    assert summary["complete"] is True and summary["period"] == "2026-11-22"
    assert summary["results"][VALIDATOR] == {"task": VALIDATOR}
    assert state.last_weekly_intel == FIXED_NOW
    assert state.weekly_intel_period_outcome == "complete"
    assert set(state.weekly_intel_outcome.values()) == {"done"}


def test_real_task_table_calls_the_same_functions_with_the_same_arguments(clock, monkeypatch):
    """The wrappers keep each weekly task's call as it was."""
    from ingestion.altdata import supply_chain_parser
    from intelligence import (
        actor_network, cross_reference, lever_pullers, supply_chain_edge_validator, trust_scorer,
    )

    seen: list[tuple[str, tuple, dict]] = []

    def rec(label):
        def _f(*args, **kwargs):
            seen.append((label, args, kwargs))
            return label
        return _f

    monkeypatch.setattr(cross_reference, "run_all_checks", rec("cross_reference"))
    monkeypatch.setattr(lever_pullers, "generate_lever_report", rec("lever"))
    monkeypatch.setattr(trust_scorer, "generate_trust_report", rec("trust"))
    monkeypatch.setattr(actor_network, "generate_actor_report", rec("actor"))
    monkeypatch.setattr(supply_chain_parser, "run_weekly", rec("parser"))
    monkeypatch.setattr(supply_chain_edge_validator, "run_weekly", rec("validator"))

    engine = MagicMock()
    state = OperatorState()
    state.last_weekly_intel = LAST_COMPLETE
    out: dict[str, Any] = {}
    ho._run_weekly_intel_step(engine, state, False, out)

    assert seen == [
        ("cross_reference", (engine,), {"skip_narrative": False}),
        ("lever", (engine,), {}),
        ("trust", (engine,), {}),
        ("actor", (engine,), {}),
        ("parser", (engine,), {}),
        ("validator", (engine,), {}),
    ]
    assert out["weekly_intel"]["complete"] is True
    # task_status keys are unchanged (the hermes-status endpoint reads them)
    for t in ho.WEEKLY_INTEL_TASKS:
        assert state.task_status[t.name]["success"] is True


# ─── 2. not due: does not run ─────────────────────────────────────────────


def test_not_due_does_not_run(clock, monkeypatch):
    calls: list[str] = []
    _tasks(monkeypatch, *_six(calls))
    state = OperatorState()
    state.last_weekly_intel = datetime(2026, 11, 22, 3, 20, tzinfo=timezone.utc)  # completed this period
    out: dict[str, Any] = {}
    ho._run_weekly_intel_step(MagicMock(), state, False, out)
    assert calls == [] and "weekly_intel" not in out
    assert state.last_weekly_intel == datetime(2026, 11, 22, 3, 20, tzinfo=timezone.utc)


def test_dry_run_does_not_run(clock, monkeypatch):
    calls: list[str] = []
    _tasks(monkeypatch, *_six(calls))
    state = OperatorState()
    ho._run_weekly_intel_step(MagicMock(), state, True, {})
    assert calls == [] and state.last_weekly_intel is None


# ─── 3. an interrupted block resumes the remaining tasks ──────────────────


def test_budget_cut_block_resumes_remaining_tasks_next_cycle(clock, monkeypatch):
    calls: list[str] = []
    # Budget 1 s, run 0.3 s: in a 1.5 s cycle budget the first starts at 0,
    # the second at ~0.3 (0.3 + 1 <= 1.5), the third would start at ~0.6
    # (0.6 + 1 > 1.5) and waits for the next cycle.
    _tasks(monkeypatch, *_six(calls, sleep_s=0.3))
    monkeypatch.setattr(ho, "WEEKLY_INTEL_CYCLE_BUDGET_SECONDS", 1.5)
    state = OperatorState()
    state.last_weekly_intel = LAST_COMPLETE
    names = [t.name for t in ho.WEEKLY_INTEL_TASKS]

    first: dict[str, Any] = {}
    ho._run_weekly_intel_step(MagicMock(), state, False, first)
    assert calls == names[:2]
    assert first["weekly_intel"]["waiting_for_next_cycle"] == names[2:]
    assert state.last_weekly_intel == LAST_COMPLETE, "not complete yet"

    ho._run_weekly_intel_step(MagicMock(), state, False, {})
    ho._run_weekly_intel_step(MagicMock(), state, False, {})
    assert calls == names, "each task ran once, in order"
    assert state.last_weekly_intel == FIXED_NOW
    ho._run_weekly_intel_step(MagicMock(), state, False, {})
    assert calls == names, "complete: not due again this period"


def test_restart_mid_period_resumes_from_the_persisted_ledger(clock, monkeypatch):
    """A restart (or the 11-15 kind of abandonment) loses no finished work."""
    calls: list[str] = []
    _tasks(monkeypatch, *_six(calls, sleep_s=0.3))
    monkeypatch.setattr(ho, "WEEKLY_INTEL_CYCLE_BUDGET_SECONDS", 1.5)
    before = OperatorState()
    before.last_weekly_intel = LAST_COMPLETE
    ho._run_weekly_intel_step(MagicMock(), before, False, {})
    assert len(calls) == 2

    payload = {"operator_state": before.to_dict()}
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = (payload,)
    after = OperatorState()
    assert after.hydrate_from_snapshot(engine) is True
    assert after.weekly_intel_period == "2026-11-22"
    assert after.weekly_intel_outcome == before.weekly_intel_outcome

    calls.clear()
    monkeypatch.setattr(ho, "WEEKLY_INTEL_CYCLE_BUDGET_SECONDS", 1200)
    ho._run_weekly_intel_step(MagicMock(), after, False, {})
    assert calls == [t.name for t in ho.WEEKLY_INTEL_TASKS][2:]
    assert after.last_weekly_intel == FIXED_NOW


def test_new_period_clears_the_previous_ledger(clock, monkeypatch):
    calls: list[str] = []
    _tasks(monkeypatch, *_six(calls))
    state = OperatorState()
    state.last_weekly_intel = LAST_COMPLETE
    state.weekly_intel_period = "2026-11-15"
    state.weekly_intel_outcome = {t.name: "done" for t in ho.WEEKLY_INTEL_TASKS[:4]}
    state.weekly_intel_attempts = {"weekly_trust_report": 1}
    ho._run_weekly_intel_step(MagicMock(), state, False, {})
    assert calls == [t.name for t in ho.WEEKLY_INTEL_TASKS]
    assert state.weekly_intel_period == "2026-11-22"
    assert state.weekly_intel_attempts == {}


# ─── 4. a hung task times out and the validator still runs ────────────────


def test_hung_trust_report_times_out_and_the_validator_still_runs(clock, monkeypatch):
    calls: list[str] = []
    release = threading.Event()
    tasks = _six(calls, weekly_trust_report=_recording("weekly_trust_report", calls, block=release))
    tasks[2] = tasks[2]._replace(budget_s=0.2)
    _tasks(monkeypatch, *tasks)
    state = OperatorState()
    state.last_weekly_intel = LAST_COMPLETE
    out: dict[str, Any] = {}
    try:
        ho._run_weekly_intel_step(MagicMock(), state, False, out)

        assert calls == [t.name for t in tasks], "every task after the hung one ran"
        assert state.weekly_intel_outcome["weekly_trust_report"] == "timed_out"
        assert state.weekly_intel_outcome[VALIDATOR] == "done"
        assert out["weekly_intel"]["complete"] is True
        assert state.last_weekly_intel == FIXED_NOW
        assert state.weekly_intel_period_outcome == "complete_with_failures"

        # The timeout is final for the period: no retry beside the orphan.
        state.last_weekly_intel = LAST_COMPLETE  # force due again
        ho._run_weekly_intel_step(MagicMock(), state, False, {})
        assert calls.count("weekly_trust_report") == 1
    finally:
        release.set()
    assert _wait_until(lambda: state.weekly_intel_outcome["weekly_trust_report"] == "done_late")


def test_hung_task_with_budget_left_for_nothing_else_resumes_validator_next_cycle(clock, monkeypatch):
    """With the real budgets the trust report alone fills a cycle; the
    validator gets its turn on a later cycle, not next week."""
    calls: list[str] = []
    release = threading.Event()
    tasks = _six(calls, budget_s=0.2,
                 weekly_trust_report=_recording("weekly_trust_report", calls, block=release))
    tasks[2] = tasks[2]._replace(budget_s=0.5)
    _tasks(monkeypatch, *tasks)
    monkeypatch.setattr(ho, "WEEKLY_INTEL_CYCLE_BUDGET_SECONDS", 0.65)
    state = OperatorState()
    state.last_weekly_intel = LAST_COMPLETE
    try:
        first: dict[str, Any] = {}
        ho._run_weekly_intel_step(MagicMock(), state, False, first)
        assert calls == [t.name for t in tasks[:3]]
        assert first["weekly_intel"]["waiting_for_next_cycle"] == [t.name for t in tasks[3:]]
        assert state.weekly_intel_outcome["weekly_trust_report"] == "timed_out"
        ho._run_weekly_intel_step(MagicMock(), state, False, {})
    finally:
        release.set()
    assert calls == [t.name for t in tasks]
    assert state.weekly_intel_outcome[VALIDATOR] == "done"
    assert state.weekly_intel_outcome["weekly_trust_report"] in ("timed_out", "done_late")
    assert state.last_weekly_intel == FIXED_NOW


def test_failing_task_is_retried_after_the_others_then_recorded_failed(clock, monkeypatch):
    calls: list[str] = []
    tasks = _six(calls, weekly_lever_report=_recording("weekly_lever_report", calls, fail=True))
    _tasks(monkeypatch, *tasks)
    state = OperatorState()
    state.last_weekly_intel = LAST_COMPLETE

    ho._run_weekly_intel_step(MagicMock(), state, False, {})
    assert VALIDATOR in calls, "a failing task does not hold back the rest"
    assert state.weekly_intel_outcome["weekly_lever_report"] == "retry_pending"
    assert state.last_weekly_intel == LAST_COMPLETE

    calls.clear()
    ho._run_weekly_intel_step(MagicMock(), state, False, {})
    assert calls == ["weekly_lever_report"]
    assert state.weekly_intel_outcome["weekly_lever_report"] == "failed"
    assert state.weekly_intel_attempts["weekly_lever_report"] == ho.WEEKLY_INTEL_MAX_ATTEMPTS
    assert state.last_weekly_intel == FIXED_NOW
    assert state.weekly_intel_period_outcome == "complete_with_failures"


def test_failure_swallowed_by_run_intel_task_is_visible_to_the_ledger(clock, monkeypatch):
    from intelligence import supply_chain_edge_validator

    def boom(engine):
        raise RuntimeError("validator boom")

    monkeypatch.setattr(supply_chain_edge_validator, "run_weekly", boom)
    real = {t.name: t for t in ho.WEEKLY_INTEL_TASKS}
    _tasks(monkeypatch, real[VALIDATOR])
    state = OperatorState()
    state.last_weekly_intel = LAST_COMPLETE
    ho._run_weekly_intel_step(MagicMock(), state, False, {})
    assert state.weekly_intel_outcome[VALIDATOR] == "retry_pending"
    assert state.task_status[VALIDATOR]["success"] is False


def test_trust_report_waits_for_a_running_trust_cycle_pass(clock, monkeypatch):
    calls: list[str] = []
    _tasks(monkeypatch, *_six(calls))
    monkeypatch.setattr(ho, "WEEKLY_INTEL_MAX_DEFERRALS", 2)
    hold = threading.Event()
    worker = threading.Thread(target=hold.wait, args=(10,), daemon=True)
    worker.start()
    ho._TRUST_CYCLE_WORKER["thread"] = worker.ident
    state = OperatorState()
    state.last_weekly_intel = LAST_COMPLETE
    try:
        ho._run_weekly_intel_step(MagicMock(), state, False, {})
        assert "weekly_trust_report" not in calls
        assert VALIDATOR in calls
        assert state.weekly_intel_outcome["weekly_trust_report"] == "deferred"
        assert state.last_weekly_intel == LAST_COMPLETE

        ho._TRUST_CYCLE_WORKER["thread"] = None
        ho._run_weekly_intel_step(MagicMock(), state, False, {})
        assert calls.count("weekly_trust_report") == 1
        assert state.last_weekly_intel == FIXED_NOW
        assert state.weekly_intel_period_outcome == "complete"
    finally:
        hold.set()


def test_trust_report_deferral_is_capped(clock, monkeypatch):
    calls: list[str] = []
    _tasks(monkeypatch, *_six(calls))
    monkeypatch.setattr(ho, "WEEKLY_INTEL_MAX_DEFERRALS", 2)
    monkeypatch.setattr(ho, "_trust_cycle_worker_alive", lambda: True)
    state = OperatorState()
    state.last_weekly_intel = LAST_COMPLETE
    ho._run_weekly_intel_step(MagicMock(), state, False, {})
    ho._run_weekly_intel_step(MagicMock(), state, False, {})
    assert state.weekly_intel_outcome["weekly_trust_report"] == "failed_deferred"
    assert state.last_weekly_intel == FIXED_NOW


# ─── 5. the daily block still runs first and is not affected ──────────────


def _night_state() -> OperatorState:
    """Everything recently run except the daily block (last ran yesterday)
    and the weekly block (last completed 2026-11-01)."""
    state = OperatorState()
    for name in list(vars(state)):
        if name.startswith("last_"):
            setattr(state, name, FIXED_NOW - timedelta(minutes=1))
    state.last_daily_intel = datetime(2026, 11, 21, 2, 28, 54, tzinfo=timezone.utc)
    state.last_weekly_intel = LAST_COMPLETE
    return state


def test_daily_block_runs_first_and_weekly_tasks_are_not_in_intelligence_tasks(clock, monkeypatch):
    order: list[str] = []
    monkeypatch.setattr(
        ho, "_run_daily_intel_block",
        lambda engine, state, now, results: order.append("daily"),
    )
    monkeypatch.setattr(ho, "_run_sector_health_step", lambda engine, state, dry_run: {})
    from alpha_research.edge_stream import hermes as edge_hermes
    monkeypatch.setattr(edge_hermes, "maybe_spawn_cycle", lambda now: None)
    _tasks(monkeypatch, *_six(order))
    state = _night_state()
    cycle_result: dict[str, Any] = {}

    ho._run_sector_and_intelligence_steps(MagicMock(), state, False, cycle_result)
    ho._run_trust_cycle_step(MagicMock(), state, False, cycle_result)  # not due: no-op
    ho._run_weekly_intel_step(MagicMock(), state, False, cycle_result)

    assert order == ["daily"] + [t.name for t in ho.WEEKLY_INTEL_TASKS]
    intel = cycle_result["intelligence"]
    assert intel != {"timeout": True}
    assert not any(k.startswith("weekly_") for k in intel)
    assert cycle_result["weekly_intel"]["complete"] is True
    assert state.last_daily_intel == datetime(2026, 11, 21, 2, 28, 54, tzinfo=timezone.utc), (
        "the weekly step does not touch the daily marker"
    )


def test_hung_weekly_task_does_not_hold_the_intelligence_step(clock, monkeypatch):
    daily: list[datetime] = []
    monkeypatch.setattr(
        ho, "_run_daily_intel_block",
        lambda engine, state, now, results: daily.append(now),
    )
    monkeypatch.setattr(ho, "_run_sector_health_step", lambda engine, state, dry_run: {})
    from alpha_research.edge_stream import hermes as edge_hermes
    monkeypatch.setattr(edge_hermes, "maybe_spawn_cycle", lambda now: None)
    calls: list[str] = []
    release = threading.Event()
    tasks = _six(calls, weekly_cross_reference=_recording("weekly_cross_reference", calls, block=release))
    tasks[0] = tasks[0]._replace(budget_s=0.2)
    _tasks(monkeypatch, *tasks)
    monkeypatch.setattr(ho, "INTELLIGENCE_TASKS_TIMEOUT_SECONDS", 5)
    state = _night_state()
    cycle_result: dict[str, Any] = {}
    try:
        ho._run_sector_and_intelligence_steps(MagicMock(), state, False, cycle_result)
        ho._run_weekly_intel_step(MagicMock(), state, False, cycle_result)
    finally:
        release.set()
    assert daily == [FIXED_NOW]
    assert cycle_result["intelligence"] != {"timeout": True}
    assert state.weekly_intel_outcome["weekly_cross_reference"] == "timed_out"
    assert state.weekly_intel_outcome[VALIDATOR] == "done"


def test_dispatch_order_and_old_window_is_gone():
    cycle_src = inspect.getsource(ho.run_cycle)
    assert (cycle_src.index("_run_sector_and_intelligence_steps(engine")
            < cycle_src.index("_run_trust_cycle_step(engine")
            < cycle_src.index("_run_weekly_intel_step(engine"))
    intel_src = inspect.getsource(ho.run_intelligence_tasks)
    for gone in ("is_weekly_window", "weekly_due", "last_weekly_intel = now",
                 "generate_trust_report", "supply_chain_edge_validator"):
        assert gone not in intel_src, gone
    # The daily window and catch-up gate are unchanged.
    assert "is_daily_window = (now.hour == 2 and now.minute < 10)" in intel_src
    assert "_hours_since(state.last_daily_intel) >= 20" in intel_src


# ─── budgets ──────────────────────────────────────────────────────────────


def test_every_weekly_task_fits_in_one_cycle_budget():
    for t in ho.WEEKLY_INTEL_TASKS:
        assert 0 < t.budget_s <= ho.WEEKLY_INTEL_CYCLE_BUDGET_SECONDS, t.name


def test_weekly_step_fits_in_the_cycle_beside_intelligence_and_trust():
    assert (ho.WEEKLY_INTEL_CYCLE_BUDGET_SECONDS + ho.INTELLIGENCE_TASKS_TIMEOUT_SECONDS
            + ho.TRUST_CYCLE_TIMEOUT_SECONDS) < ho.CYCLE_TIMEOUT_SECONDS


def test_parser_budget_covers_its_own_cooperative_budget():
    from ingestion.altdata.supply_chain_parser import WEEKLY_BUDGET_SECONDS
    assert ho.WEEKLY_INTEL_PARSER_BUDGET_S >= WEEKLY_BUDGET_SECONDS + 60


def test_weekly_table_order_is_the_pre_existing_order():
    assert [t.name for t in ho.WEEKLY_INTEL_TASKS] == [
        "weekly_cross_reference", "weekly_lever_report", "weekly_trust_report",
        "weekly_actor_report", "weekly_supply_chain_parser", VALIDATOR,
    ]
