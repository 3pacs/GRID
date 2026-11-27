"""The postmortem batch ends inside its budget instead of timing out and finishing late (#959 follow-up).

Journal 2026-11-26/27: 20 LLM-narrated rows took 542.8 s and 412.2 s against a 180 s step budget, so the step
timed out every night, was blacklisted for 24 h and finished as done_late with its results never published.
The batch now takes a deadline after which it stops STARTING rows; the budget is sized to the work.
"""
from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DB_PASSWORD", "ci_placeholder_pw")

import intelligence.postmortem as pm_mod
from scripts import hermes_operator as ho


class _Clock:
    def __init__(self, start=0.0):
        self.now = start

    def monotonic(self):
        return self.now


def _batch(monkeypatch, *, trade_ids=(), pred_ids=(), per_row_s=40.0, deadline=None, limit=None):
    clock = _Clock()
    monkeypatch.setattr(pm_mod, "time", SimpleNamespace(monotonic=clock.monotonic))
    monkeypatch.setattr(pm_mod, "_ensure_tables", lambda _e: None)
    monkeypatch.setattr(pm_mod, "_log_aggregate_patterns", lambda *_a, **_k: None)

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, stmt, params=None):
            sql = " ".join(str(stmt).split())
            res = MagicMock()
            if "FROM options_recommendations" in sql:
                res.fetchall.return_value = [(i,) for i in trade_ids]
            elif "oracle_predictions" in sql:
                res.fetchall.return_value = [(i,) for i in pred_ids]
            else:
                res.fetchall.return_value = []
            return res

    engine = MagicMock()
    engine.connect.side_effect = lambda: _Conn()
    started: list = []

    def _gen(_engine, ident):
        started.append((ident, clock.now))
        clock.now += per_row_s
        return SimpleNamespace(ticker="X", failure_category="c", actual_return=-0.1, which_signals_were_wrong=[])

    monkeypatch.setattr(pm_mod, "generate_postmortem", _gen)
    monkeypatch.setattr(pm_mod, "generate_prediction_postmortem", _gen)
    result = pm_mod.batch_postmortem(engine, days=30, limit=limit, deadline=deadline)
    return result, started


def test_no_deadline_runs_every_selected_row(monkeypatch):
    result, started = _batch(monkeypatch, pred_ids=["a", "b", "c", "d"])
    assert len(result) == 4 and [i for i, _ in started] == ["a", "b", "c", "d"]


def test_rows_stop_being_started_at_the_deadline_and_a_running_row_finishes(monkeypatch):
    # 40 s per row, deadline at t=100: rows start at 0, 40, 80 (80 < 100, so it runs to 120), the next would start at 120.
    result, started = _batch(monkeypatch, pred_ids=["a", "b", "c", "d", "e"], deadline=100.0)
    assert [i for i, _ in started] == ["a", "b", "c"]
    assert len(result) == 3


def test_the_deadline_applies_across_trades_and_predictions(monkeypatch):
    result, started = _batch(monkeypatch, trade_ids=[1, 2], pred_ids=["a", "b"], per_row_s=60.0, deadline=100.0)
    # Sources alternate: t=0 trade 1, t=60 prediction a, t=120 >= deadline: nothing else starts.
    assert [i for i, _ in started] == [1, "a"]


def test_a_long_trade_queue_cannot_starve_predictions(monkeypatch):
    # Codex: trades consumed the whole window before predictions could start.
    result, started = _batch(monkeypatch, trade_ids=list(range(10)), pred_ids=list("abcdefghij"), per_row_s=10.0, deadline=65.0)
    ids = [i for i, _ in started]
    assert ids == [0, "a", 1, "b", 2, "c", 3]   # alternating; the next start at t=70 is past the deadline (65)
    assert any(isinstance(i, str) for i in ids) and any(isinstance(i, int) for i in ids)


def test_unequal_queues_still_drain_the_longer_one(monkeypatch):
    result, started = _batch(monkeypatch, trade_ids=[1], pred_ids=["a", "b", "c"], per_row_s=1.0, deadline=1e9)
    assert [i for i, _ in started] == [1, "a", "b", "c"]


def test_an_already_passed_deadline_starts_nothing(monkeypatch):
    result, started = _batch(monkeypatch, pred_ids=["a", "b"], deadline=-1.0)
    assert started == [] and result == []


def test_the_deadline_still_respects_the_row_limit_query_and_other_arguments(monkeypatch):
    result, started = _batch(monkeypatch, pred_ids=["a", "b", "c"], deadline=1e9, limit=2)
    assert len(started) == 3      # the fake returns every id; LIMIT is applied in SQL, not here


def test_the_postmortem_step_hands_the_batch_a_deadline_inside_its_budget(monkeypatch):
    seen: dict = {}
    clock = _Clock(1000.0)
    monkeypatch.setattr(ho, "time", SimpleNamespace(monotonic=clock.monotonic, time=lambda: 0.0))

    def _fake_run(name, fn, state, engine, **kwargs):
        seen.update(name=name, fn=fn, kwargs=kwargs)
        return []

    monkeypatch.setattr(ho, "_run_intel_task", _fake_run)
    monkeypatch.setattr(ho, "_daily_intel_raise_if_task_status_failed", lambda *_a, **_k: None)
    results: dict = {}
    ho._daily_intel_postmortem_batch(MagicMock(), MagicMock(), None, results)
    assert seen["name"] == "postmortem_batch" and seen["fn"] is pm_mod.batch_postmortem
    assert seen["kwargs"]["limit"] == ho.POSTMORTEM_BATCH_LIMIT
    assert seen["kwargs"]["deadline"] == 1000.0 + ho.DAILY_INTEL_POSTMORTEM_TASK_BUDGET_S - ho.DAILY_INTEL_POSTMORTEM_DEADLINE_MARGIN_S


def test_the_budget_is_sized_to_the_work_and_fits_the_daily_intel_cycle():
    assert ho.DAILY_INTEL_POSTMORTEM_TASK_BUDGET_S > ho.DAILY_INTEL_LLM_TASK_BUDGET_S      # 180 s timed out every night
    assert ho.DAILY_INTEL_POSTMORTEM_TASK_BUDGET_S <= ho.DAILY_INTEL_CYCLE_BUDGET_SECONDS  # a task must fit a cycle
    assert ho.DAILY_INTEL_POSTMORTEM_DEADLINE_MARGIN_S >= 45                               # one worst-case LLM row (~30 s observed)
    assert ho.DAILY_INTEL_POSTMORTEM_DEADLINE_MARGIN_S < ho.DAILY_INTEL_POSTMORTEM_TASK_BUDGET_S
    task = next(t for t in ho.DAILY_INTEL_TASKS if t.name == "postmortem_batch")
    assert task.budget_s == ho.DAILY_INTEL_POSTMORTEM_TASK_BUDGET_S
    assert ho.DAILY_INTEL_TASKS[-1].name == "postmortem_batch"      # still last: it must not delay the others


# ── falling behind must be visible: the run logs how many eligible rows are still waiting ───────────────

def _batch_with_counts(monkeypatch, counts, *, count_fails=False):
    """batch_postmortem on an engine whose bounded COUNT(*) queries return ``counts`` per source."""
    clock = _Clock()
    monkeypatch.setattr(pm_mod, "time", SimpleNamespace(monotonic=clock.monotonic))
    monkeypatch.setattr(pm_mod, "_ensure_tables", lambda _e: None)
    monkeypatch.setattr(pm_mod, "_log_aggregate_patterns", lambda *_a, **_k: None)
    seen_sql: list[str] = []

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, stmt, params=None):
            sql = " ".join(str(stmt).split())
            seen_sql.append(sql)
            res = MagicMock()
            if sql.startswith("SELECT COUNT(*) FROM ("):
                if count_fails:
                    raise RuntimeError("count blip")
                res.scalar.return_value = counts["trades" if "options_recommendations" in sql else "predictions"]
            else:
                res.fetchall.return_value = []
            return res

    engine = MagicMock()
    engine.connect.side_effect = lambda: _Conn()
    fake_log = MagicMock()
    monkeypatch.setattr(pm_mod, "log", fake_log)
    result = pm_mod.batch_postmortem(engine, days=30, limit=20, deadline=100.0)
    complete = [c for c in fake_log.info.call_args_list if "Batch post-mortem complete" in c.args[0]]
    assert len(complete) == 1
    return result, complete[0].kwargs["w"], seen_sql


def test_the_run_reports_how_many_eligible_rows_are_still_waiting(monkeypatch):
    result, waiting, sql = _batch_with_counts(monkeypatch, {"trades": 0, "predictions": 37})
    assert result == []
    assert waiting == "trades=0, predictions=37"
    counts = [q for q in sql if q.startswith("SELECT COUNT(*) FROM (")]
    assert len(counts) == 2
    # Bounded, so a nightly step never counts the whole corpus; and it is the same selection the batch uses.
    assert all(q.endswith("LIMIT 1001) AS waiting") for q in counts)
    assert any("options_recommendations" in q and "actual_return" in q for q in counts)
    assert any("oracle_predictions" in q and "pnl_pct" in q for q in counts)


def test_a_large_backlog_reads_as_at_least_the_bound(monkeypatch):
    _result, waiting, _sql = _batch_with_counts(monkeypatch, {"trades": 5, "predictions": 1001})
    assert waiting == "trades=5, predictions=1000+"


def test_a_failed_count_reads_unknown_and_never_fails_the_batch(monkeypatch):
    result, waiting, _sql = _batch_with_counts(monkeypatch, {}, count_fails=True)
    assert result == []
    assert waiting == "trades=unknown, predictions=unknown"


# ── Codex review 6085439290: one absolute deadline for work AND diagnostics ─────────────────────────────────

def _timed_batch(monkeypatch, *, row_s, count_s, rows, deadline, finish_by):
    """batch_postmortem on a fake clock where every row costs row_s and every COUNT costs count_s."""
    clock = _Clock()
    monkeypatch.setattr(pm_mod, "time", SimpleNamespace(monotonic=clock.monotonic))
    monkeypatch.setattr(pm_mod, "_ensure_tables", lambda _e: None)
    monkeypatch.setattr(pm_mod, "_log_aggregate_patterns", lambda *_a, **_k: None)
    statements: list[str] = []

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, stmt, params=None):
            sql = " ".join(str(stmt).split())
            statements.append(sql)
            res = MagicMock()
            if sql.startswith("SELECT COUNT(*) FROM ("):
                clock.now += count_s
                res.scalar.return_value = 7
            elif "oracle_predictions" in sql:
                res.fetchall.return_value = [(i,) for i in rows]
            else:
                res.fetchall.return_value = []
            return res

    engine = MagicMock()
    engine.connect.side_effect = lambda: _Conn()

    def _gen(_e, ident):
        clock.now += row_s
        return SimpleNamespace(ticker="X", failure_category="c", actual_return=-0.1, which_signals_were_wrong=[])

    monkeypatch.setattr(pm_mod, "generate_prediction_postmortem", _gen)
    monkeypatch.setattr(pm_mod, "generate_postmortem", _gen)
    fake_log = MagicMock()
    monkeypatch.setattr(pm_mod, "log", fake_log)
    result = pm_mod.batch_postmortem(engine, days=30, limit=20, deadline=deadline, finish_by=finish_by)
    complete = [c for c in fake_log.info.call_args_list if "Batch post-mortem complete" in c.args[0]]
    return result, clock.now, complete[0].kwargs["w"], statements


def test_a_near_limit_row_plus_slow_counts_still_finishes_inside_the_wrapper(monkeypatch):
    # Codex's reproduction: five 59 s rows (last starts at t=236 < the 240 deadline) and two 7 s counts finished at
    # t=309 against the 300 s wrapper. Now the counts do not start when they cannot finish before finish_by=295.
    result, elapsed, waiting, statements = _timed_batch(
        monkeypatch, row_s=59.0, count_s=7.0, rows=list(range(30)), deadline=240.0, finish_by=295.0)
    assert len(result) == 5
    assert elapsed <= 300.0
    assert waiting == "trades=skipped (no time left), predictions=skipped (no time left)"
    assert not any(q.startswith("SELECT COUNT(*) FROM (") for q in statements)


def test_the_counts_run_when_there_is_time_and_each_is_bounded_by_a_statement_timeout(monkeypatch):
    result, elapsed, waiting, statements = _timed_batch(
        monkeypatch, row_s=10.0, count_s=2.0, rows=list(range(3)), deadline=240.0, finish_by=295.0)
    assert len(result) == 3
    assert waiting == "trades=7, predictions=7"
    assert elapsed == 30.0 + 4.0
    for idx, q in enumerate(statements):
        if q.startswith("SELECT COUNT(*) FROM ("):
            assert statements[idx - 1] == "SET LOCAL statement_timeout = '3000ms'"


def test_without_finish_by_the_counts_still_run_for_callers_that_pass_no_budget(monkeypatch):
    result, _elapsed, waiting, _s = _timed_batch(
        monkeypatch, row_s=1.0, count_s=1.0, rows=[1], deadline=None, finish_by=None)
    assert waiting == "trades=7, predictions=7"


def test_the_postmortem_step_passes_a_finish_by_inside_the_wrapper_budget(monkeypatch):
    seen: dict = {}
    clock = _Clock(500.0)
    monkeypatch.setattr(ho, "time", SimpleNamespace(monotonic=clock.monotonic, time=lambda: 0.0))
    monkeypatch.setattr(ho, "_run_intel_task", lambda name, fn, state, engine, **kw: seen.update(kw) or [])
    monkeypatch.setattr(ho, "_daily_intel_raise_if_task_status_failed", lambda *_a, **_k: None)
    ho._daily_intel_postmortem_batch(MagicMock(), MagicMock(), None, {})
    assert seen["finish_by"] == 500.0 + ho.DAILY_INTEL_POSTMORTEM_TASK_BUDGET_S - ho.DAILY_INTEL_POSTMORTEM_FINISH_GUARD_S
    assert seen["deadline"] < seen["finish_by"] < 500.0 + ho.DAILY_INTEL_POSTMORTEM_TASK_BUDGET_S


# ── outer-step composition (Codex hold at add76357d): the task's allowance comes from the OUTER remainder ──────────

import inspect  # noqa: E402

from datetime import datetime, timezone  # noqa: E402

from scripts.hermes_health import OperatorState  # noqa: E402

_NOW = datetime(2026, 11, 27, 2, 5, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _clean_allowance_and_in_flight():
    ho._DAILY_INTEL_TASK_ALLOWANCE_S.clear()
    ho._DAILY_INTEL_IN_FLIGHT.clear()
    yield
    ho._DAILY_INTEL_TASK_ALLOWANCE_S.clear()
    ho._DAILY_INTEL_IN_FLIGHT.clear()


def _driver(monkeypatch, *, elapsed_in_step, work_s=0.0, outer=None):
    """Run the real driver over the real postmortem task on a synthetic clock.

    ``elapsed_in_step`` is how much of the outer step is already gone when the driver reaches the task (scorer +
    earlier daily tasks). Returns (state, calls) where calls holds the wrapper timeout the driver passed, the kwargs
    the batch was handed, and the clock value when the task ended.
    """
    clock = _Clock(10_000.0)
    step_started = clock.now
    monkeypatch.setattr(ho, "time", SimpleNamespace(monotonic=clock.monotonic, time=lambda: 0.0))
    pm_task = next(t for t in ho.DAILY_INTEL_TASKS if t.name == "postmortem_batch")
    monkeypatch.setattr(ho, "DAILY_INTEL_TASKS", (pm_task,))
    calls: dict = {}

    def _fake_wrapper(name, fn, timeout_s, state):
        calls["wrapper_timeout"] = timeout_s
        calls["wrapper_started"] = clock.now
        fn()
        clock.now += work_s
        calls["ended"] = clock.now
        return None, True

    monkeypatch.setattr(ho, "_run_with_timeout", _fake_wrapper)
    monkeypatch.setattr(ho, "_run_intel_task", lambda name, fn, state, engine, **kw: calls.update(batch=kw) or [])
    monkeypatch.setattr(ho, "_daily_intel_raise_if_task_status_failed", lambda *_a, **_k: None)
    clock.now = step_started + elapsed_in_step
    deadline = step_started + (ho.INTELLIGENCE_TASKS_TIMEOUT_SECONDS if outer is None else outer)
    state = OperatorState()
    ho._run_daily_intel_block(MagicMock(), state, _NOW, {}, step_deadline=deadline)
    calls["outer_deadline"] = deadline
    return state, calls


def test_worst_case_admission_is_capped_to_the_outer_remainder(monkeypatch):
    # 240 s scorer + 479 s block = 719 s gone; the old code granted the full 300 s (-> 1019 s)
    state, calls = _driver(monkeypatch, elapsed_in_step=240 + 479, work_s=0.0)
    left = ho.INTELLIGENCE_TASKS_TIMEOUT_SECONDS - (240 + 479) - ho.DAILY_INTEL_OUTER_STEP_RESERVE_S
    assert calls["wrapper_timeout"] == left == 121
    assert calls["wrapper_timeout"] < ho.DAILY_INTEL_POSTMORTEM_TASK_BUDGET_S
    started = calls["wrapper_started"]
    assert started + calls["wrapper_timeout"] <= calls["outer_deadline"] - ho.DAILY_INTEL_OUTER_STEP_RESERVE_S
    # the batch's own deadline and finish_by are placed from that allowance, not from the 300 s constant
    assert calls["batch"]["deadline"] == started + 121 - ho.DAILY_INTEL_POSTMORTEM_DEADLINE_MARGIN_S
    assert calls["batch"]["finish_by"] == started + 121 - ho.DAILY_INTEL_POSTMORTEM_FINISH_GUARD_S
    assert state.daily_intel_task_outcome["postmortem_batch"] == "done"


def test_the_task_is_deferred_not_started_when_too_little_of_the_outer_step_is_left(monkeypatch):
    state, calls = _driver(monkeypatch, elapsed_in_step=240 + 479 + 45)     # 116 s left after the reserve < 120 minimum
    assert "wrapper_timeout" not in calls and "batch" not in calls
    # a deferral is not an attempt and not an outcome: it just runs next cycle
    assert state.daily_intel_attempts.get("postmortem_batch", 0) == 0
    assert "postmortem_batch" not in state.daily_intel_done
    assert "postmortem_batch" not in state.daily_intel_skipped_for_period
    assert "postmortem_batch" not in state.daily_intel_task_outcome


def test_every_admitted_start_leaves_the_reserve_inside_the_outer_step(monkeypatch):
    admitted = deferred = 0
    for elapsed in range(0, 900, 15):
        ho._DAILY_INTEL_IN_FLIGHT.clear()
        _state, calls = _driver(monkeypatch, elapsed_in_step=elapsed)
        if "wrapper_timeout" in calls:
            admitted += 1
            assert calls["wrapper_started"] + calls["wrapper_timeout"] <= calls["outer_deadline"] - ho.DAILY_INTEL_OUTER_STEP_RESERVE_S
            assert calls["wrapper_timeout"] >= ho.DAILY_INTEL_POSTMORTEM_MIN_ALLOWANCE_S
            assert calls["wrapper_timeout"] <= ho.DAILY_INTEL_POSTMORTEM_TASK_BUDGET_S
        else:
            deferred += 1
    assert admitted and deferred                                              # both regimes are exercised


def test_an_early_step_still_gets_the_full_budget(monkeypatch):
    _state, calls = _driver(monkeypatch, elapsed_in_step=0)
    assert calls["wrapper_timeout"] == ho.DAILY_INTEL_POSTMORTEM_TASK_BUDGET_S


def test_without_an_outer_deadline_the_driver_keeps_the_task_budget(monkeypatch):
    clock = _Clock(0.0)
    monkeypatch.setattr(ho, "time", SimpleNamespace(monotonic=clock.monotonic, time=lambda: 0.0))
    pm_task = next(t for t in ho.DAILY_INTEL_TASKS if t.name == "postmortem_batch")
    monkeypatch.setattr(ho, "DAILY_INTEL_TASKS", (pm_task,))
    seen: dict = {}
    monkeypatch.setattr(ho, "_run_with_timeout", lambda name, fn, timeout_s, state: seen.update(t=timeout_s) or (None, True))
    ho._run_daily_intel_block(MagicMock(), OperatorState(), _NOW, {})
    assert seen["t"] == ho.DAILY_INTEL_POSTMORTEM_TASK_BUDGET_S


def test_only_the_postmortem_task_is_bounded_and_the_step_passes_its_deadline():
    assert [t.name for t in ho.DAILY_INTEL_TASKS if t.bounded_by_outer] == ["postmortem_batch"]
    src = inspect.getsource(ho.run_intelligence_tasks)
    assert "step_deadline = time.monotonic() + INTELLIGENCE_TASKS_TIMEOUT_SECONDS" in src
    assert "step_deadline=step_deadline" in src


def test_the_minimum_allowance_covers_the_margin_and_fits_the_budget():
    assert ho.DAILY_INTEL_POSTMORTEM_MIN_ALLOWANCE_S > ho.DAILY_INTEL_POSTMORTEM_DEADLINE_MARGIN_S
    assert ho.DAILY_INTEL_POSTMORTEM_MIN_ALLOWANCE_S <= ho.DAILY_INTEL_POSTMORTEM_TASK_BUDGET_S
