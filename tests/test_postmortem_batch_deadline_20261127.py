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
    # t=0 trade 1, t=60 trade 2, t=120 >= deadline: no prediction is started.
    assert [i for i, _ in started] == [1, 2]


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
