"""Trust cycle as its own bounded, resumable Hermes step (2026-11-18).

Production (grid-svr, journalctl -u grid-hermes): a full trust cycle took
~39 min ("Intel task 'trust_cycle' completed in 2326.2s", 2026-11-18). It
used to run inside the 900 s ``intelligence_tasks`` step, ahead of the
daily-intel block, so the step timed out on every 4-hourly trust cycle. On
2026-11-18 the 02:09 restart put the trust cycle at 02:24:49, the step timed
out at 02:39:49 and the daily-intel block never ran. ``state.last_trust_cycle``
was set only after the orphaned worker finished, so the 02:51 cycle started a
second trust cycle beside the first.

These tests pin the fix:
  * a due daily-intel block runs even when the trust cycle takes longer than
    its step cap;
  * a timed-out trust pass does not make trust due on every cycle, and no
    second pass starts while an abandoned one is still running;
  * a pass that stops at its budget resumes from its cursor instead of
    restarting, and a whole cycle propagates each source once (the report no
    longer re-propagates).
"""
from __future__ import annotations

import inspect
import threading
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from intelligence import trust_scorer as ts
from scripts import hermes_operator as ho
from scripts.hermes_health import OperatorState

# 2026-11-18 02:24:49Z: when the trust cycle started in production that night.
FIXED_NOW = datetime(2026, 11, 18, 2, 24, 49, tzinfo=timezone.utc)


class _FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):  # noqa: D401 — datetime API
        return FIXED_NOW if tz is not None else FIXED_NOW.replace(tzinfo=None)


def _hours(ts_: datetime | None) -> float:
    return 999.0 if ts_ is None else (FIXED_NOW - ts_).total_seconds() / 3600


def _minutes(ts_: datetime | None) -> float:
    return 99999.0 if ts_ is None else (FIXED_NOW - ts_).total_seconds() / 60


@pytest.fixture
def clock(monkeypatch):
    monkeypatch.setattr(ho, "datetime", _FixedDateTime)
    monkeypatch.setattr(ho, "_hours_since", _hours)
    monkeypatch.setattr(ho, "_minutes_since", _minutes)
    monkeypatch.setattr(ho, "_TRUST_CYCLE_PROGRESS", {})
    monkeypatch.setattr(ho, "_TRUST_CYCLE_WORKER", {"thread": None})
    return FIXED_NOW


def _night_state() -> OperatorState:
    """Everything recently run except the daily block (last ran yesterday)
    and the trust cycle (due) — the 2026-11-18 02:24 production state."""
    state = OperatorState()
    for name in list(vars(state)):
        if name.startswith("last_"):
            setattr(state, name, FIXED_NOW - timedelta(minutes=1))
    state.last_daily_intel = datetime(2026, 11, 17, 2, 21, 37, tzinfo=timezone.utc)
    state.last_trust_cycle = None
    return state


class _SlowTrust:
    """Stands in for run_trust_cycle: blocks past the step cap until released."""

    def __init__(self) -> None:
        self.release = threading.Event()
        self.started = threading.Event()
        self.calls = 0

    def __call__(self, engine: Any, *, should_continue=None, progress=None) -> dict[str, Any]:
        self.calls += 1
        self.started.set()
        self.release.wait(10)
        return {"complete": True, "passes": 1, "scoring": {"scored": 0},
                "trust_update": {"total": 0}}


def _run_cycle_steps(engine, state, cycle_result) -> None:
    """The run_cycle dispatch order for these steps (step 7f, then 7f')."""
    ho._run_sector_and_intelligence_steps(engine, state, False, cycle_result)
    ho._run_trust_cycle_step(engine, state, False, cycle_result)


# ─── 1. The daily-intel block cannot be starved by the trust cycle ────────


def test_due_daily_block_runs_even_when_trust_outlasts_its_step_cap(clock, monkeypatch):
    daily_calls: list[datetime] = []
    monkeypatch.setattr(
        ho, "_run_daily_intel_block",
        lambda engine, state, now, results, **_kw: daily_calls.append(now),
    )
    monkeypatch.setattr(ho, "_run_sector_health_step", lambda engine, state, dry_run: {})
    slow = _SlowTrust()
    monkeypatch.setattr(ts, "run_trust_cycle", slow)
    monkeypatch.setattr(ho, "INTELLIGENCE_TASKS_TIMEOUT_SECONDS", 5)
    monkeypatch.setattr(ho, "TRUST_CYCLE_TIMEOUT_SECONDS", 1)

    state = _night_state()
    cycle_result: dict[str, Any] = {}
    try:
        _run_cycle_steps(MagicMock(), state, cycle_result)
        assert daily_calls == [FIXED_NOW], "the due daily-intel block must run"
        assert cycle_result.get("intelligence") != {"timeout": True}
        assert slow.calls == 1
        assert cycle_result["trust_cycle"] == {"ok": False}
        assert state.last_trust_cycle == FIXED_NOW
    finally:
        slow.release.set()


def test_daily_block_sits_ahead_of_every_periodic_task_and_trust_is_not_in_the_step():
    src = inspect.getsource(ho.run_intelligence_tasks)
    daily = src.index("_run_daily_intel_block(engine, state, now, results, step_deadline=step_deadline)")
    assert src.index("score_due_active_hypotheses") < daily
    for later in ("state.last_signal_forecasts", "state.last_options_recommendations",
                  "state.last_cross_reference_checks", "state.last_lever_pullers",
                  "state.last_actor_wealth"):
        assert daily < src.index(later), later
    assert "import run_trust_cycle" not in src and "run_trust_cycle(" not in src, (
        "the trust cycle must not run inside intelligence_tasks again"
    )
    cycle_src = inspect.getsource(ho.run_cycle)
    assert (cycle_src.index("_run_sector_and_intelligence_steps(engine")
            < cycle_src.index("_run_trust_cycle_step(engine"))


# ─── 2. A timed-out pass is not due on every cycle ────────────────────────


def test_timed_out_trust_pass_is_not_due_next_cycle_and_never_doubles_up(clock, monkeypatch):
    slow = _SlowTrust()
    monkeypatch.setattr(ts, "run_trust_cycle", slow)
    monkeypatch.setattr(ho, "TRUST_CYCLE_TIMEOUT_SECONDS", 1)
    state = _night_state()
    engine = MagicMock()
    try:
        first: dict[str, Any] = {}
        ho._run_trust_cycle_step(engine, state, False, first)
        assert first["trust_cycle"] == {"ok": False}
        assert state.last_trust_cycle == FIXED_NOW

        # Abandoned worker still running: even a forced-due state must not
        # start a second trust cycle beside it (02:51 in production).
        state.last_trust_cycle = None
        second: dict[str, Any] = {}
        ho._run_trust_cycle_step(engine, state, False, second)
        assert second["trust_cycle"] == {"skipped": "in_flight"}
        assert slow.calls == 1
    finally:
        slow.release.set()

    # Worker finished. With the due-state the timeout recorded, the next
    # cycle does nothing.
    worker_gone = threading.Event()
    for _ in range(100):
        if not ho._daily_intel_thread_alive(ho._TRUST_CYCLE_WORKER.get("thread")):
            worker_gone.set()
            break
        threading.Event().wait(0.05)
    assert worker_gone.is_set()
    state.last_trust_cycle = FIXED_NOW
    third: dict[str, Any] = {}
    ho._run_trust_cycle_step(engine, state, False, third)
    assert "trust_cycle" not in third
    assert slow.calls == 1


def test_failed_pass_holds_a_paused_cycle_until_the_interval(clock, monkeypatch):
    calls: list[dict[str, Any]] = []

    def boom(engine, *, should_continue=None, progress=None):
        calls.append(dict(progress))
        raise RuntimeError("db went away")

    monkeypatch.setattr(ts, "run_trust_cycle", boom)
    ho._TRUST_CYCLE_PROGRESS.update({"stage": "propagate", "resume_after": ["news", "m"]})
    state = _night_state()
    state.last_trust_cycle = FIXED_NOW - timedelta(minutes=30)  # paused cycle in progress
    out: dict[str, Any] = {}
    ho._run_trust_cycle_step(MagicMock(), state, False, out)
    assert out["trust_cycle"] == {"ok": False}
    assert state.last_trust_cycle == FIXED_NOW
    again: dict[str, Any] = {}
    ho._run_trust_cycle_step(MagicMock(), state, False, again)
    assert len(calls) == 1 and "trust_cycle" not in again


# ─── 3. Budgeted passes resume; completion sets the due-state ─────────────


def test_paused_pass_resumes_next_cycle_and_completion_sets_due_state(clock, monkeypatch):
    seen: list[Any] = []

    def fake(engine, *, should_continue=None, progress=None):
        assert should_continue is not None and should_continue()
        seen.append(progress.get("resume_after"))
        if not progress:
            progress.update({"stage": "propagate", "resume_after": ["news", "b"], "passes": 1})
            return {"complete": False, "passes": 1, "propagated_sources": 2,
                    "resume_after": ["news", "b"], "scoring": {"scored": 3}}
        progress.clear()
        return {"complete": True, "passes": 2, "scoring": {"scored": 3},
                "trust_update": {"total": 3, "sources": [{} for _ in range(3)]},
                "convergence_count": 0, "actor_trust_synced": 0, "report": "x" * 10_000}

    monkeypatch.setattr(ts, "run_trust_cycle", fake)
    state = _night_state()
    previous = state.last_trust_cycle = FIXED_NOW - timedelta(hours=5)

    first: dict[str, Any] = {}
    ho._run_trust_cycle_step(MagicMock(), state, False, first)
    assert first["trust_cycle"]["complete"] is False
    assert state.last_trust_cycle == previous, "a paused cycle is not finished"

    state.last_trust_cycle = FIXED_NOW - timedelta(minutes=20)  # interval not passed
    second: dict[str, Any] = {}
    ho._run_trust_cycle_step(MagicMock(), state, False, second)
    assert seen == [None, ["news", "b"]]
    assert second["trust_cycle"]["complete"] is True
    assert second["trust_cycle"]["sources"] == 3
    assert "report" not in second["trust_cycle"], "snapshot payload stays small"
    assert state.last_trust_cycle == FIXED_NOW


# ─── 4. trust_scorer: resumable propagation, one propagation per cycle ────


def _stub_scorer(monkeypatch, n_sources: int = 5) -> dict[str, list]:
    calls: dict[str, list] = {"score": [], "forward": [], "actor": [], "propagate": [],
                              "converge": [], "order": []}
    sources = [
        {"source_type": "news", "source_id": chr(ord("a") + i), "trust_score": 0.5 + i / 100,
         "hit_count": 1, "miss_count": 1, "avg_lead_time_hours": 1.0, "win_rate": 0.5,
         "total_signals": 2, "best_ticker": ""}
        for i in range(n_sources)
    ]
    ranked = sorted(sources, key=lambda s: -s["trust_score"])
    for rank, s in enumerate(ranked, 1):
        s["rank"] = rank

    monkeypatch.setattr(ts, "_ensure_tables", lambda engine: None)
    monkeypatch.setattr(ts, "score_pending_signals",
                        lambda engine: calls["score"].append(1) or calls["order"].append("score")
                        or {"scored": 7})
    monkeypatch.setattr(ts, "_score_forward_outcomes",
                        lambda engine: calls["forward"].append(1) or {"skipped": "off"})
    monkeypatch.setattr(ts, "_sync_actor_trust",
                        lambda engine: calls["actor"].append(1) or calls["order"].append("actor") or 4)
    monkeypatch.setattr(ts, "compute_trust_scores", lambda engine: {
        "sources": [dict(s) for s in ranked], "total": len(ranked), "outcome_basis": "legacy"})

    def propagate(engine, entry):
        calls["propagate"].append(entry["source_id"])
        calls["order"].append("propagate")
        return 1

    monkeypatch.setattr(ts, "_propagate_trust", propagate)
    monkeypatch.setattr(ts, "detect_convergence",
                        lambda engine, **kw: calls["converge"].append(1) or [])
    monkeypatch.setattr(ts, "_get_llm_trust_narrative", lambda sources, conv: None)
    return calls


def test_budgeted_trust_cycle_resumes_from_cursor_and_propagates_each_source_once(monkeypatch):
    calls = _stub_scorer(monkeypatch)
    budget = {"left": 2}

    def should_continue() -> bool:
        budget["left"] -= 1
        return budget["left"] >= 0

    progress: dict[str, Any] = {}
    first = ts.run_trust_cycle(object(), should_continue=should_continue, progress=progress)
    assert first["complete"] is False
    assert calls["propagate"] == ["a", "b"]
    assert progress["resume_after"] == ["news", "b"]
    assert calls["order"][:2] == ["score", "actor"], "actor sync precedes propagation"

    budget["left"] = 100
    second = ts.run_trust_cycle(object(), should_continue=should_continue, progress=progress)
    assert second["complete"] is True
    assert calls["propagate"] == ["a", "b", "c", "d", "e"], "each source once per cycle"
    assert calls["score"] == [1] and calls["actor"] == [1], "pre-propagation steps run once"
    assert calls["converge"] == [1]
    assert progress == {}
    assert second["passes"] == 2
    assert "Total tracked sources: 5" in second["report"]
    assert "ranked_sources" not in second["trust_update"]


def test_unbudgeted_trust_cycle_still_completes_in_one_call_with_one_propagation(monkeypatch):
    calls = _stub_scorer(monkeypatch)
    result = ts.run_trust_cycle(object())
    assert result["complete"] is True
    # Before 2026-11-18 the report called update_trust_scores again: 10 writes.
    assert sorted(calls["propagate"]) == ["a", "b", "c", "d", "e"]
    assert calls["converge"] == [1]
    assert result["trust_update"]["total"] == 5
    assert [s["rank"] for s in result["trust_update"]["sources"]] == [1, 2, 3, 4, 5]
