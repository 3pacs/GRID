"""The periodic active-hypothesis scorer is held (2026-09-26).

``score_due_active_hypotheses`` flips ``discovered_hypotheses`` rows to
confirmed/invalidated — a learning write with no trial ledger, FDR or
holdout. ``ACTIVE_HYPO_SCORING_ENABLED`` keeps it out of the Hermes cycle
until a reviewed change re-enables it.
"""
from __future__ import annotations

import sys
import types
from datetime import datetime, timezone
from unittest.mock import MagicMock

from scripts import hermes_operator as ho


def _state_with_only_scorer_due() -> object:
    state = ho.OperatorState()
    now = datetime.now(timezone.utc)
    for name in vars(state):
        if name.startswith("last_"):
            setattr(state, name, now)
    state.last_active_hypo_scoring = None
    return state


def _run_and_record(monkeypatch) -> list[str]:
    called: list[str] = []

    def fake_run_intel_task(name, fn, state, engine, **kwargs):
        called.append(name)
        return None

    fake_engine_mod = types.ModuleType("intelligence.hypothesis_engine")
    fake_engine_mod.score_due_active_hypotheses = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "intelligence.hypothesis_engine", fake_engine_mod)
    monkeypatch.setattr(ho, "_run_intel_task", fake_run_intel_task)
    ho.run_intelligence_tasks(MagicMock(), _state_with_only_scorer_due())
    return called


def test_active_hypo_scoring_is_held_by_default() -> None:
    assert ho.ACTIVE_HYPO_SCORING_ENABLED is False


def test_held_scorer_is_never_dispatched(monkeypatch) -> None:
    assert "active_hypo_scoring" not in _run_and_record(monkeypatch)


def test_scorer_dispatches_when_explicitly_enabled(monkeypatch) -> None:
    # Positive control: proves the hold above is what suppresses the call.
    monkeypatch.setattr(ho, "ACTIVE_HYPO_SCORING_ENABLED", True)
    assert "active_hypo_scoring" in _run_and_record(monkeypatch)
