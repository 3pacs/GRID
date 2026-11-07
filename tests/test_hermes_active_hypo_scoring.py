"""Hermes' 30-minute active-hypothesis tick (``score_due_active_hypotheses``).

Since 2026-11-06 the tick delegates to the v2 scorer
(``intelligence.hypothesis_trials.score_batch``) under the S11 trial ledger's
lock. These tests pin the wiring: arguments, the ledger requirement, count
mapping and graceful failure. The statistics are pinned in
``tests/test_hypothesis_trials.py``; the production hold in
``tests/test_hypothesis_scoring_hold.py``.
"""
from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from intelligence import hypothesis_engine, hypothesis_trials
from intelligence.hypothesis_engine import score_due_active_hypotheses


@pytest.fixture(autouse=True)
def _scoring_unheld(monkeypatch):
    monkeypatch.setattr(hypothesis_engine, "ACTIVE_HYPOTHESIS_SCORING_HELD", False)


@contextmanager
def _ledger_cm():
    yield "LEDGER"


def _batch_result(**verdicts) -> dict:
    return {
        "scanned": 12, "declared": sum(verdicts.values()), "pending": 3, "timed_out": False,
        "verdicts": verdicts, "scorer_version": hypothesis_trials.SCORER_VERSION,
        "record_sha256": "abc", "applied": verdicts, "replayed": {}, "ledger": {"q": 0.1},
    }


def test_delegates_to_the_ledgered_batch_scorer() -> None:
    engine = MagicMock()
    with patch.object(hypothesis_trials, "open_trial_ledger", _ledger_cm), \
         patch.object(hypothesis_trials, "score_batch",
                      return_value=_batch_result(confirmed=1, invalidated=4, expired=2)) as sb:
        out = score_due_active_hypotheses(engine, batch_size=50, max_runtime_s=30)
    sb.assert_called_once_with(engine, ledger="LEDGER", limit=50, max_runtime_s=30)
    assert out["scanned"] == 12 and out["scored"] == 7 and out["pending"] == 3
    assert (out["confirmed"], out["invalidated"], out["expired"]) == (1, 4, 2)
    assert out["record_sha256"] == "abc"
    assert out["scorer_version"] == hypothesis_trials.SCORER_VERSION


def test_unconfigured_ledger_writes_nothing() -> None:
    engine = MagicMock()

    @contextmanager
    def refuse():
        raise hypothesis_trials.TrialLedgerNotConfigured("not configured")
        yield  # pragma: no cover

    with patch.object(hypothesis_trials, "open_trial_ledger", refuse):
        out = score_due_active_hypotheses(engine, batch_size=10, max_runtime_s=5)
    assert out["errors"] == 1 and out["scored"] == 0
    assert "not configured" in out["error"]
    engine.connect.assert_not_called()
    engine.begin.assert_not_called()


def test_batch_failure_is_counted_not_raised() -> None:
    with patch.object(hypothesis_trials, "open_trial_ledger", _ledger_cm), \
         patch.object(hypothesis_trials, "score_batch", side_effect=RuntimeError("DB exploded")):
        out = score_due_active_hypotheses(MagicMock(), batch_size=10, max_runtime_s=5)
    assert out["errors"] == 1 and out["scored"] == 0 and "DB exploded" in out["error"]


def test_invalid_arguments_raise() -> None:
    with pytest.raises(ValueError):
        score_due_active_hypotheses(MagicMock(), batch_size=0, max_runtime_s=10)
    with pytest.raises(ValueError):
        score_due_active_hypotheses(MagicMock(), batch_size=10, max_runtime_s=0)


def test_single_row_path_defers_to_the_batch() -> None:
    engine = MagicMock()
    result = hypothesis_engine.HypothesisGenerator(engine).score_hypothesis("h-1")
    assert result["outcome"] == "deferred"
    engine.connect.assert_not_called()
    engine.begin.assert_not_called()


def test_score_all_is_one_batch() -> None:
    with patch.object(hypothesis_engine, "score_due_active_hypotheses",
                      return_value={"verdicts": {}}) as due:
        hypothesis_engine.HypothesisGenerator(MagicMock()).score_all()
    due.assert_called_once()
