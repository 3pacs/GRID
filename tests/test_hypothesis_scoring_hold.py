"""Active-hypothesis scoring is held at its single write path (2026-09-26).

``HypothesisGenerator.score_hypothesis`` is reached from Hermes' periodic
batch, the goal_worker ``score_active_hypothesis`` handler and the
generator's own batch scorer. Holding it there covers every caller.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from intelligence import hypothesis_engine as he
from scripts import goal_worker


def test_scoring_is_held_by_default() -> None:
    assert he.ACTIVE_HYPOTHESIS_SCORING_HELD is True


def test_held_score_hypothesis_touches_no_database() -> None:
    engine = MagicMock()
    result = he.HypothesisGenerator(engine).score_hypothesis("h-1")
    assert result["held"] is True
    assert result["outcome"] == "held"
    engine.connect.assert_not_called()
    engine.begin.assert_not_called()


def test_held_batch_scorer_queries_nothing() -> None:
    engine = MagicMock()
    counts = he.score_due_active_hypotheses(engine, batch_size=10, max_runtime_s=5)
    assert counts["held"] is True
    assert counts["scored"] == 0
    engine.connect.assert_not_called()
    engine.begin.assert_not_called()


def test_goal_worker_handler_is_held() -> None:
    engine = MagicMock()
    goal = MagicMock(target_id="h-2", allow_cloud=False)
    summary = goal_worker.handle_score_active_hypothesis(engine, goal)
    assert summary["outcome"] == "held"
    engine.connect.assert_not_called()
    engine.begin.assert_not_called()


def test_batch_scorer_runs_when_unheld(monkeypatch) -> None:
    # Positive control: proves the hold is what suppresses the DB query.
    monkeypatch.setattr(he, "ACTIVE_HYPOTHESIS_SCORING_HELD", False)
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = []
    counts = he.score_due_active_hypotheses(engine, batch_size=10, max_runtime_s=5)
    assert "held" not in counts
    engine.connect.assert_called_once()
