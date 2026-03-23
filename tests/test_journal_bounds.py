"""Tests for journal/log.py input bounds and NaN/infinity checks."""

from __future__ import annotations

import math
from unittest.mock import MagicMock

import pytest

from journal.log import DecisionJournal


@pytest.fixture
def journal(mock_engine):
    return DecisionJournal(db_engine=mock_engine)


def _default_kwargs(**overrides):
    base = {
        "model_version_id": 1,
        "inferred_state": "EXPANSION",
        "state_confidence": 0.8,
        "transition_probability": 0.1,
        "contradiction_flags": {},
        "grid_recommendation": "BUY",
        "baseline_recommendation": "HOLD",
        "action_taken": "BUY",
        "counterfactual": "Would have held",
        "operator_confidence": "HIGH",
    }
    base.update(overrides)
    return base


class TestNaNRejection:
    def test_nan_state_confidence_rejected(self, journal):
        with pytest.raises(ValueError, match="state_confidence"):
            journal.log_decision(**_default_kwargs(state_confidence=float("nan")))

    def test_math_nan_state_confidence_rejected(self, journal):
        with pytest.raises(ValueError, match="state_confidence"):
            journal.log_decision(**_default_kwargs(state_confidence=math.nan))

    def test_nan_transition_probability_rejected(self, journal):
        with pytest.raises(ValueError, match="transition_probability"):
            journal.log_decision(**_default_kwargs(transition_probability=float("nan")))


class TestInfinityRejection:
    def test_inf_state_confidence_rejected(self, journal):
        with pytest.raises(ValueError, match="state_confidence"):
            journal.log_decision(**_default_kwargs(state_confidence=float("inf")))

    def test_positive_inf_transition_probability_rejected(self, journal):
        with pytest.raises(ValueError, match="transition_probability"):
            journal.log_decision(**_default_kwargs(transition_probability=float("inf")))

    def test_neg_inf_transition_probability_rejected(self, journal):
        with pytest.raises(ValueError, match="transition_probability"):
            journal.log_decision(**_default_kwargs(transition_probability=float("-inf")))


class TestValidBoundaryValues:
    def _setup_insert_return(self, mock_engine):
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (99,)
        mock_conn.execute.return_value = mock_result
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    def test_zero_confidence_accepted(self, journal, mock_engine):
        self._setup_insert_return(mock_engine)
        decision_id = journal.log_decision(
            **_default_kwargs(state_confidence=0.0, transition_probability=0.0)
        )
        assert decision_id == 99

    def test_one_confidence_accepted(self, journal, mock_engine):
        self._setup_insert_return(mock_engine)
        decision_id = journal.log_decision(
            **_default_kwargs(state_confidence=1.0, transition_probability=1.0)
        )
        assert decision_id == 99

    def test_mid_range_accepted(self, journal, mock_engine):
        self._setup_insert_return(mock_engine)
        decision_id = journal.log_decision(
            **_default_kwargs(state_confidence=0.42, transition_probability=0.73)
        )
        assert decision_id == 99

    def test_above_one_state_confidence_rejected(self, journal):
        with pytest.raises(ValueError, match="state_confidence"):
            journal.log_decision(**_default_kwargs(state_confidence=1.5))

    def test_negative_state_confidence_rejected(self, journal):
        with pytest.raises(ValueError, match="state_confidence"):
            journal.log_decision(**_default_kwargs(state_confidence=-0.1))

    def test_negative_transition_probability_rejected(self, journal):
        with pytest.raises(ValueError, match="transition_probability"):
            journal.log_decision(**_default_kwargs(transition_probability=-0.1))
