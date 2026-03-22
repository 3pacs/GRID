"""Tests for journal/log.py input bounds and NaN/infinity checks."""

from __future__ import annotations

import math
from unittest.mock import MagicMock

import pytest

from journal.log import DecisionJournal


@pytest.fixture
def journal():
    engine = MagicMock()
    return DecisionJournal(db_engine=engine)


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
        with pytest.raises(ValueError, match="finite number"):
            journal.log_decision(**_default_kwargs(state_confidence=float("nan")))

    def test_nan_transition_probability_rejected(self, journal):
        with pytest.raises(ValueError, match="finite number"):
            journal.log_decision(**_default_kwargs(transition_probability=float("nan")))


class TestInfinityRejection:
    def test_inf_state_confidence_rejected(self, journal):
        with pytest.raises(ValueError, match="finite number"):
            journal.log_decision(**_default_kwargs(state_confidence=float("inf")))

    def test_neg_inf_transition_probability_rejected(self, journal):
        with pytest.raises(ValueError, match="finite number"):
            journal.log_decision(**_default_kwargs(transition_probability=float("-inf")))


class TestValidBoundaryValues:
    def test_zero_confidence_accepted(self, journal):
        # Should not raise — validation passes, will fail at DB insert (mocked)
        try:
            journal.log_decision(**_default_kwargs(state_confidence=0.0))
        except ValueError:
            pytest.fail("0.0 should be a valid state_confidence")
        except Exception:
            pass  # DB insert will fail on mock, that's fine

    def test_one_confidence_accepted(self, journal):
        try:
            journal.log_decision(**_default_kwargs(state_confidence=1.0))
        except ValueError:
            pytest.fail("1.0 should be a valid state_confidence")
        except Exception:
            pass

    def test_out_of_range_rejected(self, journal):
        with pytest.raises(ValueError):
            journal.log_decision(**_default_kwargs(state_confidence=1.5))

    def test_negative_rejected(self, journal):
        with pytest.raises(ValueError):
            journal.log_decision(**_default_kwargs(state_confidence=-0.1))
