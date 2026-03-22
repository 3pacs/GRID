"""Unit tests for inference/live.py recommendation engine."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from inference.live import LiveInference


def _make_inference():
    """Create a LiveInference instance without real dependencies."""
    return LiveInference.__new__(LiveInference)


class TestGenerateRecommendationAllPositive:
    """All positive feature values with weighted scoring."""

    def test_positive_scores_picks_highest(self):
        li = _make_inference()
        features = {"f1": 0.5, "f2": 0.8, "f3": 0.3}
        params = {
            "state_thresholds": {
                "BULL": {"weights": {"f1": 1.0, "f2": 1.0}, "action": "BUY"},
                "BEAR": {"weights": {"f1": -1.0, "f2": -0.5}, "action": "SELL"},
            }
        }
        result = li._generate_recommendation(features, params)
        assert result["inferred_state"] == "BULL"
        assert result["suggested_action"] == "BUY"

    def test_confidence_is_bounded_by_one(self):
        li = _make_inference()
        features = {"f1": 100.0}
        params = {
            "state_thresholds": {
                "UP": {"weights": {"f1": 1.0}, "action": "BUY"},
            }
        }
        result = li._generate_recommendation(features, params)
        assert result["state_confidence"] <= 1.0

    def test_action_matches_winning_state(self):
        li = _make_inference()
        features = {"x": 5.0, "y": 3.0}
        params = {
            "state_thresholds": {
                "ALPHA": {"weights": {"x": 0.1}, "action": "SCALE_IN"},
                "BETA": {"weights": {"x": 2.0, "y": 1.0}, "action": "FULL_POSITION"},
            }
        }
        result = li._generate_recommendation(features, params)
        assert result["inferred_state"] == "BETA"
        assert result["suggested_action"] == "FULL_POSITION"


class TestGenerateRecommendationAllNegative:
    """All negative feature values -- should pick strongest absolute."""

    def test_negative_scores_picks_strongest_absolute(self):
        li = _make_inference()
        features = {"f1": -2.0, "f2": -1.0}
        params = {
            "state_thresholds": {
                "BULL": {"weights": {"f1": 1.0, "f2": 1.0}, "action": "BUY"},
                "BEAR": {"weights": {"f1": 0.5, "f2": 0.5}, "action": "SELL"},
            }
        }
        result = li._generate_recommendation(features, params)
        # BULL score: -2+(-1)=-3 (abs=3), BEAR score: -1+(-0.5)=-1.5 (abs=1.5)
        assert result["inferred_state"] == "BULL"

    def test_negative_scores_produce_nonzero_confidence(self):
        li = _make_inference()
        features = {"x": -10.0}
        params = {
            "state_thresholds": {
                "DOWN": {"weights": {"x": 1.0}, "action": "SELL"},
            }
        }
        result = li._generate_recommendation(features, params)
        assert result["state_confidence"] > 0


class TestInsufficientFeatureCoverage:
    """Coverage below 50% triggers a HOLD recommendation."""

    def test_below_50pct_coverage(self):
        li = _make_inference()
        features = {"f1": 0.5, "f2": None, "f3": None, "f4": None}
        params = {"state_thresholds": {"BULL": {"weights": {"f1": 1.0}}}}
        result = li._generate_recommendation(features, params)
        assert "HOLD" in result["suggested_action"]
        assert result["inferred_state"] == "UNKNOWN"

    def test_exactly_50pct_is_sufficient(self):
        li = _make_inference()
        features = {"a": 1.0, "b": None}
        params = {
            "state_thresholds": {
                "UP": {"weights": {"a": 1.0}, "action": "BUY"},
            }
        }
        result = li._generate_recommendation(features, params)
        assert result["feature_coverage"] == 0.5
        assert result["inferred_state"] == "UP"

    def test_all_none_features(self):
        li = _make_inference()
        features = {"a": None, "b": None, "c": None}
        result = li._generate_recommendation(features, {})
        assert result["feature_coverage"] == 0.0
        assert "insufficient" in result["suggested_action"].lower()


class TestNoThresholdsConfigured:
    """Missing or empty state_thresholds returns defaults."""

    def test_empty_snapshot(self):
        li = _make_inference()
        features = {"f1": 0.5}
        result = li._generate_recommendation(features, {})
        assert result["inferred_state"] == "UNKNOWN"
        assert result["suggested_action"] == "HOLD"

    def test_empty_thresholds_dict(self):
        li = _make_inference()
        features = {"f1": 0.5}
        result = li._generate_recommendation(features, {"state_thresholds": {}})
        assert result["inferred_state"] == "UNKNOWN"

    def test_snapshot_without_thresholds_key(self):
        li = _make_inference()
        features = {"a": 1.0}
        result = li._generate_recommendation(features, {"other_key": "value"})
        assert result["inferred_state"] == "UNKNOWN"
