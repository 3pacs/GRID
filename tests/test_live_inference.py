"""Unit tests for inference/live.py recommendation engine."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from inference.live import LiveInference


def _make_inference():
    """Create a LiveInference instance without real dependencies."""
    return LiveInference.__new__(LiveInference)


class TestGenerateRecommendation:
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
        assert result["inferred_state"] == "BULL"

    def test_insufficient_coverage_returns_hold(self):
        li = _make_inference()
        features = {"f1": 0.5, "f2": None, "f3": None, "f4": None}
        params = {"state_thresholds": {"BULL": {"weights": {"f1": 1.0}}}}
        result = li._generate_recommendation(features, params)
        assert "HOLD" in result["suggested_action"]

    def test_no_thresholds(self):
        li = _make_inference()
        features = {"f1": 0.5}
        result = li._generate_recommendation(features, {})
        assert result["inferred_state"] == "UNKNOWN"

    def test_empty_thresholds(self):
        li = _make_inference()
        features = {"f1": 0.5}
        result = li._generate_recommendation(features, {"state_thresholds": {}})
        assert result["inferred_state"] == "UNKNOWN"
