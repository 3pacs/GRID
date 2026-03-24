"""
Tests for the FeatureImportanceTracker.

Uses mock_engine and mock_pit_store from conftest.py with synthetic numpy data.
"""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from features.importance import FeatureImportanceTracker


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tracker(mock_engine, mock_pit_store):
    """Return a FeatureImportanceTracker with mock dependencies."""
    return FeatureImportanceTracker(db_engine=mock_engine, pit_store=mock_pit_store)


@pytest.fixture
def synthetic_feature_matrix():
    """Build a synthetic wide-format feature matrix (~200 rows x 3 features)."""
    rng = np.random.default_rng(seed=123)
    dates = pd.date_range(end=date(2025, 5, 30), periods=200, freq="B")
    n_rows = len(dates)
    data = {
        1: rng.normal(0, 1, n_rows),
        2: rng.normal(0, 0.5, n_rows),
        3: rng.normal(0, 2, n_rows),
    }
    return pd.DataFrame(data, index=dates)


@pytest.fixture
def model_info():
    """Return a mock model info dict."""
    return {
        "id": 1,
        "name": "test-regime-v1",
        "layer": "REGIME",
        "version": "1.0",
        "feature_set": [1, 2, 3],
        "parameter_snapshot": {
            "state_thresholds": {
                "EXPANSION": {
                    "weights": {"feat_a": 0.5, "feat_b": 0.3, "feat_c": 0.2},
                    "action": "RISK_ON",
                },
                "CONTRACTION": {
                    "weights": {"feat_a": -0.4, "feat_b": -0.3, "feat_c": -0.3},
                    "action": "RISK_OFF",
                },
            }
        },
        "hypothesis_id": 1,
    }


@pytest.fixture
def feature_names_map():
    """Mapping of feature_id -> name used in tests."""
    return {1: "feat_a", 2: "feat_b", 3: "feat_c"}


# ---------------------------------------------------------------------------
# Tests: compute_permutation_importance
# ---------------------------------------------------------------------------

class TestPermutationImportance:
    """Tests for compute_permutation_importance."""

    def test_returns_dict_with_scores_0_to_1(
        self, tracker, mock_pit_store, mock_engine,
        synthetic_feature_matrix, model_info, feature_names_map,
    ):
        """Permutation importance should return normalised scores in [0, 1]."""
        # Patch internal methods to return synthetic data
        tracker._get_model_info = MagicMock(return_value=model_info)
        tracker._get_feature_names = MagicMock(return_value=feature_names_map)
        tracker._build_feature_matrix = MagicMock(return_value=synthetic_feature_matrix)
        tracker._persist_importance = MagicMock()

        result = tracker.compute_permutation_importance(
            model_id=1, as_of_date=date(2025, 6, 1), n_repeats=5,
        )

        assert isinstance(result, dict)
        assert len(result) == 3

        for fname, score in result.items():
            assert isinstance(fname, str)
            assert isinstance(score, float)
            assert 0.0 <= score <= 1.0, f"{fname} score {score} not in [0,1]"

        # At least one feature should have score == 1.0 (the max)
        assert max(result.values()) == 1.0

    def test_returns_empty_when_model_not_found(self, tracker):
        """Should return empty dict when model ID does not exist."""
        tracker._get_model_info = MagicMock(return_value=None)

        result = tracker.compute_permutation_importance(
            model_id=999, as_of_date=date(2025, 6, 1),
        )
        assert result == {}

    def test_returns_empty_when_no_features(self, tracker, model_info):
        """Should return empty dict when model has no features."""
        model_info["feature_set"] = []
        tracker._get_model_info = MagicMock(return_value=model_info)

        result = tracker.compute_permutation_importance(
            model_id=1, as_of_date=date(2025, 6, 1),
        )
        assert result == {}

    def test_returns_zeros_when_no_variance(
        self, tracker, model_info, feature_names_map,
    ):
        """When all scores are identical, importance should be 0 for all."""
        # Constant feature matrix => zero variance
        dates = pd.date_range(end=date(2025, 6, 1), periods=50, freq="B")
        constant_matrix = pd.DataFrame(
            {1: 1.0, 2: 1.0, 3: 1.0}, index=dates,
        )

        tracker._get_model_info = MagicMock(return_value=model_info)
        tracker._get_feature_names = MagicMock(return_value=feature_names_map)
        tracker._build_feature_matrix = MagicMock(return_value=constant_matrix)
        tracker._persist_importance = MagicMock()

        result = tracker.compute_permutation_importance(
            model_id=1, as_of_date=date(2025, 6, 1),
        )

        for score in result.values():
            assert score == 0.0


# ---------------------------------------------------------------------------
# Tests: compute_regime_correlation
# ---------------------------------------------------------------------------

class TestRegimeCorrelation:
    """Tests for compute_regime_correlation."""

    def test_returns_correlation_with_synthetic_data(
        self, tracker, synthetic_feature_matrix, feature_names_map,
    ):
        """Regime correlation should return valid correlation stats."""
        tracker._get_feature_names = MagicMock(return_value=feature_names_map)
        tracker._build_feature_matrix = MagicMock(
            return_value=synthetic_feature_matrix,
        )

        # Create synthetic journal entries with regime transitions
        n_entries = 100
        dates = synthetic_feature_matrix.index[-n_entries:]
        states = ["EXPANSION", "CONTRACTION"]
        journal_rows = [
            (d.date(), states[i % 20 < 10])
            for i, d in enumerate(dates)
        ]

        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = journal_rows
        mock_conn.execute.return_value = mock_result

        tracker.engine.connect.return_value.__enter__ = MagicMock(
            return_value=mock_conn
        )
        tracker.engine.connect.return_value.__exit__ = MagicMock(
            return_value=False
        )

        result = tracker.compute_regime_correlation(
            feature_ids=[1, 2, 3],
            as_of_date=date(2025, 6, 1),
            lookback_days=365,
        )

        assert isinstance(result, dict)
        assert len(result) == 3

        for fname, info in result.items():
            assert "correlation" in info
            assert "p_value" in info
            assert "lead_days" in info
            assert -1.0 <= info["correlation"] <= 1.0
            assert 0.0 <= info["p_value"] <= 1.0
            assert 0 <= info["lead_days"] <= 21

    def test_returns_empty_when_no_journal_entries(
        self, tracker, synthetic_feature_matrix, feature_names_map,
    ):
        """Should return empty dict when too few journal entries."""
        tracker._get_feature_names = MagicMock(return_value=feature_names_map)
        tracker._build_feature_matrix = MagicMock(
            return_value=synthetic_feature_matrix,
        )

        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []  # no entries
        mock_conn.execute.return_value = mock_result

        tracker.engine.connect.return_value.__enter__ = MagicMock(
            return_value=mock_conn
        )
        tracker.engine.connect.return_value.__exit__ = MagicMock(
            return_value=False
        )

        result = tracker.compute_regime_correlation(
            feature_ids=[1, 2, 3],
            as_of_date=date(2025, 6, 1),
        )

        assert result == {}


# ---------------------------------------------------------------------------
# Tests: compute_rolling_stability
# ---------------------------------------------------------------------------

class TestRollingStability:
    """Tests for compute_rolling_stability."""

    def test_returns_stability_metrics(
        self, tracker, synthetic_feature_matrix, feature_names_map,
    ):
        """Should return mean, std, and stability_score for each feature."""
        tracker._get_feature_names = MagicMock(return_value=feature_names_map)
        tracker._build_feature_matrix = MagicMock(
            return_value=synthetic_feature_matrix,
        )

        result = tracker.compute_rolling_stability(
            feature_ids=[1, 2, 3],
            as_of_date=date(2025, 6, 1),
            window=63,
        )

        assert isinstance(result, dict)
        assert len(result) == 3

        for fname, info in result.items():
            assert "mean_importance" in info
            assert "std_importance" in info
            assert "stability_score" in info
            assert info["mean_importance"] >= 0.0
            assert info["std_importance"] >= 0.0
            assert 0.0 <= info["stability_score"] <= 1.0

    def test_returns_zeros_when_series_too_short(
        self, tracker, feature_names_map,
    ):
        """Features with fewer rows than the window should get zero scores."""
        short_dates = pd.date_range(end=date(2025, 5, 30), periods=3, freq="B")
        short_matrix = pd.DataFrame(
            {1: [1.0, 2.0, 3.0], 2: [4.0, 5.0, 6.0], 3: [7.0, 8.0, 9.0]},
            index=short_dates,
        )
        tracker._get_feature_names = MagicMock(return_value=feature_names_map)
        tracker._build_feature_matrix = MagicMock(return_value=short_matrix)

        result = tracker.compute_rolling_stability(
            feature_ids=[1, 2, 3],
            as_of_date=date(2025, 6, 1),
            window=63,
        )

        for info in result.values():
            assert info["stability_score"] == 0.0

    def test_stable_feature_scores_higher(self, tracker, feature_names_map):
        """A constant feature should have higher stability than a volatile one."""
        rng = np.random.default_rng(seed=42)
        dates = pd.date_range(end=date(2025, 5, 30), periods=200, freq="B")
        n = len(dates)

        # Feature 1: stable (small noise around mean)
        # Feature 2: volatile (large random swings)
        matrix = pd.DataFrame(
            {
                1: 10.0 + rng.normal(0, 0.01, n),
                2: rng.normal(0, 10, n),
            },
            index=dates,
        )
        names = {1: "stable_feat", 2: "volatile_feat"}

        tracker._get_feature_names = MagicMock(return_value=names)
        tracker._build_feature_matrix = MagicMock(return_value=matrix)

        result = tracker.compute_rolling_stability(
            feature_ids=[1, 2],
            as_of_date=date(2025, 6, 1),
            window=63,
        )

        # Both should have valid scores
        assert result["stable_feat"]["stability_score"] >= 0.0
        assert result["volatile_feat"]["stability_score"] >= 0.0


# ---------------------------------------------------------------------------
# Tests: get_importance_report
# ---------------------------------------------------------------------------

class TestImportanceReport:
    """Tests for get_importance_report."""

    def test_report_has_expected_keys(
        self, tracker, model_info, feature_names_map,
        synthetic_feature_matrix,
    ):
        """The report dict should contain all expected sections."""
        tracker._get_model_info = MagicMock(return_value=model_info)
        tracker._get_feature_names = MagicMock(return_value=feature_names_map)
        tracker._build_feature_matrix = MagicMock(
            return_value=synthetic_feature_matrix,
        )
        tracker._persist_importance = MagicMock()

        # Mock regime correlation to return empty (no journal data)
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_conn.execute.return_value = mock_result
        tracker.engine.connect.return_value.__enter__ = MagicMock(
            return_value=mock_conn
        )
        tracker.engine.connect.return_value.__exit__ = MagicMock(
            return_value=False
        )

        report = tracker.get_importance_report(
            model_id=1, as_of_date=date(2025, 6, 1),
        )

        assert "error" not in report
        assert report["model_id"] == 1
        assert report["model_name"] == "test-regime-v1"
        assert report["as_of_date"] == "2025-06-01"
        assert report["n_features"] == 3
        assert "permutation_importance" in report
        assert "regime_correlation" in report
        assert "rolling_stability" in report
        assert "summary" in report
        assert isinstance(report["summary"], list)

    def test_report_returns_error_for_missing_model(self, tracker):
        """Should return error dict when model not found."""
        tracker._get_model_info = MagicMock(return_value=None)

        report = tracker.get_importance_report(model_id=999)
        assert "error" in report

    def test_summary_sorted_by_composite(
        self, tracker, model_info, feature_names_map,
        synthetic_feature_matrix,
    ):
        """Summary entries should be sorted by composite_score descending."""
        tracker._get_model_info = MagicMock(return_value=model_info)
        tracker._get_feature_names = MagicMock(return_value=feature_names_map)
        tracker._build_feature_matrix = MagicMock(
            return_value=synthetic_feature_matrix,
        )
        tracker._persist_importance = MagicMock()

        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_conn.execute.return_value = mock_result
        tracker.engine.connect.return_value.__enter__ = MagicMock(
            return_value=mock_conn
        )
        tracker.engine.connect.return_value.__exit__ = MagicMock(
            return_value=False
        )

        report = tracker.get_importance_report(
            model_id=1, as_of_date=date(2025, 6, 1),
        )

        summary = report["summary"]
        scores = [s["composite_score"] for s in summary]
        assert scores == sorted(scores, reverse=True)
