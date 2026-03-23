"""
Tests for the GRID feature transformation engine (features/lab.py).

Covers module-level transformation functions and the FeatureLab class
with mocked database and PITStore dependencies.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from store.pit import PITStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_engine() -> MagicMock:
    """Create a mock SQLAlchemy engine with working context managers."""
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return mock_engine, mock_conn


# ---------------------------------------------------------------------------
# Module-level transformation function tests
# ---------------------------------------------------------------------------

class TestZscoreNormalize:
    """Tests for zscore_normalize transformation."""

    def test_correct_zscore_computation(self):
        """Z-score should produce mean ~0 and std ~1 over the window."""
        from features.lab import zscore_normalize

        np.random.seed(42)
        series = pd.Series(np.random.randn(300) * 5 + 100)
        result = zscore_normalize(series, window=252)

        # After full window, values should be roughly standard-normal
        tail = result.dropna().tail(30)
        assert abs(tail.mean()) < 1.5, "Z-score mean should be near zero"
        assert 0.1 < tail.std() < 3.0, "Z-score std should be reasonable"

    def test_handles_constant_series(self):
        """Constant series should produce NaN (no division by zero error)."""
        from features.lab import zscore_normalize

        series = pd.Series([5.0] * 300)
        result = zscore_normalize(series, window=50)

        # std is 0 -> replaced with NaN -> result should be NaN
        assert result.dropna().empty or result.isna().all(), (
            "Constant series should yield all NaN z-scores"
        )

    def test_short_series_returns_nan(self):
        """Series shorter than half the window should produce NaN."""
        from features.lab import zscore_normalize

        series = pd.Series([1.0, 2.0, 3.0])
        result = zscore_normalize(series, window=252)

        # min_periods = 126, so 3 values should all be NaN
        assert result.isna().all()


class TestRollingSlope:
    """Tests for rolling_slope transformation."""

    def test_positive_slope_direction(self):
        """An upward-trending series should produce a positive slope."""
        from features.lab import rolling_slope

        series = pd.Series(np.arange(100, dtype=float))
        result = rolling_slope(series, window=20)
        valid = result.dropna()
        assert len(valid) > 0, "Should have some non-NaN results"
        assert valid.iloc[-1] > 0, "Upward trend should have positive slope"

    def test_negative_slope_direction(self):
        """A downward-trending series should produce a negative slope."""
        from features.lab import rolling_slope

        series = pd.Series(np.arange(100, 0, -1, dtype=float))
        result = rolling_slope(series, window=20)
        valid = result.dropna()
        assert len(valid) > 0, "Should have some non-NaN results"
        assert valid.iloc[-1] < 0, "Downward trend should have negative slope"

    def test_flat_series_near_zero_slope(self):
        """A flat series should produce a slope near zero."""
        from features.lab import rolling_slope

        series = pd.Series([50.0] * 100)
        result = rolling_slope(series, window=20)
        valid = result.dropna()
        assert len(valid) > 0
        assert abs(valid.iloc[-1]) < 1e-6, "Flat series should have ~0 slope"


class TestPctChangeLagged:
    """Tests for pct_change_lagged transformation."""

    def test_basic_percentage_change(self):
        """Should compute correct percentage change over the lag period."""
        from features.lab import pct_change_lagged

        series = pd.Series([100.0, 110.0, 121.0, 133.1])
        result = pct_change_lagged(series, lag_days=1)

        # First value is NaN (no previous), second should be 0.10
        assert pd.isna(result.iloc[0])
        assert abs(result.iloc[1] - 0.10) < 1e-6

    def test_multi_day_lag(self):
        """Percentage change with lag > 1 should compare to the correct period."""
        from features.lab import pct_change_lagged

        series = pd.Series([100.0, 105.0, 110.0, 120.0])
        result = pct_change_lagged(series, lag_days=2)

        # Index 2: (110 - 100) / 100 = 0.10
        assert pd.isna(result.iloc[0])
        assert pd.isna(result.iloc[1])
        assert abs(result.iloc[2] - 0.10) < 1e-6


class TestRatio:
    """Tests for ratio transformation."""

    def test_basic_ratio(self):
        """Element-wise ratio should compute correctly."""
        from features.lab import ratio

        a = pd.Series([10.0, 20.0, 30.0])
        b = pd.Series([2.0, 5.0, 10.0])
        result = ratio(a, b)

        assert abs(result.iloc[0] - 5.0) < 1e-6
        assert abs(result.iloc[1] - 4.0) < 1e-6
        assert abs(result.iloc[2] - 3.0) < 1e-6

    def test_zero_denominator_produces_nan(self):
        """Zero in the denominator should be replaced with NaN."""
        from features.lab import ratio

        a = pd.Series([10.0, 20.0, 30.0])
        b = pd.Series([2.0, 0.0, 10.0])
        result = ratio(a, b)

        assert abs(result.iloc[0] - 5.0) < 1e-6
        assert pd.isna(result.iloc[1]), "Zero denominator should produce NaN"
        assert abs(result.iloc[2] - 3.0) < 1e-6

    def test_nan_in_denominator(self):
        """NaN in the denominator should remain NaN."""
        from features.lab import ratio

        a = pd.Series([10.0, 20.0])
        b = pd.Series([5.0, np.nan])
        result = ratio(a, b)

        assert abs(result.iloc[0] - 2.0) < 1e-6
        assert pd.isna(result.iloc[1])


class TestSpread:
    """Tests for spread transformation."""

    def test_basic_difference(self):
        """Should compute element-wise difference."""
        from features.lab import spread

        a = pd.Series([10.0, 20.0, 30.0])
        b = pd.Series([3.0, 8.0, 15.0])
        result = spread(a, b)

        assert abs(result.iloc[0] - 7.0) < 1e-6
        assert abs(result.iloc[1] - 12.0) < 1e-6
        assert abs(result.iloc[2] - 15.0) < 1e-6

    def test_negative_spread(self):
        """Spread can be negative when series_b > series_a."""
        from features.lab import spread

        a = pd.Series([5.0, 10.0])
        b = pd.Series([8.0, 3.0])
        result = spread(a, b)

        assert result.iloc[0] < 0
        assert result.iloc[1] > 0


# ---------------------------------------------------------------------------
# FeatureLab class tests
# ---------------------------------------------------------------------------

class TestFeatureLabComputeFeature:
    """Tests for FeatureLab.compute_feature method."""

    def test_returns_none_when_feature_not_found(self):
        """compute_feature should return None when the feature is not in the registry."""
        mock_engine, mock_conn = _make_mock_engine()
        mock_pit = MagicMock(spec=PITStore)

        # _get_feature_id_by_name returns None (no row found)
        mock_conn.execute.return_value.fetchone.return_value = None

        from features.lab import FeatureLab
        lab = FeatureLab(mock_engine, mock_pit)
        result = lab.compute_feature("nonexistent_feature", date(2025, 1, 15))

        assert result is None

    def test_returns_value_when_feature_exists(self):
        """compute_feature should return a float value for an existing feature."""
        mock_engine, mock_conn = _make_mock_engine()
        mock_pit = MagicMock(spec=PITStore)

        # Return normalization info: RAW with lag=0
        mock_conn.execute.return_value.fetchone.return_value = ("RAW", 0)

        from features.lab import FeatureLab
        lab = FeatureLab(mock_engine, mock_pit)

        # Patch _get_pit_series to return a valid series directly
        test_series = pd.Series(
            [50.0 + i * 0.1 for i in range(28)],
            index=[date(2024, 1, d) for d in range(1, 29)],
            name="test_feature",
        )
        with patch.object(lab, "_get_pit_series", return_value=test_series):
            result = lab.compute_feature("test_feature", date(2024, 1, 28))

        assert result is not None
        assert isinstance(result, float)

    def test_returns_none_when_pit_returns_empty(self):
        """compute_feature should return None when PIT store has no data."""
        mock_engine, mock_conn = _make_mock_engine()
        mock_pit = MagicMock(spec=PITStore)

        # Feature ID found
        mock_conn.execute.return_value.fetchone.return_value = (42,)
        # PIT store returns empty DataFrame
        mock_pit.get_pit.return_value = pd.DataFrame()

        from features.lab import FeatureLab
        lab = FeatureLab(mock_engine, mock_pit)
        result = lab.compute_feature("sparse_feature", date(2025, 1, 15))

        assert result is None


class TestFeatureLabComputeDerived:
    """Tests for FeatureLab.compute_derived_features method."""

    def test_returns_dict_with_expected_keys(self):
        """compute_derived_features should return a dict with all expected feature keys."""
        mock_engine, mock_conn = _make_mock_engine()
        mock_pit = MagicMock(spec=PITStore)

        # Make all DB lookups return None (feature not found) so everything
        # gracefully returns None, but the dict structure is still correct.
        mock_conn.execute.return_value.fetchone.return_value = None
        mock_pit.get_pit.return_value = pd.DataFrame()

        from features.lab import FeatureLab
        lab = FeatureLab(mock_engine, mock_pit)
        result = lab.compute_derived_features(date(2025, 6, 1))

        assert isinstance(result, dict)

        expected_keys = {
            "yld_curve_2s10s",
            "fed_funds_3m_chg",
            "hy_spread_3m_chg",
            "copper_gold_ratio",
            "copper_gold_slope",
            "sp500_mom_12_1",
            "sp500_mom_3m",
            "real_ffr",
            "vix_3m_ratio",
            "sp500_kinetic_energy",
            "sp500_potential_energy",
            "market_temperature",
            "sp500_ou_theta",
            "sp500_ou_half_life",
            "sp500_hurst",
        }
        for key in expected_keys:
            assert key in result, f"Missing expected key: {key}"

    def test_all_none_when_no_data(self):
        """All derived features should be None when no underlying data exists."""
        mock_engine, mock_conn = _make_mock_engine()
        mock_pit = MagicMock(spec=PITStore)

        mock_conn.execute.return_value.fetchone.return_value = None
        mock_pit.get_pit.return_value = pd.DataFrame()

        from features.lab import FeatureLab
        lab = FeatureLab(mock_engine, mock_pit)
        result = lab.compute_derived_features(date(2025, 6, 1))

        for key, val in result.items():
            assert val is None, f"Expected None for '{key}' with no data, got {val}"


class TestFeatureLabTsfresh:
    """Tests for FeatureLab.run_tsfresh_extraction graceful degradation."""

    @patch.dict("sys.modules", {"tsfresh": None, "tsfresh.utilities.dataframe_functions": None})
    def test_graceful_degradation_without_tsfresh(self):
        """run_tsfresh_extraction should return empty dict when tsfresh is not installed."""
        mock_engine, mock_conn = _make_mock_engine()
        mock_pit = MagicMock(spec=PITStore)

        from features.lab import FeatureLab
        lab = FeatureLab(mock_engine, mock_pit)

        # Force reimport so the ImportError path is hit
        import importlib
        import features.lab as lab_mod
        importlib.reload(lab_mod)

        lab2 = lab_mod.FeatureLab(mock_engine, mock_pit)
        result = lab2.run_tsfresh_extraction("test_series", date(2025, 1, 15))

        assert result == {}
        assert isinstance(result, dict)

    def test_returns_empty_dict_when_insufficient_data(self):
        """run_tsfresh_extraction should return empty dict when series has < 10 points."""
        mock_engine, mock_conn = _make_mock_engine()
        mock_pit = MagicMock(spec=PITStore)

        # Feature ID found
        mock_conn.execute.return_value.fetchone.return_value = (1,)
        # PIT store returns very short series with date (not datetime) obs_date
        pit_df = pd.DataFrame({
            "obs_date": [date(2025, 1, d) for d in range(6, 11)],
            "value": [1.0, 2.0, 3.0, 4.0, 5.0],
            "feature_id": [1] * 5,
            "release_date": [date(2025, 1, d) for d in range(6, 11)],
            "vintage_date": [date(2025, 1, d) for d in range(6, 11)],
        })
        mock_pit.get_pit.return_value = pit_df

        from features.lab import FeatureLab
        lab = FeatureLab(mock_engine, mock_pit)
        result = lab.run_tsfresh_extraction("short_series", date(2025, 1, 15))

        assert result == {}
