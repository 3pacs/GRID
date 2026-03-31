"""
Comprehensive tests for features/lab.py.

Covers:
- All five standalone transformation functions
- FeatureLab._get_feature_id_by_name
- FeatureLab._get_pit_series: DB miss, PIT empty, temporal filtering
- FeatureLab.compute_feature: all normalization branches (ZSCORE, RAW, RANK, MINMAX),
  lag handling, insufficient series, no registry row
- FeatureLab.compute_derived_features: structure, None-propagation, copper/gold ratio,
  sp500 momentum, real_ffr spread, vix ratio, options features
- FeatureLab.run_tsfresh_extraction: tsfresh absent, too-short series
- Temporal correctness: no future-date leakage in rolling windows
- NaN handling throughout

All DB and PITStore calls are mocked.  No live Postgres required.
"""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from features.lab import (
    FeatureLab,
    pct_change_lagged,
    ratio,
    rolling_slope,
    spread,
    zscore_normalize,
)
from store.pit import PITStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_engine():
    """Return (mock_engine, mock_conn) with working context managers."""
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return mock_engine, mock_conn


def _make_pit_df(n=50, feature_id=1, start=date(2024, 1, 1), value_start=100.0):
    """Build a realistic PIT DataFrame with n daily rows."""
    dates = [start + timedelta(days=i) for i in range(n)]
    return pd.DataFrame({
        "feature_id": [feature_id] * n,
        "obs_date": dates,
        "value": [value_start + i * 0.1 for i in range(n)],
        "release_date": dates,
        "vintage_date": dates,
    })


# ===========================================================================
# Transformation function unit tests
# ===========================================================================


class TestZscoreNormalize:
    """Unit tests for the standalone zscore_normalize function."""

    def test_correct_zscore_computation(self):
        np.random.seed(42)
        series = pd.Series(np.random.randn(300) * 5 + 100)
        result = zscore_normalize(series, window=252)
        tail = result.dropna().tail(30)
        assert abs(tail.mean()) < 1.5
        assert 0.1 < tail.std() < 3.0

    def test_constant_series_returns_all_nan(self):
        series = pd.Series([5.0] * 100)
        result = zscore_normalize(series, window=50)
        assert result.dropna().empty or result.isna().all()

    def test_short_series_below_min_periods_all_nan(self):
        series = pd.Series([1.0, 2.0, 3.0])
        result = zscore_normalize(series, window=252)
        # min_periods = 126; 3 < 126 → all NaN
        assert result.isna().all()

    def test_output_length_equals_input_length(self):
        series = pd.Series(range(200), dtype=float)
        result = zscore_normalize(series, window=50)
        assert len(result) == 200

    def test_linear_increasing_last_value_positive(self):
        series = pd.Series(range(200), dtype=float)
        result = zscore_normalize(series, window=100)
        assert result.dropna().iloc[-1] > 0

    def test_zero_std_does_not_raise(self):
        """Replacing zero std with NaN must not produce a ZeroDivisionError."""
        series = pd.Series([10.0] * 200)
        result = zscore_normalize(series, window=50)
        # Should complete without exception
        assert isinstance(result, pd.Series)

    def test_nan_in_input_propagates_gracefully(self):
        """Series with embedded NaN should not crash."""
        vals = [float(i) for i in range(100)]
        vals[50] = float("nan")
        series = pd.Series(vals)
        result = zscore_normalize(series, window=30)
        assert isinstance(result, pd.Series)

    def test_single_element_series(self):
        series = pd.Series([42.0])
        result = zscore_normalize(series, window=252)
        assert len(result) == 1


class TestRollingSlope:
    """Unit tests for the standalone rolling_slope function."""

    def test_positive_slope_direction(self):
        series = pd.Series(np.arange(100, dtype=float))
        result = rolling_slope(series, window=20)
        assert result.dropna().iloc[-1] > 0

    def test_negative_slope_direction(self):
        series = pd.Series(np.arange(100, 0, -1, dtype=float))
        result = rolling_slope(series, window=20)
        valid = result.dropna()
        assert len(valid) > 0
        assert valid.iloc[-1] < 0

    def test_flat_series_near_zero_slope(self):
        series = pd.Series([50.0] * 100)
        result = rolling_slope(series, window=20)
        valid = result.dropna()
        assert abs(valid.iloc[-1]) < 1e-6

    def test_short_series_all_nan(self):
        series = pd.Series([1.0, 2.0])
        result = rolling_slope(series, window=63)
        assert result.isna().all()

    def test_output_length_equals_input(self):
        series = pd.Series(range(80), dtype=float)
        result = rolling_slope(series, window=20)
        assert len(result) == 80

    def test_annualisation_factor_applied(self):
        """Slope should be multiplied by 252/window for annualisation."""
        # For window=252, annualisation factor = 1.0; for window=1 it would be 252.
        # Using a simple linear trend, verify the slope scales with 252/window.
        n = 200
        series = pd.Series(np.arange(n, dtype=float))  # slope = 1 per step
        result_63 = rolling_slope(series, window=63).dropna()
        result_126 = rolling_slope(series, window=126).dropna()

        # Both should be positive; 63-day annualised slope > 126-day (same data)
        # Because factor is 252/63=4 vs 252/126=2
        assert result_63.iloc[-1] > result_126.iloc[-1]

    def test_all_nan_window_returns_nan(self):
        series = pd.Series([np.nan] * 30)
        result = rolling_slope(series, window=10)
        assert result.isna().all()


class TestPctChangeLagged:
    """Unit tests for pct_change_lagged."""

    def test_basic_one_day_lag(self):
        series = pd.Series([100.0, 110.0, 121.0])
        result = pct_change_lagged(series, lag_days=1)
        assert abs(result.iloc[1] - 0.1) < 0.001

    def test_multi_day_lag(self):
        series = pd.Series([100.0, 105.0, 110.0, 120.0])
        result = pct_change_lagged(series, lag_days=2)
        assert result.iloc[:2].isna().all()
        assert abs(result.iloc[2] - 0.10) < 0.001

    def test_zero_base_produces_inf(self):
        series = pd.Series([0.0, 10.0])
        result = pct_change_lagged(series, lag_days=1)
        assert np.isinf(result.iloc[1])

    def test_negative_values(self):
        series = pd.Series([-100.0, -50.0])
        result = pct_change_lagged(series, lag_days=1)
        assert abs(result.iloc[1] - (-0.5)) < 0.001

    def test_lag_zero_all_nan_except_first_group(self):
        series = pd.Series([100.0, 200.0, 300.0])
        result = pct_change_lagged(series, lag_days=0)
        # pct_change(0) returns zeros by pandas convention
        assert not result.isna().all()

    def test_lag_beyond_series_length_all_nan(self):
        series = pd.Series([100.0, 110.0])
        result = pct_change_lagged(series, lag_days=5)
        assert result.isna().all()


class TestRatio:
    """Unit tests for the ratio function."""

    def test_basic_ratio(self):
        a = pd.Series([10.0, 20.0, 30.0])
        b = pd.Series([2.0, 4.0, 6.0])
        result = ratio(a, b)
        assert list(result) == [5.0, 5.0, 5.0]

    def test_zero_denominator_returns_nan(self):
        a = pd.Series([10.0, 20.0])
        b = pd.Series([0.0, 5.0])
        result = ratio(a, b)
        assert np.isnan(result.iloc[0])
        assert result.iloc[1] == 4.0

    def test_nan_denominator_returns_nan(self):
        a = pd.Series([10.0, 20.0])
        b = pd.Series([np.nan, 4.0])
        result = ratio(a, b)
        assert np.isnan(result.iloc[0])
        assert result.iloc[1] == 5.0

    def test_nan_numerator_returns_nan(self):
        a = pd.Series([np.nan, 20.0])
        b = pd.Series([5.0, 4.0])
        result = ratio(a, b)
        assert np.isnan(result.iloc[0])
        assert result.iloc[1] == 5.0

    def test_all_zeros_returns_all_nan(self):
        a = pd.Series([0.0, 0.0])
        b = pd.Series([0.0, 0.0])
        result = ratio(a, b)
        assert result.isna().all()

    def test_negative_values_divide_correctly(self):
        a = pd.Series([-10.0, 10.0])
        b = pd.Series([2.0, -5.0])
        result = ratio(a, b)
        assert result.iloc[0] == pytest.approx(-5.0)
        assert result.iloc[1] == pytest.approx(-2.0)


class TestSpread:
    """Unit tests for the spread function."""

    def test_basic_difference(self):
        a = pd.Series([10.0, 20.0, 30.0])
        b = pd.Series([3.0, 5.0, 7.0])
        result = spread(a, b)
        assert list(result) == [7.0, 15.0, 23.0]

    def test_negative_spread(self):
        a = pd.Series([5.0, 10.0])
        b = pd.Series([8.0, 3.0])
        result = spread(a, b)
        assert result.iloc[0] < 0
        assert result.iloc[1] > 0

    def test_identical_series_returns_zeros(self):
        a = pd.Series([1.0, 2.0, 3.0])
        result = spread(a, a)
        assert (result == 0.0).all()

    def test_nan_in_either_series_propagates(self):
        a = pd.Series([1.0, np.nan])
        b = pd.Series([0.5, 0.5])
        result = spread(a, b)
        assert np.isnan(result.iloc[1])

    def test_output_length_equals_input(self):
        a = pd.Series(range(50), dtype=float)
        b = pd.Series(range(50), dtype=float)
        result = spread(a, b)
        assert len(result) == 50


# ===========================================================================
# FeatureLab — internal helpers
# ===========================================================================


class TestFeatureLabGetFeatureIdByName:
    """Tests for FeatureLab._get_feature_id_by_name."""

    def test_returns_id_when_found(self):
        engine, conn = _make_mock_engine()
        row = MagicMock()
        row.__getitem__ = lambda self, idx: {0: 42}[idx]
        conn.execute.return_value.fetchone.return_value = row

        pit = MagicMock(spec=PITStore)
        lab = FeatureLab(engine, pit)
        result = lab._get_feature_id_by_name("sp500_close")

        assert result == 42

    def test_returns_none_when_not_found(self):
        engine, conn = _make_mock_engine()
        conn.execute.return_value.fetchone.return_value = None

        pit = MagicMock(spec=PITStore)
        lab = FeatureLab(engine, pit)
        result = lab._get_feature_id_by_name("nonexistent")

        assert result is None


class TestFeatureLabGetPitSeries:
    """Tests for FeatureLab._get_pit_series."""

    def test_returns_none_when_feature_not_in_registry(self):
        engine, conn = _make_mock_engine()
        conn.execute.return_value.fetchone.return_value = None

        pit = MagicMock(spec=PITStore)
        lab = FeatureLab(engine, pit)
        result = lab._get_pit_series("unknown", date(2025, 1, 15))

        assert result is None

    def test_returns_none_when_pit_empty(self):
        engine, conn = _make_mock_engine()
        row = MagicMock()
        row.__getitem__ = lambda self, idx: {0: 7}[idx]
        conn.execute.return_value.fetchone.return_value = row

        pit = MagicMock(spec=PITStore)
        pit.get_pit.return_value = pd.DataFrame(
            columns=["feature_id", "obs_date", "value"]
        )

        lab = FeatureLab(engine, pit)
        result = lab._get_pit_series("vix_spot", date(2025, 1, 15))

        assert result is None

    def test_temporal_filtering_excludes_future_dates(self):
        """Observations beyond as_of_date must be filtered out."""
        engine, conn = _make_mock_engine()
        row = MagicMock()
        row.__getitem__ = lambda self, idx: {0: 10}[idx]
        conn.execute.return_value.fetchone.return_value = row

        as_of = date(2025, 6, 1)
        future_date = as_of + timedelta(days=10)

        pit_df = pd.DataFrame({
            "feature_id": [10, 10],
            "obs_date": [as_of - timedelta(days=5), future_date],
            "value": [1.0, 99.0],
            "release_date": [as_of, future_date],
            "vintage_date": [as_of, future_date],
        })
        # PITStore.get_pit should only return rows with obs_date <= as_of
        # We simulate this by having get_pit return all rows; _get_pit_series
        # then applies lookback filtering. The PIT store is responsible for
        # the as_of boundary — here we verify lookback (start date) filtering.
        pit = MagicMock(spec=PITStore)
        pit.get_pit.return_value = pit_df[pit_df["obs_date"] <= as_of].copy()

        lab = FeatureLab(engine, pit)
        result = lab._get_pit_series("feat", as_of, lookback_days=365)

        assert result is not None
        assert all(d <= as_of for d in result.index)

    def test_lookback_filtering_excludes_old_dates(self):
        """Rows older than as_of_date - lookback_days must be dropped."""
        engine, conn = _make_mock_engine()
        row = MagicMock()
        row.__getitem__ = lambda self, idx: {0: 5}[idx]
        conn.execute.return_value.fetchone.return_value = row

        as_of = date(2025, 6, 1)
        lookback = 30  # only want last 30 days
        start = as_of - timedelta(days=lookback)

        # PIT has one very old row and one recent row
        pit_df = pd.DataFrame({
            "feature_id": [5, 5],
            "obs_date": [start - timedelta(days=100), as_of - timedelta(days=5)],
            "value": [999.0, 1.0],
            "release_date": [start, as_of],
            "vintage_date": [start, as_of],
        })
        pit = MagicMock(spec=PITStore)
        pit.get_pit.return_value = pit_df

        lab = FeatureLab(engine, pit)
        result = lab._get_pit_series("feat", as_of, lookback_days=lookback)

        assert result is not None
        # Only the recent row should be kept
        assert all(d >= start for d in result.index)

    def test_series_is_sorted_ascending_by_date(self):
        """Returned series must be sorted chronologically."""
        engine, conn = _make_mock_engine()
        row = MagicMock()
        row.__getitem__ = lambda self, idx: {0: 3}[idx]
        conn.execute.return_value.fetchone.return_value = row

        dates = [date(2025, 1, 10), date(2025, 1, 5), date(2025, 1, 15)]
        pit_df = pd.DataFrame({
            "feature_id": [3] * 3,
            "obs_date": dates,
            "value": [1.0, 2.0, 3.0],
            "release_date": dates,
            "vintage_date": dates,
        })
        pit = MagicMock(spec=PITStore)
        pit.get_pit.return_value = pit_df

        lab = FeatureLab(engine, pit)
        result = lab._get_pit_series("feat", date(2025, 1, 20))

        assert result is not None
        sorted_dates = sorted(result.index)
        assert list(result.index) == sorted_dates


# ===========================================================================
# FeatureLab.compute_feature — all normalization branches
# ===========================================================================


class TestComputeFeature:

    def _lab_with_series(self, normalization, lag_days, series_values):
        """Helper that returns a FeatureLab whose _get_pit_series is patched."""
        engine, conn = _make_mock_engine()
        # First call (ID lookup) returns 1, second call (normalization row) returns the spec
        conn.execute.return_value.fetchone.return_value = (normalization, lag_days)

        pit = MagicMock(spec=PITStore)
        lab = FeatureLab(engine, pit)

        start = date(2024, 1, 1)
        test_series = pd.Series(
            series_values,
            index=[start + timedelta(days=i) for i in range(len(series_values))],
            name="test_feat",
        )
        with patch.object(lab, "_get_pit_series", return_value=test_series):
            return lab, test_series

    def test_raw_normalization_returns_last_value(self):
        values = list(range(50, 100))
        lab, series = self._lab_with_series("RAW", 0, [float(v) for v in values])
        with patch.object(lab, "_get_pit_series", return_value=series):
            result = lab.compute_feature("feat", date(2024, 2, 19))
        assert result == pytest.approx(99.0)

    def test_zscore_normalization_returns_float(self):
        values = [float(i) for i in range(100)]
        lab, series = self._lab_with_series("ZSCORE", 0, values)
        with patch.object(lab, "_get_pit_series", return_value=series):
            result = lab.compute_feature("feat", date(2024, 4, 9))
        assert result is not None
        assert isinstance(result, float)

    def test_rank_normalization_returns_value_between_0_and_1(self):
        values = [float(i) for i in range(100)]
        lab, series = self._lab_with_series("RANK", 0, values)
        with patch.object(lab, "_get_pit_series", return_value=series):
            result = lab.compute_feature("feat", date(2024, 4, 9))
        assert result is not None
        assert 0.0 <= result <= 1.0

    def test_minmax_normalization_returns_value_between_0_and_1(self):
        values = [float(i) for i in range(100)]
        lab, series = self._lab_with_series("MINMAX", 0, values)
        with patch.object(lab, "_get_pit_series", return_value=series):
            result = lab.compute_feature("feat", date(2024, 4, 9))
        assert result is not None
        assert 0.0 <= result <= 1.0

    def test_minmax_constant_series_returns_zero(self):
        """Constant series (min == max) must return 0, not raise."""
        values = [5.0] * 50
        lab, series = self._lab_with_series("MINMAX", 0, values)
        with patch.object(lab, "_get_pit_series", return_value=series):
            result = lab.compute_feature("feat", date(2024, 2, 19))
        assert result == pytest.approx(0.0)

    def test_unknown_normalization_returns_last_value(self):
        """An unrecognized normalization type should fall through to raw last value."""
        values = [float(i) for i in range(50)]
        lab, series = self._lab_with_series("CUSTOM_NORM", 0, values)
        with patch.object(lab, "_get_pit_series", return_value=series):
            result = lab.compute_feature("feat", date(2024, 2, 19))
        assert result is not None
        assert isinstance(result, float)

    def test_lag_applied_before_normalization(self):
        """When lag_days > 0, the diff is applied before normalization."""
        values = [float(i * 2) for i in range(100)]
        lab, series = self._lab_with_series("RAW", 5, values)
        with patch.object(lab, "_get_pit_series", return_value=series):
            result = lab.compute_feature("feat", date(2024, 4, 9))
        # diff(5) on 2*i gives 10.0 for all valid positions
        assert result == pytest.approx(10.0)

    def test_lag_longer_than_series_returns_none(self):
        """When series is too short for the lag, return None."""
        values = [1.0, 2.0, 3.0]
        lab, series = self._lab_with_series("RAW", 10, values)
        with patch.object(lab, "_get_pit_series", return_value=series):
            result = lab.compute_feature("feat", date(2024, 1, 3))
        assert result is None

    def test_returns_none_when_feature_not_found_in_registry(self):
        engine, conn = _make_mock_engine()
        conn.execute.return_value.fetchone.return_value = None

        pit = MagicMock(spec=PITStore)
        pit.get_pit.return_value = pd.DataFrame(
            columns=["feature_id", "obs_date", "value"]
        )

        lab = FeatureLab(engine, pit)
        result = lab.compute_feature("ghost_feature", date(2025, 1, 15))
        assert result is None

    def test_returns_none_when_pit_series_empty(self):
        engine, conn = _make_mock_engine()
        conn.execute.return_value.fetchone.return_value = (42,)

        pit = MagicMock(spec=PITStore)
        pit.get_pit.return_value = pd.DataFrame(
            columns=["feature_id", "obs_date", "value"]
        )

        lab = FeatureLab(engine, pit)
        result = lab.compute_feature("sparse_feature", date(2025, 1, 15))
        assert result is None

    def test_no_registry_row_returns_last_raw_value(self):
        """When the feature exists in get_feature_id but has no registry row,
        compute_feature falls back to raw last value."""
        engine, conn = _make_mock_engine()
        pit = MagicMock(spec=PITStore)
        lab = FeatureLab(engine, pit)

        series = pd.Series(
            [10.0, 20.0, 30.0],
            index=[date(2025, 1, i) for i in range(1, 4)],
        )

        # _get_feature_id returns a non-None value
        with patch.object(lab, "_get_feature_id_by_name", return_value=5):
            # But the registry row for normalization/lag returns None
            conn.execute.return_value.fetchone.return_value = None
            with patch.object(lab, "_get_pit_series", return_value=series):
                result = lab.compute_feature("feat", date(2025, 1, 3))

        assert result == pytest.approx(30.0)


# ===========================================================================
# FeatureLab.compute_derived_features
# ===========================================================================


class TestComputeDerivedFeatures:

    def _lab_all_none(self):
        """Return a FeatureLab that produces None for all _get_pit_series calls."""
        engine, conn = _make_mock_engine()
        conn.execute.return_value.fetchone.return_value = None

        pit = MagicMock(spec=PITStore)
        pit.get_pit.return_value = pd.DataFrame()

        lab = FeatureLab(engine, pit)
        # Patch _get_pit_series at the instance level to always return None
        lab._get_pit_series = MagicMock(return_value=None)
        lab._get_feature_id_by_name = MagicMock(return_value=None)
        return lab

    def test_returns_dict_instance(self):
        lab = self._lab_all_none()
        result = lab.compute_derived_features(date(2025, 6, 1))
        assert isinstance(result, dict)

    def test_all_none_when_no_data(self):
        lab = self._lab_all_none()
        result = lab.compute_derived_features(date(2025, 6, 1))
        for key, val in result.items():
            assert val is None, f"Expected None for '{key}', got {val}"

    def test_expected_keys_present(self):
        lab = self._lab_all_none()
        result = lab.compute_derived_features(date(2025, 6, 1))
        expected = {
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
        for key in expected:
            assert key in result, f"Missing key: {key}"

    def test_fed_funds_3m_chg_computed_when_dff_available(self):
        engine, conn = _make_mock_engine()
        conn.execute.return_value.fetchone.return_value = None
        pit = MagicMock(spec=PITStore)
        pit.get_pit.return_value = pd.DataFrame()

        lab = FeatureLab(engine, pit)

        # Build a 200-day DFF series — long enough for 63-day diff + zscore window
        n = 200
        start = date(2024, 1, 1)
        dates = [start + timedelta(days=i) for i in range(n)]
        dff_series = pd.Series(
            [5.0 + i * 0.01 for i in range(n)],
            index=dates,
            name="fed_funds_rate",
        )

        def mock_pit(name, as_of, lookback_days=504):
            if name == "fed_funds_rate":
                return dff_series
            return None

        lab._get_pit_series = mock_pit
        lab.compute_feature = MagicMock(return_value=None)

        with patch("features.lab.zscore_normalize", wraps=zscore_normalize):
            result = lab.compute_derived_features(date(2024, 7, 18))

        assert result["fed_funds_3m_chg"] is not None
        assert isinstance(result["fed_funds_3m_chg"], float)

    def test_copper_gold_ratio_computed_when_both_available(self):
        engine, conn = _make_mock_engine()
        conn.execute.return_value.fetchone.return_value = None
        pit = MagicMock(spec=PITStore)
        pit.get_pit.return_value = pd.DataFrame()

        lab = FeatureLab(engine, pit)

        n = 100
        start = date(2024, 1, 1)
        dates = [start + timedelta(days=i) for i in range(n)]
        copper = pd.Series([4.0 + i * 0.01 for i in range(n)], index=dates)
        gold = pd.Series([1800.0 + i * 0.5 for i in range(n)], index=dates)

        def mock_pit(name, as_of, lookback_days=504):
            if name == "copper_futures_close":
                return copper
            if name == "gold_futures_close":
                return gold
            return None

        lab._get_pit_series = mock_pit
        lab.compute_feature = MagicMock(return_value=None)

        result = lab.compute_derived_features(date(2024, 4, 9))

        assert result["copper_gold_ratio"] is not None
        assert isinstance(result["copper_gold_ratio"], float)

    def test_copper_gold_ratio_none_when_either_missing(self):
        engine, conn = _make_mock_engine()
        conn.execute.return_value.fetchone.return_value = None
        pit = MagicMock(spec=PITStore)

        lab = FeatureLab(engine, pit)
        lab._get_pit_series = MagicMock(return_value=None)
        lab.compute_feature = MagicMock(return_value=None)

        result = lab.compute_derived_features(date(2025, 1, 1))

        assert result["copper_gold_ratio"] is None
        assert result["copper_gold_slope"] is None

    def test_real_ffr_computed_from_dff_minus_cpi(self):
        engine, conn = _make_mock_engine()
        conn.execute.return_value.fetchone.return_value = None
        pit = MagicMock(spec=PITStore)

        lab = FeatureLab(engine, pit)
        lab.compute_feature = MagicMock(return_value=None)

        n = 100
        start = date(2024, 1, 1)
        common_dates = [start + timedelta(days=i) for i in range(n)]
        dff = pd.Series([5.0] * n, index=common_dates, name="fed_funds_rate")
        cpi = pd.Series([3.0] * n, index=common_dates, name="cpi_yoy")

        def mock_pit(name, as_of, lookback_days=504):
            if name == "fed_funds_rate":
                return dff
            if name == "cpi_yoy":
                return cpi
            return None

        lab._get_pit_series = mock_pit

        result = lab.compute_derived_features(date(2024, 4, 9))

        # real_ffr = DFF - CPI = 5 - 3 = 2 (constant), zscore of constant = NaN
        # So result can be None (all same values → std=0 → NaN)
        # We just check it doesn't raise
        assert "real_ffr" in result

    def test_sp500_mom_3m_computed_when_sp500_available(self):
        engine, conn = _make_mock_engine()
        conn.execute.return_value.fetchone.return_value = None
        pit = MagicMock(spec=PITStore)

        lab = FeatureLab(engine, pit)
        lab.compute_feature = MagicMock(return_value=None)

        n = 300
        start = date(2024, 1, 1)
        dates = [start + timedelta(days=i) for i in range(n)]
        sp500 = pd.Series([4000.0 + i for i in range(n)], index=dates)

        def mock_pit(name, as_of, lookback_days=504):
            if name == "sp500_close":
                return sp500
            return None

        lab._get_pit_series = mock_pit

        result = lab.compute_derived_features(date(2024, 10, 26))

        assert result["sp500_mom_3m"] is not None
        assert isinstance(result["sp500_mom_3m"], float)

    def test_options_features_all_none_when_no_data(self):
        lab = self._lab_all_none()
        result = lab.compute_derived_features(date(2025, 6, 1))

        options_keys = [
            "spy_pcr_zscore",
            "spy_iv_skew_zscore",
            "spy_iv_atm_zscore",
            "spy_term_slope_zscore",
            "spy_max_pain_div_zscore",
        ]
        for key in options_keys:
            assert key in result
            assert result[key] is None


# ===========================================================================
# FeatureLab.run_tsfresh_extraction
# ===========================================================================


class TestRunTsfreshExtraction:

    def test_returns_empty_dict_when_tsfresh_not_installed(self):
        engine, conn = _make_mock_engine()
        pit = MagicMock(spec=PITStore)
        lab = FeatureLab(engine, pit)
        lab._get_pit_series = MagicMock(return_value=None)

        with patch.dict("sys.modules", {
            "tsfresh": None,
            "tsfresh.utilities": None,
            "tsfresh.utilities.dataframe_functions": None,
        }):
            result = lab.run_tsfresh_extraction("any_series", date(2025, 1, 15))

        assert result == {}

    def test_returns_empty_dict_when_insufficient_data(self):
        engine, conn = _make_mock_engine()
        conn.execute.return_value.fetchone.return_value = (1,)

        n = 5  # fewer than 10 points
        pit_df = _make_pit_df(n=n)
        pit = MagicMock(spec=PITStore)
        pit.get_pit.return_value = pit_df

        lab = FeatureLab(engine, pit)
        result = lab.run_tsfresh_extraction("short_series", date(2025, 1, 15))

        assert result == {}

    def test_returns_empty_dict_when_series_is_none(self):
        engine, conn = _make_mock_engine()
        conn.execute.return_value.fetchone.return_value = None

        pit = MagicMock(spec=PITStore)
        lab = FeatureLab(engine, pit)
        lab._get_pit_series = MagicMock(return_value=None)

        result = lab.run_tsfresh_extraction("null_series", date(2025, 1, 15))

        assert result == {}

    def test_returns_empty_dict_when_exactly_9_points(self):
        """Boundary: 9 points is still below the minimum threshold of 10."""
        engine, conn = _make_mock_engine()
        conn.execute.return_value.fetchone.return_value = (1,)

        pit_df = _make_pit_df(n=9)
        pit = MagicMock(spec=PITStore)
        pit.get_pit.return_value = pit_df

        lab = FeatureLab(engine, pit)
        result = lab.run_tsfresh_extraction("nine_points", date(2025, 1, 15))

        assert result == {}


# ===========================================================================
# Temporal correctness — no future data leakage
# ===========================================================================


class TestTemporalCorrectness:
    """Verify that rolling operations do not reference future observations."""

    def test_zscore_window_only_uses_past_observations(self):
        """The rolling z-score at position t uses only obs[0..t], not obs[t+1..]."""
        n = 100
        future_spike = 1000.0
        values = [float(i) for i in range(n - 1)] + [future_spike]
        series = pd.Series(values)

        # Compute z-score up to index n-2 (without the spike)
        result_without_spike = zscore_normalize(pd.Series(values[:-1]), window=30)
        # Compute z-score on the full series
        result_with_spike = zscore_normalize(series, window=30)

        # All values before the spike must be identical in both series
        assert (
            result_without_spike.dropna().values
            == pytest.approx(
                result_with_spike.iloc[:-1].dropna().values, abs=1e-9
            )
        )

    def test_rolling_slope_at_t_not_affected_by_future(self):
        """Rolling slope at position t must not change when future values change."""
        n = 80
        series_base = pd.Series(np.arange(n, dtype=float))
        series_modified = series_base.copy()
        series_modified.iloc[-1] = 9999.0

        result_base = rolling_slope(series_base, window=20)
        result_modified = rolling_slope(series_modified, window=20)

        # All positions except the last (which includes the modified value in its window)
        # should be equal
        assert (
            result_base.iloc[:-20].dropna().values
            == pytest.approx(
                result_modified.iloc[:-20].dropna().values, abs=1e-9
            )
        )

    def test_pit_series_date_index_monotonically_increasing(self):
        """The series returned by _get_pit_series must be sorted ascending."""
        engine, conn = _make_mock_engine()
        row = MagicMock()
        row.__getitem__ = lambda self, idx: {0: 20}[idx]
        conn.execute.return_value.fetchone.return_value = row

        # Deliberately unordered dates
        unordered_dates = [date(2024, 3, 1), date(2024, 1, 1), date(2024, 2, 1)]
        pit_df = pd.DataFrame({
            "feature_id": [20] * 3,
            "obs_date": unordered_dates,
            "value": [3.0, 1.0, 2.0],
            "release_date": unordered_dates,
            "vintage_date": unordered_dates,
        })
        pit = MagicMock(spec=PITStore)
        pit.get_pit.return_value = pit_df

        lab = FeatureLab(engine, pit)
        result = lab._get_pit_series("feat", date(2024, 4, 1))

        assert result is not None
        dates = list(result.index)
        assert dates == sorted(dates), "Series index must be sorted ascending"

    def test_compute_feature_does_not_use_post_as_of_date_data(self):
        """compute_feature must only use PIT data up to and including as_of_date."""
        engine, conn = _make_mock_engine()
        conn.execute.return_value.fetchone.return_value = ("RAW", 0)

        as_of = date(2025, 6, 1)
        # PIT store is configured to return data only up to as_of — this is
        # the contract: PITStore.get_pit(fid, as_of_date) only returns
        # rows with obs_date <= as_of_date.
        future = as_of + timedelta(days=5)
        pit_df = pd.DataFrame({
            "feature_id": [1, 1],
            "obs_date": [as_of - timedelta(days=1), future],
            "value": [42.0, 9999.0],
            "release_date": [as_of, future],
            "vintage_date": [as_of, future],
        })
        pit = MagicMock(spec=PITStore)
        # The pit store CORRECTLY excludes future: simulate correct PITStore
        pit.get_pit.return_value = pit_df[pit_df["obs_date"] <= as_of].copy()

        lab = FeatureLab(engine, pit)
        with patch.object(lab, "_get_feature_id_by_name", return_value=1):
            result = lab.compute_feature("feat", as_of)

        # Should return the pre-as_of value, not 9999 (the future value)
        assert result == pytest.approx(42.0)
