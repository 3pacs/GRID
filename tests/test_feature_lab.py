"""Unit tests for features/lab.py transformation functions."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from features.lab import pct_change_lagged, ratio, rolling_slope, spread, zscore_normalize


class TestZscoreNormalize:
    def test_basic_zscore(self):
        series = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0] * 60)
        result = zscore_normalize(series, window=50)
        assert not result.dropna().empty
        assert abs(result.dropna().mean()) < 1.0

    def test_constant_series_returns_nan(self):
        series = pd.Series([5.0] * 100)
        result = zscore_normalize(series, window=50)
        clean = result.dropna()
        assert clean.empty or all(np.isnan(clean))

    def test_short_series(self):
        series = pd.Series([1.0, 2.0, 3.0])
        result = zscore_normalize(series, window=252)
        assert len(result.dropna()) <= len(series)


class TestRollingSlope:
    def test_linear_trend_positive(self):
        series = pd.Series(range(100), dtype=float)
        result = rolling_slope(series, window=20)
        assert result.dropna().iloc[-1] > 0

    def test_flat_series_zero_slope(self):
        series = pd.Series([5.0] * 100)
        result = rolling_slope(series, window=20)
        assert abs(result.dropna().iloc[-1]) < 0.01


class TestRatio:
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


class TestPctChangeLagged:
    def test_basic(self):
        series = pd.Series([100.0, 110.0, 121.0])
        result = pct_change_lagged(series, lag_days=1)
        assert abs(result.iloc[1] - 0.1) < 0.001


class TestSpread:
    def test_basic(self):
        a = pd.Series([10.0, 20.0, 30.0])
        b = pd.Series([3.0, 5.0, 7.0])
        result = spread(a, b)
        assert list(result) == [7.0, 15.0, 23.0]
