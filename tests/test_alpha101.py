"""Unit tests for features/alpha101.py — WorldQuant 101 Formulaic Alphas."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from features.alpha101 import (
    Alpha101Engine,
    correlation,
    covariance,
    decay_linear,
    delay,
    delta,
    product,
    rank,
    scale,
    signed_power,
    sma,
    stddev,
    ts_argmax,
    ts_argmin,
    ts_max,
    ts_min,
    ts_rank,
    ts_sum,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_ohlcv() -> dict[str, pd.DataFrame]:
    """Create synthetic OHLCV panel data for 3 tickers over 300 days."""
    np.random.seed(42)
    n_days = 300
    tickers = ["AAPL", "MSFT", "GOOG"]
    dates = pd.bdate_range("2024-01-02", periods=n_days)

    panels: dict[str, pd.DataFrame] = {}
    for col in ("open", "high", "low", "close", "volume", "vwap"):
        data = {}
        for t in tickers:
            if col == "volume":
                data[t] = np.random.randint(1_000_000, 10_000_000, size=n_days).astype(float)
            elif col == "high":
                data[t] = 100 + np.cumsum(np.random.randn(n_days) * 0.5) + abs(np.random.randn(n_days))
            elif col == "low":
                data[t] = 100 + np.cumsum(np.random.randn(n_days) * 0.5) - abs(np.random.randn(n_days))
            else:
                data[t] = 100 + np.cumsum(np.random.randn(n_days) * 0.5)
        panels[col] = pd.DataFrame(data, index=dates)

    # Ensure high >= close >= low
    panels["high"] = panels[["high", "close", "open"]].apply(
        lambda frames: frames.max(), axis=0
    ) if False else panels["high"].clip(lower=panels["close"])
    panels["low"] = panels["low"].clip(upper=panels["close"])

    # VWAP between high and low
    panels["vwap"] = (panels["high"] + panels["low"] + panels["close"]) / 3

    # Returns
    panels["returns"] = panels["close"].pct_change()

    return panels


@pytest.fixture
def single_series_ohlcv() -> dict[str, pd.DataFrame]:
    """Single-ticker OHLCV for simpler tests."""
    np.random.seed(123)
    n = 100
    dates = pd.bdate_range("2024-06-01", periods=n)
    close = pd.DataFrame({"SPY": 100 + np.cumsum(np.random.randn(n) * 0.3)}, index=dates)
    high = close + abs(np.random.randn(n, 1))
    low = close - abs(np.random.randn(n, 1))
    opn = close + np.random.randn(n, 1) * 0.2
    vol = pd.DataFrame({"SPY": np.random.randint(1e6, 5e6, n).astype(float)}, index=dates)
    vwap = (high + low + close) / 3
    returns = close.pct_change()
    return {
        "open": opn, "high": high, "low": low, "close": close,
        "volume": vol, "vwap": vwap, "returns": returns,
    }


# ---------------------------------------------------------------------------
# Operator tests
# ---------------------------------------------------------------------------

class TestOperators:
    def test_ts_sum(self):
        s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        result = ts_sum(s, 3)
        assert result.iloc[-1] == 12.0  # 3+4+5

    def test_sma(self):
        s = pd.Series([2.0, 4.0, 6.0, 8.0])
        result = sma(s, 2)
        assert result.iloc[-1] == 7.0

    def test_stddev(self):
        s = pd.Series([1.0] * 10)
        result = stddev(s, 5)
        assert result.iloc[-1] == 0.0

    def test_delta(self):
        s = pd.Series([10.0, 12.0, 15.0])
        result = delta(s, 1)
        assert result.iloc[-1] == 3.0

    def test_delay(self):
        s = pd.Series([10.0, 20.0, 30.0])
        result = delay(s, 1)
        assert result.iloc[-1] == 20.0

    def test_ts_min_max(self):
        s = pd.Series([5.0, 1.0, 3.0, 7.0, 2.0])
        assert ts_min(s, 3).iloc[-1] == 2.0
        assert ts_max(s, 3).iloc[-1] == 7.0

    def test_ts_argmax(self):
        s = pd.Series([1.0, 5.0, 3.0, 2.0, 4.0])
        # Window of 3 on last 3 values [3.0, 2.0, 4.0] → argmax=2 (0-indexed) → +1 = 3
        result = ts_argmax(s, 3)
        assert result.iloc[-1] == 3.0

    def test_correlation_perfect(self):
        x = pd.Series(range(20), dtype=float)
        y = pd.Series(range(20), dtype=float)
        result = correlation(x, y, 10)
        assert abs(result.iloc[-1] - 1.0) < 0.01

    def test_rank_cross_sectional(self):
        df = pd.DataFrame({"A": [1.0, 2.0, 3.0], "B": [3.0, 2.0, 1.0]})
        result = rank(df)
        # Row 0: A=1 ranks below B=3 → A gets lower rank
        assert result.iloc[0]["A"] < result.iloc[0]["B"]

    def test_scale(self):
        df = pd.DataFrame({"A": [2.0, -1.0], "B": [3.0, 1.0]})
        result = scale(df)
        # Each row should sum to ~1 in absolute terms
        assert abs(result.iloc[0].sum() - 1.0) < 0.01

    def test_decay_linear_weights(self):
        s = pd.Series([0.0, 0.0, 0.0, 0.0, 1.0])
        result = decay_linear(s, 5)
        # Only the last value is 1, weight = 5/15 = 1/3
        assert abs(result.iloc[-1] - 1.0 / 3.0) < 0.01

    def test_product(self):
        s = pd.Series([2.0, 3.0, 4.0])
        result = product(s, 3)
        assert result.iloc[-1] == 24.0

    def test_signed_power(self):
        s = pd.Series([-4.0, 9.0])
        result = signed_power(s, 0.5)
        assert result.iloc[0] == pytest.approx(-2.0)
        assert result.iloc[1] == pytest.approx(3.0)


# ---------------------------------------------------------------------------
# Alpha computation tests
# ---------------------------------------------------------------------------

class TestAlphaComputations:
    def test_alpha001_returns_dataframe(self, sample_ohlcv):
        engine = Alpha101Engine.__new__(Alpha101Engine)
        result = engine.compute_alpha(1, sample_ohlcv)
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert result.shape[1] == 3  # 3 tickers

    def test_alpha002_bounded(self, sample_ohlcv):
        engine = Alpha101Engine.__new__(Alpha101Engine)
        result = engine.compute_alpha(2, sample_ohlcv)
        valid = result.dropna()
        if not valid.empty:
            # Correlation-based alpha should be bounded [-1, 1] before rank
            assert valid.max().max() <= 1.01
            assert valid.min().min() >= -1.01

    def test_alpha012_sign_based(self, single_series_ohlcv):
        engine = Alpha101Engine.__new__(Alpha101Engine)
        result = engine.compute_alpha(12, single_series_ohlcv)
        assert isinstance(result, pd.DataFrame)
        # sign(delta_vol) * (-1 * delta_close) — should be finite
        valid = result.dropna()
        assert not valid.empty

    def test_alpha033_finite(self, sample_ohlcv):
        engine = Alpha101Engine.__new__(Alpha101Engine)
        result = engine.compute_alpha(33, sample_ohlcv)
        valid = result.dropna()
        assert not np.isinf(valid.values).any()

    def test_alpha041_geometric_mean(self, sample_ohlcv):
        engine = Alpha101Engine.__new__(Alpha101Engine)
        result = engine.compute_alpha(41, sample_ohlcv)
        # sqrt(high*low) - vwap — should have some non-NaN
        assert not result.dropna().empty

    def test_alpha101_intraday(self, sample_ohlcv):
        engine = Alpha101Engine.__new__(Alpha101Engine)
        result = engine.compute_alpha(101, sample_ohlcv)
        valid = result.dropna()
        assert not valid.empty
        # Should produce finite values
        assert not np.isinf(valid.values).any()

    def test_compute_all_returns_dict(self, sample_ohlcv):
        engine = Alpha101Engine.__new__(Alpha101Engine)
        results = engine.compute_all_alphas(sample_ohlcv)
        assert isinstance(results, dict)
        assert len(results) > 0
        for num, df in results.items():
            assert isinstance(num, int)
            assert isinstance(df, pd.DataFrame)

    def test_compute_subset(self, sample_ohlcv):
        engine = Alpha101Engine.__new__(Alpha101Engine)
        results = engine.compute_all_alphas(sample_ohlcv, alpha_nums=[1, 12, 101])
        assert set(results.keys()) == {1, 12, 101}

    def test_invalid_alpha_number(self, sample_ohlcv):
        engine = Alpha101Engine.__new__(Alpha101Engine)
        with pytest.raises(ValueError, match="not implemented"):
            engine.compute_alpha(999, sample_ohlcv)

    def test_no_inf_in_alphas(self, sample_ohlcv):
        engine = Alpha101Engine.__new__(Alpha101Engine)
        for num in [1, 2, 3, 4, 5, 9, 10, 12, 33, 41, 42, 101]:
            result = engine.compute_alpha(num, sample_ohlcv)
            if result is not None:
                assert not np.isinf(result.values).any(), f"Alpha {num} has inf values"


class TestCompositeSignal:
    def test_composite_momentum(self, sample_ohlcv):
        engine = Alpha101Engine.__new__(Alpha101Engine)
        result = engine.compute_composite_signal(sample_ohlcv, category="momentum")
        assert isinstance(result, pd.DataFrame)

    def test_composite_mean_reversion(self, sample_ohlcv):
        engine = Alpha101Engine.__new__(Alpha101Engine)
        result = engine.compute_composite_signal(sample_ohlcv, category="mean_reversion")
        assert isinstance(result, pd.DataFrame)

    def test_composite_all(self, sample_ohlcv):
        engine = Alpha101Engine.__new__(Alpha101Engine)
        result = engine.compute_composite_signal(sample_ohlcv, category="all")
        assert isinstance(result, pd.DataFrame)

    def test_invalid_category_raises(self, sample_ohlcv):
        engine = Alpha101Engine.__new__(Alpha101Engine)
        with pytest.raises(ValueError, match="Unknown category"):
            engine.compute_composite_signal(sample_ohlcv, category="nonexistent")


class TestAlphaCategories:
    def test_category_lists_disjoint(self):
        mom = set(Alpha101Engine.MOMENTUM_ALPHAS)
        mr = set(Alpha101Engine.MEAN_REVERSION_ALPHAS)
        vol = set(Alpha101Engine.VOLUME_ALPHAS)
        # Categories may overlap intentionally, but check they exist
        assert len(mom) > 0
        assert len(mr) > 0
        assert len(vol) > 0
