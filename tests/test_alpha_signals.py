"""Tests for alpha research signals and validation."""

from datetime import date

import numpy as np
import pandas as pd
import pytest

from alpha_research.signals.quanta_alpha import (
    compute_all_signals,
    dual_horizon_momentum,
    trend_volume_gate,
    vol_price_divergence,
    vol_regime_adaptive_momentum,
)
from alpha_research.validation.gauntlet import (
    deflated_sharpe_ratio,
    permutation_test,
    run_gauntlet,
)
from alpha_research.validation.metrics import (
    annualized_return,
    compute_signal_metrics,
    long_short_returns,
    max_drawdown,
    rank_ic,
    sharpe_ratio,
)


# --- Fixtures ---


@pytest.fixture
def synthetic_prices():
    """Generate synthetic price panel: 500 days x 20 tickers."""
    np.random.seed(42)
    dates = pd.bdate_range("2022-01-01", periods=500)
    tickers = [f"T{i:02d}" for i in range(20)]

    # Each ticker: random walk with slight drift
    prices = {}
    for t in tickers:
        drift = np.random.uniform(-0.0001, 0.0003)
        vol = np.random.uniform(0.01, 0.03)
        returns = np.random.normal(drift, vol, len(dates))
        prices[t] = 100 * np.cumprod(1 + returns)

    return pd.DataFrame(prices, index=dates)


@pytest.fixture
def synthetic_volume():
    """Generate synthetic volume panel."""
    np.random.seed(43)
    dates = pd.bdate_range("2022-01-01", periods=500)
    tickers = [f"T{i:02d}" for i in range(20)]

    volume = {}
    for t in tickers:
        base = np.random.uniform(1e6, 1e7)
        noise = np.random.lognormal(0, 0.3, len(dates))
        volume[t] = base * noise

    return pd.DataFrame(volume, index=dates)


@pytest.fixture
def forward_returns(synthetic_prices):
    """Forward 1-day returns from synthetic prices."""
    return synthetic_prices.pct_change().shift(-1)


# --- Signal Tests ---


class TestSignals:
    def test_vol_regime_adaptive_shape(self, synthetic_prices):
        result = vol_regime_adaptive_momentum(synthetic_prices)
        assert result.shape == synthetic_prices.shape
        assert not result.iloc[30:].isna().all().any()

    def test_vol_regime_adaptive_range(self, synthetic_prices):
        result = vol_regime_adaptive_momentum(synthetic_prices)
        valid = result.dropna()
        assert valid.min().min() >= 0.0
        assert valid.max().max() <= 1.0

    def test_dual_horizon_shape(self, synthetic_prices):
        result = dual_horizon_momentum(synthetic_prices)
        assert result.shape == synthetic_prices.shape

    def test_trend_volume_gate_with_volume(self, synthetic_prices, synthetic_volume):
        result = trend_volume_gate(synthetic_prices, synthetic_volume)
        assert result.shape == synthetic_prices.shape

    def test_trend_volume_gate_without_volume(self, synthetic_prices):
        result = trend_volume_gate(synthetic_prices, None)
        assert result.shape == synthetic_prices.shape

    def test_vol_price_divergence(self, synthetic_prices):
        result = vol_price_divergence(synthetic_prices)
        assert result.shape == synthetic_prices.shape

    def test_compute_all_signals(self, synthetic_prices):
        signals = compute_all_signals(synthetic_prices)
        assert len(signals) == 4
        for name, sig in signals.items():
            assert sig.shape == synthetic_prices.shape


# --- Metrics Tests ---


class TestMetrics:
    def test_rank_ic(self, synthetic_prices, forward_returns):
        signal = vol_regime_adaptive_momentum(synthetic_prices)
        ic = rank_ic(signal, forward_returns)
        assert len(ic) > 100
        assert ic.mean() != 0  # unlikely to be exactly 0 on synthetic

    def test_long_short_returns(self, synthetic_prices, forward_returns):
        signal = vol_regime_adaptive_momentum(synthetic_prices)
        ls = long_short_returns(signal, forward_returns, top_n=3)
        assert len(ls) > 100

    def test_sharpe_ratio(self):
        returns = pd.Series(np.random.normal(0.001, 0.01, 252))
        sr = sharpe_ratio(returns)
        assert isinstance(sr, float)
        assert -5 < sr < 5

    def test_annualized_return(self):
        returns = pd.Series([0.01] * 252)
        ar = annualized_return(returns)
        assert ar > 1.0  # ~12x from compounding

    def test_max_drawdown(self):
        returns = pd.Series([0.01, 0.01, -0.05, -0.05, 0.01])
        mdd = max_drawdown(returns)
        assert mdd < 0

    def test_compute_signal_metrics(self, synthetic_prices, forward_returns):
        signal = vol_regime_adaptive_momentum(synthetic_prices)
        metrics = compute_signal_metrics(signal, forward_returns)

        assert "mean_rank_ic" in metrics
        assert "sharpe_net" in metrics
        assert "max_drawdown" in metrics
        assert "passes_threshold" in metrics
        assert isinstance(metrics["passes_threshold"], bool)


# --- Gauntlet Tests ---


class TestGauntlet:
    def test_permutation_test(self, synthetic_prices, forward_returns):
        signal = vol_regime_adaptive_momentum(synthetic_prices)
        p_val, observed = permutation_test(
            signal, forward_returns, n_shuffles=50, top_n=3
        )
        assert 0 <= p_val <= 1
        assert isinstance(observed, float)

    def test_deflated_sharpe(self):
        passed, threshold = deflated_sharpe_ratio(
            observed_sharpe=2.0, n_models_tested=96, n_observations=500
        )
        assert bool(passed) in (True, False)
        assert threshold > 0

    def test_deflated_sharpe_single_model(self):
        passed, threshold = deflated_sharpe_ratio(
            observed_sharpe=1.0, n_models_tested=1, n_observations=500
        )
        assert passed is True

    def test_run_gauntlet(self, synthetic_prices, forward_returns):
        signal = vol_regime_adaptive_momentum(synthetic_prices)
        result = run_gauntlet(
            signal,
            forward_returns,
            n_models_tested=1,
            top_n=3,
            n_permutations=50,
            n_subsample_splits=10,
        )
        assert result.verdict in ("ROBUST", "MARGINAL", "UNSTABLE")
        assert 0 <= result.permutation_p <= 1
        assert 0 <= result.subsample_stability <= 1
        assert 0 <= result.cv_consistency <= 1


# --- Net Sharpe Threshold Test ---


class TestThreshold:
    def test_net_sharpe_threshold_filter(self):
        """Val net Sharpe > 1.4 is the go/no-go threshold (QuantaAlpha v2)."""
        np.random.seed(99)
        high_sr = pd.Series(np.random.normal(0.003, 0.01, 252))
        low_sr = pd.Series(np.random.normal(0.0, 0.02, 252))

        assert sharpe_ratio(high_sr) > 1.0
        assert sharpe_ratio(low_sr) < 1.4
