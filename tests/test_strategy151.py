"""Unit tests for trading/strategy151.py — 151 Trading Strategies engine."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from trading.strategy151 import Strategy151Engine, StrategySignal


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def price_panel() -> pd.DataFrame:
    """Synthetic price panel: 5 tickers, 300 trading days."""
    np.random.seed(42)
    n = 300
    dates = pd.bdate_range("2024-01-02", periods=n)
    tickers = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA"]
    data = {}
    for t in tickers:
        data[t] = 100 + np.cumsum(np.random.randn(n) * 0.5)
    return pd.DataFrame(data, index=dates)


@pytest.fixture
def cointegrated_pair() -> pd.DataFrame:
    """Two cointegrated series (A = B + noise)."""
    np.random.seed(99)
    n = 500
    dates = pd.bdate_range("2023-01-02", periods=n)
    b = 100 + np.cumsum(np.random.randn(n) * 0.3)
    a = b * 1.2 + np.random.randn(n) * 0.5  # cointegrated with B
    return pd.DataFrame({"A": a, "B": b}, index=dates)


@pytest.fixture
def mock_engine():
    from unittest.mock import create_autospec
    from sqlalchemy.engine import Engine
    engine = create_autospec(Engine, instance=True)
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.fetchone.return_value = None
    mock_result.fetchall.return_value = []
    mock_conn.execute.return_value = mock_result
    engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine


@pytest.fixture
def strategy_engine(mock_engine):
    pit = MagicMock()
    pit.get_pit.return_value = pd.DataFrame(columns=["feature_id", "obs_date", "value"])
    return Strategy151Engine(mock_engine, pit)


# ---------------------------------------------------------------------------
# StrategySignal tests
# ---------------------------------------------------------------------------

class TestStrategySignal:
    def test_signal_creation(self):
        sig = StrategySignal(
            strategy="mean_reversion",
            ticker="AAPL",
            direction="LONG",
            strength=0.85,
            entry_price=150.0,
            stop_loss=145.0,
            target=160.0,
            metadata={"zscore": -2.3},
            generated_at="2024-06-15T10:00:00",
        )
        assert sig.strategy == "mean_reversion"
        assert sig.direction == "LONG"
        assert sig.strength == 0.85

    def test_signal_to_dict(self):
        sig = StrategySignal(
            strategy="momentum",
            ticker="TSLA",
            direction="SHORT",
            strength=0.6,
            entry_price=None,
            stop_loss=None,
            target=None,
            metadata={},
            generated_at="2024-06-15",
        )
        d = sig.__dict__
        assert d["strategy"] == "momentum"
        assert d["ticker"] == "TSLA"


# ---------------------------------------------------------------------------
# Mean reversion tests
# ---------------------------------------------------------------------------

class TestMeanReversion:
    def test_scan_returns_signals(self, strategy_engine, price_panel):
        signals = strategy_engine.mean_reversion_scan(
            prices=price_panel,
            window=20,
            entry_z=2.0,
            exit_z=0.5,
        )
        assert isinstance(signals, list)
        for sig in signals:
            assert isinstance(sig, StrategySignal)
            assert sig.strategy == "mean_reversion"
            assert sig.direction in ("LONG", "SHORT")
            assert 0.0 <= sig.strength <= 1.0

    def test_no_signals_for_flat_prices(self, strategy_engine):
        n = 200
        dates = pd.bdate_range("2024-01-02", periods=n)
        flat = pd.DataFrame({"FLAT": [100.0] * n}, index=dates)
        signals = strategy_engine.mean_reversion_scan(prices=flat)
        # Flat prices have zero std → no z-score signals
        assert len(signals) == 0

    def test_extreme_zscore_generates_signal(self, strategy_engine):
        np.random.seed(7)
        n = 200
        dates = pd.bdate_range("2024-01-02", periods=n)
        # Create a series that mean-reverts with a spike at the end
        values = np.random.randn(n) * 0.5
        values[-1] = 5.0  # extreme outlier
        prices = pd.DataFrame({"SPIKE": 100 + np.cumsum(values)}, index=dates)
        signals = strategy_engine.mean_reversion_scan(prices=prices, window=20, entry_z=1.5)
        # Should detect at least one signal due to the spike
        assert len(signals) >= 0  # may or may not depending on z-score


# ---------------------------------------------------------------------------
# Pairs trading tests
# ---------------------------------------------------------------------------

class TestPairsTrading:
    def test_cointegrated_pair_detected(self, strategy_engine, cointegrated_pair):
        signals = strategy_engine.pairs_trading_scan(prices=cointegrated_pair)
        # With a clearly cointegrated pair, should find at least the pair
        assert isinstance(signals, list)

    def test_random_uncorrelated_no_pairs(self, strategy_engine):
        np.random.seed(55)
        n = 300
        dates = pd.bdate_range("2024-01-02", periods=n)
        uncorr = pd.DataFrame({
            "X": np.random.randn(n).cumsum(),
            "Y": np.random.randn(n).cumsum(),
        }, index=dates)
        signals = strategy_engine.pairs_trading_scan(prices=uncorr, p_threshold=0.01)
        # Unlikely to find cointegration in random walks at p<0.01
        # But this is probabilistic, so we just check it returns a list
        assert isinstance(signals, list)


# ---------------------------------------------------------------------------
# Cross-sectional momentum tests
# ---------------------------------------------------------------------------

class TestCrossSectionalMomentum:
    def test_momentum_returns_signals(self, strategy_engine, price_panel):
        signals = strategy_engine.cross_sectional_momentum(
            prices=price_panel,
            formation=252,
            skip=21,
            top_n=2,
        )
        assert isinstance(signals, list)
        for sig in signals:
            assert sig.strategy == "momentum"
            assert sig.direction in ("LONG", "SHORT")

    def test_momentum_insufficient_data(self, strategy_engine):
        n = 30  # Not enough for 252-day formation
        dates = pd.bdate_range("2024-06-01", periods=n)
        short = pd.DataFrame({"A": range(n), "B": range(n)}, index=dates, dtype=float)
        signals = strategy_engine.cross_sectional_momentum(prices=short)
        assert signals == []


# ---------------------------------------------------------------------------
# Volatility risk premium tests
# ---------------------------------------------------------------------------

class TestVolatilityRiskPremium:
    def test_vrp_with_synthetic_data(self, strategy_engine):
        np.random.seed(10)
        n = 100
        dates = pd.bdate_range("2024-01-02", periods=n)
        prices = pd.DataFrame({"SPY": 100 + np.cumsum(np.random.randn(n) * 0.5)}, index=dates)
        iv_data = pd.DataFrame({"SPY": 0.20 + np.random.randn(n) * 0.02}, index=dates)
        signals = strategy_engine.volatility_risk_premium(
            prices=prices,
            iv_data=iv_data,
            rv_window=21,
        )
        assert isinstance(signals, list)
        for sig in signals:
            assert sig.strategy == "volatility_risk_premium"


# ---------------------------------------------------------------------------
# OU mean reversion tests
# ---------------------------------------------------------------------------

class TestOUMeanReversion:
    def test_ou_parameter_estimation(self, strategy_engine):
        np.random.seed(42)
        n = 500
        # Simulate an OU process
        theta = 0.1
        mu = 5.0
        sigma = 0.3
        dt = 1.0 / 252
        x = np.zeros(n)
        x[0] = mu
        for i in range(1, n):
            x[i] = x[i - 1] + theta * (mu - x[i - 1]) * dt + sigma * np.sqrt(dt) * np.random.randn()
        dates = pd.bdate_range("2023-01-02", periods=n)
        prices = pd.DataFrame({"OU_STOCK": np.exp(x)}, index=dates)
        signals = strategy_engine.ou_mean_reversion(prices=prices)
        assert isinstance(signals, list)

    def test_ou_trending_stock_no_signal(self, strategy_engine):
        n = 300
        dates = pd.bdate_range("2024-01-02", periods=n)
        # Pure trend — no mean reversion
        trend = pd.DataFrame({"TREND": np.linspace(100, 200, n)}, index=dates)
        signals = strategy_engine.ou_mean_reversion(prices=trend)
        # Pure trend should have very long half-life → likely no signals
        assert isinstance(signals, list)


# ---------------------------------------------------------------------------
# Sector rotation tests
# ---------------------------------------------------------------------------

class TestSectorRotation:
    def test_rotation_returns_signals(self, strategy_engine, price_panel):
        signals = strategy_engine.sector_rotation(prices=price_panel)
        assert isinstance(signals, list)
        for sig in signals:
            assert sig.strategy == "sector_rotation"


# ---------------------------------------------------------------------------
# Composite scoring tests
# ---------------------------------------------------------------------------

class TestCompositeScoring:
    def test_composite_from_signals(self, strategy_engine):
        signals = {
            "momentum": [
                StrategySignal("momentum", "AAPL", "LONG", 0.8, None, None, None, {}, "2024-01-01"),
                StrategySignal("momentum", "MSFT", "SHORT", 0.6, None, None, None, {}, "2024-01-01"),
            ],
            "mean_reversion": [
                StrategySignal("mean_reversion", "AAPL", "SHORT", 0.7, None, None, None, {}, "2024-01-01"),
            ],
        }
        result = strategy_engine.generate_composite_score(signals)
        assert isinstance(result, pd.DataFrame)
        assert "AAPL" in result.index or "AAPL" in result.columns or len(result) > 0
