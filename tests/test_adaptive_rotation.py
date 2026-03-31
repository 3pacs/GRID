"""Tests for Adaptive Rotation Strategy, Exposure Scaler, Credit Cycle, and Signal Adapter."""

from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from alpha_research.signals.exposure_scaler import (
    compute_vix_exposure_scalar,
    compute_vix_exposure_series,
)
from alpha_research.signals.credit_cycle import compute_credit_cycle
from alpha_research.strategies.adaptive_rotation import (
    ASSET_GROUPS,
    PositionState,
    check_stops,
    detect_regime,
    run_rotation,
    score_groups,
)
from alpha_research.heartbeat import HeartbeatAlert, format_alerts, run_heartbeat


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture
def spy_prices():
    """200 days of SPY prices with upward trend."""
    np.random.seed(42)
    dates = pd.bdate_range("2025-01-01", periods=200)
    returns = np.random.normal(0.0005, 0.01, 200)
    prices = 500 * np.cumprod(1 + returns)
    return pd.Series(prices, index=dates, name="SPY")


@pytest.fixture
def vix_series():
    """200 days of VIX with mean ~20."""
    np.random.seed(43)
    dates = pd.bdate_range("2025-01-01", periods=200)
    vix = 20 + np.cumsum(np.random.normal(0, 0.5, 200))
    vix = np.clip(vix, 10, 80)
    return pd.Series(vix, index=dates, name="VIX")


@pytest.fixture
def multi_ticker_prices():
    """Price panel with all tickers needed for rotation."""
    np.random.seed(44)
    dates = pd.bdate_range("2024-06-01", periods=300)
    tickers = ["SPY", "QQQ", "AAPL", "MSFT", "NVDA", "META", "AMZN", "GOOGL", "TSLA",
               "XOM", "CVX", "GLD", "SLV", "XLE",
               "TLT", "XLU", "XLV", "XLB", "XLI"]

    prices = {}
    for t in tickers:
        drift = np.random.uniform(-0.0002, 0.0004)
        vol = np.random.uniform(0.01, 0.025)
        r = np.random.normal(drift, vol, len(dates))
        prices[t] = 100 * np.cumprod(1 + r)

    return pd.DataFrame(prices, index=dates)


# ── Regime Detection Tests ────────────────────────────────────────────


class TestRegimeDetection:
    def test_risk_on_regime(self, spy_prices, vix_series):
        # Force strong uptrend
        spy_up = spy_prices * np.linspace(0.8, 1.2, len(spy_prices))
        vix_low = pd.Series(15.0, index=vix_series.index, name="VIX")

        regime = detect_regime(spy_up, vix_low, date(2025, 10, 1))
        assert regime.label == "risk-on"
        assert regime.max_groups == 2
        assert regime.cash_floor == 0.0

    def test_risk_off_regime(self, spy_prices, vix_series):
        # Force downtrend + high VIX
        spy_down = spy_prices * np.linspace(1.2, 0.8, len(spy_prices))
        vix_high = pd.Series(45.0, index=vix_series.index, name="VIX")

        regime = detect_regime(spy_down, vix_high, date(2025, 10, 1))
        assert regime.label == "risk-off"
        assert regime.cash_floor >= 0.20

    def test_fast_risk_off_drawdown(self, spy_prices, vix_series):
        # 3-day crash
        spy_crash = spy_prices.copy()
        spy_crash.iloc[-3:] = spy_crash.iloc[-4] * np.array([0.99, 0.97, 0.95])

        regime = detect_regime(spy_crash, vix_series, date(2025, 10, 1))
        assert regime.fast_risk_off is True
        assert regime.label == "risk-off"

    def test_fast_risk_off_vix_spike(self, spy_prices):
        # VIX spike > 3 z-score
        vix_spike = pd.Series(20.0, index=spy_prices.index)
        vix_spike.iloc[-1] = 50.0  # sudden spike

        regime = detect_regime(spy_prices, vix_spike, date(2025, 10, 1))
        assert regime.fast_risk_off is True


# ── Group Scoring Tests ───────────────────────────────────────────────


class TestGroupScoring:
    def test_scores_all_groups(self, multi_ticker_prices):
        scores = score_groups(multi_ticker_prices, multi_ticker_prices["QQQ"])
        assert len(scores) == 3
        for g in scores:
            assert g.name in ASSET_GROUPS
            assert len(g.top_tickers) <= ASSET_GROUPS[g.name]["max_positions"]

    def test_groups_sorted_by_ir(self, multi_ticker_prices):
        scores = score_groups(multi_ticker_prices, multi_ticker_prices["QQQ"])
        irs = [g.info_ratio for g in scores]
        assert irs == sorted(irs, reverse=True)


# ── Stop Loss Tests ───────────────────────────────────────────────────


class TestStops:
    def test_absolute_stop(self):
        pos = {"AAPL": PositionState("AAPL", date(2025, 1, 1), 100.0, 110.0, date(2025, 1, 15))}
        stopped = check_stops(pos, {"AAPL": 94.0}, date(2025, 2, 1))
        assert "AAPL" in stopped

    def test_trailing_stop(self):
        pos = {"AAPL": PositionState("AAPL", date(2025, 1, 1), 100.0, 130.0, date(2025, 1, 15))}
        # Price dropped 11% from peak of 130 → 115.7
        stopped = check_stops(pos, {"AAPL": 115.0}, date(2025, 2, 1))
        assert "AAPL" in stopped

    def test_no_stop(self):
        pos = {"AAPL": PositionState("AAPL", date(2025, 1, 1), 100.0, 110.0, date(2025, 1, 15))}
        stopped = check_stops(pos, {"AAPL": 108.0}, date(2025, 2, 1))
        assert stopped == []


# ── Heartbeat Tests ───────────────────────────────────────────────────


class TestHeartbeat:
    def test_format_alerts_empty(self):
        assert "all clear" in format_alerts([])

    def test_format_alerts_with_items(self):
        alerts = [
            HeartbeatAlert("CRITICAL", "vix", "VIX is high", {}),
            HeartbeatAlert("INFO", "pit", "Data is fresh", {}),
        ]
        formatted = format_alerts(alerts)
        assert "2 alert(s)" in formatted
        assert "CRITICAL" in formatted

    def test_alerts_sorted_by_severity(self):
        alerts = [
            HeartbeatAlert("INFO", "a", "info", {}),
            HeartbeatAlert("CRITICAL", "b", "critical", {}),
            HeartbeatAlert("WARNING", "c", "warning", {}),
        ]
        formatted = format_alerts(alerts)
        # CRITICAL should appear before WARNING which appears before INFO
        crit_pos = formatted.index("CRITICAL")
        warn_pos = formatted.index("WARNING")
        info_pos = formatted.index("INFO")
        assert crit_pos < warn_pos < info_pos
