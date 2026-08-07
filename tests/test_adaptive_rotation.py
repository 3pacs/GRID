"""Tests for Adaptive Rotation Strategy, Exposure Scaler, Credit Cycle, and Signal Adapter."""

from datetime import date

import numpy as np
import pandas as pd
import pytest

from alpha_research.strategies.adaptive_rotation import (
    ASSET_GROUPS,
    PositionState,
    check_stops,
    detect_regime,
    run_rotation,
    score_groups,
)
from alpha_research.heartbeat import HeartbeatAlert, format_alerts


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

    def test_update_peak_advances_on_new_high(self):
        pos = PositionState("AAPL", date(2025, 1, 1), 100.0, 110.0, date(2025, 1, 15))
        pos.update_peak(125.0, date(2025, 2, 1))
        assert pos.peak_price == 125.0
        assert pos.peak_date == date(2025, 2, 1)

    def test_update_peak_ignores_lower_price(self):
        pos = PositionState("AAPL", date(2025, 1, 1), 100.0, 130.0, date(2025, 1, 15))
        pos.update_peak(120.0, date(2025, 2, 1))
        assert pos.peak_price == 130.0
        assert pos.peak_date == date(2025, 1, 15)


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


# ── run_rotation Determinism (regression for synthetic-random VIX bug) ────────


class TestRunRotationDeterministicVix:
    """run_rotation previously fabricated a 20-day VIX history via
    np.random.normal(...) when only the current value was known — every call
    produced a different vix_zscore and could flip the regime label.
    """

    def test_run_rotation_source_has_no_np_random(self):
        import inspect
        from alpha_research.strategies import adaptive_rotation

        src = inspect.getsource(adaptive_rotation.run_rotation)
        assert "np.random" not in src, (
            "run_rotation must not call np.random — feeds non-deterministic "
            "VIX history into detect_regime (P0, auditor 2026-06-07)."
        )

    def test_run_rotation_uses_pit_correct_vix_query(self, monkeypatch, multi_ticker_prices):
        from alpha_research.strategies import adaptive_rotation

        captured_vix_calls = []

        def fake_get_vix_series(engine, start_date=None, end_date=None, as_of_date=None):
            captured_vix_calls.append({
                "start_date": start_date,
                "end_date": end_date,
                "as_of_date": as_of_date,
            })
            # Return deterministic 30-day VIX series
            idx = pd.bdate_range(end=as_of_date, periods=30)
            return pd.Series(np.linspace(18.0, 22.0, 30), index=idx, name="VIX")

        def fake_build_price_panel(engine, tickers=None, start_date=None, end_date=None):
            return multi_ticker_prices.loc[:pd.Timestamp(end_date)]

        monkeypatch.setattr(adaptive_rotation, "get_vix_series", fake_get_vix_series)
        monkeypatch.setattr(adaptive_rotation, "build_price_panel", fake_build_price_panel)

        as_of = date(2025, 5, 1)
        r1 = run_rotation(engine=object(), as_of_date=as_of)
        r2 = run_rotation(engine=object(), as_of_date=as_of)

        # Determinism: identical inputs -> identical regime.vix_zscore
        assert r1.regime.vix_zscore == r2.regime.vix_zscore
        # PIT contract: as_of_date is plumbed through
        assert captured_vix_calls[0]["as_of_date"] == as_of
        # No silent random fallback: vix_zscore is computed from the real series
        assert r1.regime.vix_zscore != 0.0

    def test_run_rotation_falls_back_safely_when_vix_unavailable(self, monkeypatch, multi_ticker_prices):
        from alpha_research.strategies import adaptive_rotation

        monkeypatch.setattr(
            adaptive_rotation,
            "get_vix_series",
            lambda *a, **kw: pd.Series(dtype=float, name="VIX"),
        )
        monkeypatch.setattr(
            adaptive_rotation,
            "build_price_panel",
            lambda *a, **kw: multi_ticker_prices,
        )
        # compute_vix_exposure_scalar is imported lazily inside run_rotation;
        # patch its module so the lazy import resolves to our stub.
        import alpha_research.signals.exposure_scaler as exposure_scaler

        monkeypatch.setattr(
            exposure_scaler,
            "compute_vix_exposure_scalar",
            lambda *a, **kw: {"vix": 21.5, "vix_ma": 20.0},
        )

        result = run_rotation(engine=object(), as_of_date=date(2025, 5, 1))
        # With <20 obs, detect_regime returns vix_zscore=0 (deterministic).
        assert result.regime.vix_zscore == 0.0
