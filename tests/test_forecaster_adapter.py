"""
Tests for the oracle ↔ TimesFM forecaster adapter.

Tests signal conversion, anti-signal detection, and prediction generation
from TimesFM forecast results.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pytest

from oracle.engine import Signal, AntiSignal, PredictionType
from oracle.forecaster_adapter import (
    forecast_to_anti_signals,
    forecast_to_prediction,
    forecast_to_signals,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def bullish_forecast() -> MagicMock:
    """A forecast that predicts price going up."""
    fr = MagicMock()
    fr.predictions = [100.0, 102.0, 104.0, 106.0, 108.0, 110.0, 112.0]
    fr.lower_bound = [98.0, 99.0, 100.0, 101.0, 102.0, 103.0, 104.0]
    fr.upper_bound = [102.0, 105.0, 108.0, 111.0, 114.0, 117.0, 120.0]
    fr.forecast_std = [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0]
    fr.horizon = 7
    fr.model_version = "timesfm-2.0-200m"
    fr.series_id = "SPY"
    fr.forecast_date = date.today()
    return fr


@pytest.fixture
def bearish_forecast() -> MagicMock:
    """A forecast that predicts price going down."""
    fr = MagicMock()
    fr.predictions = [100.0, 98.0, 96.0, 94.0, 92.0, 90.0, 88.0]
    fr.lower_bound = [95.0, 93.0, 91.0, 89.0, 87.0, 85.0, 83.0]
    fr.upper_bound = [105.0, 103.0, 101.0, 99.0, 97.0, 95.0, 93.0]
    fr.forecast_std = [2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5]
    fr.horizon = 7
    fr.model_version = "timesfm-2.0-200m"
    fr.series_id = "SPY"
    fr.forecast_date = date.today()
    return fr


@pytest.fixture
def flat_forecast() -> MagicMock:
    """A forecast that's basically flat."""
    fr = MagicMock()
    fr.predictions = [100.0, 100.1, 100.2, 100.1, 100.0, 100.1, 100.2]
    fr.lower_bound = [98.0] * 7
    fr.upper_bound = [102.0] * 7
    fr.forecast_std = [1.0] * 7
    fr.horizon = 7
    fr.model_version = "timesfm-2.0-200m"
    fr.series_id = "SPY"
    fr.forecast_date = date.today()
    return fr


# ---------------------------------------------------------------------------
# forecast_to_signals
# ---------------------------------------------------------------------------


class TestForecastToSignals:
    def test_bullish_signals(self, bullish_forecast: MagicMock) -> None:
        signals = forecast_to_signals(bullish_forecast, current_price=100.0)

        assert len(signals) >= 2  # direction + confidence + maybe momentum

        # Direction signal should be bullish
        dir_sig = [s for s in signals if "direction" in s.name][0]
        assert dir_sig.direction == "bullish"
        assert dir_sig.family == "timeseries_forecast"
        assert dir_sig.value > 0  # Positive move %

    def test_bearish_signals(self, bearish_forecast: MagicMock) -> None:
        signals = forecast_to_signals(bearish_forecast, current_price=100.0)

        dir_sig = [s for s in signals if "direction" in s.name][0]
        assert dir_sig.direction == "bearish"
        assert dir_sig.value < 0

    def test_flat_signals(self, flat_forecast: MagicMock) -> None:
        signals = forecast_to_signals(flat_forecast, current_price=100.0)

        dir_sig = [s for s in signals if "direction" in s.name][0]
        assert dir_sig.direction == "neutral"

    def test_includes_confidence_signal(self, bullish_forecast: MagicMock) -> None:
        signals = forecast_to_signals(bullish_forecast, current_price=100.0)

        conf_sigs = [s for s in signals if "confidence" in s.name]
        assert len(conf_sigs) == 1
        assert conf_sigs[0].family == "timeseries_forecast"

    def test_includes_momentum_signal(self, bullish_forecast: MagicMock) -> None:
        signals = forecast_to_signals(bullish_forecast, current_price=100.0)

        mom_sigs = [s for s in signals if "momentum" in s.name]
        assert len(mom_sigs) == 1

    def test_empty_forecast(self) -> None:
        signals = forecast_to_signals(None, current_price=100.0)
        assert signals == []

    def test_no_current_price(self, bullish_forecast: MagicMock) -> None:
        """Should still work without current_price (uses first prediction)."""
        signals = forecast_to_signals(bullish_forecast, current_price=None)
        assert len(signals) >= 2

    def test_signal_weight_higher_than_default(self, bullish_forecast: MagicMock) -> None:
        signals = forecast_to_signals(bullish_forecast, current_price=100.0)
        dir_sig = [s for s in signals if "direction" in s.name][0]
        assert dir_sig.weight > 1.0  # Higher weight for forward-looking model


# ---------------------------------------------------------------------------
# forecast_to_anti_signals
# ---------------------------------------------------------------------------


class TestForecastToAntiSignals:
    def test_no_contradiction(self, bullish_forecast: MagicMock) -> None:
        """Bullish forecast + bullish consensus = no anti-signal."""
        bullish_signals = [
            Signal("test1", "equity", 1.0, 1.5, "bullish", 1.0, 0),
            Signal("test2", "flows", 1.0, 1.0, "bullish", 1.0, 0),
        ]
        anti = forecast_to_anti_signals(bullish_forecast, bullish_signals)
        assert len(anti) == 0

    def test_contradiction_detected(self, bullish_forecast: MagicMock) -> None:
        """Bullish forecast + bearish consensus = anti-signal."""
        bearish_signals = [
            Signal("test1", "equity", -1.0, -1.5, "bearish", 1.0, 0),
            Signal("test2", "flows", -1.0, -1.0, "bearish", 1.0, 0),
        ]
        anti = forecast_to_anti_signals(bullish_forecast, bearish_signals)
        assert len(anti) == 1
        assert "contradicts" in anti[0].name
        assert anti[0].severity > 0

    def test_flat_forecast_no_contradiction(self, flat_forecast: MagicMock) -> None:
        signals = [Signal("t", "equity", 1.0, 1.0, "bullish", 1.0, 0)]
        anti = forecast_to_anti_signals(flat_forecast, signals)
        # Flat forecast direction is "neutral" — no contradiction
        assert len(anti) == 0

    def test_empty_forecast(self) -> None:
        anti = forecast_to_anti_signals(None, [])
        assert anti == []


# ---------------------------------------------------------------------------
# forecast_to_prediction
# ---------------------------------------------------------------------------


class TestForecastToPrediction:
    def test_bullish_prediction(self, bullish_forecast: MagicMock) -> None:
        pred = forecast_to_prediction(
            bullish_forecast,
            ticker="SPY",
            current_price=100.0,
        )

        assert pred is not None
        assert pred.ticker == "SPY"
        assert pred.direction == "CALL"
        assert pred.prediction_type == PredictionType.DIRECTION
        assert pred.confidence > 0
        assert pred.confidence <= 1.0
        assert pred.expected_move_pct > 0
        assert pred.model_name == "timeseries_enhanced"
        assert pred.target_price > pred.current_price

    def test_bearish_prediction(self, bearish_forecast: MagicMock) -> None:
        pred = forecast_to_prediction(
            bearish_forecast,
            ticker="QQQ",
            current_price=100.0,
        )

        assert pred is not None
        assert pred.direction == "PUT"
        assert pred.expected_move_pct < 0

    def test_flat_returns_none(self, flat_forecast: MagicMock) -> None:
        """Too flat to generate a meaningful prediction."""
        pred = forecast_to_prediction(
            flat_forecast,
            ticker="SPY",
            current_price=100.0,
        )
        assert pred is None

    def test_prediction_has_flow_context(self, bullish_forecast: MagicMock) -> None:
        pred = forecast_to_prediction(
            bullish_forecast,
            ticker="SPY",
            current_price=100.0,
        )
        assert pred is not None
        assert "forecast_horizon" in pred.flow_context
        assert pred.flow_context["forecast_horizon"] == 7

    def test_prediction_with_signals(self, bullish_forecast: MagicMock) -> None:
        signals = [
            Signal("test", "equity", 1.0, 1.5, "bullish", 1.0, 0),
        ]
        pred = forecast_to_prediction(
            bullish_forecast,
            ticker="SPY",
            current_price=100.0,
            signals=signals,
        )
        assert pred is not None
        assert len(pred.signals) == 1
        assert pred.coherence > 0

    def test_empty_forecast(self) -> None:
        pred = forecast_to_prediction(None, "SPY", 100.0)
        assert pred is None

    def test_prediction_id_is_deterministic(self, bullish_forecast: MagicMock) -> None:
        """Same inputs should produce same prediction ID."""
        p1 = forecast_to_prediction(bullish_forecast, "SPY", 100.0)
        p2 = forecast_to_prediction(bullish_forecast, "SPY", 100.0)
        assert p1 is not None and p2 is not None
        assert p1.id == p2.id
