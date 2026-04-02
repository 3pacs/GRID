"""
Tests for the TimesFM forecaster module.

Tests the TimesFMForecaster class with mocked timesfm imports
so the actual model weights are not required.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch, PropertyMock

import numpy as np
import pandas as pd
import pytest

from timeseries.timesfm_forecaster import (
    BatchForecastResult,
    ForecastResult,
    TimesFMForecaster,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def forecaster() -> TimesFMForecaster:
    """Create a forecaster with default settings."""
    return TimesFMForecaster(
        model_name="google/timesfm-2.0-200m-pytorch",
        backend="cpu",
        context_length=512,
        horizon=7,
    )


@pytest.fixture
def sample_series() -> np.ndarray:
    """Generate a synthetic price series for testing."""
    np.random.seed(42)
    # Random walk with drift (simulates stock price)
    returns = np.random.normal(0.001, 0.02, 100)
    prices = 100.0 * np.cumprod(1 + returns)
    return prices.astype(np.float32)


@pytest.fixture
def mock_timesfm_module():
    """Create a mock timesfm module."""
    mock_module = MagicMock()

    # Mock TimesFmHparams
    mock_module.TimesFmHparams.return_value = MagicMock()

    # Mock TimesFmCheckpoint
    mock_module.TimesFmCheckpoint.return_value = MagicMock()

    # Mock TimesFm model
    mock_model = MagicMock()
    mock_module.TimesFm.return_value = mock_model

    return mock_module, mock_model


# ---------------------------------------------------------------------------
# ForecastResult
# ---------------------------------------------------------------------------


class TestForecastResult:
    def test_frozen_dataclass(self) -> None:
        result = ForecastResult(
            series_id="SPY",
            forecast_date=date(2026, 4, 1),
            horizon=7,
            predictions=[100.0, 101.0, 102.0],
            lower_bound=[98.0, 99.0, 100.0],
            upper_bound=[102.0, 103.0, 104.0],
            forecast_std=[1.0, 1.0, 1.0],
        )
        assert result.series_id == "SPY"
        assert result.horizon == 7
        assert len(result.predictions) == 3

        with pytest.raises(AttributeError):
            result.series_id = "QQQ"  # type: ignore[misc]

    def test_default_values(self) -> None:
        result = ForecastResult(
            series_id="BTC",
            forecast_date=date.today(),
            horizon=7,
            predictions=[1.0],
            lower_bound=[0.5],
            upper_bound=[1.5],
            forecast_std=[0.25],
        )
        assert result.model_version == "timesfm-2.0-200m"
        assert result.frequency == "daily"


# ---------------------------------------------------------------------------
# TimesFMForecaster — availability
# ---------------------------------------------------------------------------


class TestForecasterAvailability:
    def test_available_when_timesfm_installed(self, forecaster: TimesFMForecaster) -> None:
        with patch.dict("sys.modules", {"timesfm": MagicMock()}):
            forecaster._available = None  # Reset cache
            assert forecaster.is_available is True

    def test_unavailable_when_not_installed(self, forecaster: TimesFMForecaster) -> None:
        with patch.dict("sys.modules", {"timesfm": None}):
            with patch("builtins.__import__", side_effect=ImportError("no timesfm")):
                forecaster._available = None  # Reset cache
                assert forecaster.is_available is False

    def test_health_check(self, forecaster: TimesFMForecaster) -> None:
        hc = forecaster.health_check()
        assert "available" in hc
        assert hc["model"] == "google/timesfm-2.0-200m-pytorch"
        assert hc["backend"] == "cpu"
        assert hc["context_length"] == 512
        assert hc["default_horizon"] == 7
        assert hc["model_loaded"] is False


# ---------------------------------------------------------------------------
# TimesFMForecaster — single forecast
# ---------------------------------------------------------------------------


class TestForecasterForecast:
    def test_forecast_with_numpy_array(
        self,
        forecaster: TimesFMForecaster,
        sample_series: np.ndarray,
        mock_timesfm_module: tuple,
    ) -> None:
        mock_module, mock_model = mock_timesfm_module

        # Setup mock forecast output
        horizon = 7
        point_forecasts = np.array([[110.0, 111.0, 112.0, 113.0, 114.0, 115.0, 116.0]])
        quantile_forecasts = np.random.randn(1, horizon, 7) * 5 + 110  # (batch, horizon, quantiles)

        mock_model.forecast.return_value = (point_forecasts, quantile_forecasts)

        with patch.dict("sys.modules", {"timesfm": mock_module}):
            forecaster._available = True
            forecaster._model = mock_model

            result = forecaster.forecast(
                series=sample_series,
                horizon=horizon,
                frequency="daily",
                series_id="TEST",
            )

        assert isinstance(result, ForecastResult)
        assert result.series_id == "TEST"
        assert result.horizon == horizon
        assert len(result.predictions) == horizon
        assert len(result.lower_bound) == horizon
        assert len(result.upper_bound) == horizon
        assert len(result.forecast_std) == horizon
        assert result.frequency == "daily"

    def test_forecast_with_pandas_series(
        self,
        forecaster: TimesFMForecaster,
        sample_series: np.ndarray,
        mock_timesfm_module: tuple,
    ) -> None:
        mock_module, mock_model = mock_timesfm_module

        pd_series = pd.Series(sample_series)

        point_forecasts = np.array([[100.0] * 7])
        mock_model.forecast.return_value = (point_forecasts, None)

        with patch.dict("sys.modules", {"timesfm": mock_module}):
            forecaster._available = True
            forecaster._model = mock_model

            result = forecaster.forecast(
                series=pd_series,
                horizon=7,
                series_id="PD_TEST",
            )

        assert isinstance(result, ForecastResult)
        assert result.series_id == "PD_TEST"

    def test_forecast_truncates_long_series(
        self,
        forecaster: TimesFMForecaster,
        mock_timesfm_module: tuple,
    ) -> None:
        mock_module, mock_model = mock_timesfm_module

        # Create a series longer than context_length
        long_series = np.random.randn(1000).astype(np.float32)

        point_forecasts = np.array([[100.0] * 7])
        mock_model.forecast.return_value = (point_forecasts, None)

        with patch.dict("sys.modules", {"timesfm": mock_module}):
            forecaster._available = True
            forecaster._model = mock_model

            result = forecaster.forecast(
                series=long_series,
                series_id="LONG",
            )

        # Verify the model was called with truncated data
        call_args = mock_model.forecast.call_args
        actual_input = call_args[0][0][0]
        assert len(actual_input) == 512  # context_length

    def test_forecast_fallback_intervals_when_no_quantiles(
        self,
        forecaster: TimesFMForecaster,
        sample_series: np.ndarray,
        mock_timesfm_module: tuple,
    ) -> None:
        """When quantile_forecasts is None, intervals are estimated from series std."""
        mock_module, mock_model = mock_timesfm_module

        point = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0]
        point_forecasts = np.array([point])
        mock_model.forecast.return_value = (point_forecasts, None)

        with patch.dict("sys.modules", {"timesfm": mock_module}):
            forecaster._available = True
            forecaster._model = mock_model

            result = forecaster.forecast(
                series=sample_series,
                series_id="NO_Q",
            )

        # Lower bound should be below predictions
        for lb, pred in zip(result.lower_bound, result.predictions):
            assert lb < pred

        # Upper bound should be above predictions
        for ub, pred in zip(result.upper_bound, result.predictions):
            assert ub > pred

    def test_forecast_raises_when_unavailable(
        self,
        forecaster: TimesFMForecaster,
        sample_series: np.ndarray,
    ) -> None:
        forecaster._available = False
        with pytest.raises(RuntimeError, match="TimesFM not available"):
            forecaster.forecast(sample_series, series_id="FAIL")


# ---------------------------------------------------------------------------
# TimesFMForecaster — batch forecast
# ---------------------------------------------------------------------------


class TestForecasterBatch:
    def test_batch_forecast(
        self,
        forecaster: TimesFMForecaster,
        sample_series: np.ndarray,
        mock_timesfm_module: tuple,
    ) -> None:
        mock_module, mock_model = mock_timesfm_module

        series_dict = {
            "SPY": sample_series,
            "QQQ": sample_series * 1.1,
            "IWM": sample_series * 0.9,
        }

        # Mock batch output
        n = len(series_dict)
        horizon = 7
        point_forecasts = np.random.randn(n, horizon).astype(np.float32) + 100
        quantile_forecasts = None

        mock_model.forecast.return_value = (point_forecasts, quantile_forecasts)

        with patch.dict("sys.modules", {"timesfm": mock_module}):
            forecaster._available = True
            forecaster._model = mock_model

            result = forecaster.batch_forecast(
                series_dict=series_dict,
                horizon=horizon,
            )

        assert isinstance(result, BatchForecastResult)
        assert len(result.forecasts) == 3
        assert "SPY" in result.forecasts
        assert "QQQ" in result.forecasts
        assert "IWM" in result.forecasts
        assert result.elapsed_seconds >= 0

        for sid, fr in result.forecasts.items():
            assert fr.series_id == sid
            assert fr.horizon == horizon
            assert len(fr.predictions) == horizon

    def test_batch_with_pandas_series(
        self,
        forecaster: TimesFMForecaster,
        sample_series: np.ndarray,
        mock_timesfm_module: tuple,
    ) -> None:
        mock_module, mock_model = mock_timesfm_module

        series_dict = {
            "A": pd.Series(sample_series),
            "B": pd.Series(sample_series * 1.1),
        }

        point_forecasts = np.random.randn(2, 7).astype(np.float32) + 100
        mock_model.forecast.return_value = (point_forecasts, None)

        with patch.dict("sys.modules", {"timesfm": mock_module}):
            forecaster._available = True
            forecaster._model = mock_model

            result = forecaster.batch_forecast(series_dict)

        assert len(result.forecasts) == 2


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------


class TestForecasterSingleton:
    def test_get_forecaster_returns_same_instance(self) -> None:
        import timeseries.timesfm_forecaster as tf

        tf._forecaster_instance = None  # Reset

        mock_settings = MagicMock()
        mock_settings.TIMESFM_MODEL_NAME = "google/timesfm-2.0-200m-pytorch"
        mock_settings.TIMESFM_BACKEND = "cpu"
        mock_settings.TIMESFM_CONTEXT_LENGTH = 512
        mock_settings.TIMESFM_HORIZON = 7

        with patch("config.settings", mock_settings):
            f1 = tf.get_forecaster()
            f2 = tf.get_forecaster()

        assert f1 is f2
        tf._forecaster_instance = None  # Clean up
