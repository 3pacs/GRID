"""
GRID TimesFM Forecaster.

Google's TimesFM is a pretrained time-series foundation model that produces
probabilistic forecasts with calibrated uncertainty. This module wraps it
for GRID's oracle engine and signal pipeline.

TimesFM features:
  - Zero-shot forecasting — no fine-tuning required
  - 200M parameters trained on 100B+ time points (Google Trends, Wiki, synthetic)
  - Supports variable context lengths (up to 512 time steps for v1, 2048 for v2)
  - Outputs quantile forecasts for calibrated uncertainty
  - Runs on GPU (CUDA) or CPU

Installation:
  pip install timesfm   # or: pip install timesfm[torch]

Usage:
  forecaster = TimesFMForecaster()
  result = forecaster.forecast(prices, horizon=7)
  # result.predictions, result.lower_bound, result.upper_bound
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log


@dataclass(frozen=True)
class ForecastResult:
    """Result of a single time-series forecast.

    Attributes:
        series_id: Identifier for the forecasted series (e.g. ticker or series_id).
        forecast_date: Date the forecast was generated.
        horizon: Number of steps forecasted.
        predictions: Point forecast values (median).
        lower_bound: Lower confidence interval (2.5th percentile).
        upper_bound: Upper confidence interval (97.5th percentile).
        forecast_std: Standard deviation of the forecast distribution.
        model_version: TimesFM model version used.
        frequency: Data frequency (daily, weekly, etc.).
    """

    series_id: str
    forecast_date: date
    horizon: int
    predictions: list[float]
    lower_bound: list[float]
    upper_bound: list[float]
    forecast_std: list[float]
    model_version: str = "timesfm-2.0-200m"
    frequency: str = "daily"


@dataclass(frozen=True)
class BatchForecastResult:
    """Result of batch forecasting across multiple series.

    Attributes:
        forecasts: Mapping of series_id to ForecastResult.
        elapsed_seconds: Time taken for the batch.
        model_version: TimesFM model version used.
    """

    forecasts: dict[str, ForecastResult]
    elapsed_seconds: float
    model_version: str = "timesfm-2.0-200m"


# Frequency mapping for TimesFM
_FREQ_MAP: dict[str, int] = {
    "daily": 0,       # TimesFM freq_type=0 for generic/daily
    "weekly": 1,      # freq_type=1 for weekly
    "monthly": 2,     # freq_type=2 for monthly
    "hourly": 0,      # treat as generic
    "minutely": 0,    # treat as generic
}


class TimesFMForecaster:
    """Wrapper around Google TimesFM for GRID signal forecasting.

    Lazily loads the model on first forecast call to avoid blocking
    server startup when TimesFM is not needed.

    Parameters:
        model_name: HuggingFace model name or local path.
        backend: Inference backend ("gpu", "cpu", or "tpu").
        context_length: Maximum context window (time steps).
        horizon: Default forecast horizon (time steps).
        quantiles: Quantile levels for uncertainty intervals.
    """

    def __init__(
        self,
        model_name: str = "google/timesfm-2.0-200m-pytorch",
        backend: str = "gpu",
        context_length: int = 512,
        horizon: int = 7,
        quantiles: tuple[float, ...] = (0.025, 0.1, 0.25, 0.5, 0.75, 0.9, 0.975),
    ) -> None:
        self.model_name = model_name
        self.backend = backend
        self.context_length = context_length
        self.default_horizon = horizon
        self.quantiles = quantiles
        self._model: Any = None
        self._available: bool | None = None  # None = not yet checked

    @property
    def is_available(self) -> bool:
        """Whether TimesFM is importable and the model can be loaded."""
        if self._available is None:
            try:
                import timesfm  # noqa: F401
                self._available = True
            except ImportError:
                log.warning(
                    "timesfm package not installed — "
                    "install with: pip install timesfm"
                )
                self._available = False
        return self._available

    def _ensure_model(self) -> Any:
        """Lazily load the TimesFM model on first use.

        Returns:
            The loaded TimesFM model instance.

        Raises:
            RuntimeError: If TimesFM is not installed.
        """
        if self._model is not None:
            return self._model

        if not self.is_available:
            raise RuntimeError(
                "TimesFM not available — install with: pip install timesfm"
            )

        import timesfm

        log.info(
            "Loading TimesFM model={m}, backend={b}, ctx={c}",
            m=self.model_name,
            b=self.backend,
            c=self.context_length,
        )

        self._model = timesfm.TimesFm(
            hparams=timesfm.TimesFmHparams(
                per_core_batch_size=32,
                horizon_len=self.default_horizon,
                context_len=self.context_length,
                backend=self.backend,
            ),
            checkpoint=timesfm.TimesFmCheckpoint(
                huggingface_repo_id=self.model_name,
            ),
        )

        log.info("TimesFM model loaded successfully")
        return self._model

    def forecast(
        self,
        series: np.ndarray | pd.Series,
        horizon: int | None = None,
        frequency: str = "daily",
        series_id: str = "unknown",
    ) -> ForecastResult:
        """Generate a probabilistic forecast for a single time series.

        Parameters:
            series: Historical values as a 1-D array or pandas Series.
                    NaN values are handled by TimesFM internally.
            horizon: Number of steps to forecast (overrides default).
            frequency: Data frequency ("daily", "weekly", "monthly").
            series_id: Identifier for logging and result tracking.

        Returns:
            ForecastResult with point forecast and confidence intervals.
        """
        model = self._ensure_model()

        if isinstance(series, pd.Series):
            series = series.values

        # Truncate to context_length if needed
        if len(series) > self.context_length:
            series = series[-self.context_length:]

        horizon = horizon or self.default_horizon
        freq_type = _FREQ_MAP.get(frequency, 0)

        log.debug(
            "TimesFM forecast — series={s}, len={n}, horizon={h}, freq={f}",
            s=series_id,
            n=len(series),
            h=horizon,
            f=frequency,
        )

        # TimesFM expects list of arrays for batch interface
        point_forecasts, quantile_forecasts = model.forecast(
            [series.astype(np.float32)],
            freq=[freq_type],
        )

        # Extract results for this single series
        point = point_forecasts[0][:horizon].tolist()

        # Quantile forecasts shape: (batch, horizon, num_quantiles)
        if quantile_forecasts is not None and len(quantile_forecasts) > 0:
            q_data = quantile_forecasts[0][:horizon]  # (horizon, num_quantiles)
            # Find indices for 2.5th and 97.5th percentiles
            q_list = list(self.quantiles)
            idx_lower = q_list.index(0.025) if 0.025 in q_list else 0
            idx_upper = q_list.index(0.975) if 0.975 in q_list else -1
            lower = q_data[:, idx_lower].tolist()
            upper = q_data[:, idx_upper].tolist()
        else:
            # Fallback: estimate intervals from point forecast volatility
            series_std = float(np.nanstd(series[-min(30, len(series)):]))
            lower = [p - 1.96 * series_std for p in point]
            upper = [p + 1.96 * series_std for p in point]

        # Compute forecast std from interval width
        forecast_std = [
            (u - l) / 3.92 for u, l in zip(upper, lower)  # 97.5 - 2.5 = 3.92 sigma
        ]

        return ForecastResult(
            series_id=series_id,
            forecast_date=date.today(),
            horizon=horizon,
            predictions=point,
            lower_bound=lower,
            upper_bound=upper,
            forecast_std=forecast_std,
            model_version=self.model_name.split("/")[-1] if "/" in self.model_name else self.model_name,
            frequency=frequency,
        )

    def batch_forecast(
        self,
        series_dict: dict[str, np.ndarray | pd.Series],
        horizon: int | None = None,
        frequency: str = "daily",
    ) -> BatchForecastResult:
        """Forecast multiple time series in a single batch.

        Parameters:
            series_dict: Mapping of series_id to historical values.
            horizon: Number of steps to forecast.
            frequency: Data frequency for all series.

        Returns:
            BatchForecastResult with per-series ForecastResult objects.
        """
        import time as _time

        model = self._ensure_model()
        horizon = horizon or self.default_horizon
        freq_type = _FREQ_MAP.get(frequency, 0)

        start = _time.monotonic()

        # Prepare batch arrays
        series_ids = list(series_dict.keys())
        arrays = []
        for sid in series_ids:
            s = series_dict[sid]
            if isinstance(s, pd.Series):
                s = s.values
            if len(s) > self.context_length:
                s = s[-self.context_length:]
            arrays.append(s.astype(np.float32))

        log.info(
            "TimesFM batch forecast — {n} series, horizon={h}",
            n=len(arrays),
            h=horizon,
        )

        # Run batch inference
        point_forecasts, quantile_forecasts = model.forecast(
            arrays,
            freq=[freq_type] * len(arrays),
        )

        elapsed = _time.monotonic() - start

        # Build per-series results
        forecasts: dict[str, ForecastResult] = {}
        for i, sid in enumerate(series_ids):
            point = point_forecasts[i][:horizon].tolist()

            if quantile_forecasts is not None and len(quantile_forecasts) > i:
                q_data = quantile_forecasts[i][:horizon]
                q_list = list(self.quantiles)
                idx_lower = q_list.index(0.025) if 0.025 in q_list else 0
                idx_upper = q_list.index(0.975) if 0.975 in q_list else -1
                lower = q_data[:, idx_lower].tolist()
                upper = q_data[:, idx_upper].tolist()
            else:
                series_std = float(np.nanstd(arrays[i][-min(30, len(arrays[i])):]))
                lower = [p - 1.96 * series_std for p in point]
                upper = [p + 1.96 * series_std for p in point]

            forecast_std = [(u - l) / 3.92 for u, l in zip(upper, lower)]

            forecasts[sid] = ForecastResult(
                series_id=sid,
                forecast_date=date.today(),
                horizon=horizon,
                predictions=point,
                lower_bound=lower,
                upper_bound=upper,
                forecast_std=forecast_std,
                model_version=self.model_name.split("/")[-1] if "/" in self.model_name else self.model_name,
                frequency=frequency,
            )

        log.info(
            "TimesFM batch complete — {n} series in {t:.1f}s",
            n=len(forecasts),
            t=elapsed,
        )

        return BatchForecastResult(
            forecasts=forecasts,
            elapsed_seconds=elapsed,
            model_version=self.model_name.split("/")[-1] if "/" in self.model_name else self.model_name,
        )

    def health_check(self) -> dict[str, Any]:
        """Return a structured health-check result.

        Returns:
            dict: Keys ``available``, ``model``, ``backend``, ``context_length``.
        """
        return {
            "available": self.is_available,
            "model_loaded": self._model is not None,
            "model": self.model_name,
            "backend": self.backend,
            "context_length": self.context_length,
            "default_horizon": self.default_horizon,
        }


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_forecaster_instance: TimesFMForecaster | None = None


def get_forecaster() -> TimesFMForecaster:
    """Return a cached TimesFMForecaster singleton.

    Returns:
        TimesFMForecaster: Shared forecaster instance.
    """
    global _forecaster_instance
    if _forecaster_instance is None:
        from config import settings

        _forecaster_instance = TimesFMForecaster(
            model_name=settings.TIMESFM_MODEL_NAME,
            backend=settings.TIMESFM_BACKEND,
            context_length=settings.TIMESFM_CONTEXT_LENGTH,
            horizon=settings.TIMESFM_HORIZON,
        )
    return _forecaster_instance
