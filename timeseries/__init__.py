"""
GRID Time-Series Forecasting.

Provides TimesFM-based probabilistic forecasting for all GRID signals.
TimesFM is Google's pretrained time-series foundation model that generates
point forecasts with calibrated uncertainty intervals.

Features:
  - Zero-shot forecasting (no fine-tuning required)
  - Probabilistic outputs with confidence intervals
  - Batch forecasting across multiple series
  - Oracle engine integration via signal adapter
"""

from timeseries.timesfm_forecaster import TimesFMForecaster, get_forecaster

__all__ = ["TimesFMForecaster", "get_forecaster"]
