"""TimesFM forecast endpoints — generate and retrieve time-series forecasts."""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/forecasts", tags=["forecasts"])


# ── Request / Response Models ─────────────────────────────────────────────

class ForecastRequest(BaseModel):
    """Request body for generating a forecast."""

    series_id: str = Field(..., description="Series identifier (e.g. ticker symbol or FRED series)")
    horizon: int = Field(7, ge=1, le=90, description="Number of steps to forecast")
    frequency: str = Field("daily", description="Data frequency: daily, weekly, monthly")


class BatchForecastRequest(BaseModel):
    """Request body for batch forecasting."""

    series_ids: list[str] = Field(..., min_length=1, max_length=100, description="Series identifiers")
    horizon: int = Field(7, ge=1, le=90, description="Number of steps to forecast")
    frequency: str = Field("daily", description="Data frequency: daily, weekly, monthly")


class ForecastResponse(BaseModel):
    """Single forecast result."""

    series_id: str
    forecast_date: str
    horizon: int
    predictions: list[float]
    lower_bound: list[float]
    upper_bound: list[float]
    forecast_std: list[float]
    model_version: str
    frequency: str


# ── GET /health ───────────────────────────────────────────────────────────

@router.get("/health")
async def forecast_health(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Check TimesFM forecaster availability."""
    try:
        from timeseries.timesfm_forecaster import get_forecaster
        forecaster = get_forecaster()
        return forecaster.health_check()
    except Exception as exc:
        return {
            "available": False,
            "error": str(exc),
        }


# ── POST /generate ────────────────────────────────────────────────────────

@router.post("/generate", response_model=ForecastResponse)
async def generate_forecast(
    req: ForecastRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Generate a TimesFM forecast for a single series.

    Fetches historical data from the PIT store and runs TimesFM inference.
    """
    engine = get_db_engine()
    if engine is None:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        from timeseries.timesfm_forecaster import get_forecaster
        forecaster = get_forecaster()
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"TimesFM not available: {exc}",
        )

    if not forecaster.is_available:
        raise HTTPException(
            status_code=503,
            detail="TimesFM package not installed — install with: pip install timesfm",
        )

    # Fetch historical data from resolved_series
    import pandas as pd
    from sqlalchemy import text

    series_data = _fetch_series_data(engine, req.series_id)
    if series_data is None or len(series_data) < 10:
        raise HTTPException(
            status_code=404,
            detail=f"Insufficient data for series '{req.series_id}' (need >= 10 observations)",
        )

    result = forecaster.forecast(
        series=series_data,
        horizon=req.horizon,
        frequency=req.frequency,
        series_id=req.series_id,
    )

    return {
        "series_id": result.series_id,
        "forecast_date": result.forecast_date.isoformat(),
        "horizon": result.horizon,
        "predictions": result.predictions,
        "lower_bound": result.lower_bound,
        "upper_bound": result.upper_bound,
        "forecast_std": result.forecast_std,
        "model_version": result.model_version,
        "frequency": result.frequency,
    }


# ── POST /batch ───────────────────────────────────────────────────────────

@router.post("/batch")
async def batch_forecast(
    req: BatchForecastRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Generate TimesFM forecasts for multiple series in one batch."""
    engine = get_db_engine()
    if engine is None:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        from timeseries.timesfm_forecaster import get_forecaster
        forecaster = get_forecaster()
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"TimesFM not available: {exc}",
        )

    if not forecaster.is_available:
        raise HTTPException(
            status_code=503,
            detail="TimesFM package not installed",
        )

    # Fetch data for all requested series
    import numpy as np

    series_dict: dict[str, np.ndarray] = {}
    missing: list[str] = []

    for sid in req.series_ids:
        data = _fetch_series_data(engine, sid)
        if data is not None and len(data) >= 10:
            series_dict[sid] = data
        else:
            missing.append(sid)

    if not series_dict:
        raise HTTPException(
            status_code=404,
            detail=f"No valid series found. Missing/insufficient: {missing}",
        )

    batch_result = forecaster.batch_forecast(
        series_dict=series_dict,
        horizon=req.horizon,
        frequency=req.frequency,
    )

    forecasts_out = {}
    for sid, fr in batch_result.forecasts.items():
        forecasts_out[sid] = {
            "series_id": fr.series_id,
            "forecast_date": fr.forecast_date.isoformat(),
            "horizon": fr.horizon,
            "predictions": fr.predictions,
            "lower_bound": fr.lower_bound,
            "upper_bound": fr.upper_bound,
            "forecast_std": fr.forecast_std,
            "model_version": fr.model_version,
            "frequency": fr.frequency,
        }

    return {
        "forecasts": forecasts_out,
        "elapsed_seconds": round(batch_result.elapsed_seconds, 2),
        "model_version": batch_result.model_version,
        "missing_series": missing,
    }


# ── Helpers ───────────────────────────────────────────────────────────────

def _fetch_series_data(engine: Any, series_id: str) -> Any:
    """Fetch historical values for a series from resolved_series.

    Uses PIT-correct ordering by obs_date. Returns a numpy array
    of float values, or None if the series is not found.
    """
    import numpy as np
    from sqlalchemy import text

    query = text("""
        SELECT obs_date, value
        FROM resolved_series
        WHERE series_id = :sid
        ORDER BY obs_date ASC
        LIMIT 2048
    """).bindparams(sid=series_id)

    try:
        with engine.connect() as conn:
            rows = conn.execute(query).fetchall()
    except Exception as exc:
        log.warning("Failed to fetch series {s}: {e}", s=series_id, e=str(exc))
        return None

    if not rows:
        return None

    values = np.array([float(r[1]) for r in rows], dtype=np.float32)
    return values
