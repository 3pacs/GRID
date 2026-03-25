"""Live signals endpoints."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from api.auth import require_auth
from api.dependencies import get_db_engine, get_pit_store

router = APIRouter(prefix="/api/v1/signals", tags=["signals"])


@router.get("")
async def get_signals(_token: str = Depends(require_auth)) -> dict:
    """Return live signals from the inference engine."""
    try:
        from inference.live import LiveInference

        engine = get_db_engine()
        pit = get_pit_store()
        li = LiveInference(engine, pit)
        result = li.run_inference()
        return {"signals": result}
    except Exception as exc:
        log.warning("Signal generation failed: {e}", e=str(exc))
        return {"signals": {"error": str(exc), "layers": {}}}


@router.get("/snapshot")
async def get_snapshot(_token: str = Depends(require_auth)) -> dict:
    """Return current feature snapshot."""
    try:
        from inference.live import LiveInference

        engine = get_db_engine()
        pit = get_pit_store()
        li = LiveInference(engine, pit)
        df = li.get_feature_snapshot()
        records = df.to_dict("records") if not df.empty else []
        return {"features": records, "count": len(records)}
    except Exception as exc:
        log.warning("Feature snapshot failed: {e}", e=str(exc))
        return {"features": [], "count": 0, "error": str(exc)}


@router.get("/crucix")
async def crucix_signals(
    _token: str = Depends(require_auth),
    engine: Engine = Depends(get_db_engine),
) -> dict:
    """Return latest Crucix-sourced signals."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT f.name, rs.value, rs.obs_date "
                    "FROM resolved_series rs "
                    "JOIN feature_registry f ON f.id = rs.feature_id "
                    "WHERE f.name LIKE :prefix "
                    "AND rs.obs_date = ("
                    "  SELECT MAX(rs2.obs_date) FROM resolved_series rs2 "
                    "  WHERE rs2.feature_id = rs.feature_id"
                    ") "
                    "ORDER BY f.name"
                ),
                {"prefix": "crucix_%"},
            ).fetchall()

            signals = {}
            for name, value, obs_date in rows:
                signals[name] = {
                    "value": float(value) if value is not None else None,
                    "as_of": obs_date.isoformat() if obs_date else None,
                }

            return {
                "source": "crucix",
                "signals": signals,
                "count": len(signals),
            }
    except Exception as exc:
        log.warning("Crucix signals fetch failed: {e}", e=str(exc))
        return {"source": "crucix", "signals": {}, "count": 0, "error": str(exc)}


@router.get("/timeseries")
async def get_timeseries(
    features: str = Query(..., description="Comma-separated feature names"),
    days: int = Query(default=30, ge=7, le=252),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return recent time-series values for one or more features.

    Used for sparkline rendering and trend analysis. Returns one array
    of values per feature, sampled at daily frequency.
    """
    engine = get_db_engine()
    feature_names = [f.strip() for f in features.split(",") if f.strip()][:20]

    if not feature_names:
        return {"series": {}, "days": days}

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT f.name, rs.obs_date, rs.value "
                    "FROM resolved_series rs "
                    "JOIN feature_registry f ON f.id = rs.feature_id "
                    "WHERE f.name = ANY(:names) "
                    "AND rs.obs_date >= CURRENT_DATE - :days "
                    "ORDER BY f.name, rs.obs_date"
                ),
                {"names": feature_names, "days": days},
            ).fetchall()

        series: dict[str, list[float]] = {}
        for name, obs_date, value in rows:
            if name not in series:
                series[name] = []
            series[name].append(float(value) if value is not None else 0.0)

        return {"series": series, "days": days, "count": len(series)}
    except Exception as exc:
        log.warning("Timeseries fetch failed: {e}", e=str(exc))
        return {"series": {}, "days": days, "error": str(exc)}
