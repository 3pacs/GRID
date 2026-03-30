"""Live signals endpoints."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
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
    except ImportError as exc:
        raise HTTPException(status_code=500, detail=f"Inference engine not available: {exc}") from exc
    except Exception as exc:
        log.warning("Signal generation failed: {e}", e=str(exc))
        raise HTTPException(status_code=500, detail=f"Signal generation failed: {exc}") from exc


@router.get("/snapshot")
async def get_snapshot(_token: str = Depends(require_auth)) -> dict:
    """Return current feature snapshot with z-scores."""
    try:
        from inference.live import LiveInference

        engine = get_db_engine()
        pit = get_pit_store()
        li = LiveInference(engine, pit)
        df = li.get_feature_snapshot()
        if df.empty:
            return {"features": [], "count": 0}

        records = df.to_dict("records")

        # Compute z-scores from historical data (252-day lookback)
        try:
            with engine.connect() as conn:
                feat_rows = conn.execute(text(
                    "SELECT id, name FROM feature_registry "
                    "WHERE model_eligible = TRUE"
                )).fetchall()
            name_to_id = {r[1]: r[0] for r in feat_rows}
            feature_ids = [name_to_id[r["name"]] for r in records if r["name"] in name_to_id]

            if feature_ids:
                today = date.today()
                hist = pit.get_feature_matrix(
                    feature_ids=feature_ids,
                    start_date=today - timedelta(days=504),
                    end_date=today,
                    as_of_date=today,
                    vintage_policy="LATEST_AS_OF",
                )
                if hist is not None and len(hist) > 20:
                    means = hist.mean()
                    stds = hist.std().replace(0, 1)
                    last = hist.ffill().iloc[-1]
                    z_map = ((last - means) / stds).to_dict()

                    # Map feature_id columns back to names
                    id_to_name = {r[0]: r[1] for r in feat_rows}
                    z_by_name = {}
                    for col, z in z_map.items():
                        name = id_to_name.get(col)
                        if name is not None:
                            z_by_name[name] = round(z, 4) if z == z else None

                    for rec in records:
                        z = z_by_name.get(rec["name"])
                        if z is not None:
                            rec["z_score"] = z
        except Exception as zex:
            log.warning("Z-score computation failed: {e}", e=str(zex))

        return {"features": records, "count": len(records)}
    except ImportError as exc:
        raise HTTPException(status_code=500, detail=f"Inference engine not available: {exc}") from exc
    except Exception as exc:
        log.warning("Feature snapshot failed: {e}", e=str(exc))
        raise HTTPException(status_code=500, detail=f"Feature snapshot failed: {exc}") from exc


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
        raise HTTPException(status_code=500, detail=f"Crucix signals fetch failed: {exc}") from exc


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
        raise HTTPException(status_code=400, detail="At least one feature name is required")

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
        raise HTTPException(status_code=500, detail=f"Timeseries fetch failed: {exc}") from exc


@router.get("/timeframes")
async def get_timeframes(
    feature: str = Query(..., description="Feature name to compare across timeframes"),
    periods: str = Query(default="5d,5w,3m,1y,5y", description="Comma-separated periods"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return feature values across multiple timeframes for visual comparison.

    Each period returns: values array, start/end values, change_pct, and dates.
    Periods: Nd (days), Nw (weeks), Nm (months), Ny (years).
    """
    import re

    engine = get_db_engine()
    pit = get_pit_store()
    today = date.today()

    # Parse periods
    period_days = {}
    for p in [x.strip() for x in periods.split(",") if x.strip()][:8]:
        m = re.match(r"^(\d+)([dwmy])$", p.lower())
        if not m:
            continue
        n, unit = int(m.group(1)), m.group(2)
        days = {"d": 1, "w": 7, "m": 30, "y": 365}[unit] * n
        period_days[p] = days

    if not period_days:
        raise HTTPException(status_code=400, detail="No valid periods specified")

    # Resolve feature ID
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id FROM feature_registry WHERE name = :name"),
            {"name": feature},
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Feature '{feature}' not found")
        feature_id = row[0]

    # Fetch the maximum lookback we need
    max_days = max(period_days.values())
    try:
        hist = pit.get_feature_matrix(
            feature_ids=[feature_id],
            start_date=today - timedelta(days=max_days + 30),
            end_date=today,
            as_of_date=today,
            vintage_policy="LATEST_AS_OF",
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Historical data fetch failed: {exc}") from exc

    if hist is None or hist.empty or feature_id not in hist.columns:
        raise HTTPException(status_code=404, detail="No historical data available for feature")

    series = hist[feature_id].dropna()
    if series.empty:
        raise HTTPException(status_code=404, detail="No data values available for feature")

    result_periods = {}
    for label, days in period_days.items():
        cutoff = today - timedelta(days=days)
        window = series[series.index >= cutoff]
        if window.empty:
            result_periods[label] = {"values": [], "error": "No data for period"}
            continue

        vals = window.tolist()
        start_val = float(vals[0])
        end_val = float(vals[-1])
        change_pct = round(((end_val - start_val) / abs(start_val)) * 100, 2) if start_val != 0 else 0.0

        # Downsample to max ~60 points for charting
        step = max(1, len(vals) // 60)
        sampled = vals[::step]
        if vals[-1] != sampled[-1]:
            sampled.append(vals[-1])

        result_periods[label] = {
            "values": [round(v, 6) for v in sampled],
            "dates": [str(d) for d in window.index[::step].tolist()][:len(sampled)],
            "start": round(start_val, 6),
            "end": round(end_val, 6),
            "change_pct": change_pct,
            "count": len(vals),
        }

    return {"feature": feature, "periods": result_periods}
