"""Regime-matched analog engine API endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/intelligence", tags=["intelligence", "regime"])


@router.get("/regime")
async def get_regime(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Current regime classification and state vector."""
    try:
        from intelligence.regime.state_vector import get_or_compute_state_vector
        from intelligence.regime.classifier import classify_regime

        engine = get_db_engine()
        sv = get_or_compute_state_vector(engine)
        regime = classify_regime(sv)

        return {
            "state_vector": sv.to_dict(),
            "regime": regime.to_dict(),
        }
    except Exception as exc:
        log.warning("Regime classification failed: {e}", e=str(exc))
        return {"error": str(exc)}


@router.get("/regime/analogs")
async def get_regime_analogs(
    n: int = Query(20, ge=5, le=50, description="Number of matches"),
    min_quality: float = Query(0.4, ge=0.1, le=0.9, description="Min match quality"),
    include_timesfm: bool = Query(True, description="Include TimesFM comparison"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Find analogous historical episodes and generate conditional forecast.

    Optionally includes TimesFM foundation model forecast for side-by-side
    comparison. The analog forecast is the primary signal; TimesFM is a
    second opinion from a different methodology.
    """
    try:
        from intelligence.regime.state_vector import get_or_compute_state_vector
        from intelligence.regime.episode_matcher import find_analogous_episodes
        from intelligence.regime.forecast import generate_conditional_forecast
        from intelligence.regime.classifier import classify_regime

        engine = get_db_engine()
        sv = get_or_compute_state_vector(engine)
        regime = classify_regime(sv)
        matches = find_analogous_episodes(engine, sv, n=n, min_quality=min_quality)
        forecast = generate_conditional_forecast(engine, matches)

        result: dict[str, Any] = {
            "regime": regime.to_dict(),
            "matches": matches.to_dict(),
            "forecast": forecast.to_dict(),
        }

        # TimesFM comparison (non-blocking — failure doesn't break the response)
        if include_timesfm:
            try:
                result["timesfm"] = _get_timesfm_comparison(engine)
            except Exception as tfm_exc:
                log.debug("TimesFM comparison skipped: {e}", e=str(tfm_exc))
                result["timesfm"] = {"available": False, "reason": str(tfm_exc)}

        return result
    except Exception as exc:
        log.warning("Regime analogs failed: {e}", e=str(exc))
        return {"error": str(exc)}


def _get_timesfm_comparison(engine: object) -> dict[str, Any]:
    """Run TimesFM forecasts on key series for side-by-side comparison.

    Returns a dict of ticker -> {horizon -> forecast_value} using the
    TimesFM foundation model with maximum available context.
    """
    import numpy as np
    from sqlalchemy import text as sa_text

    TICKERS = {
        'SPY': 'YF:SPY:close',
        'QQQ': 'YF:QQQ:close',
        'GLD': 'YF:GLD:close',
        'TLT': 'YF:TLT:close',
        'VIX': 'VIXCLS',
    }
    HORIZONS = [7, 14, 30, 60, 90, 128]

    try:
        import timesfm
        import torch
        torch.set_float32_matmul_precision('high')
    except ImportError:
        return {"available": False, "reason": "timesfm not installed"}

    # Check GPU availability
    gpu_available = torch.cuda.is_available()
    gpu_free = 0
    if gpu_available:
        gpu_free = (torch.cuda.get_device_properties(0).total_memory - torch.cuda.memory_allocated()) / 1e9

    if gpu_available and gpu_free < 2.0:
        return {"available": False, "reason": f"insufficient GPU memory ({gpu_free:.1f}GB free)"}

    try:
        model = timesfm.TimesFM_2p5_200M_torch.from_pretrained('google/timesfm-2.5-200m-pytorch')
        config = timesfm.ForecastConfig(max_context=16256, max_horizon=128, per_core_batch_size=32)
        model.compile(config)
    except Exception as exc:
        return {"available": False, "reason": f"model load failed: {exc}"}

    results: dict[str, Any] = {"available": True, "model": "timesfm-2.5-200m"}
    forecasts: dict[str, dict] = {}

    for label, series_id in TICKERS.items():
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    sa_text(
                        "SELECT value FROM raw_series WHERE series_id = :sid "
                        "AND pull_status = 'SUCCESS' AND value IS NOT NULL "
                        "ORDER BY obs_date"
                    ),
                    {"sid": series_id},
                ).fetchall()

            if len(rows) < 100:
                continue

            prices = np.array([float(r[0]) for r in rows]).astype(np.float32)
            current = float(prices[-1])
            series = prices[-min(len(prices), 16256):]

            pf_raw, qf = model.forecast(128, [series])
            pf = pf_raw[0][:128]

            # Quantiles: col 0 = point dup, 1 = lowest, -1 = highest
            q = qf[0][:128] if qf is not None else None

            horizon_data: dict[str, Any] = {}
            for h in HORIZONS:
                i = h - 1
                entry: dict[str, float] = {
                    "forecast": float(pf[i]),
                    "current": current,
                }
                if label in ('VIX', 'HY_SPREAD'):
                    entry["change"] = float(pf[i] - current)
                else:
                    entry["return"] = float((pf[i] - current) / current)

                if q is not None:
                    entry["low_10"] = float(q[i, 2])
                    entry["high_90"] = float(q[i, -2])
                    entry["low_2_5"] = float(q[i, 1])
                    entry["high_97_5"] = float(q[i, -1])

                horizon_data[str(h)] = entry

            forecasts[label] = {
                "current": current,
                "context_length": len(series),
                "horizons": horizon_data,
            }
        except Exception as exc:
            log.debug("TimesFM forecast for {t} failed: {e}", t=label, e=str(exc))

    results["forecasts"] = forecasts

    del model
    if gpu_available:
        torch.cuda.empty_cache()

    return results


@router.get("/regime/history")
async def get_regime_history(
    days: int = Query(365, ge=30, le=3650, description="Lookback days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Regime classification history over a date range."""
    try:
        from datetime import date, timedelta
        from intelligence.regime.state_vector import load_cached_vectors
        from intelligence.regime.classifier import classify_regime

        engine = get_db_engine()
        cutoff = date.today() - timedelta(days=days)
        vectors = load_cached_vectors(engine)
        recent = [v for v in vectors if v.as_of_date >= cutoff]

        history = []
        for sv in recent:
            regime = classify_regime(sv)
            history.append({
                "date": sv.as_of_date.isoformat(),
                "composite_label": regime.composite_label,
                "completeness": sv.completeness,
                "labels": {l.axis: l.label for l in regime.labels},
            })

        return {
            "history": history,
            "count": len(history),
            "from": cutoff.isoformat(),
            "to": date.today().isoformat(),
        }
    except Exception as exc:
        log.warning("Regime history failed: {e}", e=str(exc))
        return {"error": str(exc)}
