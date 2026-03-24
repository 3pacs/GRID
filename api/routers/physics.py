"""
GRID API — Market physics endpoints.

Provides REST API access to physics verification, conventions, and transforms:
  GET  /api/v1/physics/verify           — Run full verification suite
  GET  /api/v1/physics/momentum         — News sentiment momentum analysis
  GET  /api/v1/physics/conventions      — List all financial conventions
  GET  /api/v1/physics/conventions/{domain} — Get convention for a domain
  GET  /api/v1/physics/ou/{feature}     — Estimate OU parameters for a feature
  GET  /api/v1/physics/hurst/{feature}  — Compute Hurst exponent for a feature
  GET  /api/v1/physics/energy/{feature} — Compute energy decomposition for a feature
"""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from loguru import logger as log

router = APIRouter(prefix="/api/v1/physics", tags=["physics"])


@router.get("/verify")
async def verify(as_of: str | None = Query(default=None)) -> dict[str, Any]:
    """Run full market physics verification suite."""
    from db import get_engine
    from physics.verify import MarketPhysicsVerifier
    from store.pit import PITStore

    try:
        as_of_date = date.fromisoformat(as_of) if as_of else date.today()
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid date format '{as_of}'. Use ISO format: YYYY-MM-DD",
        )

    try:
        engine = get_engine()
        pit = PITStore(engine)
        verifier = MarketPhysicsVerifier(engine, pit)
        results = verifier.verify_all(as_of_date)
        return results
    except Exception as exc:
        log.error("Physics verification endpoint failed: {e}", e=str(exc))
        raise HTTPException(
            status_code=500,
            detail=f"Verification failed: {str(exc)}",
        )


@router.get("/momentum")
async def momentum(
    as_of: str | None = Query(default=None),
    lookback_days: int = Query(default=90, ge=7, le=365),
) -> dict[str, Any]:
    """Analyze news sentiment momentum using GDELT data.

    Returns sentiment trend, momentum direction, kinetic energy state,
    and optional cross-correlation with price features.
    Gracefully degrades if GDELT data is not yet available.
    """
    from db import get_engine
    from physics.momentum import NewsMomentumAnalyzer
    from store.pit import PITStore

    try:
        as_of_date = date.fromisoformat(as_of) if as_of else date.today()
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid date format '{as_of}'. Use ISO format: YYYY-MM-DD",
        )

    try:
        engine = get_engine()
        pit = PITStore(engine)
        analyzer = NewsMomentumAnalyzer(engine, pit)
        result = analyzer.analyze(as_of_date, lookback_days=lookback_days)
        return result.to_dict()
    except Exception as exc:
        log.error("News momentum endpoint failed: {e}", e=str(exc))
        raise HTTPException(
            status_code=500,
            detail=f"Momentum analysis failed: {str(exc)}",
        )


@router.get("/conventions")
async def list_conventions() -> dict[str, Any]:
    """List all financial conventions."""
    from physics.conventions import list_conventions

    return {"conventions": list_conventions()}


@router.get("/conventions/{domain}")
async def get_convention(domain: str) -> dict[str, Any]:
    """Get convention for a specific domain."""
    from physics.conventions import get_convention as _get

    conv = _get(domain)
    if conv is None:
        raise HTTPException(status_code=404, detail=f"Domain '{domain}' not found")

    return {
        "domain": conv.domain,
        "unit": conv.unit,
        "annualized": conv.annualized,
        "day_count": conv.day_count,
        "method": conv.method,
        "trading_days": conv.trading_days,
        "frequency": conv.frequency,
        "notes": conv.notes,
    }


@router.get("/ou/{feature}")
async def ou_parameters(
    feature: str,
    window: int = Query(default=252, ge=30, le=2520),
) -> dict[str, Any]:
    """Estimate Ornstein-Uhlenbeck parameters for a feature.

    Returns theta (mean-reversion speed), mu (equilibrium), sigma (noise),
    and half-life in trading days.
    """
    from db import get_engine
    from features.lab import FeatureLab
    from physics.transforms import estimate_ou_parameters
    from store.pit import PITStore

    engine = get_engine()
    pit = PITStore(engine)
    lab = FeatureLab(engine, pit)

    series = lab._get_pit_series(feature, date.today(), lookback_days=window * 2)
    if series is None or len(series) < 30:
        raise HTTPException(
            status_code=404,
            detail=f"Insufficient data for feature '{feature}'",
        )

    params = estimate_ou_parameters(series)
    params["feature"] = feature
    params["window"] = window
    params["data_points"] = len(series)
    return params


@router.get("/hurst/{feature}")
async def hurst(
    feature: str,
    max_lag: int = Query(default=100, ge=10, le=500),
) -> dict[str, Any]:
    """Compute Hurst exponent for a feature.

    H < 0.5: mean-reverting, H = 0.5: random walk, H > 0.5: trending.
    """
    from db import get_engine
    from features.lab import FeatureLab
    from physics.transforms import hurst_exponent
    from store.pit import PITStore

    engine = get_engine()
    pit = PITStore(engine)
    lab = FeatureLab(engine, pit)

    series = lab._get_pit_series(feature, date.today(), lookback_days=504)
    if series is None or len(series) < 50:
        raise HTTPException(
            status_code=404,
            detail=f"Insufficient data for feature '{feature}'",
        )

    h = hurst_exponent(series, max_lag)
    interpretation = "random walk"
    if h < 0.45:
        interpretation = "mean-reverting"
    elif h > 0.55:
        interpretation = "trending/persistent"

    return {
        "feature": feature,
        "hurst_exponent": round(float(h), 4) if not (h != h) else None,
        "interpretation": interpretation,
        "data_points": len(series),
    }


@router.get("/energy/{feature}")
async def energy_decomposition(
    feature: str,
    short_window: int = Query(default=21),
    long_window: int = Query(default=252),
) -> dict[str, Any]:
    """Compute kinetic/potential/total energy decomposition for a feature."""
    from db import get_engine
    from features.lab import FeatureLab
    from physics.transforms import kinetic_energy, potential_energy, total_energy
    from store.pit import PITStore

    engine = get_engine()
    pit = PITStore(engine)
    lab = FeatureLab(engine, pit)

    series = lab._get_pit_series(feature, date.today(), lookback_days=long_window * 2)
    if series is None or len(series) < long_window:
        raise HTTPException(
            status_code=404,
            detail=f"Insufficient data for feature '{feature}'",
        )

    ke = kinetic_energy(series, short_window)
    pe = potential_energy(series, long_window)
    te = total_energy(series, short_window, long_window)

    # Return latest values
    ke_val = ke.dropna().iloc[-1] if not ke.dropna().empty else None
    pe_val = pe.dropna().iloc[-1] if not pe.dropna().empty else None
    te_val = te.dropna().iloc[-1] if not te.dropna().empty else None

    return {
        "feature": feature,
        "kinetic_energy": round(float(ke_val), 6) if ke_val is not None else None,
        "potential_energy": round(float(pe_val), 6) if pe_val is not None else None,
        "total_energy": round(float(te_val), 6) if te_val is not None else None,
        "short_window": short_window,
        "long_window": long_window,
    }
