"""
GRID API — Market physics endpoints.

Provides REST API access to physics verification, conventions, and transforms:
  GET  /api/v1/physics/verify           — Run full verification suite
  GET  /api/v1/physics/conventions      — List all financial conventions
  GET  /api/v1/physics/conventions/{domain} — Get convention for a domain
  GET  /api/v1/physics/ou/{feature}     — Estimate OU parameters for a feature
  GET  /api/v1/physics/hurst/{feature}  — Compute Hurst exponent for a feature
  GET  /api/v1/physics/energy/{feature} — Compute energy decomposition for a feature
  GET  /api/v1/physics/news-energy      — News energy decomposition from Crucix/GDELT
  GET  /api/v1/physics/dashboard        — Comprehensive physics dashboard
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

    as_of_date = date.fromisoformat(as_of) if as_of else date.today()

    engine = get_engine()
    pit = PITStore(engine)
    verifier = MarketPhysicsVerifier(engine, pit)

    results = verifier.verify_all(as_of_date)
    return results


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


@router.get("/news-energy")
async def news_energy(
    lookback_days: int = Query(default=30, ge=7, le=365),
    as_of: str | None = Query(default=None),
) -> dict[str, Any]:
    """Compute news energy decomposition from Crucix/GDELT data streams.

    Decomposes all available news sources into kinetic energy (rate of
    change), potential energy (deviation from baseline), and total energy.
    Detects regime shifts via energy conservation violations and builds
    a force vector showing which sources are injecting the most energy.

    Parameters:
        lookback_days: Days of history to analyze (default 30).
        as_of: Decision date in ISO format (default today).
    """
    from db import get_engine
    from physics.news_energy import NewsEnergyEngine
    from store.pit import PITStore

    as_of_date = date.fromisoformat(as_of) if as_of else date.today()

    engine = get_engine()
    pit = PITStore(engine)
    nee = NewsEnergyEngine(engine, pit)

    result = nee.analyze(lookback_days=lookback_days, as_of_date=as_of_date)
    return result


@router.get("/dashboard")
async def physics_dashboard(
    as_of: str | None = Query(default=None),
) -> dict[str, Any]:
    """Comprehensive physics dashboard for the frontend.

    Returns in a single call:
    1. Market energy state (KE, PE, total) for key assets
    2. News energy decomposition from Crucix/GDELT sources
    3. Hurst exponents for key features
    4. OU mean-reversion estimates for key features
    5. Energy conservation check (equilibrium vs transitioning)
    6. Plain-English summary of current conditions

    Parameters:
        as_of: Decision date in ISO format (default today).
    """
    from db import get_engine
    from features.lab import FeatureLab
    from physics.news_energy import NewsEnergyEngine
    from physics.transforms import (
        estimate_ou_parameters,
        hurst_exponent,
        kinetic_energy,
        potential_energy,
        total_energy,
    )
    from store.pit import PITStore

    as_of_date = date.fromisoformat(as_of) if as_of else date.today()

    engine = get_engine()
    pit = PITStore(engine)
    lab = FeatureLab(engine, pit)

    # Key market features to profile
    key_features = ["sp500", "vix", "us_treasury_10y", "us_treasury_2y", "dxy_index"]

    # 1. Market energy state
    market_energy: dict[str, Any] = {}
    for feat_name in key_features:
        series = lab._get_pit_series(feat_name, as_of_date, lookback_days=600)
        if series is None or len(series) < 30:
            market_energy[feat_name] = {
                "kinetic_energy": None,
                "potential_energy": None,
                "total_energy": None,
                "status": "insufficient_data",
            }
            continue

        ke = kinetic_energy(series, window=21)
        pe = potential_energy(series, window=252)

        ke_val = float(ke.dropna().iloc[-1]) if not ke.dropna().empty else None
        pe_val = float(pe.dropna().iloc[-1]) if not pe.dropna().empty else None
        te_val = (ke_val or 0) + (pe_val or 0) if ke_val is not None or pe_val is not None else None

        market_energy[feat_name] = {
            "kinetic_energy": round(ke_val, 6) if ke_val is not None else None,
            "potential_energy": round(pe_val, 6) if pe_val is not None else None,
            "total_energy": round(te_val, 6) if te_val is not None else None,
            "status": "ok",
        }

    # 2. News energy decomposition
    nee = NewsEnergyEngine(engine, pit)
    news_result = nee.analyze(lookback_days=30, as_of_date=as_of_date)

    # 3. Hurst exponents
    hurst_results: dict[str, Any] = {}
    for feat_name in key_features:
        series = lab._get_pit_series(feat_name, as_of_date, lookback_days=600)
        if series is None or len(series) < 50:
            hurst_results[feat_name] = {"hurst": None, "interpretation": "insufficient_data"}
            continue

        h = hurst_exponent(series, max_lag=100)
        if h != h:  # NaN check
            hurst_results[feat_name] = {"hurst": None, "interpretation": "computation_failed"}
        else:
            interp = "random walk"
            if h < 0.45:
                interp = "mean-reverting"
            elif h > 0.55:
                interp = "trending"
            hurst_results[feat_name] = {
                "hurst": round(float(h), 4),
                "interpretation": interp,
            }

    # 4. OU mean-reversion estimates
    ou_results: dict[str, Any] = {}
    for feat_name in key_features:
        series = lab._get_pit_series(feat_name, as_of_date, lookback_days=600)
        if series is None or len(series) < 50:
            ou_results[feat_name] = {"theta": None, "mu": None, "half_life_days": None}
            continue

        params = estimate_ou_parameters(series)
        ou_results[feat_name] = {
            "theta": params.get("theta"),
            "mu": params.get("mu"),
            "sigma": params.get("sigma"),
            "half_life_days": params.get("half_life_days"),
            "mean_reverting": params.get("mean_reverting", False),
        }

    # 5. Energy conservation check
    total_market_e = sum(
        v["total_energy"] for v in market_energy.values()
        if v.get("total_energy") is not None
    )
    news_regime = news_result.get("regime_signal", {})
    conservation = {
        "total_market_energy": round(total_market_e, 6),
        "total_news_energy": news_result.get("total_news_energy", 0.0),
        "regime_signal": news_regime,
    }

    # Determine equilibrium state from regime violations
    n_violations = news_regime.get("violations", 0)
    is_equilibrium = news_regime.get("equilibrium", True)
    if n_violations > 2:
        eq_state = "transitioning"
    elif not is_equilibrium:
        eq_state = "stressed"
    else:
        eq_state = "equilibrium"
    conservation["state"] = eq_state

    # 6. Plain-English summary
    summary_parts = []

    # Market energy summary
    high_energy_assets = [
        k for k, v in market_energy.items()
        if v.get("total_energy") is not None and v["total_energy"] > 0.5
    ]
    if high_energy_assets:
        summary_parts.append(
            f"High energy detected in {', '.join(high_energy_assets)} — "
            "these assets are moving significantly or far from equilibrium."
        )
    else:
        summary_parts.append("Market energy is within normal bounds across key assets.")

    # Trending/reverting summary
    trending = [k for k, v in hurst_results.items() if v.get("interpretation") == "trending"]
    reverting = [k for k, v in hurst_results.items() if v.get("interpretation") == "mean-reverting"]
    if trending:
        summary_parts.append(f"Trending behavior in: {', '.join(trending)}.")
    if reverting:
        summary_parts.append(f"Mean-reverting behavior in: {', '.join(reverting)}.")

    # News energy summary
    n_news_sources = news_result.get("n_news_sources", 0)
    if n_news_sources > 0:
        summary_parts.append(news_result.get("summary", ""))
    else:
        summary_parts.append("No news data available for energy analysis.")

    # Equilibrium state
    summary_parts.append(f"Overall market state: {eq_state}.")

    return {
        "as_of_date": as_of_date.isoformat(),
        "market_energy": market_energy,
        "news_energy": {
            "n_sources": n_news_sources,
            "total_news_energy": news_result.get("total_news_energy", 0.0),
            "coherence": news_result.get("coherence", {}),
            "force_vector": news_result.get("force_vector", []),
            "energy_by_source": news_result.get("energy_by_source", []),
            "regime_signal": news_regime,
        },
        "hurst_exponents": hurst_results,
        "ou_parameters": ou_results,
        "energy_conservation": conservation,
        "summary": " ".join(summary_parts),
    }
