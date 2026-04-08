"""Geo-spatial data endpoints for flow visualization."""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/geo", tags=["geo"])


# Known financial center coordinates (fallback for actors without geocoded locations)
FINANCIAL_CENTERS: dict[str, dict[str, Any]] = {
    "US": {"lat": 40.7128, "lng": -74.0060, "name": "New York"},
    "UK": {"lat": 51.5074, "lng": -0.1278, "name": "London"},
    "JP": {"lat": 35.6762, "lng": 139.6503, "name": "Tokyo"},
    "CN": {"lat": 31.2304, "lng": 121.4737, "name": "Shanghai"},
    "HK": {"lat": 22.3193, "lng": 114.1694, "name": "Hong Kong"},
    "SG": {"lat": 1.3521, "lng": 103.8198, "name": "Singapore"},
    "DE": {"lat": 50.1109, "lng": 8.6821, "name": "Frankfurt"},
    "CH": {"lat": 47.3769, "lng": 8.5417, "name": "Zurich"},
    "AE": {"lat": 25.2048, "lng": 55.2708, "name": "Dubai"},
    "AU": {"lat": -33.8688, "lng": 151.2093, "name": "Sydney"},
    "CA": {"lat": 43.6532, "lng": -79.3832, "name": "Toronto"},
    "BR": {"lat": -23.5505, "lng": -46.6333, "name": "Sao Paulo"},
    "IN": {"lat": 19.0760, "lng": 72.8777, "name": "Mumbai"},
    "KR": {"lat": 37.5665, "lng": 126.9780, "name": "Seoul"},
    "RU": {"lat": 55.7558, "lng": 37.6173, "name": "Moscow"},
    "SA": {"lat": 24.7136, "lng": 46.6753, "name": "Riyadh"},
    "PA": {"lat": 8.9824, "lng": -79.5199, "name": "Panama City"},
    "VG": {"lat": 18.4207, "lng": -64.6400, "name": "British Virgin Islands"},
    "KY": {"lat": 19.3133, "lng": -81.2546, "name": "Cayman Islands"},
    "LU": {"lat": 49.6117, "lng": 6.1300, "name": "Luxembourg"},
    "BM": {"lat": 32.3078, "lng": -64.7505, "name": "Bermuda"},
    "IE": {"lat": 53.3498, "lng": -6.2603, "name": "Dublin"},
    "NL": {"lat": 52.3676, "lng": 4.9041, "name": "Amsterdam"},
}


def _extract_geo(metadata: Any, name: str) -> dict[str, float] | None:
    """Extract geographic coordinates from actor metadata or infer from name/jurisdiction."""
    if metadata:
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (json.JSONDecodeError, TypeError):
                metadata = {}

        if isinstance(metadata, dict):
            # Direct coordinates
            if metadata.get("lat") and metadata.get("lng"):
                try:
                    return {"lat": float(metadata["lat"]), "lng": float(metadata["lng"])}
                except (ValueError, TypeError):
                    pass

            # Jurisdiction/country code
            country = (
                metadata.get("jurisdiction")
                or metadata.get("country")
                or metadata.get("headquarters_country")
            )
            if country and isinstance(country, str):
                code = country.upper()[:2]
                if code in FINANCIAL_CENTERS:
                    fc = FINANCIAL_CENTERS[code]
                    return {"lat": fc["lat"], "lng": fc["lng"]}

    # Infer from common names/patterns
    name_lower = (name or "").lower()
    if any(x in name_lower for x in ["federal reserve", "fed ", "treasury", "sec ", "congress"]):
        fc = FINANCIAL_CENTERS["US"]
        return {"lat": fc["lat"], "lng": fc["lng"]}
    if any(x in name_lower for x in ["bank of england", "boe", "ftse"]):
        fc = FINANCIAL_CENTERS["UK"]
        return {"lat": fc["lat"], "lng": fc["lng"]}
    if any(x in name_lower for x in ["bank of japan", "boj", "nikkei"]):
        fc = FINANCIAL_CENTERS["JP"]
        return {"lat": fc["lat"], "lng": fc["lng"]}
    if any(x in name_lower for x in ["pboc", "china", "shanghai"]):
        fc = FINANCIAL_CENTERS["CN"]
        return {"lat": fc["lat"], "lng": fc["lng"]}
    if any(x in name_lower for x in ["ecb", "european central"]):
        fc = FINANCIAL_CENTERS["DE"]
        return {"lat": fc["lat"], "lng": fc["lng"]}

    return None


@router.get("/flows")
async def get_geo_flows(
    flow_type: str = Query("capital", description="capital|commodity|military"),
    days: int = Query(90, ge=1, le=365),
    min_amount: float = Query(0, ge=0),
    engine=Depends(get_db_engine),
    _=Depends(require_auth),
):
    """Get geo-coded capital flows for map visualization."""
    flows: list[dict[str, Any]] = []

    with engine.connect() as conn:
        # Query wealth_flows for capital flows between actors with known jurisdictions
        rows = conn.execute(
            text("""
                SELECT wf.from_actor, wf.to_entity, wf.amount_estimate, wf.confidence,
                       wf.flow_date, wf.evidence,
                       a1.category AS from_category, a1.metadata AS from_meta,
                       a2.category AS to_category, a2.metadata AS to_meta
                FROM wealth_flows wf
                LEFT JOIN actors a1 ON LOWER(a1.name) = LOWER(wf.from_actor)
                LEFT JOIN actors a2 ON LOWER(a2.name) = LOWER(wf.to_entity)
                WHERE wf.flow_date >= NOW() - MAKE_INTERVAL(days => :days)
                AND (:min_amount = 0 OR wf.amount_estimate >= :min_amount)
                ORDER BY wf.amount_estimate DESC NULLS LAST
                LIMIT 500
            """).bindparams(days=days, min_amount=min_amount),
        ).mappings().all()

        for r in rows:
            from_geo = _extract_geo(r.get("from_meta"), r.get("from_actor", ""))
            to_geo = _extract_geo(r.get("to_meta"), r.get("to_entity", ""))

            if from_geo and to_geo:
                flows.append({
                    "from_lat": from_geo["lat"],
                    "from_lng": from_geo["lng"],
                    "from_name": r.get("from_actor", ""),
                    "to_lat": to_geo["lat"],
                    "to_lng": to_geo["lng"],
                    "to_name": r.get("to_entity", ""),
                    "amount": r.get("amount_estimate"),
                    "confidence": r.get("confidence", "estimated"),
                    "date": str(r.get("flow_date", "")),
                    "type": flow_type,
                })

        # Also query dollar_flows for more coverage
        rows2 = conn.execute(
            text("""
                SELECT source_type, actor_name, ticker, amount_usd, direction,
                       confidence, flow_date
                FROM dollar_flows
                WHERE flow_date >= NOW() - MAKE_INTERVAL(days => :days)
                AND (:min_amount = 0 OR ABS(amount_usd) >= :min_amount)
                ORDER BY ABS(amount_usd) DESC NULLS LAST
                LIMIT 300
            """).bindparams(days=days, min_amount=min_amount),
        ).mappings().all()

        us_default = FINANCIAL_CENTERS["US"]

        for r in rows2:
            from_geo = _extract_geo(None, r.get("actor_name", ""))
            if from_geo:
                flows.append({
                    "from_lat": from_geo["lat"],
                    "from_lng": from_geo["lng"],
                    "from_name": r.get("actor_name", ""),
                    "to_lat": us_default["lat"],
                    "to_lng": us_default["lng"],
                    "to_name": r.get("ticker", "US Markets"),
                    "amount": abs(r.get("amount_usd", 0) or 0),
                    "confidence": r.get("confidence", "estimated"),
                    "date": str(r.get("flow_date", "")),
                    "type": r.get("source_type", flow_type),
                })

    return {"flows": flows, "flow_type": flow_type, "count": len(flows)}


@router.get("/actors")
async def get_geo_actors(
    min_influence: float = Query(0, ge=0, le=1),
    category: str | None = None,
    limit: int = Query(200, ge=1, le=1000),
    engine=Depends(get_db_engine),
    _=Depends(require_auth),
):
    """Get actors with geographic coordinates for map scatter plot."""
    actors: list[dict[str, Any]] = []

    with engine.connect() as conn:
        query = """
            SELECT id, name, category, tier, influence_score, net_worth_estimate, metadata
            FROM actors
            WHERE influence_score >= :min_inf
        """
        params: dict[str, Any] = {"min_inf": min_influence, "lim": limit}

        if category:
            query += " AND category = :cat"
            params["cat"] = category

        query += " ORDER BY influence_score DESC NULLS LAST LIMIT :lim"

        rows = conn.execute(text(query).bindparams(**params)).mappings().all()

        for r in rows:
            geo = _extract_geo(r.get("metadata"), r.get("name", ""))
            if geo:
                actors.append({
                    "id": r["id"],
                    "name": r["name"],
                    "lat": geo["lat"],
                    "lng": geo["lng"],
                    "category": r.get("category"),
                    "tier": r.get("tier"),
                    "influence": r.get("influence_score", 0),
                    "net_worth": r.get("net_worth_estimate"),
                })

    return {"actors": actors, "count": len(actors)}


@router.get("/signals/density")
async def get_signal_density(
    days: int = Query(30, ge=1, le=365),
    engine=Depends(get_db_engine),
    _=Depends(require_auth),
):
    """Get signal density by geographic region for heatmap."""
    density: list[dict[str, Any]] = []

    with engine.connect() as conn:
        # Group signals by actor -> look up actor jurisdiction
        rows = conn.execute(
            text("""
                SELECT sd.actor, COUNT(*) as signal_count,
                       a.metadata, a.category
                FROM signal_data sd
                LEFT JOIN actors a ON LOWER(a.name) = LOWER(sd.actor)
                WHERE sd.signal_date >= NOW() - MAKE_INTERVAL(days => :days)
                AND sd.actor IS NOT NULL
                GROUP BY sd.actor, a.metadata, a.category
                HAVING COUNT(*) >= 2
                ORDER BY signal_count DESC
                LIMIT 500
            """).bindparams(days=days),
        ).mappings().all()

        for r in rows:
            geo = _extract_geo(r.get("metadata"), r.get("actor", ""))
            if geo:
                density.append({
                    "lat": geo["lat"],
                    "lng": geo["lng"],
                    "weight": r["signal_count"],
                    "actor": r.get("actor"),
                    "category": r.get("category"),
                })

    return {"density": density, "count": len(density)}
