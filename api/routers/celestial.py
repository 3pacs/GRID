"""Celestial signals endpoint.

Serves celestial/astro feature data (lunar phase, solar activity,
vedic nakshatra, planetary aspects, chinese zodiac) from the PIT store.
Gracefully returns empty data if no celestial features are registered.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine, get_pit_store

router = APIRouter(prefix="/api/v1/signals/celestial", tags=["signals", "celestial"])

# Feature name patterns that indicate celestial data.
# Maps pattern prefix -> display category for the frontend.
CELESTIAL_PATTERNS: dict[str, str] = {
    "lunar": "lunar",
    "moon": "lunar",
    "solar_kp": "solar",
    "solar_flux": "solar",
    "sunspot": "solar",
    "vedic": "vedic",
    "nakshatra": "vedic",
    "planetary": "planetary",
    "planet": "planetary",
    "aspect": "planetary",
    "chinese_zodiac": "chinese",
    "chinese_year": "chinese",
    "celestial": "celestial",
}


def _categorize_feature(name: str) -> str | None:
    """Return the celestial category for a feature name, or None."""
    name_lower = name.lower()
    for pattern, category in CELESTIAL_PATTERNS.items():
        if pattern in name_lower:
            return category
    return None


@router.get("")
async def get_celestial_signals(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return current celestial feature values from the PIT store.

    Queries feature_registry for features whose names match celestial
    patterns, then fetches the latest PIT-correct values for each.
    """
    engine = get_db_engine()

    try:
        # Find celestial features in the registry by name pattern.
        # Build a LIKE-based query with parameterized patterns.
        patterns = list(CELESTIAL_PATTERNS.keys())
        like_clauses = " OR ".join(
            f"LOWER(name) LIKE :p{i}" for i in range(len(patterns))
        )
        params = {f"p{i}": f"%{p}%" for i, p in enumerate(patterns)}

        query = text(
            f"SELECT id, name, description FROM feature_registry "
            f"WHERE {like_clauses} "
            f"ORDER BY name"
        ).bindparams(**params)

        with engine.connect() as conn:
            rows = conn.execute(query).fetchall()

        if not rows:
            log.debug("No celestial features found in feature_registry")
            return {
                "features": [],
                "categories": _empty_categories(),
                "as_of": str(date.today()),
                "count": 0,
            }

        feature_ids = [r[0] for r in rows]
        feature_map = {r[0]: {"name": r[1], "description": r[2]} for r in rows}

        # Fetch latest PIT-correct values
        pit = get_pit_store()
        latest_df = pit.get_latest_values(feature_ids)

        features = []
        categories: dict[str, list[dict]] = {}

        for _, row in latest_df.iterrows():
            fid = row["feature_id"]
            meta = feature_map.get(fid, {})
            name = meta.get("name", f"feature_{fid}")
            category = _categorize_feature(name) or "celestial"
            value = row["value"]
            obs_date = str(row["obs_date"]) if row.get("obs_date") is not None else None

            entry = {
                "feature_id": int(fid),
                "name": name,
                "description": meta.get("description"),
                "category": category,
                "value": float(value) if value is not None else None,
                "obs_date": obs_date,
            }
            features.append(entry)
            categories.setdefault(category, []).append(entry)

        return {
            "features": features,
            "categories": categories,
            "as_of": str(date.today()),
            "count": len(features),
        }

    except Exception as exc:
        log.warning("Celestial signals query failed: {e}", e=str(exc))
        return {
            "features": [],
            "categories": _empty_categories(),
            "as_of": str(date.today()),
            "count": 0,
            "error": str(exc),
        }


def _empty_categories() -> dict[str, list]:
    """Return empty category map for consistent frontend shape."""
    return {
        "lunar": [],
        "solar": [],
        "vedic": [],
        "planetary": [],
        "chinese": [],
    }
