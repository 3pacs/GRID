"""Sector health endpoint.

Thin FastAPI wrapper around ``intelligence.sector_health``. All the
computation lives in the domain module — this router just caches and
serves the result.

    GET /api/v1/sectors/{sector_name}/health
      → { sector, score, trend_30d, components, narrative, as_of }
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine
from utils.ttl_cache import TTLCache

router = APIRouter(prefix="/api/v1/sectors", tags=["sector_health"])

_CACHE: TTLCache = TTLCache(ttl=600.0, max_size=64)


@router.get("/{sector_name}/health")
async def get_sector_health(
    sector_name: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the sector health composite score + components."""
    cached = _CACHE.get(sector_name)
    if cached is not None:
        return cached

    from analysis.sector_map import SECTOR_MAP
    if sector_name not in SECTOR_MAP:
        raise HTTPException(
            status_code=404, detail=f"Sector '{sector_name}' not found",
        )

    try:
        from intelligence.sector_health import compute_sector_health
        engine = get_db_engine()
        result = compute_sector_health(engine, sector_name)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        log.warning(
            "sector_health endpoint failed for {s}: {e}",
            s=sector_name, e=str(exc),
        )
        raise HTTPException(
            status_code=500, detail="Failed to compute sector health",
        ) from exc

    _CACHE.set(sector_name, result)
    return result
