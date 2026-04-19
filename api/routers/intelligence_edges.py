"""Intelligence sub-router: structural market-edge scanner."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine
from intelligence.market_edge_scanner import build_market_edge_snapshot

router = APIRouter(tags=["intelligence"])


@router.get("/edges")
async def get_market_edges(
    limit: int = Query(10, ge=1, le=25, description="Maximum number of ranked levers to return"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return ranked structural mispricing levers from public clue chains."""
    try:
        engine = get_db_engine()
        return build_market_edge_snapshot(engine, limit=limit)
    except Exception as exc:
        log.warning("Market-edge scanner failed: {e}", e=str(exc))
        fallback = build_market_edge_snapshot(None, limit=limit)
        fallback["error"] = str(exc)
        return fallback
