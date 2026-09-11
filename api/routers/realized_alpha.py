"""Realized-alpha truth gate endpoint (GRID-4 pivot §8.1).

Thin FastAPI wrapper around ``alpha_research.realized_alpha``. All the
computation lives in the domain module — this router only validates the
query parameters and serves the persisted daily rows.

    GET /api/v1/alpha/realized?source=&horizon_days=&days=&limit=&offset=
      → { entries, total, limit, offset, has_more }
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/alpha", tags=["realized_alpha"])


@router.get("/realized")
async def get_realized_alpha(
    source: str | None = Query(
        default=None, pattern=r"^(paper_trades|oracle_predictions)$",
    ),
    horizon_days: int | None = Query(default=None, ge=1, le=3650),
    days: int = Query(default=90, ge=1, le=3650),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Paginated rolling realized alpha vs SPY, net of costs, per horizon."""
    try:
        from alpha_research.realized_alpha import fetch_realized_alpha

        return fetch_realized_alpha(
            get_db_engine(),
            source=source,
            horizon_days=horizon_days,
            days=days,
            limit=limit,
            offset=offset,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        log.warning("realized_alpha endpoint failed: {e}", e=str(exc))
        raise HTTPException(
            status_code=500, detail="Failed to read realized alpha",
        ) from exc
