"""Cross-lens supply-shock attribution endpoint.

Returns persisted rows from ``supply_shock_attributions`` for a given actor,
with a template-narrative summary. No LLM calls.

The attribution rows themselves are written by the ``run_cross_lens`` batch
job (see ``scripts/run_cross_lens.py``) — this endpoint is read-only.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine
from intelligence.cross_lens import (
    DEFAULT_LOOKBACK_DAYS,
    get_attributions_for_actor,
)

router = APIRouter(prefix="/api/v1/actors", tags=["attributions"])


@router.get("/{actor_id}/attributions")
async def get_actor_attributions(
    actor_id: str,
    lookback_days: int = Query(DEFAULT_LOOKBACK_DAYS, ge=1, le=3650),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return cross-lens supply-shock attributions for the actor.

    The actor can appear as either the upstream (shock source) or downstream
    (affected ticker). Rows are ordered by absolute correlation magnitude
    then by shock date descending.
    """
    if not actor_id:
        raise HTTPException(status_code=400, detail="actor_id required")
    engine = get_db_engine()
    try:
        return get_attributions_for_actor(
            engine, actor_id=actor_id, lookback_days=lookback_days
        )
    except Exception as exc:
        log.warning(
            "attributions: failure for {a}: {e}", a=actor_id, e=str(exc)
        )
        raise HTTPException(
            status_code=500, detail="attributions lookup failed"
        )
