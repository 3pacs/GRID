"""Supply chain graph endpoint for actor profile drawer.

Returns upstream + downstream dependency graph for a given actor.
Backed by supply_chain_nodes/edges tables; falls back to flows.py
_SUPPLY_CHAIN + sector_map subsector overlap when DB is empty.
No LLM calls — narrative is a template string.

All non-router logic lives in ``supply_chain_helpers`` so this module
stays under the 200-line target (see feedback_test_before_ship + the
450-line soft cap). Keep this file as a thin FastAPI shell.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine
from api.routers.supply_chain_helpers import (
    _assemble_db_result,
    _bfs,
    _fallback_graph,
    _resolve_actor,
    _resolve_labels,
    _table_exists,
)
from utils.ttl_cache import TTLCache

router = APIRouter(prefix="/api/v1/actors", tags=["supply_chain"])

_supply_chain_cache: TTLCache = TTLCache(ttl=600.0, max_size=256)


@router.get("/{actor_id}/supply_chain")
async def get_supply_chain(
    actor_id: str,
    direction: str = Query("both", pattern="^(upstream|downstream|both)$"),
    depth: int = Query(2, ge=1, le=5),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Upstream + downstream supply graph. Fallback when DB empty."""
    # Strip canvas graph node-id prefixes (e.g. "a:corp_KO" → "KO").
    _CANVAS_PREFIXES = ("a:corp_", "a:ticker_", "a:person_", "a:govt_", "a:org_", "a:fund_", "a:")
    for _pfx in _CANVAS_PREFIXES:
        if actor_id.startswith(_pfx):
            actor_id = actor_id[len(_pfx):]
            break
    if not actor_id:
        raise HTTPException(status_code=400, detail="actor_id required")

    cache_key = f"{actor_id}|{direction}|{depth}"
    cached = _supply_chain_cache.get(cache_key)
    if cached is not None:
        log.debug("supply_chain cache hit {k}", k=cache_key)
        return cached
    log.debug("supply_chain cache miss {k}", k=cache_key)

    engine = get_db_engine()
    resolved = _resolve_actor(engine, actor_id)
    if resolved is None:
        raise HTTPException(status_code=404, detail=f"actor '{actor_id}' not found")
    actor_meta, seed_id = resolved

    try:
        with engine.connect() as conn:
            if not _table_exists(conn, "supply_chain_edges"):
                log.debug("supply_chain_edges missing, fallback for {a}", a=actor_id)
                result = _fallback_graph(actor_id, direction, depth)
                _supply_chain_cache.set(cache_key, result)
                return result

            up_edges, up_tier = ([], {})
            down_edges, down_tier = ([], {})
            if direction in ("upstream", "both"):
                up_edges, up_tier = _bfs(conn, seed_id, depth, upstream=True)
            if direction in ("downstream", "both"):
                down_edges, down_tier = _bfs(conn, seed_id, depth, upstream=False)

            all_edges = up_edges + down_edges
            if not all_edges:
                log.debug("supply_chain: zero rows for {a}, fallback", a=seed_id)
                result = _fallback_graph(actor_id, direction, depth)
                _supply_chain_cache.set(cache_key, result)
                return result

            label_map = _resolve_labels(
                conn,
                list(set(list(up_tier.keys()) + list(down_tier.keys()))),
            )
            result = _assemble_db_result(
                actor_meta, seed_id, direction, depth,
                up_edges, down_edges, up_tier, down_tier, label_map,
            )
            _supply_chain_cache.set(cache_key, result)
            return result
    except HTTPException:
        raise
    except Exception as exc:
        log.warning("supply_chain: failure for {a}: {e}", a=actor_id, e=str(exc))
        result = _fallback_graph(actor_id, direction, depth)
        _supply_chain_cache.set(cache_key, result)
        return result
