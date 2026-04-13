"""Fundamental-vs-price divergence endpoints.

Thin FastAPI wrapper around the ``fundamental_divergence`` table. All
computation lives in ``intelligence.fundamental_divergence`` — this
router only reads pre-computed rows.

    GET /api/v1/divergence/{classification}?limit=20
    GET /api/v1/actors/{id}/divergence
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine
from utils.ttl_cache import TTLCache

router = APIRouter(prefix="/api/v1", tags=["divergence"])

_CACHE: TTLCache = TTLCache(ttl=300.0, max_size=64)

_VALID_CLASSIFICATIONS: frozenset[str] = frozenset(
    {"long_candidate", "short_candidate", "aligned"}
)


def _table_exists(conn: Any, name: str) -> bool:
    try:
        row = conn.execute(
            text("SELECT to_regclass(:n)").bindparams(n=name)
        ).fetchone()
        return bool(row and row[0])
    except Exception:
        return False


def _row_to_dict(row: Any) -> dict[str, Any]:
    return {
        "ticker": row[0],
        "as_of": row[1].isoformat() if row[1] else None,
        "sector": row[2],
        "fundamental_score": float(row[3]) if row[3] is not None else None,
        "price_score": float(row[4]) if row[4] is not None else None,
        "divergence": float(row[5]) if row[5] is not None else None,
        "classification": row[6],
        "narrative": row[7],
    }


@router.get("/divergence/{classification}")
async def list_divergence(
    classification: str,
    limit: int = Query(20, ge=1, le=200),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the top-N rows of one classification, newest snapshot,
    ranked by absolute divergence (largest first)."""
    if classification not in _VALID_CLASSIFICATIONS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid classification {classification!r}. Must be one of: "
                f"{sorted(_VALID_CLASSIFICATIONS)}"
            ),
        )

    cache_key = f"list|{classification}|{limit}"
    cached = _CACHE.get(cache_key)
    if cached is not None:
        return cached

    engine = get_db_engine()
    try:
        with engine.connect() as conn:
            if not _table_exists(conn, "public.fundamental_divergence"):
                payload = {
                    "classification": classification,
                    "rows": [],
                    "count": 0,
                    "provenance": {
                        "source": "fallback",
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                        "reason": "table_missing",
                    },
                }
                _CACHE.set(cache_key, payload)
                return payload

            rows = conn.execute(
                text(
                    """
                    SELECT ticker, as_of, sector, fundamental_score,
                           price_score, divergence, classification, narrative
                    FROM fundamental_divergence
                    WHERE classification = :c
                      AND as_of = (
                        SELECT MAX(as_of) FROM fundamental_divergence
                      )
                    ORDER BY ABS(divergence) DESC
                    LIMIT :lim
                    """
                ).bindparams(c=classification, lim=limit)
            ).fetchall()
    except Exception as exc:
        log.warning(
            "divergence list endpoint failed for {c}: {e}",
            c=classification, e=str(exc),
        )
        raise HTTPException(
            status_code=500, detail="Failed to fetch divergence rows",
        ) from exc

    items = [_row_to_dict(r) for r in rows]
    payload: dict[str, Any] = {
        "classification": classification,
        "rows": items,
        "count": len(items),
        "provenance": {
            "source": "db",
            "generated_at": datetime.now(timezone.utc).isoformat(),
        },
    }
    _CACHE.set(cache_key, payload)
    return payload


@router.get("/actors/{actor_id}/divergence")
async def get_actor_divergence(
    actor_id: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the latest divergence row for one ticker/actor."""
    cache_key = f"actor|{actor_id}"
    cached = _CACHE.get(cache_key)
    if cached is not None:
        return cached

    # Case-insensitive lookup — seed stores lowercase, map uses upper.
    variants = list({actor_id, actor_id.upper(), actor_id.lower()})

    engine = get_db_engine()
    try:
        with engine.connect() as conn:
            if not _table_exists(conn, "public.fundamental_divergence"):
                raise HTTPException(
                    status_code=404,
                    detail="fundamental_divergence table not available",
                )
            row = conn.execute(
                text(
                    """
                    SELECT ticker, as_of, sector, fundamental_score,
                           price_score, divergence, classification, narrative
                    FROM fundamental_divergence
                    WHERE ticker = ANY(:ids)
                    ORDER BY as_of DESC
                    LIMIT 1
                    """
                ).bindparams(ids=variants)
            ).fetchone()
    except HTTPException:
        raise
    except Exception as exc:
        log.warning(
            "divergence actor endpoint failed for {a}: {e}",
            a=actor_id, e=str(exc),
        )
        raise HTTPException(
            status_code=500, detail="Failed to fetch divergence row",
        ) from exc

    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"No divergence row found for {actor_id!r}",
        )

    payload: dict[str, Any] = {
        "actor": _row_to_dict(row),
        "provenance": {
            "source": "db",
            "generated_at": datetime.now(timezone.utc).isoformat(),
        },
    }
    _CACHE.set(cache_key, payload)
    return payload
