"""Full-text intelligence search across the entire GRID corpus.

Searches actors, signals, hypotheses, and analytical snapshots using
PostgreSQL full-text search (tsvector/tsquery) via the `intelligence_search`
materialized view.

Endpoints:
  GET  /search/intelligence   — ranked search with snippets
  POST /search/intelligence/refresh — refresh the materialized view
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/search", tags=["intelligence-search"])

# Valid source types that can appear in the materialized view
_VALID_TYPES = frozenset({"actor", "signal", "hypothesis", "snapshot"})


@router.get("/intelligence")
def search_intelligence(
    q: str = Query(..., min_length=1, max_length=500, description="Search query"),
    types: str | None = Query(
        default=None,
        description="Comma-separated source types to search: actor,signal,hypothesis,snapshot",
    ),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    _user: dict = Depends(require_auth),
) -> dict:
    """Full-text search across the intelligence corpus.

    Uses PostgreSQL ts_rank for relevance scoring and ts_headline for
    context-aware snippet generation.  Results are drawn from the
    `intelligence_search` materialized view (actors + signals +
    hypotheses + snapshots).
    """
    log.info("Intelligence FTS: q={q!r} types={t} limit={l} offset={o}",
             q=q, t=types, l=limit, o=offset)

    # Parse and validate type filters
    type_filter: list[str] | None = None
    if types:
        requested = [t.strip().lower() for t in types.split(",") if t.strip()]
        invalid = [t for t in requested if t not in _VALID_TYPES]
        if invalid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid source types: {', '.join(invalid)}. Valid: {', '.join(sorted(_VALID_TYPES))}",
            )
        type_filter = requested if requested else None

    engine = get_db_engine()

    try:
        with engine.connect() as conn:
            # Build the query dynamically based on type filter
            type_clause = ""
            params: dict = {"query": q, "limit": limit, "offset": offset}

            if type_filter:
                type_clause = "AND source_type = ANY(:types)"
                params["types"] = type_filter

            # Count total matches
            count_sql = text(f"""
                SELECT COUNT(*)
                FROM intelligence_search
                WHERE tsv @@ plainto_tsquery('english', :query)
                {type_clause}
            """)

            total_row = conn.execute(count_sql, params).fetchone()
            total = total_row[0] if total_row else 0

            if total == 0:
                return {"results": [], "total": 0, "query": q}

            # Fetch ranked results with snippets
            search_sql = text(f"""
                SELECT
                    source_type,
                    source_id,
                    title,
                    ts_headline(
                        'english', body,
                        plainto_tsquery('english', :query),
                        'MaxWords=35, MinWords=15, StartSel=<mark>, StopSel=</mark>'
                    ) AS snippet,
                    ts_rank(tsv, plainto_tsquery('english', :query)) AS relevance
                FROM intelligence_search
                WHERE tsv @@ plainto_tsquery('english', :query)
                {type_clause}
                ORDER BY relevance DESC, source_type, source_id
                LIMIT :limit OFFSET :offset
            """)

            rows = conn.execute(search_sql, params).fetchall()

            results = [
                {
                    "source_type": row.source_type,
                    "source_id": row.source_id,
                    "title": row.title,
                    "snippet": row.snippet,
                    "relevance": round(float(row.relevance), 6),
                }
                for row in rows
            ]

    except Exception as exc:
        error_msg = str(exc)
        if "intelligence_search" in error_msg and ("does not exist" in error_msg or "not exist" in error_msg):
            log.warning(
                "intelligence_search materialized view not found — run migration or POST /refresh"
            )
            return {"results": [], "total": 0, "query": q, "error": "Materialized view not yet created. Run the migration or POST /api/v1/search/intelligence/refresh."}
        log.error("Intelligence FTS failed: {e}", e=error_msg)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Intelligence search failed",
        )

    return {"results": results, "total": total, "query": q}


@router.post("/intelligence/refresh")
def refresh_intelligence_search(
    _user: dict = Depends(require_auth),
) -> dict:
    """Refresh the intelligence_search materialized view.

    Uses CONCURRENTLY so reads are not blocked during refresh.
    The unique index on (source_type, source_id) is required for
    concurrent refresh.
    """
    log.info("Refreshing intelligence_search materialized view")
    engine = get_db_engine()

    try:
        with engine.begin() as conn:
            conn.execute(text(
                "REFRESH MATERIALIZED VIEW CONCURRENTLY intelligence_search"
            ))
    except Exception as exc:
        error_msg = str(exc)
        if "does not exist" in error_msg:
            log.warning("intelligence_search view does not exist — cannot refresh")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Materialized view intelligence_search does not exist. Run the Alembic migration first.",
            )
        log.error("Failed to refresh intelligence_search: {e}", e=error_msg)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to refresh materialized view",
        )

    log.info("intelligence_search materialized view refreshed successfully")
    return {"status": "refreshed", "message": "intelligence_search materialized view refreshed"}
