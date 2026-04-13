"""Actor news endpoint — serves rows from the actor_news table.

Consumed by the ActorProfileDrawer "External" section to show recent
news headlines that reference a given actor. Graceful fallback when
the table does not exist yet (the intel pipeline builds it).

Endpoint:
    GET /api/v1/actors/{actor_id}/news?limit=20
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/actors", tags=["actors"])


def _table_exists(conn: Any, table_name: str) -> bool:
    try:
        row = conn.execute(
            text("SELECT to_regclass(:n)").bindparams(n=table_name)
        ).fetchone()
        return bool(row and row[0])
    except Exception:
        return False


def _column_set(conn: Any, table_name: str) -> set[str]:
    """Return the set of column names for a table, empty on failure."""
    try:
        # table_name may be bare or schema-qualified; normalize.
        if "." in table_name:
            schema, tname = table_name.split(".", 1)
        else:
            schema, tname = "public", table_name
        rows = conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = :s AND table_name = :t
                """
            ).bindparams(s=schema, t=tname)
        ).fetchall()
        return {r[0] for r in rows}
    except Exception:
        return set()


@router.get("/{actor_id}/news")
async def get_actor_news(
    actor_id: str,
    limit: int = Query(20, ge=1, le=200),
    _token: str = Depends(require_auth),
) -> dict:
    """Return recent rows from actor_news table for this actor.

    Falls back to an empty list if the table does not yet exist. The
    table is built by the Intel-1 pipeline; this endpoint will light up
    automatically once rows land.
    """
    engine = get_db_engine()
    actor_id_clean = (actor_id or "").strip()
    if not actor_id_clean:
        return {"actor_id": actor_id, "count": 0, "items": [], "available": False}

    try:
        with engine.connect() as conn:
            if not _table_exists(conn, "actor_news"):
                return {
                    "actor_id": actor_id_clean,
                    "count": 0,
                    "items": [],
                    "available": False,
                    "reason": "actor_news table not yet populated",
                }

            cols = _column_set(conn, "actor_news")
            # Map flexible column names to a canonical payload. The
            # sibling Intel-1 agent owns the schema, so we accept several.
            id_col = next((c for c in ("actor_id", "actor", "entity_id", "entity") if c in cols), None)
            title_col = next((c for c in ("title", "headline", "name") if c in cols), None)
            url_col = next((c for c in ("url", "link", "source_url") if c in cols), None)
            source_col = next((c for c in ("source", "publisher", "outlet") if c in cols), None)
            date_col = next(
                (c for c in ("published_at", "pub_date", "published", "created_at", "seen_at")
                 if c in cols),
                None,
            )
            summary_col = next((c for c in ("summary", "description", "snippet") if c in cols), None)

            if not id_col or not title_col:
                return {
                    "actor_id": actor_id_clean,
                    "count": 0,
                    "items": [],
                    "available": False,
                    "reason": "actor_news missing required columns",
                }

            select_cols = [f"{title_col} AS title"]
            if url_col:
                select_cols.append(f"{url_col} AS url")
            else:
                select_cols.append("NULL AS url")
            if source_col:
                select_cols.append(f"{source_col} AS source")
            else:
                select_cols.append("NULL AS source")
            if date_col:
                select_cols.append(f"{date_col} AS published_at")
            else:
                select_cols.append("NULL AS published_at")
            if summary_col:
                select_cols.append(f"{summary_col} AS summary")
            else:
                select_cols.append("NULL AS summary")

            order_clause = f"ORDER BY {date_col} DESC NULLS LAST" if date_col else ""

            sql = (
                f"SELECT {', '.join(select_cols)} FROM actor_news "
                f"WHERE {id_col} = :aid {order_clause} LIMIT :lim"
            )
            rows = conn.execute(
                text(sql).bindparams(aid=actor_id_clean, lim=int(limit))
            ).fetchall()

            items = []
            for r in rows:
                m = r._mapping if hasattr(r, "_mapping") else dict(r)
                published = m.get("published_at")
                items.append(
                    {
                        "title": m.get("title"),
                        "url": m.get("url"),
                        "source": m.get("source"),
                        "published_at": (
                            published.isoformat() if hasattr(published, "isoformat") else published
                        ),
                        "summary": m.get("summary"),
                    }
                )

            return {
                "actor_id": actor_id_clean,
                "count": len(items),
                "items": items,
                "available": True,
            }
    except Exception as exc:
        log.warning("get_actor_news failed for {a}: {e}", a=actor_id_clean, e=str(exc))
        return {
            "actor_id": actor_id_clean,
            "count": 0,
            "items": [],
            "available": False,
            "reason": str(exc),
        }
