"""
Vault API router — Obsidian Bridge CRUD, FTS search, dashboard, and sync trigger.

Endpoints:
  GET    /api/v1/vault/notes                     — paginated list with domain/status filters
  GET    /api/v1/vault/notes/{note_id}           — single note by id
  POST   /api/v1/vault/notes                     — create note (sets pending_write flag)
  PATCH  /api/v1/vault/notes/{note_id}/status    — change status (sets pending_write flag)
  GET    /api/v1/vault/search                    — FTS using body_tsvector
  GET    /api/v1/vault/actions                   — audit log (obsidian_actions)
  GET    /api/v1/vault/dashboard                 — review items + aggregate stats
  POST   /api/v1/vault/sync                      — trigger manual sync run
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(
    prefix="/api/v1/vault",
    tags=["vault"],
    dependencies=[Depends(require_auth)],
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOTE_COLS = (
    "id, vault_path, domain, status, title, content_hash, "
    "frontmatter, body, agent_flags, synced_at, modified_at, created_at"
)


def _row_to_dict(row: Any) -> dict:
    """Serialize a DB row to a plain dict, converting timestamps to ISO strings."""
    d = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
    for key in ("synced_at", "modified_at", "created_at"):
        if d.get(key) is not None:
            d[key] = str(d[key])
    return d


def _action_row_to_dict(row: Any) -> dict:
    d = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
    if d.get("created_at") is not None:
        d["created_at"] = str(d["created_at"])
    return d


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# GET /notes — paginated list with optional domain/status filters
# ---------------------------------------------------------------------------

@router.get("/notes")
async def list_notes(
    domain: str | None = Query(default=None, description="Filter by domain"),
    status_filter: str | None = Query(default=None, alias="status", description="Filter by status"),
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """Return paginated vault notes with optional domain and status filters."""
    engine = get_db_engine()

    conditions: list[str] = []
    params: dict[str, Any] = {"limit": limit, "offset": offset}

    if domain:
        conditions.append("domain = :domain")
        params["domain"] = domain
    if status_filter:
        conditions.append("status = :status")
        params["status"] = status_filter

    where = " WHERE " + " AND ".join(conditions) if conditions else ""

    query = f"SELECT {_NOTE_COLS} FROM obsidian_notes{where} ORDER BY modified_at DESC LIMIT :limit OFFSET :offset"
    count_query = f"SELECT COUNT(*) FROM obsidian_notes{where}"

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()
        total = conn.execute(text(count_query), {k: v for k, v in params.items() if k not in ("limit", "offset")}).fetchone()[0]

    notes = [_row_to_dict(r) for r in rows]
    return {
        "notes": notes,
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": (offset + limit) < total,
    }


# ---------------------------------------------------------------------------
# GET /notes/{note_id} — single note
# ---------------------------------------------------------------------------

@router.get("/notes/{note_id}")
async def get_note(note_id: int) -> dict:
    """Return a single vault note by id."""
    engine = get_db_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(f"SELECT {_NOTE_COLS} FROM obsidian_notes WHERE id = :id"),
            {"id": note_id},
        ).fetchone()

    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Note not found")

    return _row_to_dict(row)


# ---------------------------------------------------------------------------
# POST /notes — create note (sets pending_write flag)
# ---------------------------------------------------------------------------

@router.post("/notes", status_code=status.HTTP_201_CREATED)
async def create_note(payload: dict) -> dict:
    """Create a new vault note. Sets pending_write agent flag so sync writes it to disk."""
    vault_path: str | None = payload.get("vault_path")
    if not vault_path:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="vault_path is required")

    title = payload.get("title", "")
    body = payload.get("body", "")
    domain = payload.get("domain", "grid")
    note_status = payload.get("status", "inbox")
    frontmatter = payload.get("frontmatter", {})

    # Seed agent_flags with pending_write so next sync cycle pushes to disk
    agent_flags = {"pending_write": True}

    import hashlib
    content_hash = hashlib.sha256((title + body).encode()).hexdigest()
    now = _now_utc()

    engine = get_db_engine()
    try:
        with engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    INSERT INTO obsidian_notes
                        (vault_path, domain, status, title, content_hash, frontmatter, body, agent_flags, synced_at, modified_at, created_at)
                    VALUES
                        (:vault_path, :domain, :status, :title, :content_hash, :frontmatter, :body, :agent_flags, :now, :now, :now)
                    RETURNING id
                    """
                ),
                {
                    "vault_path": vault_path,
                    "domain": domain,
                    "status": note_status,
                    "title": title,
                    "content_hash": content_hash,
                    "frontmatter": json.dumps(frontmatter),
                    "body": body,
                    "agent_flags": json.dumps(agent_flags),
                    "now": now,
                },
            ).fetchone()
            note_id = row[0]

        log.info("vault: created note id={id} path={path}", id=note_id, path=vault_path)
        return {"id": note_id, "vault_path": vault_path, "status": note_status, "created": True}
    except Exception as exc:
        log.error("vault: create_note failed: {e}", e=str(exc))
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


# ---------------------------------------------------------------------------
# PATCH /notes/{note_id}/status — change status (sets pending_write flag)
# ---------------------------------------------------------------------------

@router.patch("/notes/{note_id}/status")
async def update_note_status(note_id: int, payload: dict) -> dict:
    """Change a note's status. Sets pending_write flag so sync writes the change to disk."""
    new_status: str | None = payload.get("status")
    if not new_status:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="status is required")

    valid_statuses = {"inbox", "review", "active", "archived", "done"}
    if new_status not in valid_statuses:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"status must be one of {sorted(valid_statuses)}",
        )

    engine = get_db_engine()
    now = _now_utc()

    with engine.begin() as conn:
        # Verify note exists
        existing = conn.execute(
            text("SELECT id, agent_flags FROM obsidian_notes WHERE id = :id"),
            {"id": note_id},
        ).fetchone()
        if existing is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Note not found")

        # Merge pending_write into existing agent_flags
        existing_flags = existing._mapping["agent_flags"] or {}
        if isinstance(existing_flags, str):
            existing_flags = json.loads(existing_flags)
        updated_flags = {**existing_flags, "pending_write": True}

        conn.execute(
            text(
                """
                UPDATE obsidian_notes
                SET status = :status,
                    agent_flags = :agent_flags,
                    modified_at = :now
                WHERE id = :id
                """
            ),
            {
                "status": new_status,
                "agent_flags": json.dumps(updated_flags),
                "now": now,
                "id": note_id,
            },
        )

        # Log the action
        conn.execute(
            text(
                """
                INSERT INTO obsidian_actions (note_id, actor, action, detail, created_at)
                VALUES (:note_id, 'api', 'status_change', :detail, :now)
                """
            ),
            {
                "note_id": note_id,
                "detail": json.dumps({"new_status": new_status}),
                "now": now,
            },
        )

    log.info("vault: note id={id} status -> {s}", id=note_id, s=new_status)
    return {"id": note_id, "status": new_status, "updated": True}


# ---------------------------------------------------------------------------
# GET /search — FTS using body_tsvector
# ---------------------------------------------------------------------------

@router.get("/search")
async def search_notes(
    q: str = Query(description="Full-text search query"),
    domain: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """Full-text search across vault notes using the body_tsvector GIN index."""
    if not q or not q.strip():
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="q must not be empty")

    engine = get_db_engine()
    params: dict[str, Any] = {"q": q, "limit": limit, "offset": offset}
    domain_clause = ""
    if domain:
        domain_clause = " AND domain = :domain"
        params["domain"] = domain

    query = f"""
        SELECT {_NOTE_COLS},
               ts_rank(body_tsvector, plainto_tsquery('english', :q)) AS rank
        FROM obsidian_notes
        WHERE body_tsvector @@ plainto_tsquery('english', :q){domain_clause}
        ORDER BY rank DESC
        LIMIT :limit OFFSET :offset
    """
    count_query = f"""
        SELECT COUNT(*) FROM obsidian_notes
        WHERE body_tsvector @@ plainto_tsquery('english', :q){domain_clause}
    """

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()
        total = conn.execute(
            text(count_query),
            {k: v for k, v in params.items() if k not in ("limit", "offset")},
        ).fetchone()[0]

    results = []
    for r in rows:
        d = _row_to_dict(r)
        d["rank"] = float(r._mapping.get("rank", 0))
        results.append(d)

    return {
        "results": results,
        "total": total,
        "query": q,
        "limit": limit,
        "offset": offset,
    }


# ---------------------------------------------------------------------------
# GET /actions — audit log
# ---------------------------------------------------------------------------

@router.get("/actions")
async def list_actions(
    note_id: int | None = Query(default=None, description="Filter by note id"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """Return the obsidian_actions audit log, optionally filtered by note."""
    engine = get_db_engine()
    params: dict[str, Any] = {"limit": limit, "offset": offset}
    where = ""
    if note_id is not None:
        where = " WHERE note_id = :note_id"
        params["note_id"] = note_id

    query = f"SELECT * FROM obsidian_actions{where} ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
    count_query = f"SELECT COUNT(*) FROM obsidian_actions{where}"

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()
        count_params = {k: v for k, v in params.items() if k not in ("limit", "offset")}
        total = conn.execute(text(count_query), count_params).fetchone()[0]

    return {
        "actions": [_action_row_to_dict(r) for r in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": (offset + limit) < total,
    }


# ---------------------------------------------------------------------------
# GET /dashboard — review items + aggregate stats
# ---------------------------------------------------------------------------

@router.get("/dashboard")
async def get_dashboard() -> dict:
    """Return vault dashboard: review queue, status counts, domain breakdown, recent actions."""
    engine = get_db_engine()
    with engine.connect() as conn:
        # Status breakdown
        status_rows = conn.execute(
            text("SELECT status, COUNT(*) AS cnt FROM obsidian_notes GROUP BY status ORDER BY cnt DESC")
        ).fetchall()

        # Domain breakdown
        domain_rows = conn.execute(
            text("SELECT domain, COUNT(*) AS cnt FROM obsidian_notes GROUP BY domain ORDER BY cnt DESC")
        ).fetchall()

        # Notes awaiting review (status = 'review')
        review_rows = conn.execute(
            text(
                f"SELECT {_NOTE_COLS} FROM obsidian_notes WHERE status = 'review' ORDER BY modified_at DESC LIMIT 50"
            )
        ).fetchall()

        # Pending writes (agent_flags->>'pending_write' = 'true')
        pending_count = conn.execute(
            text("SELECT COUNT(*) FROM obsidian_notes WHERE agent_flags->>'pending_write' = 'true'")
        ).fetchone()[0]

        # Total note count
        total = conn.execute(text("SELECT COUNT(*) FROM obsidian_notes")).fetchone()[0]

        # Recent actions (last 20)
        recent_actions = conn.execute(
            text("SELECT * FROM obsidian_actions ORDER BY created_at DESC LIMIT 20")
        ).fetchall()

    return {
        "total_notes": total,
        "pending_writes": pending_count,
        "status_counts": {r._mapping["status"]: r._mapping["cnt"] for r in status_rows},
        "domain_counts": {r._mapping["domain"]: r._mapping["cnt"] for r in domain_rows},
        "review_queue": [_row_to_dict(r) for r in review_rows],
        "recent_actions": [_action_row_to_dict(r) for r in recent_actions],
    }


# ---------------------------------------------------------------------------
# POST /sync — trigger manual sync run
# ---------------------------------------------------------------------------

@router.post("/sync")
async def trigger_sync() -> dict:
    """Trigger a manual Obsidian vault sync (inbound + outbound)."""
    try:
        from ingestion.altdata.obsidian_sync import run_sync
        result = run_sync()
        log.info("vault: manual sync triggered via API, result={r}", r=result)
        return {"triggered": True, "result": result}
    except ImportError as exc:
        log.warning("vault: obsidian_sync not available: {e}", e=str(exc))
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Obsidian sync engine not available",
        )
    except Exception as exc:
        log.error("vault: sync failed: {e}", e=str(exc))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Sync failed: {exc}",
        )
