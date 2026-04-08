"""Canvas sub-router: board CRUD endpoints."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from loguru import logger as log
from pydantic import BaseModel
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(tags=["canvas"])


# ── Request schemas ──────────────────────────────────────────────────────

class BoardCreate(BaseModel):
    name: str
    description: str | None = None


class BoardUpdate(BaseModel):
    name: str | None = None
    description: str | None = None


# ── Helpers ──────────────────────────────────────────────────────────────

def _row_to_dict(row: Any) -> dict:
    """Convert a SQLAlchemy row to a plain dict with ISO timestamps."""
    d = dict(row._mapping)
    for key in ("created_at", "updated_at"):
        if key in d and isinstance(d[key], datetime):
            d[key] = d[key].isoformat()
    return d


# ── Endpoints ────────────────────────────────────────────────────────────

@router.get("/boards")
async def list_boards(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    _token: str = Depends(require_auth),
) -> dict:
    """Return all canvas boards with pagination."""
    engine = get_db_engine()

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT * FROM canvas_boards"
                " ORDER BY updated_at DESC"
                " LIMIT :limit OFFSET :offset"
            ),
            {"limit": limit, "offset": offset},
        ).fetchall()

        total = conn.execute(
            text("SELECT COUNT(*) FROM canvas_boards")
        ).fetchone()[0]

    items = [_row_to_dict(row) for row in rows]
    return {
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": (offset + limit) < total,
    }


@router.post("/boards", status_code=201)
async def create_board(
    body: BoardCreate,
    _token: str = Depends(require_auth),
) -> dict:
    """Create a new canvas board."""
    if not body.name or not body.name.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Board name cannot be empty",
        )

    engine = get_db_engine()

    with engine.begin() as conn:
        row = conn.execute(
            text(
                "INSERT INTO canvas_boards (name, description)"
                " VALUES (:name, :description)"
                " RETURNING id, name, description, created_at, updated_at"
            ),
            {"name": body.name.strip(), "description": body.description},
        ).fetchone()

    board = _row_to_dict(row)
    log.info("Canvas board created: {id} — {name}", id=board["id"], name=board["name"])
    return board


@router.get("/boards/{board_id}")
async def get_board(
    board_id: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Return a single board with all its nodes and edges."""
    engine = get_db_engine()

    with engine.connect() as conn:
        board_row = conn.execute(
            text("SELECT * FROM canvas_boards WHERE id = :board_id"),
            {"board_id": board_id},
        ).fetchone()

        if board_row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Board {board_id} not found",
            )

        nodes = conn.execute(
            text(
                "SELECT * FROM canvas_nodes"
                " WHERE board_id = :board_id"
                " ORDER BY created_at"
            ),
            {"board_id": board_id},
        ).fetchall()

        edges = conn.execute(
            text(
                "SELECT * FROM canvas_edges"
                " WHERE board_id = :board_id"
                " ORDER BY created_at"
            ),
            {"board_id": board_id},
        ).fetchall()

    board = _row_to_dict(board_row)
    board["nodes"] = [_row_to_dict(n) for n in nodes]
    board["edges"] = [_row_to_dict(e) for e in edges]
    return board


@router.put("/boards/{board_id}")
async def update_board(
    board_id: str,
    body: BoardUpdate,
    _token: str = Depends(require_auth),
) -> dict:
    """Update board name and/or description."""
    engine = get_db_engine()

    with engine.begin() as conn:
        # Check existence
        existing = conn.execute(
            text("SELECT id FROM canvas_boards WHERE id = :board_id"),
            {"board_id": board_id},
        ).fetchone()

        if existing is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Board {board_id} not found",
            )

        # Build SET clause dynamically from provided fields
        updates: dict[str, Any] = {"board_id": board_id}
        set_parts: list[str] = ["updated_at = NOW()"]

        if body.name is not None:
            if not body.name.strip():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Board name cannot be empty",
                )
            set_parts.append("name = :name")
            updates["name"] = body.name.strip()

        if body.description is not None:
            set_parts.append("description = :description")
            updates["description"] = body.description

        row = conn.execute(
            text(
                f"UPDATE canvas_boards SET {', '.join(set_parts)}"
                " WHERE id = :board_id"
                " RETURNING id, name, description, created_at, updated_at"
            ),
            updates,
        ).fetchone()

    board = _row_to_dict(row)
    log.info("Canvas board updated: {id}", id=board["id"])
    return board


@router.delete("/boards/{board_id}")
async def delete_board(
    board_id: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Delete a board and all its nodes/edges (CASCADE)."""
    engine = get_db_engine()

    with engine.begin() as conn:
        result = conn.execute(
            text("DELETE FROM canvas_boards WHERE id = :board_id RETURNING id"),
            {"board_id": board_id},
        ).fetchone()

    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Board {board_id} not found",
        )

    log.info("Canvas board deleted: {id}", id=board_id)
    return {"status": "deleted", "id": board_id}
