"""Canvas sub-router: node and edge CRUD + bulk graph save."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from loguru import logger as log
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.engine import Connection

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(tags=["canvas"])


# ── Request schemas ──────────────────────────────────────────────────────

class NodeCreate(BaseModel):
    # Accept either id/node_id for flexibility
    id: str | None = None
    node_id: str | None = None
    node_type: str = "note"
    label: str | None = None
    # Accept either x/y or position_x/position_y
    x: float | None = None
    y: float | None = None
    position_x: float | None = None
    position_y: float | None = None
    data: dict | None = None


class NodeUpdate(BaseModel):
    label: str | None = None
    position_x: float | None = None
    position_y: float | None = None
    data: dict | None = None


class EdgeCreate(BaseModel):
    id: str | None = None
    edge_id: str | None = None
    source_node_id: str | None = None
    target_node_id: str | None = None
    source: str | None = None
    target: str | None = None
    edge_type: str | None = "default"
    label: str | None = None
    data: dict | None = None


class BulkGraphSave(BaseModel):
    nodes: list[dict] = []
    edges: list[dict] = []


# ── Helpers ──────────────────────────────────────────────────────────────

def _row_to_dict(row: Any) -> dict:
    """Convert a SQLAlchemy row to a plain dict with ISO timestamps."""
    d = dict(row._mapping)
    for key in ("created_at", "updated_at"):
        if key in d and isinstance(d[key], datetime):
            d[key] = d[key].isoformat()
    return d


def _touch_board(conn: Connection, board_id: str) -> None:
    """Bump updated_at on the parent board after any graph mutation."""
    conn.execute(
        text("UPDATE canvas_boards SET updated_at = NOW() WHERE id = :board_id"),
        {"board_id": board_id},
    )


def _ensure_board_exists(conn: Connection, board_id: str) -> None:
    """Raise 404 if the board does not exist."""
    row = conn.execute(
        text("SELECT id FROM canvas_boards WHERE id = :board_id"),
        {"board_id": board_id},
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Board {board_id} not found",
        )


# ── Node endpoints ───────────────────────────────────────────────────────

@router.post("/boards/{board_id}/nodes", status_code=201)
async def add_node(
    board_id: str,
    body: NodeCreate,
    _token: str = Depends(require_auth),
) -> dict:
    """Add a node to a board (UPSERT via ON CONFLICT)."""
    engine = get_db_engine()

    # Resolve flexible field names
    node_id = body.node_id or body.id
    if not node_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="node_id (or id) is required",
        )

    pos_x = body.position_x if body.position_x is not None else (body.x or 0.0)
    pos_y = body.position_y if body.position_y is not None else (body.y or 0.0)

    with engine.begin() as conn:
        _ensure_board_exists(conn, board_id)

        row = conn.execute(
            text(
                "INSERT INTO canvas_nodes (id, board_id, node_type, label, position_x, position_y, data)"
                " VALUES (:id, :board_id, :node_type, :label, :position_x, :position_y, :data)"
                " ON CONFLICT (id) DO UPDATE SET"
                "   node_type = EXCLUDED.node_type,"
                "   label = EXCLUDED.label,"
                "   position_x = EXCLUDED.position_x,"
                "   position_y = EXCLUDED.position_y,"
                "   data = EXCLUDED.data"
                " RETURNING *"
            ),
            {
                "id": node_id,
                "board_id": board_id,
                "node_type": body.node_type,
                "label": body.label,
                "position_x": pos_x,
                "position_y": pos_y,
                "data": json.dumps(body.data) if body.data else None,
            },
        ).fetchone()

        _touch_board(conn, board_id)

    return _row_to_dict(row)


@router.put("/boards/{board_id}/nodes/{node_id}")
async def update_node(
    board_id: str,
    node_id: str,
    body: NodeUpdate,
    _token: str = Depends(require_auth),
) -> dict:
    """Update a node's position, label, or data."""
    engine = get_db_engine()

    with engine.begin() as conn:
        # Check existence
        existing = conn.execute(
            text(
                "SELECT id FROM canvas_nodes"
                " WHERE id = :node_id AND board_id = :board_id"
            ),
            {"node_id": node_id, "board_id": board_id},
        ).fetchone()

        if existing is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Node {node_id} not found on board {board_id}",
            )

        # Build SET clause from provided fields
        updates: dict[str, Any] = {"node_id": node_id, "board_id": board_id}
        set_parts: list[str] = []

        if body.label is not None:
            set_parts.append("label = :label")
            updates["label"] = body.label

        if body.position_x is not None:
            set_parts.append("position_x = :position_x")
            updates["position_x"] = body.position_x

        if body.position_y is not None:
            set_parts.append("position_y = :position_y")
            updates["position_y"] = body.position_y

        if body.data is not None:
            set_parts.append("data = :data")
            updates["data"] = json.dumps(body.data)

        if not set_parts:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No fields to update",
            )

        row = conn.execute(
            text(
                f"UPDATE canvas_nodes SET {', '.join(set_parts)}"
                " WHERE id = :node_id AND board_id = :board_id"
                " RETURNING *"
            ),
            updates,
        ).fetchone()

        _touch_board(conn, board_id)

    return _row_to_dict(row)


@router.delete("/boards/{board_id}/nodes/{node_id}")
async def delete_node(
    board_id: str,
    node_id: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Delete a node and all connected edges."""
    engine = get_db_engine()

    with engine.begin() as conn:
        # Delete connected edges first
        conn.execute(
            text(
                "DELETE FROM canvas_edges"
                " WHERE board_id = :board_id"
                "   AND (source_node_id = :node_id OR target_node_id = :node_id)"
            ),
            {"board_id": board_id, "node_id": node_id},
        )

        result = conn.execute(
            text(
                "DELETE FROM canvas_nodes"
                " WHERE id = :node_id AND board_id = :board_id"
                " RETURNING id"
            ),
            {"node_id": node_id, "board_id": board_id},
        ).fetchone()

        if result is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Node {node_id} not found on board {board_id}",
            )

        _touch_board(conn, board_id)

    log.info("Canvas node deleted: {node} from board {board}", node=node_id, board=board_id)
    return {"status": "deleted", "id": node_id}


# ── Edge endpoints ───────────────────────────────────────────────────────

@router.post("/boards/{board_id}/edges", status_code=201)
async def add_edge(
    board_id: str,
    body: EdgeCreate,
    _token: str = Depends(require_auth),
) -> dict:
    """Add an edge between two nodes (UPSERT via ON CONFLICT)."""
    engine = get_db_engine()

    # Resolve flexible field names
    edge_id = body.edge_id or body.id
    if not edge_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="edge_id (or id) is required",
        )

    source = body.source_node_id or body.source
    target = body.target_node_id or body.target

    if not source or not target:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="source_node_id (or source) and target_node_id (or target) are required",
        )

    with engine.begin() as conn:
        _ensure_board_exists(conn, board_id)

        row = conn.execute(
            text(
                "INSERT INTO canvas_edges (id, board_id, source_node_id, target_node_id, edge_type, label, data)"
                " VALUES (:id, :board_id, :source_node_id, :target_node_id, :edge_type, :label, :data)"
                " ON CONFLICT (id) DO UPDATE SET"
                "   source_node_id = EXCLUDED.source_node_id,"
                "   target_node_id = EXCLUDED.target_node_id,"
                "   edge_type = EXCLUDED.edge_type,"
                "   label = EXCLUDED.label,"
                "   data = EXCLUDED.data"
                " RETURNING *"
            ),
            {
                "id": edge_id,
                "board_id": board_id,
                "source_node_id": source,
                "target_node_id": target,
                "edge_type": body.edge_type or "default",
                "label": body.label,
                "data": json.dumps(body.data) if body.data else None,
            },
        ).fetchone()

        _touch_board(conn, board_id)

    return _row_to_dict(row)


@router.delete("/boards/{board_id}/edges/{edge_id}")
async def delete_edge(
    board_id: str,
    edge_id: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Delete an edge."""
    engine = get_db_engine()

    with engine.begin() as conn:
        result = conn.execute(
            text(
                "DELETE FROM canvas_edges"
                " WHERE id = :edge_id AND board_id = :board_id"
                " RETURNING id"
            ),
            {"edge_id": edge_id, "board_id": board_id},
        ).fetchone()

        if result is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Edge {edge_id} not found on board {board_id}",
            )

        _touch_board(conn, board_id)

    return {"status": "deleted", "id": edge_id}


# ── Bulk graph save ──────────────────────────────────────────────────────

@router.put("/boards/{board_id}/graph")
async def bulk_save_graph(
    board_id: str,
    body: BulkGraphSave,
    _token: str = Depends(require_auth),
) -> dict:
    """Replace entire graph: delete all nodes+edges, reinsert from body.

    This is the primary save path used by the frontend canvas.
    """
    engine = get_db_engine()

    with engine.begin() as conn:
        _ensure_board_exists(conn, board_id)

        # Delete existing graph (edges first due to FK)
        conn.execute(
            text("DELETE FROM canvas_edges WHERE board_id = :board_id"),
            {"board_id": board_id},
        )
        conn.execute(
            text("DELETE FROM canvas_nodes WHERE board_id = :board_id"),
            {"board_id": board_id},
        )

        # Insert nodes
        for node in body.nodes:
            node_id = node.get("id") or node.get("node_id")
            if not node_id:
                continue

            pos_x = node.get("position_x") or node.get("x") or 0.0
            pos_y = node.get("position_y") or node.get("y") or 0.0
            data = node.get("data")

            conn.execute(
                text(
                    "INSERT INTO canvas_nodes (id, board_id, node_type, label, position_x, position_y, data)"
                    " VALUES (:id, :board_id, :node_type, :label, :position_x, :position_y, :data)"
                ),
                {
                    "id": node_id,
                    "board_id": board_id,
                    "node_type": node.get("node_type", "note"),
                    "label": node.get("label"),
                    "position_x": pos_x,
                    "position_y": pos_y,
                    "data": json.dumps(data) if data else None,
                },
            )

        # Insert edges
        for edge in body.edges:
            edge_id = edge.get("id") or edge.get("edge_id")
            if not edge_id:
                continue

            source = edge.get("source_node_id") or edge.get("source")
            target = edge.get("target_node_id") or edge.get("target")
            if not source or not target:
                continue

            data = edge.get("data")

            conn.execute(
                text(
                    "INSERT INTO canvas_edges (id, board_id, source_node_id, target_node_id, edge_type, label, data)"
                    " VALUES (:id, :board_id, :source_node_id, :target_node_id, :edge_type, :label, :data)"
                ),
                {
                    "id": edge_id,
                    "board_id": board_id,
                    "source_node_id": source,
                    "target_node_id": target,
                    "edge_type": edge.get("edge_type", "default"),
                    "label": edge.get("label"),
                    "data": json.dumps(data) if data else None,
                },
            )

        _touch_board(conn, board_id)

    log.info(
        "Canvas bulk save: board={board} nodes={n} edges={e}",
        board=board_id,
        n=len(body.nodes),
        e=len(body.edges),
    )
    return {
        "status": "saved",
        "board_id": board_id,
        "nodes": len(body.nodes),
        "edges": len(body.edges),
    }
