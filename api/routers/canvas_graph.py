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


class EvidenceCreate(BaseModel):
    evidence_type: str
    content: str | None = None
    source_url: str | None = None
    source_table: str | None = None
    source_id: str | None = None
    confidence: str = "derived"
    metadata: dict | None = None


# ── Helpers ──────────────────────────────────────────────────────────────

VALID_CONFIDENCE_LABELS = {"confirmed", "derived", "estimated", "rumored", "inferred"}

VALID_EVIDENCE_TYPES = {"signal", "filing", "quote", "chart", "news", "prediction"}


def _row_to_dict(row: Any) -> dict:
    """Convert a SQLAlchemy row to a plain dict with ISO timestamps."""
    d = dict(row._mapping)
    for key in ("created_at", "updated_at", "captured_at"):
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
                "INSERT INTO canvas_nodes (node_id, board_id, node_type, label, position_x, position_y, data)"
                " VALUES (:nid, :board_id, :node_type, :label, :position_x, :position_y, :data)"
                " ON CONFLICT (node_id) WHERE node_id IS NOT NULL DO UPDATE SET"
                "   node_type = EXCLUDED.node_type,"
                "   label = EXCLUDED.label,"
                "   position_x = EXCLUDED.position_x,"
                "   position_y = EXCLUDED.position_y,"
                "   data = EXCLUDED.data"
                " RETURNING *"
            ),
            {
                "nid": node_id,
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
        # Check existence (the primary-key column was renamed id → node_id
        # in migration 20260411_rename_canvas_nodes_id.sql; selecting
        # node_id is functionally identical since the result is only used
        # to detect row presence).
        existing = conn.execute(
            text(
                "SELECT node_id FROM canvas_nodes"
                " WHERE node_id = :node_id AND board_id = :board_id"
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

        # set_parts is built from hardcoded column names; user values are bind params
        set_clause = ", ".join(set_parts)
        update_sql = (
            "UPDATE canvas_nodes SET " + set_clause
            + " WHERE node_id = :node_id AND board_id = :board_id"
            + " RETURNING *"
        )
        row = conn.execute(text(update_sql), updates).fetchone()

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
                " WHERE node_id = :node_id AND board_id = :board_id"
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
                    "INSERT INTO canvas_nodes (node_id, board_id, node_type, label, position_x, position_y, data)"
                    " VALUES (:nid, :board_id, :node_type, :label, :position_x, :position_y, :data)"
                ),
                {
                    "nid": node_id,
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


# ── Evidence endpoints ──────────────────────────────────────────────────


@router.post("/boards/{board_id}/nodes/{node_id}/evidence", status_code=201)
async def add_evidence(
    board_id: str,
    node_id: str,
    body: EvidenceCreate,
    _token: str = Depends(require_auth),
) -> dict:
    """Pin a piece of evidence (signal, filing, quote, chart, etc.) to a node."""
    engine = get_db_engine()

    # Validate confidence label
    confidence = body.confidence or "derived"
    if confidence not in VALID_CONFIDENCE_LABELS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid confidence label: {confidence}. Must be one of: {', '.join(sorted(VALID_CONFIDENCE_LABELS))}",
        )

    # Validate evidence type
    if body.evidence_type not in VALID_EVIDENCE_TYPES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid evidence_type: {body.evidence_type}. Must be one of: {', '.join(sorted(VALID_EVIDENCE_TYPES))}",
        )

    with engine.begin() as conn:
        _ensure_board_exists(conn, board_id)

        row = conn.execute(
            text(
                "INSERT INTO investigation_evidence"
                " (board_id, node_id, evidence_type, content, source_url, source_table, source_id, confidence, metadata)"
                " VALUES (:board_id, :node_id, :evidence_type, :content, :source_url, :source_table, :source_id, :confidence, :metadata)"
                " RETURNING id, evidence_type, content, source_url, confidence, captured_at"
            ),
            {
                "board_id": board_id,
                "node_id": node_id,
                "evidence_type": body.evidence_type,
                "content": body.content,
                "source_url": body.source_url,
                "source_table": body.source_table,
                "source_id": body.source_id,
                "confidence": confidence,
                "metadata": json.dumps(body.metadata) if body.metadata else "{}",
            },
        ).fetchone()

        _touch_board(conn, board_id)

    log.info(
        "Evidence added: board={board} node={node} type={etype}",
        board=board_id,
        node=node_id,
        etype=body.evidence_type,
    )
    return _row_to_dict(row)


@router.get("/boards/{board_id}/nodes/{node_id}/evidence")
async def get_node_evidence(
    board_id: str,
    node_id: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Return all evidence attached to a specific node on a board."""
    engine = get_db_engine()

    with engine.connect() as conn:
        _ensure_board_exists(conn, board_id)

        rows = conn.execute(
            text(
                "SELECT id, evidence_type, content, source_url, confidence, captured_at, metadata"
                " FROM investigation_evidence"
                " WHERE board_id = :board_id AND node_id = :node_id"
                " ORDER BY captured_at DESC"
            ),
            {"board_id": board_id, "node_id": node_id},
        ).fetchall()

    return {"evidence": [_row_to_dict(r) for r in rows]}


@router.delete("/boards/{board_id}/evidence/{evidence_id}")
async def delete_evidence(
    board_id: str,
    evidence_id: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Delete a single evidence item."""
    engine = get_db_engine()

    with engine.begin() as conn:
        result = conn.execute(
            text(
                "DELETE FROM investigation_evidence"
                " WHERE id = :evidence_id AND board_id = :board_id"
                " RETURNING id"
            ),
            {"evidence_id": evidence_id, "board_id": board_id},
        ).fetchone()

        if result is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Evidence {evidence_id} not found on board {board_id}",
            )

        _touch_board(conn, board_id)

    log.info("Evidence deleted: {eid} from board {board}", eid=evidence_id, board=board_id)
    return {"status": "deleted", "id": evidence_id}
