"""Canvas sub-router: node and edge CRUD + bulk graph save."""

from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException, status
from loguru import logger as log
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.engine import Connection

from api.auth import require_auth
from api.dependencies import get_db_engine
from api.routers.canvas_board_store import (
    delete_graph_edge,
    delete_graph_node,
    edge_response,
    ensure_investigation_boards_table,
    get_board_graph_state,
    graph_edge_from_payload,
    graph_node_from_payload,
    node_response,
    row_to_dict,
    save_board_graph_state,
    sync_legacy_canvas_from_board,
    upsert_graph_edge,
    upsert_graph_node,
    update_graph_node,
)

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


def _touch_board(conn: Connection, board_id: str) -> None:
    """Bump updated_at on the parent board after any graph mutation."""
    conn.execute(
        text("UPDATE investigation_boards SET updated_at = NOW() WHERE id = :board_id"),
        {"board_id": board_id},
    )


def _ensure_board_exists(conn: Connection, board_id: str) -> None:
    """Raise 404 if the board does not exist."""
    row = conn.execute(
        text("SELECT id FROM investigation_boards WHERE id = :board_id"),
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
    """Add or update a node in the canonical board graph state."""
    engine = get_db_engine()
    ensure_investigation_boards_table(engine)

    payload = body.dict(exclude_none=True)
    try:
        node = graph_node_from_payload(payload)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="node_id (or id) is required",
        ) from None

    with engine.begin() as conn:
        graph_state = get_board_graph_state(conn, board_id)
        if graph_state is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Board {board_id} not found",
            )
        saved_node = upsert_graph_node(graph_state, node)
        save_board_graph_state(conn, board_id, graph_state)
        sync_legacy_canvas_from_board(conn, board_id, graph_state)

    return node_response(board_id, saved_node)


@router.put("/boards/{board_id}/nodes/{node_id}")
async def update_node(
    board_id: str,
    node_id: str,
    body: NodeUpdate,
    _token: str = Depends(require_auth),
) -> dict:
    """Update a node's position, label, or data in graph_state."""
    engine = get_db_engine()
    ensure_investigation_boards_table(engine)
    updates = body.dict(exclude_none=True)

    if not updates:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No fields to update",
        )

    with engine.begin() as conn:
        graph_state = get_board_graph_state(conn, board_id)
        if graph_state is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Board {board_id} not found",
            )
        saved_node = update_graph_node(graph_state, node_id, updates)
        if saved_node is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Node {node_id} not found on board {board_id}",
            )
        save_board_graph_state(conn, board_id, graph_state)
        sync_legacy_canvas_from_board(conn, board_id, graph_state)

    return node_response(board_id, saved_node)


@router.delete("/boards/{board_id}/nodes/{node_id}")
async def delete_node(
    board_id: str,
    node_id: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Delete a node and all connected edges from graph_state."""
    engine = get_db_engine()
    ensure_investigation_boards_table(engine)

    with engine.begin() as conn:
        graph_state = get_board_graph_state(conn, board_id)
        if graph_state is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Board {board_id} not found",
            )
        deleted = delete_graph_node(graph_state, node_id)
        if deleted is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Node {node_id} not found on board {board_id}",
            )
        save_board_graph_state(conn, board_id, graph_state)
        sync_legacy_canvas_from_board(conn, board_id, graph_state)

    log.info("Canvas node deleted: {node} from board {board}", node=node_id, board=board_id)
    return {"status": "deleted", "id": node_id}


# ── Edge endpoints ───────────────────────────────────────────────────────

@router.post("/boards/{board_id}/edges", status_code=201)
async def add_edge(
    board_id: str,
    body: EdgeCreate,
    _token: str = Depends(require_auth),
) -> dict:
    """Add or update an edge in the canonical board graph state."""
    engine = get_db_engine()
    ensure_investigation_boards_table(engine)

    payload = body.dict(exclude_none=True)
    try:
        edge = graph_edge_from_payload(payload)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="source_node_id (or source) and target_node_id (or target) are required",
        ) from None

    with engine.begin() as conn:
        graph_state = get_board_graph_state(conn, board_id)
        if graph_state is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Board {board_id} not found",
            )
        saved_edge = upsert_graph_edge(graph_state, edge)
        save_board_graph_state(conn, board_id, graph_state)
        sync_legacy_canvas_from_board(conn, board_id, graph_state)

    return edge_response(board_id, saved_edge)


@router.delete("/boards/{board_id}/edges/{edge_id}")
async def delete_edge(
    board_id: str,
    edge_id: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Delete an edge from graph_state."""
    engine = get_db_engine()
    ensure_investigation_boards_table(engine)

    with engine.begin() as conn:
        graph_state = get_board_graph_state(conn, board_id)
        if graph_state is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Board {board_id} not found",
            )
        deleted = delete_graph_edge(graph_state, edge_id)
        if deleted is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Edge {edge_id} not found on board {board_id}",
            )
        save_board_graph_state(conn, board_id, graph_state)
        sync_legacy_canvas_from_board(conn, board_id, graph_state)

    return {"status": "deleted", "id": edge_id}


# ── Bulk graph save ──────────────────────────────────────────────────────

@router.put("/boards/{board_id}/graph")
async def bulk_save_graph(
    board_id: str,
    body: BulkGraphSave,
    _token: str = Depends(require_auth),
) -> dict:
    """Replace the entire graph_state for a board."""
    engine = get_db_engine()
    ensure_investigation_boards_table(engine)
    graph_state = {
        "nodes": [
            graph_node_from_payload(node)
            for node in body.nodes
            if isinstance(node, dict) and (node.get("id") or node.get("key") or node.get("node_id"))
        ],
        "edges": [],
    }
    node_ids = {
        node.get("id") or node.get("key")
        for node in graph_state["nodes"]
        if isinstance(node, dict)
    }
    for edge_payload in body.edges:
        if not isinstance(edge_payload, dict):
            continue
        try:
            edge = graph_edge_from_payload(edge_payload)
        except ValueError:
            continue
        if edge["source"] in node_ids and edge["target"] in node_ids:
            graph_state["edges"].append(edge)

    with engine.begin() as conn:
        if not save_board_graph_state(conn, board_id, graph_state):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Board {board_id} not found",
            )
        sync_legacy_canvas_from_board(conn, board_id, graph_state)

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
    ensure_investigation_boards_table(engine)

    # Validate confidence label
    confidence = body.confidence or "derived"
    if confidence not in VALID_CONFIDENCE_LABELS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Invalid confidence label: {confidence}. Must be one of: "
                f"{', '.join(sorted(VALID_CONFIDENCE_LABELS))}"
            ),
        )

    # Validate evidence type
    if body.evidence_type not in VALID_EVIDENCE_TYPES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Invalid evidence_type: {body.evidence_type}. Must be one of: "
                f"{', '.join(sorted(VALID_EVIDENCE_TYPES))}"
            ),
        )

    with engine.begin() as conn:
        _ensure_board_exists(conn, board_id)
        sync_legacy_canvas_from_board(conn, board_id)

        row = conn.execute(
            text(
                "INSERT INTO investigation_evidence"
                " (board_id, node_id, evidence_type, content, source_url, "
                "source_table, source_id, confidence, metadata)"
                " VALUES (:board_id, :node_id, :evidence_type, :content, "
                ":source_url, :source_table, :source_id, :confidence, :metadata)"
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
    return row_to_dict(row)


@router.get("/boards/{board_id}/nodes/{node_id}/evidence")
async def get_node_evidence(
    board_id: str,
    node_id: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Return all evidence attached to a specific node on a board."""
    engine = get_db_engine()
    ensure_investigation_boards_table(engine)

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

    return {"evidence": [row_to_dict(r) for r in rows]}


@router.delete("/boards/{board_id}/evidence/{evidence_id}")
async def delete_evidence(
    board_id: str,
    evidence_id: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Delete a single evidence item."""
    engine = get_db_engine()
    ensure_investigation_boards_table(engine)

    with engine.begin() as conn:
        _ensure_board_exists(conn, board_id)
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
