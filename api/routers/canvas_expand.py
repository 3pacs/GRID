"""Canvas sub-router: graph expansion — expand network, path finding, suggest connections."""

from __future__ import annotations

import json
import math
import uuid
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

class PathRequest(BaseModel):
    source_node_id: str
    target_node_id: str


# ── Helpers ──────────────────────────────────────────────────────────────

def _row_to_dict(row: Any) -> dict:
    """Convert a SQLAlchemy row to a plain dict with ISO timestamps."""
    d = dict(row._mapping)
    for key in ("created_at", "updated_at"):
        if key in d and isinstance(d[key], datetime):
            d[key] = d[key].isoformat()
    return d


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


def _touch_board(conn: Connection, board_id: str) -> None:
    """Bump updated_at on the parent board after any graph mutation."""
    conn.execute(
        text("UPDATE canvas_boards SET updated_at = NOW() WHERE id = :board_id"),
        {"board_id": board_id},
    )


def _get_graph():
    """Return the shared spider graph engine (lazy — reuses the one loaded by main.py)."""
    import api.routers.intelligence_spider as spider_router

    if spider_router._graph_engine is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Graph engine not initialized yet — still loading",
        )
    return spider_router._graph_engine


def _circular_positions(
    center_x: float, center_y: float, count: int, radius: float = 200.0
) -> list[tuple[float, float]]:
    """Calculate positions arranged in a circle around a center point."""
    if count == 0:
        return []
    positions = []
    for i in range(count):
        angle = (2 * math.pi * i) / count
        x = center_x + radius * math.cos(angle)
        y = center_y + radius * math.sin(angle)
        positions.append((x, y))
    return positions


# ── Expand endpoint ──────────────────────────────────────────────────────

@router.post("/boards/{board_id}/expand/{node_id}")
async def expand_node(
    board_id: str,
    node_id: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Expand an actor/company node to show connected actors on the canvas.

    Uses the spider graph engine (BFS depth=1) to find neighbors,
    creates canvas nodes positioned in a circle around the source node,
    and inserts new nodes + edges into the database.
    """
    engine = get_db_engine()
    graph = _get_graph()

    with engine.begin() as conn:
        _ensure_board_exists(conn, board_id)

        # Get the source node to find entity_id and position
        source_row = conn.execute(
            text(
                "SELECT id, node_type, label, position_x, position_y, data"
                " FROM canvas_nodes"
                " WHERE node_id = :node_id AND board_id = :board_id"
            ),
            {"node_id": node_id, "board_id": board_id},
        ).fetchone()

        if source_row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Node {node_id} not found on board {board_id}",
            )

        source = dict(source_row._mapping)
        node_type = source["node_type"]

        if node_type not in ("actor", "company"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cannot expand node of type '{node_type}' — only actor and company nodes",
            )

        # Determine the actor_id to look up in the graph
        source_data = source.get("data")
        if isinstance(source_data, str):
            try:
                source_data = json.loads(source_data)
            except (json.JSONDecodeError, TypeError):
                source_data = {}
        elif source_data is None:
            source_data = {}

        entity_id = source_data.get("entityId") or source_data.get("entity_id")
        if not entity_id:
            # Fall back: try to resolve by label
            entity_id = graph.resolve_name(source.get("label", ""))

        if not entity_id or not graph.has_actor(entity_id):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Actor not found in graph for node '{node_id}'",
            )

        # Get existing node IDs on this board to avoid duplicates
        existing_rows = conn.execute(
            text("SELECT id, data FROM canvas_nodes WHERE board_id = :board_id"),
            {"board_id": board_id},
        ).fetchall()

        existing_entity_ids: set[str] = set()
        for erow in existing_rows:
            edata = erow._mapping.get("data")
            if isinstance(edata, str):
                try:
                    edata = json.loads(edata)
                except (json.JSONDecodeError, TypeError):
                    edata = {}
            elif edata is None:
                edata = {}
            eid = edata.get("entityId") or edata.get("entity_id")
            if eid:
                existing_entity_ids.add(eid)

        existing_node_ids = {str(erow._mapping["id"]) for erow in existing_rows}

        # BFS depth=1 from the graph engine
        neighbors = graph.get_neighbors(entity_id)
        if not neighbors:
            return {"new_nodes": [], "new_edges": [], "message": "No connections found"}

        # Filter out actors already on the board
        new_neighbors = {
            nid: meta for nid, meta in neighbors.items()
            if nid not in existing_entity_ids
        }

        if not new_neighbors:
            return {"new_nodes": [], "new_edges": [], "message": "All neighbors already on board"}

        # Position new nodes in a circle around the source
        center_x = float(source.get("position_x") or 0)
        center_y = float(source.get("position_y") or 0)
        positions = _circular_positions(center_x, center_y, len(new_neighbors))

        new_nodes = []
        new_edges = []

        for i, (neighbor_id, meta) in enumerate(new_neighbors.items()):
            actor_data = graph.get_actor(neighbor_id) or {}
            canvas_node_id = f"actor-{neighbor_id}-{uuid.uuid4().hex[:8]}"
            pos_x, pos_y = positions[i]

            node_data_json = {
                "entityId": neighbor_id,
                "category": actor_data.get("category", ""),
                "trust_score": actor_data.get("trust_score"),
                "influence_score": actor_data.get("influence_score"),
                "tier": actor_data.get("tier", ""),
            }

            # Insert the node
            node_row = conn.execute(
                text(
                    "INSERT INTO canvas_nodes (id, board_id, node_type, label, position_x, position_y, data)"
                    " VALUES (:id, :board_id, :node_type, :label, :position_x, :position_y, :data)"
                    " RETURNING *"
                ),
                {
                    "id": canvas_node_id,
                    "board_id": board_id,
                    "node_type": "actor",
                    "label": actor_data.get("name", neighbor_id),
                    "position_x": pos_x,
                    "position_y": pos_y,
                    "data": json.dumps(node_data_json),
                },
            ).fetchone()
            new_nodes.append(_row_to_dict(node_row))

            # Insert edge from source to new node
            edge_id = f"edge-{uuid.uuid4().hex[:12]}"
            edge_row = conn.execute(
                text(
                    "INSERT INTO canvas_edges (id, board_id, source_node_id, target_node_id, edge_type, label, data)"
                    " VALUES (:id, :board_id, :source_node_id, :target_node_id, :edge_type, :label, :data)"
                    " RETURNING *"
                ),
                {
                    "id": edge_id,
                    "board_id": board_id,
                    "source_node_id": node_id,
                    "target_node_id": canvas_node_id,
                    "edge_type": "smoothstep",
                    "label": meta.relationship or "",
                    "data": json.dumps({
                        "strength": meta.strength,
                        "confidence_tier": meta.confidence_tier,
                    }),
                },
            ).fetchone()
            new_edges.append(_row_to_dict(edge_row))

        _touch_board(conn, board_id)

    log.info(
        "Canvas expand: node={node} board={board} new_nodes={n} new_edges={e}",
        node=node_id, board=board_id, n=len(new_nodes), e=len(new_edges),
    )
    return {
        "new_nodes": new_nodes,
        "new_edges": new_edges,
        "expanded_count": len(new_nodes),
    }


# ── Shortest path endpoint ──────────────────────────────────────────────

@router.post("/boards/{board_id}/path")
async def find_path(
    board_id: str,
    body: PathRequest,
    _token: str = Depends(require_auth),
) -> dict:
    """Find the shortest path between two actor nodes on the canvas.

    Uses the spider graph engine's Dijkstra shortest-path algorithm.
    Returns the path as actor details.
    """
    engine = get_db_engine()
    graph = _get_graph()

    with engine.connect() as conn:
        _ensure_board_exists(conn, board_id)

        # Resolve both nodes to entity_ids
        entity_ids = {}
        for nid in (body.source_node_id, body.target_node_id):
            row = conn.execute(
                text(
                    "SELECT id, node_type, label, data"
                    " FROM canvas_nodes"
                    " WHERE node_id = :node_id AND board_id = :board_id"
                ),
                {"node_id": nid, "board_id": board_id},
            ).fetchone()

            if row is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Node {nid} not found on board {board_id}",
                )

            node = dict(row._mapping)
            ndata = node.get("data")
            if isinstance(ndata, str):
                try:
                    ndata = json.loads(ndata)
                except (json.JSONDecodeError, TypeError):
                    ndata = {}
            elif ndata is None:
                ndata = {}

            eid = ndata.get("entityId") or ndata.get("entity_id")
            if not eid:
                eid = graph.resolve_name(node.get("label", ""))
            if not eid:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Node '{nid}' has no actor entity mapping",
                )
            entity_ids[nid] = eid

    source_eid = entity_ids[body.source_node_id]
    target_eid = entity_ids[body.target_node_id]

    path = graph.shortest_path(source_eid, target_eid)
    if path is None:
        return {"path": None, "degrees": -1, "message": "No connection found"}

    path_details = []
    for i, aid in enumerate(path):
        actor = graph.get_actor(aid) or {}
        entry: dict[str, Any] = {
            "actor_id": aid,
            "name": actor.get("name", aid),
            "category": actor.get("category", ""),
            "degree": i,
        }
        if i > 0:
            nbrs = graph.get_neighbors(path[i - 1])
            meta = nbrs.get(aid)
            if meta:
                entry["connection"] = {
                    "relationship": meta.relationship,
                    "strength": meta.strength,
                    "confidence_tier": meta.confidence_tier,
                }
        path_details.append(entry)

    return {
        "path": path_details,
        "degrees": len(path) - 1,
        "source": body.source_node_id,
        "target": body.target_node_id,
    }


# ── Suggest connections endpoint ─────────────────────────────────────────

@router.post("/boards/{board_id}/suggest-connections")
async def suggest_connections(
    board_id: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Suggest connections between existing actor nodes on the board.

    Checks the actor_connections table for any known connections
    between actors already placed on the canvas.
    """
    engine = get_db_engine()

    with engine.connect() as conn:
        _ensure_board_exists(conn, board_id)

        # Get all actor nodes on the board with their entity_ids
        rows = conn.execute(
            text(
                "SELECT id, data, label FROM canvas_nodes"
                " WHERE board_id = :board_id AND node_type = 'actor'"
            ),
            {"board_id": board_id},
        ).fetchall()

        if len(rows) < 2:
            return {"suggestions": [], "message": "Need at least 2 actor nodes"}

        # Build mapping: entity_id -> canvas_node_id
        entity_to_canvas: dict[str, str] = {}
        for row in rows:
            rdata = row._mapping.get("data")
            if isinstance(rdata, str):
                try:
                    rdata = json.loads(rdata)
                except (json.JSONDecodeError, TypeError):
                    rdata = {}
            elif rdata is None:
                rdata = {}

            eid = rdata.get("entityId") or rdata.get("entity_id")
            if eid:
                entity_to_canvas[eid] = str(row._mapping["id"])

        if len(entity_to_canvas) < 2:
            return {"suggestions": [], "message": "Not enough actor entities resolved"}

        entity_list = list(entity_to_canvas.keys())

        # Get existing edges on the board to avoid suggesting duplicates
        existing_edges = conn.execute(
            text(
                "SELECT source_node_id, target_node_id FROM canvas_edges"
                " WHERE board_id = :board_id"
            ),
            {"board_id": board_id},
        ).fetchall()

        existing_pairs: set[tuple[str, str]] = set()
        for edge in existing_edges:
            em = edge._mapping
            existing_pairs.add((str(em["source_node_id"]), str(em["target_node_id"])))
            existing_pairs.add((str(em["target_node_id"]), str(em["source_node_id"])))

        # Query actor_connections for connections between our entities
        # We need to check all pairs — build a parameterized IN clause
        suggestions = []
        # Use a single query with ANY() to check connections between board actors
        rows = conn.execute(
            text(
                "SELECT actor_a, actor_b, relationship, strength"
                " FROM actor_connections"
                " WHERE actor_a = ANY(:ids) AND actor_b = ANY(:ids)"
            ),
            {"ids": entity_list},
        ).fetchall()

        for row in rows:
            rm = row._mapping
            actor_a = rm["actor_a"]
            actor_b = rm["actor_b"]

            canvas_a = entity_to_canvas.get(actor_a)
            canvas_b = entity_to_canvas.get(actor_b)

            if not canvas_a or not canvas_b:
                continue

            # Skip if edge already exists
            if (canvas_a, canvas_b) in existing_pairs:
                continue

            suggestions.append({
                "source_node_id": canvas_a,
                "target_node_id": canvas_b,
                "relationship": rm["relationship"],
                "strength": float(rm["strength"]) if rm["strength"] else 0.5,
                "edge_id": f"suggest-{uuid.uuid4().hex[:12]}",
            })
            # Mark as seen to avoid duplicates from bidirectional entries
            existing_pairs.add((canvas_a, canvas_b))
            existing_pairs.add((canvas_b, canvas_a))

    return {
        "suggestions": suggestions,
        "count": len(suggestions),
    }
