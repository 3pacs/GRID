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


def _resolve_entity_by_name(conn: Connection, name: str) -> str | None:
    """Try to resolve an entity ID from the actors table by name."""
    row = conn.execute(
        text("SELECT id FROM actors WHERE name ILIKE :name LIMIT 1"),
        {"name": name},
    ).fetchone()
    return str(row[0]) if row else None


def _get_neighbors_from_db(conn: Connection, entity_id: str, entity_name: str = "", limit: int = 8) -> list[dict]:
    """Get connected actors from actor_connections table (replaces in-memory graph).

    Tries entity_id first; if no results, tries all matching IDs for the entity name.
    """
    # Direct ID match
    rows = conn.execute(
        text("""
            (SELECT actor_b AS neighbor, relationship, strength
             FROM actor_connections WHERE actor_a = :eid ORDER BY strength DESC LIMIT :lim)
            UNION
            (SELECT actor_a AS neighbor, relationship, strength
             FROM actor_connections WHERE actor_b = :eid ORDER BY strength DESC LIMIT :lim)
        """),
        {"eid": entity_id, "lim": limit},
    ).fetchall()

    if rows:
        return [dict(r._mapping) for r in rows]

    # Fall back: find all actor IDs matching this name, then look up connections
    if entity_name:
        name_ids = conn.execute(
            text("SELECT id FROM actors WHERE name ILIKE :name"),
            {"name": entity_name},
        ).fetchall()
        all_ids = [str(r[0]) for r in name_ids] + [entity_id]

        rows = conn.execute(
            text("""
                (SELECT actor_b AS neighbor, relationship, strength
                 FROM actor_connections WHERE actor_a = ANY(:ids) ORDER BY strength DESC LIMIT :lim)
                UNION
                (SELECT actor_a AS neighbor, relationship, strength
                 FROM actor_connections WHERE actor_b = ANY(:ids) ORDER BY strength DESC LIMIT :lim)
            """),
            {"ids": all_ids, "lim": limit},
        ).fetchall()
        return [dict(r._mapping) for r in rows]

    return []


def _get_actor_details(conn: Connection, actor_id: str) -> dict:
    """Get actor details from the actors table."""
    row = conn.execute(
        text("SELECT id, name, category FROM actors WHERE id = :aid LIMIT 1"),
        {"aid": actor_id},
    ).fetchone()
    if row is None:
        return {"name": actor_id, "category": ""}
    m = row._mapping
    return {"name": m["name"], "category": m["category"] or ""}


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

    with engine.begin() as conn:
        _ensure_board_exists(conn, board_id)

        # Get the source node to find entity_id and position
        source_row = conn.execute(
            text(
                "SELECT id, node_id, node_type, label, position_x, position_y, data"
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

        if node_type not in ("actor", "company", "signal", "news", "hypothesis"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cannot expand node of type '{node_type}'",
            )

        # Determine the actor_id to look up
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
            # Fall back: try to resolve by label from actors table
            entity_id = _resolve_entity_by_name(conn, source.get("label", ""))

        if not entity_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Actor not found for node '{node_id}' — no entityId and name not in actors table",
            )

        # Get existing entity IDs on this board to avoid duplicates
        existing_rows = conn.execute(
            text("SELECT node_id, data FROM canvas_nodes WHERE board_id = :board_id"),
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
                existing_entity_ids.add(str(eid))

        label = source.get("label", "")
        ticker = source_data.get("ticker") or ""

        # ── Resolve ticker from company_profiles or label ──────────────
        if not ticker:
            # Actor name might be a company — check company_profiles
            cp_row = conn.execute(
                text("SELECT ticker FROM company_profiles WHERE name ILIKE :name LIMIT 1"),
                {"name": label},
            ).fetchone()
            if cp_row:
                ticker = cp_row[0]

        center_x = float(source.get("position_x") or 0)
        center_y = float(source.get("position_y") or 0)

        new_nodes = []
        new_edges = []

        def _insert_node(nid, ntype, nlabel, px, py, data_dict):
            row = conn.execute(
                text(
                    "INSERT INTO canvas_nodes (node_id, board_id, node_type, label, position_x, position_y, data)"
                    " VALUES (:node_id, :board_id, :node_type, :label, :position_x, :position_y, :data)"
                    " RETURNING *"
                ),
                {"node_id": nid, "board_id": board_id, "node_type": ntype,
                 "label": nlabel, "position_x": px, "position_y": py,
                 "data": json.dumps(data_dict)},
            ).fetchone()
            new_nodes.append(_row_to_dict(row))
            return nid

        def _insert_edge(src, tgt, elabel, edata=None):
            eid = f"edge-{uuid.uuid4().hex[:12]}"
            row = conn.execute(
                text(
                    "INSERT INTO canvas_edges (edge_id, board_id, source_node_id, target_node_id, edge_type, label, data)"
                    " VALUES (:edge_id, :board_id, :source_node_id, :target_node_id, :edge_type, :label, :data)"
                    " RETURNING *"
                ),
                {"edge_id": eid, "board_id": board_id, "source_node_id": src,
                 "target_node_id": tgt, "edge_type": "smoothstep",
                 "label": elabel, "data": json.dumps(edata) if edata else None},
            ).fetchone()
            new_edges.append(_row_to_dict(row))

        # ── 1. Actor connections (max 6) ──────────────────────────────
        neighbors = _get_neighbors_from_db(conn, entity_id, entity_name=label, limit=6)
        actor_neighbors = [n for n in neighbors if str(n["neighbor"]) not in existing_entity_ids]
        actor_positions = _circular_positions(center_x, center_y, len(actor_neighbors), radius=220)

        for i, nbr in enumerate(actor_neighbors):
            nid_str = str(nbr["neighbor"])
            actor_data = _get_actor_details(conn, nid_str)
            cnid = f"actor-{nid_str}-{uuid.uuid4().hex[:6]}"
            px, py = actor_positions[i]
            _insert_node(cnid, "actor", actor_data.get("name", nid_str), px, py,
                         {"entityId": nid_str, "category": actor_data.get("category", "")})
            _insert_edge(node_id, cnid, nbr.get("relationship") or "",
                         {"strength": float(nbr["strength"]) if nbr.get("strength") else 0.5})

        # ── 2. Signals (max 5 recent) ─────────────────────────────────
        # Match by ticker, actor name, or entity ID
        match_terms = [t for t in [ticker, label, entity_id] if t]
        if match_terms:
            sig_rows = conn.execute(
                text("""
                    SELECT id, signal_type, signal_date, ticker, actor, direction,
                           magnitude, description, confidence
                    FROM signal_data
                    WHERE (ticker = ANY(:terms) OR actor = ANY(:terms))
                    ORDER BY signal_date DESC NULLS LAST
                    LIMIT 5
                """),
                {"terms": match_terms},
            ).fetchall()

            sig_positions = _circular_positions(center_x + 300, center_y - 100, len(sig_rows), radius=120)
            for i, sr in enumerate(sig_rows):
                m = sr._mapping
                sid = f"signal-{m['id']}-{uuid.uuid4().hex[:6]}"
                px, py = sig_positions[i] if i < len(sig_positions) else (center_x + 350, center_y + i * 70)
                sig_label = m["description"] or f"{m['signal_type']}: {m['ticker'] or ''}"
                if len(sig_label) > 80:
                    sig_label = sig_label[:77] + "..."
                _insert_node(sid, "signal", sig_label, px, py, {
                    "signal_type": m["signal_type"], "ticker": m["ticker"],
                    "direction": m["direction"], "magnitude": m["magnitude"],
                    "confidence": m["confidence"],
                })
                _insert_edge(node_id, sid, m["signal_type"] or "signal")

        # ── 3. Oracle predictions (max 3) ─────────────────────────────
        if ticker:
            pred_rows = conn.execute(
                text("""
                    SELECT id, ticker, direction, confidence, expected_move_pct,
                           target_price, entry_price, verdict, model_name, created_at
                    FROM oracle_predictions
                    WHERE ticker = :t
                    ORDER BY created_at DESC
                    LIMIT 3
                """),
                {"t": ticker},
            ).fetchall()

            for i, pr in enumerate(pred_rows):
                pm = pr._mapping
                pid = f"prediction-{pm['id']}-{uuid.uuid4().hex[:6]}"
                px = center_x - 280
                py = center_y - 100 + i * 90
                verdict = pm["verdict"] or pm["direction"] or "?"
                conf = f"{float(pm['confidence'])*100:.0f}%" if pm["confidence"] else ""
                plabel = f"Oracle: {pm['ticker']} {verdict} {conf}"
                _insert_node(pid, "hypothesis", plabel, px, py, {
                    "ticker": pm["ticker"], "direction": pm["direction"],
                    "confidence": pm["confidence"],
                    "status": pm["verdict"] or "pending",
                    "role": "thesis",
                    "expected_move_pct": str(pm["expected_move_pct"]) if pm["expected_move_pct"] else None,
                    "model": pm["model_name"],
                })
                _insert_edge(node_id, pid, "prediction")

        # ── 4. Hypotheses (max 3 active) ──────────────────────────────
        hyp_rows = conn.execute(
            text("""
                SELECT id, thesis, confidence, status, role, pattern_type, kill_reason
                FROM discovered_hypotheses
                WHERE (thesis ILIKE :pct_name OR thesis ILIKE :pct_ticker)
                AND status IN ('active', 'confirmed')
                ORDER BY confidence DESC NULLS LAST
                LIMIT 3
            """),
            {"pct_name": f"%{label}%", "pct_ticker": f"%{ticker}%" if ticker else "%__NOMATCH__%"},
        ).fetchall()

        for i, hr in enumerate(hyp_rows):
            hm = hr._mapping
            hid = f"hyp-{hm['id']}-{uuid.uuid4().hex[:6]}"
            px = center_x - 200
            py = center_y + 150 + i * 90
            hlabel = hm["thesis"]
            if len(hlabel) > 100:
                hlabel = hlabel[:97] + "..."
            _insert_node(hid, "hypothesis", hlabel, px, py, {
                "confidence": hm["confidence"], "status": hm["status"],
                "role": hm["role"] or "thesis",
                "pattern_type": hm["pattern_type"],
                "kill_reason": hm["kill_reason"],
            })
            _insert_edge(node_id, hid, "hypothesis")

        # ── 5. Wealth flows (max 4) ──────────────────────────────────
        flow_rows = conn.execute(
            text("""
                SELECT id, from_actor, to_entity, amount_estimate, confidence, implication
                FROM wealth_flows
                WHERE from_actor ILIKE :pct_name OR to_entity ILIKE :pct_name
                ORDER BY amount_estimate DESC NULLS LAST
                LIMIT 4
            """),
            {"pct_name": f"%{label}%"},
        ).fetchall()

        for i, fr in enumerate(flow_rows):
            fm = fr._mapping
            fid = f"flow-{fm['id']}-{uuid.uuid4().hex[:6]}"
            px = center_x + 200
            py = center_y + 150 + i * 70
            amt = float(fm["amount_estimate"]) if fm["amount_estimate"] else 0
            amt_str = f"${amt/1e6:.1f}M" if amt > 1e6 else f"${amt/1e3:.0f}K" if amt > 1e3 else f"${amt:.0f}"
            flow_label = f"{amt_str} → {fm['to_entity']}" if fm["from_actor"] and label.lower() in fm["from_actor"].lower() else f"{fm['from_actor']} → {amt_str}"
            _insert_node(fid, "evidence", flow_label, px, py, {
                "evidence_type": "wealth_flow",
                "confidence": fm["confidence"] or "estimated",
                "content": fm["implication"] or f"Flow: {fm['from_actor']} → {fm['to_entity']}",
                "amount": str(amt),
            })
            _insert_edge(node_id, fid, amt_str, {"strength": min(amt / 1e9, 1.0) if amt else 0.1})

        # ── 6. Company profile (1 if exists) ─────────────────────────
        if ticker:
            cp_row = conn.execute(
                text("SELECT ticker, name, sector, suspicion_score, narrative FROM company_profiles WHERE ticker = :t LIMIT 1"),
                {"t": ticker},
            ).fetchone()
            if cp_row:
                cm = cp_row._mapping
                cpid = f"company-{cm['ticker']}-{uuid.uuid4().hex[:6]}"
                px = center_x
                py = center_y - 220
                _insert_node(cpid, "company", cm["name"] or cm["ticker"], px, py, {
                    "ticker": cm["ticker"], "sector": cm["sector"],
                    "suspicion_score": float(cm["suspicion_score"]) if cm["suspicion_score"] else None,
                    "entityId": cm["ticker"],
                })
                _insert_edge(node_id, cpid, cm["sector"] or "company")

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
                eid = _resolve_entity_by_name(conn, node.get("label", ""))
            if not eid:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Node '{nid}' has no actor entity mapping",
                )
            entity_ids[nid] = eid

    source_eid = entity_ids[body.source_node_id]
    target_eid = entity_ids[body.target_node_id]

    # Simple 2-hop path via DB (not full shortest path but covers most cases)
    with engine.connect() as conn:
        # Direct connection?
        direct = conn.execute(
            text("SELECT relationship, strength FROM actor_connections WHERE (actor_a = :s AND actor_b = :t) OR (actor_a = :t AND actor_b = :s) LIMIT 1"),
            {"s": source_eid, "t": target_eid},
        ).fetchone()

        if direct:
            src_details = _get_actor_details(conn, source_eid)
            tgt_details = _get_actor_details(conn, target_eid)
            return {
                "path": [
                    {"actor_id": source_eid, "name": src_details["name"], "category": src_details["category"], "degree": 0},
                    {"actor_id": target_eid, "name": tgt_details["name"], "category": tgt_details["category"], "degree": 1,
                     "connection": {"relationship": direct._mapping["relationship"], "strength": float(direct._mapping["strength"] or 0.5)}},
                ],
                "degrees": 1,
                "source": body.source_node_id,
                "target": body.target_node_id,
            }

    return {"path": None, "degrees": -1, "message": "No direct connection found (2-hop search not yet implemented)"}


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
                "SELECT node_id, data, label FROM canvas_nodes"
                " WHERE board_id = :board_id AND node_type IN ('actor', 'company')"
            ),
            {"board_id": board_id},
        ).fetchall()

        if len(rows) < 2:
            return {"suggestions": [], "message": "Need at least 2 actor nodes"}

        # Build mapping: entity_id -> canvas_node_id
        # Also build name->canvas mapping for fallback
        entity_to_canvas: dict[str, str] = {}
        for row in rows:
            rm = row._mapping
            rdata = rm.get("data")
            if isinstance(rdata, str):
                try:
                    rdata = json.loads(rdata)
                except (json.JSONDecodeError, TypeError):
                    rdata = {}
            elif rdata is None:
                rdata = {}

            canvas_nid = str(rm["node_id"])
            eid = rdata.get("entityId") or rdata.get("entity_id")
            if eid:
                entity_to_canvas[str(eid)] = canvas_nid

            # Also resolve all IDs for this actor name from the actors table
            label = rm.get("label")
            if label:
                name_rows = conn.execute(
                    text("SELECT id FROM actors WHERE name ILIKE :name"),
                    {"name": label},
                ).fetchall()
                for nr in name_rows:
                    entity_to_canvas[str(nr[0])] = canvas_nid

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
