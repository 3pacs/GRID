"""Canvas sub-router: auto-investigate — build rich investigation boards from a search query.

Search "NVDA" → creates a board with NVIDIA as center node, expands to connected actors
(board members, major holders, lobbyists), pulls recent signals, flows, and kicks off
LLM research to find notable connections.

Search "Elon" → finds Elon Musk, expands to Tesla/SpaceX/X/Neuralink, connected politicians,
lobbying targets, insider trades, wealth flows.
"""

from __future__ import annotations

import json
import math
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from loguru import logger as log
from pydantic import BaseModel
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine
from api.routers.canvas_board_store import sync_board_from_legacy_canvas

router = APIRouter(tags=["canvas"])


class InvestigateRequest(BaseModel):
    query: str  # ticker, actor name, or topic
    depth: int = 2  # how many hops to expand (1=direct, 2=friends-of-friends)
    max_nodes: int = 30  # cap to keep the board readable
    include_signals: bool = True
    include_flows: bool = True
    llm_research: bool = True  # kick off LLM background research


class InvestigateResponse(BaseModel):
    board_id: str
    board_name: str
    nodes_created: int
    edges_created: int
    signals_found: int
    flows_found: int
    llm_research_started: bool


@router.post("/investigate")
async def auto_investigate(
    req: InvestigateRequest,
    engine=Depends(get_db_engine),
    _=Depends(require_auth),
) -> InvestigateResponse:
    """Auto-build an investigation board from a search query."""

    query = req.query.strip()
    if not query:
        raise HTTPException(400, "Query is required")

    with engine.begin() as conn:
        # 1. Find the seed entity — actor, company, or ticker
        seed_actors = _find_seed_actors(conn, query)
        if not seed_actors:
            raise HTTPException(404, f"No actors found for '{query}'")

        # 2. Create a new canvas board
        board_name = f"Investigation: {query}"
        board_row = conn.execute(
            text("""
                INSERT INTO canvas_boards (name, description, created_at, updated_at)
                VALUES (:name, :desc, NOW(), NOW())
                RETURNING id
            """),
            {"name": board_name, "desc": f"Auto-generated investigation for '{query}'"},
        ).fetchone()
        board_id = board_row._mapping["id"]

        # 3. Place seed actors as center nodes
        nodes_created = 0
        edges_created = 0
        placed_entities: dict[str, str] = {}  # entity_id → canvas_node_id

        center_x, center_y = 600.0, 400.0
        seed_positions = _circular_positions(center_x, center_y, len(seed_actors), radius=0)
        if len(seed_actors) > 1:
            seed_positions = _circular_positions(center_x, center_y, len(seed_actors), radius=200)

        for i, actor in enumerate(seed_actors[:5]):  # max 5 seeds
            px, py = seed_positions[i] if i < len(seed_positions) else (center_x + i * 250, center_y)
            nid = _place_actor_node(conn, board_id, actor, px, py)
            placed_entities[actor["id"]] = nid
            nodes_created += 1

        # 4. Expand: find connected actors (depth 1 and optionally 2)
        for depth_round in range(req.depth):
            current_entities = list(placed_entities.keys())
            for entity_id in current_entities:
                if nodes_created >= req.max_nodes:
                    break

                connections = _get_connections(conn, entity_id, limit=8 if depth_round == 0 else 4)
                source_nid = placed_entities[entity_id]

                for conn_actor in connections:
                    if nodes_created >= req.max_nodes:
                        break
                    if conn_actor["id"] in placed_entities:
                        # Just add edge if not exists
                        target_nid = placed_entities[conn_actor["id"]]
                        _place_edge(conn, board_id, source_nid, target_nid,
                                    conn_actor.get("relationship", "connected"),
                                    conn_actor.get("strength", 0.5))
                        edges_created += 1
                        continue

                    # Place new node in orbit around source
                    source_pos = _get_node_position(conn, board_id, source_nid)
                    orbit_pos = _orbit_position(source_pos[0], source_pos[1],
                                                nodes_created, len(connections))
                    nid = _place_actor_node(conn, board_id, conn_actor, orbit_pos[0], orbit_pos[1])
                    placed_entities[conn_actor["id"]] = nid
                    nodes_created += 1

                    _place_edge(conn, board_id, source_nid, nid,
                                conn_actor.get("relationship", "connected"),
                                conn_actor.get("strength", 0.5))
                    edges_created += 1

        # 5. Find and attach signals
        signals_found = 0
        if req.include_signals:
            signals_found = _attach_signals(conn, board_id, query, placed_entities, center_x, center_y)
            nodes_created += signals_found

        # 6. Find and attach wealth flows
        flows_found = 0
        if req.include_flows:
            flows_found = _attach_flows(conn, board_id, placed_entities)
            edges_created += flows_found

        # 7. Suggest connections between existing nodes
        _auto_suggest_edges(conn, board_id, placed_entities)
        sync_board_from_legacy_canvas(conn, str(board_id))

    # 8. Kick off LLM research in background
    llm_started = False
    if req.llm_research:
        llm_started = _start_llm_research(query, str(board_id), list(placed_entities.keys()))

    log.info(
        "Auto-investigate: query={q} board={b} nodes={n} edges={e} signals={s} flows={f}",
        q=query, b=board_id, n=nodes_created, e=edges_created, s=signals_found, f=flows_found,
    )

    return InvestigateResponse(
        board_id=str(board_id),
        board_name=board_name,
        nodes_created=nodes_created,
        edges_created=edges_created,
        signals_found=signals_found,
        flows_found=flows_found,
        llm_research_started=llm_started,
    )


# ── Helpers ──────────────────────────────────────────────────────────────


def _find_seed_actors(conn, query: str) -> list[dict]:
    """Find actors matching the query — by name, ticker, or category."""
    results = []

    # Try exact ticker match first (company)
    rows = conn.execute(
        text("""
            SELECT id, name, category, tier, influence_score, trust_score, net_worth_estimate
            FROM actors
            WHERE LOWER(name) LIKE :q_like
               OR id IN (SELECT id FROM actors WHERE LOWER(name) = :q_exact)
            ORDER BY influence_score DESC NULLS LAST
            LIMIT 10
        """),
        {"q_like": f"%{query.lower()}%", "q_exact": query.lower()},
    ).fetchall()

    for r in rows:
        m = r._mapping
        results.append({
            "id": m["id"],
            "name": m["name"],
            "category": m.get("category"),
            "tier": m.get("tier"),
            "influence_score": float(m["influence_score"]) if m.get("influence_score") else 0,
            "trust_score": float(m["trust_score"]) if m.get("trust_score") else None,
            "net_worth_estimate": float(m["net_worth_estimate"]) if m.get("net_worth_estimate") else None,
        })

    # Deduplicate by preferring higher influence
    seen = set()
    unique = []
    for r in sorted(results, key=lambda x: x["influence_score"], reverse=True):
        if r["id"] not in seen:
            seen.add(r["id"])
            unique.append(r)
    return unique[:5]


def _get_connections(conn, actor_id: str, limit: int = 8) -> list[dict]:
    """Get top connected actors from actor_connections."""
    rows = conn.execute(
        text("""
            SELECT connected_id, relationship, strength, name, category, tier, influence_score, trust_score
            FROM (
                SELECT ac.actor_b AS connected_id, ac.relationship, ac.strength,
                       a.name, a.category, a.tier, a.influence_score, a.trust_score
                FROM actor_connections ac
                JOIN actors a ON a.id = ac.actor_b
                WHERE ac.actor_a = :aid

                UNION ALL

                SELECT ac.actor_a AS connected_id, ac.relationship, ac.strength,
                       a.name, a.category, a.tier, a.influence_score, a.trust_score
                FROM actor_connections ac
                JOIN actors a ON a.id = ac.actor_a
                WHERE ac.actor_b = :aid
            ) sub
            ORDER BY strength DESC NULLS LAST, influence_score DESC NULLS LAST
            LIMIT :lim
        """),
        {"aid": actor_id, "lim": limit},
    ).fetchall()

    results = []
    seen = set()
    for r in rows:
        m = r._mapping
        cid = m["connected_id"]
        if cid in seen or cid == actor_id:
            continue
        seen.add(cid)
        results.append({
            "id": cid,
            "name": m["name"],
            "category": m.get("category"),
            "tier": m.get("tier"),
            "influence_score": float(m["influence_score"]) if m.get("influence_score") else 0,
            "trust_score": float(m["trust_score"]) if m.get("trust_score") else None,
            "relationship": m.get("relationship", "connected"),
            "strength": float(m["strength"]) if m.get("strength") else 0.5,
        })
    return results[:limit]


def _place_actor_node(conn, board_id: int, actor: dict, px: float, py: float) -> str:
    """Insert an actor node onto the canvas board. Returns canvas node_id."""
    nid = f"actor-{actor['id'][:20]}-{uuid.uuid4().hex[:6]}"
    data = {
        "entityId": actor["id"],
        "category": actor.get("category", ""),
        "tier": actor.get("tier", ""),
        "influence_score": actor.get("influence_score"),
        "trust_score": actor.get("trust_score"),
        "net_worth_estimate": actor.get("net_worth_estimate"),
    }
    conn.execute(
        text("""
            INSERT INTO canvas_nodes (node_id, board_id, node_type, label, position_x, position_y, data)
            VALUES (:nid, :bid, 'actor', :label, :px, :py, :data)
        """),
        {"nid": nid, "bid": board_id, "label": actor["name"], "px": px, "py": py,
         "data": json.dumps(data)},
    )
    return nid


def _place_edge(conn, board_id: int, source_nid: str, target_nid: str,
                relationship: str = "connected", strength: float = 0.5) -> str:
    """Insert an edge between two canvas nodes."""
    eid = f"edge-{uuid.uuid4().hex[:10]}"
    conn.execute(
        text("""
            INSERT INTO canvas_edges (id, board_id, source_node_id, target_node_id, edge_type, label, data)
            VALUES (:eid, :bid, :src, :tgt, 'smoothstep', :label, :data)
        """),
        {"eid": eid, "bid": board_id, "src": source_nid, "tgt": target_nid,
         "label": relationship,
         "data": json.dumps({"strength": strength})},
    )
    return eid


def _get_node_position(conn, board_id: int, node_id: str) -> tuple[float, float]:
    """Get a node's position."""
    row = conn.execute(
        text("SELECT position_x, position_y FROM canvas_nodes WHERE node_id = :nid AND board_id = :bid"),
        {"nid": node_id, "bid": board_id},
    ).fetchone()
    if row:
        return (float(row._mapping["position_x"] or 600), float(row._mapping["position_y"] or 400))
    return (600.0, 400.0)


def _attach_signals(conn, board_id: int, query: str, placed_entities: dict, cx: float, cy: float) -> int:
    """Find recent signals related to the query and place them as signal nodes."""
    rows = conn.execute(
        text("""
            SELECT id, signal_type, ticker, actor, direction, magnitude, description,
                   signal_date, confidence
            FROM signal_data
            WHERE (LOWER(ticker) = :ticker OR LOWER(actor) LIKE :actor_like
                   OR LOWER(description) LIKE :desc_like)
              AND signal_type NOT IN ('whale_flow', 'social')
            ORDER BY signal_date DESC NULLS LAST
            LIMIT 10
        """),
        {"ticker": query.upper(), "actor_like": f"%{query.lower()}%",
         "desc_like": f"%{query.lower()}%"},
    ).fetchall()

    count = 0
    for i, r in enumerate(rows):
        m = r._mapping
        nid = f"signal-{m['id']}-{uuid.uuid4().hex[:4]}"
        label = f"{m.get('signal_type','signal')}: {m.get('ticker','')}"
        if m.get("direction"):
            label += f" ({m['direction']})"

        px = cx + 500 + (i % 3) * 200
        py = cy - 200 + (i // 3) * 150

        data = {
            "signal_id": str(m["id"]),
            "signal_type": m.get("signal_type"),
            "ticker": m.get("ticker"),
            "actor": m.get("actor"),
            "direction": m.get("direction"),
            "magnitude": float(m["magnitude"]) if m.get("magnitude") else None,
            "description": m.get("description", "")[:200],
            "signal_date": str(m.get("signal_date", "")),
            "confidence": m.get("confidence"),
        }

        conn.execute(
            text("""
                INSERT INTO canvas_nodes (node_id, board_id, node_type, label, position_x, position_y, data)
                VALUES (:nid, :bid, 'signal', :label, :px, :py, :data)
            """),
            {"nid": nid, "bid": board_id, "label": label, "px": px, "py": py,
             "data": json.dumps(data)},
        )
        count += 1

        # Link signal to matching actor if on board
        actor_name = (m.get("actor") or "").lower()
        for entity_id, canvas_nid in placed_entities.items():
            # Simple name match
            if actor_name and actor_name in entity_id.lower():
                _place_edge(conn, board_id, canvas_nid, nid, "signal", 0.8)
                break

    return count


def _attach_flows(conn, board_id: int, placed_entities: dict) -> int:
    """Find wealth flows between placed actors and add as flow edges."""
    entity_ids = list(placed_entities.keys())
    if len(entity_ids) < 2:
        return 0

    rows = conn.execute(
        text("""
            SELECT wf.from_actor, wf.to_entity, wf.amount_estimate, wf.confidence
            FROM wealth_flows wf
            WHERE wf.from_actor = ANY(:ids) OR wf.to_entity = ANY(:ids)
            LIMIT 20
        """),
        {"ids": entity_ids},
    ).fetchall()

    count = 0
    for r in rows:
        m = r._mapping
        from_nid = placed_entities.get(m["from_actor"])
        to_nid = placed_entities.get(m["to_entity"])
        if from_nid and to_nid and from_nid != to_nid:
            amount = float(m["amount_estimate"]) if m.get("amount_estimate") else 0
            label = f"${amount / 1e6:.1f}M" if amount > 0 else "flow"
            _place_edge(conn, board_id, from_nid, to_nid, label, 0.9)
            count += 1
    return count


def _auto_suggest_edges(conn, board_id: int, placed_entities: dict) -> int:
    """Add edges between board actors that have known connections."""
    entity_ids = list(placed_entities.keys())
    if len(entity_ids) < 2:
        return 0

    # Get existing edges
    existing = set()
    rows = conn.execute(
        text("SELECT source_node_id, target_node_id FROM canvas_edges WHERE board_id = :bid"),
        {"bid": board_id},
    ).fetchall()
    for r in rows:
        m = r._mapping
        existing.add((m["source_node_id"], m["target_node_id"]))
        existing.add((m["target_node_id"], m["source_node_id"]))

    # Find connections
    rows = conn.execute(
        text("""
            SELECT actor_a, actor_b, relationship, strength
            FROM actor_connections
            WHERE actor_a = ANY(:ids) AND actor_b = ANY(:ids)
            LIMIT 50
        """),
        {"ids": entity_ids},
    ).fetchall()

    count = 0
    for r in rows:
        m = r._mapping
        nid_a = placed_entities.get(m["actor_a"])
        nid_b = placed_entities.get(m["actor_b"])
        if nid_a and nid_b and (nid_a, nid_b) not in existing:
            _place_edge(conn, board_id, nid_a, nid_b,
                        m.get("relationship", "connected"),
                        float(m["strength"]) if m.get("strength") else 0.5)
            existing.add((nid_a, nid_b))
            count += 1
    return count


def _start_llm_research(query: str, board_id: str, entity_ids: list[str]) -> bool:
    """Kick off background LLM research on the investigation topic."""
    try:
        from events.producer import emit
        emit("canvas", {
            "event_type": "investigation_started",
            "query": query,
            "board_id": board_id,
            "entity_count": len(entity_ids),
        })
        return True
    except Exception as e:
        log.warning(f"LLM research kickoff failed: {e}")
        return False


def _circular_positions(cx: float, cy: float, count: int, radius: float = 250) -> list[tuple[float, float]]:
    """Generate positions in a circle around (cx, cy)."""
    if count <= 1:
        return [(cx, cy)]
    positions = []
    for i in range(count):
        angle = (2 * math.pi * i / count) - math.pi / 2
        x = cx + radius * math.cos(angle)
        y = cy + radius * math.sin(angle)
        positions.append((x, y))
    return positions


def _orbit_position(cx: float, cy: float, index: int, total: int) -> tuple[float, float]:
    """Position a node in orbit around a parent."""
    radius = 180 + (index % 3) * 40
    angle = (2 * math.pi * index / max(total, 1)) - math.pi / 2
    return (cx + radius * math.cos(angle), cy + radius * math.sin(angle))
