"""Canvas sub-router: graph expansion — expand network, path finding, suggest connections."""

from __future__ import annotations

import json
import math
import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from loguru import logger as log
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.engine import Connection

from api.auth import require_auth
from api.dependencies import get_db_engine
from api.routers.canvas_board_store import (
    sync_board_from_legacy_canvas,
    sync_legacy_canvas_from_board,
)

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
    sync_legacy_canvas_from_board(conn, board_id)
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
    sync_board_from_legacy_canvas(conn, board_id)


def _resolve_canonical_id(conn: Connection, entity_id: str) -> str:
    """Resolve any actor ID to its canonical form via actor_id_map.

    Returns the canonical_id if mapped, otherwise the input ID unchanged.
    """
    row = conn.execute(
        text("SELECT canonical_id FROM actor_id_map WHERE alias_id = :eid LIMIT 1"),
        {"eid": entity_id},
    ).fetchone()
    return str(row[0]) if row else entity_id


def _resolve_entity_by_name(conn: Connection, name: str) -> str | None:
    """Resolve an actor name to its canonical ID via actor_id_map + actors table."""
    # First try: find an actor by name, then map to canonical
    row = conn.execute(
        text("""
            SELECT m.canonical_id FROM actors a
            JOIN actor_id_map m ON m.alias_id = a.id
            WHERE a.name ILIKE :name
            ORDER BY m.confidence DESC
            LIMIT 1
        """),
        {"name": name},
    ).fetchone()
    if row:
        return str(row[0])
    # Fallback: just find any actor by name
    row = conn.execute(
        text("SELECT id FROM actors WHERE name ILIKE :name LIMIT 1"),
        {"name": name},
    ).fetchone()
    return str(row[0]) if row else None


def _get_all_ids_for_entity(conn: Connection, entity_id: str, entity_name: str = "") -> list[str]:
    """Get all known IDs (canonical + aliases) for an entity, for querying connections."""
    ids = set()
    canonical = _resolve_canonical_id(conn, entity_id)
    ids.add(canonical)
    ids.add(entity_id)

    # All aliases that share the same canonical ID
    rows = conn.execute(
        text("SELECT alias_id FROM actor_id_map WHERE canonical_id = :cid"),
        {"cid": canonical},
    ).fetchall()
    for r in rows:
        ids.add(str(r[0]))

    # Also resolve by name if provided
    if entity_name:
        name_rows = conn.execute(
            text("SELECT id FROM actors WHERE name ILIKE :name"),
            {"name": entity_name},
        ).fetchall()
        for r in name_rows:
            ids.add(str(r[0]))
            # And their canonical mappings
            cid = _resolve_canonical_id(conn, str(r[0]))
            ids.add(cid)

    return list(ids)


def _get_neighbors_from_db(conn: Connection, entity_id: str, entity_name: str = "", limit: int = 8) -> list[dict]:
    """Get connected actors from actor_connections via canonical ID resolution."""
    all_ids = _get_all_ids_for_entity(conn, entity_id, entity_name)

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


def _get_actor_details(conn: Connection, actor_id: str) -> dict:
    """Get actor details from the actors table, trying canonical ID first."""
    row = conn.execute(
        text("SELECT id, name, category FROM actors WHERE id = :aid LIMIT 1"),
        {"aid": actor_id},
    ).fetchone()
    if row is None:
        canonical = _resolve_canonical_id(conn, actor_id)
        if canonical != actor_id:
            row = conn.execute(
                text("SELECT id, name, category FROM actors WHERE id = :aid LIMIT 1"),
                {"aid": canonical},
            ).fetchone()
    if row is None:
        return {"name": actor_id, "category": ""}
    m = row._mapping
    return {"name": m["name"], "category": m["category"] or ""}


def _get_lever_puller_data(conn: Connection, name: str) -> dict | None:
    """Check if an actor is a lever puller and return their profile."""
    row = conn.execute(
        text("""
            SELECT name, category, position, influence_rank, trust_score,
                   motivation_model, total_signals, correct_signals,
                   avg_lead_time_days, metadata
            FROM lever_pullers
            WHERE name ILIKE :name
            ORDER BY trust_score * influence_rank DESC
            LIMIT 1
        """),
        {"name": name},
    ).fetchone()
    if row is None:
        return None
    m = row._mapping
    accuracy = (float(m["correct_signals"]) / float(m["total_signals"]) * 100) if m["total_signals"] and int(m["total_signals"]) > 0 else 0
    return {
        "is_lever_puller": True,
        "lever_category": m["category"],
        "lever_position": m["position"],
        "influence_rank": float(m["influence_rank"]) if m["influence_rank"] else 0.5,
        "trust_score": float(m["trust_score"]) if m["trust_score"] else 0.5,
        "motivation_model": m["motivation_model"],
        "total_signals": int(m["total_signals"] or 0),
        "correct_signals": int(m["correct_signals"] or 0),
        "accuracy_pct": round(accuracy, 1),
        "avg_lead_time_days": float(m["avg_lead_time_days"]) if m["avg_lead_time_days"] else None,
    }


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
    depth: int = Query(1, ge=1, le=3),
    _token: str = Depends(require_auth),
) -> dict:
    """Expand a node with tiered intelligence depth.

    depth=1: Core connections (4 actors) + top signals (3) + company + top hypothesis
    depth=2: + insider trades, congressional trades, lever pullers, oracle predictions
    depth=3: + cross-reference reality checks, investigation leads, entity_relationships
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

        # ── Build board entity index (dedup + cross-connect) ─────────
        # Maps: entityId→node_id, label(lower)→node_id, ticker→node_id
        existing_rows = conn.execute(
            text("SELECT node_id, node_type, label, data FROM canvas_nodes WHERE board_id = :board_id"),
            {"board_id": board_id},
        ).fetchall()

        board_index: dict[str, str] = {}  # lookup_key → canvas node_id
        existing_edge_pairs: set[tuple[str, str]] = set()

        for erow in existing_rows:
            em = erow._mapping
            enid = str(em["node_id"])
            edata = em.get("data")
            if isinstance(edata, str):
                try:
                    edata = json.loads(edata)
                except (json.JSONDecodeError, TypeError):
                    edata = {}
            elif edata is None:
                edata = {}

            # Index by every identifier we can extract
            elabel = (em.get("label") or "").strip().lower()
            eid = edata.get("entityId") or edata.get("entity_id") or ""
            eticker = edata.get("ticker") or ""

            if eid:
                board_index[str(eid).lower()] = enid
            if elabel:
                board_index[elabel] = enid
            if eticker:
                board_index[eticker.lower()] = enid

        # Index existing edges to avoid duplicate edges
        edge_rows = conn.execute(
            text("SELECT source_node_id, target_node_id FROM canvas_edges WHERE board_id = :board_id"),
            {"board_id": board_id},
        ).fetchall()
        for er in edge_rows:
            erm = er._mapping
            existing_edge_pairs.add((str(erm["source_node_id"]), str(erm["target_node_id"])))
            existing_edge_pairs.add((str(erm["target_node_id"]), str(erm["source_node_id"])))

        def _find_existing(entity_id_or_name: str, ticker: str = "", label_str: str = "") -> str | None:
            """Check if an entity is already on the board. Returns canvas node_id or None."""
            for key in [entity_id_or_name, ticker, label_str]:
                if key and key.strip().lower() in board_index:
                    return board_index[key.strip().lower()]
            return None

        label = source.get("label", "")
        ticker = source_data.get("ticker") or ""

        if not ticker:
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
            """Insert a node, or return existing node_id if entity already on board."""
            eid = data_dict.get("entityId") or data_dict.get("entity_id") or ""
            eticker = data_dict.get("ticker") or ""
            existing = _find_existing(eid, eticker, nlabel)
            if existing:
                return existing  # Don't create duplicate — return existing node_id

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
            # Register in index so subsequent inserts find this node
            if eid:
                board_index[str(eid).lower()] = nid
            if nlabel:
                board_index[nlabel.strip().lower()] = nid
            if eticker:
                board_index[eticker.lower()] = nid
            return nid

        def _insert_edge(src, tgt, elabel, edata=None):
            if src == tgt:
                return  # No self-loops
            pair = (str(src), str(tgt))
            if pair in existing_edge_pairs:
                return  # Edge already exists
            existing_edge_pairs.add(pair)
            existing_edge_pairs.add((str(tgt), str(src)))
            eid = f"edge-{uuid.uuid4().hex[:12]}"
            row = conn.execute(
                text(
                    "INSERT INTO canvas_edges (id, board_id, source_node_id, target_node_id, edge_type, label, data)"
                    " VALUES (:edge_id, :board_id, :source_node_id, :target_node_id, :edge_type, :label, :data)"
                    " RETURNING *"
                ),
                {"edge_id": eid, "board_id": board_id, "source_node_id": src,
                 "target_node_id": tgt, "edge_type": "smoothstep",
                 "label": elabel, "data": json.dumps(edata) if edata else None},
            ).fetchone()
            new_edges.append(_row_to_dict(row))

        # ── Layout: dynamic positioning based on board state ─────────
        # Find the bounding box of existing nodes to place new ones outside
        all_board_nodes = conn.execute(
            text("SELECT position_x, position_y FROM canvas_nodes WHERE board_id = :bid"),
            {"bid": board_id},
        ).fetchall()
        if all_board_nodes:
            all_xs = [float(r[0] or 0) for r in all_board_nodes]
            all_ys = [float(r[1] or 0) for r in all_board_nodes]
            board_min_x, board_max_x = min(all_xs), max(all_xs)
            board_min_y, board_max_y = min(all_ys), max(all_ys)
            board_span_x = board_max_x - board_min_x
            board_span_y = board_max_y - board_min_y
        else:
            board_span_x = board_span_y = 0

        # Scale radius based on how many nodes are already on the board
        # More nodes → push new ones further out
        n_existing = len(all_board_nodes)
        R_BASE = 350 + n_existing * 30  # grows with board density
        R_ACTOR = R_BASE
        R_SIGNAL = R_BASE + 150
        R_FLOW = R_BASE + 100

        match_terms = [t for t in [ticker, label, entity_id] if t]
        pct_name = f"%{label}%"
        pct_ticker = f"%{ticker}%" if ticker else "%__NOMATCH__%"

        # ══════════════════════════════════════════════════════════════
        # DEPTH 1: Core intelligence (clean, ~10-12 nodes max)
        # ══════════════════════════════════════════════════════════════

        # ── 1a. Actor connections (4 strongest) ──────────────────────
        neighbors = _get_neighbors_from_db(conn, entity_id, entity_name=label, limit=4)
        actor_neighbors = neighbors  # dedup handled inside _insert_node
        actor_positions = _circular_positions(center_x, center_y, len(actor_neighbors), radius=R_ACTOR)

        for i, nbr in enumerate(actor_neighbors):
            nid_str = str(nbr["neighbor"])
            actor_data = _get_actor_details(conn, nid_str)
            cnid = f"actor-{nid_str}-{uuid.uuid4().hex[:6]}"
            px, py = actor_positions[i]
            node_payload = {"entityId": nid_str, "category": actor_data.get("category", "")}
            lever = _get_lever_puller_data(conn, actor_data.get("name", nid_str))
            if lever:
                node_payload.update(lever)
            _insert_node(cnid, "actor", actor_data.get("name", nid_str), px, py, node_payload)
            edge_label = nbr.get("relationship") or ""
            if lever:
                edge_label = f"{edge_label} [LEVER]" if edge_label else "LEVER PULLER"
            _insert_edge(node_id, cnid, edge_label,
                         {"strength": float(nbr["strength"]) if nbr.get("strength") else 0.5})

        # ── 1b. Top 3 signals ────────────────────────────────────────
        if match_terms:
            sig_rows = conn.execute(
                text("""
                    SELECT id, signal_type, signal_date, ticker, actor, direction,
                           magnitude, description, confidence
                    FROM signal_data
                    WHERE (ticker = ANY(:terms) OR actor = ANY(:terms))
                    ORDER BY signal_date DESC NULLS LAST LIMIT 3
                """),
                {"terms": match_terms},
            ).fetchall()

            for i, sr in enumerate(sig_rows):
                m = sr._mapping
                sid = f"signal-{m['id']}-{uuid.uuid4().hex[:6]}"
                px = center_x + R_SIGNAL
                py = center_y - 200 + i * 110
                sig_label = m["description"] or f"{m['signal_type']}: {m['ticker'] or ''}"
                if len(sig_label) > 80:
                    sig_label = sig_label[:77] + "..."
                _insert_node(sid, "signal", sig_label, px, py, {
                    "signal_type": m["signal_type"], "ticker": m["ticker"],
                    "direction": m["direction"], "magnitude": m["magnitude"],
                    "confidence": m["confidence"],
                })
                _insert_edge(node_id, sid, m["signal_type"] or "signal")
                # Cross-connect: link signal to other board entities it mentions
                for xkey in [m["ticker"], m["actor"]]:
                    if xkey:
                        xnode = _find_existing(xkey, xkey, xkey)
                        if xnode and xnode != node_id and xnode != sid:
                            _insert_edge(sid, xnode, m["signal_type"] or "mentions")

        # ── 1c. Top hypothesis ───────────────────────────────────────
        hyp_row = conn.execute(
            text("""
                SELECT id, thesis, confidence, status, role, pattern_type, kill_reason
                FROM discovered_hypotheses
                WHERE (thesis ILIKE :pn OR thesis ILIKE :pt) AND status IN ('active','confirmed')
                ORDER BY confidence DESC NULLS LAST LIMIT 1
            """),
            {"pn": pct_name, "pt": pct_ticker},
        ).fetchone()
        if hyp_row:
            hm = hyp_row._mapping
            hid = f"hyp-{hm['id']}-{uuid.uuid4().hex[:6]}"
            hlabel = hm["thesis"][:97] + "..." if len(hm["thesis"]) > 100 else hm["thesis"]
            _insert_node(hid, "hypothesis", hlabel, center_x - R_SIGNAL, center_y - 50, {
                "confidence": hm["confidence"], "status": hm["status"],
                "role": hm["role"] or "thesis", "pattern_type": hm["pattern_type"],
            })
            _insert_edge(node_id, hid, "hypothesis")

        # ── 1d. Company profile ──────────────────────────────────────
        if ticker:
            cp_row = conn.execute(
                text("SELECT ticker, name, sector, suspicion_score FROM company_profiles WHERE ticker = :t LIMIT 1"),
                {"t": ticker},
            ).fetchone()
            if cp_row:
                cm = cp_row._mapping
                cpid = f"company-{cm['ticker']}-{uuid.uuid4().hex[:6]}"
                _insert_node(cpid, "company", cm["name"] or cm["ticker"], center_x, center_y - R_ACTOR - 80, {
                    "ticker": cm["ticker"], "sector": cm["sector"],
                    "suspicion_score": float(cm["suspicion_score"]) if cm["suspicion_score"] else None,
                    "entityId": cm["ticker"],
                })
                _insert_edge(node_id, cpid, cm["sector"] or "company")

        # ── 1e. Wealth flows (max 3) — who's paying whom ─────────────
        flow_rows = conn.execute(
            text("""
                SELECT id, from_actor, to_entity, amount_estimate, confidence, implication
                FROM wealth_flows
                WHERE from_actor ILIKE :pn OR to_entity ILIKE :pn
                ORDER BY amount_estimate DESC NULLS LAST LIMIT 3
            """),
            {"pn": pct_name},
        ).fetchall()

        for i, fr in enumerate(flow_rows):
            fm = fr._mapping
            fid = f"flow-{fm['id']}-{uuid.uuid4().hex[:6]}"
            px = center_x + R_FLOW
            py = center_y + 100 + i * 100
            amt = float(fm["amount_estimate"]) if fm["amount_estimate"] else 0
            amt_str = f"${amt/1e6:.1f}M" if amt > 1e6 else f"${amt/1e3:.0f}K" if amt > 1e3 else f"${amt:.0f}"
            flow_from = fm["from_actor"] or "?"
            flow_to = fm["to_entity"] or "?"
            flow_label = f"{flow_from} → {flow_to}: {amt_str}"
            target_nid = _insert_node(fid, "evidence", flow_label, px, py, {
                "evidence_type": "wealth_flow",
                "confidence": fm["confidence"] or "estimated",
                "content": fm["implication"] or flow_label,
            })
            _insert_edge(node_id, target_nid, amt_str, {"strength": min(amt / 1e9, 1.0) if amt else 0.1})
            # Cross-connect to from/to entities if on board
            for xkey in [flow_from, flow_to]:
                xnode = _find_existing(xkey, "", xkey)
                if xnode and xnode != node_id and xnode != target_nid:
                    _insert_edge(target_nid, xnode, "flow")

        # ── 1f. Dollar flows (max 3) ─────────────────────────────────
        if ticker:
            df_rows = conn.execute(
                text("""
                    SELECT id, source_type, actor_name, ticker, amount_usd,
                           direction, confidence, flow_date
                    FROM dollar_flows
                    WHERE ticker = :t OR actor_name ILIKE :pn
                    ORDER BY amount_usd DESC NULLS LAST LIMIT 3
                """),
                {"t": ticker, "pn": pct_name},
            ).fetchall()

            for i, dr in enumerate(df_rows):
                dm = dr._mapping
                did = f"dollar-{dm['id']}-{uuid.uuid4().hex[:6]}"
                px = center_x - R_FLOW
                py = center_y + 100 + i * 100
                amt = float(dm["amount_usd"]) if dm["amount_usd"] else 0
                amt_str = f"${amt/1e6:.1f}M" if amt > 1e6 else f"${amt/1e3:.0f}K" if amt > 1e3 else f"${amt:.0f}"
                direction = dm["direction"] or "flow"
                dlabel = f"{dm['actor_name'] or '?'} {direction} {dm['ticker'] or ''}: {amt_str}"
                _insert_node(did, "evidence", dlabel, px, py, {
                    "evidence_type": "dollar_flow",
                    "confidence": dm["confidence"] or "estimated",
                    "content": dlabel,
                })
                _insert_edge(node_id, did, f"{direction} {amt_str}")

        # ══════════════════════════════════════════════════════════════
        # DEPTH 2: Insider activity + lever pullers + predictions
        # ══════════════════════════════════════════════════════════════
        if depth >= 2 and ticker:

            # ── 2a. Insider trades (max 3) ───────────────────────────
            ins_rows = conn.execute(
                text("""
                    SELECT id, ticker, trade_date, insider_name, insider_title,
                           trade_type, shares, value, is_cluster_buy
                    FROM insider_trades
                    WHERE ticker = :t ORDER BY trade_date DESC LIMIT 3
                """),
                {"t": ticker},
            ).fetchall()

            for i, ir in enumerate(ins_rows):
                im = ir._mapping
                iid = f"insider-{im['id']}-{uuid.uuid4().hex[:6]}"
                px = center_x - 350
                py = center_y + 120 + i * 100
                val = float(im["value"]) if im["value"] else 0
                val_str = f"${val/1e6:.1f}M" if val > 1e6 else f"${val/1e3:.0f}K" if val > 1e3 else f"${val:.0f}"
                ilabel = f"{im['insider_name']}: {im['trade_type']} {val_str}"
                _insert_node(iid, "signal", ilabel, px, py, {
                    "signal_type": "insider_trade",
                    "ticker": im["ticker"],
                    "direction": "buy" if "purchase" in (im["trade_type"] or "").lower() or "buy" in (im["trade_type"] or "").lower() else "sell",
                    "magnitude": min(val / 1e6, 10) if val else 0,
                    "confidence": "confirmed",
                    "insider_name": im["insider_name"],
                    "insider_title": im["insider_title"],
                    "is_cluster_buy": im["is_cluster_buy"],
                })
                edge_lbl = "CLUSTER BUY" if im["is_cluster_buy"] else im["trade_type"] or "insider"
                _insert_edge(node_id, iid, edge_lbl)
                # Cross-connect insider to ticker node if on board
                ticker_node = _find_existing(im["ticker"], im["ticker"], "")
                if ticker_node and ticker_node != node_id:
                    _insert_edge(iid, ticker_node, "insider trade")

            # ── 2b. Congressional trades (max 3) ─────────────────────
            cong_rows = conn.execute(
                text("""
                    SELECT id, ticker, disclosure_date, representative, chamber,
                           party, state, transaction_type, amount_midpoint, committee
                    FROM congressional_trades
                    WHERE ticker = :t ORDER BY disclosure_date DESC LIMIT 3
                """),
                {"t": ticker},
            ).fetchall()

            for i, cr in enumerate(cong_rows):
                cm = cr._mapping
                cid = f"congress-{cm['id']}-{uuid.uuid4().hex[:6]}"
                px = center_x - 350
                py = center_y - 200 + i * 100
                amt = float(cm["amount_midpoint"]) if cm["amount_midpoint"] else 0
                amt_str = f"${amt/1e3:.0f}K" if amt > 0 else "?"
                party_tag = f"({cm['party']}-{cm['state']})" if cm["party"] and cm["state"] else ""
                clabel = f"Rep. {cm['representative']} {party_tag}: {cm['transaction_type']} {amt_str}"
                _insert_node(cid, "signal", clabel, px, py, {
                    "signal_type": "congressional",
                    "ticker": cm["ticker"],
                    "direction": "buy" if "purchase" in (cm["transaction_type"] or "").lower() else "sell",
                    "confidence": "confirmed",
                    "representative": cm["representative"],
                    "party": cm["party"], "state": cm["state"],
                    "committee": cm["committee"],
                })
                committee_label = cm["committee"] or "congressional trade"
                _insert_edge(node_id, cid, committee_label)
                # Cross-connect congress member to ticker node if on board
                ticker_node = _find_existing(cm["ticker"], cm["ticker"], "")
                if ticker_node and ticker_node != node_id:
                    _insert_edge(cid, ticker_node, "congressional trade")

            # ── 2c. Oracle predictions (max 2) ───────────────────────
            pred_rows = conn.execute(
                text("""
                    SELECT id, ticker, direction, confidence, expected_move_pct,
                           verdict, model_name
                    FROM oracle_predictions WHERE ticker = :t
                    ORDER BY created_at DESC LIMIT 2
                """),
                {"t": ticker},
            ).fetchall()

            for i, pr in enumerate(pred_rows):
                pm = pr._mapping
                pid = f"pred-{pm['id']}-{uuid.uuid4().hex[:6]}"
                px = center_x + 350
                py = center_y + 120 + i * 100
                conf = f"{float(pm['confidence'])*100:.0f}%" if pm["confidence"] else ""
                plabel = f"Oracle: {pm['ticker']} {pm['verdict'] or pm['direction'] or '?'} {conf}"
                _insert_node(pid, "hypothesis", plabel, px, py, {
                    "ticker": pm["ticker"], "direction": pm["direction"],
                    "confidence": pm["confidence"],
                    "status": pm["verdict"] or "pending", "role": "thesis",
                    "model": pm["model_name"],
                })
                _insert_edge(node_id, pid, "prediction")

            # ── 2d. Insider CLUSTERS — friends trading together ────
            # Find other insiders who traded the same ticker within ±7 days
            try:
                cluster_rows = conn.execute(
                    text("""
                        SELECT DISTINCT s2.actor, COUNT(*) AS co_trades
                        FROM signal_data s1
                        JOIN signal_data s2
                            ON s1.ticker = s2.ticker
                            AND s1.actor != s2.actor
                            AND ABS(s1.signal_date - s2.signal_date) <= 7
                        WHERE s1.ticker = :t
                        AND s1.signal_type IN ('insider', 'quiverquant:insider')
                        AND s2.signal_type IN ('insider', 'quiverquant:insider')
                        AND s2.actor IS NOT NULL AND s2.actor != ''
                        GROUP BY s2.actor
                        HAVING COUNT(*) >= 2
                        ORDER BY COUNT(*) DESC
                        LIMIT 4
                    """),
                    {"t": ticker},
                ).fetchall()

                for i, cr in enumerate(cluster_rows):
                    cluster_name = cr[0]
                    co_count = int(cr[1])
                    crid = f"cluster-{uuid.uuid4().hex[:8]}"
                    px = center_x + 400
                    py = center_y - 100 + i * 90
                    _insert_node(crid, "actor", f"{cluster_name} (insider cluster)", px, py, {
                        "entityId": f"ins_{cluster_name.lower().replace(' ', '_')[:40]}",
                        "category": "insider",
                        "co_trades": co_count,
                        "is_cluster": True,
                    })
                    _insert_edge(node_id, crid, f"cluster ({co_count} co-trades)")
            except Exception as exc:
                log.debug("Canvas expand insider clusters: {e}", e=str(exc))

            # ── 2e. Fresh NEWS about this ticker (last 7 days) ──────
            try:
                news_rows = conn.execute(
                    text("""
                        SELECT DISTINCT ON (headline) id, headline, source,
                               signal_date, direction, confidence, actor
                        FROM signal_data
                        WHERE signal_type IN ('news', 'breaking_news', 'tiingo_news',
                                              'marketwatch_news', 'polygon_news')
                        AND (ticker = :t OR actor ILIKE :pn)
                        AND signal_date >= CURRENT_DATE - INTERVAL '7 days'
                        AND headline IS NOT NULL AND headline != ''
                        ORDER BY headline, signal_date DESC
                        LIMIT 4
                    """),
                    {"t": ticker, "pn": pct_name},
                ).fetchall()

                for i, nr in enumerate(news_rows):
                    nm = nr._mapping
                    nwid = f"news-{nm['id']}-{uuid.uuid4().hex[:6]}"
                    px = center_x - 500
                    py = center_y - 200 + i * 100
                    headline = (nm["headline"] or "")[:80]
                    _insert_node(nwid, "news", headline, px, py, {
                        "title": nm["headline"],
                        "source": nm["source"] or "GRID",
                        "published_at": str(nm["signal_date"]) if nm["signal_date"] else None,
                        "direction": nm["direction"],
                        "confidence": nm["confidence"],
                        "reporter": nm["actor"],
                    })
                    _insert_edge(node_id, nwid, nm["source"] or "news")
                    # Cross-connect to reporter if they're an actor on the board
                    if nm["actor"]:
                        reporter_node = _find_existing(nm["actor"], "", nm["actor"])
                        if reporter_node and reporter_node != node_id:
                            _insert_edge(nwid, reporter_node, "reported by")
            except Exception as exc:
                log.debug("Canvas expand news: {e}", e=str(exc))

            # ── 2f. Congress-insider OVERLAP — smoking gun ───────────
            try:
                overlap_rows = conn.execute(
                    text("""
                        SELECT c.actor AS congress_member, i.actor AS insider_name, COUNT(*) AS co_trades
                        FROM signal_data c
                        JOIN signal_data i
                            ON c.ticker = i.ticker
                            AND ABS(c.signal_date - i.signal_date) <= 14
                        WHERE c.ticker = :t
                        AND c.signal_type IN ('congressional', 'quiverquant:house', 'quiverquant:senate')
                        AND i.signal_type IN ('insider', 'quiverquant:insider')
                        AND c.actor IS NOT NULL AND i.actor IS NOT NULL
                        GROUP BY c.actor, i.actor
                        HAVING COUNT(*) >= 1
                        ORDER BY COUNT(*) DESC
                        LIMIT 3
                    """),
                    {"t": ticker},
                ).fetchall()

                for i, olr in enumerate(overlap_rows):
                    cong_name = olr[0]
                    ins_name = olr[1]
                    co_count = int(olr[2])
                    # Add the congress member if not on board
                    cong_nid = f"overlap-pol-{uuid.uuid4().hex[:8]}"
                    px = center_x - 350
                    py = center_y - 350 + i * 90
                    cong_nid = _insert_node(cong_nid, "actor", f"Rep. {cong_name}", px, py, {
                        "entityId": f"pol_{cong_name.lower().replace(' ', '_')[:40]}",
                        "category": "politician",
                        "overlap_trades": co_count,
                    })
                    # Add the insider if not on board
                    ins_nid = f"overlap-ins-{uuid.uuid4().hex[:8]}"
                    ins_nid = _insert_node(ins_nid, "actor", f"{ins_name} (insider)", px + 200, py, {
                        "entityId": f"ins_{ins_name.lower().replace(' ', '_')[:40]}",
                        "category": "insider",
                        "overlap_trades": co_count,
                    })
                    # Connect them
                    _insert_edge(cong_nid, ins_nid, f"OVERLAP ({co_count}x ±14d)")
                    _insert_edge(node_id, cong_nid, "traded same ticker")
                    _insert_edge(node_id, ins_nid, "traded same ticker")
            except Exception as exc:
                log.debug("Canvas expand congress-insider overlap: {e}", e=str(exc))

            # ── 2g. Lever pullers for ticker (max 3) ─────────────────
            lp_rows = conn.execute(
                text("""
                    SELECT lp.name, lp.category, lp.position,
                           lp.influence_rank, lp.trust_score, lp.motivation_model,
                           lp.total_signals, lp.correct_signals, lp.source_id
                    FROM lever_pullers lp
                    WHERE lp.source_id IN (
                        SELECT DISTINCT ss.source_id FROM signal_sources ss
                        WHERE ss.ticker = :t
                    )
                    ORDER BY lp.trust_score * lp.influence_rank DESC LIMIT 3
                """),
                {"t": ticker},
            ).fetchall()

            for i, lr in enumerate(lp_rows):
                lm = lr._mapping
                lpid = f"lever-{lm['source_id']}-{uuid.uuid4().hex[:6]}"
                px = center_x + 250
                py = center_y - 250 + i * 100
                accuracy = (int(lm["correct_signals"] or 0) / int(lm["total_signals"]) * 100) if lm["total_signals"] and int(lm["total_signals"]) > 0 else 0
                _insert_node(lpid, "actor", f"{lm['name']} ({lm['position'] or lm['category']})", px, py, {
                    "entityId": lm["source_id"], "category": lm["category"],
                    "is_lever_puller": True, "lever_position": lm["position"],
                    "influence_rank": float(lm["influence_rank"]) if lm["influence_rank"] else 0.5,
                    "trust_score": float(lm["trust_score"]) if lm["trust_score"] else 0.5,
                    "motivation_model": lm["motivation_model"], "accuracy_pct": round(accuracy, 1),
                })
                _insert_edge(node_id, lpid, f"lever: {lm['category']}")

        # ══════════════════════════════════════════════════════════════
        # DEPTH 3: Deep investigation — reality checks, flows, leads
        # ══════════════════════════════════════════════════════════════
        if depth >= 3:

            # ── 3a. Cross-reference reality checks (max 2) ───────────
            xref_rows = conn.execute(
                text("""
                    SELECT id, name, category, official_value, physical_value,
                           divergence_zscore, assessment, implication, confidence
                    FROM cross_reference_checks
                    WHERE name ILIKE :pn OR category ILIKE :pn
                    ORDER BY ABS(divergence_zscore) DESC NULLS LAST LIMIT 2
                """),
                {"pn": pct_name},
            ).fetchall()

            for i, xr in enumerate(xref_rows):
                xm = xr._mapping
                xid = f"xref-{xm['id']}-{uuid.uuid4().hex[:6]}"
                px = center_x
                py = center_y + 350 + i * 100
                zscore = float(xm["divergence_zscore"]) if xm["divergence_zscore"] else 0
                xlabel = f"Reality check: {xm['name']} (z={zscore:.1f})"
                _insert_node(xid, "evidence", xlabel, px, py, {
                    "evidence_type": "cross_reference",
                    "confidence": xm["confidence"] or "derived",
                    "content": xm["assessment"] or xm["implication"] or "",
                    "official_value": str(xm["official_value"]) if xm["official_value"] else None,
                    "physical_value": str(xm["physical_value"]) if xm["physical_value"] else None,
                })
                _insert_edge(node_id, xid, f"reality z={zscore:.1f}")

            # (wealth flows moved to depth 1)

            # ── 3b. Investigation leads (max 2 open) ─────────────────
            lead_rows = conn.execute(
                text("""
                    SELECT id, question, category, priority, evidence, status
                    FROM investigation_leads
                    WHERE (question ILIKE :pn OR question ILIKE :pt) AND status != 'resolved'
                    ORDER BY priority ASC NULLS LAST LIMIT 2
                """),
                {"pn": pct_name, "pt": pct_ticker},
            ).fetchall()

            for i, lr in enumerate(lead_rows):
                lm = lr._mapping
                lid = f"lead-{lm['id']}-{uuid.uuid4().hex[:6]}"
                px = center_x - 300
                py = center_y + 300 + i * 100
                llabel = lm["question"][:90] + "..." if len(lm["question"] or "") > 90 else lm["question"] or "?"
                _insert_node(lid, "evidence", llabel, px, py, {
                    "evidence_type": "investigation_lead",
                    "confidence": "rumored",
                    "content": str(lm["evidence"])[:200] if lm["evidence"] else "",
                    "category": lm["category"],
                    "priority": lm["priority"],
                })
                _insert_edge(node_id, lid, f"lead: {lm['category'] or 'open'}")

            # ── 3d. More hypotheses (2 more) ─────────────────────────
            more_hyps = conn.execute(
                text("""
                    SELECT id, thesis, confidence, status, role, pattern_type
                    FROM discovered_hypotheses
                    WHERE (thesis ILIKE :pn OR thesis ILIKE :pt) AND status IN ('active','confirmed')
                    ORDER BY confidence DESC NULLS LAST LIMIT 3 OFFSET 1
                """),
                {"pn": pct_name, "pt": pct_ticker},
            ).fetchall()

            for i, hr in enumerate(more_hyps):
                hm = hr._mapping
                hid = f"hyp-{hm['id']}-{uuid.uuid4().hex[:6]}"
                px = center_x - 400
                py = center_y + 50 + i * 100
                hlabel = hm["thesis"][:97] + "..." if len(hm["thesis"]) > 100 else hm["thesis"]
                _insert_node(hid, "hypothesis", hlabel, px, py, {
                    "confidence": hm["confidence"], "status": hm["status"],
                    "role": hm["role"] or "thesis",
                })
                _insert_edge(node_id, hid, "hypothesis")

        # ── Enrich source node with lever puller data ────────────────
        source_lever = _get_lever_puller_data(conn, label)
        if source_lever:
            existing_data = source_data or {}
            existing_data.update(source_lever)
            conn.execute(
                text("UPDATE canvas_nodes SET data = :data WHERE node_id = :nid AND board_id = :bid"),
                {"data": json.dumps(existing_data), "nid": node_id, "bid": board_id},
            )

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

        # Build mapping: all known IDs for each board actor -> canvas_node_id
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
            label = rm.get("label") or ""

            # Resolve all IDs through the canonical map
            all_ids = _get_all_ids_for_entity(conn, eid or "", label) if (eid or label) else []
            for aid in all_ids:
                entity_to_canvas[aid] = canvas_nid

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
