"""
GRID Canvas — Unified graph intelligence API.

Connects ALL the dots: actors, tickers, signals, wealth flows,
co-trading patterns, congressional timing, offshore entities,
signal convergence, and causation chains.

Endpoints:
    GET  /graph                    — unified canvas graph (BFS traversal)
    GET  /node/{type}/{id}         — detail panel for a node
    GET  /expand/{type}/{id}       — incremental expansion
    GET  /dots                     — cross-reference dot connections
    POST /boards                   — create investigation board
    GET  /boards                   — list boards
    GET  /boards/{id}              — get board state
    PUT  /boards/{id}              — update board
    DELETE /boards/{id}            — delete board
    POST /boards/{id}/fork         — duplicate board
"""

from __future__ import annotations

import json
import uuid
from collections import defaultdict, deque
from datetime import date, datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.engine import Engine

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/canvas", tags=["canvas"])


# ══════════════════════════════════════════════════════════════════════════
# TABLE DDL — investigation_boards
# ══════════════════════════════════════════════════════════════════════════

_BOARDS_DDL = """
CREATE TABLE IF NOT EXISTS investigation_boards (
    id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    name TEXT NOT NULL,
    description TEXT,
    graph_state JSONB NOT NULL DEFAULT '{}',
    camera_state JSONB,
    filters JSONB,
    pinned_nodes TEXT[] DEFAULT '{}',
    annotations JSONB DEFAULT '[]',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_investigation_boards_updated
    ON investigation_boards (updated_at DESC);
"""

_boards_ensured = False


def _ensure_boards_table(engine: Engine) -> None:
    """Create investigation_boards table if it does not exist."""
    global _boards_ensured
    if _boards_ensured:
        return
    try:
        with engine.begin() as conn:
            conn.execute(text(_BOARDS_DDL))
        _boards_ensured = True
    except Exception as exc:
        log.warning("Failed to ensure investigation_boards table: {e}", e=str(exc))


# ══════════════════════════════════════════════════════════════════════════
# LAYER MAPPING — maps layer names to signal source types + actor categories
# ══════════════════════════════════════════════════════════════════════════

_LAYER_SOURCE_MAP: dict[str, list[str]] = {
    "financial":   ["13f", "etf_flow", "darkpool", "institutional"],
    "insider":     ["insider", "form4", "insider_cluster"],
    "political":   ["congressional", "congress", "political", "lobbying"],
    "news":        ["news", "gdelt", "social", "sentiment"],
    "options":     ["options_flow", "whale_options", "dealer_gamma", "gex"],
    "macro":       ["fed", "fomc", "treasury", "ecb", "boj", "macro", "economic"],
    "offshore":    ["panama", "pandora", "icij", "offshore"],
    "predictions": ["prediction_market", "polymarket", "prediction"],
}

_LAYER_CATEGORY_MAP: dict[str, list[str]] = {
    "financial":   ["fund", "asset_manager", "swf", "bank"],
    "insider":     ["insider", "corporation"],
    "political":   ["politician", "government"],
    "news":        [],
    "options":     [],
    "macro":       ["central_bank"],
    "offshore":    ["offshore", "shell"],
    "predictions": [],
}


def _parse_layers(layers_str: str) -> set[str]:
    """Parse comma-separated layer string into a set of valid layer names."""
    if layers_str == "all":
        return set(_LAYER_SOURCE_MAP.keys())
    parsed = set()
    for layer in layers_str.split(","):
        layer = layer.strip().lower()
        if layer in _LAYER_SOURCE_MAP:
            parsed.add(layer)
    return parsed or set(_LAYER_SOURCE_MAP.keys())


def _get_allowed_source_types(layers: set[str]) -> set[str]:
    """Get the set of allowed signal source types for the given layers."""
    types: set[str] = set()
    for layer in layers:
        types.update(_LAYER_SOURCE_MAP.get(layer, []))
    return types


def _get_allowed_categories(layers: set[str]) -> set[str]:
    """Get the set of allowed actor categories for the given layers."""
    cats: set[str] = set()
    for layer in layers:
        cats.update(_LAYER_CATEGORY_MAP.get(layer, []))
    return cats


# ══════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════

def _resolve_center(engine: Engine, center: str) -> dict[str, Any]:
    """Determine if center is an actor ID, actor name, or ticker symbol.

    Returns dict with keys: entity_type ('actor' | 'ticker'), id, name, data.
    """
    # 1. Check actors table by id
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT id, name, tier, category, influence_score, trust_score, title "
                "FROM actors WHERE id = :center LIMIT 1"
            ).bindparams(center=center),
        ).mappings().fetchone()

        if row:
            return {
                "entity_type": "actor",
                "id": row["id"],
                "name": row["name"],
                "data": dict(row),
            }

        # 2. Check actors by name (case-insensitive partial match)
        row = conn.execute(
            text(
                "SELECT id, name, tier, category, influence_score, trust_score, title "
                "FROM actors WHERE LOWER(name) = LOWER(:center) LIMIT 1"
            ).bindparams(center=center),
        ).mappings().fetchone()

        if row:
            return {
                "entity_type": "actor",
                "id": row["id"],
                "name": row["name"],
                "data": dict(row),
            }

        # 3. Check if it's a ticker (signals exist for it)
        ticker_upper = center.upper()
        ticker_row = conn.execute(
            text(
                "SELECT ticker, COUNT(*) as signal_count "
                "FROM signal_data WHERE UPPER(ticker) = :ticker "
                "GROUP BY ticker LIMIT 1"
            ).bindparams(ticker=ticker_upper),
        ).mappings().fetchone()

        if ticker_row:
            return {
                "entity_type": "ticker",
                "id": ticker_upper,
                "name": ticker_upper,
                "data": {"ticker": ticker_upper, "signal_count": ticker_row["signal_count"]},
            }

    raise HTTPException(
        status_code=404,
        detail=f"Center entity '{center}' not found as actor or ticker",
    )


def _bfs_actors(
    engine: Engine,
    start_actor_id: str,
    depth: int,
    limit: int,
) -> tuple[dict[str, dict], list[dict]]:
    """BFS traversal of actor_connections from a starting actor.

    Returns:
        actors: dict of actor_id -> actor data
        edges: list of connection dicts
    """
    visited: dict[str, dict] = {}
    edges: list[dict] = []
    queue: deque[tuple[str, int]] = deque([(start_actor_id, 0)])
    seen_edges: set[tuple[str, str]] = set()

    with engine.connect() as conn:
        # Pre-load center actor
        row = conn.execute(
            text(
                "SELECT id, name, tier, category, influence_score, trust_score, title "
                "FROM actors WHERE id = :aid LIMIT 1"
            ).bindparams(aid=start_actor_id),
        ).mappings().fetchone()

        if row:
            visited[start_actor_id] = dict(row)

        while queue and len(visited) < limit:
            current_id, current_depth = queue.popleft()

            if current_depth >= depth:
                continue

            # Get connections in both directions
            rows = conn.execute(
                text(
                    "SELECT from_actor, to_actor, connection_type, strength, "
                    "       metadata "
                    "FROM actor_connections "
                    "WHERE from_actor = :aid OR to_actor = :aid"
                ).bindparams(aid=current_id),
            ).mappings().fetchall()

            for r in rows:
                from_a = r["from_actor"]
                to_a = r["to_actor"]
                neighbor = to_a if from_a == current_id else from_a
                edge_key = tuple(sorted([from_a, to_a]))

                # Record edge
                if edge_key not in seen_edges:
                    seen_edges.add(edge_key)
                    meta = r["metadata"]
                    if isinstance(meta, str):
                        try:
                            meta = json.loads(meta)
                        except (json.JSONDecodeError, TypeError):
                            meta = {}
                    edges.append({
                        "source": f"a:{from_a}",
                        "target": f"a:{to_a}",
                        "type": "connection",
                        "connection_type": r["connection_type"] or "connected",
                        "strength": float(r["strength"] or 0.5),
                        "confidence": float((meta or {}).get("confidence", 0.7)),
                    })

                # Visit neighbor
                if neighbor not in visited and len(visited) < limit:
                    # Load actor data
                    actor_row = conn.execute(
                        text(
                            "SELECT id, name, tier, category, influence_score, "
                            "       trust_score, title "
                            "FROM actors WHERE id = :aid LIMIT 1"
                        ).bindparams(aid=neighbor),
                    ).mappings().fetchone()

                    if actor_row:
                        visited[neighbor] = dict(actor_row)
                        queue.append((neighbor, current_depth + 1))

    return visited, edges


def _load_signals_for_actors(
    engine: Engine,
    actor_ids: list[str],
    since: str | None,
    limit: int,
) -> tuple[list[dict], list[dict], set[str]]:
    """Load signals from signal_data for a set of actors.

    Returns:
        signal_nodes: list of signal node dicts
        signal_edges: list of signal_link edge dicts (actor -> ticker)
        tickers_seen: set of ticker symbols found
    """
    if not actor_ids:
        return [], [], set()

    signal_nodes: list[dict] = []
    signal_edges: list[dict] = []
    tickers_seen: set[str] = set()
    seen_signals: set[str] = set()

    with engine.connect() as conn:
        # Build query — use ANY for array of actor IDs
        since_clause = ""
        params: dict[str, Any] = {}
        if since:
            since_clause = "AND signal_date >= :since"
            params["since"] = since

        # Query signals for these actors
        # We use a text query with LIKE matching since actor field may be name or ID
        for actor_id in actor_ids[:50]:  # Limit to avoid query explosion
            rows = conn.execute(
                text(
                    f"SELECT id, signal_type, signal_date, ticker, actor, "
                    f"       direction, magnitude, confidence, description "
                    f"FROM signal_data "
                    f"WHERE (actor = :actor_id OR actor = :actor_name) "
                    f"{since_clause} "
                    f"ORDER BY signal_date DESC LIMIT :lim"
                ).bindparams(
                    actor_id=actor_id,
                    actor_name=actor_id,  # May match by name too
                    lim=min(limit, 20),
                    **params,
                ),
            ).mappings().fetchall()

            for r in rows:
                sig_id = f"s:{r['id']}"
                if sig_id in seen_signals:
                    continue
                seen_signals.add(sig_id)

                signal_nodes.append({
                    "id": sig_id,
                    "type": "signal",
                    "source_type": r["signal_type"],
                    "direction": r["direction"],
                    "confidence": float(r["confidence"] or 0.5),
                    "magnitude": float(r["magnitude"] or 0),
                    "signal_date": str(r["signal_date"]) if r["signal_date"] else None,
                    "description": r["description"],
                    "ticker": r["ticker"],
                    "actor": r["actor"],
                })

                # Signal link: actor -> ticker
                if r["ticker"]:
                    ticker = str(r["ticker"]).upper()
                    tickers_seen.add(ticker)
                    signal_edges.append({
                        "source": f"a:{actor_id}",
                        "target": f"t:{ticker}",
                        "type": "signal_link",
                        "signal_id": sig_id,
                        "signal_type": r["signal_type"],
                        "direction": r["direction"],
                    })

    return signal_nodes, signal_edges, tickers_seen


def _load_wealth_flows(
    engine: Engine,
    actor_ids: list[str],
) -> list[dict]:
    """Load wealth flow edges between actors."""
    if not actor_ids:
        return []

    flow_edges: list[dict] = []
    seen: set[tuple[str, str]] = set()

    with engine.connect() as conn:
        for actor_id in actor_ids[:50]:
            rows = conn.execute(
                text(
                    "SELECT from_actor, to_entity, amount_estimate, confidence, "
                    "       evidence, flow_date "
                    "FROM wealth_flows "
                    "WHERE from_actor = :aid OR to_entity = :aid "
                    "ORDER BY flow_date DESC LIMIT 50"
                ).bindparams(aid=actor_id),
            ).mappings().fetchall()

            for r in rows:
                key = (r["from_actor"], r["to_entity"])
                if key in seen:
                    continue
                seen.add(key)

                flow_edges.append({
                    "source": f"a:{r['from_actor']}",
                    "target": f"a:{r['to_entity']}",
                    "type": "flow",
                    "amount": float(r["amount_estimate"] or 0),
                    "confidence": r["confidence"] or "estimated",
                    "flow_date": str(r["flow_date"]) if r["flow_date"] else None,
                })

    return flow_edges


def _load_dollar_flows(
    engine: Engine,
    actor_ids: list[str],
) -> list[dict]:
    """Load dollar flow edges from the dollar_flows table."""
    if not actor_ids:
        return []

    flow_edges: list[dict] = []

    with engine.connect() as conn:
        for actor_id in actor_ids[:50]:
            rows = conn.execute(
                text(
                    "SELECT source_type, actor_name, ticker, amount_usd, "
                    "       direction, confidence, flow_date "
                    "FROM dollar_flows "
                    "WHERE actor_name = :aid "
                    "ORDER BY flow_date DESC LIMIT 20"
                ).bindparams(aid=actor_id),
            ).mappings().fetchall()

            for r in rows:
                if r["ticker"]:
                    flow_edges.append({
                        "source": f"a:{actor_id}",
                        "target": f"t:{r['ticker'].upper()}",
                        "type": "flow",
                        "flow_type": r["source_type"],
                        "amount": float(r["amount_usd"] or 0),
                        "direction": r["direction"],
                        "confidence": r["confidence"] or "estimated",
                        "flow_date": str(r["flow_date"]) if r["flow_date"] else None,
                    })

    return flow_edges


def _detect_co_traded(
    engine: Engine,
    actor_ids: list[str],
    window_days: int = 14,
) -> list[dict]:
    """Find actor pairs who traded the same ticker within a window.

    Returns co_traded edges.
    """
    if len(actor_ids) < 2:
        return []

    co_traded: list[dict] = []
    seen: set[tuple[str, str, str]] = set()

    with engine.connect() as conn:
        # Find tickers each actor traded recently
        actor_tickers: dict[str, list[dict]] = defaultdict(list)
        for actor_id in actor_ids[:50]:
            rows = conn.execute(
                text(
                    "SELECT DISTINCT ticker, signal_date, direction "
                    "FROM signal_data "
                    "WHERE actor = :aid AND ticker IS NOT NULL "
                    "AND signal_date >= CURRENT_DATE - :window "
                    "ORDER BY signal_date DESC LIMIT 30"
                ).bindparams(aid=actor_id, window=window_days * 3),
            ).mappings().fetchall()

            for r in rows:
                actor_tickers[actor_id].append({
                    "ticker": r["ticker"],
                    "date": r["signal_date"],
                    "direction": r["direction"],
                })

        # Compare all pairs
        actor_list = list(actor_tickers.keys())
        for i in range(len(actor_list)):
            for j in range(i + 1, len(actor_list)):
                a1, a2 = actor_list[i], actor_list[j]
                for t1 in actor_tickers[a1]:
                    for t2 in actor_tickers[a2]:
                        if (
                            t1["ticker"] == t2["ticker"]
                            and t1["date"]
                            and t2["date"]
                        ):
                            try:
                                d1 = t1["date"] if isinstance(t1["date"], date) else date.fromisoformat(str(t1["date"]))
                                d2 = t2["date"] if isinstance(t2["date"], date) else date.fromisoformat(str(t2["date"]))
                                delta = abs((d1 - d2).days)
                            except (ValueError, TypeError):
                                continue

                            if delta <= window_days:
                                key = tuple(sorted([a1, a2]) + [t1["ticker"]])
                                if key in seen:
                                    continue
                                seen.add(key)

                                co_traded.append({
                                    "source": f"a:{a1}",
                                    "target": f"a:{a2}",
                                    "type": "co_traded",
                                    "ticker": t1["ticker"],
                                    "days_apart": delta,
                                    "direction_match": t1["direction"] == t2["direction"],
                                })

    return co_traded


def _build_ticker_nodes(
    engine: Engine,
    tickers: set[str],
) -> list[dict]:
    """Build ticker nodes with signal counts."""
    if not tickers:
        return []

    nodes: list[dict] = []
    with engine.connect() as conn:
        for ticker in list(tickers)[:100]:
            row = conn.execute(
                text(
                    "SELECT COUNT(*) as cnt "
                    "FROM signal_data WHERE UPPER(ticker) = :t"
                ).bindparams(t=ticker.upper()),
            ).mappings().fetchone()

            nodes.append({
                "id": f"t:{ticker}",
                "type": "ticker",
                "ticker": ticker,
                "signal_count": row["cnt"] if row else 0,
            })

    return nodes


def _load_signals_for_ticker(
    engine: Engine,
    ticker: str,
    since: str | None,
    limit: int,
) -> tuple[list[dict], list[dict], set[str]]:
    """Load signals for a specific ticker and find related actors.

    Returns:
        signal_nodes, signal_edges, actor_ids_found
    """
    signal_nodes: list[dict] = []
    signal_edges: list[dict] = []
    actor_ids: set[str] = set()

    with engine.connect() as conn:
        since_clause = ""
        params: dict[str, Any] = {"ticker": ticker.upper(), "lim": min(limit, 100)}
        if since:
            since_clause = "AND signal_date >= :since"
            params["since"] = since

        rows = conn.execute(
            text(
                f"SELECT id, signal_type, signal_date, ticker, actor, "
                f"       direction, magnitude, confidence, description "
                f"FROM signal_data "
                f"WHERE UPPER(ticker) = :ticker "
                f"{since_clause} "
                f"ORDER BY signal_date DESC LIMIT :lim"
            ).bindparams(**params),
        ).mappings().fetchall()

        for r in rows:
            sig_id = f"s:{r['id']}"
            signal_nodes.append({
                "id": sig_id,
                "type": "signal",
                "source_type": r["signal_type"],
                "direction": r["direction"],
                "confidence": float(r["confidence"] or 0.5),
                "magnitude": float(r["magnitude"] or 0),
                "signal_date": str(r["signal_date"]) if r["signal_date"] else None,
                "description": r["description"],
                "ticker": r["ticker"],
                "actor": r["actor"],
            })

            if r["actor"]:
                actor_ids.add(r["actor"])
                signal_edges.append({
                    "source": f"a:{r['actor']}",
                    "target": f"t:{ticker.upper()}",
                    "type": "signal_link",
                    "signal_id": sig_id,
                    "signal_type": r["signal_type"],
                    "direction": r["direction"],
                })

    return signal_nodes, signal_edges, actor_ids


# ══════════════════════════════════════════════════════════════════════════
# PYDANTIC MODELS
# ══════════════════════════════════════════════════════════════════════════

class BoardCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    description: str | None = None


class BoardUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    graph_state: dict | None = None
    camera_state: dict | None = None
    filters: dict | None = None
    pinned_nodes: list[str] | None = None
    annotations: list[dict] | None = None


# ══════════════════════════════════════════════════════════════════════════
# ENDPOINT 1 — GET /graph — Unified canvas graph
# ══════════════════════════════════════════════════════════════════════════

@router.get("/graph")
async def get_canvas_graph(
    center: str = Query(..., description="Actor ID or ticker symbol"),
    depth: int = Query(2, ge=1, le=4, description="BFS depth (1-4 hops)"),
    layers: str = Query("all", description="Comma-separated layers to include"),
    since: str | None = Query(None, description="ISO date for temporal filter"),
    limit: int = Query(500, ge=10, le=2000, description="Max nodes returned"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the unified canvas graph centered on an actor or ticker.

    BFS traverses actor_connections to the specified depth, loads signals,
    wealth flows, dollar flows, co-trading patterns, and ticker nodes.
    Applies layer filters and prioritizes by influence_score.
    """
    engine = get_db_engine()
    active_layers = _parse_layers(layers)

    try:
        center_entity = _resolve_center(engine, center)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(404, f"Failed to resolve center: {exc}")

    nodes: list[dict] = []
    edges: list[dict] = []
    all_actor_ids: list[str] = []
    tickers_seen: set[str] = set()

    if center_entity["entity_type"] == "actor":
        # BFS from actor
        actor_data, connection_edges = _bfs_actors(
            engine, center_entity["id"], depth, limit,
        )
        all_actor_ids = list(actor_data.keys())

        # Build actor nodes
        for aid, data in actor_data.items():
            nodes.append({
                "id": f"a:{aid}",
                "type": "actor",
                "name": data.get("name", aid),
                "tier": data.get("tier", "unknown"),
                "category": data.get("category", "unknown"),
                "influence": float(data.get("influence_score", 0.5)),
                "trust_score": float(data.get("trust_score", 0.5)),
                "title": data.get("title", ""),
                "is_center": aid == center_entity["id"],
            })

        edges.extend(connection_edges)

        # Load signals for actors
        sig_nodes, sig_edges, sig_tickers = _load_signals_for_actors(
            engine, all_actor_ids, since, limit,
        )
        nodes.extend(sig_nodes)
        edges.extend(sig_edges)
        tickers_seen.update(sig_tickers)

    elif center_entity["entity_type"] == "ticker":
        # Start from ticker — find related actors
        ticker = center_entity["id"]
        tickers_seen.add(ticker)

        sig_nodes, sig_edges, related_actors = _load_signals_for_ticker(
            engine, ticker, since, limit,
        )
        nodes.extend(sig_nodes)
        edges.extend(sig_edges)

        # Load actor data for discovered actors
        with engine.connect() as conn:
            for actor_id in list(related_actors)[:limit]:
                row = conn.execute(
                    text(
                        "SELECT id, name, tier, category, influence_score, "
                        "       trust_score, title "
                        "FROM actors WHERE id = :aid OR LOWER(name) = LOWER(:aid) "
                        "LIMIT 1"
                    ).bindparams(aid=actor_id),
                ).mappings().fetchone()

                if row:
                    resolved_id = row["id"]
                    if resolved_id not in [a for a in all_actor_ids]:
                        all_actor_ids.append(resolved_id)
                        nodes.append({
                            "id": f"a:{resolved_id}",
                            "type": "actor",
                            "name": row["name"],
                            "tier": row["tier"],
                            "category": row["category"],
                            "influence": float(row["influence_score"] or 0.5),
                            "trust_score": float(row["trust_score"] or 0.5),
                            "title": row["title"],
                            "is_center": False,
                        })

        # BFS from discovered actors if depth > 1
        if depth > 1 and all_actor_ids:
            for aid in all_actor_ids[:10]:
                actor_data, conn_edges = _bfs_actors(
                    engine, aid, depth - 1, limit - len(nodes),
                )
                for neighbor_id, data in actor_data.items():
                    if not any(n["id"] == f"a:{neighbor_id}" for n in nodes):
                        nodes.append({
                            "id": f"a:{neighbor_id}",
                            "type": "actor",
                            "name": data.get("name", neighbor_id),
                            "tier": data.get("tier", "unknown"),
                            "category": data.get("category", "unknown"),
                            "influence": float(data.get("influence_score", 0.5)),
                            "trust_score": float(data.get("trust_score", 0.5)),
                            "title": data.get("title", ""),
                            "is_center": False,
                        })
                edges.extend(conn_edges)

    # Wealth flows
    try:
        flow_edges = _load_wealth_flows(engine, all_actor_ids)
        edges.extend(flow_edges)
    except Exception as exc:
        log.debug("Wealth flow loading failed: {e}", e=str(exc))

    # Dollar flows
    try:
        dollar_edges = _load_dollar_flows(engine, all_actor_ids)
        edges.extend(dollar_edges)
    except Exception as exc:
        log.debug("Dollar flow loading failed: {e}", e=str(exc))

    # Co-traded detection
    try:
        co_traded_edges = _detect_co_traded(engine, all_actor_ids)
        edges.extend(co_traded_edges)
    except Exception as exc:
        log.debug("Co-traded detection failed: {e}", e=str(exc))

    # Build ticker nodes
    try:
        ticker_nodes = _build_ticker_nodes(engine, tickers_seen)
        nodes.extend(ticker_nodes)
    except Exception as exc:
        log.debug("Ticker node building failed: {e}", e=str(exc))

    # ── Apply layer filters ──
    allowed_sources = _get_allowed_source_types(active_layers)
    allowed_categories = _get_allowed_categories(active_layers)

    if layers != "all":
        # Filter signal nodes by source type
        nodes = [
            n for n in nodes
            if n.get("type") != "signal"
            or n.get("source_type", "").lower() in allowed_sources
            or not allowed_sources
        ]
        # Filter actor nodes by category (keep center always)
        if allowed_categories:
            nodes = [
                n for n in nodes
                if n.get("type") != "actor"
                or n.get("category", "").lower() in allowed_categories
                or n.get("is_center", False)
            ]

    # ── Prioritize by influence and apply limit ──
    actor_nodes = sorted(
        [n for n in nodes if n.get("type") == "actor"],
        key=lambda n: n.get("influence", 0),
        reverse=True,
    )[:limit]
    other_nodes = [n for n in nodes if n.get("type") != "actor"][:limit]
    nodes = actor_nodes + other_nodes

    # Keep only edges whose endpoints are in the node set
    node_ids = {n["id"] for n in nodes}
    edges = [
        e for e in edges
        if e.get("source") in node_ids and e.get("target") in node_ids
    ]

    # Deduplicate edges
    edge_keys: set[tuple[str, str, str]] = set()
    deduped_edges: list[dict] = []
    for e in edges:
        key = (e.get("source", ""), e.get("target", ""), e.get("type", ""))
        if key not in edge_keys:
            edge_keys.add(key)
            deduped_edges.append(e)
    edges = deduped_edges

    # Signal counts per actor
    signal_counts: dict[str, int] = defaultdict(int)
    for n in nodes:
        if n.get("type") == "signal" and n.get("actor"):
            signal_counts[n["actor"]] += 1

    for n in nodes:
        if n.get("type") == "actor":
            aid = n["id"].removeprefix("a:")
            n["signal_count"] = signal_counts.get(aid, 0)

    metadata = {
        "center": center,
        "center_type": center_entity["entity_type"],
        "depth": depth,
        "layers": list(active_layers),
        "node_count": len(nodes),
        "edge_count": len(edges),
        "actor_count": sum(1 for n in nodes if n.get("type") == "actor"),
        "ticker_count": sum(1 for n in nodes if n.get("type") == "ticker"),
        "signal_count": sum(1 for n in nodes if n.get("type") == "signal"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    return {"nodes": nodes, "edges": edges, "metadata": metadata}


# ══════════════════════════════════════════════════════════════════════════
# ENDPOINT 2 — GET /node/{node_type}/{node_id} — Detail panel
# ══════════════════════════════════════════════════════════════════════════

@router.get("/node/{node_type}/{node_id}")
async def get_node_detail(
    node_type: str,
    node_id: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return rich detail for the selected node.

    For actors: trust breakdown, recent actions, wealth flows, board seats,
    political affiliations, connected actors.
    For tickers: sector, related actors, recent signals, options positioning.
    """
    engine = get_db_engine()

    if node_type == "actor":
        return await _actor_detail(engine, node_id)
    elif node_type == "ticker":
        return await _ticker_detail(engine, node_id)
    elif node_type == "signal":
        return await _signal_detail(engine, node_id)
    else:
        raise HTTPException(400, f"Unknown node_type: {node_type}")


async def _actor_detail(engine: Engine, actor_id: str) -> dict[str, Any]:
    """Detailed actor information."""
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT id, name, tier, category, influence_score, trust_score, "
                "       title, connections, board_seats, net_worth_estimate, aum "
                "FROM actors WHERE id = :aid LIMIT 1"
            ).bindparams(aid=actor_id),
        ).mappings().fetchone()

        if not row:
            raise HTTPException(404, f"Actor '{actor_id}' not found")

        actor = dict(row)

        # Parse JSONB fields
        for field_name in ("connections", "board_seats"):
            val = actor.get(field_name)
            if isinstance(val, str):
                try:
                    actor[field_name] = json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    actor[field_name] = []

        # Recent signals (30 days)
        signals = conn.execute(
            text(
                "SELECT signal_type, signal_date, ticker, direction, magnitude, "
                "       confidence, description "
                "FROM signal_data "
                "WHERE actor = :aid AND signal_date >= CURRENT_DATE - 30 "
                "ORDER BY signal_date DESC LIMIT 50"
            ).bindparams(aid=actor_id),
        ).mappings().fetchall()

        # Wealth flows in/out
        outflows = conn.execute(
            text(
                "SELECT to_entity, amount_estimate, confidence, flow_date "
                "FROM wealth_flows WHERE from_actor = :aid "
                "ORDER BY flow_date DESC LIMIT 20"
            ).bindparams(aid=actor_id),
        ).mappings().fetchall()

        inflows = conn.execute(
            text(
                "SELECT from_actor, amount_estimate, confidence, flow_date "
                "FROM wealth_flows WHERE to_entity = :aid "
                "ORDER BY flow_date DESC LIMIT 20"
            ).bindparams(aid=actor_id),
        ).mappings().fetchall()

        # Connected actors (top 20 by strength)
        connected = conn.execute(
            text(
                "SELECT ac.from_actor, ac.to_actor, ac.connection_type, ac.strength, "
                "       a.name, a.tier, a.influence_score "
                "FROM actor_connections ac "
                "JOIN actors a ON (a.id = CASE "
                "    WHEN ac.from_actor = :aid THEN ac.to_actor "
                "    ELSE ac.from_actor END) "
                "WHERE ac.from_actor = :aid OR ac.to_actor = :aid "
                "ORDER BY ac.strength DESC LIMIT 20"
            ).bindparams(aid=actor_id),
        ).mappings().fetchall()

        # Dollar flows for this actor
        dollar_flows = conn.execute(
            text(
                "SELECT source_type, ticker, amount_usd, direction, confidence, "
                "       flow_date "
                "FROM dollar_flows WHERE actor_name = :aid "
                "ORDER BY flow_date DESC LIMIT 30"
            ).bindparams(aid=actor_id),
        ).mappings().fetchall()

    return {
        "node_type": "actor",
        "actor": {
            "id": actor["id"],
            "name": actor["name"],
            "tier": actor["tier"],
            "category": actor["category"],
            "influence_score": float(actor.get("influence_score", 0.5)),
            "trust_score": float(actor.get("trust_score", 0.5)),
            "title": actor.get("title"),
            "net_worth_estimate": actor.get("net_worth_estimate"),
            "aum": actor.get("aum"),
            "board_seats": actor.get("board_seats", []),
        },
        "recent_signals": [dict(s) for s in signals],
        "wealth_flows_out": [dict(f) for f in outflows],
        "wealth_flows_in": [dict(f) for f in inflows],
        "dollar_flows": [dict(f) for f in dollar_flows],
        "connected_actors": [dict(c) for c in connected],
    }


async def _ticker_detail(engine: Engine, ticker: str) -> dict[str, Any]:
    """Detailed ticker information."""
    ticker_upper = ticker.upper()

    with engine.connect() as conn:
        # Signal count and breakdown
        signal_breakdown = conn.execute(
            text(
                "SELECT signal_type, direction, COUNT(*) as cnt, "
                "       AVG(confidence) as avg_confidence "
                "FROM signal_data WHERE UPPER(ticker) = :t "
                "GROUP BY signal_type, direction "
                "ORDER BY cnt DESC"
            ).bindparams(t=ticker_upper),
        ).mappings().fetchall()

        # Recent signals
        recent_signals = conn.execute(
            text(
                "SELECT signal_type, signal_date, actor, direction, magnitude, "
                "       confidence, description "
                "FROM signal_data WHERE UPPER(ticker) = :t "
                "ORDER BY signal_date DESC LIMIT 50"
            ).bindparams(t=ticker_upper),
        ).mappings().fetchall()

        # Related actors (distinct actors who generated signals)
        related_actors = conn.execute(
            text(
                "SELECT DISTINCT sd.actor, a.name, a.tier, a.influence_score, "
                "       COUNT(*) as signal_count "
                "FROM signal_data sd "
                "LEFT JOIN actors a ON a.id = sd.actor "
                "WHERE UPPER(sd.ticker) = :t AND sd.actor IS NOT NULL "
                "GROUP BY sd.actor, a.name, a.tier, a.influence_score "
                "ORDER BY signal_count DESC LIMIT 30"
            ).bindparams(t=ticker_upper),
        ).mappings().fetchall()

        # Dollar flows for this ticker
        dollar_flows = conn.execute(
            text(
                "SELECT source_type, actor_name, amount_usd, direction, "
                "       confidence, flow_date "
                "FROM dollar_flows WHERE UPPER(ticker) = :t "
                "ORDER BY flow_date DESC LIMIT 30"
            ).bindparams(t=ticker_upper),
        ).mappings().fetchall()

        # Options positioning from signal_registry
        options_signals = conn.execute(
            text(
                "SELECT signal_type, direction, value, z_score, confidence, "
                "       valid_from, valid_until "
                "FROM signal_registry "
                "WHERE UPPER(ticker) = :t "
                "AND signal_type IN ('gex', 'vanna', 'charm', 'dealer_gamma', "
                "                    'options_flow', 'put_call_ratio') "
                "AND (valid_until IS NULL OR valid_until >= CURRENT_DATE) "
                "ORDER BY valid_from DESC LIMIT 20"
            ).bindparams(t=ticker_upper),
        ).mappings().fetchall()

    return {
        "node_type": "ticker",
        "ticker": ticker_upper,
        "signal_breakdown": [dict(s) for s in signal_breakdown],
        "recent_signals": [dict(s) for s in recent_signals],
        "related_actors": [dict(a) for a in related_actors],
        "dollar_flows": [dict(f) for f in dollar_flows],
        "options_positioning": [dict(o) for o in options_signals],
    }


async def _signal_detail(engine: Engine, signal_id: str) -> dict[str, Any]:
    """Detailed signal information."""
    # signal_id may be numeric or have the s: prefix stripped already
    try:
        sid = int(signal_id)
    except ValueError:
        raise HTTPException(400, f"Invalid signal ID: {signal_id}")

    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT id, signal_type, signal_date, ticker, actor, direction, "
                "       magnitude, confidence, description "
                "FROM signal_data WHERE id = :sid LIMIT 1"
            ).bindparams(sid=sid),
        ).mappings().fetchone()

        if not row:
            raise HTTPException(404, f"Signal {signal_id} not found")

    return {
        "node_type": "signal",
        "signal": dict(row),
    }


# ══════════════════════════════════════════════════════════════════════════
# ENDPOINT 3 — GET /expand/{node_type}/{node_id} — Incremental expansion
# ══════════════════════════════════════════════════════════════════════════

@router.get("/expand/{node_type}/{node_id}")
async def expand_node(
    node_type: str,
    node_id: str,
    depth: int = Query(1, ge=1, le=3, description="Expansion depth"),
    layers: str = Query("all", description="Comma-separated layers"),
    existing_ids: str = Query("", description="Comma-separated existing node IDs"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Expand from a node — return only NEW nodes and edges not already on the graph.

    Pass existing_ids to exclude already-rendered nodes.
    """
    engine = get_db_engine()
    active_layers = _parse_layers(layers)
    existing: set[str] = set()
    if existing_ids:
        existing = {eid.strip() for eid in existing_ids.split(",") if eid.strip()}

    new_nodes: list[dict] = []
    new_edges: list[dict] = []

    if node_type == "actor":
        # BFS from this actor
        actor_data, conn_edges = _bfs_actors(engine, node_id, depth, 200)

        for aid, data in actor_data.items():
            nid = f"a:{aid}"
            if nid not in existing:
                new_nodes.append({
                    "id": nid,
                    "type": "actor",
                    "name": data.get("name", aid),
                    "tier": data.get("tier", "unknown"),
                    "category": data.get("category", "unknown"),
                    "influence": float(data.get("influence_score", 0.5)),
                    "trust_score": float(data.get("trust_score", 0.5)),
                    "title": data.get("title", ""),
                    "is_center": False,
                })

        # Only include edges that connect to at least one new node
        new_node_ids = {n["id"] for n in new_nodes}
        all_known = existing | new_node_ids
        for e in conn_edges:
            if e["source"] in all_known and e["target"] in all_known:
                if e["source"] in new_node_ids or e["target"] in new_node_ids:
                    new_edges.append(e)

        # Signals for new actors
        new_actor_ids = [n["id"].removeprefix("a:") for n in new_nodes]
        sig_nodes, sig_edges, tickers = _load_signals_for_actors(
            engine, new_actor_ids, None, 100,
        )
        for sn in sig_nodes:
            if sn["id"] not in existing:
                new_nodes.append(sn)
        new_edges.extend(sig_edges)

        # Ticker nodes
        for tn in _build_ticker_nodes(engine, tickers):
            if tn["id"] not in existing:
                new_nodes.append(tn)

    elif node_type == "ticker":
        # Find actors related to this ticker
        sig_nodes, sig_edges, actor_ids = _load_signals_for_ticker(
            engine, node_id, None, 100,
        )
        for sn in sig_nodes:
            if sn["id"] not in existing:
                new_nodes.append(sn)
        new_edges.extend(sig_edges)

        # Load actor details
        with engine.connect() as conn:
            for actor_id in list(actor_ids)[:50]:
                row = conn.execute(
                    text(
                        "SELECT id, name, tier, category, influence_score, "
                        "       trust_score, title "
                        "FROM actors WHERE id = :aid OR LOWER(name) = LOWER(:aid) "
                        "LIMIT 1"
                    ).bindparams(aid=actor_id),
                ).mappings().fetchone()

                if row:
                    nid = f"a:{row['id']}"
                    if nid not in existing:
                        new_nodes.append({
                            "id": nid,
                            "type": "actor",
                            "name": row["name"],
                            "tier": row["tier"],
                            "category": row["category"],
                            "influence": float(row["influence_score"] or 0.5),
                            "trust_score": float(row["trust_score"] or 0.5),
                            "title": row["title"],
                            "is_center": False,
                        })

    else:
        raise HTTPException(400, f"Cannot expand node_type: {node_type}")

    # Apply layer filters
    allowed_sources = _get_allowed_source_types(active_layers)
    if layers != "all" and allowed_sources:
        new_nodes = [
            n for n in new_nodes
            if n.get("type") != "signal"
            or n.get("source_type", "").lower() in allowed_sources
        ]

    # Prune edges to valid nodes
    all_ids = existing | {n["id"] for n in new_nodes}
    new_edges = [
        e for e in new_edges
        if e.get("source") in all_ids and e.get("target") in all_ids
    ]

    return {
        "new_nodes": new_nodes,
        "new_edges": new_edges,
        "metadata": {
            "expanded_from": f"{node_type}:{node_id}",
            "new_node_count": len(new_nodes),
            "new_edge_count": len(new_edges),
        },
    }


# ══════════════════════════════════════════════════════════════════════════
# ENDPOINT 4 — Board CRUD
# ══════════════════════════════════════════════════════════════════════════

@router.post("/boards")
async def create_board(
    body: BoardCreate,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Create a new investigation board."""
    engine = get_db_engine()
    _ensure_boards_table(engine)

    board_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO investigation_boards (id, name, description) "
                "VALUES (:id, :name, :desc)"
            ).bindparams(id=board_id, name=body.name, desc=body.description),
        )

    return {
        "id": board_id,
        "name": body.name,
        "description": body.description,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/boards")
async def list_boards(
    _token: str = Depends(require_auth),
) -> list[dict[str, Any]]:
    """List all investigation boards."""
    engine = get_db_engine()
    _ensure_boards_table(engine)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id, name, description, "
                "       COALESCE(jsonb_array_length(graph_state->'nodes'), 0) as node_count, "
                "       created_at, updated_at "
                "FROM investigation_boards "
                "ORDER BY updated_at DESC"
            ),
        ).mappings().fetchall()

    return [
        {
            "id": r["id"],
            "name": r["name"],
            "description": r["description"],
            "node_count": r["node_count"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
        }
        for r in rows
    ]


@router.get("/boards/{board_id}")
async def get_board(
    board_id: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get full board state including graph, camera, filters."""
    engine = get_db_engine()
    _ensure_boards_table(engine)

    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT id, name, description, graph_state, camera_state, "
                "       filters, pinned_nodes, annotations, created_at, updated_at "
                "FROM investigation_boards WHERE id = :bid"
            ).bindparams(bid=board_id),
        ).mappings().fetchone()

    if not row:
        raise HTTPException(404, f"Board '{board_id}' not found")

    result = dict(row)
    for ts_field in ("created_at", "updated_at"):
        if result.get(ts_field):
            result[ts_field] = result[ts_field].isoformat()

    return result


@router.put("/boards/{board_id}")
async def update_board(
    board_id: str,
    body: BoardUpdate,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Update board state (graph, camera, filters, name, etc.)."""
    engine = get_db_engine()
    _ensure_boards_table(engine)

    # Build dynamic SET clause from non-None fields
    updates: dict[str, Any] = {}
    if body.name is not None:
        updates["name"] = body.name
    if body.description is not None:
        updates["description"] = body.description
    if body.graph_state is not None:
        updates["graph_state"] = json.dumps(body.graph_state)
    if body.camera_state is not None:
        updates["camera_state"] = json.dumps(body.camera_state)
    if body.filters is not None:
        updates["filters"] = json.dumps(body.filters)
    if body.pinned_nodes is not None:
        updates["pinned_nodes"] = body.pinned_nodes
    if body.annotations is not None:
        updates["annotations"] = json.dumps(body.annotations)

    if not updates:
        raise HTTPException(400, "No fields to update")

    # Build parameterized SET clause
    set_parts = []
    params: dict[str, Any] = {"bid": board_id}
    for i, (col, val) in enumerate(updates.items()):
        param_name = f"v{i}"
        if col == "pinned_nodes":
            set_parts.append(f"{col} = :{param_name}")
            params[param_name] = val
        elif col in ("graph_state", "camera_state", "filters", "annotations"):
            set_parts.append(f"{col} = CAST(:{param_name} AS JSONB)")
            params[param_name] = val
        else:
            set_parts.append(f"{col} = :{param_name}")
            params[param_name] = val

    set_parts.append("updated_at = NOW()")
    set_clause = ", ".join(set_parts)

    with engine.begin() as conn:
        result = conn.execute(
            text(
                f"UPDATE investigation_boards SET {set_clause} "
                f"WHERE id = :bid RETURNING id"
            ).bindparams(**params),
        )
        if result.rowcount == 0:
            raise HTTPException(404, f"Board '{board_id}' not found")

    return {"id": board_id, "status": "updated"}


@router.delete("/boards/{board_id}")
async def delete_board(
    board_id: str,
    _token: str = Depends(require_auth),
) -> dict[str, str]:
    """Delete an investigation board."""
    engine = get_db_engine()
    _ensure_boards_table(engine)

    with engine.begin() as conn:
        result = conn.execute(
            text("DELETE FROM investigation_boards WHERE id = :bid").bindparams(
                bid=board_id,
            ),
        )
        if result.rowcount == 0:
            raise HTTPException(404, f"Board '{board_id}' not found")

    return {"id": board_id, "status": "deleted"}


@router.post("/boards/{board_id}/fork")
async def fork_board(
    board_id: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Duplicate an investigation board."""
    engine = get_db_engine()
    _ensure_boards_table(engine)

    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT name, description, graph_state, camera_state, filters, "
                "       pinned_nodes, annotations "
                "FROM investigation_boards WHERE id = :bid"
            ).bindparams(bid=board_id),
        ).mappings().fetchone()

    if not row:
        raise HTTPException(404, f"Board '{board_id}' not found")

    new_id = str(uuid.uuid4())
    new_name = f"{row['name']} (copy)"

    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO investigation_boards "
                "(id, name, description, graph_state, camera_state, filters, "
                " pinned_nodes, annotations) "
                "VALUES (:id, :name, :desc, :gs, :cs, :fl, :pn, :an)"
            ).bindparams(
                id=new_id,
                name=new_name,
                desc=row["description"],
                gs=json.dumps(row["graph_state"]) if isinstance(row["graph_state"], dict) else row["graph_state"],
                cs=json.dumps(row["camera_state"]) if isinstance(row["camera_state"], dict) else row["camera_state"],
                fl=json.dumps(row["filters"]) if isinstance(row["filters"], dict) else row["filters"],
                pn=row["pinned_nodes"],
                an=json.dumps(row["annotations"]) if isinstance(row["annotations"], list) else row["annotations"],
            ),
        )

    return {
        "id": new_id,
        "name": new_name,
        "forked_from": board_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


# ══════════════════════════════════════════════════════════════════════════
# ENDPOINT 5 — GET /dots — Cross-reference dot connections
# ══════════════════════════════════════════════════════════════════════════

@router.get("/dots")
async def get_dot_connections(
    center: str = Query(..., description="Actor ID or ticker symbol"),
    days: int = Query(30, ge=1, le=365, description="Lookback window"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Cross-reference intelligence: find ALL the ways things connect.

    Detects insider clusters, congressional timing suspicion, whale flow
    convergence, money trails, signal divergence, board interlocks, and
    offshore connections.
    """
    engine = get_db_engine()
    connections: list[dict[str, Any]] = []

    try:
        center_entity = _resolve_center(engine, center)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(404, f"Failed to resolve center: {exc}")

    since_date = (datetime.now(timezone.utc) - timedelta(days=days)).date()

    with engine.connect() as conn:
        if center_entity["entity_type"] == "ticker":
            ticker = center_entity["id"]

            # ── Insider clusters ──
            try:
                insider_rows = conn.execute(
                    text(
                        "SELECT actor, direction, signal_date, magnitude, confidence "
                        "FROM signal_data "
                        "WHERE UPPER(ticker) = :ticker "
                        "AND signal_type IN ('insider', 'form4', 'insider_cluster') "
                        "AND signal_date >= :since "
                        "ORDER BY signal_date DESC"
                    ).bindparams(ticker=ticker.upper(), since=since_date),
                ).mappings().fetchall()

                if len(insider_rows) >= 2:
                    actors = list({r["actor"] for r in insider_rows if r["actor"]})
                    directions = [r["direction"] for r in insider_rows if r["direction"]]
                    buy_pct = directions.count("bullish") / len(directions) if directions else 0

                    connections.append({
                        "type": "insider_cluster",
                        "actors": actors,
                        "ticker": ticker,
                        "evidence": [
                            f"{len(insider_rows)} insider transactions in {days}d",
                            f"{buy_pct:.0%} bullish alignment",
                        ],
                        "confidence": min(0.9, 0.3 + len(insider_rows) * 0.1),
                        "description": (
                            f"Insider cluster: {len(actors)} insiders traded {ticker} — "
                            f"{buy_pct:.0%} bullish consensus"
                        ),
                    })
            except Exception as exc:
                log.debug("Insider cluster check failed: {e}", e=str(exc))

            # ── Congressional timing ──
            try:
                congress_rows = conn.execute(
                    text(
                        "SELECT actor, direction, signal_date, confidence "
                        "FROM signal_data "
                        "WHERE UPPER(ticker) = :ticker "
                        "AND signal_type IN ('congressional', 'congress') "
                        "AND signal_date >= :since "
                        "ORDER BY signal_date DESC"
                    ).bindparams(ticker=ticker.upper(), since=since_date),
                ).mappings().fetchall()

                # Check if any congressional trades preceded insider buys
                for cr in congress_rows:
                    if not cr["signal_date"]:
                        continue
                    cr_date = cr["signal_date"] if isinstance(cr["signal_date"], date) else date.fromisoformat(str(cr["signal_date"]))
                    for ir in insider_rows if 'insider_rows' in dir() else []:
                        if not ir["signal_date"]:
                            continue
                        ir_date = ir["signal_date"] if isinstance(ir["signal_date"], date) else date.fromisoformat(str(ir["signal_date"]))
                        delta = (ir_date - cr_date).days
                        if 0 < delta <= 14:
                            connections.append({
                                "type": "congressional_timing",
                                "actors": [cr["actor"], ir["actor"]],
                                "ticker": ticker,
                                "evidence": [
                                    f"Congress: {cr['actor']} traded {delta}d before insider {ir['actor']}",
                                    f"Congressional direction: {cr['direction']}",
                                ],
                                "confidence": min(0.85, 0.5 + (14 - delta) * 0.025),
                                "description": (
                                    f"Timing suspicion: {cr['actor']} (Congress) traded {ticker} "
                                    f"{delta} days before insider {ir['actor']}"
                                ),
                            })
            except Exception as exc:
                log.debug("Congressional timing check failed: {e}", e=str(exc))

            # ── Whale flow convergence ──
            try:
                whale_rows = conn.execute(
                    text(
                        "SELECT signal_type, direction, magnitude, confidence, signal_date "
                        "FROM signal_data "
                        "WHERE UPPER(ticker) = :ticker "
                        "AND signal_type IN ('darkpool', 'options_flow', 'whale_options', "
                        "                    '13f', 'etf_flow', 'institutional') "
                        "AND signal_date >= :since "
                        "ORDER BY signal_date DESC"
                    ).bindparams(ticker=ticker.upper(), since=since_date),
                ).mappings().fetchall()

                if len(whale_rows) >= 2:
                    source_types = list({r["signal_type"] for r in whale_rows})
                    directions = [r["direction"] for r in whale_rows if r["direction"]]
                    if directions:
                        most_common = max(set(directions), key=directions.count)
                        agreement = directions.count(most_common) / len(directions)

                        connections.append({
                            "type": "whale_convergence",
                            "actors": [],
                            "ticker": ticker,
                            "evidence": [
                                f"{len(source_types)} whale sources: {', '.join(source_types)}",
                                f"{agreement:.0%} directional agreement ({most_common})",
                            ],
                            "confidence": min(0.95, agreement * 0.8 + len(source_types) * 0.05),
                            "description": (
                                f"Whale convergence on {ticker}: {len(source_types)} sources "
                                f"agree {agreement:.0%} {most_common}"
                            ),
                        })
            except Exception as exc:
                log.debug("Whale convergence check failed: {e}", e=str(exc))

            # ── Signal divergence (smart money vs retail) ──
            try:
                all_signals = conn.execute(
                    text(
                        "SELECT signal_type, direction, confidence "
                        "FROM signal_data "
                        "WHERE UPPER(ticker) = :ticker AND signal_date >= :since "
                        "AND direction IS NOT NULL"
                    ).bindparams(ticker=ticker.upper(), since=since_date),
                ).mappings().fetchall()

                smart_money_types = {"insider", "form4", "darkpool", "13f", "congressional"}
                retail_types = {"social", "sentiment", "news"}

                smart_dirs = [r["direction"] for r in all_signals if r["signal_type"] in smart_money_types]
                retail_dirs = [r["direction"] for r in all_signals if r["signal_type"] in retail_types]

                if smart_dirs and retail_dirs:
                    smart_bull = smart_dirs.count("bullish") / len(smart_dirs)
                    retail_bull = retail_dirs.count("bullish") / len(retail_dirs)
                    divergence = abs(smart_bull - retail_bull)

                    if divergence > 0.3:
                        connections.append({
                            "type": "signal_divergence",
                            "actors": [],
                            "ticker": ticker,
                            "evidence": [
                                f"Smart money {smart_bull:.0%} bullish vs retail {retail_bull:.0%} bullish",
                                f"Divergence: {divergence:.0%}",
                            ],
                            "confidence": min(0.9, divergence),
                            "description": (
                                f"Smart-retail divergence on {ticker}: smart money "
                                f"{'bullish' if smart_bull > 0.5 else 'bearish'} while retail "
                                f"{'bullish' if retail_bull > 0.5 else 'bearish'}"
                            ),
                        })
            except Exception as exc:
                log.debug("Signal divergence check failed: {e}", e=str(exc))

        elif center_entity["entity_type"] == "actor":
            actor_id = center_entity["id"]

            # ── Board interlocks ──
            try:
                actor_row = conn.execute(
                    text(
                        "SELECT board_seats, connections FROM actors WHERE id = :aid"
                    ).bindparams(aid=actor_id),
                ).mappings().fetchone()

                if actor_row:
                    board_seats = actor_row["board_seats"]
                    if isinstance(board_seats, str):
                        try:
                            board_seats = json.loads(board_seats)
                        except (json.JSONDecodeError, TypeError):
                            board_seats = []

                    if board_seats:
                        # Find other actors who share board seats
                        for seat in board_seats if isinstance(board_seats, list) else []:
                            seat_str = str(seat)
                            shared = conn.execute(
                                text(
                                    "SELECT id, name FROM actors "
                                    "WHERE id != :aid "
                                    "AND board_seats::text ILIKE :seat "
                                    "LIMIT 10"
                                ).bindparams(aid=actor_id, seat=f"%{seat_str}%"),
                            ).mappings().fetchall()

                            for s in shared:
                                connections.append({
                                    "type": "board_interlock",
                                    "actors": [actor_id, s["id"]],
                                    "evidence": [
                                        f"Both serve on: {seat_str}",
                                    ],
                                    "confidence": 0.95,
                                    "description": (
                                        f"Board interlock: {center_entity['name']} and {s['name']} "
                                        f"both connected to {seat_str}"
                                    ),
                                })
            except Exception as exc:
                log.debug("Board interlock check failed: {e}", e=str(exc))

            # ── Money trails (A -> B -> C) ──
            try:
                direct_flows = conn.execute(
                    text(
                        "SELECT to_entity, amount_estimate, confidence "
                        "FROM wealth_flows WHERE from_actor = :aid "
                        "ORDER BY amount_estimate DESC LIMIT 20"
                    ).bindparams(aid=actor_id),
                ).mappings().fetchall()

                for flow in direct_flows:
                    # Follow the chain one more hop
                    second_hop = conn.execute(
                        text(
                            "SELECT to_entity, amount_estimate "
                            "FROM wealth_flows WHERE from_actor = :mid "
                            "ORDER BY amount_estimate DESC LIMIT 5"
                        ).bindparams(mid=flow["to_entity"]),
                    ).mappings().fetchall()

                    for sh in second_hop:
                        connections.append({
                            "type": "money_trail",
                            "actors": [actor_id, flow["to_entity"], sh["to_entity"]],
                            "evidence": [
                                f"{actor_id} -> {flow['to_entity']} (${flow['amount_estimate']:,.0f})" if flow['amount_estimate'] else f"{actor_id} -> {flow['to_entity']}",
                                f"{flow['to_entity']} -> {sh['to_entity']} (${sh['amount_estimate']:,.0f})" if sh['amount_estimate'] else f"{flow['to_entity']} -> {sh['to_entity']}",
                            ],
                            "confidence": 0.6,
                            "description": (
                                f"Money trail: {actor_id} -> {flow['to_entity']} -> {sh['to_entity']}"
                            ),
                        })
            except Exception as exc:
                log.debug("Money trail check failed: {e}", e=str(exc))

            # ── Offshore connections ──
            try:
                offshore = conn.execute(
                    text(
                        "SELECT signal_type, description, signal_date, confidence "
                        "FROM signal_data "
                        "WHERE actor = :aid "
                        "AND signal_type IN ('panama', 'pandora', 'icij', 'offshore') "
                        "ORDER BY signal_date DESC LIMIT 10"
                    ).bindparams(aid=actor_id),
                ).mappings().fetchall()

                if offshore:
                    connections.append({
                        "type": "offshore_connection",
                        "actors": [actor_id],
                        "evidence": [
                            f"{len(offshore)} offshore records found",
                            *(r["description"] for r in offshore[:3] if r["description"]),
                        ],
                        "confidence": max(float(r["confidence"] or 0.5) for r in offshore),
                        "description": (
                            f"Offshore connections: {center_entity['name']} linked to "
                            f"{len(offshore)} offshore entities"
                        ),
                    })
            except Exception as exc:
                log.debug("Offshore connection check failed: {e}", e=str(exc))

    return {
        "center": center,
        "center_type": center_entity["entity_type"],
        "connections": connections,
        "total": len(connections),
        "lookback_days": days,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
