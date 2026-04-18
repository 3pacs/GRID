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
from api.routers.canvas_board_store import (
    delete_legacy_canvas_board,
    sync_legacy_canvas_from_board,
)

router = APIRouter(prefix="/api/v1/canvas", tags=["canvas"])

# Mount the decomposed sub-routers that hold the endpoints the pwa
# frontend calls (add/edit/delete node + edge, expand, suggest-connections,
# investigate, explain, predict). These files were split out of canvas.py
# but never wired up at the facade. canvas_core is intentionally NOT
# mounted here — its /boards routes collide with this file's own /boards
# routes and that refactor is still mid-flight.
try:
    from api.routers.canvas_expand import router as _canvas_expand_router
    from api.routers.canvas_graph import router as _canvas_graph_router
    from api.routers.canvas_investigate import router as _canvas_investigate_router
    from api.routers.canvas_llm import router as _canvas_llm_router
    from api.routers.canvas_predict import router as _canvas_predict_router

    router.include_router(_canvas_graph_router)
    router.include_router(_canvas_expand_router)
    router.include_router(_canvas_investigate_router)
    router.include_router(_canvas_llm_router)
    router.include_router(_canvas_predict_router)
except Exception as _canvas_subrouter_exc:  # pragma: no cover — defensive
    log.warning(
        "canvas facade: sub-router wiring failed — {e}",
        e=_canvas_subrouter_exc,
    )


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
_CANVAS_GRAPH_CACHE_TTL_SECONDS = 60
_canvas_graph_cache: dict[tuple[str, int, str, str | None, int], tuple[datetime, dict[str, Any]]] = {}


def _strip_canvas_graph_id(node_type: str, node_id: str) -> str:
    """Convert graph node IDs like ``a:corp_x``, ``t:NVDA``, ``s:123`` to DB IDs."""
    nid = str(node_id or "")
    if node_type == "actor" and nid.startswith("a:"):
        return nid[2:]
    if node_type == "ticker" and nid.startswith("t:"):
        return nid[2:]
    if node_type == "signal" and nid.startswith("s:"):
        return nid[2:]
    return nid


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

_ACTOR_ID_PREFIXES = ("corp_", "ticker_", "person_", "govt_", "org_", "fund_")


def _strip_actor_technical_prefix(value: Any) -> str:
    """Remove internal graph/actor prefixes while preserving the original case."""
    if value is None:
        return ""

    text_value = str(value).strip()
    if text_value.lower().startswith("a:"):
        text_value = text_value[2:]

    lower_value = text_value.lower()
    for prefix in _ACTOR_ID_PREFIXES:
        if lower_value.startswith(prefix):
            return text_value[len(prefix):].strip()

    return text_value


def _looks_like_technical_actor_id(value: Any) -> bool:
    if value is None:
        return True
    text_value = str(value).strip().lower()
    return (
        not text_value
        or text_value.startswith("a:")
        or any(text_value.startswith(prefix) for prefix in _ACTOR_ID_PREFIXES)
    )


def _prettify_actor_identifier(value: Any) -> str:
    raw = _strip_actor_technical_prefix(value)
    label = " ".join(raw.replace("_", " ").replace("-", " ").split())
    if not label:
        return ""

    compact = label.replace(" ", "")
    if compact.isalnum() and len(compact) <= 6:
        return compact.upper()

    return label.title()


def _format_actor_label(actor_id: Any, name: Any = None) -> str:
    """Prefer real actor names, falling back to readable IDs such as NVDA."""
    if name is not None and not _looks_like_technical_actor_id(name):
        label = " ".join(str(name).strip().replace("_", " ").split())
        if label:
            return label

    return _prettify_actor_identifier(actor_id) or str(actor_id or "Actor")


def _format_signal_label(
    signal_type: Any,
    ticker: Any,
    direction: Any,
    actor: Any,
    description: Any,
) -> str:
    parts: list[str] = []
    if signal_type:
        parts.append(str(signal_type).strip().replace(" ", "_").upper())
    if ticker:
        parts.append(str(ticker).strip().upper())
    if direction:
        parts.append(str(direction).strip().upper())
    if parts:
        return ":".join(parts)

    if actor:
        return f"SIGNAL:{_format_actor_label(actor)}"
    if description:
        return str(description).strip()[:48]
    return "SIGNAL"


def _limit_canvas_nodes(nodes: list[dict], limit: int) -> list[dict]:
    """Apply a true total node cap while preserving center/high-influence actors."""
    if limit <= 0:
        return []

    center_nodes = [n for n in nodes if n.get("is_center")]
    actor_nodes = sorted(
        [
            n for n in nodes
            if n.get("type") == "actor" and not n.get("is_center")
        ],
        key=lambda n: n.get("influence", 0),
        reverse=True,
    )
    other_nodes = [
        n for n in nodes
        if n.get("type") != "actor" and not n.get("is_center")
    ]

    capped: list[dict] = []
    seen_ids: set[Any] = set()
    for node in center_nodes + actor_nodes + other_nodes:
        node_id = node.get("id")
        if node_id in seen_ids:
            continue
        capped.append(node)
        seen_ids.add(node_id)
        if len(capped) >= limit:
            break
    return capped


def _resolve_center(engine: Engine, center: str) -> dict[str, Any]:
    """Determine if center is an actor ID, actor name, or ticker symbol.

    Returns dict with keys: entity_type ('actor' | 'ticker'), id, name, data.
    Special case: 'all' returns a virtual "power_map" actor list.
    """
    # Special case: "all" loads top actors by influence
    if center.lower() == "all":
        return {
            "entity_type": "power_map",
            "id": "all",
            "name": "Power Map",
            "data": {},
        }

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
                    "SELECT actor_a, actor_b, relationship, strength "
                    "FROM actor_connections "
                    "WHERE actor_a = :aid OR actor_b = :aid "
                    "LIMIT 200"
                ).bindparams(aid=current_id),
            ).mappings().fetchall()

            for r in rows:
                from_a = r["actor_a"]
                to_a = r["actor_b"]
                neighbor = to_a if from_a == current_id else from_a
                edge_key = tuple(sorted([from_a, to_a]))

                # Record edge
                if edge_key not in seen_edges:
                    seen_edges.add(edge_key)
                    edges.append({
                        "source": f"a:{from_a}",
                        "target": f"a:{to_a}",
                        "type": "connection",
                        "label": r["relationship"] or "connected",
                        "strength": float(r["strength"] or 0.5),
                        "confidence": 0.7,
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
        # since_clause is built from a static string literal only
        actor_signal_sql = (
            "SELECT id, signal_type, signal_date, ticker, actor, "
            "       direction, magnitude, confidence, description "
            "FROM signal_data "
            "WHERE (actor = :actor_id OR actor = :actor_name) "
            + since_clause + " "
            "ORDER BY signal_date DESC LIMIT :lim"
        )
        for actor_id in actor_ids[:50]:  # Limit to avoid query explosion
            rows = conn.execute(
                text(actor_signal_sql).bindparams(
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

                # Confidence may be a label (confirmed/derived/etc) or numeric
                conf_raw = r["confidence"]
                if isinstance(conf_raw, str):
                    conf_map = {"confirmed": 1.0, "derived": 0.8, "estimated": 0.6,
                                "rumored": 0.3, "inferred": 0.5}
                    conf_val = conf_map.get(conf_raw.lower(), 0.5)
                else:
                    conf_val = float(conf_raw or 0.5)

                label = _format_signal_label(
                    r["signal_type"],
                    r["ticker"],
                    r["direction"],
                    r["actor"],
                    r["description"],
                )
                signal_nodes.append({
                    "id": sig_id,
                    "type": "signal",
                    "label": label,
                    "source_type": r["signal_type"],
                    "direction": r["direction"],
                    "confidence": conf_val,
                    "confidence_label": str(conf_raw) if conf_raw else "estimated",
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
                "label": ticker,
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

        # since_clause is built from a static string literal only
        ticker_signal_sql = (
            "SELECT id, signal_type, signal_date, ticker, actor, "
            "       direction, magnitude, confidence, description "
            "FROM signal_data "
            "WHERE UPPER(ticker) = :ticker "
            + since_clause + " "
            "ORDER BY signal_date DESC LIMIT :lim"
        )
        rows = conn.execute(
            text(ticker_signal_sql).bindparams(**params),
        ).mappings().fetchall()

        for r in rows:
            sig_id = f"s:{r['id']}"
            cr = r["confidence"]
            if isinstance(cr, str):
                cm = {"confirmed": 1.0, "derived": 0.8, "estimated": 0.6,
                      "rumored": 0.3, "inferred": 0.5}
                cv = cm.get(cr.lower(), 0.5)
            else:
                cv = float(cr or 0.5)
            label = _format_signal_label(
                r["signal_type"],
                r["ticker"],
                r["direction"],
                r["actor"],
                r["description"],
            )
            signal_nodes.append({
                "id": sig_id,
                "type": "signal",
                "label": label,
                "source_type": r["signal_type"],
                "direction": r["direction"],
                "confidence": cv,
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
    cache_key = (center.lower(), depth, layers, since, limit)
    if center.lower() == "all":
        cached = _canvas_graph_cache.get(cache_key)
        if cached:
            cached_at, cached_payload = cached
            age = (datetime.now(timezone.utc) - cached_at).total_seconds()
            if age < _CANVAS_GRAPH_CACHE_TTL_SECONDS:
                return cached_payload

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

    if center_entity["entity_type"] == "power_map":
        # Load top N actors by influence — the full power map
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT id, name, tier, category, influence_score, trust_score, title "
                    "FROM actors ORDER BY influence_score DESC NULLS LAST LIMIT :lim"
                ).bindparams(lim=limit),
            ).mappings().fetchall()

        for row in rows:
            aid = row["id"]
            all_actor_ids.append(aid)
            nodes.append({
                "id": f"a:{aid}",
                "type": "actor",
                "name": row["name"],
                "label": _format_actor_label(aid, row["name"]),
                "tier": row["tier"] or "unknown",
                "category": row["category"] or "unknown",
                "influence": float(row["influence_score"] or 50) / 100.0,
                "trust_score": float(row["trust_score"] or 50) / 100.0,
                "title": row["title"] or "",
                "is_center": False,
            })

        # Load connections between these actors
        if all_actor_ids:
            with engine.connect() as conn:
                conn_rows = conn.execute(
                    text(
                        "SELECT actor_a, actor_b, relationship, strength "
                        "FROM actor_connections "
                        "WHERE actor_a = ANY(:aids) AND actor_b = ANY(:aids) "
                        "LIMIT 5000"
                    ).bindparams(aids=all_actor_ids),
                ).mappings().fetchall()

                seen_edge_keys: set[tuple[str, str]] = set()
                for cr in conn_rows:
                    edge_key = (cr["actor_a"], cr["actor_b"])
                    seen_edge_keys.add(edge_key)
                    seen_edge_keys.add((cr["actor_b"], cr["actor_a"]))
                    edges.append({
                        "source": f"a:{cr['actor_a']}",
                        "target": f"a:{cr['actor_b']}",
                        "type": "connection",
                        "label": cr["relationship"] or "",
                        "strength": float(cr["strength"] or 0.5),
                        "confidence": "confirmed",
                    })

            # ── Implicit edges: same category ──
            category_groups: dict[str, list[str]] = defaultdict(list)
            for n in nodes:
                if n.get("type") == "actor":
                    cat = n.get("category", "unknown")
                    if cat and cat != "unknown":
                        category_groups[cat].append(
                            n["id"].removeprefix("a:")
                        )

            # category_peer edges removed — they create visual noise
            # when most actors share the same category (e.g., 100 "billionaire").
            # Rely on real connections + co_signal edges instead.

            # ── Implicit edges: co-signal (actors who traded the same tickers) ──
            try:
                with engine.connect() as conn:
                    co_rows = conn.execute(
                        text(
                            "SELECT sd.actor, sd.ticker "
                            "FROM signal_data sd "
                            "WHERE sd.actor = ANY(:aids) "
                            "AND sd.ticker IS NOT NULL "
                            "AND sd.signal_date >= CURRENT_DATE - 90 "
                            "ORDER BY sd.signal_date DESC "
                            "LIMIT 10000"
                        ).bindparams(aids=all_actor_ids),
                    ).mappings().fetchall()

                    # Group actors by ticker
                    ticker_actors: dict[str, set[str]] = defaultdict(set)
                    for cr in co_rows:
                        if cr["actor"] and cr["ticker"]:
                            ticker_actors[cr["ticker"]].add(cr["actor"])

                    # Create co_signal edges for actors sharing tickers
                    aid_set = set(all_actor_ids)
                    for ticker, actors_for_ticker in ticker_actors.items():
                        shared = [a for a in actors_for_ticker if a in aid_set]
                        if len(shared) < 2:
                            continue
                        for i in range(len(shared)):
                            for j in range(i + 1, len(shared)):
                                a, b = shared[i], shared[j]
                                if (a, b) not in seen_edge_keys:
                                    seen_edge_keys.add((a, b))
                                    seen_edge_keys.add((b, a))
                                    edges.append({
                                        "source": f"a:{a}",
                                        "target": f"a:{b}",
                                        "type": "co_signal",
                                        "label": ticker,
                                        "strength": 0.4,
                                        "confidence": "derived",
                                    })
            except Exception as exc:
                log.debug("Co-signal edge detection failed: {e}", e=str(exc))

            # ── Implicit edges: same tier (only if sparse) ──
            # tier_peer edges removed — they create visual noise
            # when most actors share the same tier (e.g., 100 "sovereign").

    elif center_entity["entity_type"] == "actor":
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
                "label": _format_actor_label(aid, data.get("name")),
                "tier": data.get("tier", "unknown"),
                "category": data.get("category", "unknown"),
                "influence": float(data.get("influence_score") or 50) / 100.0,
                "trust_score": float(data.get("trust_score") or 50) / 100.0,
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
                            "label": _format_actor_label(resolved_id, row["name"]),
                            "tier": row["tier"],
                            "category": row["category"],
                            "influence": float(row["influence_score"] or 50) / 100.0,
                            "trust_score": float(row["trust_score"] or 50) / 100.0,
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
                            "label": _format_actor_label(neighbor_id, data.get("name")),
                            "tier": data.get("tier", "unknown"),
                            "category": data.get("category", "unknown"),
                            "influence": float(data.get("influence_score") or 50) / 100.0,
                            "trust_score": float(data.get("trust_score") or 50) / 100.0,
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

    # ── Prioritize by influence and apply a true total limit ──
    nodes = _limit_canvas_nodes(nodes, limit)

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

    payload = {"nodes": nodes, "edges": edges, "metadata": metadata}
    if center.lower() == "all":
        _canvas_graph_cache[cache_key] = (datetime.now(timezone.utc), payload)

    return payload


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
    resolved_node_id = _strip_canvas_graph_id(node_type, node_id)

    if node_type == "actor":
        return await _actor_detail(engine, resolved_node_id)
    elif node_type == "ticker":
        return await _ticker_detail(engine, resolved_node_id)
    elif node_type == "signal":
        return await _signal_detail(engine, resolved_node_id)
    else:
        raise HTTPException(400, f"Unknown node_type: {node_type}")


async def _actor_detail(engine: Engine, actor_id: str) -> dict[str, Any]:
    """Detailed actor information."""
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT id, name, tier, category, influence_score, trust_score, "
                "       title, connections, board_seats, net_worth_estimate, aum "
                "FROM actors WHERE id = :aid OR LOWER(name) = LOWER(:aid) LIMIT 1"
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
                "SELECT ac.actor_a, ac.actor_b, ac.relationship, ac.strength, "
                "       a.name, a.tier, a.influence_score "
                "FROM actor_connections ac "
                "JOIN actors a ON (a.id = CASE "
                "    WHEN ac.actor_a = :aid THEN ac.actor_b "
                "    ELSE ac.actor_a END) "
                "WHERE ac.actor_a = :aid OR ac.actor_b = :aid "
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
            "influence_score": float(actor.get("influence_score") or 50) / 100.0,
            "trust_score": float(actor.get("trust_score") or 50) / 100.0,
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
    node_id = _strip_canvas_graph_id(node_type, node_id)
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
                    "influence": float(data.get("influence_score") or 50) / 100.0,
                    "trust_score": float(data.get("trust_score") or 50) / 100.0,
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
                            "influence": float(row["influence_score"] or 50) / 100.0,
                            "trust_score": float(row["trust_score"] or 50) / 100.0,
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
        sync_legacy_canvas_from_board(conn, board_id, {"nodes": [], "edges": []})

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

    # set_clause is built from hardcoded column names; user values are bind params
    update_board_sql = (
        "UPDATE investigation_boards SET " + set_clause + " "
        "WHERE id = :bid RETURNING id"
    )
    with engine.begin() as conn:
        result = conn.execute(
            text(update_board_sql).bindparams(**params),
        )
        if result.rowcount == 0:
            raise HTTPException(404, f"Board '{board_id}' not found")
        if any(key in updates for key in ("name", "description", "graph_state")):
            sync_legacy_canvas_from_board(conn, board_id, body.graph_state)

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
        delete_legacy_canvas_board(conn, board_id)
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
        sync_legacy_canvas_from_board(conn, new_id)

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
    center: str = Query(..., description="Actor ID, ticker symbol, or 'all'"),
    days: int = Query(30, ge=1, le=365, description="Lookback window"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Cross-reference intelligence: find ALL the ways things connect.

    Scans signal_data for insider clusters, whale convergence, lobbying-insider
    correlation, signal divergence, geopolitical hot spots, unusual options
    activity, and multi-source convergence.

    center='all' scans the entire signal_data table.
    center=<ticker> filters to that ticker.
    center=<actor_name> filters by actor ILIKE match.
    """
    engine = get_db_engine()
    connections: list[dict[str, Any]] = []

    since_date = (datetime.now(timezone.utc) - timedelta(days=days)).date()

    # Determine filter mode
    scan_all = center.lower() == "all"
    filter_ticker: str | None = None
    filter_actor: str | None = None

    if not scan_all:
        try:
            center_entity = _resolve_center(engine, center)
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(404, f"Failed to resolve center: {exc}")

        if center_entity["entity_type"] == "ticker":
            filter_ticker = center_entity["id"].upper()
        elif center_entity["entity_type"] == "actor":
            filter_actor = center_entity.get("name", center)
            # Also try ticker if it looks like one (short uppercase)
            filter_ticker = center.upper() if len(center) <= 5 else None

    # Build WHERE clause fragments for optional ticker/actor filtering
    ticker_clause = ""
    actor_clause = ""
    bind_extras: dict[str, Any] = {"since": since_date}

    if filter_ticker:
        ticker_clause = " AND UPPER(ticker) = :filter_ticker"
        bind_extras["filter_ticker"] = filter_ticker
    if filter_actor and not filter_ticker:
        actor_clause = " AND actor ILIKE :filter_actor"
        bind_extras["filter_actor"] = f"%{filter_actor}%"

    with engine.connect() as conn:

        # ── 1. Insider Clusters ──
        # Tickers where multiple insider signals occurred within the window
        try:
            q_insider = text(
                "SELECT ticker, COUNT(*) as cnt, "
                "       array_agg(DISTINCT actor) as actors "
                "FROM signal_data "
                "WHERE signal_type = 'insider' "
                "AND signal_date >= :since"
                + ticker_clause + actor_clause
                + " GROUP BY ticker "
                "HAVING COUNT(*) >= 3 "
                "ORDER BY cnt DESC LIMIT 20"
            ).bindparams(**bind_extras)

            for row in conn.execute(q_insider).mappings().fetchall():
                actors_list = row["actors"] if row["actors"] else []
                cnt = row["cnt"]
                connections.append({
                    "type": "insider_cluster",
                    "actors": (
                        [a for a in actors_list if a]
                        if isinstance(actors_list, list)
                        else [str(actors_list)]
                    ),
                    "ticker": row["ticker"],
                    "evidence": [
                        f"{cnt} insider transactions on {row['ticker']} in {days}d",
                        (
                            f"Insiders: "
                            f"{', '.join(str(a) for a in actors_list[:5]) if isinstance(actors_list, list) else str(actors_list)}"
                        ),
                    ],
                    "confidence": min(0.9, 0.3 + cnt * 0.05),
                    "description": (
                        f"Insider cluster: {cnt} insider trades on {row['ticker']} -- "
                        f"{len(actors_list) if isinstance(actors_list, list) else 1} distinct insiders"
                    ),
                })
        except Exception as exc:
            log.debug("Insider cluster query failed: {e}", e=str(exc))

        # ── 2. Whale Convergence ──
        # Tickers where whale_options AND whale_flow agree on direction within 7 days
        try:
            whale_binds: dict[str, Any] = {"since": since_date}
            if filter_ticker:
                whale_binds["filter_ticker"] = filter_ticker
            q_whale = text(
                "SELECT a.ticker, a.direction, COUNT(*) as agree_count "
                "FROM signal_data a "
                "JOIN signal_data b ON a.ticker = b.ticker "
                "    AND a.signal_type = 'whale_options' "
                "    AND b.signal_type = 'whale_flow' "
                "    AND a.direction = b.direction "
                "    AND ABS(EXTRACT(EPOCH FROM a.signal_date - b.signal_date)) < 604800 "
                "WHERE a.signal_date >= :since"
                + (" AND UPPER(a.ticker) = :filter_ticker" if filter_ticker else "")
                + " GROUP BY a.ticker, a.direction "
                "HAVING COUNT(*) >= 5 "
                "ORDER BY agree_count DESC LIMIT 20"
            ).bindparams(**whale_binds)

            for row in conn.execute(q_whale).mappings().fetchall():
                cnt = row["agree_count"]
                connections.append({
                    "type": "whale_convergence",
                    "actors": [row["ticker"]],
                    "ticker": row["ticker"],
                    "evidence": [
                        f"{cnt} whale_options + whale_flow agreements on {row['ticker']}",
                        f"Consensus direction: {row['direction']}",
                    ],
                    "confidence": min(0.95, 0.4 + cnt * 0.03),
                    "description": (
                        f"Whale convergence: {cnt} whale_options/whale_flow signals agree "
                        f"{row['direction']} on {row['ticker']}"
                    ),
                })
        except Exception as exc:
            log.debug("Whale convergence query failed: {e}", e=str(exc))

        # ── 3. Lobbying + Insider Correlation ──
        # Tickers with both lobbying and insider activity within 30 days
        try:
            lobby_binds: dict[str, Any] = {"since": since_date}
            if filter_ticker:
                lobby_binds["filter_ticker"] = filter_ticker
            q_lobby = text(
                "SELECT a.ticker, COUNT(*) as overlap "
                "FROM signal_data a "
                "JOIN signal_data b ON a.ticker = b.ticker "
                "    AND a.signal_type = 'insider' "
                "    AND b.signal_type = 'quiverquant:lobbying' "
                "    AND ABS(EXTRACT(EPOCH FROM a.signal_date - b.signal_date)) < 2592000 "
                "WHERE a.signal_date >= :since"
                + (" AND UPPER(a.ticker) = :filter_ticker" if filter_ticker else "")
                + " GROUP BY a.ticker "
                "HAVING COUNT(*) >= 2 "
                "ORDER BY overlap DESC LIMIT 10"
            ).bindparams(**lobby_binds)

            for row in conn.execute(q_lobby).mappings().fetchall():
                cnt = row["overlap"]
                connections.append({
                    "type": "lobbying_insider",
                    "actors": [row["ticker"]],
                    "ticker": row["ticker"],
                    "evidence": [
                        f"{cnt} lobbying-insider overlaps on {row['ticker']} within 30d windows",
                        "Company lobbying activity coincides with insider trading",
                    ],
                    "confidence": min(0.85, 0.4 + cnt * 0.05),
                    "description": (
                        f"Lobbying-insider correlation: {row['ticker']} has {cnt} instances "
                        f"of insider trades near lobbying disclosures"
                    ),
                })
        except Exception as exc:
            log.debug("Lobbying-insider query failed: {e}", e=str(exc))

        # ── 4. Signal Divergence ──
        # Tickers where different signal types disagree on direction
        try:
            q_diverge = text(
                "SELECT ticker, "
                "    SUM(CASE WHEN direction IN ('bullish', 'buy') THEN 1 ELSE 0 END) as bullish, "
                "    SUM(CASE WHEN direction IN ('bearish', 'sell') THEN 1 ELSE 0 END) as bearish "
                "FROM signal_data "
                "WHERE signal_date >= :since AND direction IS NOT NULL"
                + ticker_clause + actor_clause
                + " GROUP BY ticker "
                "HAVING SUM(CASE WHEN direction IN ('bullish','buy') THEN 1 ELSE 0 END) >= 3 "
                "   AND SUM(CASE WHEN direction IN ('bearish','sell') THEN 1 ELSE 0 END) >= 3 "
                "ORDER BY "
                "    (SUM(CASE WHEN direction IN ('bullish','buy') THEN 1 ELSE 0 END) "
                "   + SUM(CASE WHEN direction IN ('bearish','sell') THEN 1 ELSE 0 END)) DESC "
                "LIMIT 10"
            ).bindparams(**bind_extras)

            for row in conn.execute(q_diverge).mappings().fetchall():
                bull = row["bullish"]
                bear = row["bearish"]
                total = bull + bear
                connections.append({
                    "type": "signal_divergence",
                    "actors": [row["ticker"]],
                    "ticker": row["ticker"],
                    "evidence": [
                        f"{bull} bullish vs {bear} bearish signals on {row['ticker']}",
                        f"Divergence ratio: {bull}/{total} bullish, {bear}/{total} bearish",
                    ],
                    "confidence": min(0.9, 0.3 + min(bull, bear) / max(bull, bear, 1) * 0.6),
                    "description": (
                        f"Signal divergence on {row['ticker']}: "
                        f"{bull} bullish vs {bear} bearish -- market is conflicted"
                    ),
                })
        except Exception as exc:
            log.debug("Signal divergence query failed: {e}", e=str(exc))

        # ── 5. Geopolitical Hot Spots ──
        # Actors/countries with elevated geopolitical tension
        try:
            geo_binds: dict[str, Any] = {"since": since_date}
            if filter_actor:
                geo_binds["filter_actor"] = f"%{filter_actor}%"
            q_geo = text(
                "SELECT actor, AVG(magnitude) as avg_tension, COUNT(*) as events "
                "FROM signal_data "
                "WHERE signal_type IN ('geopolitical_tone', 'geopolitical_tension') "
                "AND signal_date >= :since "
                "AND actor IS NOT NULL AND actor != ''"
                + (" AND actor ILIKE :filter_actor" if filter_actor else "")
                + " GROUP BY actor "
                "HAVING COUNT(*) >= 5 AND AVG(magnitude) > 2 "
                "ORDER BY avg_tension DESC LIMIT 10"
            ).bindparams(**geo_binds)

            for row in conn.execute(q_geo).mappings().fetchall():
                tension = float(row["avg_tension"] or 0)
                events = row["events"]
                connections.append({
                    "type": "geopolitical_hotspot",
                    "actors": [row["actor"]],
                    "evidence": [
                        f"{events} geopolitical events involving {row['actor']}",
                        f"Average tension magnitude: {tension:.1f}",
                    ],
                    "confidence": min(0.9, 0.3 + tension * 0.1),
                    "description": (
                        f"Geopolitical hot spot: {row['actor']} -- "
                        f"{events} events, avg tension {tension:.1f}"
                    ),
                })
        except Exception as exc:
            log.debug("Geopolitical hotspot query failed: {e}", e=str(exc))

        # ── 6. Unusual Options Activity ──
        # Tickers with concentrated unusual options flow
        try:
            q_unusual = text(
                "SELECT ticker, COUNT(*) as unusual_count, "
                "    SUM(CASE WHEN direction IN ('bullish','buy') THEN 1 ELSE 0 END) as bull, "
                "    SUM(CASE WHEN direction IN ('bearish','sell') THEN 1 ELSE 0 END) as bear "
                "FROM signal_data "
                "WHERE signal_type IN ('unusual_options', 'options_flow') "
                "AND signal_date >= :since"
                + ticker_clause + actor_clause
                + " GROUP BY ticker "
                "HAVING COUNT(*) >= 10 "
                "ORDER BY unusual_count DESC LIMIT 10"
            ).bindparams(**bind_extras)

            for row in conn.execute(q_unusual).mappings().fetchall():
                cnt = row["unusual_count"]
                bull = row["bull"]
                bear = row["bear"]
                lean = "bullish" if bull > bear else "bearish" if bear > bull else "neutral"
                connections.append({
                    "type": "unusual_options",
                    "actors": [row["ticker"]],
                    "ticker": row["ticker"],
                    "evidence": [
                        f"{cnt} unusual options signals on {row['ticker']}",
                        f"Directional lean: {bull} bullish, {bear} bearish ({lean})",
                    ],
                    "confidence": min(0.9, 0.3 + cnt * 0.03),
                    "description": (
                        f"Unusual options: {cnt} signals on {row['ticker']} -- "
                        f"skewing {lean} ({bull}B/{bear}S)"
                    ),
                })
        except Exception as exc:
            log.debug("Unusual options query failed: {e}", e=str(exc))

        # ── 7. Multi-Source Convergence ──
        # Tickers where 3+ different signal types agree on direction
        try:
            q_multi = text(
                "SELECT ticker, direction, "
                "    COUNT(DISTINCT signal_type) as source_count, "
                "    array_agg(DISTINCT signal_type) as sources "
                "FROM signal_data "
                "WHERE signal_date >= :since "
                "AND direction IS NOT NULL AND direction != ''"
                + ticker_clause + actor_clause
                + " GROUP BY ticker, direction "
                "HAVING COUNT(DISTINCT signal_type) >= 3 "
                "ORDER BY source_count DESC LIMIT 15"
            ).bindparams(**bind_extras)

            for row in conn.execute(q_multi).mappings().fetchall():
                src_count = row["source_count"]
                sources = row["sources"] if row["sources"] else []
                connections.append({
                    "type": "multi_source_convergence",
                    "actors": [row["ticker"]],
                    "ticker": row["ticker"],
                    "evidence": [
                        f"{src_count} independent sources agree {row['direction']} on {row['ticker']}",
                        (
                            f"Sources: "
                            f"{', '.join(str(s) for s in sources) if isinstance(sources, list) else str(sources)}"
                        ),
                    ],
                    "confidence": min(0.95, 0.4 + src_count * 0.1),
                    "description": (
                        f"Multi-source convergence: {src_count} signal types agree "
                        f"{row['direction']} on {row['ticker']}"
                    ),
                })
        except Exception as exc:
            log.debug("Multi-source convergence query failed: {e}", e=str(exc))

    center_type = "all"
    if filter_ticker:
        center_type = "ticker"
    elif filter_actor:
        center_type = "actor"

    return {
        "center": center,
        "center_type": center_type,
        "connections": connections,
        "total": len(connections),
        "lookback_days": days,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
