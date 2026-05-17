"""Spider API endpoints — status, stats, inject, neighborhood, path finding.

Graph engine bridge (task #154):
    The spider daemon (intelligence.spider.daemon) is a oneshot systemd unit
    that pops actors from the priority queue, expands them, persists results
    to Postgres, and exits. There is no long-running spider process to query.

    Instead, the API process owns an in-memory ``GraphEngine`` loaded from
    Postgres (which the spider keeps populating). The bridge here:

      * ``_get_or_load_graph(engine)``: thread-safe lazy load of the FULL
        graph used by neighborhood / path / connections endpoints.
      * ``_get_or_load_stats_graph(engine)``: smaller bounded load used by
        the lightweight stats endpoint (kept for backward compatibility —
        legacy 5k actor / 20k edge cap when no full graph yet).
      * ``warm_graph_async()``: kicked off by ``api.main.lifespan`` so the
        full graph is hot before the first user request lands.
      * ``refresh_graph()`` + ``/spider/graph/reload``: lets the spider
        oneshot ping the API after a successful run to pick up new edges.
      * ``get_graph_info()`` + ``/spider/graph/health``: exposes load
        timestamps and counts so the dashboard can show staleness.

    Endpoints that need fast traversal call ``get_graph()`` which now
    falls back to the lazy full-load path. If the load fails the endpoint
    returns 503 (never 500) so the dashboard can surface a degraded
    banner instead of crashing.
"""

from __future__ import annotations

import os
import threading
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log
from pydantic import BaseModel, Field

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/intelligence", tags=["spider"])

# ── Shared in-memory graph state ──────────────────────────────────────
# The graph engine instance is shared across all endpoints. It is loaded
# lazily on first request, warmed on API startup, and refreshed on a
# periodic timer (and on-demand via /spider/graph/reload).

_graph_engine = None
_graph_loaded_at: float | None = None        # epoch seconds of last successful load
_graph_load_error: str | None = None
_graph_load_lock = threading.Lock()
_graph_is_full: bool = False                 # True if loaded without the stats caps


def _stats_limits() -> tuple[int, int]:
    actor_limit = int(os.getenv("GRID_SPIDER_STATS_ACTOR_LIMIT", "5000"))
    connection_limit = int(os.getenv("GRID_SPIDER_STATS_CONNECTION_LIMIT", "20000"))
    return actor_limit, connection_limit


def _full_load_limits() -> tuple[int | None, int | None]:
    """Caps for the full in-memory graph used by neighborhood/path endpoints.

    Defaults bound the load to the most influential actors and strongest
    edges so cold-start stays under ~10s on a 1.6M-actor / 5M-edge DB.
    Override with ``GRID_SPIDER_GRAPH_ACTOR_LIMIT`` /
    ``GRID_SPIDER_GRAPH_CONNECTION_LIMIT`` (0 = unbounded).
    """
    actor_env = int(os.getenv("GRID_SPIDER_GRAPH_ACTOR_LIMIT", "50000"))
    edge_env = int(os.getenv("GRID_SPIDER_GRAPH_CONNECTION_LIMIT", "500000"))
    return (actor_env or None, edge_env or None)


def _refresh_interval_seconds() -> int:
    """How often the background refresher reloads the graph from DB.

    Default 900s (15 min) keeps the in-memory graph fresh after each
    bounded spider run without hammering Postgres.
    """
    return int(os.getenv("GRID_SPIDER_GRAPH_REFRESH_SEC", "900"))


def _load_stats_graph_from_db(engine):
    from intelligence.spider.graph_engine import GraphEngine

    actor_limit, connection_limit = _stats_limits()
    graph = GraphEngine()
    graph.load_from_db(
        engine,
        actor_limit=actor_limit,
        connection_limit=connection_limit,
        allow_missing_connections=True,
    )
    return graph


def _load_full_graph_from_db(engine):
    """Load the FULL actor graph (or capped by env if set).

    Returns the populated GraphEngine. Raises if the actor table cannot
    be read at all — callers translate that to a 503.
    """
    from intelligence.spider.graph_engine import GraphEngine

    actor_limit, connection_limit = _full_load_limits()
    graph = GraphEngine()
    graph.load_from_db(
        engine,
        actor_limit=actor_limit,
        connection_limit=connection_limit,
        allow_missing_connections=True,
    )
    return graph


def _get_or_load_stats_graph(engine):
    """Lazy-load the bounded stats graph; reused if already full-loaded."""
    global _graph_engine, _graph_load_error, _graph_loaded_at
    if _graph_engine is not None:
        return _graph_engine
    with _graph_load_lock:
        if _graph_engine is not None:
            return _graph_engine
        try:
            _graph_engine = _load_stats_graph_from_db(engine)
            _graph_loaded_at = time.time()
            _graph_load_error = None
            return _graph_engine
        except Exception as exc:  # noqa: BLE001 - stats endpoint must report degraded state.
            _graph_load_error = str(exc)
            return None


def _get_or_load_graph(engine):
    """Lazy-load the FULL graph used by neighborhood / path / connections.

    Returns None on failure (caller emits 503). Thread-safe. If the
    bounded stats graph was loaded first, this upgrades to the full one.
    """
    global _graph_engine, _graph_load_error, _graph_loaded_at, _graph_is_full
    if _graph_engine is not None and _graph_is_full:
        return _graph_engine
    with _graph_load_lock:
        if _graph_engine is not None and _graph_is_full:
            return _graph_engine
        t0 = time.time()
        try:
            _graph_engine = _load_full_graph_from_db(engine)
            _graph_loaded_at = time.time()
            _graph_is_full = True
            _graph_load_error = None
            log.info(
                "Spider bridge: full graph loaded "
                "({a} actors, {c} edges in {ms}ms)",
                a=_graph_engine.actor_count,
                c=_graph_engine.connection_count,
                ms=int((time.time() - t0) * 1000),
            )
            return _graph_engine
        except Exception as exc:  # noqa: BLE001 - never 500.
            _graph_load_error = str(exc)
            log.warning("Spider bridge: full graph load failed: {e}", e=str(exc))
            return None


def refresh_graph(engine=None) -> dict[str, Any]:
    """Force a re-load of the in-memory graph from Postgres.

    Called by the periodic refresher thread and the /spider/graph/reload
    admin endpoint (which the spider oneshot pings after a successful run).
    """
    global _graph_engine, _graph_load_error, _graph_loaded_at, _graph_is_full
    if engine is None:
        engine = get_db_engine()
    with _graph_load_lock:
        t0 = time.time()
        try:
            new_graph = _load_full_graph_from_db(engine)
        except Exception as exc:  # noqa: BLE001
            _graph_load_error = str(exc)
            log.warning("Spider bridge: refresh failed: {e}", e=str(exc))
            return {"status": "error", "error": str(exc)}
        _graph_engine = new_graph
        _graph_loaded_at = time.time()
        _graph_is_full = True
        _graph_load_error = None
        info = {
            "status": "ok",
            "actors": new_graph.actor_count,
            "connections": new_graph.connection_count,
            "load_ms": int((time.time() - t0) * 1000),
            "loaded_at": _graph_loaded_at,
        }
        log.info(
            "Spider bridge: graph refreshed "
            "({a} actors, {c} edges in {ms}ms)",
            a=info["actors"],
            c=info["connections"],
            ms=info["load_ms"],
        )
        return info


def warm_graph_async() -> threading.Thread:
    """Kick off the full graph load + periodic refresh in a daemon thread.

    Called from ``api.main.lifespan`` so the first user request to
    neighborhood / path / connections doesn't pay the 1-2s cold-load.
    Returns the thread so callers can join in tests if they want.
    """
    def _run():
        try:
            engine = get_db_engine()
        except Exception as exc:  # noqa: BLE001
            log.warning("Spider bridge: cannot acquire DB engine for warmup: {e}", e=str(exc))
            return
        try:
            _get_or_load_graph(engine)
        except Exception as exc:  # noqa: BLE001
            log.warning("Spider bridge: warmup load raised: {e}", e=str(exc))
        # Periodic refresh
        interval = _refresh_interval_seconds()
        if interval <= 0:
            return
        while True:
            time.sleep(interval)
            try:
                refresh_graph(engine)
            except Exception as exc:  # noqa: BLE001
                log.warning("Spider bridge: periodic refresh raised: {e}", e=str(exc))

    t = threading.Thread(target=_run, name="spider-graph-warmer", daemon=True)
    t.start()
    return t


def get_graph_info() -> dict[str, Any]:
    """Diagnostic snapshot for /spider/graph/health."""
    info: dict[str, Any] = {
        "loaded": _graph_engine is not None,
        "is_full_load": _graph_is_full,
        "loaded_at": _graph_loaded_at,
        "actors": _graph_engine.actor_count if _graph_engine else 0,
        "connections": _graph_engine.connection_count if _graph_engine else 0,
        "refresh_interval_sec": _refresh_interval_seconds(),
    }
    if _graph_loaded_at is not None:
        info["age_seconds"] = round(time.time() - _graph_loaded_at, 1)
    if _graph_load_error:
        info["last_error"] = _graph_load_error
    return info


def _empty_stats(status: str, error: str | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": status,
        "total_actors": 0,
        "total_connections": 0,
        "by_degree": {},
        "by_source": {},
        "max_degree": 0,
    }
    if error:
        payload["error"] = error
    return payload


def get_graph(engine=None):
    """Return the shared graph engine instance.

    If the graph hasn't been loaded yet, lazy-load it from Postgres
    (the bridge entry point). On failure, raise 503 — endpoints translate
    that to a degraded response, never a 500.
    """
    if _graph_engine is not None and _graph_is_full:
        return _graph_engine
    # Lazy-load: try to upgrade to full graph.
    if engine is None:
        try:
            engine = get_db_engine()
        except Exception:  # noqa: BLE001
            engine = None
    if engine is not None:
        graph = _get_or_load_graph(engine)
        if graph is not None:
            return graph
    # Last resort: serve from whatever bounded graph we already have.
    if _graph_engine is not None:
        return _graph_engine
    raise HTTPException(
        503,
        f"Graph engine not ready yet — still loading from DB "
        f"(last_error={_graph_load_error or 'none'})",
    )


class InjectActorRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=500)
    category: str = Field(default="corporation", max_length=50)
    tier: str = Field(default="institutional", max_length=50)
    title: str = Field(default="", max_length=500)
    connections: list[dict[str, Any]] = Field(default_factory=list)
    notes: str = Field(default="", max_length=2000)


@router.get("/actor/{actor_id}/neighborhood")
async def get_neighborhood(
    actor_id: str,
    depth: int = Query(3, ge=1, le=11),
    max_nodes: int = Query(2000, ge=10, le=10000),
    engine=Depends(get_db_engine),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get the subgraph around an actor for frontend rendering."""
    graph = get_graph(engine)
    if not graph.has_actor(actor_id):
        raise HTTPException(404, f"Actor '{actor_id}' not found")

    nodes, links = graph.subgraph(actor_id, depth=depth, max_nodes=max_nodes)
    return {
        "center": actor_id,
        "depth": depth,
        "nodes": nodes,
        "links": links,
        "node_count": len(nodes),
        "link_count": len(links),
        "via": "spider_graph_engine",
    }


@router.get("/actor/{actor_id}/path/{target_id}")
async def get_shortest_path(
    actor_id: str,
    target_id: str,
    engine=Depends(get_db_engine),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Find the shortest path between two actors."""
    graph = get_graph(engine)
    for aid in (actor_id, target_id):
        if not graph.has_actor(aid):
            raise HTTPException(404, f"Actor '{aid}' not found")

    path = graph.shortest_path(actor_id, target_id)
    if path is None:
        return {"path": None, "degrees": -1, "message": "No connection found"}

    path_details = []
    for i, aid in enumerate(path):
        actor = graph.get_actor(aid) or {}
        entry: dict[str, Any] = {"id": aid, **actor, "degree": i}
        if i > 0:
            neighbors = graph.get_neighbors(path[i - 1])
            meta = neighbors.get(aid)
            if meta:
                entry["connection"] = {
                    "relationship": meta.relationship,
                    "strength": meta.strength,
                    "confidence_tier": meta.confidence_tier,
                }
        path_details.append(entry)

    return {"path": path_details, "degrees": len(path) - 1, "from": actor_id, "to": target_id}


@router.get("/actor/{actor_id}/connections")
async def get_actor_connections(
    actor_id: str,
    engine=Depends(get_db_engine),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get all connections for an actor with metadata."""
    graph = get_graph(engine)
    if not graph.has_actor(actor_id):
        raise HTTPException(404, f"Actor '{actor_id}' not found")

    neighbors = graph.get_neighbors(actor_id)
    connections = []
    for nid, meta in neighbors.items():
        neighbor_data = graph.get_actor(nid) or {}
        connections.append({
            "actor_id": nid,
            "name": neighbor_data.get("name", nid),
            "category": neighbor_data.get("category", ""),
            "relationship": meta.relationship,
            "strength": meta.strength,
            "confidence_tier": meta.confidence_tier,
            "sources": meta.sources,
        })

    connections.sort(key=lambda c: c["strength"], reverse=True)
    return {"actor_id": actor_id, "connections": connections, "count": len(connections)}


@router.get("/spider/stats")
async def get_spider_stats(
    engine=Depends(get_db_engine),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get overall graph statistics."""
    graph = _get_or_load_stats_graph(engine)
    if graph is None:
        return _empty_stats("unavailable", _graph_load_error)

    by_degree: dict[int, int] = {}
    by_source: dict[str, int] = {}
    for data in graph._actors.values():
        deg = data.get("degree", 0)
        by_degree[deg] = by_degree.get(deg, 0) + 1
        src = data.get("source", "unknown")
        by_source[src] = by_source.get(src, 0) + 1

    warnings = list(getattr(graph, "load_warnings", []) or [])
    status = "degraded" if warnings else "ready"
    payload: dict[str, Any] = {
        "status": status,
        "total_actors": graph.actor_count,
        "total_connections": graph.connection_count,
        "by_degree": dict(sorted(by_degree.items())),
        "by_source": dict(sorted(by_source.items(), key=lambda kv: -kv[1])),
        "max_degree": max(by_degree.keys()) if by_degree else 0,
    }
    if warnings:
        payload["warnings"] = warnings
    return payload


@router.get("/spider/graph/health")
async def graph_health(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Diagnostic — report load timestamp, age, and counts for the bridge."""
    return get_graph_info()


@router.post("/spider/graph/reload")
async def graph_reload(
    engine=Depends(get_db_engine),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Force-reload the in-memory graph from Postgres.

    Called by the spider daemon after a successful bounded run so the
    API picks up the new actors and connections without waiting for the
    next periodic refresh.
    """
    return refresh_graph(engine)


@router.post("/spider/inject")
async def inject_actor(
    body: InjectActorRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Operator injects a new actor or lead into the graph."""
    from intelligence.actors.db import save_actor
    from intelligence.entity_resolver import SpiderEntityResolver as EntityResolver

    graph = get_graph()
    engine = get_db_engine()
    resolver = EntityResolver(graph)

    existing = resolver.resolve(body.name, {"category": body.category})
    if existing:
        return {"status": "exists", "actor_id": existing, "message": f"Actor already exists as '{existing}'"}

    actor_id = resolver.generate_id(body.name, body.category)
    actor_data = {
        "name": body.name,
        "category": body.category,
        "tier": body.tier,
        "title": body.title,
        "influence_score": 0.5,
        "trust_score": 0.5,
        "degree": 0,
        "source": "operator",
        "credibility": "rumor",
        "data_sources": ["operator"],
    }

    graph.add_actor(actor_id, actor_data)
    save_actor(engine, actor_id, actor_data)

    return {"status": "created", "actor_id": actor_id, "name": body.name}
