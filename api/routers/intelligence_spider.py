"""Spider API endpoints — status, stats, inject, neighborhood, path finding."""

from __future__ import annotations

import os
import threading
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/intelligence", tags=["spider"])

_graph_engine = None
_graph_load_error: str | None = None
_graph_load_lock = threading.Lock()


def _stats_limits() -> tuple[int, int]:
    actor_limit = int(os.getenv("GRID_SPIDER_STATS_ACTOR_LIMIT", "5000"))
    connection_limit = int(os.getenv("GRID_SPIDER_STATS_CONNECTION_LIMIT", "20000"))
    return actor_limit, connection_limit


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


def _get_or_load_stats_graph(engine):
    global _graph_engine, _graph_load_error
    if _graph_engine is not None:
        return _graph_engine
    with _graph_load_lock:
        if _graph_engine is not None:
            return _graph_engine
        try:
            _graph_engine = _load_stats_graph_from_db(engine)
            _graph_load_error = None
            return _graph_engine
        except Exception as exc:  # noqa: BLE001 - stats endpoint must report degraded state.
            _graph_load_error = str(exc)
            return None


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


def get_graph():
    """Return the shared graph engine instance."""
    if _graph_engine is None:
        raise HTTPException(503, "Graph engine not initialized yet — still loading")
    return _graph_engine


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
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get the subgraph around an actor for frontend rendering."""
    graph = get_graph()
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
    }


@router.get("/actor/{actor_id}/path/{target_id}")
async def get_shortest_path(
    actor_id: str,
    target_id: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Find the shortest path between two actors."""
    graph = get_graph()
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
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get all connections for an actor with metadata."""
    graph = get_graph()
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
