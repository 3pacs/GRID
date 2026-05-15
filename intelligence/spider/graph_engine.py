"""In-memory actor graph with microsecond traversal.

Holds all actors and connections in RAM. Provides BFS, shortest path,
subgraph extraction, and name resolution. Thread-safe for concurrent reads.
"""

from __future__ import annotations

import json
import threading
from collections import defaultdict
from typing import Any, Optional

from loguru import logger as log

from intelligence.spider.models import ConnectionMeta


def _normalize_name(name: str) -> str:
    """Lowercase, strip whitespace, collapse spaces."""
    return " ".join(name.lower().split())


def _parse_json(val: Any) -> list:
    """Safely parse a JSONB field."""
    if val is None:
        return []
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            return parsed if isinstance(parsed, list) else [parsed]
        except (json.JSONDecodeError, TypeError):
            return []
    return []


def _actor_select_expr(column: str, actor_columns: set[str], fallback: str) -> str:
    if column in actor_columns:
        return column
    return f"{fallback} AS {column}"


class GraphEngine:
    """In-memory actor graph engine.

    Data structures:
        _actors:    dict[actor_id -> actor_dict]
        _adj:       dict[actor_id -> dict[neighbor_id -> ConnectionMeta]]
        _names:     dict[normalized_name -> actor_id]
    """

    def __init__(self) -> None:
        self._actors: dict[str, dict[str, Any]] = {}
        self._adj: dict[str, dict[str, ConnectionMeta]] = defaultdict(dict)
        self._names: dict[str, str] = {}
        self._lock = threading.RLock()
        self.load_warnings: list[str] = []

    # -- Mutation (called by spider daemon, behind lock) ---------------

    def add_actor(self, actor_id: str, data: dict[str, Any]) -> None:
        """Add or update an actor in the graph."""
        with self._lock:
            self._actors[actor_id] = data
            name = data.get("name", "")
            if name:
                self._names[_normalize_name(name)] = actor_id

    def add_connection(
        self, actor_a: str, actor_b: str, meta: ConnectionMeta
    ) -> None:
        """Add a bidirectional connection between two actors."""
        with self._lock:
            self._adj[actor_a][actor_b] = meta
            self._adj[actor_b][actor_a] = meta

    def remove_actor(self, actor_id: str) -> None:
        """Remove an actor and all its connections."""
        with self._lock:
            for neighbor in list(self._adj.get(actor_id, {})):
                self._adj[neighbor].pop(actor_id, None)
            self._adj.pop(actor_id, None)
            data = self._actors.pop(actor_id, None)
            if data and data.get("name"):
                self._names.pop(_normalize_name(data["name"]), None)

    # -- Read (thread-safe, no lock needed for dict reads) -------------

    @property
    def actor_count(self) -> int:
        return len(self._actors)

    @property
    def connection_count(self) -> int:
        return sum(len(neighbors) for neighbors in self._adj.values()) // 2

    def has_actor(self, actor_id: str) -> bool:
        return actor_id in self._actors

    def get_actor(self, actor_id: str) -> Optional[dict[str, Any]]:
        return self._actors.get(actor_id)

    def get_neighbors(self, actor_id: str) -> dict[str, ConnectionMeta]:
        """Return {neighbor_id: ConnectionMeta} for an actor."""
        return dict(self._adj.get(actor_id, {}))

    def resolve_name(self, name: str) -> Optional[str]:
        """Look up actor_id by name (case-insensitive)."""
        return self._names.get(_normalize_name(name))

    # ── Traversal ────────────────────────────────────────────────

    def bfs(self, start: str, max_depth: int = 11) -> dict[str, int]:
        """Breadth-first search. Returns {actor_id: degree} for all reachable actors."""
        if start not in self._actors:
            return {}
        visited: dict[str, int] = {start: 0}
        queue: list[tuple[str, int]] = [(start, 0)]
        while queue:
            current, depth = queue.pop(0)
            if depth >= max_depth:
                continue
            for neighbor in self._adj.get(current, {}):
                if neighbor not in visited:
                    visited[neighbor] = depth + 1
                    queue.append((neighbor, depth + 1))
        return visited

    def shortest_path(self, start: str, end: str) -> Optional[list[str]]:
        """Dijkstra shortest path using 1/strength as edge weight. Returns actor_id list or None."""
        import heapq

        if start == end:
            return [start]
        if start not in self._actors or end not in self._actors:
            return None

        dist: dict[str, float] = {start: 0.0}
        prev: dict[str, Optional[str]] = {start: None}
        heap: list[tuple[float, str]] = [(0.0, start)]

        while heap:
            d, current = heapq.heappop(heap)
            if current == end:
                path = []
                node: Optional[str] = end
                while node is not None:
                    path.append(node)
                    node = prev.get(node)
                return list(reversed(path))
            if d > dist.get(current, float("inf")):
                continue
            for neighbor, meta in self._adj.get(current, {}).items():
                weight = 1.0 / max(meta.strength, 0.01)
                new_dist = d + weight
                if new_dist < dist.get(neighbor, float("inf")):
                    dist[neighbor] = new_dist
                    prev[neighbor] = current
                    heapq.heappush(heap, (new_dist, neighbor))

        return None

    def subgraph(
        self, center: str, depth: int = 3, max_nodes: int = 2000
    ) -> tuple[list[dict], list[dict]]:
        """Extract a neighborhood subgraph for frontend rendering.

        Returns (nodes, links) where each node is the actor dict + id,
        and each link is {source, target, relationship, strength, confidence_tier}.
        """
        reachable = self.bfs(center, max_depth=depth)

        if len(reachable) > max_nodes:
            sorted_actors = sorted(
                reachable.items(),
                key=lambda kv: self._actors.get(kv[0], {}).get("influence_score", 0),
                reverse=True,
            )
            reachable = dict(sorted_actors[:max_nodes])

        kept = set(reachable.keys())
        nodes = []
        for aid in kept:
            data = self._actors.get(aid, {})
            nodes.append({**data, "id": aid, "degree": reachable[aid]})

        links = []
        seen: set[tuple[str, str]] = set()
        for aid in kept:
            for neighbor, meta in self._adj.get(aid, {}).items():
                if neighbor in kept and (aid, neighbor) not in seen:
                    links.append({
                        "source": aid,
                        "target": neighbor,
                        "relationship": meta.relationship,
                        "strength": meta.strength,
                        "confidence_tier": meta.confidence_tier,
                    })
                    seen.add((aid, neighbor))
                    seen.add((neighbor, aid))

        return nodes, links

    # ── Database Loading ─────────────────────────────────────────

    def load_from_db(
        self,
        engine: Any,
        connection_limit: int | None = None,
        actor_limit: int | None = None,
        allow_missing_connections: bool = False,
    ) -> None:
        """Load actor graph from Postgres into RAM.

        ``actor_limit`` keeps the resolver's name map focused on the most
        influential actors for scheduled spider runs.
        ``connection_limit`` keeps service runs bounded on production-sized
        graphs while still loading the strongest relationships first.
        """
        from sqlalchemy import text

        with self._lock:
            self._actors.clear()
            self._adj.clear()
            self._names.clear()
            self.load_warnings.clear()

        actor_count = 0
        with engine.connect() as conn:
            actor_columns = {
                row[0]
                for row in conn.execute(text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'actors'"
                )).fetchall()
            }
            degree_expr = _actor_select_expr("degree", actor_columns, "0")
            source_expr = _actor_select_expr("source", actor_columns, "'legacy'")
            actor_sql = f"""
                SELECT id, name, tier, category, title,
                       net_worth_estimate, aum, influence_score,
                       trust_score, motivation_model,
                       connections, known_positions, board_seats,
                       political_affiliations, data_sources, credibility,
                       {degree_expr}, {source_expr}
                FROM actors
                ORDER BY influence_score DESC
            """
            actor_params: dict[str, Any] = {}
            if actor_limit and actor_limit > 0:
                actor_sql += " LIMIT :limit"
                actor_params["limit"] = actor_limit
            rows = conn.execute(text(actor_sql), actor_params).fetchall()
            for r in rows:
                self.add_actor(r[0], {
                    "name": r[1],
                    "tier": r[2],
                    "category": r[3],
                    "title": r[4] or "",
                    "net_worth_estimate": float(r[5]) if r[5] is not None else None,
                    "aum": float(r[6]) if r[6] is not None else None,
                    "influence_score": float(r[7]) if r[7] is not None else 0.5,
                    "trust_score": float(r[8]) if r[8] is not None else 0.5,
                    "motivation_model": r[9] or "unknown",
                    "data_sources": _parse_json(r[14]),
                    "credibility": r[15] or "inferred",
                    "degree": r[16] if r[16] is not None else 0,
                    "source": r[17] or "unknown",
                })
                actor_count += 1

        conn_count = 0
        try:
            with engine.connect() as conn:
                connection_sql = """
                    SELECT actor_a, actor_b, relationship, strength, evidence,
                           CASE
                               WHEN evidence::text LIKE '%hard_data%' THEN 1
                               WHEN evidence::text LIKE '%public_record%' THEN 2
                               WHEN evidence::text LIKE '%inferred%' THEN 3
                               ELSE 4
                           END AS confidence_tier
                    FROM actor_connections
                    ORDER BY strength DESC NULLS LAST
                """
                params: dict[str, Any] = {}
                if connection_limit and connection_limit > 0:
                    connection_sql += " LIMIT :limit"
                    params["limit"] = connection_limit
                rows = conn.execute(text(connection_sql), params).fetchall()
                for r in rows:
                    evidence = _parse_json(r[4])
                    sources = list({e.get("source", "unknown") for e in evidence if isinstance(e, dict)})
                    self.add_connection(
                        r[0], r[1],
                        ConnectionMeta(
                            relationship=r[2],
                            strength=float(r[3]) if r[3] is not None else 0.5,
                            confidence_tier=int(r[5]) if r[5] is not None else 3,
                            sources=sources,
                        ),
                    )
                    conn_count += 1
        except Exception as exc:  # noqa: BLE001 - DB drivers expose table-missing differently.
            if not allow_missing_connections:
                raise
            log.warning(
                "Graph actor load succeeded but actor_connections could not load; "
                "continuing with actor-only stats: {e}",
                e=str(exc),
            )
            compact = str(exc).splitlines()[0] if str(exc) else exc.__class__.__name__
            self.load_warnings.append(f"actor_connections unavailable: {compact}")

        log.info(
            "Graph loaded from DB: {a} actors (limit={actor_limit}), "
            "{c} connections (limit={conn_limit})",
            a=actor_count,
            actor_limit=actor_limit or "none",
            c=conn_count,
            conn_limit=connection_limit or "none",
        )
