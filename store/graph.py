"""Apache AGE graph query wrapper for GRID.

Provides Cypher query execution over the grid_graph created by Apache AGE.
Falls back gracefully if AGE is not available.

Usage:
    from store.graph import GraphStore
    gs = GraphStore(engine)
    path = gs.shortest_path("actor-123", "actor-456")
    neighbors = gs.expand(actor_id="actor-123", depth=2)
"""

from __future__ import annotations

import json
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


class GraphStore:
    """Thin wrapper around Apache AGE Cypher queries."""

    GRAPH_NAME = "grid_graph"

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._available: bool | None = None

    @property
    def available(self) -> bool:
        """Check if AGE extension is loaded and graph exists."""
        if self._available is not None:
            return self._available
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SET search_path = ag_catalog, public"))
                row = conn.execute(
                    text("SELECT count(*) FROM ag_graph WHERE name = :name"),
                    {"name": self.GRAPH_NAME},
                ).fetchone()
                self._available = row[0] > 0
        except Exception as exc:
            log.debug("AGE not available: {e}", e=str(exc))
            self._available = False
        return self._available

    def _cypher(self, query: str, params: dict | None = None) -> list[dict]:
        """Execute a Cypher query and return results as list of dicts."""
        if not self.available:
            return []

        # AGE requires LOAD and search_path per connection
        cypher_sql = f"""
            SELECT * FROM cypher('{self.GRAPH_NAME}', $$
                {query}
            $$) AS (result agtype)
        """

        try:
            with self._engine.connect() as conn:
                conn.execute(text("SET search_path = ag_catalog, public"))
                rows = conn.execute(text(cypher_sql)).fetchall()
                return [{"result": _parse_agtype(row[0])} for row in rows]
        except Exception as exc:
            log.warning("Cypher query failed: {e}", e=str(exc))
            return []

    def _cypher_multi(self, query: str, columns: list[str]) -> list[dict]:
        """Execute a Cypher query returning multiple columns."""
        if not self.available:
            return []

        col_defs = ", ".join(f"{c} agtype" for c in columns)
        cypher_sql = f"""
            SELECT * FROM cypher('{self.GRAPH_NAME}', $$
                {query}
            $$) AS ({col_defs})
        """

        try:
            with self._engine.connect() as conn:
                conn.execute(text("SET search_path = ag_catalog, public"))
                rows = conn.execute(text(cypher_sql)).fetchall()
                return [
                    {col: _parse_agtype(row[i]) for i, col in enumerate(columns)}
                    for row in rows
                ]
        except Exception as exc:
            log.warning("Cypher multi-column query failed: {e}", e=str(exc))
            return []

    # ── Public API ──────────────────────────────────────────────────

    def actor_count(self) -> int:
        """Count Actor vertices in the graph."""
        results = self._cypher("MATCH (a:Actor) RETURN count(a)")
        if results:
            return int(results[0]["result"])
        return 0

    def expand(self, actor_id: str, depth: int = 1, limit: int = 50) -> list[dict]:
        """Get neighbors within N hops of an actor."""
        query = f"""
            MATCH (a:Actor {{actor_id: '{actor_id}'}})-[r*1..{depth}]-(neighbor:Actor)
            RETURN DISTINCT neighbor
            LIMIT {limit}
        """
        results = self._cypher(query)
        return [r["result"] for r in results if r.get("result")]

    def shortest_path(self, from_id: str, to_id: str, max_depth: int = 6) -> list[dict]:
        """Find shortest path between two actors."""
        results = self._cypher_multi(
            f"""
            MATCH p = shortestPath(
                (a:Actor {{actor_id: '{from_id}'}})-[*..{max_depth}]-(b:Actor {{actor_id: '{to_id}'}})
            )
            RETURN nodes(p) AS path_nodes, relationships(p) AS path_edges
            """,
            ["path_nodes", "path_edges"],
        )
        return results

    def connected_actors_by_type(
        self, actor_id: str, relationship: str, limit: int = 20
    ) -> list[dict]:
        """Find actors connected by a specific relationship type."""
        query = f"""
            MATCH (a:Actor {{actor_id: '{actor_id}'}})-[r:{relationship}]-(b:Actor)
            RETURN b
            LIMIT {limit}
        """
        results = self._cypher(query)
        return [r["result"] for r in results if r.get("result")]

    def multi_hop_search(
        self, actor_id: str, target_category: str, max_hops: int = 3
    ) -> list[dict]:
        """Find all actors of a specific category within N hops."""
        results = self._cypher_multi(
            f"""
            MATCH path = (a:Actor {{actor_id: '{actor_id}'}})-[*1..{max_hops}]-(b:Actor)
            WHERE b.category = '{target_category}'
            RETURN b AS actor, length(path) AS distance
            ORDER BY distance
            LIMIT 20
            """,
            ["actor", "distance"],
        )
        return results

    def community_members(self, actor_id: str, depth: int = 2) -> list[dict]:
        """Get all actors in the same community (within N hops)."""
        results = self._cypher_multi(
            f"""
            MATCH (a:Actor {{actor_id: '{actor_id}'}})-[r*1..{depth}]-(b:Actor)
            RETURN DISTINCT b AS actor, min(length(r)) AS distance
            ORDER BY distance
            LIMIT 100
            """,
            ["actor", "distance"],
        )
        return results


def _parse_agtype(val: Any) -> Any:
    """Parse an AGE agtype value into a Python object."""
    if val is None:
        return None
    s = str(val)
    # AGE returns agtype as string representations
    try:
        return json.loads(s)
    except (json.JSONDecodeError, TypeError):
        return s


# ── Module-level singleton ──────────────────────────────────────────

_graph_store: GraphStore | None = None


def get_graph_store(engine: Engine | None = None) -> GraphStore:
    """Return the shared GraphStore singleton."""
    global _graph_store
    if _graph_store is None:
        if engine is None:
            from api.dependencies import get_db_engine
            engine = get_db_engine()
        _graph_store = GraphStore(engine)
    return _graph_store
