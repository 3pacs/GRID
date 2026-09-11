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


# ── Precomputed Analytics Queries ──────────────────────────────────────


_VALID_METRICS = frozenset({
    "pagerank", "betweenness", "eigenvector",
    "degree_centrality", "hub_score", "authority_score",
})


# Analytics scopes -> table. "full" is the whole actor graph (dominated by
# the ICIJ / PEP / sanctions dumps); "curated" is the named-market-actor
# subgraph written by ``scripts/graph_analytics.py --scope curated``. The
# table name is only ever taken from this whitelist.
ANALYTICS_TABLES: dict[str, str] = {
    "full": "actor_analytics",
    "curated": "actor_analytics_curated",
}


def analytics_table(scope: str) -> str:
    """Whitelisted analytics table for ``scope``; ValueError on anything else."""
    try:
        return ANALYTICS_TABLES[scope]
    except KeyError:
        raise ValueError(
            f"Invalid analytics scope '{scope}'. Must be one of: {sorted(ANALYTICS_TABLES)}"
        ) from None


def get_actor_analytics(
    actor_id: str, engine: Engine | None = None, scope: str = "full"
) -> dict | None:
    """Get precomputed graph analytics for an actor.

    Returns dict with pagerank, community_id, betweenness, eigenvector,
    degree_centrality, hub_score, authority_score, computed_at. None if not found.
    """
    table = analytics_table(scope)
    if engine is None:
        from api.dependencies import get_db_engine
        engine = get_db_engine()

    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT actor_id, pagerank, community_id, betweenness, "
                "eigenvector, degree_centrality, hub_score, authority_score, "
                "computed_at "
                f"FROM {table} WHERE actor_id = :aid"
            ),
            {"aid": actor_id},
        ).fetchone()

    if row is None:
        return None
    return {
        "actor_id": row[0],
        "pagerank": float(row[1] or 0),
        "community_id": row[2],
        "betweenness": float(row[3] or 0),
        "eigenvector": float(row[4] or 0),
        "degree_centrality": float(row[5] or 0),
        "hub_score": float(row[6] or 0),
        "authority_score": float(row[7] or 0),
        "computed_at": str(row[8]) if row[8] else None,
    }


def get_community_members(
    community_id: int, limit: int = 50, engine: Engine | None = None, scope: str = "full"
) -> list[dict]:
    """Get all actors in a community, ordered by PageRank descending."""
    table = analytics_table(scope)
    if engine is None:
        from api.dependencies import get_db_engine
        engine = get_db_engine()

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT aa.actor_id, a.name, a.category, aa.pagerank, "
                "aa.betweenness, aa.eigenvector, aa.hub_score, aa.authority_score "
                f"FROM {table} aa "
                "JOIN actors a ON aa.actor_id = a.id "
                "WHERE aa.community_id = :cid "
                "ORDER BY aa.pagerank DESC "
                "LIMIT :lim"
            ),
            {"cid": community_id, "lim": limit},
        ).fetchall()

    return [
        {
            "actor_id": r[0],
            "name": r[1],
            "category": r[2],
            "pagerank": float(r[3] or 0),
            "betweenness": float(r[4] or 0),
            "eigenvector": float(r[5] or 0),
            "hub_score": float(r[6] or 0),
            "authority_score": float(r[7] or 0),
        }
        for r in rows
    ]


def get_top_actors(
    metric: str = "pagerank", limit: int = 20, engine: Engine | None = None, scope: str = "full"
) -> list[dict]:
    """Get top actors by any analytics metric.

    Allowed metrics: pagerank, betweenness, eigenvector,
    degree_centrality, hub_score, authority_score.
    """
    if metric not in _VALID_METRICS:
        raise ValueError(
            f"Invalid metric '{metric}'. Must be one of: {sorted(_VALID_METRICS)}"
        )
    table = analytics_table(scope)

    if engine is None:
        from api.dependencies import get_db_engine
        engine = get_db_engine()

    # metric is validated against _VALID_METRICS and table against
    # ANALYTICS_TABLES, so both are safe as identifiers
    sql = (
        "SELECT aa.actor_id, a.name, a.category, "
        f"aa.{metric}, aa.community_id, aa.pagerank "
        f"FROM {table} aa "
        "JOIN actors a ON aa.actor_id = a.id "
        f"ORDER BY aa.{metric} DESC "
        "LIMIT :lim"
    )

    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"lim": limit}).fetchall()

    return [
        {
            "actor_id": r[0],
            "name": r[1],
            "category": r[2],
            "score": float(r[3] or 0),
            "community_id": r[4],
            "pagerank": float(r[5] or 0),
        }
        for r in rows
    ]


def get_community_list(
    engine: Engine | None = None, scope: str = "full", limit: int | None = None
) -> list[dict]:
    """Get communities with member counts and top member, largest first.

    One query: a GROUP BY for counts joined to a DISTINCT ON pick of the
    top-PageRank member per community (the previous version issued one
    extra query per community, a 44K-query N+1 on the full graph).
    """
    table = analytics_table(scope)
    if engine is None:
        from api.dependencies import get_db_engine
        engine = get_db_engine()

    sql = f"""
        WITH agg AS (
            SELECT community_id,
                   COUNT(*)                     AS member_count,
                   COALESCE(MAX(pagerank), 0)   AS max_pagerank
            FROM {table}
            WHERE community_id IS NOT NULL
            GROUP BY community_id
        ),
        top AS (
            SELECT DISTINCT ON (aa.community_id)
                   aa.community_id, a.name, a.category
            FROM {table} aa
            JOIN actors a ON a.id = aa.actor_id
            WHERE aa.community_id IS NOT NULL
            ORDER BY aa.community_id, aa.pagerank DESC NULLS LAST
        )
        SELECT agg.community_id, agg.member_count, agg.max_pagerank, top.name, top.category
        FROM agg
        LEFT JOIN top ON top.community_id = agg.community_id
        ORDER BY agg.member_count DESC, agg.community_id
    """
    params: dict[str, Any] = {}
    if limit is not None:
        sql += " LIMIT :lim"
        params["lim"] = int(limit)

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()

    return [
        {
            "community_id": r[0],
            "member_count": int(r[1]),
            "max_pagerank": float(r[2] or 0),
            "top_member": r[3],
            "top_category": r[4],
        }
        for r in rows
    ]


def get_community_category_mix(
    community_id: int, limit: int = 5, engine: Engine | None = None, scope: str = "full"
) -> list[dict]:
    """Dominant actor categories in a community: ``[{category, count}, ...]``."""
    table = analytics_table(scope)
    if engine is None:
        from api.dependencies import get_db_engine
        engine = get_db_engine()

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT a.category, COUNT(*) AS n "
                f"FROM {table} aa "
                "JOIN actors a ON a.id = aa.actor_id "
                "WHERE aa.community_id = :cid "
                "GROUP BY a.category ORDER BY n DESC LIMIT :lim"
            ),
            {"cid": community_id, "lim": limit},
        ).fetchall()
    return [{"category": r[0], "count": int(r[1])} for r in rows]


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
