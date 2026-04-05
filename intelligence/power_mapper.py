"""
Power Mapper — unified power-mapping layer combining multiple sources.

Combines LittleSis, FinDKG, OpenSecrets, Wikidata, and ICIJ data into
a weighted influence graph for centrality analysis and cluster detection.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class PowerEdge:
    """A weighted connection between two entities."""
    source: str
    target: str
    edge_type: str          # donation, board_seat, lobbying, business, offshore, ownership
    weight: float           # influence weight (higher = stronger)
    data_source: str        # littlesis, opensecrets, wikidata, icij, findkg
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class PowerNode:
    """An entity in the power graph."""
    name: str
    node_type: str          # person, company, government, fund
    centrality: float = 0.0
    connections: int = 0
    sources: set[str] = field(default_factory=set)
    edges: list[PowerEdge] = field(default_factory=list)


# Edge type weights for influence scoring
EDGE_WEIGHTS = {
    "board_seat": 5.0,
    "donation": 3.0,
    "lobbying": 4.0,
    "business": 2.0,
    "offshore": 6.0,        # Hidden connections are high signal
    "ownership": 5.0,
    "subsidiary": 3.0,
    "supplier": 2.0,
    "customer": 2.0,
    "competitor": 1.0,
    "partner": 2.0,
}


class PowerMapper:
    """Unified power-mapping and influence analysis."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._nodes: dict[str, PowerNode] = {}
        self._edges: list[PowerEdge] = []

    def build_graph(self) -> dict[str, Any]:
        """Build the full power graph from all data sources.

        Returns:
            Summary with node/edge counts.
        """
        self._load_icij_edges()
        self._load_littlesis_edges()
        self._load_wikidata_edges()
        self._load_findkg_edges()
        self._compute_centrality()

        return {
            "nodes": len(self._nodes),
            "edges": len(self._edges),
            "sources": list({e.data_source for e in self._edges}),
        }

    def _add_edge(self, edge: PowerEdge) -> None:
        """Add an edge and ensure both nodes exist."""
        self._edges.append(edge)

        for name in (edge.source, edge.target):
            if name not in self._nodes:
                self._nodes[name] = PowerNode(name=name, node_type="unknown")
            node = self._nodes[name]
            node.connections += 1
            node.sources.add(edge.data_source)
            node.edges.append(edge)

    def _load_icij_edges(self) -> None:
        """Load offshore connections from ICIJ data."""
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT r.from_node, r.to_node, r.rel_type, "
                    "e1.name AS from_name, e2.name AS to_name "
                    "FROM icij_relationships r "
                    "LEFT JOIN icij_entities e1 ON r.from_node = e1.node_id "
                    "LEFT JOIN icij_officers e2 ON r.to_node = e2.node_id "
                    "WHERE e1.name IS NOT NULL AND e2.name IS NOT NULL "
                    "LIMIT 50000"
                )).fetchall()

            for r in rows:
                self._add_edge(PowerEdge(
                    source=r[3] or f"node:{r[0]}",
                    target=r[4] or f"node:{r[1]}",
                    edge_type="offshore",
                    weight=EDGE_WEIGHTS["offshore"],
                    data_source="icij",
                    metadata={"rel_type": r[2]},
                ))

            log.info("ICIJ: loaded {n} edges", n=len(rows))
        except Exception as exc:
            log.debug("ICIJ edges unavailable: {e}", e=str(exc))

    def _load_littlesis_edges(self) -> None:
        """Load LittleSis relationships from raw_series."""
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT raw_payload FROM raw_series "
                    "WHERE series_id LIKE 'littlesis:rel:%' "
                    "AND pull_status = 'SUCCESS' "
                    "ORDER BY pull_timestamp DESC LIMIT 50000"
                )).fetchall()

            import json
            for r in rows:
                payload = json.loads(r[0]) if isinstance(r[0], str) else r[0]
                if not payload:
                    continue
                self._add_edge(PowerEdge(
                    source=str(payload.get("entity1_id", "")),
                    target=str(payload.get("entity2_id", "")),
                    edge_type=_categorize_littlesis(payload.get("category_id")),
                    weight=EDGE_WEIGHTS.get("business", 2.0),
                    data_source="littlesis",
                    metadata=payload,
                ))

            log.info("LittleSis: loaded {n} edges", n=len(rows))
        except Exception as exc:
            log.debug("LittleSis edges unavailable: {e}", e=str(exc))

    def _load_wikidata_edges(self) -> None:
        """Load Wikidata relationships from raw_series."""
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT raw_payload FROM raw_series "
                    "WHERE series_id LIKE 'wikidata:%' "
                    "AND pull_status = 'SUCCESS' "
                    "ORDER BY pull_timestamp DESC LIMIT 50000"
                )).fetchall()

            import json
            for r in rows:
                payload = json.loads(r[0]) if isinstance(r[0], str) else r[0]
                if not payload:
                    continue
                rel_type = payload.get("type", "business")
                edge_type = {
                    "board_member": "board_seat",
                    "subsidiary": "subsidiary",
                    "ownership": "ownership",
                }.get(rel_type, "business")

                source = payload.get("company", payload.get("parent", ""))
                target = payload.get("person", payload.get("subsidiary", payload.get("owner", "")))

                if source and target:
                    self._add_edge(PowerEdge(
                        source=source, target=target,
                        edge_type=edge_type,
                        weight=EDGE_WEIGHTS.get(edge_type, 2.0),
                        data_source="wikidata",
                    ))

            log.info("Wikidata: loaded {n} edges", n=len(rows))
        except Exception as exc:
            log.debug("Wikidata edges unavailable: {e}", e=str(exc))

    def _load_findkg_edges(self) -> None:
        """Load FinDKG relationships from raw_series."""
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT raw_payload FROM raw_series "
                    "WHERE series_id LIKE 'findkg:%' "
                    "AND pull_status = 'SUCCESS' "
                    "ORDER BY pull_timestamp DESC LIMIT 50000"
                )).fetchall()

            import json
            for r in rows:
                payload = json.loads(r[0]) if isinstance(r[0], str) else r[0]
                if not payload:
                    continue
                rel = payload.get("relation", "").lower().replace(" ", "_")
                edge_type = rel if rel in EDGE_WEIGHTS else "business"

                self._add_edge(PowerEdge(
                    source=payload.get("head", ""),
                    target=payload.get("tail", ""),
                    edge_type=edge_type,
                    weight=EDGE_WEIGHTS.get(edge_type, 2.0),
                    data_source="findkg",
                ))

            log.info("FinDKG: loaded {n} edges", n=len(rows))
        except Exception as exc:
            log.debug("FinDKG edges unavailable: {e}", e=str(exc))

    def _compute_centrality(self) -> None:
        """Compute weighted degree centrality for all nodes."""
        for node in self._nodes.values():
            node.centrality = sum(
                EDGE_WEIGHTS.get(e.edge_type, 1.0) for e in node.edges
            )

    def top_influencers(self, limit: int = 50) -> list[dict[str, Any]]:
        """Return the most influential nodes by centrality.

        Args:
            limit: Number of top nodes to return.

        Returns:
            List of node dicts sorted by centrality.
        """
        nodes = sorted(self._nodes.values(), key=lambda n: n.centrality, reverse=True)
        return [
            {
                "name": n.name,
                "node_type": n.node_type,
                "centrality": round(n.centrality, 2),
                "connections": n.connections,
                "sources": sorted(n.sources),
            }
            for n in nodes[:limit]
        ]

    def get_clusters(self, min_size: int = 3) -> list[list[str]]:
        """Detect power clusters via connected components.

        Simple BFS-based connected components. For production,
        upgrade to community detection (Louvain/Leiden).

        Args:
            min_size: Minimum cluster size.

        Returns:
            List of node name lists per cluster.
        """
        adjacency: dict[str, set[str]] = defaultdict(set)
        for edge in self._edges:
            adjacency[edge.source].add(edge.target)
            adjacency[edge.target].add(edge.source)

        visited: set[str] = set()
        clusters: list[list[str]] = []

        for node_name in self._nodes:
            if node_name in visited:
                continue

            # BFS
            cluster: list[str] = []
            queue = [node_name]
            while queue:
                current = queue.pop(0)
                if current in visited:
                    continue
                visited.add(current)
                cluster.append(current)
                for neighbor in adjacency.get(current, set()):
                    if neighbor not in visited:
                        queue.append(neighbor)

            if len(cluster) >= min_size:
                clusters.append(sorted(cluster))

        clusters.sort(key=len, reverse=True)
        log.info("Found {n} power clusters (min_size={m})", n=len(clusters), m=min_size)
        return clusters


def _categorize_littlesis(category_id: int | None) -> str:
    """Map LittleSis category IDs to edge types."""
    mapping = {
        1: "board_seat",       # Position
        2: "board_seat",       # Education
        3: "business",         # Membership
        4: "business",         # Family
        5: "donation",         # Donation
        6: "business",         # Transaction
        7: "lobbying",         # Lobbying
        8: "business",         # Social
        9: "business",         # Professional
        10: "ownership",       # Ownership
        11: "business",        # Hierarchy
        12: "donation",        # Generic
    }
    return mapping.get(category_id or 0, "business")
