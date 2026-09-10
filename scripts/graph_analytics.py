"""
GRID Graph Analytics Batch — Phase 4.6

Computes PageRank, Louvain community detection, betweenness centrality,
eigenvector centrality, degree centrality, and HITS hub/authority scores
on the actor network graph.

Results are stored in the actor_analytics table for consumption by
the frontend and other intelligence modules.

Usage:
    python scripts/graph_analytics.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

# Ensure project root is on sys.path so imports work when run standalone
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import networkx as nx
from loguru import logger as log

from db import execute_sql, get_connection


# ---------------------------------------------------------------------------
# 1. Load actor graph from PostgreSQL
# ---------------------------------------------------------------------------

# Scopes: "full" is every actor_connections edge (3.4M actors, dominated by
# the ICIJ / PEP / sanctions dumps); "curated" keeps only edges whose two
# endpoints are named market actors, so the communities describe who moves
# markets rather than which offshore service provider registered whom.
SCOPES: dict[str, str] = {
    "full": "actor_analytics",
    "curated": "actor_analytics_curated",
}

# Bulk-import categories excluded from the curated subgraph.
DUMP_CATEGORIES: tuple[str, ...] = (
    "pep", "person_of_interest", "entity_of_interest", "donor",
    "sanctioned", "ofac_sanctioned", "unknown", "other",
)
DUMP_CATEGORY_PREFIX = "icij"

_CURATED_EDGE_SQL = (
    "SELECT c.actor_a, c.actor_b, c.relationship, c.strength "
    "FROM actor_connections c "
    "JOIN actors a ON a.id = c.actor_a "
    "JOIN actors b ON b.id = c.actor_b "
    "WHERE a.category NOT LIKE %s AND b.category NOT LIKE %s "
    "  AND NOT (a.category = ANY(%s)) AND NOT (b.category = ANY(%s))"
)


def table_for_scope(scope: str) -> str:
    """Analytics table for a scope; raises on anything outside the whitelist."""
    try:
        return SCOPES[scope]
    except KeyError:
        raise ValueError(f"Unknown graph scope '{scope}'. Must be one of: {sorted(SCOPES)}") from None


def load_actor_graph(scope: str = "full") -> nx.DiGraph:
    """Build a NetworkX DiGraph from actor_connections and actors tables.

    ``scope="curated"`` restricts edges to those between named market actors
    (see ``DUMP_CATEGORIES`` / ``DUMP_CATEGORY_PREFIX``).
    """
    table_for_scope(scope)
    log.info("Loading actor connections from database (scope={s})...", s=scope)
    if scope == "curated":
        prefix = f"{DUMP_CATEGORY_PREFIX}%"
        edges = execute_sql(
            _CURATED_EDGE_SQL,
            (prefix, prefix, list(DUMP_CATEGORIES), list(DUMP_CATEGORIES)),
        )
    else:
        edges = execute_sql(
            "SELECT actor_a, actor_b, relationship, strength "
            "FROM actor_connections"
        )
    log.info("Loaded {n} edges", n=len(edges))

    if not edges:
        log.warning("No edges found in actor_connections — returning empty graph")
        return nx.DiGraph()

    G = nx.DiGraph()

    # Add edges with weight
    for edge in edges:
        weight = float(edge.get("strength") or 0.5)
        G.add_edge(
            edge["actor_a"],
            edge["actor_b"],
            weight=weight,
            relationship=edge.get("relationship", "unknown"),
        )

    log.info("Graph built: {n} nodes, {e} edges", n=G.number_of_nodes(), e=G.number_of_edges())

    # Enrich nodes with actor attributes
    actor_ids = list(G.nodes())
    if actor_ids:
        batch_size = 5000
        for i in range(0, len(actor_ids), batch_size):
            batch = actor_ids[i : i + batch_size]
            placeholders = ", ".join(["%s"] * len(batch))
            actors = execute_sql(
                f"SELECT id, name, category, influence_score "
                f"FROM actors WHERE id IN ({placeholders})",
                tuple(batch),
            )
            for actor in actors:
                nid = actor["id"]
                if nid in G:
                    G.nodes[nid]["name"] = actor.get("name", "")
                    G.nodes[nid]["category"] = actor.get("category", "")
                    G.nodes[nid]["influence_score"] = float(actor.get("influence_score") or 0)

    return G


# ---------------------------------------------------------------------------
# 2. Compute analytics
# ---------------------------------------------------------------------------

def compute_pagerank(G: nx.DiGraph) -> dict[str, float]:
    """Compute PageRank (influence ranking)."""
    log.info("Computing PageRank on {n} nodes...", n=G.number_of_nodes())
    t0 = time.time()
    try:
        pr = nx.pagerank(G, weight="weight", max_iter=200)
    except nx.PowerIterationFailedConvergence:
        log.warning("PageRank did not converge — using default alpha=0.85 with tolerance bump")
        pr = nx.pagerank(G, weight="weight", max_iter=500, tol=1e-04)
    log.info("PageRank computed in {t:.1f}s", t=time.time() - t0)
    return pr


def compute_communities(G: nx.DiGraph) -> dict[str, int]:
    """Compute Louvain community detection on the undirected projection.

    ``python-louvain`` is imported lazily so this module can be imported
    (and patched in tests) on hosts where the optional dep failed to build.
    Returns an empty dict and logs a warning when the dep is unavailable.
    """
    log.info("Computing Louvain community detection...")
    t0 = time.time()
    G_undirected = G.to_undirected()
    try:
        from community import community_louvain  # python-louvain

        partition = community_louvain.best_partition(G_undirected, weight="weight")
    except ImportError:
        # networkx >= 2.8 ships its own Louvain; use it so a missing wheel
        # does not silently leave every community_id NULL.
        louvain = getattr(getattr(nx, "community", None), "louvain_communities", None)
        if louvain is None or G_undirected.number_of_edges() == 0:
            log.warning(
                "python-louvain not installed and networkx has no louvain_communities — "
                "skipping community detection. Returning empty partition."
            )
            return {}
        log.info("python-louvain not installed — using networkx louvain_communities")
        partition = {}
        for cid, members in enumerate(louvain(G_undirected, weight="weight", seed=42)):
            for node in members:
                partition[node] = cid
    n_communities = len(set(partition.values()))
    log.info("Louvain found {c} communities in {t:.1f}s", c=n_communities, t=time.time() - t0)
    return partition


def compute_betweenness(G: nx.DiGraph) -> dict[str, float]:
    """Compute betweenness centrality with k-sampling for large graphs."""
    n = G.number_of_nodes()
    k = min(500, n) if n > 500 else None
    log.info("Computing betweenness centrality (k={k}, n={n})...", k=k or n, n=n)
    t0 = time.time()
    bc = nx.betweenness_centrality(G, weight="weight", k=k)
    log.info("Betweenness computed in {t:.1f}s", t=time.time() - t0)
    return bc


def compute_eigenvector(G: nx.DiGraph) -> dict[str, float]:
    """Compute eigenvector centrality (connected to important nodes)."""
    log.info("Computing eigenvector centrality...")
    t0 = time.time()
    try:
        ev = nx.eigenvector_centrality_numpy(G, weight="weight")
    except Exception as exc:
        log.warning("Eigenvector centrality failed ({e}) — falling back to zeros", e=str(exc))
        ev = {n: 0.0 for n in G.nodes()}
    log.info("Eigenvector centrality computed in {t:.1f}s", t=time.time() - t0)
    return ev


def compute_degree_centrality(G: nx.DiGraph) -> dict[str, float]:
    """Compute degree centrality (raw connectivity)."""
    log.info("Computing degree centrality...")
    t0 = time.time()
    dc = nx.degree_centrality(G)
    log.info("Degree centrality computed in {t:.1f}s", t=time.time() - t0)
    return dc


def compute_hits(G: nx.DiGraph) -> tuple[dict[str, float], dict[str, float]]:
    """Compute HITS hub and authority scores."""
    log.info("Computing HITS hub/authority scores...")
    t0 = time.time()
    try:
        hubs, authorities = nx.hits(G, max_iter=200)
    except nx.PowerIterationFailedConvergence:
        log.warning("HITS did not converge — using tolerance bump")
        hubs, authorities = nx.hits(G, max_iter=500, tol=1e-04)
    except Exception as exc:
        log.warning("HITS computation failed ({e}) — falling back to zeros", e=str(exc))
        hubs = {n: 0.0 for n in G.nodes()}
        authorities = {n: 0.0 for n in G.nodes()}
    log.info("HITS computed in {t:.1f}s", t=time.time() - t0)
    return hubs, authorities


# ---------------------------------------------------------------------------
# 3. Store results in actor_analytics
# ---------------------------------------------------------------------------

def store_results(
    G: nx.DiGraph,
    pagerank: dict[str, float],
    communities: dict[str, int],
    betweenness: dict[str, float],
    eigenvector: dict[str, float],
    degree_cent: dict[str, float],
    hubs: dict[str, float],
    authorities: dict[str, float],
    batch_size: int = 1000,
    scope: str = "full",
) -> int:
    """Batch UPSERT analytics results into the scope's analytics table."""
    table = table_for_scope(scope)
    nodes = list(G.nodes())
    total = len(nodes)
    log.info("Storing analytics for {n} actors into {t}...", n=total, t=table)
    t0 = time.time()
    stored = 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            # Ensure the table exists (idempotent). ``table`` comes from the
            # SCOPES whitelist, never from user input.
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    actor_id            TEXT PRIMARY KEY REFERENCES actors(id) ON DELETE CASCADE,
                    pagerank            DOUBLE PRECISION DEFAULT 0,
                    community_id        INTEGER,
                    betweenness         DOUBLE PRECISION DEFAULT 0,
                    eigenvector         DOUBLE PRECISION DEFAULT 0,
                    degree_centrality   DOUBLE PRECISION DEFAULT 0,
                    hub_score           DOUBLE PRECISION DEFAULT 0,
                    authority_score     DOUBLE PRECISION DEFAULT 0,
                    computed_at         TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            for i in range(0, total, batch_size):
                batch = nodes[i : i + batch_size]
                values_list = []
                params = []
                for idx, node_id in enumerate(batch):
                    idx * 8
                    values_list.append(
                        "(%s, %s, %s, %s, %s, %s, %s, %s, NOW())"
                    )
                    params.extend([
                        node_id,
                        pagerank.get(node_id, 0.0),
                        communities.get(node_id),
                        betweenness.get(node_id, 0.0),
                        eigenvector.get(node_id, 0.0),
                        degree_cent.get(node_id, 0.0),
                        hubs.get(node_id, 0.0),
                        authorities.get(node_id, 0.0),
                    ])

                sql = (
                    f"INSERT INTO {table} "
                    "(actor_id, pagerank, community_id, betweenness, eigenvector, "
                    "degree_centrality, hub_score, authority_score, computed_at) "
                    "VALUES " + ", ".join(values_list) + " "
                    "ON CONFLICT (actor_id) DO UPDATE SET "
                    "pagerank = EXCLUDED.pagerank, "
                    "community_id = EXCLUDED.community_id, "
                    "betweenness = EXCLUDED.betweenness, "
                    "eigenvector = EXCLUDED.eigenvector, "
                    "degree_centrality = EXCLUDED.degree_centrality, "
                    "hub_score = EXCLUDED.hub_score, "
                    "authority_score = EXCLUDED.authority_score, "
                    "computed_at = EXCLUDED.computed_at"
                )
                cur.execute(sql, params)
                stored += len(batch)

                pct = stored / total * 100
                if pct % 10 < (batch_size / total * 100) or i == 0:
                    log.info("Stored {s}/{t} actors ({p:.0f}%)", s=stored, t=total, p=pct)

    elapsed = time.time() - t0
    log.info("All {n} actors stored in {t:.1f}s", n=stored, t=elapsed)
    return stored


# ---------------------------------------------------------------------------
# 4. Community labeling helper
# ---------------------------------------------------------------------------

def label_communities(
    G: nx.DiGraph,
    communities: dict[str, int],
    pagerank: dict[str, float],
) -> dict[int, str]:
    """Label each community by the category of its highest-PageRank member."""
    community_top: dict[int, tuple[float, str]] = {}
    for node_id, cid in communities.items():
        pr = pagerank.get(node_id, 0.0)
        category = G.nodes.get(node_id, {}).get("category", "unknown")
        current = community_top.get(cid)
        if current is None or pr > current[0]:
            community_top[cid] = (pr, category)

    labels = {cid: info[1] for cid, info in community_top.items()}
    return labels


# ---------------------------------------------------------------------------
# 5. Summary printer
# ---------------------------------------------------------------------------

def print_summary(
    G: nx.DiGraph,
    pagerank: dict[str, float],
    communities: dict[str, int],
    community_labels: dict[int, str],
) -> None:
    """Print a summary of the analytics run."""
    n_communities = len(set(communities.values()))

    log.info("=" * 60)
    log.info("GRAPH ANALYTICS SUMMARY")
    log.info("=" * 60)
    log.info("Nodes analyzed:    {n}", n=G.number_of_nodes())
    log.info("Edges analyzed:    {e}", e=G.number_of_edges())
    log.info("Communities found: {c}", c=n_communities)
    log.info("")

    # Top 10 by PageRank
    top_10 = sorted(pagerank.items(), key=lambda x: x[1], reverse=True)[:10]
    log.info("Top 10 actors by PageRank:")
    for rank, (node_id, pr_score) in enumerate(top_10, 1):
        name = G.nodes.get(node_id, {}).get("name", node_id)
        category = G.nodes.get(node_id, {}).get("category", "?")
        cid = communities.get(node_id, -1)
        clabel = community_labels.get(cid, "?")
        log.info(
            "  {rank:2d}. {name:<40s} PR={pr:.6f}  community={cid} ({clabel})  [{cat}]",
            rank=rank, name=name[:40], pr=pr_score, cid=cid, clabel=clabel, cat=category,
        )

    log.info("")
    # Community size distribution
    community_sizes: dict[int, int] = {}
    for cid in communities.values():
        community_sizes[cid] = community_sizes.get(cid, 0) + 1
    top_communities = sorted(community_sizes.items(), key=lambda x: -x[1])[:10]
    log.info("Top 10 communities by size:")
    for cid, size in top_communities:
        clabel = community_labels.get(cid, "?")
        log.info("  Community {cid:4d}: {size:6d} members ({label})", cid=cid, size=size, label=clabel)
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_graph_analytics(scope: str = "full") -> dict:
    """Execute the full graph analytics pipeline. Returns summary dict.

    ``scope="curated"`` analyses only the named-market-actor subgraph and
    stores into ``actor_analytics_curated`` (the full table is untouched).
    """
    t_start = time.time()
    table_for_scope(scope)

    # Load graph
    G = load_actor_graph(scope=scope)
    if G.number_of_nodes() == 0:
        log.warning("Empty graph — nothing to analyze")
        return {"nodes": 0, "edges": 0, "communities": 0, "elapsed_s": 0, "scope": scope}

    # Compute all metrics
    pagerank = compute_pagerank(G)
    communities = compute_communities(G)
    betweenness = compute_betweenness(G)
    eigenvector = compute_eigenvector(G)
    degree_cent = compute_degree_centrality(G)
    hubs, authorities = compute_hits(G)

    # Community labeling
    community_labels = label_communities(G, communities, pagerank)

    # Store results
    stored = store_results(
        G, pagerank, communities, betweenness,
        eigenvector, degree_cent, hubs, authorities,
        scope=scope,
    )

    # Print summary
    print_summary(G, pagerank, communities, community_labels)

    elapsed = time.time() - t_start
    log.info("Total analytics pipeline completed in {t:.1f}s", t=elapsed)

    return {
        "nodes": G.number_of_nodes(),
        "edges": G.number_of_edges(),
        "communities": len(set(communities.values())),
        "stored": stored,
        "elapsed_s": round(elapsed, 1),
        "scope": scope,
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="GRID actor graph analytics")
    parser.add_argument("--scope", choices=sorted(SCOPES), default="full",
                        help="full = every edge; curated = named market actors only")
    args = parser.parse_args()
    result = run_graph_analytics(scope=args.scope)
    log.info("Done: {r}", r=result)
