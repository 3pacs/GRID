"""
GRID Intelligence — offline full-graph analytics for the actor universe.

Why this exists: the PWA's actor network was built inside a web request
(``intelligence/actors/graph.py::build_actor_graph``) and froze grid-api on
2026-11-06; hotfix #995 moved it to a background thread and capped it at the
~39K curated actors. The real universe is ~3.49M actors and ~11M edges across
``actor_connections``, ``entity_relationships`` and ``wealth_flows``. This
module loads ALL of it once, offline, into igraph and writes per-actor
metrics that requests only *read*:

* PageRank (directed, weighted), degree / weighted degree, approximate
  betweenness (Brandes–Pich source sampling), k-core (coreness), connected
  components, Leiden communities;
* a sector per actor (``intelligence/actors/sectors.py``: word-boundary
  keywords, title/category signals, then a neighbour vote);
* an ``importance`` score (PageRank percentile blended with the curated
  ``influence_score``) that the overview graph ranks by;
* community summaries, and the edges among an "overview pool" (every market
  actor plus the top of each bulk group) so the overview graph is two
  indexed reads.

Output tables (migration ``actor_universe_20261106``): ``actor_graph_runs``
(one row per run: version, status, counts, runtime, peak RSS),
``actor_graph_metrics``, ``actor_graph_communities``, ``actor_graph_edges``.
A run's rows are written in one transaction and the run flips to
``complete`` in the same transaction, so readers never see half a version.
The two newest complete versions are kept.

Identity: edge endpoints are resolved through ``actor_id_map`` (alias ->
canonical), ``actors.merged_into`` and ``actors.icij_node_id`` (the ICIJ
tables store raw node ids such as ``"11011497"`` or ``"icij_10111804"``).
Endpoints that resolve to no actor (ICIJ address nodes, tickers, trials)
stay in the graph as *external* nodes — they carry topology — but only
actors get metric rows.

Run: ``python -m intelligence.actors.universe`` (``--dry-run`` loads and
computes read-only and prints the stats, writing nothing). The intelligence
scheduler launches it as a niced subprocess (never inside grid-api); a
Postgres advisory lock keeps it single-flight across hosts.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import random
import resource
import socket
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log

# ── Tunables ───────────────────────────────────────────────────────────────

# Advisory lock id ("ACTRUNIV"): one job at a time, whichever host runs it.
JOB_LOCK_KEY = 0x4143545255_4E4956 & 0x7FFFFFFFFFFFFFFF
KEEP_VERSIONS = 2
MIN_COMMUNITY_SIZE = 3
COMMUNITY_TOP_ACTORS = 5
POOL_PER_BULK_GROUP = 25_000
POOL_EDGE_CAP = 50
COPY_CHUNK_ROWS = 250_000

# Category groups. ``market`` is the default overview population; the bulk
# screening / leak lists are opt-in groups (never silently excluded).
GROUP_MARKET = "market"
GROUP_ICIJ = "icij"
GROUP_PEP = "pep"
GROUP_SANCTIONED = "sanctioned"
GROUP_DONOR = "donor"
GROUP_WATCHLIST = "watchlist"
# Rows an operator cleanup re-categorized as ``<category>_quarantined_<date>``
# (e.g. the 2026-09-28 junk-corporation quarantine): opt-in, never "market".
GROUP_QUARANTINED = "quarantined"
CATEGORY_GROUPS: tuple[str, ...] = (
    GROUP_MARKET, GROUP_ICIJ, GROUP_PEP, GROUP_SANCTIONED, GROUP_DONOR, GROUP_WATCHLIST,
    GROUP_QUARANTINED,
)
BULK_GROUPS: tuple[str, ...] = tuple(g for g in CATEGORY_GROUPS if g != GROUP_MARKET)
_GROUP_BY_CATEGORY: dict[str, str] = {
    "pep": GROUP_PEP,
    "sanctioned": GROUP_SANCTIONED,
    "ofac_sanctioned": GROUP_SANCTIONED,
    "donor": GROUP_DONOR,
    "person_of_interest": GROUP_WATCHLIST,
    "entity_of_interest": GROUP_WATCHLIST,
}


def category_group(category: Any, source: Any = None) -> str:
    """Overview group for an actor (``market`` unless it is a bulk-list row)."""
    cat = category.strip() if isinstance(category, str) else ""
    src = source.strip().lower() if isinstance(source, str) else ""
    if cat.startswith("icij") or src == "icij":
        return GROUP_ICIJ
    if "_quarantined" in cat:
        return GROUP_QUARANTINED
    return _GROUP_BY_CATEGORY.get(cat, GROUP_MARKET)


def _betweenness_samples() -> int:
    try:
        return max(0, int(os.environ.get("GRID_ACTOR_GRAPH_BETWEENNESS_SAMPLES", "48")))
    except ValueError:
        return 48


def peak_rss_mb() -> float:
    """Peak resident set size of this process in MB (Linux ru_maxrss is KB)."""
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, 1)


# ── Loading ────────────────────────────────────────────────────────────────

_ACTORS_COPY = (
    "COPY (SELECT id, name, category, title, influence_score, icij_node_id, "
    "merged_into, source, "
    "CASE WHEN known_positions IS NOT NULL AND known_positions <> '[]'::jsonb "
    "     THEN known_positions::text END, "
    "CASE WHEN board_seats IS NOT NULL AND board_seats <> '[]'::jsonb "
    "     THEN board_seats::text END "
    "FROM actors) TO STDOUT WITH (FORMAT csv)"
)
_ACTOR_COLUMNS = [
    "id", "name", "category", "title", "influence", "icij_node_id",
    "merged_into", "source", "positions_text", "board_text",
]
_EDGE_COPIES: dict[str, str] = {
    "actor_connections": (
        "COPY (SELECT actor_a, actor_b, COALESCE(strength, 0.5), relationship "
        "FROM actor_connections WHERE actor_a IS NOT NULL AND actor_b IS NOT NULL) "
        "TO STDOUT WITH (FORMAT csv)"
    ),
    "entity_relationships": (
        "COPY (SELECT actor_a, actor_b, COALESCE(strength, 0.5), relationship "
        "FROM entity_relationships WHERE actor_a IS NOT NULL AND actor_b IS NOT NULL) "
        "TO STDOUT WITH (FORMAT csv)"
    ),
    "wealth_flows": (
        "COPY (SELECT from_actor, to_entity, "
        "CASE WHEN confidence = 'confirmed' THEN 0.6 ELSE 0.4 END, 'wealth_flow' "
        "FROM wealth_flows WHERE from_actor IS NOT NULL AND to_entity IS NOT NULL) "
        "TO STDOUT WITH (FORMAT csv)"
    ),
}
_ALIAS_COPY = (
    "COPY (SELECT alias_id, canonical_id FROM actor_id_map "
    "WHERE alias_id <> canonical_id) TO STDOUT WITH (FORMAT csv)"
)


def _copy_frame(cursor: Any, sql: str, names: list[str], dtypes: dict[str, Any]) -> Any:
    """Run a ``COPY ... TO STDOUT (FORMAT csv)`` and parse it with pandas."""
    import pandas as pd

    buf = io.BytesIO()
    cursor.copy_expert(sql, buf)
    buf.seek(0)
    if buf.getbuffer().nbytes == 0:
        return pd.DataFrame({n: pd.Series(dtype=dtypes.get(n, object)) for n in names})
    frame = pd.read_csv(
        buf, header=None, names=names, dtype=dtypes,
        keep_default_na=False, na_values={n: [""] for n in names},
    )
    return frame


@dataclass
class LoadedUniverse:
    """Raw frames as read from Postgres (one REPEATABLE READ snapshot)."""

    actors: Any
    edges: Any  # columns: src, dst, weight, relationship, table
    aliases: Any
    load_seconds: float = 0.0
    counts: dict[str, int] = field(default_factory=dict)


def load_universe(engine: Any) -> LoadedUniverse:
    """Read actors, the three edge tables and the alias map in one snapshot."""
    import pandas as pd

    t0 = time.monotonic()
    raw = engine.raw_connection()
    try:
        dbapi = getattr(raw, "driver_connection", None) or raw.connection
        cur = dbapi.cursor()
        # First statement of the transaction: one consistent read-only
        # snapshot across all five COPYs (no session state leaks to the pool).
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cur.execute("SET LOCAL statement_timeout = '30min'")
        actors = _copy_frame(
            cur, _ACTORS_COPY, _ACTOR_COLUMNS,
            {"id": str, "name": str, "category": str, "title": str, "influence": "float64",
             "icij_node_id": str, "merged_into": str, "source": str,
             "positions_text": str, "board_text": str},
        )
        log.info("actor universe: {n} actors loaded", n=len(actors))
        frames = []
        counts: dict[str, int] = {"actors": len(actors)}
        for table, sql in _EDGE_COPIES.items():
            f = _copy_frame(
                cur, sql, ["src", "dst", "weight", "relationship"],
                {"src": str, "dst": str, "weight": "float64", "relationship": str},
            )
            f["relationship"] = f["relationship"].fillna("connected").astype("category")
            f["table"] = table
            counts[table] = len(f)
            log.info("actor universe: {n} rows from {t}", n=len(f), t=table)
            frames.append(f)
        aliases = _copy_frame(cur, _ALIAS_COPY, ["alias_id", "canonical_id"],
                              {"alias_id": str, "canonical_id": str})
        counts["aliases"] = len(aliases)
        dbapi.rollback()
    finally:
        raw.close()
    edges = pd.concat(frames, ignore_index=True)
    edges["relationship"] = edges["relationship"].astype("category")
    edges["table"] = edges["table"].astype("category")
    return LoadedUniverse(actors=actors, edges=edges, aliases=aliases,
                          load_seconds=round(time.monotonic() - t0, 1), counts=counts)


# ── Identity resolution and graph assembly ─────────────────────────────────

def resolve_endpoint_ids(uniques: Any, actors: Any, aliases: Any) -> Any:
    """Map raw edge endpoint ids to canonical actor ids where possible.

    Order: ``actor_id_map`` alias -> exact actor id -> ICIJ node id (``"123"``
    or ``"icij_123"`` -> the actor whose ``icij_node_id`` is ``"123"``) ->
    ``merged_into`` chain (up to 3 hops). Unresolved ids are returned as-is
    (external nodes). Returns an object ndarray aligned with ``uniques``.
    """
    import pandas as pd

    s = pd.Series(pd.Index(uniques).astype(object), dtype=object)
    if len(aliases):
        alias_map = pd.Series(aliases["canonical_id"].values, index=aliases["alias_id"].values)
        alias_map = alias_map[~alias_map.index.duplicated()]
        mapped = s.map(alias_map)
        s = mapped.where(mapped.notna(), s)
    actor_index = pd.Index(actors["id"].values)
    is_actor = pd.Series(actor_index.get_indexer(s.values) >= 0, index=s.index)
    if (~is_actor).any():
        icij = actors.loc[actors["icij_node_id"].notna(), ["icij_node_id", "id"]]
        if len(icij):
            icij_map = pd.Series(icij["id"].values, index=icij["icij_node_id"].astype(str).values)
            icij_map = icij_map[~icij_map.index.duplicated()]
            cand = s[~is_actor].astype(str).str.extract(r"^(?:icij_)?(\d+)$", expand=False)
            hit = cand.dropna().map(icij_map).dropna()
            if len(hit):
                s.loc[hit.index] = hit.values
    merged = actors.loc[actors["merged_into"].notna(), ["id", "merged_into"]]
    if len(merged):
        merged_map = pd.Series(merged["merged_into"].values, index=merged["id"].values)
        merged_map = merged_map[merged_map.isin(actor_index)]
        merged_map = merged_map[~merged_map.index.duplicated()]
        for _ in range(3):
            nxt = s.map(merged_map)
            hit = nxt.notna()
            if not hit.any():
                break
            s.loc[hit] = nxt[hit]
    return s.to_numpy(dtype=object)


@dataclass
class AssembledGraph:
    """Integer-indexed graph arrays. Nodes ``0..n_actors-1`` are actors."""

    node_ids: Any            # object ndarray, len n_nodes
    n_actors: int
    dir_src: Any
    dir_dst: Any
    dir_w: Any
    und_src: Any
    und_dst: Any
    und_w: Any
    und_rel: Any             # object ndarray of relationship labels
    raw_edges: int = 0
    self_loops: int = 0


def _dedupe_max(src: Any, dst: Any, w: Any, n: int) -> Any:
    """Indices of unique (src, dst) pairs, keeping the max-weight duplicate."""
    import numpy as np

    key = src.astype(np.int64) * np.int64(n) + dst.astype(np.int64)
    order = np.argsort(-w, kind="stable")
    _, first = np.unique(key[order], return_index=True)
    return order[first]


def assemble_graph(universe: LoadedUniverse) -> AssembledGraph:
    """Resolve identities, index nodes and deduplicate edges."""
    import numpy as np
    import pandas as pd

    actors = universe.actors
    live = actors["merged_into"].isna()
    actor_ids = actors.loc[live, "id"].to_numpy(dtype=object)
    n_actors = len(actor_ids)

    edges = universe.edges
    n_raw = len(edges)
    ends = pd.concat([edges["src"], edges["dst"]], ignore_index=True)
    codes, uniques = pd.factorize(ends)
    resolved = resolve_endpoint_ids(uniques, actors, universe.aliases)

    base_index = pd.Index(actor_ids)
    pos = base_index.get_indexer(resolved)
    external = pd.unique(resolved[pos < 0])
    node_ids = np.concatenate([actor_ids, np.asarray(external, dtype=object)])
    node_index = pd.Index(node_ids)
    uniq_to_node = node_index.get_indexer(resolved).astype(np.int64)
    end_nodes = uniq_to_node[codes]
    src = end_nodes[:n_raw]
    dst = end_nodes[n_raw:]
    w = edges["weight"].to_numpy(dtype=np.float64)
    w = np.where(np.isfinite(w) & (w > 0), w, 0.5).astype(np.float32)
    rel = edges["relationship"].astype(str).to_numpy(dtype=object)

    loops = src == dst
    src, dst, w, rel = src[~loops], dst[~loops], w[~loops], rel[~loops]
    n = len(node_ids)

    keep = _dedupe_max(src, dst, w, n)
    dir_src, dir_dst, dir_w = src[keep], dst[keep], w[keep]

    lo = np.minimum(src, dst)
    hi = np.maximum(src, dst)
    keep_u = _dedupe_max(lo, hi, w, n)
    return AssembledGraph(
        node_ids=node_ids, n_actors=n_actors,
        dir_src=dir_src.astype(np.int32), dir_dst=dir_dst.astype(np.int32), dir_w=dir_w,
        und_src=lo[keep_u].astype(np.int32), und_dst=hi[keep_u].astype(np.int32),
        und_w=w[keep_u], und_rel=rel[keep_u],
        raw_edges=n_raw, self_loops=int(loops.sum()),
    )


# ── Metrics ────────────────────────────────────────────────────────────────

@dataclass
class GraphMetrics:
    pagerank: Any
    degree: Any
    weighted_degree: Any
    betweenness: Any
    coreness: Any
    component: Any
    component_sizes: Any
    community: Any           # int ndarray, -1 = isolated / no community
    community_sizes: Any
    timings: dict[str, float] = field(default_factory=dict)
    betweenness_samples: int = 0


def _renumber_by_size(membership: Any) -> tuple[Any, Any]:
    """Relabel cluster ids so 0 is the largest; returns (labels, sizes)."""
    import numpy as np

    membership = np.asarray(membership, dtype=np.int64)
    sizes = np.bincount(membership)
    order = np.argsort(-sizes, kind="stable")
    remap = np.empty_like(order)
    remap[order] = np.arange(order.size)
    return remap[membership], sizes[order]


def compute_metrics(graph: AssembledGraph, *, betweenness_samples: int | None = None,
                    seed: int = 42) -> GraphMetrics:
    """PageRank, degree, betweenness (sampled), k-core, components, Leiden."""
    import igraph as ig
    import numpy as np

    random.seed(seed)
    ig.set_random_number_generator(random)
    n = len(graph.node_ids)
    timings: dict[str, float] = {}

    t = time.monotonic()
    # zip of two int lists is the fastest edge feed python-igraph accepts.
    g_dir = ig.Graph(n=n, edges=list(zip(graph.dir_src.tolist(), graph.dir_dst.tolist())), directed=True)
    g_und = ig.Graph(n=n, edges=list(zip(graph.und_src.tolist(), graph.und_dst.tolist())), directed=False)
    timings["build_s"] = round(time.monotonic() - t, 1)
    log.info("actor universe: build_s = {s}", s=timings["build_s"])

    t = time.monotonic()
    pagerank = np.asarray(g_dir.pagerank(weights=graph.dir_w.tolist(), directed=True, damping=0.85),
                          dtype=np.float64)
    timings["pagerank_s"] = round(time.monotonic() - t, 1)
    log.info("actor universe: pagerank_s = {s}", s=timings["pagerank_s"])
    del g_dir

    t = time.monotonic()
    degree = np.asarray(g_und.degree(), dtype=np.int64)
    und_w = graph.und_w.tolist()
    weighted_degree = np.asarray(g_und.strength(weights=und_w), dtype=np.float64)
    coreness = np.asarray(g_und.coreness(), dtype=np.int64)
    cc = g_und.connected_components()
    component, component_sizes = _renumber_by_size(cc.membership)
    timings["degree_core_components_s"] = round(time.monotonic() - t, 1)
    log.info("actor universe: degree_core_components_s = {s}", s=timings["degree_core_components_s"])

    t = time.monotonic()
    leiden = g_und.community_leiden(objective_function="modularity", weights=und_w,
                                    n_iterations=2, resolution=1.0)
    community, community_sizes = _renumber_by_size(leiden.membership)
    community = np.where(degree > 0, community, -1)
    timings["leiden_s"] = round(time.monotonic() - t, 1)
    log.info("actor universe: leiden_s = {s}", s=timings["leiden_s"])

    k = _betweenness_samples() if betweenness_samples is None else betweenness_samples
    betweenness = np.zeros(n, dtype=np.float64)
    t = time.monotonic()
    giant = np.flatnonzero(component == 0) if component_sizes.size else np.array([], dtype=np.int64)
    if k > 0 and giant.size > 2:
        rng = np.random.default_rng(seed)
        sources = rng.choice(giant, size=min(k, giant.size), replace=False)
        raw = np.asarray(g_und.betweenness(directed=False, sources=sources.tolist(), targets=None),
                         dtype=np.float64)
        # Brandes–Pich: scale the sampled dependency sums to all sources.
        betweenness = raw * (giant.size / float(sources.size))
        k = int(sources.size)
    else:
        k = 0
    timings["betweenness_s"] = round(time.monotonic() - t, 1)
    log.info("actor universe: betweenness_s = {s}", s=timings["betweenness_s"])
    return GraphMetrics(
        pagerank=pagerank, degree=degree, weighted_degree=weighted_degree,
        betweenness=betweenness, coreness=coreness, component=component,
        component_sizes=component_sizes, community=community,
        community_sizes=community_sizes, timings=timings, betweenness_samples=k,
    )


# ── Per-actor table ────────────────────────────────────────────────────────

def build_actor_table(universe: LoadedUniverse, graph: AssembledGraph, metrics: GraphMetrics) -> Any:
    """One row per (live) actor with metrics, sector, group and importance."""
    import numpy as np
    import pandas as pd

    from intelligence.actors import sectors as sec

    actors = universe.actors
    live = actors.loc[actors["merged_into"].isna()].reset_index(drop=True)
    na = graph.n_actors
    assert len(live) == na

    sector, basis = sec.tag_sectors_frame(live)

    # Neighbour vote for actors that only have the default sector.
    labels = sorted(set(sector.tolist()))
    code_of = {lab: i for i, lab in enumerate(labels)}
    unknown = len(labels)
    codes = np.full(len(graph.node_ids), unknown, dtype=np.int64)
    voter = basis.isin([sec.BASIS_TITLE, sec.BASIS_KEYWORD, sec.BASIS_CATEGORY]).to_numpy() & \
        ~live["category"].fillna("").isin(sec.NO_INFERENCE_CATEGORIES).to_numpy()
    actor_codes = sector.map(code_of).to_numpy(dtype=np.int64)
    codes[:na] = np.where(voter, actor_codes, unknown)
    eligible = np.zeros(len(graph.node_ids), dtype=bool)
    eligible[:na] = (basis == sec.BASIS_DEFAULT).to_numpy() & \
        ~live["category"].fillna("").isin(sec.NO_INFERENCE_CATEGORIES).to_numpy()
    voted = sec.propagate_sectors(graph.und_src, graph.und_dst, codes, eligible, unknown_code=unknown)
    changed = np.flatnonzero(eligible[:na] & (voted[:na] != unknown))
    if changed.size:
        sector.iloc[changed] = [labels[c] for c in voted[changed]]
        basis.iloc[changed] = sec.BASIS_NEIGHBOR

    pr = metrics.pagerank[:na]
    pr_pct = pd.Series(pr).rank(pct=True, method="average").to_numpy()
    influence = live["influence"].astype("float64").fillna(0.5).clip(0.0, 1.0).to_numpy()
    importance = 0.6 * pr_pct + 0.4 * influence

    groups = [category_group(c, s) for c, s in zip(live["category"].tolist(), live["source"].tolist())]
    community = metrics.community[:na]
    table = pd.DataFrame({
        "actor_id": live["id"].astype(str).to_numpy(),
        "category": live["category"].fillna("unknown").astype(str).to_numpy(),
        "category_group": groups,
        "sector": sector.astype(str).to_numpy(),
        "sector_basis": basis.astype(str).to_numpy(),
        "pagerank": pr,
        "importance": np.round(importance, 6),
        "degree": metrics.degree[:na],
        "weighted_degree": np.round(metrics.weighted_degree[:na], 4),
        "betweenness": metrics.betweenness[:na],
        "coreness": metrics.coreness[:na],
        "community_id": pd.array(np.where(community >= 0, community, 0), dtype="Int64"),
        "component_id": metrics.component[:na],
    })
    table.loc[community < 0, "community_id"] = pd.NA
    table["group_rank"] = (
        table.groupby("category_group")["importance"].rank(method="first", ascending=False)
        .astype("int64")
    )
    table["in_overview_pool"] = (table["category_group"] == GROUP_MARKET) | \
        (table["group_rank"] <= POOL_PER_BULK_GROUP)
    return table


def build_community_table(table: Any, graph: AssembledGraph, metrics: GraphMetrics) -> Any:
    """Community summaries (communities with >= MIN_COMMUNITY_SIZE nodes)."""
    import numpy as np
    import pandas as pd

    comm = metrics.community
    sizes = metrics.community_sizes
    valid = np.flatnonzero(sizes >= MIN_COMMUNITY_SIZE)
    if valid.size == 0:
        return pd.DataFrame(columns=[
            "community_id", "size", "actor_count", "internal_edges", "top_actor_ids",
            "top_sectors", "top_categories", "dominant_sector", "dominant_category", "max_importance",
        ])
    cs, cd = comm[graph.und_src], comm[graph.und_dst]
    same = (cs == cd) & (cs >= 0)
    internal = np.bincount(cs[same], minlength=sizes.size)

    members = table.loc[table["community_id"].notna(),
                        ["actor_id", "community_id", "importance", "sector", "category"]].copy()
    members["community_id"] = members["community_id"].astype("int64")
    members = members[members["community_id"].isin(valid)]
    members = members.sort_values(["community_id", "importance"], ascending=[True, False])
    actor_count = members.groupby("community_id").size()
    max_imp = members.groupby("community_id")["importance"].max()
    top_ids: dict[int, list[str]] = {}
    head = members.groupby("community_id").head(COMMUNITY_TOP_ACTORS)
    for cid, aid in zip(head["community_id"].tolist(), head["actor_id"].tolist()):
        top_ids.setdefault(cid, []).append(aid)

    def _top_counts(col: str) -> dict[int, list[dict[str, Any]]]:
        c = members.groupby(["community_id", col]).size().rename("n").reset_index()
        c = c.sort_values(["community_id", "n"], ascending=[True, False])
        c = c.groupby("community_id").head(5)
        acc: dict[int, list[dict[str, Any]]] = {}
        for cid, key, cnt in zip(c["community_id"].tolist(), c[col].tolist(), c["n"].tolist()):
            acc.setdefault(cid, []).append({"name": str(key), "count": int(cnt)})
        return acc

    top_sectors = _top_counts("sector")
    top_cats = _top_counts("category")
    out = pd.DataFrame({"community_id": valid})
    out["size"] = sizes[valid]
    out["actor_count"] = out["community_id"].map(actor_count).fillna(0).astype("int64")
    out["internal_edges"] = internal[valid]
    out["top_actor_ids"] = [top_ids.get(c, []) for c in out["community_id"].tolist()]
    out["top_sectors"] = [top_sectors.get(c, []) for c in out["community_id"].tolist()]
    out["top_categories"] = [top_cats.get(c, []) for c in out["community_id"].tolist()]
    out["max_importance"] = out["community_id"].map(max_imp).fillna(0.0)
    out = out[out["actor_count"] > 0].reset_index(drop=True)
    out["dominant_sector"] = out["top_sectors"].apply(lambda v: v[0]["name"] if v else None)
    out["dominant_category"] = out["top_categories"].apply(lambda v: v[0]["name"] if v else None)
    return out


def build_pool_edges(table: Any, graph: AssembledGraph) -> Any:
    """Edges among overview-pool actors, capped at POOL_EDGE_CAP per node."""
    import numpy as np
    import pandas as pd

    na = graph.n_actors
    pool = np.zeros(len(graph.node_ids), dtype=bool)
    pool[:na] = table["in_overview_pool"].to_numpy()
    s, d = graph.und_src, graph.und_dst
    keep = pool[s] & pool[d]
    e = pd.DataFrame({"s": s[keep], "d": d[keep], "w": graph.und_w[keep], "rel": graph.und_rel[keep]})
    if e.empty:
        return pd.DataFrame(columns=["src", "dst", "weight", "relationship"])
    e = e.sort_values("w", ascending=False, kind="stable").reset_index(drop=True)
    rank_s = e.groupby("s").cumcount()
    rank_d = e.groupby("d").cumcount()
    e = e[(rank_s < POOL_EDGE_CAP) | (rank_d < POOL_EDGE_CAP)]
    ids = graph.node_ids
    return pd.DataFrame({
        "src": ids[e["s"].to_numpy()],
        "dst": ids[e["d"].to_numpy()],
        "weight": e["w"].astype("float64").round(4).to_numpy(),
        "relationship": e["rel"].astype(str).to_numpy(),
    })


# ── Writing ────────────────────────────────────────────────────────────────

_METRIC_COLUMNS = [
    "graph_version", "actor_id", "category", "category_group", "sector", "sector_basis",
    "pagerank", "importance", "degree", "weighted_degree", "betweenness", "coreness",
    "community_id", "component_id", "group_rank", "in_overview_pool",
]
_COMMUNITY_COLUMNS = [
    "graph_version", "community_id", "size", "actor_count", "internal_edges",
    "top_actor_ids", "top_sectors", "top_categories", "dominant_sector",
    "dominant_category", "max_importance",
]
_EDGE_COLUMNS = ["graph_version", "src", "dst", "weight", "relationship"]


def _pg_array(values: list[str]) -> str:
    inner = ",".join('"' + v.replace("\\", "\\\\").replace('"', '\\"') + '"' for v in values)
    return "{" + inner + "}"


def _copy_in(cursor: Any, table: str, columns: list[str], frame: Any) -> int:
    """COPY a frame into ``table`` in chunks (csv)."""
    if frame is None or len(frame) == 0:
        return 0
    cols = ", ".join(columns)
    # ``table``/``columns`` are module constants, never user input.
    sql = f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv, NULL '\\N')"
    total = 0
    for start in range(0, len(frame), COPY_CHUNK_ROWS):
        chunk = frame.iloc[start:start + COPY_CHUNK_ROWS]
        buf = io.StringIO()
        chunk.to_csv(buf, header=False, index=False, na_rep="\\N", columns=columns)
        buf.seek(0)
        cursor.copy_expert(sql, buf)
        total += len(chunk)
    return total


def write_run(engine: Any, version: str, table: Any, communities: Any, edges: Any,
              stats: dict[str, Any]) -> dict[str, int]:
    """Write one version's rows and flip the run to ``complete`` atomically."""
    from sqlalchemy import text

    metrics = table.copy()
    metrics.insert(0, "graph_version", version)
    comm = communities.copy()
    comm.insert(0, "graph_version", version)
    comm["top_actor_ids"] = comm["top_actor_ids"].apply(_pg_array)
    comm["top_sectors"] = comm["top_sectors"].apply(json.dumps)
    comm["top_categories"] = comm["top_categories"].apply(json.dumps)
    edg = edges.copy()
    edg.insert(0, "graph_version", version)

    written: dict[str, int] = {}
    raw = engine.raw_connection()
    try:
        dbapi = getattr(raw, "driver_connection", None) or raw.connection
        cur = dbapi.cursor()
        cur.execute("SET LOCAL statement_timeout = '60min'")
        written["metrics"] = _copy_in(cur, "actor_graph_metrics", _METRIC_COLUMNS, metrics)
        written["communities"] = _copy_in(cur, "actor_graph_communities", _COMMUNITY_COLUMNS, comm)
        written["edges"] = _copy_in(cur, "actor_graph_edges", _EDGE_COLUMNS, edg)
        cur.execute(
            "UPDATE actor_graph_runs SET status = 'complete', computed_at = NOW(), "
            "node_count = %(nodes)s, edge_count = %(edges)s, actor_count = %(actors)s, "
            "external_node_count = %(external)s, community_count = %(communities)s, "
            "component_count = %(components)s, giant_component_size = %(giant)s, "
            "runtime_s = %(runtime)s, peak_rss_mb = %(rss)s, stats = %(stats)s::jsonb "
            "WHERE graph_version = %(v)s",
            {
                "nodes": stats["nodes"], "edges": stats["undirected_edges"],
                "actors": stats["actors"], "external": stats["external_nodes"],
                "communities": stats["communities"], "components": stats["components"],
                "giant": stats["giant_component_size"], "runtime": stats["runtime_s"],
                "rss": stats["peak_rss_mb"], "stats": json.dumps(stats, default=str), "v": version,
            },
        )
        dbapi.commit()
    except Exception:
        dbapi.rollback()
        raise
    finally:
        raw.close()

    with engine.begin() as conn:
        keep = [r[0] for r in conn.execute(text(
            "SELECT graph_version FROM actor_graph_runs WHERE status = 'complete' "
            "ORDER BY computed_at DESC LIMIT :k"
        ), {"k": KEEP_VERSIONS}).fetchall()]
        for tbl in ("actor_graph_metrics", "actor_graph_communities", "actor_graph_edges"):
            # tbl comes from the literal tuple above.
            conn.execute(text(f"DELETE FROM {tbl} WHERE NOT (graph_version = ANY(:keep))"),
                         {"keep": keep})
    return written


def _start_run(engine: Any, version: str, params: dict[str, Any]) -> None:
    from sqlalchemy import text

    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO actor_graph_runs (graph_version, status, started_at, host, params) "
            "VALUES (:v, 'running', NOW(), :host, CAST(:params AS jsonb))"
        ), {"v": version, "host": socket.gethostname(), "params": json.dumps(params)})


def _fail_run(engine: Any, version: str, error: str) -> None:
    from sqlalchemy import text

    try:
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE actor_graph_runs SET status = 'failed', computed_at = NOW(), "
                "error = :err WHERE graph_version = :v"
            ), {"v": version, "err": error[:2000]})
    except Exception as exc:  # noqa: BLE001
        log.warning("actor universe: could not mark run {v} failed: {e}", v=version, e=str(exc))


# ── Orchestration ──────────────────────────────────────────────────────────

def compute_universe(universe: LoadedUniverse, *, betweenness_samples: int | None = None
                     ) -> tuple[Any, Any, Any, dict[str, Any]]:
    """Assemble + metrics + tables; returns (actors, communities, edges, stats)."""
    import numpy as np

    t = time.monotonic()
    graph = assemble_graph(universe)
    assemble_s = round(time.monotonic() - t, 1)
    log.info("actor universe: assembled {n} nodes / {e} undirected edges in {s}s",
             n=len(graph.node_ids), e=int(graph.und_src.size), s=assemble_s)
    metrics = compute_metrics(graph, betweenness_samples=betweenness_samples)
    t = time.monotonic()
    table = build_actor_table(universe, graph, metrics)
    communities = build_community_table(table, graph, metrics)
    edges = build_pool_edges(table, graph)
    tables_s = round(time.monotonic() - t, 1)
    log.info("actor universe: actor/community/edge tables built in {s}s", s=tables_s)
    stats = {
        "nodes": len(graph.node_ids),
        "actors": int(graph.n_actors),
        "external_nodes": int(len(graph.node_ids) - graph.n_actors),
        "raw_edges": int(graph.raw_edges),
        "self_loops": int(graph.self_loops),
        "directed_edges": int(graph.dir_src.size),
        "undirected_edges": int(graph.und_src.size),
        "communities": int(np.count_nonzero(metrics.community_sizes >= MIN_COMMUNITY_SIZE)),
        "components": int(metrics.component_sizes.size),
        "giant_component_size": int(metrics.component_sizes[0]) if metrics.component_sizes.size else 0,
        "isolated_actors": int(np.count_nonzero(metrics.degree[:graph.n_actors] == 0)),
        "max_coreness": int(metrics.coreness.max()) if metrics.coreness.size else 0,
        "betweenness_samples": metrics.betweenness_samples,
        "overview_pool": int(table["in_overview_pool"].sum()),
        "overview_pool_edges": len(edges),
        "sector_counts": table["sector"].value_counts().head(25).to_dict(),
        "sector_basis_counts": table["sector_basis"].value_counts().to_dict(),
        "group_counts": table["category_group"].value_counts().to_dict(),
        "timings": {"load_s": universe.load_seconds, "assemble_s": assemble_s,
                    **metrics.timings, "tables_s": tables_s},
        "source_rows": universe.counts,
    }
    return table, communities, edges, stats


def run_actor_universe_job(engine: Any = None, *, dry_run: bool = False,
                           betweenness_samples: int | None = None) -> dict[str, Any]:
    """Load, compute and (unless ``dry_run``) write one graph version.

    Single-flight through a Postgres advisory lock held on a dedicated
    connection for the whole run. Returns the run stats (``skipped`` when
    another run holds the lock).
    """
    from sqlalchemy import text

    if engine is None:
        from db import get_engine

        engine = get_engine()
    t0 = time.monotonic()
    version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    # A dry run is strictly read-only: no advisory lock, no run row.
    lock_conn = None if dry_run else engine.connect()
    try:
        got = True
        if lock_conn is not None:
            got = lock_conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": JOB_LOCK_KEY}).scalar()
            lock_conn.commit()
        if not got:
            log.info("actor universe: another run holds the lock; skipping")
            return {"skipped": True, "reason": "locked"}
        params = {"betweenness_samples": betweenness_samples if betweenness_samples is not None
                  else _betweenness_samples(), "pool_per_bulk_group": POOL_PER_BULK_GROUP,
                  "pool_edge_cap": POOL_EDGE_CAP, "dry_run": dry_run}
        if not dry_run:
            _start_run(engine, version, params)
        try:
            universe = load_universe(engine)
            table, communities, edges, stats = compute_universe(
                universe, betweenness_samples=betweenness_samples)
            del universe
            stats["graph_version"] = version
            stats["runtime_s"] = round(time.monotonic() - t0, 1)
            stats["peak_rss_mb"] = peak_rss_mb()
            if dry_run:
                stats["dry_run"] = True
                return stats
            t = time.monotonic()
            stats["written"] = write_run(engine, version, table, communities, edges, stats)
            stats["write_s"] = round(time.monotonic() - t, 1)
            stats["runtime_s"] = round(time.monotonic() - t0, 1)
            stats["peak_rss_mb"] = peak_rss_mb()
            log.info("actor universe {v}: {n} nodes / {e} edges / {c} communities in {s}s "
                     "(peak RSS {m} MB)", v=version, n=stats["nodes"], e=stats["undirected_edges"],
                     c=stats["communities"], s=stats["runtime_s"], m=stats["peak_rss_mb"])
            return stats
        except Exception as exc:
            if not dry_run:
                _fail_run(engine, version, f"{type(exc).__name__}: {exc}")
            raise
    finally:
        if lock_conn is not None:
            try:
                lock_conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": JOB_LOCK_KEY})
                lock_conn.commit()
            except Exception as exc:  # noqa: BLE001 - closing the session frees it anyway
                log.debug("actor universe: advisory unlock failed: {e}", e=str(exc))
            lock_conn.close()


# ── Scheduler hooks (subprocess, never in-process) ─────────────────────────

_child: Any = None


def job_enabled() -> bool:
    """``GRID_ACTOR_UNIVERSE_JOB=0`` turns the scheduled job off."""
    return os.environ.get("GRID_ACTOR_UNIVERSE_JOB", "1").strip().lower() not in ("0", "false", "no", "off")


def latest_complete_age_hours(engine: Any) -> float | None:
    """Hours since the newest complete run (None: no run / table absent)."""
    from sqlalchemy import text

    with engine.connect() as conn:
        if not conn.execute(text("SELECT to_regclass('actor_graph_runs')")).scalar():
            return None
        age = conn.execute(text(
            "SELECT EXTRACT(EPOCH FROM (NOW() - MAX(computed_at))) / 3600.0 "
            "FROM actor_graph_runs WHERE status = 'complete'"
        )).scalar()
    return float(age) if age is not None else None


def launch_job_subprocess(*, nice: int = 10) -> bool:
    """Start the job as a niced child process unless one is still running.

    The igraph work holds the GIL for minutes; running it inside the
    scheduler (or grid-api) would stall every other task, so it always runs
    in its own interpreter. Output goes to the parent's journal. Returns
    True when a child was started.
    """
    import subprocess

    global _child
    if _child is not None and _child.poll() is None:
        log.info("actor universe: previous job (pid {p}) still running; not relaunching", p=_child.pid)
        return False

    import shutil

    argv = [sys.executable, "-m", "intelligence.actors.universe"]
    nice_bin = shutil.which("nice")
    if nice_bin:  # no preexec_fn: unsafe in a threaded parent
        argv = [nice_bin, "-n", str(int(nice)), *argv]
    _child = subprocess.Popen(argv, close_fds=True)
    log.info("actor universe: job started (pid {p})", p=_child.pid)
    return True


# The 03:40 daily run skips only when a run finished this recently (e.g. a
# restart catch-up just before the slot). A wider window let a late catch-up
# (22:30) silently cancel the next morning's run.
DAILY_MIN_AGE_HOURS = 3.0
CATCH_UP_MAX_AGE_HOURS = 26.0


def maybe_launch(engine: Any = None, *, max_age_hours: float = CATCH_UP_MAX_AGE_HOURS,
                 trigger: str = "catch-up") -> bool:
    """Launch the job when it is enabled and the newest run is stale/missing.

    Every skip is logged with its reason so a run that did not happen is
    visible in the journal.
    """
    if not job_enabled():
        log.info("actor universe ({t}): GRID_ACTOR_UNIVERSE_JOB is off; not launching", t=trigger)
        return False
    if engine is None:
        from db import get_engine

        engine = get_engine()
    try:
        age = latest_complete_age_hours(engine)
    except Exception as exc:  # noqa: BLE001
        log.warning("actor universe: freshness check failed: {e}", e=str(exc))
        return False
    if age is not None and age < max_age_hours:
        log.info("actor universe ({t}): newest run is {a:.1f}h old (< {m:g}h); not launching",
                 t=trigger, a=age, m=max_age_hours)
        return False
    return launch_job_subprocess()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="GRID actor universe graph analytics")
    parser.add_argument("--dry-run", action="store_true",
                        help="load + compute read-only; print stats; write nothing")
    parser.add_argument("--betweenness-samples", type=int, default=None)
    args = parser.parse_args(argv)
    stats = run_actor_universe_job(dry_run=args.dry_run, betweenness_samples=args.betweenness_samples)
    print(json.dumps(stats, default=str, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
