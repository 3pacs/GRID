"""
GRID Intelligence — request-time reads of the precomputed actor universe.

Everything here is a bounded, indexed read of the tables written by
``intelligence/actors/universe.py`` (``actor_graph_runs`` /
``actor_graph_metrics`` / ``actor_graph_communities`` /
``actor_graph_edges``) or of the live edge tables with per-node fan-out
limits. Nothing builds a graph in Python over the whole universe.

* :func:`latest_run`        — newest complete run (version, computed_at, stats)
* :func:`overview`          — top-N actors by importance (+ edges among them),
                              filterable by group / category / sector / community
* :func:`ego_network`       — k-hop neighbourhood with bounded fan-out per node
* :func:`search_actors`     — trigram search over all actors, paginated
* :func:`community_summaries` / :func:`community_members`
* :func:`actor_metrics`     — one actor's metrics in the latest version
"""

from __future__ import annotations

import re
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.actors.provenance import stamp_actor_node
from intelligence.actors.universe import BULK_GROUPS, CATEGORY_GROUPS, GROUP_MARKET

OVERVIEW_DEFAULT_N = 2_000
OVERVIEW_MAX_N = 20_000
OVERVIEW_MAX_EDGES = 200_000
EGO_MAX_DEPTH = 3
EGO_MAX_FANOUT = 200
EGO_MAX_NODES = 2_000
SEARCH_COUNT_CAP = 10_000

# Edge colours by relationship (kept in sync with the router's legacy map).
RELATIONSHIP_COLORS: dict[str, str] = {
    "wealth_flow": "#22C55E",
    "insider_trade": "#F59E0B",
    "officer_of": "#3B82F6",
    "icij_officer_of": "#3B82F6",
    "intermediary_of": "#A855F7",
    "icij_intermediary_of": "#A855F7",
    "registered_address": "#64748B",
    "icij_registered_address": "#64748B",
    "co_traded_congress": "#EC4899",
    "signal_linked": "#06B6D4",
}

_ICIJ_DIGITS = re.compile(r"^(?:icij_)?(\d+)$")


def normalize_groups(groups: list[str] | str | None) -> list[str]:
    """Whitelisted category groups; ``None``/empty -> ``["market"]``.

    ``"all"`` selects every group. Unknown names are dropped.
    """
    if groups is None:
        return [GROUP_MARKET]
    if isinstance(groups, str):
        groups = [g for g in re.split(r"[,\s]+", groups) if g]
    out: list[str] = []
    for g in groups:
        g = g.strip().lower()
        if g == "all":
            return list(CATEGORY_GROUPS)
        if g in CATEGORY_GROUPS and g not in out:
            out.append(g)
    return out or [GROUP_MARKET]


def _split_csv(value: str | list[str] | None) -> list[str] | None:
    if value is None:
        return None
    items = value if isinstance(value, list) else re.split(r"[,]+", value)
    cleaned = [v.strip() for v in items if v and v.strip()]
    return cleaned or None


# ── Runs ───────────────────────────────────────────────────────────────────

def latest_run(engine: Engine) -> dict[str, Any] | None:
    """Newest ``complete`` row of ``actor_graph_runs`` (None if none/absent)."""
    with engine.connect() as conn:
        exists = conn.execute(text("SELECT to_regclass('actor_graph_runs')")).scalar()
        if not exists:
            return None
        row = conn.execute(text(
            "SELECT graph_version, computed_at, node_count, edge_count, actor_count, "
            "external_node_count, community_count, component_count, giant_component_size, "
            "runtime_s, peak_rss_mb, stats "
            "FROM actor_graph_runs WHERE status = 'complete' "
            "ORDER BY computed_at DESC LIMIT 1"
        )).fetchone()
    if row is None:
        return None
    stats = row[11] or {}
    return {
        "graph_version": row[0],
        "computed_at": row[1].isoformat() if row[1] is not None else None,
        "node_count": row[2],
        "edge_count": row[3],
        "actor_count": row[4],
        "external_node_count": row[5],
        "community_count": row[6],
        "component_count": row[7],
        "giant_component_size": row[8],
        "runtime_s": row[9],
        "peak_rss_mb": row[10],
        "sector_counts": stats.get("sector_counts", {}) if isinstance(stats, dict) else {},
        "group_counts": stats.get("group_counts", {}) if isinstance(stats, dict) else {},
    }


def _freshness(run: dict[str, Any] | None) -> dict[str, Any]:
    if not run:
        return {"graph_version": None, "computed_at": None}
    return {"graph_version": run["graph_version"], "computed_at": run["computed_at"]}


# ── Node details ───────────────────────────────────────────────────────────

def _actor_details(conn: Any, ids: list[str]) -> dict[str, dict[str, Any]]:
    if not ids:
        return {}
    rows = conn.execute(text(
        "SELECT id, name, tier, category, title, influence_score, trust_score, "
        "net_worth_estimate, aum, provenance, provenance_as_of "
        "FROM actors WHERE id = ANY(:ids)"
    ), {"ids": ids}).fetchall()
    return {
        r[0]: {
            "name": r[1], "tier": r[2], "category": r[3], "title": r[4] or "",
            "influence_score": float(r[5]) if r[5] is not None else 0.5,
            "trust_score": float(r[6]) if r[6] is not None else 0.5,
            "net_worth": float(r[7]) if r[7] is not None else None,
            "aum": float(r[8]) if r[8] is not None else None,
            "provenance": r[9], "provenance_as_of": r[10],
        }
        for r in rows
    }


def _node(actor_id: str, metric: dict[str, Any] | None, detail: dict[str, Any] | None) -> dict[str, Any]:
    """Legacy-compatible node dict (``label``/``influence``/``size`` kept)."""
    metric = metric or {}
    detail = detail or {}
    importance = float(metric.get("importance") or 0.0)
    node = {
        "id": actor_id,
        "label": detail.get("name") or actor_id,
        "name": detail.get("name") or actor_id,
        "tier": detail.get("tier"),
        "category": metric.get("category") or detail.get("category"),
        "category_group": metric.get("category_group"),
        "sector": metric.get("sector") or "Other",
        "title": detail.get("title", ""),
        "influence": round(importance, 4),
        "importance": round(importance, 4),
        "influence_score": detail.get("influence_score"),
        "trust_score": detail.get("trust_score"),
        "net_worth": detail.get("net_worth"),
        "aum": detail.get("aum"),
        "pagerank": metric.get("pagerank"),
        "degree": metric.get("degree"),
        "betweenness": metric.get("betweenness"),
        "coreness": metric.get("coreness"),
        "community_id": metric.get("community_id"),
        "size": max(4, int(importance * 30)),
    }
    stamp_actor_node(node, actor_id, stored=detail.get("provenance"), vintage=detail.get("provenance_as_of"))
    return node


_METRIC_SELECT = (
    "actor_id, category, category_group, sector, pagerank, importance, degree, "
    "betweenness, coreness, community_id"
)


def _metric_row(r: Any) -> dict[str, Any]:
    return {
        "actor_id": r[0], "category": r[1], "category_group": r[2], "sector": r[3],
        "pagerank": float(r[4]) if r[4] is not None else 0.0,
        "importance": float(r[5]) if r[5] is not None else 0.0,
        "degree": int(r[6]) if r[6] is not None else 0,
        "betweenness": float(r[7]) if r[7] is not None else 0.0,
        "coreness": int(r[8]) if r[8] is not None else 0,
        "community_id": r[9],
    }


# ── Overview ───────────────────────────────────────────────────────────────

def overview(
    engine: Engine,
    *,
    n: int = OVERVIEW_DEFAULT_N,
    groups: list[str] | str | None = None,
    categories: list[str] | str | None = None,
    sector: str | None = None,
    community: int | None = None,
    max_edges: int | None = None,
    run: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Top-``n`` actors by precomputed importance plus the edges among them.

    Draws from the overview pool (every market actor and the top
    ``POOL_PER_BULK_GROUP`` of each bulk group). Bulk groups (``icij``,
    ``pep``, ``sanctioned``, ``donor``, ``watchlist``) are opt-in via
    ``groups``. Returns ``status="pending_first_run"`` when no run exists.
    """
    n = max(1, min(int(n), OVERVIEW_MAX_N))
    max_edges = max(0, min(int(max_edges if max_edges is not None else 6 * n), OVERVIEW_MAX_EDGES))
    group_list = normalize_groups(groups)
    cats = _split_csv(categories)
    if run is None:
        run = latest_run(engine)
    if run is None:
        return {"status": "pending_first_run", "nodes": [], "links": [], "metadata": {
            **_freshness(None), "groups": group_list, "returned": 0}}
    v = run["graph_version"]
    with engine.connect() as conn:
        conn.execute(text("SET LOCAL statement_timeout = '15s'"))
        rows = conn.execute(text(
            "SELECT m.* FROM unnest(CAST(:groups AS text[])) AS g(grp) "
            "CROSS JOIN LATERAL ("
            f"  SELECT {_METRIC_SELECT} FROM actor_graph_metrics "
            "   WHERE graph_version = :v AND category_group = g.grp AND in_overview_pool "
            "     AND (CAST(:sector AS text) IS NULL OR sector = CAST(:sector AS text)) "
            "     AND (CAST(:community AS integer) IS NULL OR community_id = CAST(:community AS integer)) "
            "     AND (CAST(:cats AS text[]) IS NULL OR category = ANY(CAST(:cats AS text[]))) "
            "   ORDER BY importance DESC LIMIT :n"
            ") m ORDER BY m.importance DESC LIMIT :n"
        ), {"groups": group_list, "v": v, "sector": sector, "community": community,
            "cats": cats, "n": n}).fetchall()
        metrics = [_metric_row(r) for r in rows]
        ids = [m["actor_id"] for m in metrics]
        details = _actor_details(conn, ids)
        links: list[dict[str, Any]] = []
        if ids and max_edges:
            erows = conn.execute(text(
                "SELECT src, dst, weight, relationship FROM actor_graph_edges "
                "WHERE graph_version = :v AND src = ANY(:ids) AND dst = ANY(:ids) "
                "ORDER BY weight DESC LIMIT :lim"
            ), {"v": v, "ids": ids, "lim": max_edges}).fetchall()
            links = [{"source": e[0], "target": e[1], "strength": float(e[2]),
                      "relationship": e[3] or "connected"} for e in erows]
    nodes = [_node(m["actor_id"], m, details.get(m["actor_id"])) for m in metrics]
    return {
        "nodes": nodes,
        "links": links,
        "metadata": {
            **_freshness(run),
            "groups": group_list,
            "bulk_groups_available": list(BULK_GROUPS),
            "categories": cats,
            "sector_filter": sector,
            "community_filter": community,
            "requested": n,
            "returned": len(nodes),
            "links_returned": len(links),
            "links_capped": len(links) >= max_edges > 0,
            "total_actors": run.get("actor_count"),
            "total_edges": run.get("edge_count"),
            "pool_limited": True,
        },
        "sectors": [{"name": k, "count": int(c)} for k, c in
                    sorted((run.get("sector_counts") or {}).items(), key=lambda kv: -kv[1])],
        "groups": [{"name": k, "count": int(c)} for k, c in (run.get("group_counts") or {}).items()],
    }


# ── Ego network ────────────────────────────────────────────────────────────

_EGO_CONNECTIONS_SQL = text(
    "SELECT f.owner, e.other, e.strength, e.relationship FROM "
    "unnest(CAST(:keys AS text[]), CAST(:owners AS text[])) AS f(key, owner) "
    "CROSS JOIN LATERAL ("
    "  (SELECT actor_b AS other, strength, relationship FROM actor_connections "
    "    WHERE actor_a = f.key ORDER BY strength DESC NULLS LAST LIMIT :k) "
    "  UNION ALL "
    "  (SELECT actor_a AS other, strength, relationship FROM actor_connections "
    "    WHERE actor_b = f.key ORDER BY strength DESC NULLS LAST LIMIT :k)"
    ") e"
)
_EGO_ENTITY_REL_SQL = text(
    "SELECT f.owner, e.other, e.strength, e.relationship FROM "
    "unnest(CAST(:keys AS text[]), CAST(:owners AS text[])) AS f(key, owner) "
    "CROSS JOIN LATERAL ("
    "  (SELECT actor_b AS other, strength, relationship FROM entity_relationships "
    "    WHERE actor_a = f.key LIMIT :k) "
    "  UNION ALL "
    "  (SELECT actor_a AS other, strength, relationship FROM entity_relationships "
    "    WHERE actor_b = f.key LIMIT :k)"
    ") e"
)
_EGO_WEALTH_SQL = text(
    "SELECT f.owner, e.other, e.strength, 'wealth_flow' FROM "
    "unnest(CAST(:keys AS text[]), CAST(:owners AS text[])) AS f(key, owner) "
    "CROSS JOIN LATERAL ("
    "  (SELECT to_entity AS other, "
    "          CASE WHEN confidence = 'confirmed' THEN 0.6 ELSE 0.4 END AS strength, "
    "          amount_estimate FROM wealth_flows "
    "    WHERE from_actor = f.key ORDER BY amount_estimate DESC NULLS LAST LIMIT :k) "
    "  UNION ALL "
    "  (SELECT from_actor AS other, "
    "          CASE WHEN confidence = 'confirmed' THEN 0.6 ELSE 0.4 END AS strength, "
    "          amount_estimate FROM wealth_flows "
    "    WHERE to_entity = f.key ORDER BY amount_estimate DESC NULLS LAST LIMIT :k)"
    ") e"
)


def _actor_columns(conn: Any) -> set[str]:
    """Columns of ``actors`` visible on this connection (older schemas lack
    ``icij_node_id`` / ``merged_into``)."""
    rows = conn.execute(text(
        "SELECT a.attname FROM pg_attribute a "
        "WHERE a.attrelid = to_regclass('actors') AND a.attnum > 0 AND NOT a.attisdropped"
    )).fetchall()
    return {r[0] for r in rows}


def _resolve_ids(conn: Any, raw_ids: list[str], columns: set[str]) -> dict[str, str | None]:
    """Raw edge endpoint -> canonical actor id (None = not an actor)."""
    if not raw_ids:
        return {}
    out: dict[str, str | None] = {r: None for r in raw_ids}
    alias = dict(conn.execute(text(
        "SELECT alias_id, canonical_id FROM actor_id_map "
        "WHERE alias_id = ANY(:ids) AND alias_id <> canonical_id"
    ), {"ids": raw_ids}).fetchall()) if _has_table(conn, "actor_id_map") else {}
    probe = list({alias.get(r, r) for r in raw_ids})
    merged_col = "merged_into" if "merged_into" in columns else "NULL"
    rows = conn.execute(text(
        f"SELECT id, {merged_col} FROM actors WHERE id = ANY(:ids)"
    ), {"ids": probe}).fetchall()
    found = {r[0]: (r[1] or r[0]) for r in rows}
    digits: dict[str, str] = {}
    for r in raw_ids:
        cand = alias.get(r, r)
        if cand in found:
            out[r] = found[cand]
            continue
        m = _ICIJ_DIGITS.match(cand)
        if m:
            digits[r] = m.group(1)
    if digits and "icij_node_id" in columns:
        rows = conn.execute(text(
            "SELECT icij_node_id, id FROM actors WHERE icij_node_id = ANY(:d)"
        ), {"d": list(set(digits.values()))}).fetchall()
        by_node = {r[0]: r[1] for r in rows}
        for raw, d in digits.items():
            out[raw] = by_node.get(d)
    return out


def _has_table(conn: Any, name: str) -> bool:
    try:
        return bool(conn.execute(text("SELECT to_regclass(:n)"), {"n": name}).scalar())
    except Exception:  # noqa: BLE001
        return False


def _lookup_keys(conn: Any, actor_ids: list[str], columns: set[str]) -> tuple[list[str], list[str]]:
    """All edge-table spellings of each actor id: id, ICIJ node id, icij_<n>."""
    keys: list[str] = []
    owners: list[str] = []
    for a in actor_ids:
        keys.append(a)
        owners.append(a)
    if "icij_node_id" not in columns:
        return keys, owners
    rows = conn.execute(text(
        "SELECT id, icij_node_id FROM actors WHERE id = ANY(:ids) AND icij_node_id IS NOT NULL"
    ), {"ids": actor_ids}).fetchall()
    for aid, node in rows:
        for k in (str(node), f"icij_{node}"):
            keys.append(k)
            owners.append(aid)
    return keys, owners


def ego_network(
    engine: Engine,
    actor_id: str,
    *,
    depth: int = 1,
    fanout: int = 25,
    max_nodes: int = 300,
    include_offshore: bool = True,
    include_external: bool = True,
    min_strength: float = 0.0,
) -> dict[str, Any]:
    """K-hop neighbourhood of one actor with bounded fan-out per node.

    Each frontier node contributes at most ``fanout`` neighbours, chosen by
    edge weight then neighbour importance, read through the per-node
    ``(actor_a|actor_b, strength DESC)`` indexes. ``include_offshore`` adds
    ``entity_relationships`` and ``icij_*`` relationships; ``include_external``
    keeps non-actor endpoints (ICIJ addresses, tickers) as leaf nodes.
    """
    depth = max(1, min(int(depth), EGO_MAX_DEPTH))
    fanout = max(1, min(int(fanout), EGO_MAX_FANOUT))
    max_nodes = max(2, min(int(max_nodes), EGO_MAX_NODES))
    per_query = fanout * 2

    try:
        run = latest_run(engine)
    except Exception as exc:  # noqa: BLE001
        log.debug("ego: latest_run failed: {e}", e=str(exc))
        run = None
    v = run["graph_version"] if run else None

    with engine.connect() as conn:
        conn.execute(text("SET LOCAL statement_timeout = '15s'"))
        center = _actor_details(conn, [actor_id])
        if actor_id not in center:
            return {"nodes": [], "edges": [], "center": actor_id, "depth": depth,
                    "error": f"Actor '{actor_id}' not found", **_freshness(run)}
        has_er = include_offshore and _has_table(conn, "entity_relationships")
        columns = _actor_columns(conn)
        has_wf = _has_table(conn, "wealth_flows")

        metrics: dict[str, dict[str, Any]] = {}
        details: dict[str, dict[str, Any]] = dict(center)
        ring: dict[str, int] = {actor_id: 0}
        external: dict[str, int] = {}
        edges: dict[tuple[str, str, str], dict[str, Any]] = {}
        truncated = False

        def _load_metrics(ids: list[str]) -> None:
            want = [i for i in ids if i not in metrics]
            if not v or not want:
                return
            for r in conn.execute(text(
                f"SELECT {_METRIC_SELECT} FROM actor_graph_metrics "
                "WHERE graph_version = :v AND actor_id = ANY(:ids)"
            ), {"v": v, "ids": want}).fetchall():
                metrics[r[0]] = _metric_row(r)

        _load_metrics([actor_id])
        frontier = [actor_id]
        for hop in range(1, depth + 1):
            if not frontier or truncated:
                break
            keys, owners = _lookup_keys(conn, frontier, columns)
            params = {"keys": keys, "owners": owners, "k": per_query}
            cand: list[tuple[str, str, float, str]] = []
            for r in conn.execute(_EGO_CONNECTIONS_SQL, params).fetchall():
                rel = r[3] or "connected"
                if not include_offshore and rel.startswith("icij_"):
                    continue
                cand.append((r[0], r[1], float(r[2]) if r[2] is not None else 0.5, rel))
            if has_er:
                cand.extend((r[0], r[1], float(r[2]) if r[2] is not None else 0.5, r[3] or "connected")
                            for r in conn.execute(_EGO_ENTITY_REL_SQL, params).fetchall())
            if has_wf:
                cand.extend((r[0], r[1], float(r[2]) if r[2] is not None else 0.5, "wealth_flow")
                            for r in conn.execute(_EGO_WEALTH_SQL, params).fetchall())
            cand = [c for c in cand if c[2] >= min_strength and c[1]]
            resolved = _resolve_ids(conn, list({c[1] for c in cand}), columns)
            _load_metrics([x for x in resolved.values() if x])

            # Best ``fanout`` neighbours per owner: weight, then importance.
            per_owner: dict[str, list[tuple[float, float, str, str, bool]]] = {}
            for owner, raw, w, rel in cand:
                other = resolved.get(raw)
                is_ext = other is None
                if is_ext and not include_external:
                    continue
                nid = other or raw
                if nid == owner:
                    continue
                imp = metrics.get(nid, {}).get("importance", 0.0) if not is_ext else 0.0
                per_owner.setdefault(owner, []).append((w, imp, nid, rel, is_ext))
            next_frontier: list[str] = []
            for owner, lst in per_owner.items():
                lst.sort(key=lambda t: (-t[0], -t[1], t[2]))
                seen_here: set[str] = set()
                for w, _imp, nid, rel, is_ext in lst:
                    if nid in seen_here:
                        continue
                    if len(seen_here) >= fanout:
                        break
                    known = nid in ring or nid in external
                    if not known:
                        if len(ring) + len(external) >= max_nodes:
                            truncated = True
                            continue
                        if is_ext:
                            external[nid] = hop
                        else:
                            ring[nid] = hop
                            next_frontier.append(nid)
                    seen_here.add(nid)
                    a, b = (owner, nid) if owner <= nid else (nid, owner)
                    key = (a, b, rel)
                    if key not in edges or edges[key]["strength"] < w:
                        edges[key] = {"source": owner, "target": nid, "relationship": rel,
                                      "strength": round(w, 4),
                                      "color": RELATIONSHIP_COLORS.get(rel, "#64748B")}
            new_ids = [i for i in next_frontier if i not in details]
            details.update(_actor_details(conn, new_ids))
            frontier = next_frontier

    nodes: list[dict[str, Any]] = []
    for aid, r in ring.items():
        node = _node(aid, metrics.get(aid), details.get(aid))
        node["ring"] = r
        node["trust"] = node.get("trust_score")
        nodes.append(node)
    for xid, r in external.items():
        nodes.append({"id": xid, "label": xid, "name": xid, "category": "external",
                      "sector": "Other", "ring": r, "external": True, "influence": 0.0,
                      "importance": 0.0, "size": 3})
    return {
        "nodes": nodes,
        "edges": list(edges.values()),
        "center": actor_id,
        "depth": depth,
        "fanout": fanout,
        "truncated": truncated,
        "relationship_colors": RELATIONSHIP_COLORS,
        **_freshness(run),
    }


# ── Search ─────────────────────────────────────────────────────────────────

def _like_escape(q: str) -> str:
    return q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def search_actors(
    engine: Engine,
    q: str,
    *,
    limit: int = 50,
    offset: int = 0,
    groups: list[str] | str | None = None,
) -> dict[str, Any]:
    """Substring search over every actor's name (pg_trgm GIN), paginated.

    Exact name matches first, then precomputed importance. ``groups`` (when
    given) restricts to those category groups via the latest metrics.
    ``total`` is exact up to ``SEARCH_COUNT_CAP`` (``total_is_lower_bound``).
    """
    q = (q or "").strip()
    limit = max(1, min(int(limit), 200))
    offset = max(0, int(offset))
    try:
        run = latest_run(engine)
    except Exception:  # noqa: BLE001
        run = None
    v = run["graph_version"] if run else None
    group_list = normalize_groups(groups) if groups else None
    if group_list is not None and not v:
        group_list = None
    # Under 3 characters a trigram index cannot help (an ILIKE would scan
    # every actor): match the whole name exactly instead (lower(name) index).
    exact_only = len(q) < 3
    pat = q if exact_only else f"%{_like_escape(q)}%"
    match = "lower(a.name) = lower(:pat)" if exact_only else "a.name ILIKE :pat"
    with engine.connect() as conn:
        conn.execute(text("SET LOCAL statement_timeout = '10s'"))
        group_clause = (
            " AND m.category_group = ANY(CAST(:groups AS text[]))" if group_list else ""
        )
        join = "JOIN" if group_list else "LEFT JOIN"
        total = conn.execute(text(
            "SELECT count(*) FROM (SELECT 1 FROM actors a "
            f"{join} actor_graph_metrics m ON m.graph_version = :v AND m.actor_id = a.id "
            f"WHERE {match}{group_clause} LIMIT :cap) s"
        ) if v else text(
            f"SELECT count(*) FROM (SELECT 1 FROM actors a WHERE {match} LIMIT :cap) s"
        ), {"pat": pat, "v": v, "groups": group_list, "cap": SEARCH_COUNT_CAP}).scalar() or 0
        if v:
            rows = conn.execute(text(
                "SELECT a.id, a.name, a.category, a.tier, a.title, a.influence_score, "
                "a.provenance, a.provenance_as_of, m.importance, m.sector, m.community_id, "
                "m.category_group, m.degree "
                f"FROM actors a {join} actor_graph_metrics m "
                "  ON m.graph_version = :v AND m.actor_id = a.id "
                f"WHERE {match}{group_clause} "
                "ORDER BY (lower(a.name) = lower(:q)) DESC, COALESCE(m.importance, 0) DESC, a.id "
                "LIMIT :lim OFFSET :off"
            ), {"pat": pat, "q": q, "v": v, "groups": group_list, "lim": limit, "off": offset}).fetchall()
        else:
            rows = conn.execute(text(
                "SELECT a.id, a.name, a.category, a.tier, a.title, a.influence_score, "
                "a.provenance, a.provenance_as_of, NULL, NULL, NULL, NULL, NULL "
                f"FROM actors a WHERE {match} "
                "ORDER BY (lower(a.name) = lower(:q)) DESC, a.influence_score DESC NULLS LAST, a.id "
                "LIMIT :lim OFFSET :off"
            ), {"pat": pat, "q": q, "lim": limit, "off": offset}).fetchall()
    entries = [
        stamp_actor_node({
            "id": r[0], "name": r[1], "category": r[2], "tier": r[3], "title": r[4] or "",
            "influence": float(r[5]) if r[5] is not None else 0.5,
            "importance": float(r[8]) if r[8] is not None else None,
            "sector": r[9], "community_id": r[10], "category_group": r[11],
            "degree": r[12],
        }, r[0], stored=r[6], vintage=r[7])
        for r in rows
    ]
    total = int(total)
    return {
        "entries": entries,
        "total": total,
        "total_is_lower_bound": total >= SEARCH_COUNT_CAP,
        "limit": limit,
        "offset": offset,
        "has_more": offset + len(entries) < total,
        **_freshness(run),
    }


# ── Top actors ─────────────────────────────────────────────────────────────

# Legacy ``/analytics/top`` metric names -> actor_graph_metrics columns. The
# universe job does not compute eigenvector or HITS scores; those stay on the
# legacy ``actor_analytics`` path.
TOP_METRIC_COLUMNS: dict[str, str] = {
    "pagerank": "pagerank",
    "betweenness": "betweenness",
    "degree_centrality": "degree",
    "importance": "importance",
    "coreness": "coreness",
}


def top_actors(
    engine: Engine,
    *,
    metric: str = "pagerank",
    limit: int = 20,
    groups: list[str] | str | None = None,
    run: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Top actors of the latest version by ``metric``, ``market`` group by default.

    Bulk groups are opt-in via ``groups`` as in :func:`overview`, so the
    leak/screening lists do not crowd out named market actors.
    """
    column = TOP_METRIC_COLUMNS.get(metric)
    if column is None:
        raise ValueError(f"Invalid metric '{metric}'. Must be one of: {sorted(TOP_METRIC_COLUMNS)}")
    limit = max(1, min(int(limit), 1000))
    group_list = normalize_groups(groups)
    if run is None:
        run = latest_run(engine)
    if run is None:
        return {"status": "pending_first_run", "entries": [], "groups": group_list, **_freshness(None)}
    with engine.connect() as conn:
        conn.execute(text("SET LOCAL statement_timeout = '15s'"))
        # column comes from the TOP_METRIC_COLUMNS whitelist, so it is safe as an identifier
        rows = conn.execute(text(
            f"SELECT actor_id, category, category_group, {column}, community_id, pagerank "
            "FROM actor_graph_metrics "
            "WHERE graph_version = :v AND category_group = ANY(CAST(:groups AS text[])) "
            f"ORDER BY {column} DESC NULLS LAST, actor_id LIMIT :lim"
        ), {"v": run["graph_version"], "groups": group_list, "lim": limit}).fetchall()
        details = _actor_details(conn, [r[0] for r in rows])
    entries = [
        {
            "actor_id": r[0],
            "name": (details.get(r[0]) or {}).get("name", r[0]),
            "category": r[1],
            "category_group": r[2],
            "score": float(r[3] or 0.0),
            "community_id": r[4],
            "pagerank": float(r[5] or 0.0),
        }
        for r in rows
    ]
    return {"entries": entries, "groups": group_list, **_freshness(run)}


# ── Communities ────────────────────────────────────────────────────────────

def community_summaries(
    engine: Engine,
    *,
    limit: int = 100,
    offset: int = 0,
    min_size: int = 3,
    sector: str | None = None,
    run: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Precomputed community summaries, largest first, paginated."""
    limit = max(1, min(int(limit), 1000))
    offset = max(0, int(offset))
    if run is None:
        run = latest_run(engine)
    if run is None:
        return {"status": "pending_first_run", "entries": [], "total": 0, "limit": limit,
                "offset": offset, "has_more": False, **_freshness(None)}
    v = run["graph_version"]
    params = {"v": v, "min_size": int(min_size), "sector": sector, "lim": limit, "off": offset}
    where = ("WHERE graph_version = :v AND size >= :min_size "
             "AND (CAST(:sector AS text) IS NULL OR dominant_sector = CAST(:sector AS text))")
    with engine.connect() as conn:
        total = conn.execute(text(f"SELECT count(*) FROM actor_graph_communities {where}"),
                             params).scalar() or 0
        rows = conn.execute(text(
            "SELECT community_id, size, actor_count, internal_edges, top_actor_ids, top_sectors, "
            "top_categories, dominant_sector, dominant_category, max_importance "
            f"FROM actor_graph_communities {where} "
            "ORDER BY size DESC, community_id LIMIT :lim OFFSET :off"
        ), params).fetchall()
        top_ids = sorted({a for r in rows for a in (r[4] or [])})
        names = {k: d["name"] for k, d in _actor_details(conn, top_ids).items()}
    entries = []
    for r in rows:
        top = [{"id": a, "name": names.get(a, a)} for a in (r[4] or [])]
        entries.append({
            "community_id": r[0], "size": r[1], "actor_count": r[2], "internal_edges": r[3],
            "top_actors": top, "top_sectors": r[5] or [], "top_categories": r[6] or [],
            "dominant_sector": r[7], "dominant_category": r[8],
            "max_importance": float(r[9] or 0.0),
            "label": " · ".join(x for x in (r[7], top[0]["name"] if top else None) if x),
            # legacy keys used by /analytics/communities consumers
            "member_count": r[2], "top_member": top[0]["name"] if top else None,
            "top_category": r[8], "max_pagerank": float(r[9] or 0.0),
        })
    total = int(total)
    return {"entries": entries, "total": total, "limit": limit, "offset": offset,
            "has_more": offset + len(entries) < total, **_freshness(run)}


def community_members(
    engine: Engine, community_id: int, *, limit: int = 100, offset: int = 0
) -> dict[str, Any]:
    """Members of one community in the latest version, by importance."""
    limit = max(1, min(int(limit), 1000))
    offset = max(0, int(offset))
    run = latest_run(engine)
    if run is None:
        return {"status": "pending_first_run", "entries": [], "total": 0, "limit": limit,
                "offset": offset, "has_more": False, **_freshness(None)}
    v = run["graph_version"]
    with engine.connect() as conn:
        total = conn.execute(text(
            "SELECT count(*) FROM actor_graph_metrics WHERE graph_version = :v AND community_id = :c"
        ), {"v": v, "c": community_id}).scalar() or 0
        rows = conn.execute(text(
            f"SELECT {_METRIC_SELECT} FROM actor_graph_metrics "
            "WHERE graph_version = :v AND community_id = :c "
            "ORDER BY importance DESC, actor_id LIMIT :lim OFFSET :off"
        ), {"v": v, "c": community_id, "lim": limit, "off": offset}).fetchall()
        metrics = [_metric_row(r) for r in rows]
        details = _actor_details(conn, [m["actor_id"] for m in metrics])
    entries = [_node(m["actor_id"], m, details.get(m["actor_id"])) for m in metrics]
    total = int(total)
    return {"community_id": community_id, "entries": entries, "total": total, "limit": limit,
            "offset": offset, "has_more": offset + len(entries) < total, **_freshness(run)}


def actor_metrics(engine: Engine, actor_id: str) -> dict[str, Any] | None:
    """One actor's metrics in the latest complete version (None if absent)."""
    run = latest_run(engine)
    if run is None:
        return None
    with engine.connect() as conn:
        r = conn.execute(text(
            f"SELECT {_METRIC_SELECT}, weighted_degree, component_id, group_rank, sector_basis "
            "FROM actor_graph_metrics WHERE graph_version = :v AND actor_id = :a"
        ), {"v": run["graph_version"], "a": actor_id}).fetchone()
    if r is None:
        return None
    out = _metric_row(r)
    out.update({"weighted_degree": float(r[10] or 0.0), "component_id": r[11],
                "group_rank": r[12], "sector_basis": r[13], **_freshness(run)})
    return out
