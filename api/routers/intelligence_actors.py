"""Intelligence sub-router: Actor network, post-mortems, and trend endpoints."""

from __future__ import annotations

import threading
import time
from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine
from api.lf_helpers import (
    observe as _lf_observe,
    propagate_attributes as _lf_propagate_attributes,
    set_input as _lf_set_input,
    user_id_from_token as _lf_user_id_from_token,
)
from intelligence.actors.provenance import SOURCE_SECTOR_MAP, stamp_actor_node
from utils.ttl_cache import TTLCache
from utils.single_flight import single_flight

router = APIRouter(tags=["intelligence"])


# ── Actor Network Endpoints ──────────────────────────────────────────────

_ACTOR_GRAPH_TTL = 1800  # 30 minutes
_actor_graph_cache: TTLCache = TTLCache(ttl=_ACTOR_GRAPH_TTL, max_size=1)
_ACTOR_GRAPH_CACHE_KEY = "full"

# Stale-while-revalidate + single-flight for the full actor graph.
#
# 2026-11-06: a TTL miss rebuilt the graph synchronously inside the request
# (then on the event loop, ``async def``), freezing grid-api for minutes
# ("DB connection held 114.0s — acquired_by=MainThread"), and every request
# that arrived during the build started its own. Now:
#   * fresh (TTL) hit  -> served from RAM;
#   * expired, but a previous graph exists -> served stale while ONE
#     background thread rebuilds;
#   * cold (nothing built yet) -> a fast ``status="building"`` payload while
#     the background build runs (the startup hook warms it, so this is only
#     the first seconds after a restart).
_actor_graph_build_lock = threading.Lock()  # held for the whole build
_actor_graph_state_lock = threading.Lock()
_actor_graph_last: dict[str, Any] | None = None
_actor_graph_last_built_at: float | None = None  # time.time() of last success
_ACTOR_GRAPH_RETRY_SECONDS = 10


_DEFAULT_SECTORS = [
    "AI", "Crypto", "Robotics", "Gene Therapy & Pharma", "Nuclear",
    "Energy", "Metals", "Semiconductors", "Tech",
]


@router.get("/actor-network")
def get_actor_network(
    limit: int = Query(2000, ge=10, le=50000, description="Max nodes for browser display (capped at 20,000)"),
    sector: str | None = Query(None, description="Filter to a single sector"),
    groups: str | None = Query(
        None,
        description="Comma-separated category groups: market (default), icij, pep, sanctioned, donor, watchlist, or all",
    ),
    community: int | None = Query(None, ge=0, description="Filter to one community id"),
    categories: str | None = Query(None, description="Comma-separated actor categories"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the actor network graph for D3 visualization.

    Served from the offline full-graph analytics (``actor_graph_*`` tables,
    ``intelligence/actors/universe.py``): the top ``limit`` actors by
    precomputed importance plus the edges among them, filterable by sector,
    community, category and category group. Bulk lists (ICIJ, PEPs,
    sanctions, donors, watchlists) are opt-in ``groups``. Until the first
    run completes, the legacy curated background build is served instead.
    Money-flow side panels come from their own background cache.
    """
    engine = get_db_engine()
    run = latest_universe_run(engine)
    if run is not None:
        from intelligence.actors.universe_store import overview

        try:
            payload = overview(
                engine, n=min(limit, 20000), groups=groups, categories=categories,
                sector=sector, community=community, run=run,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("Actor universe overview failed, serving legacy graph: {e}", e=str(exc))
        else:
            extras = get_flow_extras(wait=False) or {}
            payload["metadata"]["source"] = "actor_universe"
            payload["metadata"]["total_actors"] = run.get("actor_count")
            for key in ("wealth_flows", "pocket_lining_alerts", "flows", "circular_flows"):
                payload[key] = extras.get(key, [])
            payload["flow_summary"] = extras.get(
                "flow_summary", {"total_tracked": "$0", "top_flow": None, "active_loops": 0})
            return payload

    # Legacy (no completed universe run yet): never builds inline; fresh or
    # stale graph from RAM, else "building".
    full = get_full_actor_graph(wait=False)
    if full is None:
        return _actor_graph_building_payload()

    nodes = full.get("nodes", [])

    # Sector filtering
    if sector:
        nodes = [n for n in nodes if n.get("sector") == sector]
    else:
        # Default: show priority sectors + high-influence actors from other sectors
        priority = set(_DEFAULT_SECTORS)
        nodes = [
            n for n in nodes
            if n.get("sector") in priority or n.get("influence", 0) >= 0.7
        ]

    # Limit by influence
    if len(nodes) > limit:
        nodes = sorted(nodes, key=lambda n: n.get("influence", 0), reverse=True)[:limit]

    kept_ids = {n["id"] for n in nodes}
    links = [l for l in full.get("links", [])
             if l.get("source") in kept_ids and l.get("target") in kept_ids]

    # Sector summary for the frontend to build filter buttons
    all_nodes = full.get("nodes", [])
    sector_counts: dict[str, int] = {}
    for n in all_nodes:
        s = n.get("sector", "Other")
        sector_counts[s] = sector_counts.get(s, 0) + 1
    sectors_list = sorted(sector_counts.items(), key=lambda x: -x[1])

    return {
        "nodes": nodes,
        "links": links,
        "metadata": {
            **full.get("metadata", {}),
            "source": "legacy_curated",
            "computed_at": full.get("metadata", {}).get("generated_at"),
            "total_actors": len(all_nodes),
            "returned": len(nodes),
            "sector_filter": sector,
        },
        "sectors": [{"name": s, "count": c} for s, c in sectors_list],
        "wealth_flows": full.get("wealth_flows", []),
        "pocket_lining_alerts": full.get("pocket_lining_alerts", []),
        "flows": full.get("flows", []),
        "circular_flows": full.get("circular_flows", []),
        "flow_summary": full.get("flow_summary", {}),
    }


# ── Precomputed actor universe: run lookup + money-flow side-panel cache ──

_UNIVERSE_RUN_TTL = 60
_universe_run_cache: TTLCache = TTLCache(ttl=_UNIVERSE_RUN_TTL, max_size=1)
_UNIVERSE_RUN_KEY = "latest"
_NO_RUN = {"graph_version": None}


def latest_universe_run(engine: Any) -> dict[str, Any] | None:
    """Newest complete ``actor_graph_runs`` row, cached for a minute.

    Any failure (table absent, no DB in tests) reads as "no run" so callers
    fall back to the legacy path. Failures are not cached.
    """
    cached = _universe_run_cache.get(_UNIVERSE_RUN_KEY)
    if cached is not None:
        return None if cached is _NO_RUN else cached
    try:
        from intelligence.actors.universe_store import latest_run

        run = latest_run(engine)
    except Exception as exc:  # noqa: BLE001
        log.debug("actor universe run lookup failed: {e}", e=str(exc))
        return None
    _universe_run_cache.set(_UNIVERSE_RUN_KEY, run if run is not None else _NO_RUN)
    return run


_FLOW_EXTRAS_TTL = 1800
_flow_extras_cache: TTLCache = TTLCache(ttl=_FLOW_EXTRAS_TTL, max_size=1)
_flow_extras_build_lock = threading.Lock()
_flow_extras_state_lock = threading.Lock()
_flow_extras_last: dict[str, Any] | None = None


def _flow_extras_worker() -> None:
    global _flow_extras_last
    try:
        extras = _build_flow_extras(get_db_engine())
        _flow_extras_cache.set("extras", extras)
        with _flow_extras_state_lock:
            _flow_extras_last = extras
    except Exception as exc:  # noqa: BLE001
        log.warning("Actor flow extras background build failed: {e}", e=str(exc))
    finally:
        _flow_extras_build_lock.release()


def get_flow_extras(*, wait: bool = False) -> dict[str, Any] | None:
    """Money-flow side panels (wealth flows, pocket lining, influence flows).

    Stale-while-revalidate + single-flight like the legacy graph: fresh ->
    served; stale -> served while one background rebuild runs; cold ->
    ``None`` (and a background build). ``wait`` is accepted for symmetry.
    """
    del wait
    fresh = _flow_extras_cache.get("extras")
    if fresh is not None:
        return fresh
    with _flow_extras_state_lock:
        stale = _flow_extras_last
    if _flow_extras_build_lock.acquire(blocking=False):
        try:
            threading.Thread(target=_flow_extras_worker, name="actor-flow-extras", daemon=True).start()
        except Exception:
            _flow_extras_build_lock.release()
            raise
    return stale


def _empty_actor_network_payload() -> dict[str, Any]:
    return {
        "nodes": [], "links": [], "metadata": {},
        "wealth_flows": [], "pocket_lining_alerts": [],
        "flows": [], "circular_flows": [],
        "flow_summary": {"total_tracked": "$0", "top_flow": None, "active_loops": 0},
        "sectors": [],
    }


def _actor_graph_building_payload() -> dict[str, Any]:
    """Cold-start answer: the graph is being built in the background."""
    return {
        **_empty_actor_network_payload(),
        "status": "building",
        "building": True,
        "retry_after_seconds": _ACTOR_GRAPH_RETRY_SECONDS,
        "message": "Actor network is being built; retry shortly.",
    }


def _actor_graph_rebuild_worker() -> None:
    """Background rebuild; the caller already holds the build lock."""
    try:
        _build_full_actor_cache()
    except Exception as exc:
        log.warning("Actor network background build failed: {e}", e=str(exc))
    finally:
        _actor_graph_build_lock.release()


def start_actor_graph_rebuild() -> bool:
    """Start one background rebuild unless one is already running.

    Returns True when this call started the rebuild.
    """
    if not _actor_graph_build_lock.acquire(blocking=False):
        return False
    try:
        threading.Thread(
            target=_actor_graph_rebuild_worker,
            name="actor-graph-builder",
            daemon=True,
        ).start()
    except Exception:
        _actor_graph_build_lock.release()
        raise
    return True


def get_full_actor_graph(*, wait: bool) -> dict[str, Any] | None:
    """Return the full actor graph without ever stacking duplicate builds.

    Fresh cache hit -> that. Expired with a previous graph -> the previous
    graph, and one background rebuild is started. Nothing built yet ->
    ``None`` (and a background build) when ``wait`` is False; with ``wait``
    True the caller blocks on the single in-flight build (never a second
    one). Must not be called on the event loop with ``wait=True``.
    """
    fresh = _actor_graph_cache.get(_ACTOR_GRAPH_CACHE_KEY)
    if fresh is not None:
        return fresh
    with _actor_graph_state_lock:
        stale = _actor_graph_last
    if stale is not None or not wait:
        start_actor_graph_rebuild()
        return stale
    with _actor_graph_build_lock:
        fresh = _actor_graph_cache.get(_ACTOR_GRAPH_CACHE_KEY)
        if fresh is not None:
            return fresh
        try:
            return _build_full_actor_cache()
        except Exception as exc:
            log.warning("Actor network build failed: {e}", e=str(exc))
            return None


def clear_actor_graph() -> None:
    """Drop the fresh and the stale graph (tests / operator reset)."""
    global _actor_graph_last, _actor_graph_last_built_at
    _actor_graph_cache.clear()
    with _actor_graph_state_lock:
        _actor_graph_last = None
        _actor_graph_last_built_at = None


def warm_actor_graph_async() -> bool:
    """Startup hook: build the graph in a background thread."""
    return start_actor_graph_rebuild()


def _build_full_actor_cache(now=None) -> dict[str, Any]:
    """Build the full actor graph and store in RAM. Called at startup and on cache miss.

    Returns the freshly built graph payload. The ``now`` parameter is
    accepted for backwards compatibility but is no longer used — TTLCache
    manages expiry internally.
    """
    del now  # unused; TTLCache handles expiry

    from intelligence.actor_network import build_actor_graph

    engine = get_db_engine()
    graph = build_actor_graph(engine)
    log.info("Actor network loaded: {n} nodes, {l} links",
             n=len(graph.get("nodes", [])), l=len(graph.get("links", [])))

    result = {**graph, **_build_flow_extras(engine)}
    _actor_graph_cache.set(_ACTOR_GRAPH_CACHE_KEY, result)
    global _actor_graph_last, _actor_graph_last_built_at
    with _actor_graph_state_lock:
        _actor_graph_last = result
        _actor_graph_last_built_at = time.time()
    log.info("Actor network cached: {n} nodes, {l} links",
             n=len(result.get("nodes", [])), l=len(result.get("links", [])))
    return result


def _build_flow_extras(engine: Any) -> dict[str, Any]:
    """Wealth flows, pocket-lining alerts and influence/dollar money flows."""
    from intelligence.actor_network import assess_pocket_lining, track_wealth_migration

    # Wealth flows
    wealth_flows: list[dict] = []
    try:
        flows_raw = track_wealth_migration(engine, days=90)
        wealth_flows = [
            {
                "from_actor": f.from_actor,
                "to_actor": f.to_actor,
                "amount": f.amount_estimate,
                "confidence": f.confidence,
                "evidence": f.evidence,
                "timestamp": f.timestamp,
                "implication": f.implication,
            }
            for f in flows_raw[:200]
        ]
    except Exception as exc:
        log.debug("Wealth flow aggregation failed: {e}", e=str(exc))

    # Pocket-lining alerts
    pocket_lining_alerts: list[dict] = []
    try:
        pocket_lining_alerts = assess_pocket_lining(engine)
    except Exception as exc:
        log.debug("Pocket-lining detection failed: {e}", e=str(exc))

    # ── Money flows from influence_network + dollar_flows ──
    money_flows: list[dict] = []
    circular_flows_data: list[dict] = []
    flow_summary: dict[str, Any] = {
        "total_tracked": "$0",
        "top_flow": None,
        "active_loops": 0,
    }
    try:
        from intelligence.influence_network import (
            build_influence_graph,
            detect_circular_flows,
        )

        influence_graph = build_influence_graph(engine)
        for link in influence_graph.get("links", []):
            flow_type = link.get("type", "")
            amount_raw = link.get("amount", 0)
            try:
                amount_val = float(amount_raw) if amount_raw else 0.0
            except (TypeError, ValueError):
                amount_val = 0.0
            if amount_val <= 0 and flow_type not in ("trade",):
                continue

            ftype_map = {"contribution": "campaign", "lobbying": "lobbying",
                         "contract": "contract", "trade": "stock_trade"}
            ftype = ftype_map.get(flow_type, flow_type or "unknown")

            money_flows.append({
                "from": link.get("source", ""),
                "to": link.get("target", ""),
                "amount": amount_val,
                "type": ftype,
                "date": link.get("date", ""),
                "label": link.get("label", ""),
            })

        try:
            loops = detect_circular_flows(engine)
            for loop in loops:
                circular_flows_data.append(loop.to_dict())
        except Exception as exc:
            log.debug("Circular flow detection failed: {e}", e=str(exc))

        try:
            from intelligence.dollar_flows import get_biggest_movers
            biggest = get_biggest_movers(engine, days=90)
            for bf in biggest:
                money_flows.append({
                    "from": bf.get("actor_name", "unknown"),
                    "to": bf.get("ticker", "market"),
                    "amount": bf.get("amount_usd", 0),
                    "type": bf.get("source_type", "unknown"),
                    "date": bf.get("flow_date", ""),
                    "label": (
                        f"${bf.get('amount_usd', 0):,.0f} "
                        f"{bf.get('direction', '')} "
                        f"({bf.get('source_type', '')})"
                    ),
                })
        except Exception as exc:
            log.debug("Dollar flow enrichment failed: {e}", e=str(exc))

        total_tracked = sum(
            abs(f.get("amount", 0)) for f in money_flows
            if isinstance(f.get("amount"), (int, float))
        )
        top_flow = max(
            money_flows,
            key=lambda f: abs(f.get("amount", 0)) if isinstance(f.get("amount"), (int, float)) else 0,
            default=None,
        )
        active_loops = sum(1 for c in circular_flows_data if c.get("circular_flow_detected"))

        def _fmt_total(val: float) -> str:
            if val >= 1e12:
                return f"${val / 1e12:.1f}T"
            if val >= 1e9:
                return f"${val / 1e9:.1f}B"
            if val >= 1e6:
                return f"${val / 1e6:.0f}M"
            return f"${val:,.0f}"

        flow_summary = {
            "total_tracked": _fmt_total(total_tracked),
            "top_flow": top_flow,
            "active_loops": active_loops,
        }
    except Exception as exc:
        log.debug("Money flow enrichment failed: {e}", e=str(exc))

    return {
        "wealth_flows": wealth_flows,
        "pocket_lining_alerts": pocket_lining_alerts,
        "flows": money_flows,
        "circular_flows": circular_flows_data,
        "flow_summary": flow_summary,
    }


@router.get("/actor/{actor_id}")
def get_actor_detail(
    actor_id: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return detailed information about a single actor.

    Includes recent actions, known positions, connections, and any
    pocket-lining alerts involving this actor.
    """
    try:
        from intelligence.actor_network import (
            find_connected_actions,
            assess_pocket_lining,
        )

        engine = get_db_engine()
        # Shared single-flight graph (this handler runs in the threadpool, so
        # waiting on a cold build is safe); never a private rebuild per call.
        graph = get_full_actor_graph(wait=True)
        if graph is None:
            return {"error": "Actor network unavailable", "actor": None}

        # Find the actor node
        actor_node = None
        for n in graph["nodes"]:
            if n["id"] == actor_id:
                actor_node = n
                break

        if not actor_node:
            return {"error": f"Actor '{actor_id}' not found", "actor": None}

        # Connections for this actor
        connections = []
        for link in graph["links"]:
            if link["source"] == actor_id:
                connections.append({"actor_id": link["target"], "relationship": link["relationship"], "strength": link["strength"]})
            elif link["target"] == actor_id:
                connections.append({"actor_id": link["source"], "relationship": link["relationship"], "strength": link["strength"]})

        # Connected actions (correlated trades)
        try:
            connected_actions = find_connected_actions(engine, actor_id)
        except Exception:
            connected_actions = []

        # Pocket-lining alerts involving this actor
        try:
            all_alerts = assess_pocket_lining(engine)
            actor_alerts = [
                a for a in all_alerts
                if actor_id in str(a.get("who", "")).lower()
                or actor_node["label"].lower() in str(a.get("who", "")).lower()
            ]
        except Exception:
            actor_alerts = []

        return {
            "actor": actor_node,
            "connections": connections,
            "connected_actions": connected_actions[:20],
            "pocket_lining_alerts": actor_alerts,
        }

    except Exception as exc:
        log.warning("Actor detail for {a} failed: {e}", a=actor_id, e=str(exc))
        return {"actor": None, "error": str(exc)}


# ── Post-Mortem Endpoints ─────────────────────────────────────────────────


# ── Graph Analytics Endpoints ────────────────────────────────────────────


@router.get("/actor/{actor_id}/analytics")
def get_actor_analytics_endpoint(
    actor_id: str,
    scope: str = Query("full", pattern="^(full|curated)$", description="full graph or curated market-actor subgraph"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return precomputed graph analytics for a single actor.

    Includes PageRank, community ID, betweenness, eigenvector, degree
    centrality, hub score, and authority score.
    """
    try:
        from store.graph import get_actor_analytics

        engine = get_db_engine()
        result = get_actor_analytics(actor_id, engine=engine, scope=scope)
        if result is None:
            return {"error": f"No analytics found for actor '{actor_id}'", "analytics": None}
        return {"analytics": result, "scope": scope}
    except Exception as exc:
        log.warning("Actor analytics for {a} failed: {e}", a=actor_id, e=str(exc))
        return {"analytics": None, "error": str(exc)}


@router.get("/analytics/top")
def get_top_actors_endpoint(
    metric: str = Query("pagerank", description="Metric to rank by"),
    limit: int = Query(20, ge=1, le=200, description="Number of actors to return"),
    scope: str = Query("full", pattern="^(full|curated)$", description="full graph or curated market-actor subgraph"),
    groups: str | None = Query(
        None, max_length=200,
        description="Comma-separated category groups (default market; bulk groups and 'all' are opt-in)",
    ),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return top actors ranked by any analytics metric.

    Allowed metrics: pagerank, betweenness, eigenvector,
    degree_centrality, hub_score, authority_score. With ``scope=full`` and a
    completed actor-universe run, pagerank/betweenness/degree_centrality are
    served from that run, limited to ``groups`` (market actors by default).
    """
    try:
        from store.graph import get_top_actors

        engine = get_db_engine()
        if scope == "full":
            from intelligence.actors.universe_store import TOP_METRIC_COLUMNS, top_actors

            run = latest_universe_run(engine) if metric in TOP_METRIC_COLUMNS else None
            if run is not None:
                res = top_actors(engine, metric=metric, limit=limit, groups=groups, run=run)
                return {"actors": res["entries"], "metric": metric, "count": len(res["entries"]),
                        "scope": scope, "groups": res["groups"], "source": "actor_universe",
                        "graph_version": res["graph_version"], "computed_at": res["computed_at"]}
        actors = get_top_actors(metric=metric, limit=limit, engine=engine, scope=scope)
        return {"actors": actors, "metric": metric, "count": len(actors), "scope": scope}
    except ValueError as exc:
        return {"actors": [], "metric": metric, "count": 0, "error": str(exc)}
    except Exception as exc:
        log.warning("Top actors query failed: {e}", e=str(exc))
        return {"actors": [], "metric": metric, "count": 0, "error": str(exc)}


# ── Community list cache ────────────────────────────────────────────────
#
# The live aggregation (store.graph.get_community_list) groups ~2.7M
# actor_analytics rows into ~44K communities AND runs one extra "top member"
# query PER community — a 44K-query N+1 that is far too slow for an
# interactive request. We read the materialized ``community_summary`` table
# (migration 0055_community_summary.sql) instead, with two layers of
# protection: a short in-process TTL cache, and graceful fallback to the live
# aggregation when the table is missing or empty.
_COMMUNITY_LIST_TTL = 300  # 5 minutes
_community_list_cache: TTLCache = TTLCache(ttl=_COMMUNITY_LIST_TTL, max_size=1)
_COMMUNITY_LIST_CACHE_KEY = "all"


def _read_community_summary(engine: Any) -> tuple[list[dict[str, Any]], str] | None:
    """Read communities from the cached ``community_summary`` table.

    Returns ``(communities, "cache")`` on a hit, or ``None`` when the table is
    absent/empty so the caller can fall back to the live aggregation. Never
    raises — a query failure is treated as a miss.
    """
    from sqlalchemy import text

    try:
        with engine.connect() as conn:
            if not _table_exists(conn, "community_summary"):
                return None
            rows = conn.execute(text(
                "SELECT community_id, member_count, max_pagerank, "
                "top_member, top_category "
                "FROM community_summary "
                "ORDER BY member_count DESC"
            )).fetchall()
    except Exception as exc:
        log.debug("community_summary read failed, falling back: {e}", e=str(exc))
        return None

    if not rows:
        return None

    communities = [
        {
            "community_id": r[0],
            "member_count": int(r[1]) if r[1] is not None else 0,
            "max_pagerank": float(r[2]) if r[2] is not None else 0.0,
            "top_member": r[3],
            "top_category": r[4],
        }
        for r in rows
    ]
    return communities, "cache"


def _load_community_list(engine: Any) -> tuple[list[dict[str, Any]], str]:
    """Cached-first community list with graceful fallback to live aggregation.

    Order: in-process TTL cache → ``community_summary`` table → live
    ``get_community_list`` aggregation. Returns ``(communities, source)`` where
    source is one of ``ttl`` / ``cache`` / ``live``.
    """
    cached = _community_list_cache.get(_COMMUNITY_LIST_CACHE_KEY)
    if cached is not None:
        return cached, "ttl"

    summary = _read_community_summary(engine)
    if summary is not None:
        communities, source = summary
        _community_list_cache.set(_COMMUNITY_LIST_CACHE_KEY, communities)
        return communities, source

    # Fallback: the slow live aggregation (table not yet materialized).
    from store.graph import get_community_list

    communities = get_community_list(engine=engine)
    _community_list_cache.set(_COMMUNITY_LIST_CACHE_KEY, communities)
    return communities, "live"


@router.get("/analytics/communities")
@single_flight
def get_communities_endpoint(
    scope: str = Query("full", pattern="^(full|curated)$", description="full graph or curated market-actor subgraph"),
    limit: int = Query(200, ge=1, le=2000, description="Max communities (curated scope)"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return list of communities with member counts and top member.

    ``scope=full`` reads the materialized ``community_summary`` cache when
    available (see migration 0055), falling back to the live aggregation.
    ``scope=curated`` reads the small named-market-actor table directly. The
    ``source`` field reports which path served the request.
    """
    try:
        engine = get_db_engine()
        if scope == "curated":
            from store.graph import get_community_list

            communities = get_community_list(engine=engine, scope="curated", limit=limit)
            return {"communities": communities, "count": len(communities), "source": "live", "scope": scope}
        run = latest_universe_run(engine)
        if run is not None:
            from intelligence.actors.universe_store import community_summaries

            res = community_summaries(engine, limit=limit, run=run)
            return {"communities": res["entries"], "count": len(res["entries"]),
                    "total": res["total"], "source": "actor_universe", "scope": scope,
                    "computed_at": res.get("computed_at"), "graph_version": res.get("graph_version")}
        communities, source = _load_community_list(engine)
        return {"communities": communities, "count": len(communities), "source": source, "scope": scope}
    except Exception as exc:
        log.warning("Community list failed: {e}", e=str(exc))
        return {"communities": [], "count": 0, "error": str(exc)}


@router.get("/analytics/community/{community_id}")
def get_community_members_endpoint(
    community_id: int,
    limit: int = Query(50, ge=1, le=500, description="Max members to return"),
    scope: str = Query("full", pattern="^(full|curated)$", description="full graph or curated market-actor subgraph"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return all actors in a community, ordered by PageRank."""
    try:
        from store.graph import get_community_members

        engine = get_db_engine()
        members = get_community_members(community_id, limit=limit, engine=engine, scope=scope)
        return {"community_id": community_id, "members": members, "count": len(members), "scope": scope}
    except Exception as exc:
        log.warning("Community members for {c} failed: {e}", c=community_id, e=str(exc))
        return {"community_id": community_id, "members": [], "count": 0, "error": str(exc)}


# ── Post-Mortem Endpoints ─────────────────────────────────────────────────


@router.get("/postmortems")
def get_postmortems(
    days: int = Query(30, ge=1, le=365, description="Lookback days"),
    ticker: str | None = Query(None, description="Filter by ticker"),
    category: str | None = Query(None, description="Filter by failure_category"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Retrieve stored post-mortem analyses for failed trades and predictions.

    Returns all post-mortems within the lookback window, with optional
    ticker and failure category filters. Includes aggregate pattern counts.
    """
    try:
        from intelligence.postmortem import load_postmortems

        engine = get_db_engine()
        records = load_postmortems(engine, days=days, ticker=ticker, category=category)

        # Aggregate pattern counts
        category_counts: dict[str, int] = {}
        for r in records:
            cat = r.get("failure_category", "unknown")
            category_counts[cat] = category_counts.get(cat, 0) + 1

        return {
            "postmortems": records,
            "count": len(records),
            "category_counts": category_counts,
            "filters": {"days": days, "ticker": ticker, "category": category},
        }
    except Exception as exc:
        log.warning("Post-mortem retrieval failed: {e}", e=str(exc))
        return {"postmortems": [], "count": 0, "error": str(exc)}


@router.post("/postmortems/generate")
@_lf_observe(name="intelligence-postmortem-batch", capture_input=False)
def trigger_batch_postmortem(
    days: int = Query(30, ge=1, le=365, description="Lookback days"),
    token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Trigger batch post-mortem generation for all recent failures.

    Generates post-mortems for all failed trades and missed predictions
    in the lookback window that do not already have a post-mortem.
    Returns a summary with the generated post-mortems.
    """
    with _lf_propagate_attributes(
        user_id=_lf_user_id_from_token(token),
        tags=["feature:postmortem-batch"],
    ):
        _lf_set_input(days=days)
        try:
            from intelligence.postmortem import batch_postmortem, generate_lessons_learned

            engine = get_db_engine()
            postmortems = batch_postmortem(engine, days=days)
            lessons = generate_lessons_learned(engine, postmortems) if postmortems else ""

            return {
                "generated": len(postmortems),
                "postmortems": [pm.to_dict() for pm in postmortems],
                "lessons_learned": lessons,
                "days": days,
            }
        except Exception as exc:
            log.warning("Batch post-mortem generation failed: {e}", e=str(exc))
            return {"generated": 0, "postmortems": [], "error": str(exc)}


@router.get("/postmortems/lessons")
def get_lessons_learned(
    days: int = Query(30, ge=1, le=365, description="Lookback days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Generate a lessons-learned report from existing post-mortems.

    Synthesises actionable recommendations from all post-mortems in the
    lookback window using LLM analysis with rule-based fallback.
    """
    try:
        from intelligence.postmortem import load_postmortems, generate_lessons_learned, PostMortem

        engine = get_db_engine()
        records = load_postmortems(engine, days=days)

        if not records:
            return {"lessons": "No post-mortems found in the last {days} days.", "count": 0}

        # Reconstruct PostMortem objects from stored records for the synthesis
        pms = []
        for r in records:
            full = r.get("full_analysis", {})
            if not full:
                continue
            try:
                pms.append(PostMortem(
                    trade_id=full.get("trade_id", 0),
                    ticker=full.get("ticker", r.get("ticker", "")),
                    direction=full.get("direction", ""),
                    outcome=full.get("outcome", r.get("outcome", "")),
                    actual_return=full.get("actual_return", 0.0),
                    data_at_decision=full.get("data_at_decision", {}),
                    thesis_at_decision=full.get("thesis_at_decision", ""),
                    sanity_results_at_decision=full.get("sanity_results_at_decision", {}),
                    what_actually_happened=full.get("what_actually_happened", ""),
                    price_path=full.get("price_path", []),
                    failure_category=full.get("failure_category", r.get("failure_category", "")),
                    root_cause=full.get("root_cause", r.get("root_cause", "")),
                    which_signals_were_wrong=full.get("which_signals_were_wrong", []),
                    which_signals_were_right=full.get("which_signals_were_right", []),
                    what_we_missed=full.get("what_we_missed", r.get("what_we_missed", "")),
                    recommended_fix=full.get("recommended_fix", r.get("recommended_fix", "")),
                    confidence_in_analysis=full.get("confidence_in_analysis", 0.5),
                    generated_at=full.get("generated_at", ""),
                ))
            except Exception:
                continue

        lessons = generate_lessons_learned(engine, pms)
        return {"lessons": lessons, "count": len(pms)}

    except Exception as exc:
        log.warning("Lessons learned generation failed: {e}", e=str(exc))
        return {"lessons": "", "count": 0, "error": str(exc)}


# ── Milestone Tracker Endpoints ─────────────────────────────────────────

def _av_earnings_income_freshness_note(engine: Any) -> str:
    """Honest freshness note for the AlphaVantage inputs this scorecard scans.

    ``scan_all_tickers`` reads ``av:earnings:%:eps`` / ``av:income:*`` from
    ``raw_series`` live on every call, even though both AlphaVantage sources
    are ``active=false`` in ``source_catalog`` (ids 6, 65 — see the Wave 3
    triage report, item #13). This never blocks the scorecard; it only
    states what it can determine about how stale those inputs are so the
    response doesn't silently imply the scan is over fresh data.
    """
    from sqlalchemy import text

    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT MAX(obs_date) FROM raw_series
                WHERE pull_status = 'SUCCESS'
                  AND (series_id LIKE 'av:earnings:%:eps' OR series_id LIKE 'av:income:%')
            """)).fetchone()
        max_obs_date = row[0] if row else None
    except Exception as exc:
        log.warning("Milestone scorecard freshness check failed: {e}", e=str(exc))
        return "AlphaVantage inputs inactive; last-refresh date could not be determined"

    if max_obs_date is None:
        return "AlphaVantage inputs inactive; no successful pull on record for these series"
    return f"AlphaVantage inputs inactive as of {max_obs_date}"


@router.get("/milestones/scorecard")
def get_milestone_scorecard(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return execution scorecard for all tracked companies.

    Scores each company on beat/miss rate, trend, streaks.

    The AlphaVantage sources this scan reads (``av:earnings:*``,
    ``av:income:*``) are inactive in ``source_catalog`` — see ``note``.
    """
    try:
        from intelligence.milestone_tracker import scan_all_tickers
        engine = get_db_engine()
        results = scan_all_tickers(engine)
        return {
            "companies": results,
            "count": len(results),
            "note": _av_earnings_income_freshness_note(engine),
        }
    except Exception as exc:
        log.warning("Milestone scorecard failed: {e}", e=str(exc))
        return {"companies": [], "count": 0, "error": str(exc)}


@router.get("/milestones/{ticker}")
def get_ticker_milestones(
    ticker: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return milestone timeline for a specific ticker."""
    try:
        from intelligence.milestone_tracker import build_earnings_timeline, score_execution
        engine = get_db_engine()
        milestones = build_earnings_timeline(engine, ticker.upper())
        score = score_execution(milestones)
        return {
            "ticker": ticker.upper(),
            "milestones": [
                {
                    "date": m.date.isoformat(),
                    "category": m.category,
                    "description": m.description,
                    "beat_miss": m.beat_miss,
                    "magnitude": m.magnitude,
                }
                for m in milestones
            ],
            "score": score,
        }
    except Exception as exc:
        log.warning("Milestone timeline for {t} failed: {e}", t=ticker, e=str(exc))
        return {"ticker": ticker, "milestones": [], "score": {}, "error": str(exc)}


# ── Attention Anomaly Endpoints ────────────────────────────────────────

@router.get("/attention/alerts")
def get_attention_alerts(
    threshold: float = Query(50.0, description="Minimum attention score"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return entities with unusual attention spikes (Wikipedia + Trends)."""
    try:
        from intelligence.attention_anomaly import get_alerts
        engine = get_db_engine()
        alerts = get_alerts(engine, threshold=threshold)
        return {"alerts": alerts, "count": len(alerts)}
    except Exception as exc:
        log.warning("Attention alerts failed: {e}", e=str(exc))
        return {"alerts": [], "count": 0, "error": str(exc)}


# ── DB-Backed Actor Network (enhanced with ICIJ + LLM profiles) ──────

@router.get("/actor-network/db")
def get_actor_network_db(
    limit: int = Query(200, ge=10, le=2000, description="Max actors to return"),
    min_degree: int = Query(2, ge=0, description="Minimum connections"),
    include_icij: bool = Query(False, description="Include ICIJ offshore matches"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return actor network from database with ICIJ matches and LLM profiles.

    This serves the 5M+ connections graph. Use limit and min_degree to
    control the size for visualization.
    """
    from sqlalchemy import text
    engine = get_db_engine()

    try:
        with engine.connect() as conn:
            # Get top actors by degree
            actors = conn.execute(text("""
                SELECT a.id, a.name, a.tier, a.category, a.degree,
                       a.influence_score, a.title, a.source,
                       a.metadata->'llm_profile'->>'title' as llm_title,
                       a.metadata->'llm_profile'->>'confidence' as llm_confidence,
                       a.provenance, a.provenance_as_of
                FROM actors a
                WHERE a.degree >= :min_deg
                  AND (CAST(:include_icij AS boolean) OR a.source IS NULL
                       OR a.source NOT LIKE 'icij%')
                ORDER BY a.degree DESC
                LIMIT :lim
            """), {"min_deg": min_degree, "lim": limit, "include_icij": include_icij}).fetchall()

            actor_ids = {a[0] for a in actors}
            nodes = [
                {
                    "id": a[0], "label": a[1], "tier": a[2], "category": a[3],
                    "degree": a[4], "influence": float(a[5] or 0),
                    "title": a[8] or a[6] or "",
                    # `a.source` is the legacy ingestion-origin column (e.g.
                    # "spider") -- a different concept from the provenance
                    # module's wire "source" label below; kept under its own key
                    # to avoid the two silently colliding on the same JSON key.
                    "ingestion_source": a[7],
                    "llm_confidence": a[9],
                    **stamp_actor_node({}, a[0], stored=a[10], vintage=a[11]),
                }
                for a in actors
            ]

            # Get connections between these actors (bound array, not an
            # interpolated IN list).
            id_list = [a[0] for a in actors]
            if not id_list:
                return {"nodes": [], "links": [], "stats": {}}

            links_raw = conn.execute(text("""
                SELECT actor_a, actor_b, relationship, strength
                FROM actor_connections
                WHERE actor_a = ANY(:ids) AND actor_b = ANY(:ids)
                LIMIT 5000
            """), {"ids": id_list}).fetchall()

            links = [
                {
                    "source": l[0], "target": l[1],
                    "relationship": l[2], "strength": float(l[3] or 0.5),
                }
                for l in links_raw
                if l[0] in actor_ids and l[1] in actor_ids
            ]

            # Stats
            total_actors = conn.execute(text("SELECT COUNT(*) FROM actors")).fetchone()[0]
            total_connections = conn.execute(text("SELECT COUNT(*) FROM actor_connections")).fetchone()[0]

        return {
            "nodes": nodes,
            "links": links,
            "stats": {
                "nodes_returned": len(nodes),
                "links_returned": len(links),
                "total_actors": total_actors,
                "total_connections": total_connections,
            },
        }
    except Exception as exc:
        log.warning("DB actor network failed: {e}", e=str(exc))
        return {"nodes": [], "links": [], "stats": {}, "error": str(exc)}


@router.get("/actor/{actor_id}/profile")
def get_actor_enriched_profile(
    actor_id: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return enriched actor profile with LLM analysis, ICIJ matches, and connections."""
    from sqlalchemy import text
    engine = get_db_engine()

    try:
        with engine.connect() as conn:
            # Actor details
            actor = conn.execute(text(
                "SELECT id, name, tier, category, title, influence_score, "
                "trust_score, degree, source, metadata, credibility, "
                "provenance, provenance_as_of "
                "FROM actors WHERE id = :id"
            ), {"id": actor_id}).fetchone()

            if not actor:
                return {"error": f"Actor '{actor_id}' not found"}

            # Connections
            connections = conn.execute(text("""
                SELECT ac.actor_b, b.name, ac.relationship, ac.strength, ac.evidence
                FROM actor_connections ac
                JOIN actors b ON ac.actor_b = b.id
                WHERE ac.actor_a = :id
                UNION
                SELECT ac.actor_a, a.name, ac.relationship, ac.strength, ac.evidence
                FROM actor_connections ac
                JOIN actors a ON ac.actor_a = a.id
                WHERE ac.actor_b = :id
                ORDER BY strength DESC LIMIT 50
            """), {"id": actor_id}).fetchall()

            # ICIJ matches
            icij_matches = conn.execute(text("""
                SELECT actor_b, b.name, relationship, strength
                FROM actor_connections ac
                JOIN actors b ON ac.actor_b = b.id
                WHERE ac.actor_a = :id AND ac.relationship LIKE :pat
                ORDER BY strength DESC LIMIT 20
            """), {"id": actor_id, "pat": "icij_%"}).fetchall()

            import json
            metadata = actor[9] if isinstance(actor[9], dict) else json.loads(actor[9] or "{}")
            llm_profile = metadata.get("llm_profile", {})

            return {
                "actor": {
                    "id": actor[0], "name": actor[1], "tier": actor[2],
                    "category": actor[3], "title": actor[4],
                    "influence_score": float(actor[5] or 0),
                    "trust_score": float(actor[6] or 0),
                    "degree": actor[7],
                    # `actor[8]` (actors.source) is the legacy ingestion-origin
                    # column -- kept under its own key so it doesn't collide
                    # with the provenance module's wire "source" label below.
                    "ingestion_source": actor[8],
                    "credibility": actor[10],
                    **stamp_actor_node({}, actor[0], stored=actor[11], vintage=actor[12]),
                },
                "llm_profile": llm_profile,
                "connections": [
                    {"id": c[0], "name": c[1], "relationship": c[2], "strength": float(c[3] or 0)}
                    for c in connections
                ],
                "icij_matches": [
                    {"id": i[0], "name": i[1], "match_type": i[2], "similarity": float(i[3] or 0)}
                    for i in icij_matches
                ],
            }
    except Exception as exc:
        log.warning("Enriched profile for {a} failed: {e}", a=actor_id, e=str(exc))
        return {"error": str(exc)}


# ── Trend Tracker Endpoints ──────────────────────────────────────────────


@router.get("/trends")
def get_trends(
    days: int = Query(90, ge=1, le=365, description="Lookback days for trend analysis"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Run trend analysis across momentum, regime, sector rotation, volatility, liquidity, and correlations.

    Returns detected trends with direction, strength, confidence, and a
    narrative synthesis of what the trends collectively indicate.
    """
    try:
        from intelligence.trend_tracker import analyze_trends

        engine = get_db_engine()
        return analyze_trends(engine, lookback_days=days)
    except Exception as exc:
        log.warning("Trend analysis failed: {e}", e=str(exc))
        return {
            "trends": [],
            "category_summaries": {},
            "narrative": f"Trend analysis engine error: {exc}",
            "generated_at": None,
            "error": str(exc),
        }


# ── Sector Power Map ────────────────────────────────────────────────────

_RELATIONSHIP_COLORS = {
    "competitor": "#EF4444",
    "industry_peer": "#3B82F6",
    "co_investor": "#22C55E",
    "co_investment": "#22C55E",
    "business_partner": "#14B8A6",
    "insider_trade": "#F59E0B",
    "insider_cluster": "#FBBF24",
    "congressional_trade": "#EC4899",
    "congress_insider_overlap": "#F43F5E",
    "officer_of": "#8B5CF6",
    "wealth_management": "#6366F1",
    "signal_linked": "#06B6D4",
    "filing_related": "#64748B",
    "gov_contract": "#10B981",
    "co_contractor": "#059669",
    "lobbying": "#A78BFA",
    "lobbying_influence": "#7C3AED",
    "foreign_lobbying": "#C084FC",
    "darkpool_activity": "#38BDF8",
    "institutional_holding": "#2DD4BF",
    "co_traded_insider": "#FB923C",
    "co_traded_congress": "#F472B6",
    # ── Multi-domain intel-expand chain types ──
    # (mirrored by EDGE_TYPE_COLORS in pwa/src/components/canvas/nodeStyles.js)
    "supplier": "#3B82F6",       # blue — supply chain (AAPL ← TSMC)
    "supply_chain": "#3B82F6",
    "customer": "#60A5FA",
    "causation": "#F97316",      # orange — causal lever (tariff → ticker)
    "jurisdiction": "#EAB308",   # gold — committee oversees ticker/sector
    "committee": "#EAB308",
    "member_trade": "#EC4899",   # pink — committee member's disclosed trade
}


@router.get("/power-map/{sector_name}")
def get_sector_power_map(
    sector_name: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return a focused power map for a sector.

    Nodes: top actors from the sector map + connected insiders/politicians/funds.
    Edges: real connections from actor_connections (competitor, co_investor,
    insider_trade, congressional_trade, etc.)
    """
    from sqlalchemy import text

    engine = get_db_engine()

    # Step 1: Get sector companies from the sector map
    try:
        from analysis.sector_map import SECTOR_MAP, get_actor_influence
    except ImportError:
        return {"nodes": [], "edges": [], "error": "sector_map not available"}

    sector = SECTOR_MAP.get(sector_name)
    if not sector:
        return {"nodes": [], "edges": [], "error": f"Sector '{sector_name}' not found"}

    sector_actors = get_actor_influence(sector_name)
    # Collect tickers and names from the sector map
    sector_tickers = {a["ticker"].upper() for a in sector_actors if a.get("ticker")}
    sector_names = {a["name"].lower() for a in sector_actors}

    # Step 2: Find matching actors in the DB
    # DB names may look like "PFIZER INC (PFE)" or "Eli Lilly" — search broadly
    actor_rows = []
    with engine.connect() as conn:
        # Match by ticker in parentheses (DB pattern: "COMPANY NAME (TICKER)")
        # and by ticker substring in name
        if sector_tickers:
            for ticker in sector_tickers:
                rows = conn.execute(text(
                    "SELECT id, name, category, influence_score, trust_score, "
                    "net_worth_estimate, title, known_positions, "
                    "provenance, provenance_as_of "
                    "FROM actors "
                    "WHERE category NOT IN ('icij_entity', 'icij_officer', 'icij_intermediary') "
                    "AND (name ILIKE :paren OR UPPER(name) LIKE :upper_pat) "
                    "ORDER BY influence_score DESC NULLS LAST "
                    "LIMIT 3"
                ), {
                    "paren": f"%({ticker})%",
                    "upper_pat": f"%{ticker}%",
                }).fetchall()
                actor_rows.extend(rows)

        # Match by sector_map name prefix (first significant word)
        for sname in sector_names:
            # Use first word for matching to handle "Johnson & Johnson" → "johnson"
            first_word = sname.split()[0] if sname else ""
            if len(first_word) < 3:
                continue
            rows = conn.execute(text(
                "SELECT id, name, category, influence_score, trust_score, "
                "net_worth_estimate, title, known_positions, "
                "provenance, provenance_as_of "
                "FROM actors "
                "WHERE category NOT IN ('icij_entity', 'icij_officer', 'icij_intermediary') "
                "AND name ILIKE :pat "
                "ORDER BY influence_score DESC NULLS LAST "
                "LIMIT 2"
            ), {"pat": f"{first_word}%"}).fetchall()
            actor_rows.extend(rows)

        # Note: high-influence actors (billionaires, insiders) are pulled via
        # 1-hop expansion below — only those connected to sector companies.

    # Deduplicate by actor ID
    seen_ids: set[str] = set()
    actor_map: dict[str, dict] = {}
    for row in actor_rows:
        aid = row[0]
        if aid in seen_ids:
            continue
        seen_ids.add(aid)
        actor_map[aid] = {
            "id": aid,
            "name": row[1],
            "category": row[2],
            "influence": float(row[3]) if row[3] else 0.5,
            "trust": float(row[4]) if row[4] else 0.5,
            "net_worth": float(row[5]) if row[5] else None,
            "title": row[6],
            "_provenance": row[8],
            "_provenance_as_of": row[9],
        }

    if not actor_map:
        # Fallback: build from sector_map data alone (no DB matches at all).
        # No `actors` row exists for anything in this sector -- same
        # synthesized-from-sector_map situation as the per-ticker synthetic
        # nodes built below when some (but not all) actors have DB matches.
        nodes = []
        for a in sector_actors[:25]:
            nodes.append({
                "id": a["name"].lower().replace(" ", "_"),
                "name": a["name"],
                "category": a["type"],
                "influence": a["influence"],
                "trust": 0.5,
                "ticker": a.get("ticker"),
                "subsector": a.get("subsector"),
                "synthetic": True,
                "source": SOURCE_SECTOR_MAP,
                "source_as_of": None,
            })
        return {
            "nodes": nodes,
            "edges": [],
            "sector": sector_name,
            "etf": sector.get("etf"),
        }

    # Step 3: Find connections BETWEEN these actors
    actor_ids = list(actor_map.keys())
    edges = []
    if len(actor_ids) >= 2:
        with engine.connect() as conn:
            # Use ANY for batch lookup
            rows = conn.execute(text(
                "SELECT actor_a, actor_b, relationship, strength "
                "FROM actor_connections "
                "WHERE actor_a = ANY(:ids) AND actor_b = ANY(:ids) "
                "AND relationship NOT LIKE 'icij_%' "
                "AND strength > 0.2 "
                "ORDER BY strength DESC "
                "LIMIT 200"
            ), {"ids": actor_ids}).fetchall()

            for r in rows:
                edges.append({
                    "source": r[0],
                    "target": r[1],
                    "relationship": r[2],
                    "strength": float(r[3]) if r[3] else 0.5,
                    "color": _RELATIONSHIP_COLORS.get(r[2], "#64748B"),
                })

    # Step 4: Also find actors connected TO our sector actors (1-hop expansion)
    # This brings in insiders, politicians, etc. connected to sector companies
    if actor_ids:
        with engine.connect() as conn:
            expand_rows = conn.execute(text(
                "SELECT actor_a, actor_b, relationship, strength "
                "FROM actor_connections "
                "WHERE (actor_a = ANY(:ids) OR actor_b = ANY(:ids)) "
                "AND relationship NOT LIKE 'icij_%' "
                "AND relationship IN ('insider_trade', 'congressional_trade', "
                "'co_investor', 'officer_of', 'business_partner', "
                "'insider_cluster', 'congress_insider_overlap', "
                "'gov_contract', 'lobbying', 'institutional_holding', "
                "'darkpool_activity', 'foreign_lobbying') "
                "AND strength > 0.5 "
                "ORDER BY strength DESC "
                "LIMIT 100"
            ), {"ids": actor_ids}).fetchall()

            new_ids = set()
            for r in expand_rows:
                a, b = r[0], r[1]
                if a not in actor_map:
                    new_ids.add(a)
                if b not in actor_map:
                    new_ids.add(b)
                edges.append({
                    "source": a,
                    "target": b,
                    "relationship": r[2],
                    "strength": float(r[3]) if r[3] else 0.5,
                    "color": _RELATIONSHIP_COLORS.get(r[2], "#64748B"),
                })

            # Fetch details for newly discovered actors
            if new_ids:
                new_rows = conn.execute(text(
                    "SELECT id, name, category, influence_score, trust_score, "
                    "net_worth_estimate, title, provenance, provenance_as_of "
                    "FROM actors WHERE id = ANY(:ids)"
                ), {"ids": list(new_ids)}).fetchall()
                for nr in new_rows:
                    actor_map[nr[0]] = {
                        "id": nr[0],
                        "name": nr[1],
                        "category": nr[2],
                        "influence": float(nr[3]) if nr[3] else 0.3,
                        "trust": float(nr[4]) if nr[4] else 0.5,
                        "net_worth": float(nr[5]) if nr[5] else None,
                        "title": nr[6],
                        "_provenance": nr[7],
                        "_provenance_as_of": nr[8],
                    }

    # Step 5: Merge sector_map metadata (ticker, subsector, price) into nodes
    # Map sector_map actors to DB actors by name similarity
    ticker_by_name: dict[str, str] = {}
    subsector_by_name: dict[str, str] = {}
    influence_by_name: dict[str, float] = {}
    for a in sector_actors:
        key = a["name"].lower()
        if a.get("ticker"):
            ticker_by_name[key] = a["ticker"]
        subsector_by_name[key] = a.get("subsector", "")
        influence_by_name[key] = a["influence"]

    # Build final node list
    # Include sector_map actors that had no DB match as synthetic nodes
    db_names_lower = {v["name"].lower() for v in actor_map.values()}
    for a in sector_actors:
        name_lower = a["name"].lower()
        if name_lower not in db_names_lower and a.get("ticker"):
            synth_id = f"sector_{a['ticker'].lower()}"
            actor_map[synth_id] = {
                "id": synth_id,
                "name": a["name"],
                "category": a["type"],
                "influence": a["influence"],
                "trust": 0.5,
                "net_worth": None,
                "title": None,
                "ticker": a["ticker"],
                "subsector": a.get("subsector"),
                "synthetic": True,
                # Not an `actors` row at all -- synthesized from
                # analysis/sector_map_data.yaml, so there is no stored
                # provenance to look up. This is exactly the case
                # SOURCE_SECTOR_MAP exists for.
                "source": SOURCE_SECTOR_MAP,
                "source_as_of": None,
            }

    nodes = []
    for actor in actor_map.values():
        name_lower = actor["name"].lower()
        stored_provenance = actor.pop("_provenance", None)
        stored_vintage = actor.pop("_provenance_as_of", None)
        node = {
            **actor,
            "ticker": actor.get("ticker") or ticker_by_name.get(name_lower),
            "subsector": actor.get("subsector") or subsector_by_name.get(name_lower),
        }
        # Use sector_map influence if available (more curated)
        if name_lower in influence_by_name:
            node["influence"] = influence_by_name[name_lower]
        if not node.get("synthetic"):
            stamp_actor_node(node, node["id"], stored=stored_provenance, vintage=stored_vintage)
        nodes.append(node)

    # Deduplicate edges
    edge_keys: set[str] = set()
    unique_edges = []
    for e in edges:
        key = f"{e['source']}:{e['target']}:{e['relationship']}"
        rev_key = f"{e['target']}:{e['source']}:{e['relationship']}"
        if key not in edge_keys and rev_key not in edge_keys:
            edge_keys.add(key)
            unique_edges.append(e)

    # Only keep nodes that appear in at least one edge, plus all sector_map actors
    sector_node_ids = {n["id"] for n in nodes if n.get("ticker") or n.get("synthetic")}
    edge_node_ids = set()
    for e in unique_edges:
        edge_node_ids.add(e["source"])
        edge_node_ids.add(e["target"])
    keep_ids = sector_node_ids | edge_node_ids
    nodes = [n for n in nodes if n["id"] in keep_ids]

    return {
        "nodes": nodes,
        "edges": unique_edges,
        "sector": sector_name,
        "etf": sector.get("etf"),
        "subsectors": list(sector.get("subsectors", {}).keys()),
        "relationship_colors": _RELATIONSHIP_COLORS,
    }


# ── Ego Graph ──────────────────────────────────────────────────────────────


@router.get("/ego-graph/search")
def ego_graph_search(
    q: str = Query(..., min_length=1, max_length=200, description="Search query (name or ticker)"),
    limit: int = Query(20, ge=1, le=100),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Search for actors by name or ticker for the ego-graph.

    Searches every actor (trigram index), ranked by precomputed importance;
    ICIJ leak rows are left out here as before (``/actor-universe/search``
    covers every group).
    """
    from intelligence.actors.universe import CATEGORY_GROUPS, GROUP_ICIJ
    from intelligence.actors.universe_store import search_actors

    try:
        groups = [g for g in CATEGORY_GROUPS if g != GROUP_ICIJ]
        res = search_actors(get_db_engine(), q, limit=limit, offset=0, groups=groups)
        if not res["entries"] and q.strip():
            # "(NVDA)"-style ticker spellings in actor names.
            res = search_actors(get_db_engine(), f"({q.strip().upper()})", limit=limit,
                                offset=0, groups=groups)
        return {"results": res["entries"], "total": res["total"]}
    except Exception as exc:  # noqa: BLE001
        log.warning("Ego-graph search failed: {e}", e=str(exc))
        return {"results": [], "error": str(exc)}


@router.get("/ego-graph/{actor_id}")
def get_ego_graph(
    actor_id: str,
    depth: int = Query(2, ge=1, le=3, description="Hop depth (1-3)"),
    max_nodes: int = Query(80, ge=10, le=300, description="Max nodes to return"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return ego-graph centered on one actor with N-degree connections.

    Nodes are tagged with ``ring`` (0=center, 1=first hop, etc.) for
    concentric layout. Bounded fan-out per node (see
    ``intelligence.actors.universe_store.ego_network``); ICIJ / offshore
    relationships stay out of this legacy view (``/actor-universe/ego``
    includes them).
    """
    from intelligence.actors.universe_store import ego_network

    try:
        fanout = max(5, min(40, max_nodes // max(depth, 1)))
        res = ego_network(
            get_db_engine(), actor_id, depth=depth, fanout=fanout, max_nodes=max_nodes,
            include_offshore=False, include_external=False, min_strength=0.2,
        )
        for e in res.get("edges", []):
            e["color"] = _RELATIONSHIP_COLORS.get(e["relationship"], e.get("color", "#64748B"))
        res["relationship_colors"] = {**res.get("relationship_colors", {}), **_RELATIONSHIP_COLORS}
        return res
    except Exception as exc:  # noqa: BLE001
        log.warning("Ego-graph for {a} failed: {e}", a=actor_id, e=str(exc))
        return {"nodes": [], "edges": [], "error": str(exc)}


# ── Actor universe (full scale, precomputed) ──────────────────────────────


@router.get("/actor-universe/status")
def get_actor_universe_status(_token: str = Depends(require_auth)) -> dict[str, Any]:
    """Latest completed full-graph analytics run (freshness + graph size)."""
    from intelligence.actors.universe_store import latest_run

    try:
        run = latest_run(get_db_engine())
    except Exception as exc:  # noqa: BLE001
        log.warning("Actor universe status failed: {e}", e=str(exc))
        return {"run": None, "error": str(exc)}
    return {"run": run, "status": "ready" if run else "pending_first_run"}


@router.get("/actor-universe/overview")
def get_actor_universe_overview(
    n: int = Query(2000, ge=10, le=20000, description="Number of actors (browser default 2,000)"),
    groups: str | None = Query(None, description="market (default), icij, pep, sanctioned, donor, watchlist, or all"),
    categories: str | None = Query(None, description="Comma-separated actor categories"),
    sector: str | None = Query(None, max_length=64),
    community: int | None = Query(None, ge=0),
    max_edges: int | None = Query(None, ge=0, le=200000),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Top-N actors by precomputed importance plus the edges among them."""
    from intelligence.actors.universe_store import overview

    try:
        return overview(get_db_engine(), n=n, groups=groups, categories=categories,
                        sector=sector, community=community, max_edges=max_edges)
    except Exception as exc:  # noqa: BLE001
        log.warning("Actor universe overview failed: {e}", e=str(exc))
        return {"nodes": [], "links": [], "metadata": {}, "error": str(exc)}


@router.get("/actor-universe/ego/{actor_id}")
def get_actor_universe_ego(
    actor_id: str,
    depth: int = Query(1, ge=1, le=3),
    fanout: int = Query(25, ge=1, le=200, description="Max neighbours kept per node"),
    max_nodes: int = Query(300, ge=2, le=2000),
    include_offshore: bool = Query(True, description="Include ICIJ / entity_relationships edges"),
    include_external: bool = Query(True, description="Keep non-actor endpoints as leaf nodes"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """K-hop neighbourhood of one actor from the edge tables, bounded fan-out."""
    from intelligence.actors.universe_store import ego_network

    try:
        return ego_network(get_db_engine(), actor_id, depth=depth, fanout=fanout,
                           max_nodes=max_nodes, include_offshore=include_offshore,
                           include_external=include_external)
    except Exception as exc:  # noqa: BLE001
        log.warning("Actor universe ego for {a} failed: {e}", a=actor_id, e=str(exc))
        return {"nodes": [], "edges": [], "center": actor_id, "error": str(exc)}


@router.get("/actor-universe/search")
def search_actor_universe(
    q: str = Query(..., min_length=3, max_length=200, description="Substring of the actor name (3+ chars)"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0, le=100000),
    groups: str | None = Query(None, description="Restrict to category groups (default: all)"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Search all ~3.5M actors by name (pg_trgm), paginated."""
    from intelligence.actors.universe_store import search_actors

    try:
        return search_actors(get_db_engine(), q, limit=limit, offset=offset, groups=groups)
    except Exception as exc:  # noqa: BLE001
        log.warning("Actor universe search failed: {e}", e=str(exc))
        return {"entries": [], "total": 0, "limit": limit, "offset": offset,
                "has_more": False, "error": str(exc)}


@router.get("/actor-universe/communities")
def get_actor_universe_communities(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    min_size: int = Query(3, ge=1),
    sector: str | None = Query(None, max_length=64),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Precomputed community summaries (largest first), paginated."""
    from intelligence.actors.universe_store import community_summaries

    try:
        return community_summaries(get_db_engine(), limit=limit, offset=offset,
                                   min_size=min_size, sector=sector)
    except Exception as exc:  # noqa: BLE001
        log.warning("Actor universe communities failed: {e}", e=str(exc))
        return {"entries": [], "total": 0, "limit": limit, "offset": offset,
                "has_more": False, "error": str(exc)}


@router.get("/actor-universe/community/{community_id}")
def get_actor_universe_community(
    community_id: int,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Members of one community by importance, paginated."""
    from intelligence.actors.universe_store import community_members

    try:
        return community_members(get_db_engine(), community_id, limit=limit, offset=offset)
    except Exception as exc:  # noqa: BLE001
        log.warning("Actor universe community {c} failed: {e}", c=community_id, e=str(exc))
        return {"community_id": community_id, "entries": [], "total": 0, "limit": limit,
                "offset": offset, "has_more": False, "error": str(exc)}


@router.get("/actor-universe/actor/{actor_id}")
def get_actor_universe_actor(
    actor_id: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """One actor's metrics (importance, pagerank, community, sector ...)."""
    from intelligence.actors.universe_store import actor_metrics

    try:
        m = actor_metrics(get_db_engine(), actor_id)
    except Exception as exc:  # noqa: BLE001
        log.warning("Actor universe metrics for {a} failed: {e}", a=actor_id, e=str(exc))
        return {"metrics": None, "error": str(exc)}
    return {"metrics": m}


# ── Multi-domain Intel Expansion ───────────────────────────────────────────
#
# Turns a flat ticker into a *typed* cross-domain chain so a canvas expansion
# can walk: TICKER → supplier (supply_chain_edges) → causal lever
# (causal_links) → congressional committee with jurisdiction → that committee's
# members' disclosed trades (congressional_trades). Each edge carries a
# discrete ``relationship``/``type`` so the frontend colours it by kind
# (supplier=blue, causation=orange, jurisdiction=gold, member_trade=pink).
#
# Example reachable chain:
#   AAPL → TSMC (supplier) → tariff (causation) → Ways & Means (jurisdiction)
#        → Rep. X: BUY (member_trade)


def _table_exists(conn: Any, table_name: str) -> bool:
    """True if a relation exists (schema-qualified or bare). Safe on error."""
    try:
        from sqlalchemy import text

        row = conn.execute(
            text("SELECT to_regclass(:n)").bindparams(n=table_name)
        ).fetchone()
        return bool(row and row[0])
    except Exception:
        return False


def _committees_for_ticker(ticker: str) -> list[str]:
    """Return congressional committees whose jurisdiction covers ``ticker``.

    Pure (no DB): inverts ``COMMITTEE_SECTOR_MAP`` from the legislation puller
    so a ticker maps back to the committees that oversee its sector. Falls
    back to an empty list if the map can't be imported.
    """
    t = (ticker or "").strip().upper()
    if not t:
        return []
    try:
        from ingestion.altdata.legislation import COMMITTEE_SECTOR_MAP
    except Exception:
        return []
    matches: list[str] = []
    for committee, tickers in COMMITTEE_SECTOR_MAP.items():
        if t in {str(x).upper() for x in tickers}:
            matches.append(committee)
    return sorted(matches)


def _intel_expand_graph(engine: Any, ticker: str, max_per_hop: int = 6) -> dict[str, Any]:
    """Build a typed cross-domain expansion graph centered on ``ticker``.

    Returns ``{nodes, edges, center, relationship_colors}``. Each table is
    probed defensively — a missing table degrades that hop rather than failing
    the whole response.
    """
    from sqlalchemy import text

    t = (ticker or "").strip().upper()
    center_id = f"t:{t}"
    nodes: dict[str, dict[str, Any]] = {
        center_id: {"id": center_id, "label": t, "type": "ticker", "ticker": t, "ring": 0},
    }
    edges: list[dict[str, Any]] = []

    def _add_node(nid: str, node: dict[str, Any]) -> None:
        if nid not in nodes:
            nodes[nid] = node

    def _add_edge(src: str, tgt: str, relationship: str, **extra: Any) -> None:
        edges.append({
            "source": src,
            "target": tgt,
            "relationship": relationship,
            "type": relationship,
            "color": _RELATIONSHIP_COLORS.get(relationship, "#64748B"),
            **extra,
        })

    with engine.connect() as conn:
        # ── Hop 1: suppliers (ticker is the DOWNSTREAM consumer) ──
        if _table_exists(conn, "supply_chain_edges"):
            try:
                sup_rows = conn.execute(text("""
                    SELECT upstream_id, relationship, annual_usd,
                           pct_downstream_cogs, chokepoint_score, confidence
                    FROM supply_chain_edges
                    WHERE UPPER(downstream_id) = :t
                    ORDER BY COALESCE(annual_usd, 0) DESC
                    LIMIT :lim
                """), {"t": t, "lim": max_per_hop}).fetchall()
            except Exception as exc:
                log.debug("intel-expand supplier hop failed: {e}", e=str(exc))
                sup_rows = []

            sup_ids = [r[0] for r in sup_rows]
            labels: dict[str, str] = {}
            if sup_ids and _table_exists(conn, "supply_chain_nodes"):
                try:
                    lab_rows = conn.execute(text(
                        "SELECT id, name FROM supply_chain_nodes WHERE id = ANY(:ids)"
                    ), {"ids": sup_ids}).fetchall()
                    labels = {lr[0]: lr[1] for lr in lab_rows}
                except Exception as exc:
                    log.debug("intel-expand supplier labels failed: {e}", e=str(exc))

            for r in sup_rows:
                up_id = r[0]
                nid = f"sc:{up_id}"
                _add_node(nid, {
                    "id": nid,
                    "label": labels.get(up_id) or str(up_id).replace("_", " ").title(),
                    "type": "company",
                    "ring": 1,
                    "chokepoint_score": float(r[4]) if r[4] is not None else None,
                })
                _add_edge(
                    nid, center_id, "supplier",
                    strength=min(1.0, float(r[4]) if r[4] is not None else 0.6),
                    annual_usd=float(r[2]) if r[2] is not None else None,
                    input_type=r[1],
                )

        # ── Hop 2: causal levers affecting the ticker ──
        from intelligence.causal_links import EARNINGS_EDGE_READ_GUARD_SQL

        cause_nodes: list[str] = []
        if _table_exists(conn, "causal_links"):
            try:
                cause_rows = conn.execute(text("""
                    SELECT cause_type, probable_cause, MAX(probability) AS prob
                    FROM causal_links
                    WHERE UPPER(ticker) = :t
                """ + EARNINGS_EDGE_READ_GUARD_SQL + """
                    GROUP BY cause_type, probable_cause
                    ORDER BY prob DESC NULLS LAST
                    LIMIT :lim
                """), {"t": t, "lim": max_per_hop}).fetchall()
            except Exception as exc:
                log.debug("intel-expand causation hop failed: {e}", e=str(exc))
                cause_rows = []

            for r in cause_rows:
                ctype = (r[0] or "cause").strip()
                cnid = f"cause:{t}:{ctype}"
                cause_nodes.append(cnid)
                _add_node(cnid, {
                    "id": cnid,
                    "label": (r[1] or ctype)[:80],
                    "type": "event",
                    "category": "macro",
                    "cause_type": ctype,
                    "ring": 1,
                })
                _add_edge(
                    cnid, center_id, "causation",
                    strength=float(r[2]) if r[2] is not None else 0.5,
                )

        # ── Hop 3: committees with jurisdiction over the ticker ──
        committees = _committees_for_ticker(t)
        committee_nodes: dict[str, str] = {}
        for committee in committees:
            cmid = f"committee:{committee}"
            committee_nodes[committee] = cmid
            _add_node(cmid, {
                "id": cmid,
                "label": committee.title() + " Cmte.",
                "type": "actor",
                "category": "government",
                "tier": "regional",
                "ring": 2,
            })
            # Link jurisdiction from each causal lever (policy → committee) when
            # present, else straight from the ticker.
            anchors = cause_nodes or [center_id]
            for anchor in anchors:
                _add_edge(cmid, anchor, "jurisdiction", strength=0.5)

        # ── Hop 4: committee members' disclosed trades in the ticker ──
        if committees and _table_exists(conn, "congressional_trades"):
            try:
                trade_rows = conn.execute(text("""
                    SELECT representative, party, state, transaction_type,
                           amount_midpoint, committee, disclosure_date
                    FROM congressional_trades
                    WHERE UPPER(ticker) = :t AND committee IS NOT NULL
                    ORDER BY disclosure_date DESC NULLS LAST
                    LIMIT :lim
                """), {"t": t, "lim": max_per_hop * 2}).fetchall()
            except Exception as exc:
                log.debug("intel-expand member-trade hop failed: {e}", e=str(exc))
                trade_rows = []

            for r in trade_rows:
                rep = r[0] or "Unknown"
                committee_raw = (r[5] or "").lower()
                # Attach to a matching jurisdiction committee node when possible.
                matched = next(
                    (committee_nodes[c] for c in committees if c in committee_raw),
                    None,
                )
                mid = f"member:{rep}"
                party_tag = f" ({r[1]}-{r[2]})" if r[1] and r[2] else ""
                _add_node(mid, {
                    "id": mid,
                    "label": f"Rep. {rep}{party_tag}",
                    "type": "actor",
                    "category": "politician",
                    "tier": "individual",
                    "ring": 3,
                })
                if matched:
                    _add_edge(matched, mid, "member_trade", strength=0.55)
                # The disclosed trade itself ties the member back to the ticker.
                amt = float(r[4]) if r[4] is not None else None
                _add_edge(
                    mid, center_id, "congressional_trade",
                    strength=0.5,
                    transaction_type=r[3],
                    amount=amt,
                )

    from intelligence.causal_links import earnings_timing_policy

    return {
        "nodes": list(nodes.values()),
        "edges": edges,
        "center": center_id,
        "ticker": t,
        "relationship_colors": _RELATIONSHIP_COLORS,
        "causal_links_coverage": "partial",
        "earnings_timing": earnings_timing_policy(),
    }


@router.get("/intel-expand/{ticker}")
def get_intel_expand(
    ticker: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Cross-domain expansion for a ticker into typed supply/causation/committee edges.

    Surfaces the chain TICKER → supplier → causal lever → committee
    (jurisdiction) → committee members' disclosed trades. Each edge is typed so
    the canvas colours it by relationship kind. Returns ``{nodes, edges}`` in
    the same shape the canvas store consumes; degrades gracefully (empty hops)
    when a backing table is absent.
    """
    t = (ticker or "").strip().upper()
    if not t:
        return {"nodes": [], "edges": [], "error": "ticker required"}
    try:
        engine = get_db_engine()
        return _intel_expand_graph(engine, t)
    except Exception as exc:
        log.warning("Intel-expand for {t} failed: {e}", t=t, e=str(exc))
        return {"nodes": [], "edges": [], "center": f"t:{t}", "error": str(exc)}


# ── Grand Power Map ────────────────────────────────────────────────────────


@router.get("/grand-power-map")
def get_grand_power_map(
    limit: int = Query(50, ge=10, le=200, description="Max top actors"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the grand power map — top actors across ALL sectors with connections.

    The Palantir view: the most influential actors in the system, cross-sector
    connections, and money flows between them.
    """
    from sqlalchemy import text
    engine = get_db_engine()

    try:
        with engine.connect() as conn:
            # Get top actors by CONNECTION DENSITY with CATEGORY DIVERSITY.
            # Take the top N from each category so corporations, politicians,
            # billionaires, funds, and government all appear — not just insiders.
            top_actors = conn.execute(text("""
                WITH conn_counts AS (
                    SELECT actor_id, count(*) AS degree
                    FROM (
                        SELECT actor_a AS actor_id FROM actor_connections
                        WHERE relationship NOT LIKE 'icij_%%'
                        AND relationship != 'signal_linked'
                        UNION ALL
                        SELECT actor_b AS actor_id FROM actor_connections
                        WHERE relationship NOT LIKE 'icij_%%'
                        AND relationship != 'signal_linked'
                    ) sub
                    GROUP BY actor_id
                    HAVING count(*) >= 3
                ),
                ranked AS (
                    SELECT a.id, a.name, a.category, a.tier, a.influence_score,
                           a.trust_score, a.net_worth_estimate, a.title, a.known_positions,
                           COALESCE(c.degree, 0) AS degree,
                           a.provenance, a.provenance_as_of,
                           ROW_NUMBER() OVER (
                               PARTITION BY a.category
                               ORDER BY c.degree DESC, a.influence_score DESC
                           ) AS cat_rank
                    FROM actors a
                    JOIN conn_counts c ON a.id = c.actor_id
                    WHERE a.category NOT IN (
                        'icij_entity', 'icij_officer', 'icij_intermediary', 'unknown'
                    )
                    AND a.id NOT LIKE 'qq_%%'
                    AND a.name NOT LIKE 'qq_%%'
                    AND a.id NOT LIKE 'ins_qq_%%'
                    AND a.id NOT LIKE 'pol_qq_%%'
                )
                SELECT id, name, category, tier, influence_score,
                       trust_score, net_worth_estimate, title, known_positions, degree,
                       provenance, provenance_as_of
                FROM ranked
                WHERE (category = 'corporation' AND cat_rank <= 15)
                   OR (category = 'politician' AND cat_rank <= 8)
                   OR (category = 'billionaire' AND cat_rank <= 6)
                   OR (category IN ('insider') AND cat_rank <= 6)
                   OR (category IN ('fund', 'pension_fund', 'swf') AND cat_rank <= 5)
                   OR (category IN ('government', 'central_bank') AND cat_rank <= 5)
                   OR (category IN ('dynasty', 'kingmaker', 'royal', 'activist') AND cat_rank <= 3)
                   OR (category NOT IN ('corporation','politician','billionaire','insider',
                       'fund','pension_fund','swf','government','central_bank',
                       'dynasty','kingmaker','royal','activist') AND cat_rank <= 2)
                ORDER BY degree DESC
                LIMIT :lim
            """), {"lim": limit}).fetchall()

            # Deduplicate actors: prefer "Expanded Profile" over base name
            seen_base_names: dict[str, int] = {}
            deduped_actors = []
            for a in top_actors:
                name = a[1]
                base = name.replace(" (Expanded Profile)", "").strip().lower()
                if base in seen_base_names:
                    # Keep the one with higher degree
                    existing_idx = seen_base_names[base]
                    if int(a[9] or 0) > int(deduped_actors[existing_idx][9] or 0):
                        deduped_actors[existing_idx] = a
                else:
                    seen_base_names[base] = len(deduped_actors)
                    deduped_actors.append(a)

            nodes = []
            actor_ids = []
            for a in deduped_actors:
                actor_ids.append(a[0])
                node = {
                    "id": a[0], "name": a[1], "category": a[2],
                    "tier": a[3],
                    "influence": float(a[4]) if a[4] else 0.5,
                    "trust": float(a[5]) if a[5] else 0.5,
                    "net_worth": float(a[6]) if a[6] else None,
                    "title": a[7],
                    "degree": int(a[9]) if len(a) > 9 else 0,
                }
                stamp_actor_node(node, a[0], stored=a[10], vintage=a[11])
                nodes.append(node)

            # Find connections between top actors (lower threshold for grand map)
            edges = []
            edge_keys: set[str] = set()
            if len(actor_ids) >= 2:
                rows = conn.execute(text("""
                    SELECT actor_a, actor_b, relationship, strength
                    FROM actor_connections
                    WHERE actor_a = ANY(:ids) AND actor_b = ANY(:ids)
                    AND relationship NOT LIKE 'icij_%%'
                    AND strength > 0.1
                    ORDER BY strength DESC
                    LIMIT 500
                """), {"ids": actor_ids}).fetchall()

                for r in rows:
                    key = f"{r[0]}:{r[1]}:{r[2]}"
                    rev = f"{r[1]}:{r[0]}:{r[2]}"
                    if key in edge_keys or rev in edge_keys:
                        continue
                    edge_keys.add(key)
                    edges.append({
                        "source": r[0], "target": r[1],
                        "relationship": r[2],
                        "strength": float(r[3]) if r[3] else 0.5,
                        "color": _RELATIONSHIP_COLORS.get(r[2], "#64748B"),
                    })

            # 1-hop expansion: find bridge actors that connect top actors
            if len(actor_ids) >= 2 and len(edges) < limit:
                bridge_rows = conn.execute(text("""
                    SELECT c1.actor_b AS bridge,
                           c1.actor_a AS from_actor, c2.actor_a AS to_actor,
                           c1.relationship AS rel1, c2.relationship AS rel2,
                           c1.strength AS s1, c2.strength AS s2,
                           (c1.strength + c2.strength) AS total_strength
                    FROM actor_connections c1
                    JOIN actor_connections c2
                        ON c1.actor_b = c2.actor_b AND c1.actor_a <> c2.actor_a
                    WHERE c1.actor_a = ANY(:ids) AND c2.actor_a = ANY(:ids)
                    AND c1.relationship NOT LIKE 'icij_%%'
                    AND c2.relationship NOT LIKE 'icij_%%'
                    AND c1.strength > 0.3 AND c2.strength > 0.3
                    ORDER BY total_strength DESC
                    LIMIT 40
                """), {"ids": actor_ids}).fetchall()

                bridge_ids: set[str] = set()
                for br in bridge_rows:
                    bridge_id = br[0]
                    bridge_ids.add(bridge_id)
                    for src, tgt, rel, s in [
                        (br[1], bridge_id, br[3], br[5]),
                        (bridge_id, br[2], br[4], br[6]),
                    ]:
                        key = f"{src}:{tgt}:{rel}"
                        rev = f"{tgt}:{src}:{rel}"
                        if key not in edge_keys and rev not in edge_keys:
                            edge_keys.add(key)
                            edges.append({
                                "source": src, "target": tgt,
                                "relationship": rel,
                                "strength": float(s) if s else 0.5,
                                "color": _RELATIONSHIP_COLORS.get(rel, "#64748B"),
                            })

                # Fetch bridge actor details
                if bridge_ids:
                    new_ids = list(bridge_ids - set(actor_ids))[:20]
                    if new_ids:
                        bridge_actors = conn.execute(text(
                            "SELECT id, name, category, tier, influence_score, "
                            "trust_score, net_worth_estimate, title, "
                            "provenance, provenance_as_of "
                            "FROM actors WHERE id = ANY(:ids)"
                        ), {"ids": new_ids}).fetchall()
                        for a in bridge_actors:
                            actor_ids.append(a[0])
                            bridge_node = {
                                "id": a[0], "name": a[1], "category": a[2],
                                "tier": a[3],
                                "influence": float(a[4]) if a[4] else 0.3,
                                "trust": float(a[5]) if a[5] else 0.5,
                                "net_worth": float(a[6]) if a[6] else None,
                                "title": a[7],
                                "bridge": True,
                                "degree": 0,
                            }
                            stamp_actor_node(bridge_node, a[0], stored=a[8], vintage=a[9])
                            nodes.append(bridge_node)

            # Wealth flows involving top actors (as source or target)
            flows = []
            if actor_ids:
                flow_rows = conn.execute(text("""
                    SELECT from_actor, to_entity, amount_estimate, confidence,
                           flow_date, implication
                    FROM wealth_flows
                    WHERE from_actor = ANY(:ids) OR to_entity = ANY(:ids)
                    ORDER BY amount_estimate DESC NULLS LAST
                    LIMIT 100
                """), {"ids": actor_ids}).fetchall()

                for f in flow_rows:
                    flows.append({
                        "from": f[0], "to": f[1],
                        "amount": float(f[2]) if f[2] else None,
                        "confidence": f[3],
                        "date": str(f[4]) if f[4] else None,
                        "implication": f[5],
                    })

            # Enrich nodes with sector_map data if available
            try:
                from analysis.sector_map import SECTOR_MAP
                ticker_to_sector: dict[str, str] = {}
                for sname, sdata in SECTOR_MAP.items():
                    for actor in sdata.get("actors", []):
                        t = actor.get("ticker", "").upper()
                        if t:
                            ticker_to_sector[t] = sname
                for node in nodes:
                    name_upper = node["name"].upper()
                    for ticker, sector in ticker_to_sector.items():
                        if ticker in name_upper:
                            node["sector"] = sector
                            node["ticker"] = ticker
                            break
            except ImportError:
                pass

            # Additional ticker lookup from company_profiles
            nodes_without_ticker = [n for n in nodes if not n.get("ticker") and n.get("category") in ("corporation", "company")]
            if nodes_without_ticker:
                names_for_lookup = [n["name"].replace(" (Expanded Profile)", "").strip() for n in nodes_without_ticker]
                try:
                    cp_rows = conn.execute(text(
                        "SELECT ticker, name FROM company_profiles WHERE name = ANY(:names)"
                    ), {"names": names_for_lookup}).fetchall()
                    cp_map = {r[1]: r[0] for r in cp_rows}
                    for n in nodes_without_ticker:
                        clean = n["name"].replace(" (Expanded Profile)", "").strip()
                        n["ticker"] = cp_map.get(clean)
                except Exception:
                    pass

            # Filter out nodes with no edges
            node_ids_in_edges = set()
            for e in edges:
                node_ids_in_edges.add(e["source"])
                node_ids_in_edges.add(e["target"])
            # Keep top actors even without edges (they have high influence)
            top_ids = {a[0] for a in top_actors[:20]}
            nodes = [n for n in nodes if n["id"] in node_ids_in_edges or n["id"] in top_ids]

            return {
                "nodes": nodes,
                "edges": edges,
                "flows": flows,
                "relationship_colors": _RELATIONSHIP_COLORS,
            }
    except Exception as exc:
        log.warning("Grand power map failed: {e}", e=str(exc))
        return {"nodes": [], "edges": [], "flows": [], "error": str(exc)}
