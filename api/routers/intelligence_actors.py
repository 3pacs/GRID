"""Intelligence sub-router: Actor network, post-mortems, and trend endpoints."""

from __future__ import annotations

import time
from dataclasses import asdict
from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(tags=["intelligence"])


# ── Actor Network Endpoints ──────────────────────────────────────────────

_actor_graph_cache: dict[str, Any] = {"data": None, "ts": None}
_ACTOR_GRAPH_TTL = 1800  # 30 minutes


_DEFAULT_SECTORS = [
    "AI", "Crypto", "Robotics", "Gene Therapy & Pharma", "Nuclear",
    "Energy", "Metals", "Semiconductors", "Tech",
]


@router.get("/actor-network")
async def get_actor_network(
    limit: int = Query(500, ge=10, le=50000, description="Max nodes for browser display"),
    sector: str | None = Query(None, description="Filter to a single sector"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the actor network graph for D3 visualization.

    ICIJ bulk data (1.6M offshore leak records) is excluded by default.
    Nodes are tagged with a ``sector`` field and the initial view focuses
    on user-priority sectors (AI, Crypto, Semiconductors, etc.).
    Use ``?sector=AI`` to drill into a single sector.
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)

    # Build/refresh graph (ICIJ excluded, ~5K actors instead of 1.6M)
    if (
        not _actor_graph_cache["data"]
        or not _actor_graph_cache["ts"]
        or (now - _actor_graph_cache["ts"]).total_seconds() >= _ACTOR_GRAPH_TTL
    ):
        try:
            _build_full_actor_cache(now)
        except Exception as exc:
            log.warning("Actor network build failed: {e}", e=str(exc))
            if not _actor_graph_cache["data"]:
                return {
                    "nodes": [], "links": [], "metadata": {},
                    "wealth_flows": [], "pocket_lining_alerts": [],
                    "flows": [], "circular_flows": [],
                    "flow_summary": {"total_tracked": "$0", "top_flow": None, "active_loops": 0},
                    "sectors": [],
                    "error": str(exc),
                }

    full = _actor_graph_cache["data"]
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
    total = len(nodes)
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


def _build_full_actor_cache(now=None):
    """Build the full actor graph and store in RAM. Called at startup and on cache miss."""
    from datetime import datetime, timezone

    if now is None:
        now = datetime.now(timezone.utc)

    from intelligence.actor_network import (
        build_actor_graph,
        track_wealth_migration,
        assess_pocket_lining,
    )

    engine = get_db_engine()
    graph = build_actor_graph(engine)
    log.info("Actor network loaded: {n} nodes, {l} links",
             n=len(graph.get("nodes", [])), l=len(graph.get("links", [])))

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

    result = {
        **graph,
        "wealth_flows": wealth_flows,
        "pocket_lining_alerts": pocket_lining_alerts,
        "flows": money_flows,
        "circular_flows": circular_flows_data,
        "flow_summary": flow_summary,
    }
    _actor_graph_cache["data"] = result
    _actor_graph_cache["ts"] = now
    log.info("Actor network cached: {s:.1f}MB in RAM",
             s=len(str(result)) / 1_000_000)


@router.get("/actor/{actor_id}")
async def get_actor_detail(
    actor_id: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return detailed information about a single actor.

    Includes recent actions, known positions, connections, and any
    pocket-lining alerts involving this actor.
    """
    try:
        from intelligence.actor_network import (
            build_actor_graph,
            find_connected_actions,
            assess_pocket_lining,
        )

        engine = get_db_engine()
        graph = build_actor_graph(engine)

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
async def get_actor_analytics_endpoint(
    actor_id: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return precomputed graph analytics for a single actor.

    Includes PageRank, community ID, betweenness, eigenvector, degree
    centrality, hub score, and authority score.
    """
    try:
        from store.graph import get_actor_analytics

        engine = get_db_engine()
        result = get_actor_analytics(actor_id, engine=engine)
        if result is None:
            return {"error": f"No analytics found for actor '{actor_id}'", "analytics": None}
        return {"analytics": result}
    except Exception as exc:
        log.warning("Actor analytics for {a} failed: {e}", a=actor_id, e=str(exc))
        return {"analytics": None, "error": str(exc)}


@router.get("/analytics/top")
async def get_top_actors_endpoint(
    metric: str = Query("pagerank", description="Metric to rank by"),
    limit: int = Query(20, ge=1, le=200, description="Number of actors to return"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return top actors ranked by any analytics metric.

    Allowed metrics: pagerank, betweenness, eigenvector,
    degree_centrality, hub_score, authority_score.
    """
    try:
        from store.graph import get_top_actors

        engine = get_db_engine()
        actors = get_top_actors(metric=metric, limit=limit, engine=engine)
        return {"actors": actors, "metric": metric, "count": len(actors)}
    except ValueError as exc:
        return {"actors": [], "metric": metric, "count": 0, "error": str(exc)}
    except Exception as exc:
        log.warning("Top actors query failed: {e}", e=str(exc))
        return {"actors": [], "metric": metric, "count": 0, "error": str(exc)}


@router.get("/analytics/communities")
async def get_communities_endpoint(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return list of all communities with member counts and top member."""
    try:
        from store.graph import get_community_list

        engine = get_db_engine()
        communities = get_community_list(engine=engine)
        return {"communities": communities, "count": len(communities)}
    except Exception as exc:
        log.warning("Community list failed: {e}", e=str(exc))
        return {"communities": [], "count": 0, "error": str(exc)}


@router.get("/analytics/community/{community_id}")
async def get_community_members_endpoint(
    community_id: int,
    limit: int = Query(50, ge=1, le=500, description="Max members to return"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return all actors in a community, ordered by PageRank."""
    try:
        from store.graph import get_community_members

        engine = get_db_engine()
        members = get_community_members(community_id, limit=limit, engine=engine)
        return {"community_id": community_id, "members": members, "count": len(members)}
    except Exception as exc:
        log.warning("Community members for {c} failed: {e}", c=community_id, e=str(exc))
        return {"community_id": community_id, "members": [], "count": 0, "error": str(exc)}


# ── Post-Mortem Endpoints ─────────────────────────────────────────────────


@router.get("/postmortems")
async def get_postmortems(
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
async def trigger_batch_postmortem(
    days: int = Query(30, ge=1, le=365, description="Lookback days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Trigger batch post-mortem generation for all recent failures.

    Generates post-mortems for all failed trades and missed predictions
    in the lookback window that do not already have a post-mortem.
    Returns a summary with the generated post-mortems.
    """
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
async def get_lessons_learned(
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

@router.get("/milestones/scorecard")
async def get_milestone_scorecard(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return execution scorecard for all tracked companies.

    Scores each company on beat/miss rate, trend, streaks.
    """
    try:
        from intelligence.milestone_tracker import scan_all_tickers
        engine = get_db_engine()
        results = scan_all_tickers(engine)
        return {"companies": results, "count": len(results)}
    except Exception as exc:
        log.warning("Milestone scorecard failed: {e}", e=str(exc))
        return {"companies": [], "count": 0, "error": str(exc)}


@router.get("/milestones/{ticker}")
async def get_ticker_milestones(
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
async def get_attention_alerts(
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
async def get_actor_network_db(
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
            source_filter = "" if include_icij else "AND a.source NOT LIKE 'icij%%'"
            actors = conn.execute(text(f"""
                SELECT a.id, a.name, a.tier, a.category, a.degree,
                       a.influence_score, a.title, a.source,
                       a.metadata->'llm_profile'->>'title' as llm_title,
                       a.metadata->'llm_profile'->>'confidence' as llm_confidence
                FROM actors a
                WHERE a.degree >= :min_deg {source_filter}
                ORDER BY a.degree DESC
                LIMIT :lim
            """), {"min_deg": min_degree, "lim": limit}).fetchall()

            actor_ids = {a[0] for a in actors}
            nodes = [
                {
                    "id": a[0], "label": a[1], "tier": a[2], "category": a[3],
                    "degree": a[4], "influence": float(a[5] or 0),
                    "title": a[8] or a[6] or "", "source": a[7],
                    "llm_confidence": a[9],
                }
                for a in actors
            ]

            # Get connections between these actors
            id_list = ",".join(f"'{a[0]}'" for a in actors)
            if not id_list:
                return {"nodes": [], "links": [], "stats": {}}

            links_raw = conn.execute(text(f"""
                SELECT actor_a, actor_b, relationship, strength
                FROM actor_connections
                WHERE actor_a IN ({id_list}) AND actor_b IN ({id_list})
                LIMIT 5000
            """)).fetchall()

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
async def get_actor_enriched_profile(
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
                "trust_score, degree, source, metadata, credibility "
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
                    "degree": actor[7], "source": actor[8],
                    "credibility": actor[10],
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
async def get_trends(
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
}


@router.get("/power-map/{sector_name}")
async def get_sector_power_map(
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
                    "net_worth_estimate, title, known_positions "
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
                "net_worth_estimate, title, known_positions "
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
        }

    if not actor_map:
        # Fallback: build from sector_map data alone (no DB matches)
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
    connected_ids: set[str] = set()
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
                    "net_worth_estimate, title "
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
            }

    nodes = []
    for actor in actor_map.values():
        name_lower = actor["name"].lower()
        node = {
            **actor,
            "ticker": actor.get("ticker") or ticker_by_name.get(name_lower),
            "subsector": actor.get("subsector") or subsector_by_name.get(name_lower),
        }
        # Use sector_map influence if available (more curated)
        if name_lower in influence_by_name:
            node["influence"] = influence_by_name[name_lower]
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
async def ego_graph_search(
    q: str = Query(..., min_length=1, description="Search query (name or ticker)"),
    limit: int = Query(20, ge=1, le=100),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Search for actors by name or ticker for the ego-graph."""
    from sqlalchemy import text
    engine = get_db_engine()

    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, name, category, tier, influence_score, title
                FROM actors
                WHERE category NOT IN ('icij_entity', 'icij_officer', 'icij_intermediary')
                AND (name ILIKE :pat OR name ILIKE :paren)
                ORDER BY influence_score DESC NULLS LAST
                LIMIT :lim
            """), {"pat": f"%{q}%", "paren": f"%({q.upper()})%", "lim": limit}).fetchall()

            return {
                "results": [
                    {
                        "id": r[0], "name": r[1], "category": r[2],
                        "tier": r[3], "influence": float(r[4]) if r[4] else 0.5,
                        "title": r[5],
                    }
                    for r in rows
                ],
            }
    except Exception as exc:
        log.warning("Ego-graph search failed: {e}", e=str(exc))
        return {"results": [], "error": str(exc)}


@router.get("/ego-graph/{actor_id}")
async def get_ego_graph(
    actor_id: str,
    depth: int = Query(2, ge=1, le=3, description="Hop depth (1-3)"),
    max_nodes: int = Query(80, ge=10, le=300, description="Max nodes to return"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return ego-graph centered on one actor with N-degree connections.

    Degree 1: direct connections.
    Degree 2: connections-of-connections.
    Degree 3: 3-hop neighborhood.

    Nodes are tagged with ``ring`` (0=center, 1=first hop, etc.)
    for concentric layout in the frontend.
    """
    from sqlalchemy import text
    engine = get_db_engine()

    try:
        with engine.connect() as conn:
            # Verify center actor exists
            center = conn.execute(text(
                "SELECT id, name, category, tier, influence_score, trust_score, "
                "net_worth_estimate, title, known_positions "
                "FROM actors WHERE id = :id"
            ), {"id": actor_id}).fetchone()

            if not center:
                return {"nodes": [], "edges": [], "error": f"Actor '{actor_id}' not found"}

            # BFS expansion ring by ring
            actor_map: dict[str, dict] = {}
            ring_map: dict[str, int] = {}
            all_edges: list[dict] = []
            edge_keys: set[str] = set()

            def _add_actor(row, ring: int) -> None:
                aid = row[0]
                if aid in actor_map:
                    return
                actor_map[aid] = {
                    "id": aid, "name": row[1], "category": row[2],
                    "tier": row[3],
                    "influence": float(row[4]) if row[4] else 0.5,
                    "trust": float(row[5]) if row[5] else 0.5,
                    "net_worth": float(row[6]) if row[6] else None,
                    "title": row[7],
                    "ring": ring,
                }
                ring_map[aid] = ring

            _add_actor(center, 0)

            frontier = {actor_id}
            for ring in range(1, depth + 1):
                if not frontier or len(actor_map) >= max_nodes:
                    break

                # Find connections from/to frontier
                frontier_list = list(frontier)
                rows = conn.execute(text("""
                    SELECT actor_a, actor_b, relationship, strength
                    FROM actor_connections
                    WHERE (actor_a = ANY(:ids) OR actor_b = ANY(:ids))
                    AND relationship NOT LIKE 'icij_%%'
                    AND strength > 0.2
                    ORDER BY strength DESC
                    LIMIT :lim
                """), {"ids": frontier_list, "lim": max_nodes * 3}).fetchall()

                next_frontier: set[str] = set()
                for r in rows:
                    a, b, rel, strength = r[0], r[1], r[2], r[3]
                    key = f"{a}:{b}:{rel}"
                    rev = f"{b}:{a}:{rel}"
                    if key in edge_keys or rev in edge_keys:
                        continue
                    edge_keys.add(key)
                    all_edges.append({
                        "source": a, "target": b,
                        "relationship": rel,
                        "strength": float(strength) if strength else 0.5,
                        "color": _RELATIONSHIP_COLORS.get(rel, "#64748B"),
                    })
                    # Track new actor IDs for next ring
                    for nid in (a, b):
                        if nid not in actor_map:
                            next_frontier.add(nid)

                # Fetch details for new actors
                if next_frontier and len(actor_map) < max_nodes:
                    remaining = max_nodes - len(actor_map)
                    new_ids = list(next_frontier)[:remaining]
                    new_rows = conn.execute(text(
                        "SELECT id, name, category, tier, influence_score, trust_score, "
                        "net_worth_estimate, title, known_positions "
                        "FROM actors WHERE id = ANY(:ids)"
                    ), {"ids": new_ids}).fetchall()
                    for nr in new_rows:
                        _add_actor(nr, ring)

                frontier = next_frontier & set(actor_map.keys())

            # Filter edges to only include nodes we have
            valid_ids = set(actor_map.keys())
            edges = [e for e in all_edges if e["source"] in valid_ids and e["target"] in valid_ids]

            nodes = list(actor_map.values())

            return {
                "nodes": nodes,
                "edges": edges,
                "center": actor_id,
                "depth": depth,
                "relationship_colors": _RELATIONSHIP_COLORS,
            }
    except Exception as exc:
        log.warning("Ego-graph for {a} failed: {e}", a=actor_id, e=str(exc))
        return {"nodes": [], "edges": [], "error": str(exc)}


# ── Grand Power Map ────────────────────────────────────────────────────────


@router.get("/grand-power-map")
async def get_grand_power_map(
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
                       trust_score, net_worth_estimate, title, known_positions, degree
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
                nodes.append({
                    "id": a[0], "name": a[1], "category": a[2],
                    "tier": a[3],
                    "influence": float(a[4]) if a[4] else 0.5,
                    "trust": float(a[5]) if a[5] else 0.5,
                    "net_worth": float(a[6]) if a[6] else None,
                    "title": a[7],
                    "degree": int(a[9]) if len(a) > 9 else 0,
                })

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
                            "trust_score, net_worth_estimate, title "
                            "FROM actors WHERE id = ANY(:ids)"
                        ), {"ids": new_ids}).fetchall()
                        for a in bridge_actors:
                            actor_ids.append(a[0])
                            nodes.append({
                                "id": a[0], "name": a[1], "category": a[2],
                                "tier": a[3],
                                "influence": float(a[4]) if a[4] else 0.3,
                                "trust": float(a[5]) if a[5] else 0.5,
                                "net_worth": float(a[6]) if a[6] else None,
                                "title": a[7],
                                "bridge": True,
                                "degree": 0,
                            })

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
