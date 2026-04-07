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


@router.get("/actor-network")
async def get_actor_network(
    limit: int = Query(2000, ge=10, le=50000, description="Max nodes for browser display"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the actor network graph for D3 force-directed visualization.

    The full actor dataset is kept in RAM (server has 512GB).
    Only the top N nodes by influence are returned for browser rendering.
    Cached for 60 minutes to avoid repeated DB queries.
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)

    # Build/refresh the full graph in memory (cached for 60min)
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
                    "error": str(exc),
                }

    full = _actor_graph_cache["data"]

    # Return top N nodes by influence for browser rendering
    nodes = full.get("nodes", [])
    total = len(nodes)
    if len(nodes) > limit:
        nodes = sorted(nodes, key=lambda n: n.get("influence", 0), reverse=True)[:limit]
        kept_ids = {n["id"] for n in nodes}
        links = [l for l in full.get("links", [])
                 if l.get("source") in kept_ids and l.get("target") in kept_ids]
    else:
        links = full.get("links", [])

    return {
        "nodes": nodes,
        "links": links,
        "metadata": {**full.get("metadata", {}), "total_actors": total, "returned": len(nodes)},
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
