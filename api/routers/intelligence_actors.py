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
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the full actor network graph for D3 force-directed visualization.

    Includes nodes (actors), links (connections), wealth flows, and
    pocket-lining alerts.  Cached for 30 minutes.
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    if (
        _actor_graph_cache["data"]
        and _actor_graph_cache["ts"]
        and (now - _actor_graph_cache["ts"]).total_seconds() < _ACTOR_GRAPH_TTL
    ):
        return _actor_graph_cache["data"]

    try:
        from intelligence.actor_network import (
            build_actor_graph,
            track_wealth_migration,
            assess_pocket_lining,
        )

        engine = get_db_engine()
        graph = build_actor_graph(engine)

        # Wealth flows
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
            wealth_flows = []

        # Pocket-lining alerts
        try:
            pocket_lining_alerts = assess_pocket_lining(engine)
        except Exception as exc:
            log.debug("Pocket-lining detection failed: {e}", e=str(exc))
            pocket_lining_alerts = []

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

            # Build influence graph and extract typed flows from links
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

                # Map influence_network link types to flow categories
                if flow_type == "contribution":
                    ftype = "campaign"
                elif flow_type == "lobbying":
                    ftype = "lobbying"
                elif flow_type == "contract":
                    ftype = "contract"
                elif flow_type == "trade":
                    ftype = "stock_trade"
                else:
                    ftype = flow_type or "unknown"

                money_flows.append({
                    "from": link.get("source", ""),
                    "to": link.get("target", ""),
                    "amount": amount_val,
                    "type": ftype,
                    "date": link.get("date", ""),
                    "label": link.get("label", ""),
                })

            # Detect circular flows
            try:
                loops = detect_circular_flows(engine)
                for loop in loops:
                    loop_dict = loop.to_dict()
                    circular_flows_data.append(loop_dict)
            except Exception as exc:
                log.debug("Circular flow detection failed: {e}", e=str(exc))

            # Dollar flows table — direct actor-to-actor flows
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

            # Build flow summary
            total_tracked = sum(
                abs(f.get("amount", 0)) for f in money_flows
                if isinstance(f.get("amount"), (int, float))
            )
            top_flow = max(
                money_flows,
                key=lambda f: abs(f.get("amount", 0)) if isinstance(f.get("amount"), (int, float)) else 0,
                default=None,
            )
            active_loops = sum(
                1 for c in circular_flows_data if c.get("circular_flow_detected")
            )

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
        return result

    except Exception as exc:
        log.warning("Actor network build failed: {e}", e=str(exc))
        return {
            "nodes": [],
            "links": [],
            "metadata": {},
            "wealth_flows": [],
            "pocket_lining_alerts": [],
            "flows": [],
            "circular_flows": [],
            "flow_summary": {"total_tracked": "$0", "top_flow": None, "active_loops": 0},
            "error": str(exc),
        }


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
