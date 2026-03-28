"""Cross-reference intelligence endpoints — lie detector for government statistics."""

from __future__ import annotations

import time
from dataclasses import asdict
from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/intelligence", tags=["intelligence"])


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

        result = {
            **graph,
            "wealth_flows": wealth_flows,
            "pocket_lining_alerts": pocket_lining_alerts,
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


@router.get("/cross-reference")
async def get_cross_reference(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Run all cross-reference checks and return the LieDetectorReport.

    Compares government statistics against physical reality indicators
    across GDP, trade, inflation, central bank, and employment categories.
    Red flags indicate where official data diverges from ground truth.
    """
    try:
        from intelligence.cross_reference import run_all_checks

        engine = get_db_engine()
        report = run_all_checks(engine)
        return {
            "checks": [asdict(c) for c in report.checks],
            "red_flags": [asdict(c) for c in report.red_flags],
            "narrative": report.narrative,
            "summary": report.summary,
            "generated_at": report.generated_at,
        }
    except Exception as exc:
        log.warning("Cross-reference engine failed: {e}", e=str(exc))
        return {
            "checks": [],
            "red_flags": [],
            "narrative": f"Cross-reference engine error: {exc}",
            "summary": {},
            "generated_at": None,
            "error": str(exc),
        }


@router.get("/cross-reference/category/{category}")
async def get_cross_reference_by_category(
    category: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Run cross-reference checks for a specific category.

    Valid categories: gdp, trade, inflation, central_bank, employment.
    """
    try:
        from intelligence.cross_reference import (
            check_gdp_vs_physical,
            check_trade_bilateral,
            check_inflation_vs_inputs,
            check_central_bank_actions_vs_words,
            check_employment_reality,
        )

        engine = get_db_engine()

        category_map = {
            "gdp": lambda: (
                check_gdp_vs_physical(engine, "US")
                + check_gdp_vs_physical(engine, "CN")
                + check_gdp_vs_physical(engine, "EU")
            ),
            "trade": lambda: check_trade_bilateral(engine),
            "inflation": lambda: check_inflation_vs_inputs(engine),
            "central_bank": lambda: check_central_bank_actions_vs_words(engine),
            "employment": lambda: check_employment_reality(engine),
        }

        check_fn = category_map.get(category.lower())
        if check_fn is None:
            return {
                "error": f"Unknown category '{category}'. "
                f"Valid: {', '.join(category_map.keys())}",
                "checks": [],
            }

        checks = check_fn()
        red_flags = [
            c for c in checks
            if c.assessment in ("major_divergence", "contradiction")
        ]

        return {
            "category": category,
            "checks": [asdict(c) for c in checks],
            "red_flags": [asdict(c) for c in red_flags],
            "total": len(checks),
            "red_flag_count": len(red_flags),
        }
    except Exception as exc:
        log.warning("Cross-reference category {c} failed: {e}", c=category, e=str(exc))
        return {"category": category, "checks": [], "error": str(exc)}


@router.get("/cross-reference/ticker/{ticker}")
async def get_cross_reference_for_ticker(
    ticker: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return cross-reference checks relevant to a specific ticker.

    Maps tickers to the categories and country-specific checks that
    affect them. E.g., EEM maps to EM GDP vs physical + trade flows.
    """
    try:
        from intelligence.cross_reference import get_cross_ref_for_ticker

        engine = get_db_engine()
        return get_cross_ref_for_ticker(engine, ticker)
    except Exception as exc:
        log.warning("Ticker cross-ref {t} failed: {e}", t=ticker, e=str(exc))
        return {"ticker": ticker, "mapped": False, "checks": [], "error": str(exc)}


@router.get("/cross-reference/history")
async def get_cross_reference_history(
    category: str | None = Query(None, description="Filter by category"),
    days: int = Query(30, ge=1, le=365, description="Lookback days"),
    assessment: str | None = Query(None, description="Filter by assessment level"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Fetch historical cross-reference checks for trend analysis.

    Shows how divergences have evolved over time, enabling detection
    of persistent vs transient inconsistencies.
    """
    try:
        from intelligence.cross_reference import get_historical_checks

        engine = get_db_engine()
        records = get_historical_checks(engine, category, days, assessment)
        return {
            "records": records,
            "count": len(records),
            "filters": {
                "category": category,
                "days": days,
                "assessment": assessment,
            },
        }
    except Exception as exc:
        log.warning("Cross-reference history failed: {e}", e=str(exc))
        return {"records": [], "count": 0, "error": str(exc)}


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


# ── Source Audit Endpoints ───────────────────────────────────────────────


@router.get("/source-audit")
async def get_source_audit(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the latest source audit results.

    Returns recent accuracy comparisons, active discrepancies,
    redundancy map size, and single-source feature count without
    re-running the full audit.
    """
    try:
        from intelligence.source_audit import get_latest_audit_summary

        engine = get_db_engine()
        return get_latest_audit_summary(engine)
    except Exception as exc:
        log.warning("Source audit summary failed: {e}", e=str(exc))
        return {
            "recent_accuracy": [],
            "recent_discrepancies": [],
            "redundancy_map_size": 0,
            "single_source_count": 0,
            "error": str(exc),
        }


@router.post("/source-audit/run")
async def trigger_source_audit(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Trigger a full source accuracy audit.

    Builds the redundancy map, compares all redundant sources pairwise,
    detects discrepancies, ranks sources, and optionally updates
    source_catalog priorities.
    """
    try:
        from intelligence.source_audit import run_full_audit, update_source_priorities

        engine = get_db_engine()
        report = run_full_audit(engine)
        priority_changes = update_source_priorities(engine, report)
        report["priority_changes"] = priority_changes
        return report
    except Exception as exc:
        log.warning("Full source audit failed: {e}", e=str(exc))
        return {"error": str(exc)}


@router.get("/source-audit/redundancy-map")
async def get_redundancy_map(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the current redundancy map showing features with 2+ sources."""
    try:
        from intelligence.source_audit import build_redundancy_map

        engine = get_db_engine()
        rmap = build_redundancy_map(engine)
        return {
            "redundancy_map": rmap,
            "total_redundant_features": len(rmap),
        }
    except Exception as exc:
        log.warning("Redundancy map failed: {e}", e=str(exc))
        return {"redundancy_map": {}, "error": str(exc)}


@router.get("/source-audit/compare/{feature_name}")
async def compare_feature_sources(
    feature_name: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Compare all sources for a specific feature and return accuracy rankings."""
    try:
        from intelligence.source_audit import compare_sources

        engine = get_db_engine()
        return compare_sources(engine, feature_name)
    except Exception as exc:
        log.warning(
            "Source comparison for {f} failed: {e}", f=feature_name, e=str(exc),
        )
        return {"feature_name": feature_name, "error": str(exc)}


@router.get("/source-audit/discrepancies")
async def get_discrepancies(
    threshold: float = Query(0.02, ge=0.001, le=0.5, description="Deviation threshold"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Detect features where sources currently disagree beyond threshold."""
    try:
        from intelligence.source_audit import detect_discrepancies

        engine = get_db_engine()
        discs = detect_discrepancies(engine, threshold=threshold)
        return {
            "discrepancies": discs,
            "count": len(discs),
            "threshold": threshold,
        }
    except Exception as exc:
        log.warning("Discrepancy detection failed: {e}", e=str(exc))
        return {"discrepancies": [], "count": 0, "error": str(exc)}


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


# ── Unified Intelligence Dashboard ──────────────────────────────────────

# Simple in-memory cache: (timestamp, data)
_dashboard_cache: dict[str, tuple[float, dict]] = {}
_DASHBOARD_CACHE_TTL = 600  # 10 minutes


def _build_dashboard_snapshot() -> dict[str, Any]:
    """Assemble a unified intelligence snapshot from all subsystems.

    Calls each intelligence module, catches failures individually so
    partial data is still returned, and computes an overall confidence.
    """
    from datetime import datetime, timezone

    engine = get_db_engine()
    snapshot: dict[str, Any] = {
        "trust": {"top_sources": [], "convergence_events": []},
        "levers": {"active_events": [], "top_pullers": []},
        "cross_ref": {"red_flags": [], "total_checks": 0},
        "source_audit": {"discrepancies": [], "single_points_of_failure": 0},
        "postmortems": {"recent_failures": [], "lessons": ""},
        "overall_confidence": 0.5,
        "narrative": "",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "errors": [],
    }

    confidence_components: list[float] = []

    # ── Trust & Convergence ──────────────────────────────────────────
    try:
        from intelligence.trust_scorer import update_trust_scores, detect_convergence

        trust_data = update_trust_scores(engine)
        all_sources = trust_data.get("sources", [])
        top_5 = all_sources[:5]
        convergence = detect_convergence(engine)

        snapshot["trust"] = {
            "top_sources": top_5,
            "convergence_events": convergence[:10],
            "total_tracked": len(all_sources),
        }

        # Confidence contribution: high if many sources, strong convergence
        if all_sources:
            avg_trust = sum(s.get("trust_score", 0.5) for s in top_5) / len(top_5) if top_5 else 0.5
            confidence_components.append(avg_trust)
        if convergence:
            confidence_components.append(min(0.9, 0.5 + 0.1 * len(convergence)))
    except Exception as exc:
        log.warning("Dashboard: trust module failed: {e}", e=str(exc))
        snapshot["errors"].append(f"trust: {exc}")

    # ── Lever Pullers ────────────────────────────────────────────────
    try:
        from intelligence.lever_pullers import (
            get_active_lever_events,
            identify_lever_pullers,
            find_lever_convergence,
        )

        events = get_active_lever_events(engine, days=14)
        pullers = identify_lever_pullers(engine)
        lever_convergence = find_lever_convergence(engine)

        event_dicts = []
        for ev in events[:10]:
            event_dicts.append({
                "puller_name": ev.puller.name,
                "category": ev.puller.category,
                "action": ev.action,
                "tickers": ev.tickers,
                "timestamp": ev.timestamp,
                "motivation": ev.motivation_assessment,
                "confidence": ev.confidence,
            })

        puller_dicts = []
        for p in pullers[:5]:
            puller_dicts.append({
                "name": p.name,
                "category": p.category,
                "trust_score": p.trust_score,
                "influence_rank": p.influence_rank,
                "position": p.position,
            })

        snapshot["levers"] = {
            "active_events": event_dicts,
            "top_pullers": puller_dicts,
            "convergence": lever_convergence[:5],
        }

        if events:
            avg_conf = sum(ev.confidence for ev in events[:10]) / min(len(events), 10)
            confidence_components.append(avg_conf)
    except Exception as exc:
        log.warning("Dashboard: lever module failed: {e}", e=str(exc))
        snapshot["errors"].append(f"levers: {exc}")

    # ── Cross-Reference (Lie Detector) ───────────────────────────────
    try:
        from intelligence.cross_reference import run_all_checks

        report = run_all_checks(engine)
        red_flags = [asdict(c) for c in report.red_flags[:10]]

        snapshot["cross_ref"] = {
            "red_flags": red_flags,
            "total_checks": len(report.checks),
            "summary": getattr(report, "summary", {}),
        }

        # More red flags = lower confidence
        flag_ratio = len(report.red_flags) / max(len(report.checks), 1)
        confidence_components.append(max(0.2, 1.0 - flag_ratio * 2))
    except Exception as exc:
        log.warning("Dashboard: cross-ref module failed: {e}", e=str(exc))
        snapshot["errors"].append(f"cross_ref: {exc}")

    # ── Source Audit ─────────────────────────────────────────────────
    try:
        from intelligence.source_audit import get_latest_audit_summary

        audit = get_latest_audit_summary(engine)

        snapshot["source_audit"] = {
            "discrepancies": audit.get("recent_discrepancies", [])[:10],
            "single_points_of_failure": audit.get("single_source_count", 0),
            "redundancy_map_size": audit.get("redundancy_map_size", 0),
            "recent_accuracy": audit.get("recent_accuracy", [])[:5],
        }

        disc_count = len(audit.get("recent_discrepancies", []))
        if disc_count > 5:
            confidence_components.append(0.4)
        elif disc_count > 0:
            confidence_components.append(0.7)
        else:
            confidence_components.append(0.85)
    except Exception as exc:
        log.warning("Dashboard: source audit failed: {e}", e=str(exc))
        snapshot["errors"].append(f"source_audit: {exc}")

    # ── Post-Mortems ─────────────────────────────────────────────────
    try:
        from intelligence.postmortem import load_postmortems

        records = load_postmortems(engine, days=30)
        recent = records[:5]

        lessons = ""
        if recent:
            try:
                from intelligence.postmortem import generate_lessons_learned, PostMortem
                pms = []
                for r in records[:10]:
                    full = r.get("full_analysis", {})
                    if full:
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
                                failure_category=full.get("failure_category", ""),
                                root_cause=full.get("root_cause", ""),
                                which_signals_were_wrong=full.get("which_signals_were_wrong", []),
                                which_signals_were_right=full.get("which_signals_were_right", []),
                                what_we_missed=full.get("what_we_missed", ""),
                                recommended_fix=full.get("recommended_fix", ""),
                                confidence_in_analysis=full.get("confidence_in_analysis", 0.5),
                                generated_at=full.get("generated_at", ""),
                            ))
                        except Exception:
                            continue
                if pms:
                    lessons = generate_lessons_learned(engine, pms)
            except Exception:
                pass

        snapshot["postmortems"] = {
            "recent_failures": recent,
            "lessons": lessons,
            "total_count": len(records),
        }
    except Exception as exc:
        log.warning("Dashboard: postmortem module failed: {e}", e=str(exc))
        snapshot["errors"].append(f"postmortems: {exc}")

    # ── Overall Confidence ───────────────────────────────────────────
    if confidence_components:
        snapshot["overall_confidence"] = round(
            sum(confidence_components) / len(confidence_components), 3
        )
    else:
        snapshot["overall_confidence"] = 0.5

    # ── Narrative ────────────────────────────────────────────────────
    try:
        narrative_parts = []
        conf = snapshot["overall_confidence"]
        if conf >= 0.7:
            narrative_parts.append(
                f"System confidence is HIGH at {conf:.0%}."
            )
        elif conf >= 0.5:
            narrative_parts.append(
                f"System confidence is MODERATE at {conf:.0%}."
            )
        else:
            narrative_parts.append(
                f"System confidence is LOW at {conf:.0%} — review red flags."
            )

        trust_data = snapshot["trust"]
        if trust_data["convergence_events"]:
            n = len(trust_data["convergence_events"])
            narrative_parts.append(
                f"{n} convergence event(s) detected — multiple sources agree."
            )

        red_flags = snapshot["cross_ref"]["red_flags"]
        if red_flags:
            categories = set(f.get("category", "unknown") for f in red_flags)
            narrative_parts.append(
                f"{len(red_flags)} cross-reference red flag(s) in: {', '.join(categories)}."
            )

        spof = snapshot["source_audit"]["single_points_of_failure"]
        if spof > 10:
            narrative_parts.append(
                f"Warning: {spof} features rely on a single data source."
            )

        pm_count = snapshot["postmortems"].get("total_count", 0)
        if pm_count > 0:
            narrative_parts.append(
                f"{pm_count} post-mortem(s) in the last 30 days — check lessons learned."
            )

        snapshot["narrative"] = " ".join(narrative_parts)
    except Exception as exc:
        snapshot["narrative"] = f"Narrative generation failed: {exc}"

    return snapshot


@router.get("/dashboard")
async def get_intelligence_dashboard(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Unified intelligence dashboard — all systems at a glance.

    Returns a combined snapshot of trust scores, convergence alerts,
    lever-puller activity, cross-reference red flags, source audit status,
    and post-mortem lessons. Cached for 10 minutes.
    """
    now = time.time()
    cache_key = "intel_dashboard"

    if cache_key in _dashboard_cache:
        cached_at, cached_data = _dashboard_cache[cache_key]
        if now - cached_at < _DASHBOARD_CACHE_TTL:
            return cached_data

    try:
        snapshot = _build_dashboard_snapshot()
        _dashboard_cache[cache_key] = (now, snapshot)
        return snapshot
    except Exception as exc:
        log.error("Intelligence dashboard build failed: {e}", e=str(exc))
        return {
            "trust": {"top_sources": [], "convergence_events": []},
            "levers": {"active_events": [], "top_pullers": []},
            "cross_ref": {"red_flags": [], "total_checks": 0},
            "source_audit": {"discrepancies": [], "single_points_of_failure": 0},
            "postmortems": {"recent_failures": [], "lessons": ""},
            "overall_confidence": 0.5,
            "narrative": f"Dashboard build error: {exc}",
            "generated_at": None,
            "error": str(exc),
        }
