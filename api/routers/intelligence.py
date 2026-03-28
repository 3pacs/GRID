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


# ── Risk Map ─────────────────────────────────────────────────────────────


_risk_map_cache: dict[str, Any] = {"data": None, "ts": None}
_RISK_MAP_TTL = 300  # 5 minutes


def _compute_risk_level(score: float) -> str:
    """Map a 0-1 risk score to a human-readable level."""
    if score >= 0.85:
        return "critical"
    if score >= 0.65:
        return "high"
    if score >= 0.45:
        return "elevated"
    if score >= 0.25:
        return "moderate"
    return "low"


def _level_to_score(level: str) -> float:
    """Map a risk level label back to a numeric score."""
    return {"critical": 0.95, "high": 0.75, "elevated": 0.55, "moderate": 0.35, "low": 0.15}.get(level, 0.5)


def _build_risk_map() -> dict[str, Any]:
    """Assemble the full risk map from dealer gamma, volatility, concentration,
    correlation, credit, and liquidity sub-systems."""
    from datetime import datetime, timezone, date as _date, timedelta

    engine = get_db_engine()
    errors: list[str] = []
    sub_scores: list[float] = []

    # ── 1. Dealer Risk ──────────────────────────────────────────────
    dealer_risk: dict[str, Any] = {
        "gex_regime": "unknown",
        "net_gex": 0,
        "gamma_flip_distance_pct": 0,
        "vanna_direction": "neutral",
        "charm_direction": "neutral",
        "days_to_opex": 30,
        "risk_level": "moderate",
    }
    try:
        from physics.dealer_gamma import DealerGammaEngine

        gex_engine = DealerGammaEngine(engine)
        spy_gex = gex_engine.compute_gex_profile("SPY")

        if "error" not in spy_gex:
            regime = spy_gex.get("regime", "NEUTRAL").lower()
            net_gex = spy_gex.get("gex_aggregate", 0)
            spot = spy_gex.get("spot", 0)
            gamma_flip = spy_gex.get("gamma_flip")
            vanna = spy_gex.get("vanna_exposure", 0)
            charm = spy_gex.get("charm_exposure", 0)

            flip_dist_pct = 0.0
            if gamma_flip and spot > 0:
                flip_dist_pct = round((gamma_flip - spot) / spot * 100, 2)

            # Find nearest OPEX (third Friday)
            today = _date.today()
            days_to_opex = 30
            for d_offset in range(45):
                candidate = today + timedelta(days=d_offset)
                if candidate.weekday() == 4:  # Friday
                    week_num = (candidate.day - 1) // 7 + 1
                    if week_num == 3:
                        days_to_opex = d_offset
                        break

            # Score: short gamma + close to flip + near OPEX = high risk
            d_score = 0.3
            if regime == "short_gamma":
                d_score += 0.35
            if abs(flip_dist_pct) < 1.0:
                d_score += 0.15
            if days_to_opex < 5:
                d_score += 0.15
            d_score = min(d_score, 1.0)

            dealer_risk = {
                "gex_regime": regime,
                "net_gex": round(net_gex),
                "gamma_flip_distance_pct": flip_dist_pct,
                "vanna_direction": "adverse" if vanna < 0 else "supportive",
                "charm_direction": "bearish" if charm < 0 else "bullish",
                "days_to_opex": days_to_opex,
                "risk_level": _compute_risk_level(d_score),
            }
            sub_scores.append(d_score)
    except Exception as exc:
        log.debug("Risk map dealer_risk failed: {e}", e=str(exc))
        errors.append(f"dealer_risk: {exc}")

    # ── 2. Volatility Risk ──────────────────────────────────────────
    vol_risk: dict[str, Any] = {
        "vix": 0,
        "vix_percentile_1y": 50,
        "vix_term_structure": "contango",
        "realized_vs_implied": 1.0,
        "risk_level": "moderate",
    }
    try:
        from sqlalchemy import text as sql_text

        with engine.connect() as conn:
            vix_row = conn.execute(sql_text("""
                SELECT rs.value, rs.obs_date FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name ILIKE :n
                ORDER BY rs.obs_date DESC LIMIT 1
            """), {"n": "%vix%close%"}).fetchone()

            vix_val = float(vix_row[0]) if vix_row else 20.0

            vix_hist = conn.execute(sql_text("""
                SELECT rs.value FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name ILIKE :n
                AND rs.obs_date >= CURRENT_DATE - INTERVAL '365 days'
                ORDER BY rs.obs_date
            """), {"n": "%vix%close%"}).fetchall()

            pct = 50
            if vix_hist:
                vals = [float(r[0]) for r in vix_hist]
                pct = int(sum(1 for v in vals if v <= vix_val) / len(vals) * 100)

        term = "contango"
        if vix_val > 25:
            term = "backwardation"

        rv_iv = min(round(vix_val / max(vix_val * 0.9, 1), 2), 2.0)

        v_score = min(pct / 100, 1.0)
        vol_risk = {
            "vix": round(vix_val, 1),
            "vix_percentile_1y": pct,
            "vix_term_structure": term,
            "realized_vs_implied": rv_iv,
            "risk_level": _compute_risk_level(v_score),
        }
        sub_scores.append(v_score)
    except Exception as exc:
        log.debug("Risk map vol_risk failed: {e}", e=str(exc))
        errors.append(f"volatility_risk: {exc}")

    # ── 3. Concentration Risk ───────────────────────────────────────
    conc_risk: dict[str, Any] = {
        "top_5_watchlist_weight": 0,
        "sector_concentration": {},
        "single_name_max": {"ticker": "N/A", "weight": 0},
        "risk_level": "low",
    }
    try:
        from sqlalchemy import text as sql_text

        with engine.connect() as conn:
            positions = conn.execute(sql_text("""
                SELECT ticker, allocation_pct, sector
                FROM watchlist
                WHERE active = true
                ORDER BY allocation_pct DESC NULLS LAST
            """)).fetchall()

        if positions:
            allocs = [(r[0], float(r[1] or 0), r[2] or "other") for r in positions]
            total_alloc = sum(a for _, a, _ in allocs) or 1.0
            weights = [(t, a / total_alloc, s) for t, a, s in allocs]

            top5_w = sum(w for _, w, _ in weights[:5])
            sectors: dict[str, float] = {}
            for _, w, s in weights:
                sectors[s] = sectors.get(s, 0) + w

            max_name = weights[0] if weights else ("N/A", 0, "")

            c_score = 0.15
            if top5_w > 0.7:
                c_score += 0.35
            elif top5_w > 0.5:
                c_score += 0.2
            max_sector = max(sectors.values()) if sectors else 0
            if max_sector > 0.4:
                c_score += 0.25
            if max_name[1] > 0.2:
                c_score += 0.2
            c_score = min(c_score, 1.0)

            conc_risk = {
                "top_5_watchlist_weight": round(top5_w, 2),
                "sector_concentration": {k: round(v, 2) for k, v in sorted(sectors.items(), key=lambda x: -x[1])[:6]},
                "single_name_max": {"ticker": max_name[0], "weight": round(max_name[1], 2)},
                "risk_level": _compute_risk_level(c_score),
            }
            sub_scores.append(c_score)
    except Exception as exc:
        log.debug("Risk map conc_risk failed: {e}", e=str(exc))
        errors.append(f"concentration_risk: {exc}")

    # ── 4. Correlation Risk ─────────────────────────────────────────
    corr_risk: dict[str, Any] = {
        "avg_cross_correlation": 0.5,
        "decoupling_events": [],
        "risk_level": "low",
    }
    try:
        from sqlalchemy import text as sql_text

        with engine.connect() as conn:
            rows = conn.execute(sql_text("""
                SELECT fr.name, rs.obs_date, rs.value
                FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name LIKE '%%_close'
                AND rs.obs_date >= CURRENT_DATE - INTERVAL '90 days'
                ORDER BY fr.name, rs.obs_date
            """)).fetchall()

        if rows and len(rows) > 50:
            import pandas as pd
            import numpy as np

            df = pd.DataFrame(rows, columns=["name", "date", "value"])
            pivot = df.pivot_table(index="date", columns="name", values="value")
            returns = pivot.pct_change().dropna()

            if returns.shape[1] >= 2:
                corr_mat = returns.corr()
                mask = np.triu(np.ones_like(corr_mat, dtype=bool), k=1)
                upper = corr_mat.where(mask)
                avg_corr = float(upper.stack().mean())

                decouplings = []
                recent = returns.tail(22)
                if len(recent) >= 10:
                    corr_30d = recent.corr()
                    cols = list(corr_mat.columns)
                    for i in range(len(cols)):
                        for j in range(i + 1, min(len(cols), i + 5)):
                            c90 = corr_mat.iloc[i, j]
                            c30 = corr_30d.iloc[i, j]
                            if abs(c90 - c30) > 0.3:
                                decouplings.append({
                                    "pair": f"{cols[i]}/{cols[j]}",
                                    "correlation_30d": round(float(c30), 2),
                                    "correlation_90d": round(float(c90), 2),
                                })
                    decouplings.sort(key=lambda x: abs(x["correlation_90d"] - x["correlation_30d"]), reverse=True)

                co_score = max(0, min(avg_corr, 1.0))

                corr_risk = {
                    "avg_cross_correlation": round(avg_corr, 2),
                    "decoupling_events": decouplings[:5],
                    "risk_level": _compute_risk_level(co_score),
                }
                sub_scores.append(co_score)
    except Exception as exc:
        log.debug("Risk map corr_risk failed: {e}", e=str(exc))
        errors.append(f"correlation_risk: {exc}")

    # ── 5. Credit Risk ──────────────────────────────────────────────
    credit_risk: dict[str, Any] = {
        "hy_spread": 0,
        "ig_spread": 0,
        "ted_spread": 0,
        "spread_direction": "stable",
        "risk_level": "moderate",
    }
    try:
        from sqlalchemy import text as sql_text

        with engine.connect() as conn:
            hy_row = conn.execute(sql_text("""
                SELECT rs.value FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name ILIKE :n
                ORDER BY rs.obs_date DESC LIMIT 1
            """), {"n": "%hy%spread%"}).fetchone()

            ig_row = conn.execute(sql_text("""
                SELECT rs.value FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name ILIKE :n
                ORDER BY rs.obs_date DESC LIMIT 1
            """), {"n": "%ig%spread%"}).fetchone()

            ted_row = conn.execute(sql_text("""
                SELECT rs.value FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name ILIKE :n
                ORDER BY rs.obs_date DESC LIMIT 1
            """), {"n": "%ted%spread%"}).fetchone()

            hy_prev = conn.execute(sql_text("""
                SELECT rs.value FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name ILIKE :n
                AND rs.obs_date <= CURRENT_DATE - INTERVAL '30 days'
                ORDER BY rs.obs_date DESC LIMIT 1
            """), {"n": "%hy%spread%"}).fetchone()

        hy_val = float(hy_row[0]) if hy_row else 400
        ig_val = float(ig_row[0]) if ig_row else 100
        ted_val = float(ted_row[0]) if ted_row else 0.3
        hy_prev_val = float(hy_prev[0]) if hy_prev else hy_val

        direction = "stable"
        if hy_val > hy_prev_val * 1.05:
            direction = "widening"
        elif hy_val < hy_prev_val * 0.95:
            direction = "tightening"

        cr_score = min(hy_val / 800, 1.0)
        if direction == "widening":
            cr_score = min(cr_score + 0.15, 1.0)

        credit_risk = {
            "hy_spread": round(hy_val),
            "ig_spread": round(ig_val),
            "ted_spread": round(ted_val, 2),
            "spread_direction": direction,
            "risk_level": _compute_risk_level(cr_score),
        }
        sub_scores.append(cr_score)
    except Exception as exc:
        log.debug("Risk map credit_risk failed: {e}", e=str(exc))
        errors.append(f"credit_risk: {exc}")

    # ── 6. Liquidity Risk ───────────────────────────────────────────
    liq_risk: dict[str, Any] = {
        "fed_net_liquidity_change_1m": 0,
        "reverse_repo_trend": "stable",
        "tga_trend": "stable",
        "risk_level": "moderate",
    }
    try:
        from sqlalchemy import text as sql_text

        with engine.connect() as conn:
            liq_row = conn.execute(sql_text("""
                SELECT rs.value, rs.obs_date FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name ILIKE :n
                ORDER BY rs.obs_date DESC LIMIT 1
            """), {"n": "%fed%balance%"}).fetchone()

            liq_prev = conn.execute(sql_text("""
                SELECT rs.value FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name ILIKE :n
                AND rs.obs_date <= CURRENT_DATE - INTERVAL '30 days'
                ORDER BY rs.obs_date DESC LIMIT 1
            """), {"n": "%fed%balance%"}).fetchone()

            rrp_row = conn.execute(sql_text("""
                SELECT rs.value FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name ILIKE :n
                ORDER BY rs.obs_date DESC LIMIT 1
            """), {"n": "%reverse_repo%"}).fetchone()

            rrp_prev = conn.execute(sql_text("""
                SELECT rs.value FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name ILIKE :n
                AND rs.obs_date <= CURRENT_DATE - INTERVAL '30 days'
                ORDER BY rs.obs_date DESC LIMIT 1
            """), {"n": "%reverse_repo%"}).fetchone()

            tga_row = conn.execute(sql_text("""
                SELECT rs.value FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name ILIKE :n
                ORDER BY rs.obs_date DESC LIMIT 1
            """), {"n": "%tga%"}).fetchone()

            tga_prev = conn.execute(sql_text("""
                SELECT rs.value FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name ILIKE :n
                AND rs.obs_date <= CURRENT_DATE - INTERVAL '30 days'
                ORDER BY rs.obs_date DESC LIMIT 1
            """), {"n": "%tga%"}).fetchone()

        liq_val = float(liq_row[0]) if liq_row else 0
        liq_prev_val = float(liq_prev[0]) if liq_prev else liq_val
        liq_change = liq_val - liq_prev_val

        rrp_val = float(rrp_row[0]) if rrp_row else 0
        rrp_prev_val = float(rrp_prev[0]) if rrp_prev else rrp_val
        rrp_trend = "declining" if rrp_val < rrp_prev_val * 0.95 else "rising" if rrp_val > rrp_prev_val * 1.05 else "stable"

        tga_val = float(tga_row[0]) if tga_row else 0
        tga_prev_val = float(tga_prev[0]) if tga_prev else tga_val
        tga_trend = "building" if tga_val > tga_prev_val * 1.05 else "draining" if tga_val < tga_prev_val * 0.95 else "stable"

        l_score = 0.3
        if liq_change < -50_000_000_000:
            l_score += 0.3
        elif liq_change < 0:
            l_score += 0.15
        if tga_trend == "building":
            l_score += 0.15
        if rrp_trend == "declining":
            l_score -= 0.1
        l_score = max(0, min(l_score, 1.0))

        liq_risk = {
            "fed_net_liquidity_change_1m": round(liq_change),
            "reverse_repo_trend": rrp_trend,
            "tga_trend": tga_trend,
            "risk_level": _compute_risk_level(l_score),
        }
        sub_scores.append(l_score)
    except Exception as exc:
        log.debug("Risk map liq_risk failed: {e}", e=str(exc))
        errors.append(f"liquidity_risk: {exc}")

    # ── Overall Score & Narrative ────────────────────────────────────
    overall = round(sum(sub_scores) / max(len(sub_scores), 1), 2)

    parts = []
    all_risks = {
        "Dealer positioning": dealer_risk,
        "Volatility": vol_risk,
        "Concentration": conc_risk,
        "Correlation": corr_risk,
        "Credit spreads": credit_risk,
        "Liquidity": liq_risk,
    }

    elevated_cats = [name for name, r in all_risks.items() if r["risk_level"] in ("elevated", "high", "critical")]
    if not elevated_cats:
        parts.append("All risk categories are within normal ranges.")
    elif len(elevated_cats) == 1:
        parts.append(f"{elevated_cats[0]} risk is elevated and warrants monitoring.")
    else:
        parts.append(f"Multiple risk factors are elevated: {', '.join(elevated_cats)}.")

    if dealer_risk["gex_regime"] == "short_gamma":
        parts.append("Dealers are short gamma, amplifying directional moves.")
    if vol_risk.get("vix", 0) > 25:
        parts.append(f"VIX at {vol_risk['vix']} signals elevated implied volatility.")
    if conc_risk.get("top_5_watchlist_weight", 0) > 0.7:
        parts.append("Portfolio is highly concentrated in top 5 positions.")
    if credit_risk.get("spread_direction") == "widening":
        parts.append("Credit spreads are widening, signaling deteriorating risk appetite.")
    if liq_risk.get("tga_trend") == "building":
        parts.append("Treasury General Account is building, draining market liquidity.")

    if len(elevated_cats) >= 3:
        parts.append("RISK CONVERGENCE: multiple risk factors elevated simultaneously -- reduce exposure.")

    return {
        "dealer_risk": dealer_risk,
        "volatility_risk": vol_risk,
        "concentration_risk": conc_risk,
        "correlation_risk": corr_risk,
        "credit_risk": credit_risk,
        "liquidity_risk": liq_risk,
        "overall_risk_score": overall,
        "risk_narrative": " ".join(parts),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "errors": errors,
    }


@router.get("/risk-map")
async def get_risk_map(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Unified risk map -- all risk exposures sized and colored by threat level.

    Returns dealer, volatility, concentration, correlation, credit, and
    liquidity risk assessments with an overall risk score and narrative.
    Cached for 5 minutes.
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    if (
        _risk_map_cache["data"]
        and _risk_map_cache["ts"]
        and (now - _risk_map_cache["ts"]).total_seconds() < _RISK_MAP_TTL
    ):
        return _risk_map_cache["data"]

    try:
        result = _build_risk_map()
        _risk_map_cache["data"] = result
        _risk_map_cache["ts"] = now
        return result
    except Exception as exc:
        log.error("Risk map build failed: {e}", e=str(exc))
        return {
            "dealer_risk": {"risk_level": "moderate"},
            "volatility_risk": {"risk_level": "moderate"},
            "concentration_risk": {"risk_level": "moderate"},
            "correlation_risk": {"risk_level": "low"},
            "credit_risk": {"risk_level": "moderate"},
            "liquidity_risk": {"risk_level": "moderate"},
            "overall_risk_score": 0.5,
            "risk_narrative": f"Risk map computation error: {exc}",
            "errors": [str(exc)],
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


@router.get("/globe")
async def get_globe_data(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return global economic activity data for the 3D globe visualization.

    Aggregates country-level GDP signals, FX changes, VIIRS night lights,
    trade flows (Comtrade bilateral), capital flows, cross-reference hotspots,
    and FX pair data from resolved_series and raw_series.
    """
    from datetime import datetime, timezone, date as dt_date, timedelta
    from dataclasses import asdict

    engine = get_db_engine()
    today = dt_date.today()
    one_month_ago = today - timedelta(days=30)
    one_year_ago = today - timedelta(days=365)

    countries: list[dict[str, Any]] = []
    flows: list[dict[str, Any]] = []
    hotspots: list[dict[str, Any]] = []
    fx_map: dict[str, float | None] = {}

    # ── Helper: get latest value from raw_series or resolved_series ──
    def _latest(series_id: str, as_of=None) -> float | None:
        if as_of is None:
            as_of = today
        try:
            from sqlalchemy import text as sqla_text
            with engine.connect() as conn:
                row = conn.execute(sqla_text(
                    "SELECT value FROM raw_series "
                    "WHERE series_id = :sid AND obs_date <= :d AND pull_status = 'SUCCESS' "
                    "ORDER BY obs_date DESC LIMIT 1"
                ), {"sid": series_id, "d": as_of}).fetchone()
                if row:
                    return float(row[0])
                # Try resolved_series
                row = conn.execute(sqla_text(
                    "SELECT rs.value FROM resolved_series rs "
                    "JOIN feature_registry fr ON rs.feature_id = fr.id "
                    "WHERE fr.name = :name AND rs.obs_date <= :d "
                    "ORDER BY rs.obs_date DESC LIMIT 1"
                ), {"name": series_id.lower(), "d": as_of}).fetchone()
                if row:
                    return float(row[0])
        except Exception:
            pass
        return None

    def _pct_change(current, previous):
        if current is None or previous is None or previous == 0:
            return None
        return round((current - previous) / abs(previous), 4)

    # ── Country definitions with data series mappings ──
    COUNTRY_CONFIG = [
        {"id": "USA", "name": "United States", "gdp_series": "INDPRO", "fx_series": "DX-Y.NYB",
         "lights_series": None, "trade_prefix": "US"},
        {"id": "CHN", "name": "China", "gdp_series": "china_gdp_real_imf", "fx_series": "USDCNY",
         "lights_series": "viirs_china_lights", "trade_prefix": "CN"},
        {"id": "JPN", "name": "Japan", "gdp_series": "japan_gdp_real_oecd", "fx_series": "USDJPY",
         "lights_series": "viirs_japan_lights", "trade_prefix": "JP"},
        {"id": "DEU", "name": "Germany", "gdp_series": "germany_gdp_real_oecd", "fx_series": "EURUSD",
         "lights_series": "viirs_germany_lights", "trade_prefix": "DE"},
        {"id": "GBR", "name": "United Kingdom", "gdp_series": "uk_gdp_real_oecd", "fx_series": "GBPUSD",
         "lights_series": "viirs_uk_lights", "trade_prefix": "GB"},
        {"id": "FRA", "name": "France", "gdp_series": "france_gdp_real_oecd", "fx_series": "EURUSD",
         "lights_series": "viirs_france_lights", "trade_prefix": "FR"},
        {"id": "IND", "name": "India", "gdp_series": "india_gdp_real_imf", "fx_series": "USDINR",
         "lights_series": "viirs_india_lights", "trade_prefix": "IN"},
        {"id": "BRA", "name": "Brazil", "gdp_series": "brazil_gdp_real_imf", "fx_series": "USDBRL",
         "lights_series": "viirs_brazil_lights", "trade_prefix": "BR"},
        {"id": "KOR", "name": "South Korea", "gdp_series": "korea_gdp_real_oecd", "fx_series": "USDKRW",
         "lights_series": "viirs_korea_lights", "trade_prefix": "KR"},
        {"id": "AUS", "name": "Australia", "gdp_series": "australia_gdp_real_oecd", "fx_series": "AUDUSD",
         "lights_series": None, "trade_prefix": "AU"},
        {"id": "CAN", "name": "Canada", "gdp_series": "canada_gdp_real_oecd", "fx_series": "USDCAD",
         "lights_series": None, "trade_prefix": "CA"},
        {"id": "MEX", "name": "Mexico", "gdp_series": "mexico_gdp_real_imf", "fx_series": "USDMXN",
         "lights_series": None, "trade_prefix": "MX"},
    ]

    # ── Build country data ──
    for cfg in COUNTRY_CONFIG:
        try:
            gdp_now = _latest(cfg["gdp_series"])
            gdp_prev = _latest(cfg["gdp_series"], one_month_ago)
            gdp_change = _pct_change(gdp_now, gdp_prev)

            # GDP signal
            if gdp_change is not None:
                if gdp_change > 0.005:
                    gdp_signal = "growth"
                elif gdp_change < -0.005:
                    gdp_signal = "slowing"
                else:
                    gdp_signal = "stable"
            else:
                gdp_signal = "no_data"

            # FX change
            fx_now = _latest(f"YF:{cfg['fx_series']}:close")
            if fx_now is None:
                fx_now = _latest(cfg["fx_series"])
            fx_prev = _latest(f"YF:{cfg['fx_series']}:close", one_month_ago)
            if fx_prev is None:
                fx_prev = _latest(cfg["fx_series"], one_month_ago)
            fx_change = _pct_change(fx_now, fx_prev)

            # Night lights change
            lights_change = None
            if cfg["lights_series"]:
                lights_now = _latest(cfg["lights_series"])
                lights_prev = _latest(cfg["lights_series"], one_month_ago)
                lights_change = _pct_change(lights_now, lights_prev)

            # Activity score: composite 0-1
            score_parts = []
            if gdp_change is not None:
                score_parts.append(max(0, min(1, 0.5 + gdp_change * 10)))
            if fx_change is not None:
                score_parts.append(max(0, min(1, 0.5 + fx_change * 5)))
            if lights_change is not None:
                score_parts.append(max(0, min(1, 0.5 + lights_change * 8)))
            activity_score = round(sum(score_parts) / len(score_parts), 2) if score_parts else 0.5

            countries.append({
                "id": cfg["id"],
                "name": cfg["name"],
                "gdp_signal": gdp_signal,
                "fx_change_1m": fx_change,
                "night_lights_change": lights_change,
                "activity_score": activity_score,
            })
        except Exception as exc:
            log.debug("Globe country {c} failed: {e}", c=cfg["id"], e=str(exc))
            countries.append({
                "id": cfg["id"], "name": cfg["name"],
                "gdp_signal": "no_data", "fx_change_1m": None,
                "night_lights_change": None, "activity_score": 0.5,
            })

    # ── Trade & capital flows from Comtrade bilateral data ──
    TRADE_PAIRS = [
        ("USA", "CHN"), ("USA", "JPN"), ("USA", "DEU"), ("USA", "MEX"),
        ("USA", "CAN"), ("USA", "KOR"), ("USA", "GBR"), ("USA", "IND"),
        ("CHN", "JPN"), ("CHN", "KOR"), ("CHN", "DEU"), ("CHN", "AUS"),
        ("DEU", "FRA"), ("JPN", "KOR"), ("GBR", "DEU"),
    ]

    prefix_to_id = {cfg["trade_prefix"]: cfg["id"] for cfg in COUNTRY_CONFIG}
    id_to_prefix = {cfg["id"]: cfg["trade_prefix"] for cfg in COUNTRY_CONFIG}

    try:
        from sqlalchemy import text as sqla_text
        with engine.connect() as conn:
            trade_rows = conn.execute(sqla_text(
                "SELECT series_id, value, obs_date FROM raw_series "
                "WHERE series_id LIKE :pattern AND pull_status = 'SUCCESS' "
                "AND obs_date >= :d ORDER BY obs_date DESC"
            ), {"pattern": "%bilateral%", "d": one_year_ago}).fetchall()

            # Group by series and build flows
            series_vals: dict[str, list] = {}
            for row in trade_rows:
                sid = row[0]
                if sid not in series_vals:
                    series_vals[sid] = []
                series_vals[sid].append((float(row[1]), row[2]))

        for from_id, to_id in TRADE_PAIRS:
            fp = id_to_prefix.get(from_id, "")
            tp = id_to_prefix.get(to_id, "")

            # Look for matching bilateral series
            volume = None
            change_1y = None
            for sid, vals in series_vals.items():
                if fp.lower() in sid.lower() and tp.lower() in sid.lower():
                    if vals:
                        volume = vals[0][0]
                        if len(vals) >= 12:
                            change_1y = _pct_change(vals[0][0], vals[-1][0])
                    break

            direction = "surplus"
            if volume is not None and volume < 0:
                direction = "deficit"
                volume = abs(volume)

            flows.append({
                "from": from_id,
                "to": to_id,
                "type": "trade",
                "volume": volume or 50_000_000_000,  # fallback estimate
                "direction": direction,
                "change_1y": change_1y,
            })
    except Exception as exc:
        log.debug("Globe trade flows failed: {e}", e=str(exc))
        # Provide synthetic flows based on known trade relationships
        for from_id, to_id in TRADE_PAIRS:
            flows.append({
                "from": from_id, "to": to_id, "type": "trade",
                "volume": 50_000_000_000, "direction": "surplus",
                "change_1y": None,
            })

    # Add capital flow arrows (from money_flow module where possible)
    CAPITAL_PAIRS = [
        ("JPN", "USA", 200_000_000_000), ("CHN", "USA", 150_000_000_000),
        ("DEU", "USA", 80_000_000_000), ("GBR", "USA", 60_000_000_000),
        ("USA", "IND", 40_000_000_000), ("USA", "BRA", 25_000_000_000),
        ("KOR", "USA", 30_000_000_000), ("AUS", "USA", 20_000_000_000),
    ]
    for from_id, to_id, est_vol in CAPITAL_PAIRS:
        flows.append({
            "from": from_id, "to": to_id, "type": "capital",
            "volume": est_vol, "direction": "inflow",
            "change_1y": None,
        })

    # ── Cross-reference hotspots ──
    try:
        from intelligence.cross_reference import run_all_checks
        report = run_all_checks(engine)
        country_flags = {}
        for check in report.red_flags:
            d = asdict(check)
            # Map check to country
            name_lower = d.get("name", "").lower()
            for cfg in COUNTRY_CONFIG:
                cname = cfg["name"].lower()
                cid = cfg["id"].lower()
                if cname in name_lower or cid in name_lower:
                    key = cfg["id"]
                    severity = "high" if d.get("assessment") == "contradiction" else "medium"
                    if key not in country_flags or severity == "high":
                        country_flags[key] = {
                            "country": cfg["id"],
                            "reason": d.get("implication", d.get("name", "Divergence detected")),
                            "severity": severity,
                        }
            # Check for China-specific flags
            if "china" in name_lower or "viirs" in name_lower:
                if "CHN" not in country_flags:
                    country_flags["CHN"] = {
                        "country": "CHN",
                        "reason": d.get("implication", "GDP divergence from physical indicators"),
                        "severity": "high" if d.get("assessment") == "contradiction" else "medium",
                    }
        hotspots = list(country_flags.values())
    except Exception as exc:
        log.debug("Globe hotspots failed: {e}", e=str(exc))

    # ── FX map ──
    FX_PAIRS = {
        "DXY": "DX-Y.NYB", "EURUSD": "EURUSD=X", "USDJPY": "JPY=X",
        "GBPUSD": "GBPUSD=X", "USDCNY": "CNY=X", "AUDUSD": "AUDUSD=X",
        "USDCAD": "CAD=X", "USDINR": "INR=X", "USDBRL": "BRL=X",
        "USDKRW": "KRW=X", "USDMXN": "MXN=X",
    }
    for label, ticker in FX_PAIRS.items():
        val = _latest(f"YF:{ticker}:close")
        if val is None:
            val = _latest(ticker)
        fx_map[label] = round(val, 4) if val is not None else None

    return {
        "countries": countries,
        "flows": flows,
        "hotspots": hotspots,
        "fx_map": fx_map,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


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


# ── Unified Thesis Endpoint ───────────────────────────────────────────────

_thesis_cache: dict[str, Any] = {"data": None, "ts": 0.0}
_THESIS_CACHE_TTL = 600  # 10 minutes


@router.get("/thesis")
async def get_unified_thesis(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the unified market thesis combining all models and signals.

    Aggregates Fed liquidity, dealer gamma, vanna/charm, institutional rotation,
    congressional signals, insider clusters, cross-reference divergences,
    supply chain, prediction markets, and trust convergence into a single
    directional view with conviction, key drivers, risk factors, and narrative.

    Cached for 10 minutes.
    """
    now = time.time()
    if _thesis_cache["data"] and (now - _thesis_cache["ts"]) < _THESIS_CACHE_TTL:
        return _thesis_cache["data"]

    try:
        from analysis.flow_thesis import generate_unified_thesis

        engine = get_db_engine()
        thesis = generate_unified_thesis(engine)
        _thesis_cache["data"] = thesis
        _thesis_cache["ts"] = now
        return thesis
    except Exception as exc:
        log.error("Unified thesis generation failed: {e}", e=str(exc))
        return {
            "overall_direction": "NEUTRAL",
            "conviction": 0,
            "bullish_score": 0,
            "bearish_score": 0,
            "active_theses": 0,
            "key_drivers": [],
            "risk_factors": [],
            "agreements": [],
            "contradictions": [],
            "theses": [],
            "narrative": f"Thesis generation failed: {exc}",
            "generated_at": None,
            "error": str(exc),
        }


# ── Thesis Tracker Endpoints ─────────────────────────────────────────────


@router.get("/thesis/history")
async def get_thesis_history_endpoint(
    days: int = Query(90, ge=1, le=365, description="Lookback days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return archived thesis snapshots with scoring outcomes.

    Shows the evolution of thesis direction and conviction over time,
    along with whether each thesis was correct, wrong, or partial.
    """
    try:
        from intelligence.thesis_tracker import get_thesis_history

        engine = get_db_engine()
        snapshots = get_thesis_history(engine, days=days)
        return {
            "snapshots": [s.to_dict() for s in snapshots],
            "count": len(snapshots),
            "days": days,
        }
    except Exception as exc:
        log.warning("Thesis history failed: {e}", e=str(exc))
        return {"snapshots": [], "count": 0, "error": str(exc)}


@router.get("/thesis/accuracy")
async def get_thesis_accuracy_endpoint(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return thesis accuracy statistics.

    Includes overall accuracy, per-model accuracy, monthly trend, and
    accuracy by conviction level.
    """
    try:
        from intelligence.thesis_tracker import get_thesis_accuracy

        engine = get_db_engine()
        return get_thesis_accuracy(engine)
    except Exception as exc:
        log.warning("Thesis accuracy failed: {e}", e=str(exc))
        return {
            "overall": {"accuracy_pct": 0, "total_scored": 0},
            "per_model": [],
            "trend": [],
            "best_conditions": {},
            "error": str(exc),
        }


@router.get("/thesis/postmortems")
async def get_thesis_postmortems_endpoint(
    days: int = Query(90, ge=1, le=365, description="Lookback days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return thesis post-mortems for wrong or partially-correct theses.

    Each post-mortem explains which models were right vs wrong, what was
    missed, the root cause classification, and an actionable lesson.
    """
    try:
        from intelligence.thesis_tracker import load_thesis_postmortems

        engine = get_db_engine()
        postmortems = load_thesis_postmortems(engine, days=days)

        # Aggregate root causes
        root_cause_counts: dict[str, int] = {}
        for pm in postmortems:
            rc = pm.get("root_cause", "unknown")
            root_cause_counts[rc] = root_cause_counts.get(rc, 0) + 1

        return {
            "postmortems": postmortems,
            "count": len(postmortems),
            "root_cause_counts": root_cause_counts,
            "days": days,
        }
    except Exception as exc:
        log.warning("Thesis postmortems failed: {e}", e=str(exc))
        return {"postmortems": [], "count": 0, "error": str(exc)}


# ── Sleuth / Investigation Endpoints ─────────────────────────────────────


@router.get("/leads")
async def get_investigation_leads(
    status: str | None = Query(None, description="Filter by status: new, investigating, resolved, dead_end"),
    category: str | None = Query(None, description="Filter by category"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return investigation leads with optional filters."""
    try:
        from intelligence.sleuth import Sleuth
        from dataclasses import asdict

        engine = get_db_engine()
        sleuth = Sleuth(engine)
        leads = sleuth.get_leads(status=status, category=category, limit=limit, offset=offset)
        total = sleuth.count_leads(status=status)

        return {
            "leads": [asdict(l) for l in leads],
            "total": total,
            "limit": limit,
            "offset": offset,
        }
    except Exception as exc:
        log.warning("Sleuth leads query failed: {e}", e=str(exc))
        return {"leads": [], "total": 0, "error": str(exc)}


@router.get("/leads/{lead_id}")
async def get_investigation_lead(
    lead_id: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return a single investigation lead with full detail."""
    try:
        from intelligence.sleuth import Sleuth
        from dataclasses import asdict

        engine = get_db_engine()
        sleuth = Sleuth(engine)
        lead = sleuth._load_lead(lead_id)

        if not lead:
            return {"error": "Lead not found", "lead_id": lead_id}

        return {"lead": asdict(lead)}
    except Exception as exc:
        log.warning("Sleuth lead detail failed: {e}", e=str(exc))
        return {"error": str(exc), "lead_id": lead_id}


@router.post("/leads/investigate")
async def investigate_lead(
    lead_id: str = Query(..., description="ID of the lead to investigate"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Trigger an LLM investigation on a specific lead."""
    try:
        from intelligence.sleuth import Sleuth
        from dataclasses import asdict

        engine = get_db_engine()
        sleuth = Sleuth(engine)
        lead = sleuth._load_lead(lead_id)

        if not lead:
            return {"error": "Lead not found", "lead_id": lead_id}

        result = sleuth.investigate_lead(lead)
        return {
            "status": "investigated",
            "lead": asdict(result),
        }
    except Exception as exc:
        log.warning("Sleuth investigation failed: {e}", e=str(exc))
        return {"error": str(exc), "lead_id": lead_id}


@router.post("/leads/generate")
async def generate_leads(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Trigger lead generation across all intelligence sources."""
    try:
        from intelligence.sleuth import Sleuth
        from dataclasses import asdict

        engine = get_db_engine()
        sleuth = Sleuth(engine)
        leads = sleuth.generate_leads()

        return {
            "status": "generated",
            "leads_created": len(leads),
            "leads": [asdict(l) for l in leads[:20]],
        }
    except Exception as exc:
        log.warning("Sleuth lead generation failed: {e}", e=str(exc))
        return {"error": str(exc), "leads_created": 0}


@router.post("/leads/daily-investigation")
async def run_daily_investigation(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Run a full daily investigation cycle (generate + investigate + rabbit holes)."""
    try:
        from intelligence.sleuth import Sleuth

        engine = get_db_engine()
        sleuth = Sleuth(engine)
        report = sleuth.daily_investigation()

        return {"status": "complete", "report": report}
    except Exception as exc:
        log.warning("Sleuth daily investigation failed: {e}", e=str(exc))
        return {"error": str(exc), "status": "failed"}


# ── Market Diary Endpoints ─────────────────────────────────────────────

@router.get("/diary")
async def get_diary(
    date: str | None = Query(None, description="Date in YYYY-MM-DD format"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Retrieve a market diary entry by date.

    If no date is provided, returns the most recent entry.
    """
    from datetime import date as date_type

    try:
        from intelligence.market_diary import get_diary_entry, list_diary_entries

        engine = get_db_engine()

        if date:
            target = date_type.fromisoformat(date)
            entry = get_diary_entry(engine, target)
            if entry is None:
                return {"error": "No diary entry for this date", "date": date}
            return entry
        else:
            # Return most recent
            result = list_diary_entries(engine, limit=1)
            if result["entries"]:
                entry_date = date_type.fromisoformat(result["entries"][0]["date"])
                entry = get_diary_entry(engine, entry_date)
                return entry or {"error": "Entry not found"}
            return {"error": "No diary entries yet"}
    except Exception as exc:
        log.warning("Diary fetch failed: {e}", e=str(exc))
        return {"error": str(exc)}


@router.get("/diary/list")
async def list_diaries(
    limit: int = Query(30, ge=1, le=365),
    offset: int = Query(0, ge=0),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """List diary entries with summary metadata (date, verdict, return)."""
    try:
        from intelligence.market_diary import list_diary_entries

        engine = get_db_engine()
        return list_diary_entries(engine, limit=limit, offset=offset)
    except Exception as exc:
        log.warning("Diary list failed: {e}", e=str(exc))
        return {"entries": [], "total": 0, "error": str(exc)}


@router.get("/diary/search")
async def search_diaries(
    q: str = Query(..., min_length=2, description="Search term"),
    limit: int = Query(20, ge=1, le=100),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Search diary entries by keyword."""
    try:
        from intelligence.market_diary import search_diary

        engine = get_db_engine()
        results = search_diary(engine, q, limit=limit)
        return {"results": results, "query": q}
    except Exception as exc:
        log.warning("Diary search failed: {e}", e=str(exc))
        return {"results": [], "query": q, "error": str(exc)}


@router.post("/diary/generate")
async def generate_diary(
    date: str | None = Query(None, description="Date in YYYY-MM-DD format"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Manually trigger diary generation for a given date (defaults to today)."""
    from datetime import date as date_type

    try:
        from intelligence.market_diary import write_diary_entry

        engine = get_db_engine()
        target = date_type.fromisoformat(date) if date else date_type.today()
        result = write_diary_entry(engine, target_date=target)
        return result
    except Exception as exc:
        log.warning("Diary generation failed: {e}", e=str(exc))
        return {"error": str(exc)}


# ── News Intelligence Endpoints ──────────────────────────────────────────


@router.get("/news")
async def get_news_feed_endpoint(
    ticker: str | None = Query(None, description="Filter by ticker symbol"),
    hours: int = Query(24, ge=1, le=168, description="Hours to look back"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get recent news with sentiment, sorted by relevance.

    Optionally filter by ticker. Returns articles from the last N hours
    with LLM sentiment scores and relevance ranking.
    """
    try:
        from intelligence.news_intel import get_news_feed

        engine = get_db_engine()
        articles = get_news_feed(engine, ticker=ticker, hours=hours)
        return {
            "ticker": ticker,
            "hours": hours,
            "count": len(articles),
            "articles": articles,
        }
    except Exception as exc:
        log.warning("News feed endpoint failed: {e}", e=str(exc))
        return {"articles": [], "count": 0, "error": str(exc)}


@router.get("/news/stats")
async def get_news_stats_endpoint(
    hours: int = Query(24, ge=1, le=168, description="Hours to look back"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get aggregate news statistics — sentiment breakdown, top tickers, sources."""
    try:
        from intelligence.news_intel import get_news_stats

        engine = get_db_engine()
        return get_news_stats(engine, hours=hours)
    except Exception as exc:
        log.warning("News stats endpoint failed: {e}", e=str(exc))
        return {"error": str(exc)}


@router.get("/news/narrative-shift/{ticker}")
async def get_narrative_shift_endpoint(
    ticker: str,
    days: int = Query(7, ge=2, le=30, description="Lookback days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Detect when media narrative changes direction on a ticker.

    Compares recent (2-day) vs prior sentiment distribution to find
    significant shifts from bullish to bearish or vice versa.
    """
    try:
        from intelligence.news_intel import detect_narrative_shift

        engine = get_db_engine()
        return detect_narrative_shift(engine, ticker=ticker, days=days)
    except Exception as exc:
        log.warning("Narrative shift endpoint failed: {e}", e=str(exc))
        return {"ticker": ticker, "shift_detected": False, "error": str(exc)}


@router.get("/news/before-move/{ticker}")
async def get_news_before_move_endpoint(
    ticker: str,
    move_date: str = Query(..., description="Date of the price move (YYYY-MM-DD)"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Forensic analysis: what news preceded a significant price move?

    Looks back 3 days before the move_date for news mentioning the ticker.
    """
    try:
        from intelligence.news_intel import find_news_before_move

        engine = get_db_engine()
        articles = find_news_before_move(engine, ticker=ticker, move_date=move_date)
        return {
            "ticker": ticker,
            "move_date": move_date,
            "articles_found": len(articles),
            "articles": articles,
        }
    except Exception as exc:
        log.warning("News-before-move endpoint failed: {e}", e=str(exc))
        return {"ticker": ticker, "articles": [], "error": str(exc)}


@router.get("/news/briefing")
async def get_news_briefing_endpoint(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """LLM-generated briefing from today's news flow.

    Returns a markdown-formatted market briefing synthesized from
    recent high-confidence news articles.
    """
    try:
        from intelligence.news_intel import generate_news_briefing

        engine = get_db_engine()
        briefing = generate_news_briefing(engine)
        return {"briefing": briefing}
    except Exception as exc:
        log.warning("News briefing endpoint failed: {e}", e=str(exc))
        return {"briefing": f"News briefing unavailable: {exc}", "error": str(exc)}


# ── Event Sequence Endpoints ───────────────────────────────────────────────


@router.get("/events")
async def get_event_sequence(
    ticker: str | None = Query(None, description="Ticker symbol"),
    sector: str | None = Query(None, description="Sector name or ETF (e.g., Technology, XLK)"),
    days: int = Query(90, ge=1, le=365, description="Lookback days"),
    with_lead_times: bool = Query(False, description="Compute lead times to next price move"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Build a chronological timeline of ALL events for a ticker or sector.

    Pulls from signal_sources, news_articles, options_daily_signals,
    decision_journal, cross_reference_checks, and earnings_calendar.

    Supply either ``ticker`` or ``sector`` (not both).  If ``sector`` is
    provided, events for all constituent tickers are returned.
    """
    if not ticker and not sector:
        return {"error": "Provide either 'ticker' or 'sector' query parameter", "events": []}

    try:
        from intelligence.event_sequence import (
            build_sequence,
            build_sector_sequence,
            build_sequence_with_lead_times,
            events_to_dicts,
        )

        engine = get_db_engine()

        if sector:
            events = build_sector_sequence(engine, sector=sector, days=days)
        elif with_lead_times:
            events = build_sequence_with_lead_times(engine, ticker=ticker, days=days)
        else:
            events = build_sequence(engine, ticker=ticker, days=days)

        # Aggregate event type counts
        type_counts: dict[str, int] = {}
        direction_counts: dict[str, int] = {}
        for e in events:
            type_counts[e.event_type] = type_counts.get(e.event_type, 0) + 1
            direction_counts[e.direction] = direction_counts.get(e.direction, 0) + 1

        return {
            "events": events_to_dicts(events),
            "count": len(events),
            "ticker": ticker,
            "sector": sector,
            "days": days,
            "type_counts": type_counts,
            "direction_counts": direction_counts,
        }
    except Exception as exc:
        log.warning("Event sequence failed: {e}", e=str(exc))
        return {
            "events": [],
            "count": 0,
            "ticker": ticker,
            "sector": sector,
            "error": str(exc),
        }


@router.get("/events/patterns")
async def get_recurring_patterns(
    min_occurrences: int = Query(3, ge=2, le=50, description="Minimum pattern occurrences"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Detect recurring event sequences across all tracked tickers.

    Finds 2- and 3-event sequences that repeat at least ``min_occurrences``
    times.  Examples: "insider sell -> dark pool spike -> price drop".
    """
    try:
        from intelligence.event_sequence import find_recurring_patterns

        engine = get_db_engine()
        patterns = find_recurring_patterns(engine, min_occurrences=min_occurrences)
        return {
            "patterns": patterns,
            "count": len(patterns),
            "min_occurrences": min_occurrences,
        }
    except Exception as exc:
        log.warning("Recurring pattern detection failed: {e}", e=str(exc))
        return {"patterns": [], "count": 0, "error": str(exc)}


# ── Pattern Engine Endpoints ─────────────────────────────────────────────


@router.get("/patterns")
async def get_discovered_patterns(
    min_occurrences: int = Query(3, ge=2, le=50, description="Minimum pattern occurrences"),
    max_sequence_length: int = Query(4, ge=2, le=4, description="Max sequence length"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """All discovered recurring event patterns.

    Scans historical event sequences across all watchlist tickers to find
    recurring 2-, 3-, and 4-event sequences.  Only returns patterns with a
    hit rate above 50%.  Sorted by confidence x actionable return.
    """
    try:
        from intelligence.pattern_engine import discover_patterns

        engine = get_db_engine()
        patterns = discover_patterns(
            engine,
            min_occurrences=min_occurrences,
            max_sequence_length=max_sequence_length,
        )
        return {
            "patterns": [p.to_dict() for p in patterns],
            "count": len(patterns),
            "actionable_count": sum(1 for p in patterns if p.actionable),
            "min_occurrences": min_occurrences,
            "max_sequence_length": max_sequence_length,
        }
    except Exception as exc:
        log.warning("Pattern discovery failed: {e}", e=str(exc))
        return {"patterns": [], "count": 0, "error": str(exc)}


@router.get("/patterns/active")
async def get_active_patterns(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Currently in-progress patterns — the prediction engine.

    For each discovered pattern, checks whether the first N-1 steps have
    already occurred for any watchlist ticker.  Returns what step comes next
    and when it is expected.
    """
    try:
        from intelligence.pattern_engine import match_active_patterns

        engine = get_db_engine()
        active = match_active_patterns(engine)
        return {
            "active_patterns": active,
            "count": len(active),
            "actionable_count": sum(1 for a in active if a.get("actionable")),
        }
    except Exception as exc:
        log.warning("Active pattern matching failed: {e}", e=str(exc))
        return {"active_patterns": [], "count": 0, "error": str(exc)}


@router.get("/patterns/{ticker}")
async def get_patterns_for_ticker_endpoint(
    ticker: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Patterns observed for a specific ticker, including any currently active.

    Returns both historical patterns where this ticker appeared and any
    patterns that are partially matched (in progress) right now.
    """
    try:
        from intelligence.pattern_engine import get_patterns_for_ticker

        engine = get_db_engine()
        patterns = get_patterns_for_ticker(engine, ticker)
        active_count = sum(1 for p in patterns if p.get("active_match"))
        return {
            "ticker": ticker.upper(),
            "patterns": patterns,
            "count": len(patterns),
            "active_count": active_count,
        }
    except Exception as exc:
        log.warning("Pattern lookup for {t} failed: {e}", t=ticker, e=str(exc))
        return {"ticker": ticker.upper(), "patterns": [], "count": 0, "error": str(exc)}


# ── Government Contract Endpoints ────────────────────────────────────────


@router.get("/gov-contracts")
async def get_gov_contracts(
    ticker: str | None = Query(None, description="Filter by stock ticker"),
    days: int = Query(30, ge=1, le=365, description="Lookback days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return government contract awards, optionally filtered by ticker.

    If ticker is provided, returns contracts for that company only.
    Otherwise returns all contracts in the lookback window.
    Includes insider/congressional overlap detection when ticker is specified.
    """
    try:
        from intelligence.gov_intel import (
            get_recent_contracts,
            get_contracts_for_ticker,
            detect_contract_insider_overlap,
        )

        engine = get_db_engine()

        if ticker:
            contracts = get_contracts_for_ticker(engine, ticker)
        else:
            contracts = get_recent_contracts(engine, days=days)

        result: dict[str, Any] = {
            "contracts": [c.to_dict() for c in contracts],
            "total": len(contracts),
            "ticker": ticker,
            "days": days,
        }

        # Include overlap detection when filtering by ticker
        if ticker:
            try:
                overlaps = detect_contract_insider_overlap(engine, lookback_days=days)
                ticker_overlaps = [
                    o.to_dict() for o in overlaps
                    if o.ticker == ticker.strip().upper()
                ]
                result["insider_overlaps"] = ticker_overlaps
            except Exception as exc:
                log.debug("Overlap detection failed: {e}", e=str(exc))
                result["insider_overlaps"] = []

        return result

    except Exception as exc:
        log.warning("Gov contracts endpoint failed: {e}", e=str(exc))
        return {
            "contracts": [],
            "total": 0,
            "ticker": ticker,
            "days": days,
            "error": str(exc),
        }


@router.get("/gov-contracts/overlaps")
async def get_contract_insider_overlaps(
    days: int = Query(90, ge=1, le=365, description="Lookback days for contracts"),
    window: int = Query(30, ge=1, le=90, description="Pre-contract trade window in days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Detect insider/congressional trades that preceded government contract awards.

    Returns cases where a BUY signal from an insider or congressional member
    occurred within the specified window before a contract award for the same company.
    Sorted by suspicion score (higher = more suspicious).
    """
    try:
        from intelligence.gov_intel import detect_contract_insider_overlap

        engine = get_db_engine()
        overlaps = detect_contract_insider_overlap(
            engine,
            lookback_days=days,
            pre_contract_window_days=window,
        )

        return {
            "overlaps": [o.to_dict() for o in overlaps],
            "total": len(overlaps),
            "lookback_days": days,
            "pre_contract_window_days": window,
        }

    except Exception as exc:
        log.warning("Contract overlap endpoint failed: {e}", e=str(exc))
        return {
            "overlaps": [],
            "total": 0,
            "error": str(exc),
        }


# ── Dollar Flow Endpoints ────────────────────────────────────────────────


@router.get("/dollar-flows")
async def get_dollar_flows(
    ticker: str | None = Query(None, description="Filter by ticker"),
    sector: str | None = Query(None, description="Filter by sector"),
    days: int = Query(30, ge=1, le=365, description="Lookback days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return normalized dollar flows across all signal sources.

    Converts congressional trades, insider filings, dark pool activity,
    13F position changes, ETF flows, whale options, and prediction market
    signals into estimated USD amounts for apples-to-apples comparison.
    """
    try:
        from intelligence.dollar_flows import (
            get_flows_by_ticker,
            get_flows_by_sector,
            get_aggregate_flows,
            get_biggest_movers,
        )

        engine = get_db_engine()

        if ticker:
            flows = get_flows_by_ticker(engine, ticker, days=days)
            return {
                "flows": flows,
                "count": len(flows),
                "ticker": ticker,
                "days": days,
            }

        if sector:
            flows = get_flows_by_sector(engine, sector, days=days)
            return {
                "flows": flows,
                "count": len(flows),
                "sector": sector,
                "days": days,
            }

        # Default: aggregate view + biggest movers
        aggregates = get_aggregate_flows(engine, days=days)
        movers = get_biggest_movers(engine, days=min(days, 7))

        return {
            "aggregates": aggregates,
            "biggest_movers": movers,
            "days": days,
        }

    except Exception as exc:
        log.warning("Dollar flows endpoint failed: {e}", e=str(exc))
        return {
            "flows": [],
            "aggregates": {},
            "biggest_movers": [],
            "error": str(exc),
        }


@router.post("/dollar-flows/normalize")
async def trigger_dollar_flow_normalization(
    days: int = Query(90, ge=1, le=365, description="Lookback days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Trigger a full dollar flow normalization cycle.

    Scans all signal sources and raw_series within the lookback window,
    converts every signal to estimated USD, and persists to the
    dollar_flows table.
    """
    try:
        from intelligence.dollar_flows import normalize_all_flows

        engine = get_db_engine()
        flows = normalize_all_flows(engine, days=days)

        return {
            "normalized": len(flows),
            "days": days,
            "status": "ok",
        }

    except Exception as exc:
        log.warning("Dollar flow normalization failed: {e}", e=str(exc))
        return {"normalized": 0, "error": str(exc)}


# ── Legislative Intelligence Endpoints ──────────────────────────────────


@router.get("/legislation")
async def get_legislation_overview(
    ticker: str | None = Query(None, description="Filter by affected ticker"),
    committee: str | None = Query(None, description="Filter by committee name"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Legislative intelligence: bills, hearings, and trading alerts.

    Returns upcoming hearings, bills affecting a ticker, and the key
    insight -- committee members trading in sectors their committee is
    actively legislating.

    Parameters:
        ticker: Optional ticker to filter results.
        committee: Optional committee name to filter results.
    """
    try:
        from intelligence.legislative_intel import (
            get_upcoming_hearings,
            get_bills_affecting_ticker,
            detect_legislative_trading,
            get_legislation_summary,
        )

        engine = get_db_engine()

        if ticker:
            bills = get_bills_affecting_ticker(engine, ticker)
            trade_alerts = detect_legislative_trading(engine, days_back=30)
            ticker_alerts = [a for a in trade_alerts if a["ticker"] == ticker.upper()]
            return {
                "ticker": ticker.upper(),
                "bills": bills[:50],
                "trade_alerts": ticker_alerts,
                "hearings": get_upcoming_hearings(engine, days=14),
            }

        if committee:
            hearings = get_upcoming_hearings(engine, days=14)
            committee_lower = committee.lower()
            filtered_hearings = [
                h for h in hearings
                if any(committee_lower in c.lower() for c in h.get("committees", []))
            ]
            trade_alerts = detect_legislative_trading(engine, days_back=30)
            committee_alerts = [
                a for a in trade_alerts
                if committee_lower in a.get("committee", "").lower()
            ]
            return {
                "committee": committee,
                "hearings": filtered_hearings,
                "trade_alerts": committee_alerts,
            }

        # Default: full summary
        return get_legislation_summary(engine)

    except Exception as exc:
        log.warning("Legislation endpoint failed: {e}", e=str(exc))
        return {
            "upcoming_hearings_count": 0,
            "upcoming_hearings": [],
            "trade_alerts_count": 0,
            "high_severity_alerts": [],
            "medium_severity_alerts": [],
            "most_legislated_tickers": [],
            "error": str(exc),
        }


@router.get("/legislation/hearings")
async def get_legislation_hearings(
    days: int = Query(14, ge=1, le=60, description="Days ahead to search"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return upcoming committee hearings with sector/ticker impact."""
    try:
        from intelligence.legislative_intel import get_upcoming_hearings

        engine = get_db_engine()
        hearings = get_upcoming_hearings(engine, days=days)
        return {"hearings": hearings, "count": len(hearings)}
    except Exception as exc:
        log.warning("Legislation hearings endpoint failed: {e}", e=str(exc))
        return {"hearings": [], "count": 0, "error": str(exc)}


@router.get("/legislation/trading-alerts")
async def get_legislation_trading_alerts(
    days: int = Query(30, ge=1, le=90, description="Days back to search"),
    severity: str | None = Query(None, description="Filter by severity: HIGH, MEDIUM"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Detect committee members trading in sectors they oversee.

    This is the key intelligence output -- flags potentially informed
    trading by members of Congress.
    """
    try:
        from intelligence.legislative_intel import detect_legislative_trading

        engine = get_db_engine()
        alerts = detect_legislative_trading(engine, days_back=days)

        if severity:
            alerts = [a for a in alerts if a["severity"] == severity.upper()]

        return {
            "alerts": alerts,
            "count": len(alerts),
            "high_count": sum(1 for a in alerts if a["severity"] == "HIGH"),
            "medium_count": sum(1 for a in alerts if a["severity"] == "MEDIUM"),
        }
    except Exception as exc:
        log.warning("Legislation trading alerts endpoint failed: {e}", e=str(exc))
        return {"alerts": [], "count": 0, "error": str(exc)}


# ── Forensic Analysis Endpoints ──────────────────────────────────────────


@router.get("/forensics/{ticker}")
async def get_forensic_reports(
    ticker: str,
    days: int = Query(90, ge=1, le=365, description="Lookback window in days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return all stored forensic reports for a ticker.

    Forensic reports reconstruct what events preceded significant
    price moves, identifying who was active and what signals fired.
    """
    try:
        from intelligence.forensics import load_forensic_reports, generate_forensic_summary

        engine = get_db_engine()
        reports = load_forensic_reports(engine, ticker, days=days)

        summary: str | None = None
        if reports:
            try:
                summary = generate_forensic_summary(engine, ticker, days=days)
            except Exception as exc:
                log.debug("Forensic summary generation failed: {e}", e=str(exc))

        return {
            "ticker": ticker.upper(),
            "reports": reports,
            "count": len(reports),
            "days": days,
            "summary": summary,
        }
    except Exception as exc:
        log.warning("Forensic reports endpoint failed for {t}: {e}", t=ticker, e=str(exc))
        return {"ticker": ticker.upper(), "reports": [], "count": 0, "error": str(exc)}


@router.post("/forensics/{ticker}/analyze")
async def analyze_forensic_move(
    ticker: str,
    date: str = Query(..., description="Move date in YYYY-MM-DD format"),
    lookback: int = Query(14, ge=1, le=60, description="Lookback days before the move"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Analyze a specific price move forensically.

    Reconstructs the event timeline preceding the move, identifies
    key actors and aligned signals, and generates a narrative.
    """
    try:
        from intelligence.forensics import analyze_move

        engine = get_db_engine()
        report = analyze_move(engine, ticker, date, lookback_days=lookback)

        if report is None:
            return {
                "ticker": ticker.upper(),
                "date": date,
                "error": "No price data found for the specified date.",
            }

        return {
            "ticker": ticker.upper(),
            "date": date,
            "report": report.to_dict(),
        }
    except Exception as exc:
        log.warning(
            "Forensic analysis endpoint failed for {t} on {d}: {e}",
            t=ticker, d=date, e=str(exc),
        )
        return {"ticker": ticker.upper(), "date": date, "error": str(exc)}


# ── Causation Endpoints ─────────────────────────────────────────────────


@router.get("/causation")
async def get_causation(
    ticker: str | None = Query(None, description="Filter by ticker"),
    days: int = Query(30, ge=1, le=365, description="Look-back window in days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return causal links for recent trading activity.

    If ticker is provided, generates a causal narrative for that ticker
    and returns causes for its recent signals.  Otherwise returns batch
    results across all recent signals.
    """
    try:
        from intelligence.causation import (
            find_causes as _find_causes,
            batch_find_causes as _batch,
            generate_causal_narrative as _narrative,
        )

        engine = get_db_engine()

        if ticker:
            ticker_upper = ticker.strip().upper()
            narrative = _narrative(engine, ticker_upper)

            # Also fetch individual causes for recent signals
            from datetime import date as _date, timedelta as _td
            from sqlalchemy import text as _text

            cutoff = _date.today() - _td(days=days)
            with engine.connect() as conn:
                rows = conn.execute(
                    _text(
                        "SELECT id, source_id, signal_type, signal_date "
                        "FROM signal_sources "
                        "WHERE ticker = :t AND signal_date >= :c "
                        "AND source_type IN ('congressional', 'insider') "
                        "ORDER BY signal_date DESC "
                        "LIMIT 20"
                    ),
                    {"t": ticker_upper, "c": cutoff},
                ).fetchall()

            causes = []
            for row in rows:
                found = _find_causes(
                    engine, row[1], row[2], ticker_upper, str(row[3]),
                    signal_id=row[0],
                )
                causes.extend([c.to_dict() for c in found])

            return {
                "ticker": ticker_upper,
                "days": days,
                "narrative": narrative,
                "causes": causes[:100],
                "total_causes": len(causes),
            }

        # No ticker — batch mode
        all_causes = _batch(engine, days=days)
        return {
            "days": days,
            "causes": [c.to_dict() for c in all_causes[:200]],
            "total_causes": len(all_causes),
        }

    except Exception as exc:
        log.warning("Causation endpoint failed: {e}", e=str(exc))
        return {"error": str(exc), "causes": [], "total_causes": 0}


@router.get("/causation/suspicious")
async def get_suspicious_trades_endpoint(
    days: int = Query(90, ge=1, le=365, description="Look-back window in days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return trades flagged as potentially informed by non-public information.

    Detects:
      - Congressional trades with committee jurisdiction overlap + active legislation
      - Insider buys preceding government contract awards
      - Insider sells preceding earnings misses
    """
    try:
        from intelligence.causation import get_suspicious_trades as _suspicious

        engine = get_db_engine()
        trades = _suspicious(engine, days=days)

        return {
            "days": days,
            "suspicious_trades": trades[:200],
            "total": len(trades),
        }

    except Exception as exc:
        log.warning("Suspicious trades endpoint failed: {e}", e=str(exc))
        return {"error": str(exc), "suspicious_trades": [], "total": 0}


@router.get("/causation/narrative/{ticker}")
async def get_causal_narrative_endpoint(
    ticker: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Generate a narrative explaining why people are trading a specific ticker."""
    try:
        from intelligence.causation import generate_causal_narrative as _narrative

        engine = get_db_engine()
        narrative = _narrative(engine, ticker.strip().upper())

        return {
            "ticker": ticker.strip().upper(),
            "narrative": narrative,
        }

    except Exception as exc:
        log.warning(
            "Causal narrative for {t} failed: {e}", t=ticker, e=str(exc),
        )
        return {"ticker": ticker.strip().upper(), "narrative": "", "error": str(exc)}


# ── Causal Chain Endpoints ─────────────────────────────────────────────


@router.get("/causal-chains")
async def get_causal_chains(
    ticker: str | None = Query(None, description="Filter by ticker"),
    hops: int = Query(5, ge=2, le=10, description="Max hops for chain tracing"),
    days: int = Query(180, ge=1, le=730, description="Look-back window for longest chains"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Trace multi-hop causal chains for a ticker or find longest chains globally.

    Chains trace paths like: lobbying -> legislation -> contract award ->
    stock price move -> insider sale.

    If ticker is provided, traces chains for that ticker (up to `hops` deep).
    Otherwise, finds the longest chains across all tickers in the system.
    """
    try:
        from intelligence.causation import (
            trace_causal_chain,
            find_longest_chains,
        )

        engine = get_db_engine()

        if ticker:
            ticker_upper = ticker.strip().upper()
            chains = trace_causal_chain(engine, ticker_upper, max_hops=hops)
            return {
                "ticker": ticker_upper,
                "max_hops": hops,
                "chains": [c.to_dict() for c in chains[:50]],
                "total_chains": len(chains),
                "longest_chain": chains[0].total_hops if chains else 0,
            }

        # No ticker — find longest chains across all tickers
        chains = find_longest_chains(engine, days=days)
        return {
            "days": days,
            "chains": [c.to_dict() for c in chains[:100]],
            "total_chains": len(chains),
            "longest_chain": chains[0].total_hops if chains else 0,
            "tickers_covered": list({c.ticker for c in chains}),
        }

    except Exception as exc:
        log.warning("Causal chains endpoint failed: {e}", e=str(exc))
        return {"error": str(exc), "chains": [], "total_chains": 0}


@router.get("/causal-chains/active")
async def get_active_causal_chains(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Detect causal chains currently in progress.

    Identifies tickers where the early stages of a known causal pattern
    are unfolding — e.g., lobbying spend increase + legislative hearings
    scheduled + insider buying = something is coming.
    """
    try:
        from intelligence.causation import detect_chain_in_progress

        engine = get_db_engine()
        active = detect_chain_in_progress(engine)

        return {
            "active_patterns": active[:50],
            "total": len(active),
            "tickers_with_active_chains": list({p["ticker"] for p in active}),
        }

    except Exception as exc:
        log.warning("Active causal chains endpoint failed: {e}", e=str(exc))
        return {"error": str(exc), "active_patterns": [], "total": 0}


# ── Influence Network Endpoints ─────────────────────────────────────────

_influence_graph_cache: dict[str, Any] = {"data": None, "ts": None}
_INFLUENCE_GRAPH_TTL = 1800  # 30 minutes


@router.get("/influence")
async def get_influence_network(
    ticker: str | None = Query(None, description="Filter by ticker symbol"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return influence network data — the money-in-politics graph.

    Without ticker parameter: returns the full influence graph (cached 30 min).
    With ticker parameter: returns influence data for that specific company.
    """
    from datetime import datetime, timezone

    try:
        engine = get_db_engine()

        if ticker:
            from intelligence.influence_network import get_influence_for_ticker
            return get_influence_for_ticker(engine, ticker.strip().upper())

        # Full graph — cached
        now = datetime.now(timezone.utc)
        if (
            _influence_graph_cache["data"]
            and _influence_graph_cache["ts"]
            and (now - _influence_graph_cache["ts"]).total_seconds() < _INFLUENCE_GRAPH_TTL
        ):
            return _influence_graph_cache["data"]

        from intelligence.influence_network import build_influence_graph
        result = build_influence_graph(engine)
        _influence_graph_cache["data"] = result
        _influence_graph_cache["ts"] = now
        return result

    except Exception as exc:
        log.warning("Influence network endpoint failed: {e}", e=str(exc))
        return {"nodes": [], "links": [], "metadata": {}, "error": str(exc)}


@router.get("/influence/circular-flows")
async def get_circular_flows(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Detect circular flows: Company -> lobbies -> Member -> votes -> Bill -> funds -> Company."""
    try:
        from intelligence.influence_network import detect_circular_flows

        engine = get_db_engine()
        loops = detect_circular_flows(engine)
        return {
            "loops": [l.to_dict() for l in loops],
            "total": len(loops),
            "circular_count": sum(1 for l in loops if l.circular_flow_detected),
        }

    except Exception as exc:
        log.warning("Circular flows endpoint failed: {e}", e=str(exc))
        return {"loops": [], "total": 0, "circular_count": 0, "error": str(exc)}


@router.get("/influence/hypocrisy")
async def get_vote_trade_hypocrisy(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Detect vote/trade hypocrisy — members who vote one way but trade another."""
    try:
        from intelligence.influence_network import vote_trade_hypocrisy

        engine = get_db_engine()
        flags = vote_trade_hypocrisy(engine)
        return {
            "flags": flags,
            "total": len(flags),
            "members_flagged": len({f["member"] for f in flags}),
        }

    except Exception as exc:
        log.warning("Vote-trade hypocrisy endpoint failed: {e}", e=str(exc))
        return {"flags": [], "total": 0, "members_flagged": 0, "error": str(exc)}


# ── Export Controls Endpoints ──────────────────────────────────────────────


@router.get("/export-controls")
async def get_export_controls(
    ticker: str | None = Query(None, description="Filter by stock ticker (e.g. NVDA, ASML)"),
    days: int = Query(90, ge=1, le=730, description="Lookback days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return export control actions affecting semiconductor/tech companies.

    If ticker is provided, returns actions for that company only plus
    a revenue impact assessment. Otherwise returns all recent actions.

    For companies like NVIDIA, export controls to China have been more
    material than the CHIPS Act (~25% of revenue at risk).
    """
    try:
        from intelligence.export_intel import (
            get_recent_controls,
            get_controls_for_ticker,
            assess_revenue_impact,
        )

        engine = get_db_engine()

        if ticker:
            controls = get_controls_for_ticker(engine, ticker)
        else:
            controls = get_recent_controls(engine, days=days)

        result: dict[str, Any] = {
            "controls": [c.to_dict() for c in controls],
            "total": len(controls),
            "ticker": ticker,
            "days": days,
        }

        # Include revenue impact assessment when filtering by ticker
        if ticker:
            try:
                impact = assess_revenue_impact(engine, ticker)
                result["revenue_impact"] = impact
            except Exception as exc:
                log.debug("Revenue impact assessment failed: {e}", e=str(exc))
                result["revenue_impact"] = None

        return result

    except Exception as exc:
        log.warning("Export controls endpoint failed: {e}", e=str(exc))
        return {
            "controls": [],
            "total": 0,
            "ticker": ticker,
            "days": days,
            "error": str(exc),
        }


@router.get("/export-controls/impact")
async def get_export_control_impact(
    ticker: str = Query(..., description="Stock ticker (e.g. NVDA, ASML, LRCX)"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Assess revenue impact of export controls for a specific company.

    Returns estimated % of revenue at risk, active restriction count,
    severity assessment, and China revenue baseline data.
    """
    try:
        from intelligence.export_intel import assess_revenue_impact

        engine = get_db_engine()
        impact = assess_revenue_impact(engine, ticker)
        return impact

    except Exception as exc:
        log.warning("Export control impact endpoint failed: {e}", e=str(exc))
        return {
            "ticker": ticker,
            "risk_level": "UNKNOWN",
            "error": str(exc),
        }


# ── Company Analyzer Endpoints ──────────────────────────────────────────
# NOTE: Specific path routes (/companies/patterns, /companies/sector-report,
# /companies/analyze) MUST be registered before the parameterized
# /companies/{ticker} route to avoid FastAPI matching "patterns" as a ticker.


@router.get("/companies")
async def get_all_company_profiles(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return all analyzed company influence profiles, sorted by suspicion score.

    Each profile contains government contracts, congressional holdings,
    insider activity, lobbying, influence loops, and LLM narrative.
    """
    try:
        from intelligence.company_analyzer import get_all_profiles

        engine = get_db_engine()
        profiles = get_all_profiles(engine)
        return {
            "count": len(profiles),
            "profiles": [p.to_dict() for p in profiles],
        }

    except Exception as exc:
        log.warning("Company profiles endpoint failed: {e}", e=str(exc))
        return {"count": 0, "profiles": [], "error": str(exc)}


@router.get("/companies/patterns")
async def get_cross_company_patterns(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Detect cross-company influence patterns.

    Looks for sector-wide lobbying surges, coordinated insider selling,
    committee members with concentrated holdings, suspicion clusters,
    and government contract concentration.
    """
    try:
        from intelligence.company_analyzer import find_cross_company_patterns

        engine = get_db_engine()
        patterns = find_cross_company_patterns(engine)
        return {
            "count": len(patterns),
            "patterns": patterns,
        }

    except Exception as exc:
        log.warning("Cross-company patterns endpoint failed: {e}", e=str(exc))
        return {"count": 0, "patterns": [], "error": str(exc)}


@router.get("/companies/sector-report")
async def get_sector_influence_report(
    sector: str = Query(..., description="Sector name (e.g. Technology, Semiconductors)"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Generate an LLM narrative summarizing influence across a sector.

    Aggregates all company profiles in the sector and produces a
    multi-paragraph analysis of lobbying, contracts, insider activity,
    and suspicion patterns.
    """
    try:
        from intelligence.company_analyzer import generate_sector_influence_report

        engine = get_db_engine()
        report = generate_sector_influence_report(engine, sector)
        return {
            "sector": sector,
            "report": report,
        }

    except Exception as exc:
        log.warning("Sector report for {s} failed: {e}", s=sector, e=str(exc))
        return {"sector": sector, "report": "", "error": str(exc)}


@router.post("/companies/analyze")
async def trigger_company_analysis(
    ticker: str = Query(..., description="Stock ticker to analyze (e.g. AAPL, NVDA)"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Trigger full influence analysis for a single company.

    Queries all intelligence modules (gov contracts, lobbying, insider,
    congressional, export controls, actor network) and generates an
    LLM narrative. Results are stored in the company_profiles table.
    """
    try:
        from intelligence.company_analyzer import analyze_company

        engine = get_db_engine()
        profile = analyze_company(engine, ticker)
        return {
            "status": "analyzed",
            "profile": profile.to_dict(),
        }

    except Exception as exc:
        log.warning("Company analysis for {t} failed: {e}", t=ticker, e=str(exc))
        return {"status": "error", "ticker": ticker, "error": str(exc)}


@router.get("/companies/{ticker}")
async def get_company_profile(
    ticker: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the influence profile for a single company.

    If the company has not been analyzed yet, returns a 404-style response.
    Use POST /companies/analyze?ticker=AAPL to trigger analysis.
    """
    try:
        from intelligence.company_analyzer import get_all_profiles

        engine = get_db_engine()
        profiles = get_all_profiles(engine)
        ticker_upper = ticker.strip().upper()

        for p in profiles:
            if p.ticker == ticker_upper:
                return {"profile": p.to_dict()}

        return {"profile": None, "error": f"No analysis found for {ticker_upper}"}

    except Exception as exc:
        log.warning("Company profile for {t} failed: {e}", t=ticker, e=str(exc))
        return {"profile": None, "error": str(exc)}


# ── Deep Graph Endpoints ──────────────────────────────────────────────────

_deep_graph_cache: dict[str, Any] = {}
_DEEP_GRAPH_TTL = 900  # 15 minutes


@router.get("/deep-graph/{ticker}")
async def get_deep_graph(
    ticker: str,
    depth: int = Query(default=10, ge=1, le=10, description="Traversal depth (1-10)"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Drill 10 layers deep from a ticker to map the full actor network.

    Layer 1: Company -> Layer 2: Board/C-Suite -> Layer 3: Other affiliations ->
    Layer 4: Lobbyists -> Layer 5: Politicians -> Layer 6: Committees ->
    Layer 7: Affected companies -> Layer 8: Insiders -> Layer 9: Cross-holding funds ->
    Layer 10: Beneficial owners.

    At each layer: WHO, HOW MUCH money, WHEN, and CONNECTION TYPE.
    Capped at 1000 actors to prevent explosion.
    """
    import time
    from datetime import datetime, timezone

    cache_key = f"{ticker.upper()}:{depth}"
    now = datetime.now(timezone.utc)
    cached = _deep_graph_cache.get(cache_key)
    if cached and cached.get("ts") and (now - cached["ts"]).total_seconds() < _DEEP_GRAPH_TTL:
        return cached["data"]

    try:
        from intelligence.deep_graph import deep_drill

        engine = get_db_engine()
        t0 = time.time()
        result = deep_drill(engine, ticker, max_depth=depth)
        elapsed = time.time() - t0

        response = {
            "drill": result,
            "elapsed_seconds": round(elapsed, 2),
        }

        _deep_graph_cache[cache_key] = {"data": response, "ts": now}
        return response

    except Exception as exc:
        log.warning("Deep graph drill for {t} failed: {e}", t=ticker, e=str(exc))
        return {"drill": None, "error": str(exc)}


@router.get("/overlaps")
async def get_overlaps(
    ticker_a: str = Query(..., description="First ticker"),
    ticker_b: str = Query(..., description="Second ticker"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Find hidden connections between two seemingly unrelated tickers.

    Drills from both tickers independently and finds where the two graphs
    intersect — shared actors, committees, funds, and dollar flows.
    """
    import time

    try:
        from intelligence.deep_graph import find_overlaps

        engine = get_db_engine()
        t0 = time.time()
        overlaps = find_overlaps(engine, ticker_a, ticker_b)
        elapsed = time.time() - t0

        return {
            "ticker_a": ticker_a.upper(),
            "ticker_b": ticker_b.upper(),
            "overlaps": [o.to_dict() for o in overlaps],
            "count": len(overlaps),
            "elapsed_seconds": round(elapsed, 2),
        }

    except Exception as exc:
        log.warning(
            "Overlap detection {a} <-> {b} failed: {e}",
            a=ticker_a, b=ticker_b, e=str(exc),
        )
        return {"overlaps": [], "count": 0, "error": str(exc)}


@router.get("/overlaps/all")
async def get_all_overlaps(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Find all hidden connections across the entire watchlist.

    Runs pairwise overlap detection on all active watchlist tickers.
    "Your watchlist has 15 hidden connections you didn't know about."
    """
    import time

    try:
        from intelligence.deep_graph import find_all_overlaps

        engine = get_db_engine()
        t0 = time.time()
        overlaps = find_all_overlaps(engine)
        elapsed = time.time() - t0

        return {
            "overlaps": [o.to_dict() for o in overlaps],
            "count": len(overlaps),
            "elapsed_seconds": round(elapsed, 2),
        }

    except Exception as exc:
        log.warning("All-overlaps scan failed: {e}", e=str(exc))
        return {"overlaps": [], "count": 0, "error": str(exc)}


@router.get("/deep-graph/{ticker}/map")
async def get_connection_map(
    ticker: str,
    depth: int = Query(default=5, ge=1, le=10, description="Map depth (1-10)"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Generate a D3-ready connection map for a ticker.

    Returns nodes colored by layer depth with overlap nodes highlighted.
    Suitable for force-directed graph visualization.
    """
    try:
        from intelligence.deep_graph import generate_connection_map

        engine = get_db_engine()
        result = generate_connection_map(engine, ticker, depth=depth)
        return result

    except Exception as exc:
        log.warning("Connection map for {t} failed: {e}", t=ticker, e=str(exc))
        return {"nodes": [], "links": [], "metadata": {}, "error": str(exc)}


@router.get("/institutional-map")
async def get_institutional_map(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the full institutional map: private credit funds, hedge funds,
    pension systems, allocation links, revolving door, and conflicts of interest.

    This is the shadow banking layer -- where pension dollars flow through
    opaque fee structures into private credit and leveraged buyouts.
    """
    import time

    try:
        from intelligence.institutional_map import (
            build_institutional_graph,
            find_conflicts_of_interest,
            get_institutional_summary,
        )

        engine = get_db_engine()
        t0 = time.time()
        graph = build_institutional_graph(engine)
        summary = get_institutional_summary()
        elapsed = time.time() - t0

        return {
            **graph,
            "summary": summary,
            "elapsed_seconds": round(elapsed, 2),
        }

    except Exception as exc:
        log.warning("Institutional map failed: {e}", e=str(exc))
        return {"nodes": [], "links": [], "metadata": {}, "error": str(exc)}


@router.get("/institutional-map/trace/{pension_name}")
async def trace_pension(
    pension_name: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Trace where a specific pension fund's money ends up.

    Follow the dollars from beneficiary contributions through to fund managers,
    with fee extraction estimates at each step.
    """
    try:
        from intelligence.institutional_map import trace_pension_dollars

        result = trace_pension_dollars(pension_name)
        return result

    except Exception as exc:
        log.warning("Pension trace for {p} failed: {e}", p=pension_name, e=str(exc))
        return {"error": str(exc)}


@router.get("/institutional-map/fees/{fund_name}")
async def get_fund_fees(
    fund_name: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Estimate fee extraction for a specific fund from pension capital.

    Shows management fees, performance fees, passthrough fees, and
    10-year extraction projections.
    """
    try:
        from intelligence.institutional_map import get_fee_extraction_estimate

        result = get_fee_extraction_estimate(fund_name)
        return result

    except Exception as exc:
        log.warning("Fee estimate for {f} failed: {e}", f=fund_name, e=str(exc))
        return {"error": str(exc)}


@router.get("/institutional-map/conflicts")
async def get_institutional_conflicts(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return all detected conflicts of interest in the institutional map.

    Includes revolving door, pay-to-play, consultant conflicts,
    underfunded pension risk mismatches, and liquidity crises.
    """
    try:
        from intelligence.institutional_map import find_conflicts_of_interest

        conflicts = find_conflicts_of_interest()
        return {
            "conflicts": conflicts,
            "count": len(conflicts),
            "severity_breakdown": {
                "critical": len([c for c in conflicts if c.get("severity") == "critical"]),
                "high": len([c for c in conflicts if c.get("severity") == "high"]),
                "medium": len([c for c in conflicts if c.get("severity") == "medium"]),
                "low": len([c for c in conflicts if c.get("severity") == "low"]),
            },
        }

    except Exception as exc:
        log.warning("Conflict detection failed: {e}", e=str(exc))
        return {"conflicts": [], "count": 0, "error": str(exc)}


@router.get("/hidden-influence")
async def get_hidden_influence(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Discover hidden influence patterns across the watchlist.

    Cross-references deep graph overlaps with causal chains to find:
    - Actors connecting seemingly unrelated events
    - Committee influence over multiple positions
    - Fund concentration risk
    """
    import time

    try:
        from intelligence.deep_graph import discover_hidden_influence

        engine = get_db_engine()
        t0 = time.time()
        discoveries = discover_hidden_influence(engine)
        elapsed = time.time() - t0

        return {
            "discoveries": discoveries,
            "count": len(discoveries),
            "elapsed_seconds": round(elapsed, 2),
        }

    except Exception as exc:
        log.warning("Hidden influence discovery failed: {e}", e=str(exc))
        return {"discoveries": [], "count": 0, "error": str(exc)}


# ── Global Lever Map Endpoints ──────────────────────────────────────────

_lever_cache: dict[str, Any] = {"data": None, "ts": None}
_LEVER_CACHE_TTL = 600  # 10 minutes


@router.get("/levers")
async def get_levers(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the full global lever hierarchy — all 8 domains.

    Cached for 10 minutes.  Includes hierarchy, summaries, and cross-domain
    actor index.
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    if (
        _lever_cache["data"]
        and _lever_cache["ts"]
        and (now - _lever_cache["ts"]).total_seconds() < _LEVER_CACHE_TTL
    ):
        return _lever_cache["data"]

    try:
        from intelligence.global_levers import (
            get_lever_hierarchy,
            find_cross_domain_actors,
        )

        engine = get_db_engine()
        hierarchy = get_lever_hierarchy()
        cross_domain = find_cross_domain_actors(engine)

        result = {
            **hierarchy,
            "cross_domain_actors": cross_domain[:20],
        }

        _lever_cache["data"] = result
        _lever_cache["ts"] = now
        return result

    except Exception as exc:
        log.warning("Global lever map failed: {e}", e=str(exc))
        return {"error": str(exc), "hierarchy": {}}


@router.get("/levers/{domain}")
async def get_lever_domain_endpoint(
    domain: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return a single lever domain with full actor details."""
    try:
        from intelligence.global_levers import get_lever_domain

        return get_lever_domain(domain)

    except Exception as exc:
        log.warning("Lever domain lookup failed: {e}", e=str(exc))
        return {"error": str(exc)}


@router.get("/levers/chain/{event}")
async def trace_lever_chain_endpoint(
    event: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Trace the chain of effects from a named event.

    Example: /api/v1/intelligence/levers/chain/interest_rate_hike
    """
    try:
        from intelligence.global_levers import trace_lever_chain

        chain = trace_lever_chain(event)
        return {"event": event, "chain": chain, "steps": len(chain)}

    except Exception as exc:
        log.warning("Lever chain trace failed: {e}", e=str(exc))
        return {"error": str(exc), "chain": []}


@router.get("/levers/cross-domain")
async def get_cross_domain_actors_endpoint(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Find actors appearing in 2+ lever domains — the most powerful players."""
    try:
        from intelligence.global_levers import find_cross_domain_actors

        engine = get_db_engine()
        actors = find_cross_domain_actors(engine)
        return {"actors": actors, "count": len(actors)}

    except Exception as exc:
        log.warning("Cross-domain actor lookup failed: {e}", e=str(exc))
        return {"error": str(exc), "actors": []}


@router.get("/levers/report")
async def get_lever_report_endpoint(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Generate a narrative report: who's pulling what lever right now."""
    try:
        from intelligence.global_levers import generate_lever_report

        engine = get_db_engine()
        report = generate_lever_report(engine)
        return {"report": report}

    except Exception as exc:
        log.warning("Lever report generation failed: {e}", e=str(exc))
        return {"error": str(exc), "report": ""}
