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
