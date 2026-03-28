"""Cross-reference intelligence endpoints — lie detector for government statistics."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/intelligence", tags=["intelligence"])


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
