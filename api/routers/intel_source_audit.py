"""Source audit endpoints — track accuracy, redundancy, and discrepancies across data sources."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/intelligence", tags=["intelligence", "source-audit"])


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
