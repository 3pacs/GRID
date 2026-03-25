"""Model comparison and drift monitoring endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine, get_pit_store

router = APIRouter(prefix="/api/v1/model-comparison", tags=["model-comparison"])


@router.get("/shadow-vs-production")
async def shadow_vs_production(
    layer: str = Query(default="REGIME"),
    days_back: int = Query(default=30, ge=1, le=365),
    _token: str = Depends(require_auth),
) -> dict:
    """Compare SHADOW model predictions vs PRODUCTION over recent period."""
    engine = get_db_engine()

    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT shadow_model_id, production_state, shadow_state, "
            "production_confidence, shadow_confidence, agreement, as_of_date "
            "FROM shadow_scores ss "
            "JOIN model_registry mr ON ss.production_model_id = mr.id "
            "WHERE mr.layer = :layer AND ss.as_of_date >= CURRENT_DATE - :days "
            "ORDER BY ss.as_of_date DESC"
        ), {"layer": layer, "days": days_back}).fetchall()

    if not rows:
        return {"message": "No shadow scores found", "comparisons": []}

    total = len(rows)
    agreements = sum(1 for r in rows if r[5])
    agreement_rate = agreements / total if total > 0 else 0

    import numpy as np
    prod_conf = np.mean([float(r[3]) for r in rows])
    shadow_conf = np.mean([float(r[4]) for r in rows])

    return {
        "layer": layer,
        "days_back": days_back,
        "total_scores": total,
        "agreement_rate": round(agreement_rate, 4),
        "production_avg_confidence": round(float(prod_conf), 4),
        "shadow_avg_confidence": round(float(shadow_conf), 4),
        "confidence_delta": round(float(shadow_conf - prod_conf), 4),
    }


@router.get("/drift-report/{model_id}")
async def drift_report(
    model_id: int,
    _token: str = Depends(require_auth),
) -> dict:
    """Get comprehensive drift report for a model."""
    from features.importance import FeatureImportanceTracker

    engine = get_db_engine()
    pit_store = get_pit_store()
    tracker = FeatureImportanceTracker(db_engine=engine, pit_store=pit_store)

    try:
        report = tracker.get_comprehensive_drift_report(model_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Drift analysis failed: {exc}")

    return report


@router.get("/metrics")
async def metrics_comparison(
    model_ids: str = Query(description="Comma-separated model IDs"),
    _token: str = Depends(require_auth),
) -> dict:
    """Side-by-side validation metrics for multiple models."""
    engine = get_db_engine()
    ids = [int(x.strip()) for x in model_ids.split(",") if x.strip().isdigit()]

    if not ids:
        raise HTTPException(status_code=400, detail="Provide comma-separated model IDs")

    results = []
    with engine.connect() as conn:
        for mid in ids[:5]:
            model_row = conn.execute(text(
                "SELECT id, name, version, state, model_type, layer FROM model_registry WHERE id = :id"
            ), {"id": mid}).fetchone()

            if not model_row:
                continue

            val_row = conn.execute(text(
                "SELECT overall_verdict, full_period_metrics, era_results "
                "FROM validation_results WHERE model_version_id = :mid "
                "ORDER BY run_timestamp DESC LIMIT 1"
            ), {"mid": mid}).fetchone()

            art_row = conn.execute(text(
                "SELECT training_metrics FROM model_artifacts "
                "WHERE model_id = :mid ORDER BY trained_at DESC LIMIT 1"
            ), {"mid": mid}).fetchone()

            result = {
                "model_id": model_row[0],
                "name": model_row[1],
                "version": model_row[2],
                "state": model_row[3],
                "model_type": model_row[4],
                "layer": model_row[5],
            }

            if val_row:
                import json
                metrics = json.loads(val_row[1]) if isinstance(val_row[1], str) else (val_row[1] or {})
                result["validation_verdict"] = val_row[0]
                result["sharpe"] = metrics.get("sharpe")
                result["max_drawdown"] = metrics.get("max_drawdown")
                result["annualized_return"] = metrics.get("annualized_return", metrics.get("return"))

            if art_row:
                import json
                t_metrics = json.loads(art_row[0]) if isinstance(art_row[0], str) else (art_row[0] or {})
                result["training_accuracy"] = t_metrics.get("avg_accuracy")
                result["training_confidence"] = t_metrics.get("avg_confidence")

            results.append(result)

    return {"models": results}
