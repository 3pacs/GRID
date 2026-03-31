"""AstroGrid sub-router: predictions, backtest, weights, review, learning-loop."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_astrogrid_store, get_db_engine
from api.routers.astrogrid_helpers import (
    AstrogridBacktestRequest,
    AstrogridLearningLoopRequest,
    AstrogridPredictionRequest,
    AstrogridReviewRequest,
    AstrogridScoreRequest,
    AstrogridWeightDecisionRequest,
    publish_astrogrid_prediction,
    _build_postmortem_stub,
    _classify_prediction_scoreability,
    _compact_prediction_snapshot,
    _infer_prediction_horizon,
    _infer_question_intent,
    _infer_target_group,
    _infer_target_symbols,
    _parse_optional_date,
    _prediction_confidence,
)

router = APIRouter(tags=["astrogrid"])


@router.post("/predictions")
async def create_prediction(
    req: AstrogridPredictionRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Persist an AstroGrid prediction and immediate postmortem stub."""
    store = get_astrogrid_store()
    stub = _build_postmortem_stub(req)
    horizon = _infer_prediction_horizon(req)
    target_symbols = _infer_target_symbols(req)
    question_intent = _infer_question_intent(req, target_symbols)
    target_group = _infer_target_group(target_symbols, req)
    scoring_class, target_statuses = _classify_prediction_scoreability(target_symbols)
    confidence = _prediction_confidence(req)

    market_overlay_snapshot = dict(req.market_overlay_snapshot or {})
    scorecard_overlay = dict(market_overlay_snapshot.get("scorecard") or {})
    scorecard_overlay["target_statuses"] = target_statuses
    scorecard_overlay["target_group"] = target_group
    market_overlay_snapshot["scorecard"] = scorecard_overlay
    market_overlay_snapshot["question_intent"] = question_intent
    market_overlay_snapshot["target_group"] = target_group

    oracle_publish_result: dict[str, Any] = {"status": "not_attempted"}
    publish_payload: dict[str, Any] = {
        "prediction_id": None,
        "question": req.question,
        "target_universe": req.target_universe,
        "question_intent": question_intent,
        "target_group": target_group,
        "scoring_class": scoring_class,
        "target_symbols": target_symbols,
        "horizon_label": horizon,
        "call": req.call,
        "timing": req.timing,
        "invalidation": req.invalidation,
        "confidence": confidence,
        "weight_version": req.weight_version,
        "model_version": req.model_version,
        "grid_summary": stub["grid_summary"],
        "mystical_summary": stub["mystical_summary"],
    }

    prediction_payload: dict[str, Any] = {
        "as_of_ts": req.as_of_ts or datetime.now(timezone.utc).isoformat(),
        "question": req.question,
        "call": req.call,
        "timing": req.timing,
        "setup": req.setup,
        "invalidation": req.invalidation,
        "note": req.note,
        "mode": req.mode,
        "lens_ids": req.lens_ids,
        "snapshot": req.snapshot,
        "seer_summary": (
            (req.seer or {}).get("prediction") or (req.seer or {}).get("reading")
        ),
        "market_overlay_snapshot": market_overlay_snapshot,
        "mystical_feature_payload": {
            "seer": req.seer,
            "engine_outputs": req.engine_outputs,
            "snapshot": _compact_prediction_snapshot(req.snapshot or {}),
        },
        "grid_feature_payload": market_overlay_snapshot,
        "weight_version": req.weight_version,
        "model_version": req.model_version,
        "live_or_local": req.live_or_local,
        "status": "pending",
        "target_universe": req.target_universe,
        "question_intent": question_intent,
        "target_group": target_group,
        "scoring_class": scoring_class,
        "target_symbols": target_symbols,
        "horizon_label": horizon,
        "postmortem_summary": stub["summary"],
        "dominant_grid_drivers": stub["dominant_grid_drivers"],
        "dominant_mystical_drivers": stub["dominant_mystical_drivers"],
        "feature_family_summary": stub["feature_family_summary"],
        "postmortem_raw_payload": {
            "question": req.question,
            "question_intent": question_intent,
            "target_group": target_group,
            "call": req.call,
            "timing": req.timing,
            "setup": req.setup,
            "invalidation": req.invalidation,
            "note": req.note,
            "seer": req.seer,
            "engine_outputs": req.engine_outputs,
            "market_overlay": market_overlay_snapshot,
        },
    }
    prediction_payload["feature_family_summary"]["question_intent"] = question_intent
    prediction_payload["feature_family_summary"]["target_group"] = target_group
    prediction_payload["prediction_id"] = str(uuid4())

    if req.publish_oracle:
        try:
            publish_payload["prediction_id"] = prediction_payload["prediction_id"]
            oracle_publish_result = publish_astrogrid_prediction(
                get_db_engine(), publish_payload
            )
        except Exception as exc:
            oracle_publish_result = {
                "status": "failed",
                "error": str(exc),
                "contract": "oracle.publish.v1",
            }
            log.warning("AstroGrid Oracle publish failed: {e}", e=str(exc))

    prediction_payload["oracle_publish"] = oracle_publish_result
    record = store.save_prediction(prediction_payload)
    if not record:
        return {"error": "Prediction persistence failed."}
    return record


@router.get("/predictions/latest")
async def get_latest_predictions(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    return {
        "predictions": get_astrogrid_store().list_predictions(limit=limit, offset=offset),
        "limit": limit,
        "offset": offset,
    }


@router.get("/postmortems")
async def get_postmortems(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    return {
        "postmortems": get_astrogrid_store().list_postmortems(limit=limit, offset=offset),
        "limit": limit,
        "offset": offset,
    }


@router.post("/predictions/score")
async def score_predictions(
    req: AstrogridScoreRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    as_of_date = None
    if req.as_of_date:
        try:
            as_of_date = _parse_optional_date(req.as_of_date)
        except ValueError:
            return {"error": f"Invalid date format: {req.as_of_date}. Use YYYY-MM-DD."}
    return get_astrogrid_store().score_predictions(
        as_of_date=as_of_date,
        limit=max(1, min(req.limit, 500)),
        prediction_ids=req.prediction_ids or None,
    )


@router.get("/predictions/scoreboard")
async def get_prediction_scoreboard(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    store = get_astrogrid_store()
    return {
        "scoreboard": store.build_prediction_scoreboard(),
        "weights": store.ensure_active_weight_version(),
    }


@router.post("/backtest/run")
async def run_backtest(
    req: AstrogridBacktestRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    try:
        window_start = _parse_optional_date(req.window_start)
        window_end = _parse_optional_date(req.window_end)
    except ValueError as exc:
        return {"error": str(exc)}
    return get_astrogrid_store().run_backtests(
        strategy_variants=req.strategy_variants,
        horizon_label=req.horizon_label,
        window_start=window_start,
        window_end=window_end,
        limit=max(1, min(req.limit, 1000)),
    )


@router.get("/backtest/summary")
async def get_backtest_summary(
    limit: int = Query(default=12, ge=1, le=100),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    return get_astrogrid_store().get_backtest_summary(limit=limit)


@router.get("/backtest/results")
async def get_backtest_results(
    strategy_variant: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    return {
        "results": get_astrogrid_store().list_backtest_results(
            strategy_variant=strategy_variant,
            limit=limit,
        ),
        "strategy_variant": strategy_variant,
    }


@router.get("/weights/current")
async def get_current_weights(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    return get_astrogrid_store().ensure_active_weight_version()


@router.post("/review/generate")
async def generate_review_run(
    req: AstrogridReviewRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    return get_astrogrid_store().generate_review_run(
        provider_mode=req.provider_mode,
        prediction_limit=max(1, min(req.prediction_limit, 1000)),
        backtest_limit=max(1, min(req.backtest_limit, 100)),
    )


@router.post("/learning-loop/run")
async def run_learning_loop(
    req: AstrogridLearningLoopRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    try:
        as_of_date = _parse_optional_date(req.as_of_date)
    except ValueError as exc:
        return {"error": str(exc)}
    return get_astrogrid_store().run_learning_loop(
        as_of_date=as_of_date,
        score_limit=max(1, min(req.score_limit, 1000)),
        backtest_limit=max(1, min(req.backtest_limit, 2000)),
        backtest_window_days=max(7, min(req.backtest_window_days, 3650)),
        provider_mode=req.provider_mode,
        horizon_label=req.horizon_label,
    )


@router.get("/review/latest")
async def get_latest_review(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    review = get_astrogrid_store().get_latest_review()
    if not review:
        return {"error": "No review run available yet."}
    return review


@router.get("/weights/proposals")
async def get_weight_proposals(
    status: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    return {
        "proposals": get_astrogrid_store().list_weight_proposals(
            status=status, limit=limit
        ),
        "status": status,
    }


@router.post("/weights/proposals/{weight_proposal_id}/approve")
async def approve_weight_proposal(
    weight_proposal_id: str,
    req: AstrogridWeightDecisionRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    proposal = get_astrogrid_store().approve_weight_proposal(
        weight_proposal_id,
        decided_by=req.decided_by,
        notes=req.notes,
    )
    if not proposal:
        return {"error": f"Weight proposal not found: {weight_proposal_id}"}
    return proposal


@router.post("/weights/proposals/{weight_proposal_id}/reject")
async def reject_weight_proposal(
    weight_proposal_id: str,
    req: AstrogridWeightDecisionRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    proposal = get_astrogrid_store().reject_weight_proposal(
        weight_proposal_id,
        decided_by=req.decided_by,
        notes=req.notes,
    )
    if not proposal:
        return {"error": f"Weight proposal not found: {weight_proposal_id}"}
    return proposal


@router.get("/predictions/{prediction_id}")
async def get_prediction_detail(
    prediction_id: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    record = get_astrogrid_store().get_prediction(prediction_id)
    if not record:
        return {"error": f"Prediction not found: {prediction_id}"}
    return record
