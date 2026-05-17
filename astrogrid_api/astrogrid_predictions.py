"""AstroGrid sub-router: predictions, backtest, weights, review, learning-loop."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, Query, Request
from loguru import logger as log

from astrogrid_api.auth import require_auth, verify_token
from astrogrid_api.dependencies import get_astrogrid_store, get_db_engine
from astrogrid_api.astrogrid_helpers import (
    AstrogridBacktestRequest,
    AstrogridGuruRequest,
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

_GURU_BULLISH_FALLBACK_CANDIDATES = {
    "crypto": ["BTC", "ETH", "SOL"],
    "equity": ["AAPL", "MSFT", "GOOGL", "NVDA", "META"],
    "macro": ["SPY", "QQQ", "GLD", "TLT", "DXY", "CL"],
    "hybrid": ["BTC", "ETH", "NVDA", "AAPL", "MSFT", "GOOGL", "META", "SOL"],
}

_GURU_BEARISH_FALLBACK_CANDIDATES = {
    "crypto": ["BTC", "ETH", "SOL"],
    "equity": ["AAPL", "MSFT", "GOOGL", "NVDA", "META"],
    "macro": ["SPY", "QQQ", "TLT", "GLD", "DXY", "CL"],
    "hybrid": ["BTC", "ETH", "NVDA", "META", "SOL"],
}


def _build_actor_context(
    *,
    actor_type: str,
    token_present: bool,
    token_valid: bool,
    request: Request | None = None,
) -> dict[str, Any]:
    headers = request.headers if request is not None else {}
    forwarded_for = headers.get("x-forwarded-for", "") if headers else ""
    client_host = getattr(getattr(request, "client", None), "host", None) if request is not None else None
    request_path = str(getattr(getattr(request, "url", None), "path", "") or "")
    publish_allowed = actor_type in {"authenticated_user", "operator_seed"}
    actor_context = {
        "actor_type": actor_type,
        "auth_state": "authenticated" if token_valid else ("token_present_invalid" if token_present else "anonymous"),
        "publish_allowed": publish_allowed,
        "publish_policy": "allowed" if publish_allowed else "suppressed_anonymous_public",
        "request_path": request_path,
        "client_ip": client_host,
        "forwarded_for": forwarded_for,
        "abuse_flags": [],
        "rate_limited": False,
    }
    if actor_context["auth_state"] == "token_present_invalid":
        actor_context["abuse_flags"].append("invalid_token")
    return actor_context


def _ranked_overlay_items(
    overlay: dict[str, Any],
    target_symbols: list[str],
    target_group: str,
    *,
    reverse: bool,
) -> list[dict[str, Any]]:
    scorecard = overlay.get("scorecard") if isinstance(overlay, dict) else {}
    source_items: list[dict[str, Any]] = []
    if isinstance(scorecard, dict):
        for key in ("items", "leaders", "laggards"):
            for item in scorecard.get(key) or []:
                if isinstance(item, dict) and item.get("symbol"):
                    source_items.append(item)
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    allowed = {symbol.upper() for symbol in target_symbols}
    for item in source_items:
        symbol = str(item.get("symbol") or "").upper()
        if not symbol or symbol in seen:
            continue
        item_group = str(item.get("group") or "").lower()
        if allowed and symbol not in allowed:
            continue
        if not allowed and target_group not in {"", "hybrid"} and item_group and item_group != target_group:
            continue
        seen.add(symbol)
        deduped.append(item)
    return sorted(
        deduped,
        key=lambda item: float(item.get("momentum_score") or item.get("score") or 0),
        reverse=reverse,
    )


def _build_guru_directive(req: AstrogridGuruRequest) -> tuple[AstrogridPredictionRequest, dict[str, Any]]:
    seed = AstrogridPredictionRequest(
        question=req.question,
        call="read field",
        timing="now",
        setup="pending Guru read",
        invalidation="break if the scored target violates its threshold",
        as_of_ts=req.as_of_ts,
        mode=req.mode,
        lens_ids=req.lens_ids,
        snapshot=req.snapshot,
        seer=req.seer,
        engine_outputs=req.engine_outputs,
        market_overlay_snapshot=req.market_overlay_snapshot,
        target_universe=req.target_universe,
        target_symbols=req.target_symbols,
        horizon_label=req.horizon_label,
        weight_version=req.weight_version,
        model_version=req.model_version,
        live_or_local=req.live_or_local,
        publish_oracle=req.publish_oracle,
    )
    candidate_symbols = _infer_target_symbols(seed)
    target_group = _infer_target_group(candidate_symbols, seed)
    question_intent = _infer_question_intent(seed, candidate_symbols)
    resolved_candidates = list(candidate_symbols)
    if not resolved_candidates:
        fallback_candidates = (
            _GURU_BEARISH_FALLBACK_CANDIDATES
            if question_intent == "avoid_now"
            else _GURU_BULLISH_FALLBACK_CANDIDATES
        )
        resolved_candidates = list(
            fallback_candidates.get(target_group)
            or fallback_candidates["hybrid"]
        )
    horizon = _infer_prediction_horizon(seed)
    overlay = dict(req.market_overlay_snapshot or {})
    bullish_items = _ranked_overlay_items(overlay, resolved_candidates, target_group, reverse=True)
    bearish_items = _ranked_overlay_items(overlay, resolved_candidates, target_group, reverse=False)
    top = bullish_items[0] if bullish_items else {}
    weak = bearish_items[0] if bearish_items else {}
    asset_label = "crypto" if target_group == "crypto" else "equity" if target_group == "equity" else "market"
    generic_targets = {"", "HYBRID", "CRYPTO", "EQUITY", "MACRO", "MARKET"}

    def selected_ref(item: dict[str, Any], *, bearish: bool = False) -> str:
        fallback = (
            resolved_candidates[-1]
            if bearish and len(resolved_candidates) > 1
            else (resolved_candidates[0] if resolved_candidates else target_group)
        )
        return str(item.get("symbol") or fallback).upper()

    def contract_symbols_for(selected: str) -> list[str]:
        if selected and selected not in {"", "HYBRID", "CRYPTO", "EQUITY", "MACRO", "MARKET"}:
            return [selected]
        if resolved_candidates:
            return [resolved_candidates[0]]
        return []

    def comparison_symbols_for(selected: str) -> list[str]:
        return [symbol for symbol in resolved_candidates if symbol != selected][:4]

    def support_line(symbol: str, compared: list[str]) -> str:
        line = f"{symbol} has the cleanest mapped relative-strength read in the current {asset_label} sleeve"
        if compared:
            line += f" over {', '.join(compared)}"
        return line

    def avoid_line(symbol: str, compared: list[str]) -> str:
        line = f"{symbol} is the weakest mapped leg in the current {asset_label} sleeve"
        if compared:
            line += f" versus {', '.join(compared)}"
        return line

    def buy_invalidation(symbol: str) -> str:
        if target_group == "crypto":
            return f"exit if {symbol} closes down 4% from entry on swing or loses 8% on the macro frame"
        if target_group == "equity":
            return f"exit if {symbol} loses the prior swing low or closes down 3% from entry"
        return f"exit if {symbol} breaks the prior swing low and fails to recover"

    def wait_invalidation(symbol: str) -> str:
        if target_group == "crypto":
            return f"cancel the long setup if {symbol} loses the current base before reclaiming momentum"
        if target_group == "equity":
            return f"cancel the long setup if {symbol} loses support before confirming the breakout"
        return f"cancel the entry if {symbol} loses support before confirming strength"

    def avoid_invalidation(symbol: str) -> str:
        if target_group == "crypto":
            return f"break the avoid call only if {symbol} reclaims momentum and survives a 5% retest"
        if target_group == "equity":
            return f"break the avoid call only if {symbol} reclaims trend and closes back above resistance"
        return f"break the avoid call only if {symbol} reclaims trend and holds the retest"

    if question_intent == "avoid_now":
        selected = selected_ref(weak, bearish=True)
        trade_symbol = contract_symbols_for(selected)[0] if contract_symbols_for(selected) else selected
        compared_symbols = comparison_symbols_for(trade_symbol)
        call = f"fade {trade_symbol}" if trade_symbol not in generic_targets else "fade the weakest mapped laggard"
        setup = f"{avoid_line(trade_symbol, compared_symbols)}. Do not add fresh long exposure while the weakness persists."
        invalidation = avoid_invalidation(trade_symbol)
    elif question_intent in {"timing_entry", "buy_or_wait"}:
        selected = selected_ref(top)
        trade_symbol = contract_symbols_for(selected)[0] if contract_symbols_for(selected) else selected
        compared_symbols = comparison_symbols_for(trade_symbol)
        call = f"buy {trade_symbol} on confirmation" if trade_symbol not in generic_targets else "buy the leader on confirmation"
        setup = f"{support_line(trade_symbol, compared_symbols)}. Entry stays inactive until price confirms the move."
        invalidation = wait_invalidation(trade_symbol)
    else:
        selected = selected_ref(top)
        trade_symbol = contract_symbols_for(selected)[0] if contract_symbols_for(selected) else selected
        compared_symbols = comparison_symbols_for(trade_symbol)
        call = f"buy {trade_symbol}" if trade_symbol not in generic_targets else "press the best mapped leader"
        setup = support_line(trade_symbol, compared_symbols)
        invalidation = buy_invalidation(trade_symbol)

    contract_symbols = contract_symbols_for(trade_symbol)
    contract = {
        "target_symbol": contract_symbols[0] if contract_symbols else None,
        "candidate_symbols": resolved_candidates[:6],
        "comparison_symbols": comparison_symbols_for(contract_symbols[0]) if contract_symbols else resolved_candidates[:5],
        "direction": "bearish" if question_intent == "avoid_now" else "bullish",
        "entry_style": "conditional" if question_intent in {"timing_entry", "buy_or_wait"} else "spot",
    }
    overlay["guru_contract"] = contract

    if horizon == "swing":
        timing = "7d swing window" if target_group == "crypto" else "10d swing window"
    else:
        timing = "30d macro window" if target_group == "crypto" else "45d macro window"
    seer_line = (
        (req.seer or {}).get("prediction")
        or (req.seer or {}).get("reading")
        or "mystical layer is advisory until it earns weight"
    )
    note = (
        f"Guru contract: {question_intent}; target: {contract.get('target_symbol') or target_group}; "
        f"candidates: {', '.join(resolved_candidates[:4]) if resolved_candidates else 'none'}; "
        f"{str(seer_line)[:120]}"
    )

    prediction_req = AstrogridPredictionRequest(
        question=req.question,
        call=call,
        timing=timing,
        setup=setup,
        invalidation=invalidation,
        as_of_ts=req.as_of_ts,
        note=note,
        mode=req.mode,
        lens_ids=req.lens_ids,
        snapshot=req.snapshot,
        seer=req.seer,
        engine_outputs=req.engine_outputs,
        market_overlay_snapshot=overlay,
        target_universe=req.target_universe,
        target_symbols=contract_symbols,
        horizon_label=horizon,
        weight_version=req.weight_version,
        model_version=req.model_version,
        live_or_local=req.live_or_local,
        publish_oracle=req.publish_oracle,
    )
    return prediction_req, {
        "call": call,
        "timing": timing,
        "setup": setup,
        "invalidation": invalidation,
        "note": note,
        "question_intent": question_intent,
        "target_group": target_group,
        "target_symbols": contract_symbols,
        "contract": contract,
        "horizon": horizon,
        "disclaimer": "Entertainment and research only. Not financial advice.",
    }


def _persist_prediction(
    req: AstrogridPredictionRequest,
    *,
    actor_context: dict[str, Any] | None = None,
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

    actor_context = dict(actor_context or {})
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

    public_publish_allowed = bool(actor_context.get("publish_allowed"))
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
            "actor_context": actor_context,
        },
    }
    prediction_payload["feature_family_summary"]["question_intent"] = question_intent
    prediction_payload["feature_family_summary"]["target_group"] = target_group
    prediction_payload["feature_family_summary"]["actor_type"] = actor_context.get("actor_type")
    prediction_payload["prediction_id"] = str(uuid4())
    market_overlay_snapshot["actor_context"] = actor_context
    prediction_payload["market_overlay_snapshot"] = market_overlay_snapshot
    prediction_payload["mystical_feature_payload"]["actor_context"] = actor_context
    prediction_payload["grid_feature_payload"] = market_overlay_snapshot

    if req.publish_oracle and public_publish_allowed:
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
    elif req.publish_oracle and not public_publish_allowed:
        oracle_publish_result = {
            "status": "not_attempted",
            "contract": "oracle.publish.v1",
            "reason": "anonymous_public_suppressed",
        }

    prediction_payload["oracle_publish"] = oracle_publish_result
    record = store.save_prediction(prediction_payload)
    if not record:
        return {"error": "Prediction persistence failed."}
    record["persistence_status"] = "persisted"
    record["persistence_actor"] = actor_context.get("actor_type", "unknown")
    return record


@router.post("/predictions")
async def create_prediction(
    req: AstrogridPredictionRequest,
    request: Request,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    actor_context = _build_actor_context(
        actor_type="authenticated_user",
        token_present=True,
        token_valid=True,
        request=request,
    )
    return _persist_prediction(req, actor_context=actor_context)


@router.post("/guru/ask")
async def ask_guru(
    req: AstrogridGuruRequest,
    request: Request,
) -> dict[str, Any]:
    """Answer a plain Guru question and persist it into the AstroGrid ledger."""
    prediction_req, answer = _build_guru_directive(req)
    auth_header = request.headers.get("authorization") or ""
    token = auth_header.removeprefix("Bearer").strip() if auth_header.lower().startswith("bearer") else ""
    if not token:
        token = request.query_params.get("token") or ""
    token_valid = bool(token and verify_token(token))
    actor_context = _build_actor_context(
        actor_type="authenticated_user" if token_valid else "anonymous_public",
        token_present=bool(token),
        token_valid=token_valid,
        request=request,
    )
    record = _persist_prediction(prediction_req, actor_context=actor_context)
    if record.get("error"):
        return {
            "answer": answer,
            "prediction": None,
            "postmortem": None,
            "disclaimer": answer["disclaimer"],
            "persistence_status": "failed",
            "error": record["error"],
        }
    return {
        "answer": answer,
        "prediction": record,
        "postmortem": record.get("postmortem"),
        "disclaimer": answer["disclaimer"],
        "persistence_status": record.get("persistence_status", "persisted"),
    }


@router.get("/predictions/latest")
async def get_latest_predictions(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
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
async def get_current_weights() -> dict[str, Any]:
    store = get_astrogrid_store()
    current = store.ensure_active_weight_version()
    latest_review = store.get_latest_review() or {}
    review_payload = latest_review.get("review") if isinstance(latest_review.get("review"), dict) else {}
    return {
        **current,
        "best_variant_by_group": dict(review_payload.get("best_variant_by_group") or {}),
        "group_conditionals": list(review_payload.get("group_conditionals") or []),
    }


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
async def get_latest_review() -> dict[str, Any]:
    review = get_astrogrid_store().get_latest_review()
    if not review:
        return {"error": "No review run available yet."}
    review_payload = review.get("review") if isinstance(review.get("review"), dict) else {}
    review["best_variant_by_group"] = dict(review_payload.get("best_variant_by_group") or {})
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
) -> dict[str, Any]:
    record = get_astrogrid_store().get_prediction(prediction_id)
    if not record:
        return {"error": f"Prediction not found: {prediction_id}"}
    return record
