"""
Signal Registry, Model Factory & Ensemble API endpoints.
"""

from __future__ import annotations
from datetime import datetime, timezone
from typing import Any
from fastapi import APIRouter, Depends, HTTPException, Query, status
from loguru import logger as log
from pydantic import BaseModel, Field, field_validator
from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1", tags=["signal-registry"])


class EnsemblePredictRequest(BaseModel):
    ticker: str = Field(..., min_length=1, max_length=20)
    regime: str | None = None

    @field_validator("ticker")
    @classmethod
    def _norm(cls, v: str) -> str:
        s = v.strip().upper()
        if not s: raise ValueError("ticker must not be blank")
        return s


def _ser(row: dict) -> dict:
    return {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in row.items()}


def _spec_dict(s: Any) -> dict:
    wc = s.weight_config
    return {
        "name": s.name, "version": s.version, "description": s.description,
        "signal_sources": s.signal_sources, "signal_filters": s.signal_filters,
        "weight_config": {"mode": wc.mode, "half_life": wc.trust_decay_half_life_days,
                          "min_weight": wc.min_weight, "max_weight": wc.max_weight},
        "prediction_type": s.prediction_type, "target_horizon_days": s.target_horizon_days,
        "min_signals": s.min_signals, "active": s.active,
        "created_by": s.created_by, "parent_model": s.parent_model,
    }


# ── Signal Registry ──────────────────────────────────────────────────────

@router.get("/signals/registry")
async def list_signals(
    ticker: str | None = Query(None),
    source_module: str | None = Query(None),
    signal_type: str | None = Query(None),
    limit: int = Query(200, ge=1, le=1000),
):
    from intelligence.signal_registry import SignalRegistry, SignalType
    parsed_type = None
    if signal_type:
        try: parsed_type = SignalType(signal_type.strip().upper())
        except ValueError:
            raise HTTPException(422, f"Invalid signal_type '{signal_type}'. Valid: {[e.value for e in SignalType]}")
    rows = SignalRegistry.query(get_db_engine(), ticker=ticker, source_module=source_module,
                                signal_type=parsed_type, limit=limit)
    return {"count": len(rows), "signals": [_ser(r) for r in rows]}


@router.get("/signals/registry/stats")
async def signal_stats():
    from intelligence.signal_registry import SignalRegistry
    counts = SignalRegistry.get_signal_count(get_db_engine())
    return {"total": sum(counts.values()), "by_source": counts, "as_of": datetime.now(timezone.utc).isoformat()}


@router.get("/signals/registry/ticker/{ticker}")
async def signals_for_ticker(ticker: str, limit: int = Query(100, ge=1, le=500)):
    from intelligence.signal_registry import SignalRegistry
    t = ticker.strip().upper()
    rows = SignalRegistry.query_for_ticker(t, get_db_engine(), limit=limit)
    return {"ticker": t, "count": len(rows), "signals": [_ser(r) for r in rows]}


@router.post("/signals/registry/refresh")
async def refresh_registry(_token: str = Depends(require_auth)):
    from intelligence.adapters import ALL_ADAPTERS
    from intelligence.adapters.base import AdapterRegistry
    from intelligence.signal_registry import SignalRegistry
    reg = AdapterRegistry([cls() for cls in ALL_ADAPTERS])
    results = reg.refresh_all(get_db_engine())
    pruned = SignalRegistry.prune_expired(get_db_engine(), days_old=7)
    return {"total_registered": sum(results.values()), "by_adapter": results, "pruned": pruned}


# ── Model Factory ────────────────────────────────────────────────────────

@router.get("/oracle/factory")
async def list_models():
    from oracle.model_factory import ModelFactory
    f = ModelFactory(get_db_engine())
    specs = f.list_active_models()
    return {"count": len(specs), "models": [_spec_dict(s) for s in specs]}


@router.get("/oracle/factory/{model_name}")
async def get_model(model_name: str):
    from oracle.model_factory import ModelFactory
    f = ModelFactory(get_db_engine())
    try:
        spec = f.get_model_spec(model_name)
    except KeyError:
        raise HTTPException(404, f"Model '{model_name}' not found")
    result = _spec_dict(spec)
    try:
        signals = f.get_signals_for_model(model_name, datetime.now(timezone.utc))
        result["live_signal_count"] = len(signals)
    except Exception:
        result["live_signal_count"] = None
    return result


# ── Ensemble ─────────────────────────────────────────────────────────────

@router.post("/ensemble/predict")
async def ensemble_predict(body: EnsemblePredictRequest, _token: str = Depends(require_auth)):
    from oracle.model_factory import ModelFactory
    from oracle.signal_aggregator import SignalAggregator

    engine = get_db_engine()
    as_of = datetime.now(timezone.utc)
    factory = ModelFactory(engine)
    specs = factory.list_active_models()
    if not specs:
        raise HTTPException(404, "No active Oracle models")

    agg = SignalAggregator()
    model_results = []
    for spec in specs:
        try:
            raw = factory.get_signals_for_model(spec.name, as_of)
            ticker_sigs = [s for s in raw if s.get("ticker") is None or s.get("ticker") == body.ticker]
            if len(ticker_sigs) < spec.min_signals:
                model_results.append({"model": spec.name, "status": "insufficient_signals", "count": len(ticker_sigs)})
                continue
            a = agg.aggregate(ticker_sigs, spec.weight_config, as_of)
            model_results.append({
                "model": spec.name, "status": "ok", "direction": a.direction,
                "strength": a.strength, "confidence": a.confidence, "coherence": a.coherence,
                "signal_count": a.signal_count, "bullish": a.bullish_count, "bearish": a.bearish_count,
            })
        except Exception as e:
            model_results.append({"model": spec.name, "status": "error", "detail": str(e)})

    ok = [m for m in model_results if m.get("status") == "ok"]
    consensus = {}
    if ok:
        bv = sum(1 for m in ok if m["direction"] == "bullish")
        brv = sum(1 for m in ok if m["direction"] == "bearish")
        consensus = {
            "direction": "bullish" if bv > brv else ("bearish" if brv > bv else "neutral"),
            "avg_confidence": round(sum(m["confidence"] for m in ok) / len(ok), 4),
            "votes": {"bullish": bv, "bearish": brv, "neutral": len(ok) - bv - brv, "total": len(ok)},
        }

    # Compute conviction score: 0-100
    score = 50
    if ok:
        bw_total = sum(m.get("confidence", 0.5) for m in ok if m["direction"] == "bullish")
        brw_total = sum(m.get("confidence", 0.5) for m in ok if m["direction"] == "bearish")
        total_conf = bw_total + brw_total or 1.0
        raw = 50 + (bw_total - brw_total) / total_conf * 50
        score = max(0, min(100, round(raw)))
        consensus["score"] = score

    return {"ticker": body.ticker, "score": score, "as_of": as_of.isoformat(), "consensus": consensus, "models": model_results}
