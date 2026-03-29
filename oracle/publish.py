"""Explicit publish contract for comparable AstroGrid oracle records."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine


def _compact_text(value: Any, fallback: str = "") -> str:
    return " ".join(str(value or fallback).split())[:240]


def _prediction_direction(payload: dict[str, Any]) -> str:
    raw = " ".join(
        [
            str(payload.get("call") or ""),
            str(payload.get("setup") or ""),
            str(payload.get("note") or ""),
        ]
    ).lower()
    if any(token in raw for token in ("sell", "short", "hedge", "fade", "risk off", "bear")):
        return "BEARISH"
    if any(token in raw for token in ("buy", "long", "press", "accumulate", "risk on", "bull")):
        return "BULLISH"
    return "NEUTRAL"


def _prediction_expiry(payload: dict[str, Any]) -> date:
    horizon = str(payload.get("horizon_label") or "swing")
    as_of = payload.get("as_of_ts")
    if isinstance(as_of, str):
        as_of_dt = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
    elif isinstance(as_of, datetime):
        as_of_dt = as_of
    else:
        as_of_dt = datetime.now(timezone.utc)
    if horizon == "macro":
        return (as_of_dt + timedelta(days=30)).date()
    return (as_of_dt + timedelta(days=7)).date()


def publish_astrogrid_prediction(engine: Engine, payload: dict[str, Any]) -> dict[str, Any]:
    """Publish a reduced comparable record into the shared Oracle path."""
    oracle_prediction_id = str(payload.get("oracle_prediction_id") or f"astrogrid:{payload['prediction_id']}")
    flow_context = {
        "source": "astrogrid",
        "target_universe": payload.get("target_universe") or "hybrid",
        "target_symbols": list(payload.get("target_symbols") or []),
        "horizon": payload.get("horizon_label") or "swing",
        "question": payload.get("question"),
        "call": payload.get("call"),
        "timing": payload.get("timing"),
        "invalidation": payload.get("invalidation"),
    }
    signals = [
        {"name": "astrogrid_grid", "detail": _compact_text(payload.get("grid_summary"))},
        {"name": "astrogrid_mystical", "detail": _compact_text(payload.get("mystical_summary"))},
    ]
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO oracle_predictions (
                    id,
                    created_at,
                    ticker,
                    prediction_type,
                    direction,
                    target_price,
                    entry_price,
                    expiry,
                    confidence,
                    expected_move_pct,
                    signal_strength,
                    coherence,
                    model_name,
                    model_version,
                    signals,
                    anti_signals,
                    flow_context,
                    model_weights
                )
                VALUES (
                    :id,
                    NOW(),
                    :ticker,
                    :prediction_type,
                    :direction,
                    NULL,
                    :entry_price,
                    :expiry,
                    :confidence,
                    NULL,
                    :signal_strength,
                    :coherence,
                    :model_name,
                    :model_version,
                    CAST(:signals AS jsonb),
                    CAST(:anti_signals AS jsonb),
                    CAST(:flow_context AS jsonb),
                    CAST(:model_weights AS jsonb)
                )
                ON CONFLICT (id) DO NOTHING
                """
            ),
            {
                "id": oracle_prediction_id,
                "ticker": (payload.get("target_symbols") or ["HYBRID"])[0],
                "prediction_type": "astrogrid",
                "direction": _prediction_direction(payload),
                "entry_price": 0.0,
                "expiry": _prediction_expiry(payload),
                "confidence": float(payload.get("confidence") or 0.5),
                "signal_strength": float(payload.get("confidence") or 0.5),
                "coherence": float(payload.get("confidence") or 0.5),
                "model_name": "astrogrid",
                "model_version": str(payload.get("model_version") or "astrogrid-oracle-v1"),
                "signals": json.dumps(signals),
                "anti_signals": json.dumps([
                    {"name": "astrogrid_invalidation", "detail": _compact_text(payload.get("invalidation"))},
                ]),
                "flow_context": json.dumps(flow_context),
                "model_weights": json.dumps({
                    "weight_version": payload.get("weight_version") or "astrogrid-v1",
                    "publish_contract": "oracle.publish.v1",
                }),
            },
        )
    return {
        "status": "published",
        "oracle_prediction_id": oracle_prediction_id,
        "contract": "oracle.publish.v1",
    }
