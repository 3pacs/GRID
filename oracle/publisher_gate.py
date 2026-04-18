"""
GRID — Publisher gate: decide publish / review / reject.

Deterministic rules based on claim verification and sanity check results.
No LLM dependency.

Also hosts the AstroGrid publish contract (merged from oracle/publish.py) so
that all oracle publication helpers live together.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal

from sqlalchemy import text
from sqlalchemy.engine import Engine

from oracle.sanity_checker import CheckedClaim

Decision = Literal["publish", "review", "reject"]


@dataclass(frozen=True)
class PublishDecision:
    """Final gate decision for an LLM output."""

    decision: Decision
    score: float
    claims: tuple[CheckedClaim, ...]
    reasons: tuple[str, ...]


# ── Thresholds ───────────────────────────────────────────────────────────

_AUTO_PUBLISH_CONFIDENCE = 0.85
_REVIEW_REWRITE_RATIO = 0.30  # >30% claims flagged → review


def gate_decision(claims: list[CheckedClaim]) -> PublishDecision:
    """Evaluate checked claims and return a publish/review/reject decision.

    Hard fail rules (reject immediately):
      - Any claim contradicted by evidence
      - Any sanity check failure on a critical claim (price/percentage)
      - Price/number that fails range check

    Auto-publish rules:
      - All claims supported, all checks pass, confidence > 0.85

    Human review:
      - Mixed verdicts, source disagreement, >30% claims flagged
    """
    if not claims:
        return PublishDecision(
            decision="publish",
            score=1.0,
            claims=tuple(claims),
            reasons=("No verifiable claims found — pass-through",),
        )

    reasons: list[str] = []
    total = len(claims)
    supported = 0
    contradicted = 0
    insufficient = 0
    flagged = 0
    critical_fails = 0

    for cc in claims:
        v = cc.verified
        if v.verdict == "supported":
            supported += 1
        elif v.verdict == "contradicted":
            contradicted += 1
            reasons.append(f"CONTRADICTED: {v.reason}")
        elif v.verdict == "insufficient":
            insufficient += 1

        if cc.critical_fail:
            critical_fails += 1
            fail_msgs = [r.message for r in cc.checks if r.flag == "fail"]
            reasons.append(f"SANITY FAIL: {'; '.join(fail_msgs)}")

        has_any_flag = any(r.flag in ("fail", "warn") for r in cc.checks)
        if has_any_flag:
            flagged += 1

    # --- Hard reject ---
    if contradicted > 0:
        return PublishDecision(
            decision="reject",
            score=0.0,
            claims=tuple(claims),
            reasons=tuple(reasons or ["Contradicted claims detected"]),
        )

    if critical_fails > 0:
        return PublishDecision(
            decision="reject",
            score=0.0,
            claims=tuple(claims),
            reasons=tuple(reasons or ["Critical sanity check failures"]),
        )

    # --- Auto-publish ---
    verifiable = supported + contradicted
    avg_confidence = (
        sum(cc.verified.confidence for cc in claims) / total
    )

    if (
        supported == verifiable
        and verifiable > 0
        and flagged == 0
        and avg_confidence >= _AUTO_PUBLISH_CONFIDENCE
    ):
        return PublishDecision(
            decision="publish",
            score=avg_confidence,
            claims=tuple(claims),
            reasons=("All verifiable claims supported, checks pass",),
        )

    # All claims insufficient (no verifiable data) — still publish with lower score
    if insufficient == total:
        return PublishDecision(
            decision="publish",
            score=0.5,
            claims=tuple(claims),
            reasons=("No verifiable claims — insufficient data for verification",),
        )

    # --- Review ---
    rewrite_ratio = flagged / total if total > 0 else 0
    if rewrite_ratio > _REVIEW_REWRITE_RATIO or flagged > 0:
        reasons.append(f"{flagged}/{total} claims flagged ({rewrite_ratio:.0%})")
        return PublishDecision(
            decision="review",
            score=avg_confidence,
            claims=tuple(claims),
            reasons=tuple(reasons),
        )

    # Default: publish with moderate confidence
    return PublishDecision(
        decision="publish",
        score=avg_confidence,
        claims=tuple(claims),
        reasons=("Mixed verdicts but no contradictions or failures",),
    )


# ══════════════════════════════════════════════════════════════════════════
# AstroGrid publish contract (merged from oracle/publish.py)
# Explicit publish contract for comparable AstroGrid oracle records.
# ══════════════════════════════════════════════════════════════════════════


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
    oracle_prediction_id = str(
        payload.get("oracle_prediction_id")
        or f"astrogrid:{payload['prediction_id']}"
    )
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
                "model_version": str(
                    payload.get("model_version") or "astrogrid-oracle-v1"
                ),
                "signals": json.dumps(signals),
                "anti_signals": json.dumps([
                    {
                        "name": "astrogrid_invalidation",
                        "detail": _compact_text(payload.get("invalidation")),
                    },
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
