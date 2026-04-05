"""
GRID — Flow Thesis Scoring and Narrative Generation.

Public API for combining all theses into a unified market view:
  update_current_states(engine) — fill live data into each thesis
  generate_unified_thesis(engine) — combine all theses into one market view
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from analysis.flow_thesis_data import (
    FLOW_KNOWLEDGE,
    BULLISH,
    BEARISH,
    NEUTRAL,
    _STATE_UPDATERS,
)


# ══════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════════════════

def update_current_states(engine: Engine) -> dict[str, dict]:
    """Build a new knowledge dict with current_state populated for every thesis.

    Returns a new dict — does not mutate the module-level FLOW_KNOWLEDGE.
    """
    knowledge: dict[str, dict] = {}
    for key, thesis in FLOW_KNOWLEDGE.items():
        updater = _STATE_UPDATERS.get(key)
        if updater is not None:
            try:
                state = updater(engine)
            except Exception as exc:
                log.warning("Thesis state update failed for {k}: {e}", k=key, e=str(exc))
                state = {
                    "direction": NEUTRAL,
                    "value": None,
                    "detail": f"Update error: {exc}",
                }
        else:
            state = {"direction": NEUTRAL, "value": None}
        knowledge[key] = {**thesis, "current_state": state}
    return knowledge


def _load_learned_weights(engine: Engine) -> dict[str, float]:
    """Load per-model accuracy from scored theses and convert to dynamic weights.

    Models that have been consistently right get higher weight.
    Models that have been consistently wrong get penalized.
    New/unscored models keep their static weight.

    Returns:
        Dict of {model_key: learned_weight_multiplier} (0.3 to 2.0).
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                WITH model_outcomes AS (
                    SELECT
                        key,
                        value->>'direction' as model_direction,
                        ts.outcome,
                        ts.actual_market_move
                    FROM thesis_snapshots ts,
                         jsonb_each(ts.model_states) AS kv(key, value)
                    WHERE ts.outcome IS NOT NULL
                      AND ts.timestamp >= NOW() - INTERVAL '90 days'
                )
                SELECT
                    key,
                    COUNT(*) as total,
                    SUM(CASE
                        WHEN (model_direction IN ('bullish', 'BULLISH') AND actual_market_move > 0.5) THEN 1
                        WHEN (model_direction IN ('bearish', 'BEARISH') AND actual_market_move < -0.5) THEN 1
                        WHEN model_direction IN ('neutral', 'NEUTRAL') THEN 0
                        ELSE 0
                    END) as correct
                FROM model_outcomes
                WHERE model_direction IS NOT NULL
                  AND model_direction NOT IN ('neutral', 'NEUTRAL')
                GROUP BY key
                HAVING COUNT(*) >= 3
            """)).fetchall()

        weights = {}
        for r in rows:
            model_key = r[0]
            total = r[1]
            correct = r[2]
            accuracy = correct / total if total > 0 else 0.5

            if accuracy >= 0.75:
                multiplier = 1.5 + (accuracy - 0.75) * 2.0
            elif accuracy >= 0.5:
                multiplier = 1.0 + (accuracy - 0.5) * 2.0
            else:
                multiplier = max(0.3, accuracy * 2.0)

            weights[model_key] = round(multiplier, 2)

        if weights:
            log.info(
                "Thesis self-learning: loaded {n} model weights from track record. "
                "Top: {top}, Bottom: {bot}",
                n=len(weights),
                top=sorted(weights.items(), key=lambda x: -x[1])[:3],
                bot=sorted(weights.items(), key=lambda x: x[1])[:3],
            )
        return weights

    except Exception as exc:
        log.debug("Failed to load learned weights: {e}", e=str(exc))
        return {}


def generate_unified_thesis(engine: Engine) -> dict[str, Any]:
    """Combine all theses into a unified market view.

    Steps:
      1. Update all current states
      2. Load learned weights from thesis track record (self-learning)
      3. Score bullish/bearish/neutral across all theses
      4. Weight by confidence level * learned accuracy multiplier
      5. Identify agreements and contradictions
      6. Produce unified direction, conviction, drivers, risks, narrative

    Returns:
        dict with: overall_direction, conviction, key_drivers, risk_factors,
                   agreements, contradictions, theses, narrative,
                   generated_at
    """
    knowledge = update_current_states(engine)

    learned_weights = _load_learned_weights(engine)

    confidence_weights = {"high": 3.0, "moderate": 2.0, "low": 1.0}

    bullish_score = 0.0
    bearish_score = 0.0
    neutral_count = 0
    total_weight = 0.0

    bullish_drivers: list[dict] = []
    bearish_drivers: list[dict] = []
    neutral_drivers: list[dict] = []
    overall_direction = NEUTRAL
    conviction = 0.0

    for key, thesis in knowledge.items():
        state = thesis.get("current_state")
        if not state or state.get("value") is None:
            continue

        base_weight = confidence_weights.get(thesis.get("confidence", "low"), 1.0)
        learned_mult = learned_weights.get(key, 1.0)
        weight = base_weight * learned_mult
        total_weight += weight
        direction = state.get("direction", NEUTRAL)

        entry = {
            "key": key,
            "name": key.replace("_", " ").title(),
            "direction": direction,
            "detail": state.get("detail", ""),
            "confidence": thesis.get("confidence", "low"),
            "weight": round(weight, 2),
            "learned_multiplier": learned_mult,
        }

        if direction == BULLISH:
            bullish_score += weight
            bullish_drivers.append(entry)
        elif direction == BEARISH:
            bearish_score += weight
            bearish_drivers.append(entry)
        else:
            neutral_count += 1
            neutral_drivers.append(entry)

    if total_weight == 0:
        overall_direction = NEUTRAL
        conviction = 0.0
    else:
        net_score = bullish_score - bearish_score
        max_possible = total_weight
        conviction = abs(net_score) / max_possible if max_possible > 0 else 0.0

        if net_score > 0.5:
            overall_direction = BULLISH
        elif net_score < -0.5:
            overall_direction = BEARISH
        else:
            overall_direction = NEUTRAL

    conviction_pct = min(100, round(conviction * 100))

    if overall_direction == BULLISH:
        key_drivers = sorted(bullish_drivers, key=lambda d: -d["weight"])[:3]
        risk_factors = sorted(bearish_drivers, key=lambda d: -d["weight"])[:3]
    elif overall_direction == BEARISH:
        key_drivers = sorted(bearish_drivers, key=lambda d: -d["weight"])[:3]
        risk_factors = sorted(bullish_drivers, key=lambda d: -d["weight"])[:3]
    else:
        all_active = bullish_drivers + bearish_drivers
        key_drivers = sorted(all_active, key=lambda d: -d["weight"])[:3]
        risk_factors = []

    agreements: list[dict] = []
    contradictions: list[dict] = []

    active_theses = bullish_drivers + bearish_drivers + neutral_drivers
    direction_groups: dict[str, list] = {BULLISH: [], BEARISH: [], NEUTRAL: []}
    for t in active_theses:
        direction_groups[t["direction"]].append(t["key"])

    for dir_label, members in direction_groups.items():
        if len(members) >= 2:
            agreements.append({
                "direction": dir_label,
                "members": members,
                "count": len(members),
                "conviction": "high" if len(members) >= 3 else "moderate",
            })

    for bull in bullish_drivers:
        for bear in bearish_drivers:
            if bull["confidence"] in ("high", "moderate") and bear["confidence"] in ("high", "moderate"):
                contradictions.append({
                    "bullish": bull["key"],
                    "bearish": bear["key"],
                    "bull_detail": bull["detail"],
                    "bear_detail": bear["detail"],
                })

    narrative = _build_narrative(
        overall_direction, conviction_pct, key_drivers,
        risk_factors, agreements, contradictions,
        bullish_score, bearish_score, len(active_theses),
    )

    theses_output = []
    for key, thesis in knowledge.items():
        state = thesis.get("current_state") or {}
        theses_output.append({
            "key": key,
            "name": key.replace("_", " ").title(),
            "thesis": thesis.get("thesis", ""),
            "mechanism": thesis.get("mechanism", ""),
            "confidence": thesis.get("confidence", "low"),
            "source": thesis.get("source", "estimated"),
            "direction": state.get("direction", NEUTRAL),
            "current_value": state.get("value"),
            "detail": state.get("detail", "No data"),
            "key_metric": thesis.get("key_metric"),
            "lead_time_days": thesis.get("lead_time_days"),
            "correlation_to_spy": thesis.get("correlation_to_spy"),
            "timing": thesis.get("timing"),
        })

    return {
        "overall_direction": overall_direction.upper(),
        "conviction": conviction_pct,
        "bullish_score": round(bullish_score, 1),
        "bearish_score": round(bearish_score, 1),
        "active_theses": len(active_theses),
        "key_drivers": key_drivers,
        "risk_factors": risk_factors,
        "agreements": agreements,
        "contradictions": contradictions,
        "theses": theses_output,
        "narrative": narrative,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def _build_narrative(
    direction: str,
    conviction: int,
    key_drivers: list[dict],
    risk_factors: list[dict],
    agreements: list[dict],
    contradictions: list[dict],
    bull_score: float,
    bear_score: float,
    total: int,
) -> str:
    """Build a human-readable narrative synthesizing the unified thesis."""
    parts: list[str] = []

    dir_label = {"bullish": "BULLISH", "bearish": "BEARISH", "neutral": "NEUTRAL"}.get(direction, "NEUTRAL")
    parts.append(
        f"GRID's unified thesis is {dir_label} with {conviction}% conviction "
        f"({bull_score:.0f} bull vs {bear_score:.0f} bear across {total} active signals)."
    )

    if key_drivers:
        driver_strs = [f"{d['name']} ({d['detail']})" for d in key_drivers]
        parts.append(f"Primary drivers: {'; '.join(driver_strs)}.")

    high_conv = [a for a in agreements if a["conviction"] == "high"]
    if high_conv:
        for a in high_conv:
            member_names = [m.replace("_", " ").title() for m in a["members"]]
            parts.append(
                f"High-conviction convergence: {len(a['members'])} models agree "
                f"{a['direction']} ({', '.join(member_names)})."
            )

    if contradictions:
        parts.append(
            f"Warning: {len(contradictions)} model contradiction(s) detected. "
        )
        for c in contradictions[:2]:
            parts.append(
                f"{c['bullish'].replace('_', ' ').title()} says bull "
                f"while {c['bearish'].replace('_', ' ').title()} says bear."
            )

    if risk_factors:
        risk_strs = [f"{r['name']} ({r['detail']})" for r in risk_factors]
        parts.append(f"Key risks that could invalidate: {'; '.join(risk_strs)}.")

    return " ".join(parts)
