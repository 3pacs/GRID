"""
Adapter between TradingAgents output and GRID's decision journal.

Parses the multi-agent deliberation result and maps it to GRID's
journal schema and the agent_runs table.
"""

from __future__ import annotations

import json
from typing import Any

import numpy as np
from loguru import logger as log


def _convert_numpy(obj: Any) -> Any:
    """Recursively convert numpy types to native Python for JSON serialization."""
    if isinstance(obj, dict):
        return {k: _convert_numpy(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_convert_numpy(v) for v in obj]
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        val = float(obj)
        return None if (np.isnan(val) or np.isinf(val)) else val
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, np.bool_):
        return bool(obj)
    return obj


def parse_agent_decision(raw_decision: Any) -> dict[str, Any]:
    """Parse TradingAgents propagate() output into a structured dict.

    TradingAgents returns a tuple of (state, decision_dict).  The
    decision_dict typically contains analyst reports, debate summaries,
    risk assessments, and the final recommendation.

    Parameters:
        raw_decision: The second element from TradingAgentsGraph.propagate().

    Returns:
        dict with normalised keys for storage.
    """
    if raw_decision is None:
        return {
            "final_decision": "ERROR",
            "decision_reasoning": "No decision returned from agents",
            "analyst_reports": {},
            "bull_bear_debate": {},
            "risk_assessment": {},
        }

    if isinstance(raw_decision, str):
        return {
            "final_decision": _extract_action(raw_decision),
            "decision_reasoning": raw_decision,
            "analyst_reports": {},
            "bull_bear_debate": {},
            "risk_assessment": {},
        }

    if isinstance(raw_decision, dict):
        return {
            "final_decision": _extract_action(
                raw_decision.get("action", raw_decision.get("decision", "HOLD"))
            ),
            "decision_reasoning": raw_decision.get(
                "reasoning", raw_decision.get("summary", json.dumps(raw_decision, default=str))
            ),
            "analyst_reports": _safe_json(raw_decision.get("analyst_reports", {})),
            "bull_bear_debate": _safe_json(raw_decision.get("debate", raw_decision.get("bull_bear_debate", {}))),
            "risk_assessment": _safe_json(raw_decision.get("risk", raw_decision.get("risk_assessment", {}))),
        }

    log.warning("Unexpected decision type: {t}", t=type(raw_decision).__name__)
    return {
        "final_decision": "HOLD",
        "decision_reasoning": str(raw_decision),
        "analyst_reports": {},
        "bull_bear_debate": {},
        "risk_assessment": {},
    }


def _extract_action(text: Any) -> str:
    """Extract a BUY/SELL/HOLD action from text."""
    if not isinstance(text, str):
        text = str(text)
    upper = text.upper()
    if "BUY" in upper or "LONG" in upper:
        return "BUY"
    if "SELL" in upper or "SHORT" in upper:
        return "SELL"
    return "HOLD"


def compute_conviction_score(parsed: dict[str, Any]) -> float:
    """Compute a conviction score (0.0-1.0) from debate consensus.

    Analyses the bull_bear_debate and risk_assessment sections to gauge
    how strongly the agents agreed on the final decision. Higher scores
    indicate clearer consensus.

    Parameters:
        parsed: Output of parse_agent_decision() with keys
                final_decision, bull_bear_debate, risk_assessment.

    Returns:
        float: 0.0 (split/no data) to 1.0 (unanimous agreement).
    """
    score = 0.5  # neutral starting point

    debate = parsed.get("bull_bear_debate", {})
    risk = parsed.get("risk_assessment", {})
    decision = parsed.get("final_decision", "HOLD")

    # If debate is just a string, wrap it
    if isinstance(debate, str):
        debate = {"raw": debate}
    if isinstance(risk, str):
        risk = {"raw": risk}

    debate_text = json.dumps(debate, default=str).upper()
    risk_text = json.dumps(risk, default=str).upper()

    # Check if debate text aligns with decision
    if decision == "BUY":
        bullish_signals = sum(1 for kw in ("BULLISH", "UPSIDE", "OPPORTUNITY", "BUY", "LONG", "STRONG")
                             if kw in debate_text)
        bearish_signals = sum(1 for kw in ("BEARISH", "DOWNSIDE", "RISK", "SELL", "SHORT", "WEAK")
                             if kw in debate_text)
    elif decision == "SELL":
        bullish_signals = sum(1 for kw in ("BEARISH", "DOWNSIDE", "RISK", "SELL", "SHORT", "WEAK")
                             if kw in debate_text)
        bearish_signals = sum(1 for kw in ("BULLISH", "UPSIDE", "OPPORTUNITY", "BUY", "LONG", "STRONG")
                             if kw in debate_text)
    else:
        # HOLD — mixed signals are expected
        return 0.5

    total = bullish_signals + bearish_signals
    if total == 0:
        return 0.5

    # Ratio of decision-aligned signals
    alignment = bullish_signals / total
    score = min(1.0, max(0.0, alignment))

    # Penalise if risk section has strong warnings
    high_risk_flags = sum(1 for kw in ("HIGH RISK", "EXTREME", "DANGEROUS", "AVOID", "CAUTION")
                         if kw in risk_text)
    if high_risk_flags >= 2:
        score *= 0.7

    return round(score, 3)


def _safe_json(obj: Any) -> dict:
    """Ensure obj is JSON-serialisable as a dict.

    Converts numpy types to native Python to prevent serialization errors.
    """
    if isinstance(obj, dict):
        return _convert_numpy(obj)
    if isinstance(obj, str):
        try:
            parsed = json.loads(obj)
            return _convert_numpy(parsed) if isinstance(parsed, dict) else {"raw": parsed}
        except (json.JSONDecodeError, TypeError):
            return {"raw": obj}
    return {"raw": str(obj)}
