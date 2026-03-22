"""
Adapter between TradingAgents output and GRID's decision journal.

Parses the multi-agent deliberation result and maps it to GRID's
journal schema and the agent_runs table.
"""

from __future__ import annotations

import json
from typing import Any

from loguru import logger as log


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


def _safe_json(obj: Any) -> dict:
    """Ensure obj is JSON-serialisable as a dict."""
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, str):
        try:
            parsed = json.loads(obj)
            return parsed if isinstance(parsed, dict) else {"raw": parsed}
        except (json.JSONDecodeError, TypeError):
            return {"raw": obj}
    return {"raw": str(obj)}
