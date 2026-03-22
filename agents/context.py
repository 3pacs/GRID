"""
GRID context builder for TradingAgents.

Fetches the current regime state, feature snapshot, and inference
results, then formats them as a context string injected into agent
analyst prompts so the multi-agent system is regime-aware.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from loguru import logger as log
from sqlalchemy.engine import Engine

from inference.live import LiveInference
from store.pit import PITStore


class GRIDContext:
    """Builds a regime-aware context summary for TradingAgents.

    Attributes:
        inference: LiveInference instance.
    """

    def __init__(self, db_engine: Engine) -> None:
        pit = PITStore(db_engine)
        self.inference = LiveInference(db_engine, pit)
        log.info("GRIDContext initialised")

    def build(self, as_of_date: date | None = None) -> dict[str, Any]:
        """Build the full GRID context for an agent run.

        Returns:
            dict with keys: regime_state, confidence, feature_summary,
            prompt_context (formatted string for injection into prompts).
        """
        if as_of_date is None:
            as_of_date = date.today()

        result = self.inference.run_inference(as_of_date)
        snapshot = self.inference.get_feature_snapshot(as_of_date)

        # Extract regime info from inference layers
        regime_state = "UNKNOWN"
        confidence = 0.0
        transition_prob = 0.0
        suggested_action = "HOLD"

        layers = result.get("layers", {})
        if "REGIME" in layers:
            regime_layer = layers["REGIME"]
            rec = regime_layer.get("recommendation", {})
            regime_state = rec.get("inferred_state", "UNKNOWN")
            confidence = rec.get("state_confidence", 0.0)
            transition_prob = rec.get("transition_probability", 0.0)
            suggested_action = rec.get("suggested_action", "HOLD")

        # Build feature summary
        feature_lines: list[str] = []
        if not snapshot.empty:
            for _, row in snapshot.head(15).iterrows():
                val = f"{row['value']:.4f}" if row["value"] is not None else "N/A"
                feature_lines.append(f"  - {row['name']} ({row['family']}): {val}")

        feature_summary = "\n".join(feature_lines) if feature_lines else "  No features available"

        # Build prompt context string
        prompt_context = (
            f"=== GRID Regime Intelligence (as of {as_of_date.isoformat()}) ===\n"
            f"Current Regime: {regime_state}\n"
            f"Regime Confidence: {confidence:.1%}\n"
            f"Transition Probability: {transition_prob:.1%}\n"
            f"GRID Suggested Action: {suggested_action}\n"
            f"\nKey Macro Features:\n{feature_summary}\n"
            f"=== End GRID Context ===\n\n"
            f"Consider the above regime intelligence when forming your analysis. "
            f"The market is currently in a '{regime_state}' regime with "
            f"{confidence:.0%} confidence."
        )

        return {
            "regime_state": regime_state,
            "confidence": confidence,
            "transition_probability": transition_prob,
            "suggested_action": suggested_action,
            "feature_summary": feature_summary,
            "prompt_context": prompt_context,
            "inference_result": result,
        }
