"""
GRID Intelligence — Flow Thesis Signal Adapter.

Translates analysis.flow_thesis output into RegisteredSignal objects.
Produces 10 per-thesis DIRECTIONAL signals + 1 unified market signal.
Validity window: now -> now + 4 hours. Refresh: every 2 hours.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy.engine import Engine

from intelligence.signal_registry import (
    Direction,
    RegisteredSignal,
    SignalType,
    make_signal_id,
)

_SOURCE_MODULE = "analysis.flow_thesis"
_VALID_HOURS = 4.0
_REFRESH_HOURS = 2.0

_CONFIDENCE_MAP = {"high": 0.75, "moderate": 0.55, "low": 0.35}


def _direction_to_value(direction: str) -> tuple[Direction, float]:
    mapping = {
        "bullish":  (Direction.BULLISH,  1.0),
        "bearish":  (Direction.BEARISH, -1.0),
        "neutral":  (Direction.NEUTRAL,  0.0),
    }
    return mapping.get(direction.lower() if direction else "neutral", (Direction.NEUTRAL, 0.0))


class FlowThesisAdapter:

    @property
    def source_module(self) -> str:
        return _SOURCE_MODULE

    @property
    def refresh_interval_hours(self) -> float:
        return _REFRESH_HOURS

    def extract_signals(self, engine: Engine) -> list[RegisteredSignal]:
        now = datetime.now(timezone.utc)
        valid_until = now + timedelta(hours=_VALID_HOURS)
        signals: list[RegisteredSignal] = []

        try:
            from analysis.flow_thesis import FLOW_KNOWLEDGE, generate_unified_thesis
            unified = generate_unified_thesis(engine)
        except Exception as exc:
            log.error("FlowThesisAdapter: generate_unified_thesis failed - {e}", e=exc)
            return []

        # Per-thesis signals
        for thesis_key, thesis in FLOW_KNOWLEDGE.items():
            state = thesis.get("current_state")
            if not state:
                continue
            try:
                raw_dir = state.get("direction", "neutral")
                direction_enum, numeric_value = _direction_to_value(raw_dir)
                confidence = _CONFIDENCE_MAP.get(thesis.get("confidence", "low"), 0.35)
                signals.append(RegisteredSignal(
                    signal_id=make_signal_id(_SOURCE_MODULE, thesis_key),
                    source_module=_SOURCE_MODULE,
                    signal_type=SignalType.DIRECTIONAL,
                    ticker=None,
                    direction=direction_enum,
                    value=numeric_value,
                    confidence=confidence,
                    valid_from=now,
                    valid_until=valid_until,
                    freshness_hours=_VALID_HOURS,
                    metadata={"detail": state.get("detail", ""), "thesis_key": thesis_key},
                    provenance=f"flow_thesis:{thesis_key}",
                ))
            except Exception as exc:
                log.warning("FlowThesisAdapter: failed for {k} - {e}", k=thesis_key, e=exc)

        # Unified signal
        try:
            raw_dir = unified.get("overall_direction", "NEUTRAL").lower()
            direction_enum, _ = _direction_to_value(raw_dir)
            conviction = min(1.0, max(0.0, unified.get("conviction", 0) / 100.0))
            bull = float(unified.get("bullish_score", 0.0))
            bear = float(unified.get("bearish_score", 0.0))
            total = bull + bear
            numeric_value = (bull - bear) / total if total > 0 else 0.0
            signals.append(RegisteredSignal(
                signal_id=make_signal_id(_SOURCE_MODULE, "unified_thesis"),
                source_module=_SOURCE_MODULE,
                signal_type=SignalType.DIRECTIONAL,
                ticker=None,
                direction=direction_enum,
                value=round(numeric_value, 4),
                confidence=round(conviction, 4),
                valid_from=now,
                valid_until=valid_until,
                freshness_hours=_VALID_HOURS,
                metadata={"bullish_score": bull, "bearish_score": bear, "active_theses": unified.get("active_theses", 0)},
                provenance="flow_thesis:unified",
            ))
        except Exception as exc:
            log.warning("FlowThesisAdapter: failed unified signal - {e}", e=exc)

        log.info("FlowThesisAdapter: produced {n} signals", n=len(signals))
        return signals
