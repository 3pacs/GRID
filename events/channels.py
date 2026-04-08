"""Event channel constants and payload schemas.

Each channel corresponds to a PostgreSQL NOTIFY channel. Producers emit
events via ``bus.emit(channel, payload)``, consumers receive them through
the SSE endpoint or internal subscribers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


# Channel name constants — match these exactly in LISTEN/NOTIFY
ACTOR_UPDATE = "grid_actor_update"
SIGNAL_FIRE = "grid_signal_fire"
REGIME_CHANGE = "grid_regime_change"
PREDICTION_SCORED = "grid_prediction_scored"
FLOW_SHIFT = "grid_flow_shift"
INVESTIGATION_ALERT = "grid_investigation_alert"
PULL_COMPLETE = "grid_pull_complete"
MODEL_PROMOTED = "grid_model_promoted"

ALL_CHANNELS: tuple[str, ...] = (
    ACTOR_UPDATE,
    SIGNAL_FIRE,
    REGIME_CHANGE,
    PREDICTION_SCORED,
    FLOW_SHIFT,
    INVESTIGATION_ALERT,
    PULL_COMPLETE,
    MODEL_PROMOTED,
)


@dataclass(frozen=True)
class Event:
    """Immutable event wrapper."""
    channel: str
    payload: dict[str, Any]
    timestamp: str  # ISO 8601 UTC

    def to_sse(self) -> str:
        """Format as SSE data line."""
        import json
        data = json.dumps({
            "channel": self.channel,
            "payload": self.payload,
            "timestamp": self.timestamp,
        })
        return f"event: {self.channel}\ndata: {data}\n\n"
