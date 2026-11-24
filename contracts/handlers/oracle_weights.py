"""Oracle weight evolver handlers (SYNTH-20, SYNTH-23).

Consume ``PredictionScored`` / ``PostmortemCompleted`` contracts to adjust
model weights in ``oracle.engine.ModelRegistry``. The heavy Bayesian math
lives on the registry — these handlers are thin adapters only.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger as log

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from contracts.schemas import PostmortemCompleted, PredictionScored

ORACLE_MODEL_PRODUCER_PREFIX = "oracle.model:"

# Track event types for which the non-oracle skip INFO log has already been
# emitted so high-volume options-trade batches log at INFO once per event type.
_SKIP_DECAY_LOGGED_EVENT_TYPES: set[str] = set()


def _extract_oracle_model_name(producer_module: str | None) -> str | None:
    """Return the exact oracle model name if ``producer_module`` attributes an
    outcome to an oracle prediction model (``oracle.model:<model_name>``).

    Non-oracle producers (such as ``"trading.options_recommender"``), signal
    families (``"vol"``, ``"sentiment"``, etc.), unmapped strings, and missing
    model names return ``None`` so decay is skipped.
    """
    if not isinstance(producer_module, str):
        return None
    val = producer_module.strip()
    if not val.startswith(ORACLE_MODEL_PRODUCER_PREFIX):
        return None
    model_name = val[len(ORACLE_MODEL_PRODUCER_PREFIX):].strip()
    if not model_name or model_name.lower() == "unknown":
        return None
    if any(ch.isspace() for ch in model_name):
        return None
    return model_name


def _log_skip_non_oracle_once(evt: object) -> None:
    """Log non-oracle skip at INFO once per event type, DEBUG thereafter."""
    event_type = type(evt).__name__
    producer = getattr(evt, "producer_module", None)
    if event_type not in _SKIP_DECAY_LOGGED_EVENT_TYPES:
        _SKIP_DECAY_LOGGED_EVENT_TYPES.add(event_type)
        log.info(
            "oracle_weights.on_postmortem_completed: "
            "skip decay: outcome not attributable to an oracle model "
            "(event_type={et} producer={p})",
            et=event_type,
            p=producer,
        )
    else:
        log.debug(
            "oracle_weights.on_postmortem_completed: "
            "skip decay: outcome not attributable to an oracle model "
            "(event_type={et} producer={p})",
            et=event_type,
            p=producer,
        )


def on_prediction_scored(
    evt: "PredictionScored", *, engine: "Engine"
) -> None:
    """Route a scored prediction into ``ModelRegistry.update_from_contract``."""
    from oracle.engine import ModelRegistry

    registry = ModelRegistry(engine)
    updates = registry.update_from_contract(evt)
    log.info(
        "oracle_weights.on_prediction_scored: verdict={v} models_updated={n}",
        v=evt.verdict, n=updates,
    )


def on_postmortem_completed(
    evt: "PostmortemCompleted", *, engine: "Engine"
) -> None:
    """Decay a specific oracle model when a postmortem reports MISS.

    Only outcomes attributable to a named oracle model
    (``producer_module="oracle.model:<model_name>"``) may decay, and only
    that exact model via ``ModelRegistry.decay_model_by_name`` — never by
    signal-family substring. Options-trade outcomes
    (``"trading.options_recommender"``) and unattributed/unmapped producers
    are skipped cleanly.
    """
    if evt.verdict != "MISS":
        log.debug(
            "oracle_weights.on_postmortem_completed: skip verdict={v}",
            v=evt.verdict,
        )
        return

    model_name = _extract_oracle_model_name(evt.producer_module)
    if not model_name:
        _log_skip_non_oracle_once(evt)
        return

    from oracle.engine import ModelRegistry

    registry = ModelRegistry(engine)
    decayed = registry.decay_model_by_name(
        model_name=model_name,
        factor=0.9,
    )
    log.info(
        "oracle_weights.on_postmortem_completed: model={m} decayed={n}",
        m=model_name, n=decayed,
    )
