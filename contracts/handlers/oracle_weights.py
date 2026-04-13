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
    """Decay contagion model heads when a postmortem reports MISS.

    The source is derived from the contract's ``producer_module`` field
    (e.g. ``"postmortem.apply_contagion_feedback"``). Non-MISS verdicts are
    a no-op so that HIT and PARTIAL postmortems don't punish the network.
    """
    if evt.verdict != "MISS":
        log.debug(
            "oracle_weights.on_postmortem_completed: skip verdict={v}",
            v=evt.verdict,
        )
        return

    from oracle.engine import ModelRegistry

    registry = ModelRegistry(engine)
    decayed = registry.decay_model_by_source(
        source=evt.producer_module,
        factor=0.9,
    )
    log.info(
        "oracle_weights.on_postmortem_completed: source={s} decayed={n}",
        s=evt.producer_module, n=decayed,
    )
