"""Calibration metrics handler (SYNTH-21).

Delegates to ``oracle.calibration.update_running_metrics`` so that every
scored prediction immediately updates the running Brier / ECE counters on
``oracle_models``. Heavy statistics stay in the calibration module.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger as log

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from contracts.schemas import PredictionScored


# Verdict → numerical outcome used for Brier score. HIT=1, PARTIAL=0.5, MISS=0.
_VERDICT_TO_OUTCOME = {"HIT": 1.0, "PARTIAL": 0.5, "MISS": 0.0}


def on_prediction_scored(
    evt: "PredictionScored", *, engine: "Engine"
) -> None:
    """Update running Brier / ECE for every model referenced by the prediction.

    ``PredictionScored`` carries the model weights at the time of the call,
    so we update one row per model the ensemble actually used.
    """
    from oracle.calibration import update_running_metrics

    outcome = _VERDICT_TO_OUTCOME.get(evt.verdict, 0.0)
    prediction = float(evt.confidence)
    model_weights = dict(getattr(evt, "model_weights_at_prediction", {}) or {})

    if not model_weights:
        log.debug(
            "calibration.on_prediction_scored: no model weights for {pid}",
            pid=evt.prediction_id,
        )
        return

    updated = 0
    for model_id in model_weights:
        update_running_metrics(
            engine,
            model_id=model_id,
            prediction=prediction,
            actual=outcome,
        )
        updated += 1

    log.info(
        "calibration.on_prediction_scored: prediction={pid} models={n} "
        "conf={c:.3f} outcome={o:.2f}",
        pid=evt.prediction_id, n=updated, c=prediction, o=outcome,
    )
