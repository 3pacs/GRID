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

    from contracts.schemas import OptionsTradeOutcome, PredictionScored


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


def on_options_trade_outcome(
    evt: "OptionsTradeOutcome", *, engine: "Engine"
) -> None:
    """Per-strategy reliability nudge when an options trade closes.

    §7.3 names this as the second handler on ``OptionsTradeOutcome``
    (alongside ``oracle_weights.on_options_trade_outcome`` which is the
    legacy ``trade_outcomes.on_options_trade_outcome`` already wired).

    The PnL sign is mapped to a Brier outcome (positive = HIT, zero or
    negative = MISS) so the running calibration metric on the strategy
    keeps a coherent reliability curve. The handler delegates to
    ``update_running_metrics`` with the strategy as a synthetic model_id
    prefixed with ``strategy:`` so it doesn't collide with the real
    oracle ensemble model ids.
    """
    from oracle.calibration import update_running_metrics

    strategy = getattr(evt, "strategy", "") or ""
    if not strategy:
        log.debug("calibration.on_options_trade_outcome: missing strategy")
        return

    pnl = float(getattr(evt, "pnl", 0.0) or 0.0)
    outcome = 1.0 if pnl > 0 else 0.0
    # Confidence proxy — strategy track records imply an implicit
    # probability of 0.5 in the absence of explicit per-strategy priors.
    # The real anchor is the outcome, not the prior.
    prediction = 0.5

    model_id = f"strategy:{strategy}"

    try:
        update_running_metrics(
            engine,
            model_id=model_id,
            prediction=prediction,
            actual=outcome,
        )
    except Exception as exc:
        log.warning(
            "calibration.on_options_trade_outcome(strategy={s}): {e}",
            s=strategy, e=str(exc),
        )
        return

    log.info(
        "calibration.on_options_trade_outcome: strategy={s} pnl={p:+.2f} "
        "outcome={o:.0f}",
        s=strategy, p=pnl, o=outcome,
    )
