"""Trust scorer handlers for dispatched contract events.

Wave A (SYNTH-19, SYNTH-22): closes the feedback loop from PredictionScored
and PostmortemCompleted contracts back into the trust scoring subsystem.

These handlers intentionally own no business logic: they construct a
``TrustScorer`` instance over the shared engine and delegate to methods on
that class. Heavy behaviour stays in ``intelligence/trust_scorer.py``.
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
    """Fan the prediction's ``signals_used`` into the trust scorer.

    The scorer re-evaluates every signal that fed the prediction so that
    downstream trust deltas pick up the verdict of this specific scoring
    pass, not only the once-per-day batch cycle.
    """
    from intelligence.trust_scorer import TrustScorer

    scorer = TrustScorer(engine)
    signals = list(getattr(evt, "signals_used", None) or [])
    scored = scorer.score_prediction_signals(
        prediction_id=evt.prediction_id,
        verdict=evt.verdict,
        signals=signals,
    )
    log.info(
        "trust.on_prediction_scored: prediction={pid} verdict={v} "
        "signals={n} scored={s}",
        pid=evt.prediction_id, v=evt.verdict, n=len(signals), s=scored,
    )


def on_postmortem_completed(
    evt: "PostmortemCompleted", *, engine: "Engine"
) -> None:
    """Bayes-update source trust using a postmortem verdict.

    The postmortem contract carries the full list of contributing signals
    plus the ground-truth verdict. Each contributing signal's source gets a
    small trust update proportional to how wrong the aggregate call was.
    """
    from intelligence.trust_scorer import TrustScorer

    scorer = TrustScorer(engine)
    updated = scorer.update_source_trust_from_postmortem(
        prediction_id=evt.prediction_id,
        verdict=evt.verdict,
        signals=list(getattr(evt, "signals_used", None) or []),
        root_cause=evt.root_cause,
    )
    log.info(
        "trust.on_postmortem_completed: prediction={pid} verdict={v} "
        "sources_updated={n}",
        pid=evt.prediction_id, v=evt.verdict, n=updated,
    )
