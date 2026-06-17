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
from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from contracts.schemas import (
        EdgeValidated,
        PostmortemCompleted,
        PredictionScored,
        SignalFired,
    )


#: Threshold below which a SignalFired contract is treated as registration
#: only — we still write the row to the trust scorer's tracking table but
#: do not nudge ``SIGNAL_TRUST_DELTA``.
_TRUST_REGISTER_MIN_STRENGTH: float = 0.0  # always register

#: Mapping from contract ``signal_type`` to trust scorer's BUY/SELL idiom.
#: Anything else is registered as a generic event row (no direction).
_BUY_SIGNAL_TYPES: frozenset = frozenset({"BUY", "buy", "LONG", "long"})
_SELL_SIGNAL_TYPES: frozenset = frozenset({"SELL", "sell", "SHORT", "short"})

_CROSS_LENS_SUPPLY_SHOCK_SOURCE_TYPE: str = "cross_lens_supply_shock"


#: Multiplicative trust factor applied to ``cross_lens_supply_shock`` rows
#: that cite an edge which just turned weak. Mirrors the value used in
#: ``contracts.handlers.edges`` so a single weak validation produces a
#: consistent decay across both handlers.
_WEAK_EDGE_TRUST_FACTOR: float = 0.75
_MIN_TRUST_FLOOR: float = 0.05


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


def on_signal_fired(
    evt: "SignalFired", *, engine: "Engine"
) -> None:
    """Register a fired signal into the trust scorer's signal_sources table.

    The §7.3 design names this as the third handler on ``SignalFired``
    (after ``oracle_signals`` and ``journal``). It delegates to
    ``trust_scorer.register_signal`` so the Bayesian beta posterior has a
    row to deposit outcomes against once the prediction is scored.

    Best-effort: ``register_signal`` already swallows DB failures and
    returns ``None`` on unrecognised source types.
    """
    from intelligence.trust_scorer import register_signal

    source = getattr(evt, "source", "") or ""
    signal_type_raw = getattr(evt, "signal_type", "") or ""
    ticker = getattr(evt, "ticker", None)
    strength = float(getattr(evt, "strength", 0.0) or 0.0)
    raw_row_ids = list(getattr(evt, "raw_row_ids", None) or [])

    if not source or not ticker:
        log.debug(
            "trust.on_signal_fired: missing source/ticker, skipping "
            "(src={s}, t={t})",
            s=source, t=ticker,
        )
        return

    # Normalise signal_type to BUY/SELL for register_signal which requires
    # a direction. If we cannot infer one, derive from sign of strength.
    if signal_type_raw in _BUY_SIGNAL_TYPES:
        direction = "BUY"
    elif signal_type_raw in _SELL_SIGNAL_TYPES:
        direction = "SELL"
    elif strength > 0:
        direction = "BUY"
    elif strength < 0:
        direction = "SELL"
    else:
        log.debug(
            "trust.on_signal_fired: cannot infer direction (sig_type={st}, "
            "strength=0.0), skipping",
            st=signal_type_raw,
        )
        return

    source_id = signal_type_raw or source
    metadata = {
        "contract_event_id": str(getattr(evt, "event_id", "")),
        "contract_signal_id": str(getattr(evt, "signal_id", "")),
        "raw_row_ids": raw_row_ids,
        "strength": strength,
    }

    try:
        row_id = register_signal(
            engine,
            source_type=source,
            source_id=source_id,
            ticker=ticker,
            signal_type=direction,
            signal_value=abs(strength) if strength else None,
            metadata=metadata,
        )
    except Exception as exc:
        log.warning(
            "trust.on_signal_fired({src}/{t}): {e}",
            src=source, t=ticker, e=str(exc),
        )
        return

    log.info(
        "trust.on_signal_fired: src={s} t={t} dir={d} strength={x:+.3f} "
        "row_id={r}",
        s=source, t=ticker, d=direction, x=strength, r=row_id,
    )


def on_edge_validated(
    evt: "EdgeValidated", *, engine: "Engine"
) -> None:
    """Bayesian-update cross_lens source trust on weak-edge validations.

    Companion to ``contracts.handlers.edges.on_edge_validated`` which owns
    the cross_lens *signal* trust downgrade. This handler owns the higher-
    level *source* trust update: the cross_lens *attribution module*
    itself loses a small slice of Bayesian credit every time one of its
    cited edges is empirically weakened.

    Best-effort: any DB failure is logged at ``warning`` and swallowed.
    """
    if not getattr(evt, "relationship_weak", False):
        return

    edge_id = int(getattr(evt, "edge_id", 0) or 0)
    if edge_id <= 0:
        return

    correlation = float(getattr(evt, "validation_correlation", 0.0) or 0.0)

    try:
        with engine.begin() as conn:
            result = conn.execute(
                text(
                    """
                    UPDATE signal_sources AS s
                    SET trust_score = GREATEST(
                            :floor,
                            COALESCE(s.trust_score, 0.5) * :factor
                        )
                    WHERE s.source_type = :source_type
                      AND s.id IN (
                          SELECT s2.id
                          FROM signal_sources s2
                          JOIN supply_shock_attributions a
                            ON a.signal_source_id = s2.id
                          WHERE a.edge_id = :eid
                      )
                    """
                ),
                {
                    "floor": _MIN_TRUST_FLOOR,
                    "factor": _WEAK_EDGE_TRUST_FACTOR,
                    "eid": edge_id,
                    "source_type": _CROSS_LENS_SUPPLY_SHOCK_SOURCE_TYPE,
                },
            )
            downgraded = getattr(result, "rowcount", 0) or 0
    except Exception as exc:
        log.warning(
            "trust.on_edge_validated(edge_id={eid}): {e}",
            eid=edge_id, e=str(exc),
        )
        return

    log.info(
        "trust.on_edge_validated: edge={eid} corr={c:+.3f} "
        "cross_lens_sources_downgraded={n}",
        eid=edge_id, c=correlation, n=downgraded,
    )
