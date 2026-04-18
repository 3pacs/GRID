"""Oracle SignalFired fanout handler (SYNTH-24..27).

Listens for ``SignalFired`` contracts emitted by the offensive-alpha
detectors (``intelligence/holder_deal_overlap.py``,
``intelligence/fundamental_divergence.py``) and projects them into the
``signal_sources`` table so that ``oracle.engine._gather_signals_from_registry``
picks them up on the next prediction cycle.

This module owns zero business logic — it just maps the contract shape
onto the same ``signal_sources`` upsert the classical signal pullers
use. Everything heavy (scoring, trust decay, convergence) stays in
``intelligence/trust_scorer.py``.

Signal types owned by this handler and their signal_sources.source_type
projection:

    holder_overlap              → source_type = 'holder_overlap'           (SYNTH-B)
    fundamental_divergence      → source_type = 'fundamental_divergence'   (SYNTH-B)
    contagion_ranked_impact     → source_type = 'contagion'                (SYNTH-C / SYNTH-35)
    chokepoint_crossing         → source_type = 'chokepoint_crossing'      (SYNTH-C / SYNTH-34)
    contagion_trigger           → source_type = 'news_trigger'             (SYNTH-C / SYNTH-38)

Any other ``signal_type`` is silently ignored so adding new detectors
later does not accidentally double-fire here.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from loguru import logger as log
from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from contracts.schemas import SignalFired


# Map each owned signal_type to the ``signal_sources.source_type`` we
# project it to. The on-disk source_type is the key the trust scorer
# uses for window / half-life / delta lookups, which is why it diverges
# from the contract ``signal_type`` in a few cases below.
_SIGNAL_TYPE_TO_SOURCE_TYPE: dict[str, str] = {
    "holder_overlap": "holder_overlap",
    "fundamental_divergence": "fundamental_divergence",
    "contagion_ranked_impact": "contagion",
    "chokepoint_crossing": "chokepoint_crossing",
    "contagion_trigger": "news_trigger",
}

_OWNED_SIGNAL_TYPES = frozenset(_SIGNAL_TYPE_TO_SOURCE_TYPE.keys())


def on_signal_fired(evt: "SignalFired", *, engine: "Engine") -> None:
    """Fan a ``SignalFired`` contract into ``signal_sources``.

    Unknown signal types are a noop — they belong to other handlers.
    Any DB failure is logged and swallowed so the contract bus keeps
    flowing (consumer failures bubble up as dead-letter rows via the
    dispatcher).
    """
    signal_type = getattr(evt, "signal_type", None)
    if signal_type not in _OWNED_SIGNAL_TYPES:
        return

    ticker = getattr(evt, "ticker", None)
    if not ticker:
        log.debug(
            "oracle_signals.on_signal_fired: no ticker on {st}, skipping",
            st=signal_type,
        )
        return

    strength = float(getattr(evt, "strength", 0.0) or 0.0)
    if abs(strength) < 1e-9:
        return

    source_type = _SIGNAL_TYPE_TO_SOURCE_TYPE[signal_type]
    direction = "BUY" if strength > 0 else "SELL"
    source = getattr(evt, "source", "unknown") or "unknown"
    now = datetime.now(timezone.utc)
    metadata = {
        "contract_event_id": str(getattr(evt, "event_id", "")),
        "producer_module": getattr(evt, "producer_module", ""),
        "raw_row_ids": list(getattr(evt, "raw_row_ids", None) or []),
        "actor_hint": getattr(evt, "actor_hint", None),
        "strength": strength,
        "contract_signal_type": signal_type,
    }

    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO signal_sources
                        (source_type, source_id, ticker, signal_type,
                         signal_date, signal_value, metadata, trust_score)
                    VALUES
                        (:st, :si, :t, :d, :sd, :p, :m, :ts)
                    """
                ),
                {
                    "st": source_type,
                    "si": source,
                    "t": ticker.upper(),
                    "d": direction,
                    "sd": now,
                    "p": abs(strength),
                    "m": json.dumps(metadata),
                    "ts": 0.5,
                },
            )
    except Exception as exc:
        log.warning(
            "oracle_signals.on_signal_fired({st}/{t}): {e}",
            st=signal_type, t=ticker, e=str(exc),
        )
        return

    log.info(
        "oracle_signals.on_signal_fired: {st} {d} {t} strength={s:+.3f}",
        st=signal_type, d=direction, t=ticker, s=strength,
    )
