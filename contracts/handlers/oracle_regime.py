"""Oracle regime handler (SYNTH-30 closure, §7.3).

Consumes ``RegimeTransition`` contracts and persists the transition into
``regime_transitions_audit`` so the next oracle scoring cycle can apply
the family-weight multipliers per ``oracle.engine._get_credit_cycle_routing``-
style routing.

Heavy logic (family-weight adjustment, transition matrix consumption)
stays in ``oracle/engine.py``. This handler is the bus persistence shim.

Non-fatal: DB failure is logged at ``warning`` and swallowed.
"""
from __future__ import annotations

import json
from typing import TYPE_CHECKING

from loguru import logger as log
from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from contracts.schemas import RegimeTransition


def on_regime_transition(
    evt: "RegimeTransition", *, engine: "Engine"
) -> None:
    """Persist a regime transition for oracle consumption.

    Auto-creates ``regime_transitions_audit`` if missing.  Idempotent on
    ``event_id`` via ``ON CONFLICT DO NOTHING``.

    Schema:
        CREATE TABLE regime_transitions_audit (
            id BIGSERIAL PRIMARY KEY,
            event_id UUID NOT NULL UNIQUE,
            from_state TEXT NOT NULL,
            to_state TEXT NOT NULL,
            confidence DOUBLE PRECISION,
            triggering_features JSONB,
            transition_probability_matrix JSONB,
            producer_module TEXT,
            correlation_id UUID,
            created_at TIMESTAMPTZ DEFAULT now()
        );
    """
    event_id = str(getattr(evt, "event_id", ""))
    if not event_id:
        return

    from_state = getattr(evt, "from_state", "") or ""
    to_state = getattr(evt, "to_state", "") or ""
    if not from_state or not to_state:
        return

    confidence = float(getattr(evt, "confidence", 0.0) or 0.0)
    triggers = list(getattr(evt, "triggering_features", None) or [])
    matrix = list(getattr(evt, "transition_probability_matrix", None) or [])
    producer = getattr(evt, "producer_module", "") or ""
    correlation_id = str(getattr(evt, "correlation_id", "") or "") or None

    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS regime_transitions_audit (
                        id BIGSERIAL PRIMARY KEY,
                        event_id UUID NOT NULL UNIQUE,
                        from_state TEXT NOT NULL,
                        to_state TEXT NOT NULL,
                        confidence DOUBLE PRECISION,
                        triggering_features JSONB,
                        transition_probability_matrix JSONB,
                        producer_module TEXT,
                        correlation_id UUID,
                        created_at TIMESTAMPTZ DEFAULT now()
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO regime_transitions_audit (
                        event_id, from_state, to_state, confidence,
                        triggering_features, transition_probability_matrix,
                        producer_module, correlation_id
                    ) VALUES (
                        CAST(:eid AS UUID), :fr, :to, :c,
                        CAST(:tf AS JSONB), CAST(:tpm AS JSONB),
                        :prod, CAST(:cid AS UUID)
                    )
                    ON CONFLICT (event_id) DO NOTHING
                    """
                ),
                {
                    "eid": event_id,
                    "fr": from_state,
                    "to": to_state,
                    "c": confidence,
                    "tf": json.dumps(triggers),
                    "tpm": json.dumps(matrix),
                    "prod": producer,
                    "cid": correlation_id,
                },
            )
    except Exception as exc:
        log.warning(
            "oracle_regime.on_regime_transition({a}->{b}): {e}",
            a=from_state, b=to_state, e=str(exc),
        )
        return

    log.info(
        "oracle_regime.on_regime_transition: {a} -> {b} conf={c:.3f}",
        a=from_state, b=to_state, c=confidence,
    )
