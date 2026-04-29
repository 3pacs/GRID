"""Provisional decision_journal handler (SYNTH-42).

Listens for ``SignalFired`` contracts and, when the signal carries a
confident strength (``|strength| > 0.7``), inserts a provisional row into
``decision_journal`` with ``source_contract_id`` set to the emitting
contract's ``event_id``. Outcome / PnL are left NULL — the existing
post-hoc scoring loop will fill them in later.

This is the "drain every confident signal into a journal row" lens of
the SYNTH-C wave — it lets the UI surface provisional decisions before
the oracle engine has gathered them into a full prediction cycle.

The handler MUST be non-fatal. Any DB failure is logged and swallowed so
that the contract bus keeps flowing.
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


#: Minimum absolute strength for a signal to earn a provisional journal row.
#: Below this threshold the handler is a silent no-op (too noisy otherwise).
PROVISIONAL_STRENGTH_THRESHOLD: float = 0.7


def on_signal_fired(evt: "SignalFired", *, engine: "Engine") -> None:
    """Insert a provisional ``decision_journal`` row when the signal is
    strong enough to be worth tracking on its own.

    Schema assumption (enforced by migration 0040):
        decision_journal.source_contract_id UUID NULL (btree index)

    All DB errors are logged at ``warning`` and swallowed — emitters
    must never observe a failure here.
    """
    strength = float(getattr(evt, "strength", 0.0) or 0.0)
    if abs(strength) <= PROVISIONAL_STRENGTH_THRESHOLD:
        return

    ticker = getattr(evt, "ticker", None)
    if not ticker:
        return

    signal_type = getattr(evt, "signal_type", None) or "unknown"
    source = getattr(evt, "source", "unknown") or "unknown"
    event_id = str(getattr(evt, "event_id", ""))
    producer = getattr(evt, "producer_module", "") or ""
    now = datetime.now(timezone.utc)

    # Serialize a minimal context blob so the row is self-describing when
    # the operator UI renders it. We deliberately don't pull the full
    # payload from contracts_audit — the event_id is the join key.
    context = {
        "signal_type": signal_type,
        "source": source,
        "producer_module": producer,
        "strength": strength,
        "raw_row_ids": list(getattr(evt, "raw_row_ids", None) or []),
    }

    # Placeholder ``operator_confidence`` bucket driven by strength.
    # ``decision_journal`` has hard NOT NULL + CHECK constraints on many
    # columns, so we fill every required field with a provisional default
    # keyed off the signal so the row is still self-describing.
    abs_s = abs(strength)
    if abs_s >= 0.9:
        op_conf = "HIGH"
    elif abs_s >= 0.75:
        op_conf = "MEDIUM"
    else:
        op_conf = "LOW"

    action = f"PROVISIONAL {signal_type.upper()} {ticker.upper()}"
    recommendation = f"WATCH {ticker.upper()}"
    counterfactual = f"provisional-from-contract:{event_id}"
    contradiction_flags = {
        "signal_type": signal_type,
        "source": source,
        "producer_module": producer,
        "strength": strength,
        "context": context,
    }

    try:
        with engine.begin() as conn:
            mv_row = conn.execute(
                text(
                    "SELECT id FROM model_registry "
                    "ORDER BY id ASC LIMIT 1"
                )
            ).fetchone()
            if mv_row is None:
                log.debug(
                    "journal.on_signal_fired: no model_registry rows, "
                    "skipping provisional insert for {st}/{t}",
                    st=signal_type, t=ticker,
                )
                return
            model_version_id = int(mv_row[0])

            conn.execute(
                text(
                    """
                    INSERT INTO decision_journal (
                        decision_timestamp, model_version_id,
                        inferred_state, state_confidence,
                        transition_probability, contradiction_flags,
                        grid_recommendation, baseline_recommendation,
                        action_taken, counterfactual, operator_confidence,
                        source_contract_id
                    ) VALUES (
                        :ts, :mvid,
                        :state, :sc,
                        :tp, CAST(:cf AS JSONB),
                        :gr, :br,
                        :at, :cft, :oc,
                        CAST(:cid AS UUID)
                    )
                    """
                ),
                {
                    "ts": now,
                    "mvid": model_version_id,
                    "state": f"PROVISIONAL_{signal_type.upper()}",
                    "sc": min(1.0, abs_s),
                    "tp": min(1.0, abs_s),
                    "cf": json.dumps(contradiction_flags),
                    "gr": recommendation,
                    "br": "HOLD",
                    "at": action,
                    "cft": counterfactual,
                    "oc": op_conf,
                    "cid": event_id,
                },
            )
    except Exception as exc:
        log.warning(
            "journal.on_signal_fired({st}/{t}): {e}",
            st=signal_type, t=ticker, e=str(exc),
        )
        return

    log.info(
        "journal.on_signal_fired: provisional row for {st} {t} "
        "strength={s:+.3f}",
        st=signal_type, t=ticker, s=strength,
    )
