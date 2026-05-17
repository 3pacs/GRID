"""Operator-alert handlers (SYNTH-31 / Wave-D, §7.3 closure).

Routes ``CrossReferenceAnomaly`` (severity >= HIGH) and ``RegimeTransition``
contracts to the existing alerts package. Heavy delivery logic
(email/push/template) stays in ``alerts/email.py`` and ``alerts/push_notify.py``;
this handler is a pure adapter from the contract envelope to the
package's existing entry-points.

Idempotency: each fired alert is recorded in ``contract_alert_log`` keyed
on ``event_id`` so a replayed contract does not double-page the operator.

Non-fatal — DB or alerter failure is logged at ``warning`` and swallowed.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger as log
from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from contracts.schemas import CrossReferenceAnomaly, RegimeTransition


#: Severity threshold for paging the operator. LOW/MEDIUM are persisted
#: in the anti-signal pipeline but do not page.
_ALERT_SEVERITIES: tuple[str, ...] = ("HIGH", "CRITICAL")


def _record_alert(engine: "Engine", *, event_id: str, kind: str) -> bool:
    """Mark an event as alerted. Returns True if we just claimed this
    event_id (first time), False if it was already recorded (replay).

    Auto-creates the dedup table on first call.
    """
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS contract_alert_log (
                        event_id UUID PRIMARY KEY,
                        kind TEXT NOT NULL,
                        fired_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
            )
            result = conn.execute(
                text(
                    """
                    INSERT INTO contract_alert_log (event_id, kind)
                    VALUES (CAST(:eid AS UUID), :k)
                    ON CONFLICT (event_id) DO NOTHING
                    """
                ),
                {"eid": event_id, "k": kind},
            )
            # rowcount is 1 on insert, 0 on dedup
            return bool(getattr(result, "rowcount", 0) or 0)
    except Exception as exc:
        log.warning(
            "alerts._record_alert({k}, evt={e}): {exc}",
            k=kind, e=event_id, exc=str(exc),
        )
        return False


def on_cross_reference_anomaly(
    evt: "CrossReferenceAnomaly", *, engine: "Engine"
) -> None:
    """Page the operator on HIGH/CRITICAL cross-reference anomalies.

    Below HIGH severity is a silent no-op — the persistence handler
    already keeps the row for later forensic review.
    """
    severity = (getattr(evt, "severity", "") or "").upper()
    if severity not in _ALERT_SEVERITIES:
        return

    event_id = str(getattr(evt, "event_id", ""))
    if not event_id:
        return

    if not _record_alert(engine, event_id=event_id, kind="cross_reference"):
        log.debug(
            "alerts.on_cross_reference_anomaly: replay skip evt={e}",
            e=event_id,
        )
        return

    statistic = getattr(evt, "statistic", "?") or "?"
    delta = float(getattr(evt, "confidence_delta", 0.0) or 0.0)

    title = f"Cross-reference anomaly: {statistic} ({severity})"
    body = (
        f"Statistic '{statistic}' diverged from physical-reality proxy "
        f"with confidence delta {delta:+.3f}. "
        f"See oracle_anti_signals for the full row."
    )

    try:
        # Late import — alerts package may not be importable in every test
        # environment; we still want the rest of the handler to run.
        from alerts.push_notify import notify_red_flag
        notify_red_flag(title=title, description=body)
    except Exception as exc:
        log.warning(
            "alerts.on_cross_reference_anomaly: push_notify failed: {e}",
            e=str(exc),
        )

    log.info(
        "alerts.on_cross_reference_anomaly: paged stat={s} sev={sv}",
        s=statistic, sv=severity,
    )


def on_regime_transition(
    evt: "RegimeTransition", *, engine: "Engine"
) -> None:
    """Page on every regime transition. Operator wants every move.

    The dedup key is the event_id, so a replay (or the dispatcher's retry
    scheduler) does not double-page.
    """
    event_id = str(getattr(evt, "event_id", ""))
    if not event_id:
        return

    if not _record_alert(engine, event_id=event_id, kind="regime_transition"):
        log.debug(
            "alerts.on_regime_transition: replay skip evt={e}", e=event_id,
        )
        return

    from_state = getattr(evt, "from_state", "?") or "?"
    to_state = getattr(evt, "to_state", "?") or "?"
    confidence = float(getattr(evt, "confidence", 0.0) or 0.0)

    try:
        from alerts.email import alert_on_regime_change
        alert_on_regime_change(
            from_regime=from_state,
            to_regime=to_state,
            confidence=confidence,
        )
    except Exception as exc:
        log.warning(
            "alerts.on_regime_transition: email failed: {e}", e=str(exc),
        )

    log.info(
        "alerts.on_regime_transition: paged {a} -> {b} (conf={c:.3f})",
        a=from_state, b=to_state, c=confidence,
    )
