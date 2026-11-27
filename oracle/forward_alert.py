"""Tell the operator once per session that the Oracle forward pass has no usable forecast.

Two processes can notice the same failing session: the Hermes operator, in
the hour it happens, and the daily freshness sweep, for a session in which
Hermes ran no pass at all. Each may also notice it more than once: Hermes
restarts, and the sweep judges "the last session that closed" again on every
day of a weekend or holiday. Without a shared record each of those would send
the same alert again.

The record is one marker row per session in ``oracle_iterations`` (notes =
``oracle.forward_health.alert_note(session_date)``), written only after the
e-mail was accepted. Both entry points go through :func:`send_session_alert`,
which checks, sends and marks under one transaction-scoped advisory lock, so
they cannot both send. A send that fails writes no marker and is tried again
by the next caller.

When an alert is due is decided from the durable record alone
(:func:`alert_due`), so it does not depend on what a restarted process
remembers.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text

from oracle.forward_health import (
    STATUS_ALL_ABSTAINED, STATUS_NO_ATTEMPTS, STATUS_NO_ROWS, STATUS_PARTIAL, alert_note, alert_text,
)

#: Passes that ran and stored nothing before the operator is told, and how
#: long they must have been at it. One such pass is common (the first capture
#: after the open often fails: 11-18 and 11-19 both began that way and had
#: their forecast an hour later). Two or more, at least an hour after the
#: first, are not. The time matters as well as the count, so that a faster
#: cadence cannot turn a slow morning into an alert.
BARREN_ATTEMPTS_BEFORE_ALERT = 2
BARREN_SPAN_BEFORE_ALERT = timedelta(hours=1)

SENT = "sent"
ALREADY_SENT = "already_sent"
SEND_FAILED = "send_failed"


def _clock(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(value) if isinstance(value, str) else None
    except ValueError:
        return None


def alert_due(health: dict[str, Any]) -> tuple[bool, str]:
    """Whether this session's record calls for the alert, and why. Needs an admin answer.

    * every candidate abstained: at once (a refused quote does not fix itself);
    * passes ran and stored nothing (no capture, or no model produced a
      candidate): once ``BARREN_ATTEMPTS_BEFORE_ALERT`` attempts are on record
      and the first of them is ``BARREN_SPAN_BEFORE_ALERT`` old;
    * no pass ran at all: once the session has ended (the sweep's case);
    * a cohort ticker has no forecast while another has one: once the session
      has ended. During the session a later pass may still give it one. A
      captured ticker on which no model produces a candidate writes no row at
      all, so nothing else would ever say so (SPY, every session since the
      forward policy began).
    """
    status = health.get("status")
    attempts = int(health.get("attempts") or 0)
    if status == STATUS_ALL_ABSTAINED:
        return True, "every candidate abstained"
    if status == STATUS_NO_ROWS and attempts >= BARREN_ATTEMPTS_BEFORE_ALERT:
        first, checked = _clock(health.get("first_attempt_at")), _clock(health.get("checked_at"))
        if first is None or checked is None or checked - first < BARREN_SPAN_BEFORE_ALERT:
            return False, "attempts stored nothing, for less than an hour so far"
        return True, f"{attempts} attempts stored nothing"
    if status == STATUS_NO_ATTEMPTS and health.get("session_state") == "ended":
        return True, "no pass ran in the session"
    if status == STATUS_PARTIAL and health.get("session_state") == "ended":
        return True, "a cohort ticker ended the session without a forecast"
    return False, "not due"


def send_session_alert(
    engine: Any, health: dict[str, Any], *,
    send: Callable[[str, str, str], bool] | None = None, extra: str = "",
) -> str:
    """Send the alert for ``health``'s session unless one was already sent. Returns the outcome.

    ``SENT``, ``ALREADY_SENT`` or ``SEND_FAILED``. The marker is written in
    the same transaction that holds the lock, after the mail was accepted. If
    the marker cannot be written the mail has still gone out, and the next
    caller may send it again: an extra alert, never a missing one.
    """
    if send is None:
        from alerts.email import send_alert

        def send(subject: str, body: str, severity: str) -> bool:
            return bool(send_alert(subject, body, severity=severity))

    session = str(health["session_date"])
    note = alert_note(session)
    with engine.begin() as conn:
        if conn.dialect.name == "postgresql":
            conn.execute(text(
                "SELECT pg_advisory_xact_lock(hashtext('oracle_forward_alert'), hashtext(:session))"
            ), {"session": session})
        if conn.execute(text("SELECT 1 FROM oracle_iterations WHERE notes = :note LIMIT 1"),
                        {"note": note}).fetchone() is not None:
            return ALREADY_SENT
        subject, body = alert_text(health)
        if extra:
            body += "\n\n" + extra
        if not send(subject, body, "critical"):
            log.error("Oracle forward alert for {s} was not sent (alert e-mail disabled or failed)",
                      s=session)
            return SEND_FAILED
        conn.execute(text(
            "INSERT INTO oracle_iterations (iteration_at, models_updated, predictions_scored, notes)"
            " VALUES (:at, 0, 0, :note)"
        ), {"at": datetime.now(timezone.utc), "note": note})
    log.error("Oracle forward pass has no usable forecast: {m}", m=health["message"])
    return SENT
