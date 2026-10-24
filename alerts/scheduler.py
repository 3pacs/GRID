"""
GRID alert scheduler.

Starts a daemon thread that runs the daily digest email once per day in
the 07:00-07:59 UTC hour. Called from ``api/main.py`` during application
startup.

Concurrency and crash safety (2026-09-17, second revision): the first
rewrite closed the two-worker race with a Postgres advisory lock held
across the whole check-send-record sequence, but still recorded state
only *after* a send attempt returned — a confirmed SMTP accept followed
by a persistence failure (or a process crash right after) left nothing
durable recorded, so the next window's check saw "never sent" and could
resend an already-accepted message. This revision persists an
"uncertain" marker *before* calling ``daily_digest()``, and only
overwrites it with a terminal "sent" or "failed" once
``alerts.email._do_send`` (via ``daily_digest``) returns a real,
three-way outcome — SMTP is genuinely ambiguous partway through a send
(the relay may have accepted a message we lost the connection before
confirming), and a crash between the pre-send marker and the terminal
write leaves exactly that same "uncertain" state on disk. Either way,
the next check must not auto-resend on top of an uncertain prior
attempt — it can only clear once the marker goes stale, and even then
that's a policy choice (favor missing a day over risking a duplicate),
not a guess that it's now safe.

Dedup state lives in ``alert_state`` (alert_type="research_daily_digest",
entity_id="daily"), the same table scripts/daily_digest.py uses for the
Hermes digest — payload JSONB carries {"status": "uncertain"|"sent"|"failed"}.
"""

from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger as log


_ALERT_TYPE = "research_daily_digest"
_ENTITY_ID = "daily"
_DIGEST_HOUR_UTC = 7
_MIN_GAP_HOURS = 20
# An "uncertain" marker older than this is treated as a finished (not
# still in-flight) attempt whose outcome we'll never learn — see
# _send_digest_once_per_window. It does not become eligible to resend;
# it stays blocked for the rest of this gap window either way, the same
# as a confirmed "sent" would, on the assumption an accepted message may
# already be in the recipient's inbox.
_UNCERTAIN_STALE_MINUTES = 5
# Arbitrary but fixed 32-bit key for the digest send lock (pg_advisory_lock
# takes a bigint; a single int key is fine and simpler than the two-int form).
_ADVISORY_LOCK_KEY = 0x47_44_49_47  # 'GDIG' bytes, no meaning beyond being stable

_scheduler_thread: threading.Thread | None = None
_stop_event = threading.Event()


def _read_state(engine: Any) -> dict[str, Any] | None:
    """Return the persisted state row for this digest, if any.

    ``{"seen_at": datetime, "status": str | None}``. Raises on a DB
    error rather than swallowing it — the caller must fail closed.
    """
    from sqlalchemy import text as sa_text

    with engine.connect() as conn:
        row = conn.execute(
            sa_text(
                "SELECT seen_at, payload FROM alert_state "
                "WHERE alert_type = :t AND entity_id = :e "
                "ORDER BY seen_at DESC LIMIT 1"
            ),
            {"t": _ALERT_TYPE, "e": _ENTITY_ID},
        ).fetchone()
    if not row or not row[0]:
        return None
    seen_at = row[0]
    seen_at = seen_at if seen_at.tzinfo else seen_at.replace(tzinfo=timezone.utc)
    payload = row[1] or {}
    if isinstance(payload, str):
        payload = json.loads(payload)
    return {"seen_at": seen_at, "status": payload.get("status")}


def _write_state(engine: Any, at: datetime, status: str) -> None:
    """Persist {status}. Raises on failure — see caller."""
    from sqlalchemy import text as sa_text

    with engine.begin() as conn:
        conn.execute(
            sa_text(
                "INSERT INTO alert_state (alert_type, entity_id, seen_at, payload) "
                "VALUES (:t, :e, :seen_at, :payload) "
                "ON CONFLICT (alert_type, entity_id) DO UPDATE SET "
                "seen_at = EXCLUDED.seen_at, payload = EXCLUDED.payload"
            ),
            {"t": _ALERT_TYPE, "e": _ENTITY_ID, "seen_at": at, "payload": json.dumps({"status": status})},
        )


def _send_digest_once_per_window(engine: Any, now: datetime) -> None:
    """Attempt the daily digest send, at most once per process at a time
    and at most once globally per ``_MIN_GAP_HOURS`` window — and never
    automatically on top of an unresolved prior attempt.

    Holds ``pg_try_advisory_lock`` for the entire check-send-record
    sequence: only the holder can pass the "eligible to send" check and
    reach ``daily_digest()``. Before calling it, persists status
    "uncertain" — if the process dies or the terminal write itself fails
    anywhere after that point, the marker that survives on disk is
    "uncertain", never silently "never attempted". Any exception
    anywhere in this function means no send this minute: fail closed,
    never fail open into sending.
    """
    if now.hour != _DIGEST_HOUR_UTC or engine is None:
        return

    from sqlalchemy import text as sa_text

    lock_conn = None
    try:
        lock_conn = engine.connect()
        got_lock = lock_conn.execute(
            sa_text("SELECT pg_try_advisory_lock(:key)"), {"key": _ADVISORY_LOCK_KEY}
        ).scalar()
        if not got_lock:
            log.debug("Another process holds the digest send lock this minute")
            return
        try:
            state = _read_state(engine)
            if state is not None:
                age = now - state["seen_at"]
                status = state["status"]
                if status not in ("sent", "failed", "uncertain"):
                    # A row with a timestamp but no recognized status —
                    # e.g. written by a version of this code before the
                    # status payload existed, or by anything else that
                    # ever touches this alert_type/entity_id key.
                    # Unknown does not mean safe: treat it exactly like a
                    # confirmed "sent" rather than falling through both
                    # named branches and treating an unrecognized-but-
                    # recent timestamp as if there were no prior state at
                    # all.
                    status = "sent"
                if status == "sent" and age < timedelta(hours=_MIN_GAP_HOURS):
                    return
                if status == "uncertain":
                    if age < timedelta(minutes=_UNCERTAIN_STALE_MINUTES):
                        log.debug("A digest send attempt is still recent and unresolved — waiting")
                        return
                    if age < timedelta(hours=_MIN_GAP_HOURS):
                        # Stale and unresolved: do not guess. Surface it
                        # loudly so a human checks whether it actually
                        # sent, and do not auto-retry within this window
                        # on the assumption it may have.
                        log.warning(
                            "Daily digest send outcome from {at} is still unresolved after "
                            "{m}+ minutes — not auto-retrying this window. Check SMTP/mail logs "
                            "and clear alert_state (alert_type={t}, entity_id={e}) if it did not "
                            "actually send.",
                            at=state["seen_at"].isoformat(), m=_UNCERTAIN_STALE_MINUTES,
                            t=_ALERT_TYPE, e=_ENTITY_ID,
                        )
                        return
                    # else: old enough that the whole window has elapsed
                    # regardless — fall through and try again, same as
                    # any other new window would.
                # status == "failed" (a clean, pre-acceptance failure) is
                # always eligible for immediate retry regardless of age.

            log.info("Alert scheduler — claimed digest send lock, building daily digest")
            _write_state(engine, now, status="uncertain")
            from alerts.email import daily_digest

            result = daily_digest()
            status = result.get("send_status", "uncertain")
            _write_state(engine, now, status=status)
            if status != "sent":
                log.warning(
                    "Daily digest did not confirm send this attempt (status={s}): {e}",
                    s=status, e=result.get("error"),
                )
        finally:
            lock_conn.execute(sa_text("SELECT pg_advisory_unlock(:key)"), {"key": _ADVISORY_LOCK_KEY})
    except Exception as exc:
        log.warning("Digest send attempt failed — failing closed, no send recorded: {e}", e=str(exc))
    finally:
        if lock_conn is not None:
            lock_conn.close()


def _run_loop() -> None:
    """Check once per minute whether the digest is due; if so, send it."""
    while not _stop_event.is_set():
        try:
            from db import get_engine

            engine = get_engine()
            now = datetime.now(timezone.utc)
            _send_digest_once_per_window(engine, now)
        except Exception as exc:
            log.debug("Alert scheduler loop error: {e}", e=str(exc))

        _stop_event.wait(60)  # Sleep 60s (interruptible)


def schedule_alerts() -> None:
    """Start the alert scheduler daemon thread.

    Safe to call multiple times — only starts one thread per process.
    Send-state dedup and mutual exclusion live in the database (see
    ``_send_digest_once_per_window``), so a restart mid-window, or a
    second process if this is ever deployed with more than one worker,
    still cannot double-send the same day.
    """
    global _scheduler_thread

    if _scheduler_thread is not None and _scheduler_thread.is_alive():
        log.debug("Alert scheduler already running")
        return

    _stop_event.clear()
    _scheduler_thread = threading.Thread(
        target=_run_loop,
        daemon=True,
        name="alert-scheduler",
    )
    _scheduler_thread.start()
    log.info(
        "Alert scheduler started — daily digest in the {h:02d}:00 UTC hour",
        h=_DIGEST_HOUR_UTC,
    )


def stop_alerts() -> None:
    """Stop the alert scheduler (for clean shutdown)."""
    _stop_event.set()
    log.info("Alert scheduler stopped")
