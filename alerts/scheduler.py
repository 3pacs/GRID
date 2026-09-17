"""
GRID alert scheduler.

Starts a daemon thread that runs the daily digest email once per day in
the 07:00-07:59 UTC hour. Called from ``api/main.py`` during application
startup.

Concurrency (2026-09-17 rewrite): the first version deduped by reading
the last-send timestamp, sending, and only afterward recording it — two
processes (or two racing loop iterations) could both pass the read
before either wrote, and a persistence failure after a real send meant
the state table never advanced, so the loop kept retrying every minute
for the rest of the hour. This version holds a Postgres advisory lock
(``pg_try_advisory_lock``) for the whole check-send-record sequence, so
only one holder can ever be inside it at a time, and fails closed — any
error acquiring the lock or reading state means no send this minute,
never "send anyway." Dedup state itself still lives in ``alert_state``
(alert_type="research_daily_digest", entity_id="daily"), the same table
scripts/daily_digest.py uses for the Hermes digest.
"""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger as log


_ALERT_TYPE = "research_daily_digest"
_ENTITY_ID = "daily"
_DIGEST_HOUR_UTC = 7
_MIN_GAP_HOURS = 20
# Arbitrary but fixed 32-bit key for the digest send lock (pg_advisory_lock
# takes a bigint; a single int key is fine and simpler than the two-int form).
_ADVISORY_LOCK_KEY = 0x47_44_49_47  # 'GDIG' bytes, no meaning beyond being stable

_scheduler_thread: threading.Thread | None = None
_stop_event = threading.Event()


def _latest_sent_at(engine: Any) -> datetime | None:
    """Return the last persisted send timestamp for this digest, if any.

    Raises on a DB error rather than swallowing it — the caller must fail
    closed (treat "can't tell" as "don't send"), not treat an unreadable
    state table as "never sent."
    """
    from sqlalchemy import text as sa_text

    with engine.connect() as conn:
        row = conn.execute(
            sa_text(
                "SELECT seen_at FROM alert_state "
                "WHERE alert_type = :t AND entity_id = :e "
                "ORDER BY seen_at DESC LIMIT 1"
            ),
            {"t": _ALERT_TYPE, "e": _ENTITY_ID},
        ).fetchone()
    if not row or not row[0]:
        return None
    ts = row[0]
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def _record_sent(engine: Any, sent_at: datetime) -> None:
    """Persist the send timestamp. Raises on failure — see caller."""
    from sqlalchemy import text as sa_text

    with engine.begin() as conn:
        conn.execute(
            sa_text(
                "INSERT INTO alert_state (alert_type, entity_id, seen_at) "
                "VALUES (:t, :e, :seen_at) "
                "ON CONFLICT (alert_type, entity_id) DO UPDATE SET "
                "seen_at = EXCLUDED.seen_at"
            ),
            {"t": _ALERT_TYPE, "e": _ENTITY_ID, "seen_at": sent_at},
        )


def _send_digest_once_per_window(engine: Any, now: datetime) -> None:
    """Attempt the daily digest send, at most once per process at a time
    and at most once globally per ``_MIN_GAP_HOURS`` window.

    Holds ``pg_try_advisory_lock`` for the entire check-send-record
    sequence: only the holder can pass the "not yet sent" check and reach
    ``daily_digest()``, closing the race a plain read-then-later-write
    left open. A failed or unconfirmed send does not record anything, so
    the next minute's iteration (once the lock is free again) can retry —
    that retry is legitimate because ``daily_digest()`` now returns a
    real, synchronous SMTP outcome (not "dispatched to a thread"), so a
    retry here is a response to a known failure, not a blind repeat of an
    ambiguous one. Any exception anywhere in this function — acquiring
    the lock, reading state, sending, or recording — means no send: fail
    closed, never fail open into sending.
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
            last = _latest_sent_at(engine)
            if last is not None and (now - last) < timedelta(hours=_MIN_GAP_HOURS):
                return
            log.info("Alert scheduler — claimed digest send lock, building daily digest")
            from alerts.email import daily_digest

            result = daily_digest()
            if result.get("sent"):
                _record_sent(engine, now)
            else:
                log.warning(
                    "Daily digest did not confirm send this attempt — will retry: {e}",
                    e=result.get("error"),
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
