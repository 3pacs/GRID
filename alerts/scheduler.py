"""
GRID alert scheduler.

Starts a daemon thread that runs the daily digest email once per day in
the 07:00-07:59 UTC hour. Called from ``api/main.py`` during application
startup.

Dedup is persisted in ``alert_state`` (alert_type="research_daily_digest",
entity_id="daily") rather than held in a process-local variable, so a
restart mid-window — or a second process, if this is ever deployed with
more than the current single worker — cannot resend the same day's
digest. Same pattern already proven by scripts/daily_digest.py's
Hermes-cycle digest, which uses the same table.
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

_scheduler_thread: threading.Thread | None = None
_stop_event = threading.Event()


def _latest_sent_at(engine: Any) -> datetime | None:
    """Return the last persisted send timestamp for this digest, if any."""
    if engine is None:
        return None
    try:
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
    except Exception as exc:
        log.debug("Could not read persisted digest timestamp: {e}", e=str(exc))
        return None


def _record_sent(engine: Any, sent_at: datetime) -> None:
    """Persist the send timestamp so a restart or second process can't resend."""
    if engine is None:
        return
    try:
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
    except Exception as exc:
        log.debug("Could not persist digest timestamp: {e}", e=str(exc))


def _should_send_now(engine: Any, now: datetime) -> bool:
    """Is it time to send the digest, per persisted state?

    Kept separate from the loop and from any real clock/thread so tests
    can drive it directly with a fake engine and a fixed ``now`` instead
    of starting the real daemon thread (which would otherwise reach real
    DB/SMTP code if a test happened to run during the actual send hour).
    """
    if now.hour != _DIGEST_HOUR_UTC:
        return False
    last = _latest_sent_at(engine)
    if last is not None and (now - last) < timedelta(hours=_MIN_GAP_HOURS):
        return False
    return True


def _run_loop() -> None:
    """Check once per minute whether the digest is due; if so, send it."""
    while not _stop_event.is_set():
        try:
            from db import get_engine

            engine = get_engine()
            now = datetime.now(timezone.utc)
            if _should_send_now(engine, now):
                log.info("Alert scheduler — triggering daily digest")
                try:
                    from alerts.email import daily_digest

                    result = daily_digest()
                    if result.get("sent"):
                        _record_sent(engine, now)
                    else:
                        log.warning(
                            "Daily digest did not send — will retry next minute: {e}",
                            e=result.get("error"),
                        )
                except Exception as exc:
                    log.warning("Daily digest failed: {e}", e=str(exc))
        except Exception as exc:
            log.debug("Alert scheduler loop error: {e}", e=str(exc))

        _stop_event.wait(60)  # Sleep 60s (interruptible)


def schedule_alerts() -> None:
    """Start the alert scheduler daemon thread.

    Safe to call multiple times — only starts one thread per process.
    Send-state dedup lives in the database (see ``_should_send_now``), so
    a restart mid-window, or a second process if this is ever deployed
    with more than one worker, still cannot double-send the same day.
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
