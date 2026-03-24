"""
GRID alert scheduler.

Starts a daemon thread that runs the daily digest email at 7 AM UTC.
Called from ``api/main.py`` during application startup.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

from loguru import logger as log


_scheduler_thread: threading.Thread | None = None
_stop_event = threading.Event()


def _run_loop() -> None:
    """Check once per minute whether it is 7:00 AM UTC; if so, send digest."""
    last_sent_date: str = ""

    while not _stop_event.is_set():
        try:
            now = datetime.now(timezone.utc)
            today_str = now.strftime("%Y-%m-%d")

            # Fire at 07:00 UTC, once per day
            if now.hour == 7 and now.minute == 0 and today_str != last_sent_date:
                last_sent_date = today_str
                log.info("Alert scheduler — triggering daily digest")
                try:
                    from alerts.email import daily_digest
                    daily_digest()
                except Exception as exc:
                    log.warning("Daily digest failed: {e}", e=str(exc))

        except Exception as exc:
            log.debug("Alert scheduler loop error: {e}", e=str(exc))

        _stop_event.wait(60)  # Sleep 60s (interruptible)


def schedule_alerts() -> None:
    """Start the alert scheduler daemon thread.

    Safe to call multiple times — only starts one thread.
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
    log.info("Alert scheduler started — daily digest at 07:00 UTC")


def stop_alerts() -> None:
    """Stop the alert scheduler (for clean shutdown)."""
    _stop_event.set()
    log.info("Alert scheduler stopped")
