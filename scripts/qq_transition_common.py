"""Shared guards for the QuiverQuant transition scripts (re-key, gov_contracts re-date).

Both scripts touch ``signal_sources`` on the production database. They share:

* the backup-window refusal (03:30-10:30 UTC, the nightly ``pg_dump`` and its
  tail), taken from ``intelligence.people_events_pipeline.readonly`` so there
  is one definition of the window; it applies to reads as well as writes;
* a read-only session for the default dry run (``default_transaction_read_only``
  plus statement and lock timeouts, checked before the first query);
* a write session with the same timeouts for ``--apply``.

Nothing here deletes anything.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from ingestion.altdata.quiverquant_identity import transition_marker_exists, transition_marker_path

from intelligence.people_events_pipeline.readonly import (
    BACKUP_WINDOW_UTC,
    WindowClosed,
    assert_db_window_open,
)

STATEMENT_TIMEOUT_MS = 20_000
LOCK_TIMEOUT_MS = 2_000

__all__ = [
    "BACKUP_WINDOW_UTC",
    "WindowClosed",
    "assert_db_window_open",
    "database_url",
    "open_engine",
    "assert_read_only",
    "check_window",
    "require_guard_closed",
    "is_lock_or_timeout",
    "transition_marker_exists",
    "transition_marker_path",
]


def database_url(db_url_env: str | None) -> str:
    """The database URL: the named env var, else ``config.settings.DB_URL``."""
    if db_url_env:
        return os.environ[db_url_env]
    from config import settings

    return settings.DB_URL


def open_engine(url: str, *, read_only: bool, application_name: str) -> Engine:
    """A one-connection engine; read-only sessions refuse every write at the server."""
    options = [
        f"-c statement_timeout={STATEMENT_TIMEOUT_MS}",
        f"-c lock_timeout={LOCK_TIMEOUT_MS}",
        "-c idle_in_transaction_session_timeout=60000",
        f"-c application_name={application_name}",
    ]
    if read_only:
        options.append("-c default_transaction_read_only=on")
    return create_engine(
        url,
        connect_args={"options": " ".join(options)},
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
    )


def assert_read_only(conn: Any) -> None:
    """Refuse to continue unless the session really is read-only."""
    value = conn.execute(text("SHOW transaction_read_only")).scalar()
    if str(value).lower() != "on":
        raise RuntimeError("connection is not read-only; refusing to continue")


def check_window(now: datetime | None = None) -> None:
    """Raise ``WindowClosed`` inside 03:30-10:30 UTC."""
    assert_db_window_open(now)


def require_guard_closed() -> None:
    """Refuse ``--apply`` / ``--revert`` once the transition marker exists.

    Until the marker is created the writer does not pull the guarded endpoints, so
    nothing can land between the dry run and the apply (a pull in between creates
    keyed duplicates next to the legacy rows, or overwrites a calendar-dated
    gov_contracts row with the next quarter's payload). A marker that already exists
    means pulls may already be flowing.
    """
    if transition_marker_exists():
        raise RuntimeError(
            f"refusing to write: the transition marker {transition_marker_path()} exists, so QuiverQuant "
            "pulls may already be running. Remove the marker to re-close the guard (and confirm the "
            "scheduler records quiverquant as SKIPPED), or pass --no-guard-check if QuiverQuant is "
            "paused another way."
        )


def is_lock_or_timeout(exc: BaseException) -> bool:
    """True for a lock_not_available (55P03) or query_canceled (57014) database error.

    Raised when ``lock_timeout`` / ``statement_timeout`` fires, e.g. against a
    concurrent uncommitted write to the same row. The scripts report and skip.
    """
    if not isinstance(exc, OperationalError):
        return False
    return getattr(getattr(exc, "orig", None), "pgcode", None) in {"55P03", "57014"}
