"""Shared guards for the QuiverQuant transition scripts (re-key, gov_contracts re-date).

Both scripts touch ``signal_sources`` on the production database. They share:

* all approved blackout windows: 03:30-10:30, 10:58-11:12 UTC and weekday
  13:25-14:20 UTC; writes reserve five seconds before a boundary;
* a read-only session for the default dry run (``default_transaction_read_only``
  plus statement and lock timeouts, checked before the first query);
* two-second statements/COMMIT, one-second locks and five-second idle limits;
* a marker/window check before each write and immediately before COMMIT.

Nothing here deletes anything.
"""

from __future__ import annotations

import os
from datetime import datetime, time, timedelta, timezone
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from ingestion.altdata.quiverquant_identity import transition_marker_exists, transition_marker_path
from ingestion.altdata import quiverquant_transactions as tx

from intelligence.people_events_pipeline.readonly import (
    BACKUP_WINDOW_UTC,
    WindowClosed,
    assert_db_window_open,
)

STATEMENT_TIMEOUT_MS = tx.STATEMENT_TIMEOUT_MS
LOCK_TIMEOUT_MS = tx.LOCK_TIMEOUT_MS

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
        "-c idle_in_transaction_session_timeout=5000",
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
    """All owner-approved blackout windows, including weekday options/GEM."""
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    assert_db_window_open(now)
    if time(10, 58) <= now.time() < time(11, 12):
        raise WindowClosed("refusing database access: 10:58-11:12Z change blackout")
    if now.weekday() < 5 and time(13, 25) <= now.time() < time(14, 20):
        raise WindowClosed("refusing database access: weekday 13:25-14:20Z options/GEM blackout")


def write_guard(*, guard_check: bool = True, now: datetime | None = None) -> None:
    """Refuse writes/COMMIT within five seconds of the next blackout too."""
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    check_window(now)
    check_window(now + timedelta(seconds=tx.TRANSACTION_SECONDS))
    if guard_check:
        require_guard_closed()


def append_audit(audit: Any, records: list[dict[str, Any]]) -> None:
    """Only acknowledged commits are logged; an I/O failure stops execution."""
    import json

    for record in records:
        audit.write(json.dumps(record) + "\n")
    audit.flush()
    os.fsync(audit.fileno())


def preserve_committed(exc: BaseException, moved: int) -> None:
    """Fatal errors carry the acknowledged prefix, excluding uncertain COMMITs."""
    exc.committed_rows = moved
    exc.commit_uncertain = isinstance(exc, tx.CommitUncertain)


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
