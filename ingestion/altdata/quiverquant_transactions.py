"""Bounded QuiverQuant writes; an uncertain COMMIT is never replayed."""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Callable, Iterator

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy import exc as sa_exc

MAX_WRITE_ROWS = 50
STATEMENT_TIMEOUT_MS = 2_000
LOCK_TIMEOUT_MS = 1_000
COMMIT_TIMEOUT_MS = 2_000
TRANSACTION_SECONDS = 5


class CommitUncertain(RuntimeError):
    """Transaction resolution was not acknowledged: stop and reconcile manually."""


def is_rolled_back_write_error(exc: BaseException) -> bool:
    """Only an acquired write body with confirmed rollback may enter fallback."""
    return (isinstance(exc, sa_exc.DBAPIError)
            and getattr(exc, "qq_write_rolled_back", False)
            and not is_connection_error(exc))


def _known_commit_rejection(exc: BaseException, conn: Connection, driver: object) -> bool:
    """A narrow server rejection plus a healthy idle psycopg connection proves rollback.

    SQLSTATE alone is insufficient. Other COMMIT failures remain uncertain,
    including disconnects, invalidation and inability to inspect driver state.
    """
    if not isinstance(exc, sa_exc.DBAPIError) or is_connection_error(exc):
        return False
    code = getattr(exc.orig, "pgcode", None) or getattr(exc.orig, "sqlstate", None)
    if code not in {"23514", "57014"}:
        return False
    try:
        return (not conn.invalidated and not driver.closed
                and driver.get_transaction_status() == 0)
    except Exception:
        return False


def validate_batch_size(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= MAX_WRITE_ROWS:
        raise ValueError(f"batch_size must be an integer from 1 to {MAX_WRITE_ROWS}")


def is_connection_error(exc: BaseException) -> bool:
    if isinstance(exc, (sa_exc.DisconnectionError, sa_exc.TimeoutError, CommitUncertain)):
        return True
    if getattr(exc, "connection_invalidated", False):
        return True
    if isinstance(exc, sa_exc.OperationalError):
        code = getattr(exc.orig, "pgcode", None) or getattr(exc.orig, "sqlstate", None)
        return code is None or code.startswith("08") or code in {"57P01", "57P02", "57P03"}
    return False


@contextmanager
def write_transaction(
    engine: Engine, *, guard: Callable[[], None] | None = None,
) -> Iterator[tuple[Connection, Callable[[], None]]]:
    """Short transaction, with guards before every write and immediately before COMMIT.

    Statement timeout also bounds COMMIT on PostgreSQL 14. The elapsed check
    prevents committing an overlong transaction. A cleanup/COMMIT failure stops
    the caller, even when the server may actually have committed.
    """
    phase = "acquisition"
    conn = driver = None
    body_error: BaseException | None = None
    started = time.monotonic()

    def check() -> None:
        if guard is not None:
            guard()
        if time.monotonic() - started >= TRANSACTION_SECONDS:
            raise RuntimeError("QuiverQuant transaction exceeded its five-second budget")

    try:
        check()
        with engine.begin() as conn:
            phase = "setup"
            if getattr(getattr(conn, "dialect", None), "name", None) == "postgresql":
                driver = conn.connection.driver_connection
            if getattr(getattr(conn, "dialect", None), "name", None) == "postgresql":
                conn.execute(text(f"SET LOCAL statement_timeout = {STATEMENT_TIMEOUT_MS}"))
                conn.execute(text(f"SET LOCAL lock_timeout = {LOCK_TIMEOUT_MS}"))
                conn.execute(text("SET LOCAL idle_in_transaction_session_timeout = 5000"))
            try:
                phase = "body"
                yield conn, check
                phase = "precommit"
                if getattr(getattr(conn, "dialect", None), "name", None) == "postgresql":
                    conn.execute(text(f"SET LOCAL statement_timeout = {COMMIT_TIMEOUT_MS}"))
                # No SQL round trip may separate the final guard/budget check
                # from COMMIT: even SET LOCAL can outlast the open window.
                check()
                phase = "commit"
            except BaseException as exc:
                body_error = exc
                raise
    except BaseException as exc:
        if phase == "commit":
            if _known_commit_rejection(exc, conn, driver):
                exc.qq_write_rolled_back = True
                exc.qq_failure_phase = "commit_rejected"
                raise
            raise CommitUncertain("QuiverQuant COMMIT resolution is uncertain; no replay") from exc
        if body_error is not None and exc is not body_error:
            raise CommitUncertain("QuiverQuant transaction resolution is uncertain; no replay") from exc
        exc.qq_failure_phase = phase
        exc.qq_write_rolled_back = phase == "body" and exc is body_error
        raise
