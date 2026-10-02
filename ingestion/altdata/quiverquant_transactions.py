"""Bounded QuiverQuant writes; a failed COMMIT is never replayed."""

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
    finished = False
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
            if getattr(getattr(conn, "dialect", None), "name", None) == "postgresql":
                conn.execute(text(f"SET LOCAL statement_timeout = {STATEMENT_TIMEOUT_MS}"))
                conn.execute(text(f"SET LOCAL lock_timeout = {LOCK_TIMEOUT_MS}"))
                conn.execute(text("SET LOCAL idle_in_transaction_session_timeout = 5000"))
            try:
                yield conn, check
                check()
                if getattr(getattr(conn, "dialect", None), "name", None) == "postgresql":
                    conn.execute(text(f"SET LOCAL statement_timeout = {COMMIT_TIMEOUT_MS}"))
                finished = True
            except BaseException as exc:
                body_error = exc
                raise
    except BaseException as exc:
        if finished or (body_error is not None and exc is not body_error):
            raise CommitUncertain("QuiverQuant transaction resolution is uncertain; no replay") from exc
        raise
