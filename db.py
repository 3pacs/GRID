"""
GRID database access layer.

Provides SQLAlchemy engine creation, raw psycopg2 connection management,
schema application, health checks, and simple query execution utilities.
All connection events are logged via loguru.
"""

from __future__ import annotations

import contextlib
import os
import sys
import time
from pathlib import Path
from typing import Any, Generator

import psycopg2
import psycopg2.extras
from loguru import logger as log
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine

from config import settings


# Permanent-looking psycopg2 OperationalError substrings that indicate the
# DB server is out of connection slots. These are transient (retry-able with
# short backoff) rather than permanent.
_CONN_SLOT_EXHAUSTION_MARKERS = (
    "remaining connection slots are reserved",
    "too many clients already",
    "sorry, too many clients",
)


def _is_slot_exhaustion_error(exc: BaseException) -> bool:
    """True iff exc looks like a postgres connection-slot exhaustion."""
    msg = str(exc).lower()
    return any(m in msg for m in _CONN_SLOT_EXHAUSTION_MARKERS)


# ---------------------------------------------------------------------------
# SQLAlchemy Engine (singleton)
# ---------------------------------------------------------------------------
_engine: Engine | None = None


def get_engine() -> Engine:
    """Return the SQLAlchemy engine, creating it on first call.

    The engine is configured with a connection pool of 5 connections,
    up to 10 overflow connections, and a 30-second timeout.

    Returns:
        sqlalchemy.engine.Engine: Configured engine instance.
    """
    global _engine
    if _engine is None:
        # Default budget lowered from 50+100=150 → 20+30=50 on 2026-04-19:
        # postgres's default max_connections is 100 with ~3 slots reserved
        # for superuser. Leaving 150 in the SQLAlchemy pool alone could
        # (and did, as of today) exhaust slots shared with raw psycopg2
        # callers (candle flusher, ws_listener, events/producer, …).
        # Override via GRID_DB_POOL_SIZE / GRID_DB_MAX_OVERFLOW if postgres
        # is sized larger.
        pool_size = int(os.getenv("GRID_DB_POOL_SIZE", os.getenv("DB_POOL_SIZE", "20")))
        max_overflow = int(os.getenv("GRID_DB_MAX_OVERFLOW", os.getenv("DB_MAX_OVERFLOW", "30")))
        log.info("Creating SQLAlchemy engine — {url}", url=settings.DB_URL.replace(settings.DB_PASSWORD, "***"))
        # Default per-statement timeout (milliseconds). Any single SQL
        # statement that runs longer than this is killed by postgres
        # before it can exhaust the connection pool. Override per-call
        # with `SET LOCAL statement_timeout = 0` for jobs that legitimately
        # need longer (long backfills, bulk resolves, etc.).
        #
        # Why 120s: the actual runaway scan that took down the lever page
        # + NVDA chart + canvas on 2026-04-15 was 215s long, so 120s still
        # catches it. 120s also leaves a big safety margin for any code
        # path that interleaves an LLM call between SQL statements — the
        # timeout is per-statement, not per-connection, so an LLM call
        # BETWEEN two fast queries is unaffected (each execute() resets
        # the clock). We grepped the codebase for stream_results /
        # server_side_cursor / yield_per and found nothing in the request
        # path, so every SELECT fetches eagerly and the statement
        # completes before the LLM call starts. 120s is conservative
        # cushion for any pattern we missed.
        statement_timeout_ms = int(
            os.getenv("GRID_DB_STATEMENT_TIMEOUT_MS", "120000")  # 120s
        )
        _engine = create_engine(
            settings.DB_URL,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=30,
            pool_pre_ping=True,
            pool_recycle=3600,  # Invalidate stale connections after 1 hour
            connect_args={
                "options": f"-c statement_timeout={statement_timeout_ms}",
            },
        )

        # Pool utilization monitoring: warn when >80% of capacity is checked out.
        _warn_threshold = int(pool_size * 0.8)

        @event.listens_for(_engine, "checkout")
        def _on_checkout(dbapi_conn, connection_rec, connection_proxy):  # noqa: ARG001
            checked_out = _engine.pool.checkedout()  # type: ignore[union-attr]
            capacity = pool_size + max_overflow
            if checked_out > _warn_threshold:
                log.warning(
                    "DB pool utilization high — {co}/{cap} connections checked out ({pct:.0f}%)",
                    co=checked_out,
                    cap=capacity,
                    pct=checked_out / capacity * 100,
                )

        log.info(
            "SQLAlchemy engine created — pool_size={ps}, max_overflow={mo}",
            ps=pool_size, mo=max_overflow,
        )
    return _engine


def _connect_with_retry(max_attempts: int = 5) -> psycopg2.extensions.connection:
    """Open a raw psycopg2 connection with retry-on-slot-exhaustion.

    Postgres returns a transient FATAL when `max_connections` is hit; the
    slot usually frees within a few hundred ms. Bare `psycopg2.connect()`
    raises OperationalError immediately, which in our case crashes
    long-lived async workers (candle flusher, ws listeners). Retry with
    bounded exponential backoff up to max_attempts.
    """
    last_exc: BaseException | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return psycopg2.connect(
                host=settings.DB_HOST,
                port=settings.DB_PORT,
                dbname=settings.DB_NAME,
                user=settings.DB_USER,
                password=settings.DB_PASSWORD,
            )
        except psycopg2.OperationalError as exc:
            last_exc = exc
            if not _is_slot_exhaustion_error(exc) or attempt == max_attempts:
                raise
            # 0.5s, 1s, 2s, 4s — capped at 4s per sleep
            delay = min(0.5 * (2 ** (attempt - 1)), 4.0)
            log.warning(
                "DB slot-exhaustion on connect (attempt {a}/{m}); "
                "retrying in {d:.1f}s",
                a=attempt, m=max_attempts, d=delay,
            )
            time.sleep(delay)
    # Unreachable: loop either returns or raises, but keep mypy happy.
    assert last_exc is not None
    raise last_exc


@contextlib.contextmanager
def get_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """Yield a raw psycopg2 connection as a context manager.

    The connection is committed on clean exit and rolled back on exception.
    Always closed when the context exits.

    Yields:
        psycopg2.extensions.connection: Active database connection.

    Raises:
        psycopg2.OperationalError: If the database is unreachable after
            retries (including slot-exhaustion retries).
    """
    conn = None
    try:
        log.debug("Opening raw psycopg2 connection")
        conn = _connect_with_retry()
        yield conn
        conn.commit()
        log.debug("Connection committed")
    except Exception:
        if conn is not None:
            conn.rollback()
            log.warning("Connection rolled back due to exception")
        raise
    finally:
        if conn is not None:
            conn.close()
            log.debug("Connection closed")


def execute_sql(sql: str, params: tuple | dict | None = None) -> list[dict[str, Any]]:
    """Execute a SQL statement and return results as a list of dicts.

    Parameters:
        sql: SQL query string. May use %s or %(name)s placeholders.
        params: Optional parameters for the query.

    Returns:
        list[dict]: Rows as dictionaries. Empty list for non-SELECT queries.
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            log.debug("Executing SQL: {sql}", sql=sql[:120])
            cur.execute(sql, params)
            if cur.description is not None:
                rows = [dict(row) for row in cur.fetchall()]
                log.debug("Query returned {n} rows", n=len(rows))
                return rows
            return []


def apply_schema(schema_path: str | None = None) -> None:
    """Read and execute schema.sql against the database.

    Parameters:
        schema_path: Path to the SQL schema file.  Defaults to
                     ``schema.sql`` in the same directory as this module.

    Raises:
        FileNotFoundError: If the schema file does not exist.
        psycopg2.Error: If the SQL is invalid.
    """
    if schema_path is None:
        schema_path = str(Path(__file__).parent / "schema.sql")

    path = Path(schema_path)
    if not path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    sql = path.read_text(encoding="utf-8")
    log.info("Applying schema from {path} ({size} bytes)", path=schema_path, size=len(sql))

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    log.info("Schema applied successfully")


def health_check() -> bool:
    """Check whether the database is reachable.

    Returns:
        bool: True if a simple query succeeds, False otherwise.
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("Database health check passed")
        return True
    except Exception as exc:
        log.error("Database health check failed: {err}", err=str(exc))
        return False


# ---------------------------------------------------------------------------
# CLI entry point: apply schema when run directly
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    log.info("Running db.py — applying schema")
    apply_schema()
    if health_check():
        log.info("Database is ready")
    else:
        log.error("Database health check failed after schema application")
        sys.exit(1)
