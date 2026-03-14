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
from pathlib import Path
from typing import Any, Generator

import psycopg2
import psycopg2.extras
from loguru import logger as log
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config import settings


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
        log.info("Creating SQLAlchemy engine — {url}", url=settings.DB_URL.replace(settings.DB_PASSWORD, "***"))
        _engine = create_engine(
            settings.DB_URL,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_pre_ping=True,
        )
        log.info("SQLAlchemy engine created successfully")
    return _engine


@contextlib.contextmanager
def get_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """Yield a raw psycopg2 connection as a context manager.

    The connection is committed on clean exit and rolled back on exception.
    Always closed when the context exits.

    Yields:
        psycopg2.extensions.connection: Active database connection.

    Raises:
        psycopg2.OperationalError: If the database is unreachable.
    """
    conn = None
    try:
        log.debug("Opening raw psycopg2 connection")
        conn = psycopg2.connect(
            host=settings.DB_HOST,
            port=settings.DB_PORT,
            dbname=settings.DB_NAME,
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
        )
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
