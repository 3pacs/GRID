"""Real PostgreSQL proof that the edge route does not mutate its read model."""

from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, event, text

os.environ.setdefault("DB_PASSWORD", "test-password")

from api.routers.watchlist_overview import get_ticker_edge


_DB_URL = os.environ.get("GRID_WATCHLIST_EDGE_TEST_DB_URL") or os.environ.get("DB_URL")
pytestmark = pytest.mark.skipif(not _DB_URL, reason="requires disposable PostgreSQL via GRID_WATCHLIST_EDGE_TEST_DB_URL or DB_URL")


def _schema_engine(schema: str):
    engine = create_engine(_DB_URL)

    @event.listens_for(engine, "connect")
    def _set_search_path(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute(f'SET search_path TO "{schema}"')
        cursor.close()

    return engine


def _create_signal_sources(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE signal_sources (
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                ticker TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                signal_date TIMESTAMPTZ NOT NULL,
                metadata JSONB,
                outcome TEXT,
                trust_score DOUBLE PRECISION
            )
        """))
        conn.execute(text("""
            INSERT INTO signal_sources
                (source_type, source_id, ticker, signal_type, signal_date, metadata, outcome, trust_score)
            VALUES (:source_type, :source_id, 'TEST', 'BUY', NOW(), CAST(:metadata AS JSONB), :outcome, :trust_score)
        """), [
            {"source_type": "congressional", "source_id": "Member A", "metadata": '{"amount":"$1000"}', "outcome": "PENDING", "trust_score": None},
            {"source_type": "insider", "source_id": "Officer B", "metadata": '{"title":"CEO"}', "outcome": "CORRECT", "trust_score": 0.8},
            {"source_type": "darkpool", "source_id": "Pool C", "metadata": '{"volume_vs_avg":2.1}', "outcome": "PENDING", "trust_score": 0.6},
        ])


def test_edge_route_uses_only_selects_against_prepared_postgres_schema():
    admin = create_engine(_DB_URL)
    schema = f"edge_readonly_{uuid.uuid4().hex}"
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = _schema_engine(schema)
    try:
        _create_signal_sources(engine)
        statements: list[str] = []

        @event.listens_for(engine, "before_cursor_execute")
        def _capture(_conn, _cursor, statement, _parameters, _context, _executemany):
            statements.append(statement.strip().upper())

        payload = get_ticker_edge("test", user={}, engine=engine)

        assert payload["status"] == "partial"
        assert payload["convergence"]["signal_type"] == "BUY"
        assert payload["convergence"]["source_count"] == 3
        assert statements and all(statement.startswith("SELECT") for statement in statements)
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        admin.dispose()


def test_edge_route_reports_missing_schema_without_ddl_or_dml():
    admin = create_engine(_DB_URL)
    schema = f"edge_missing_{uuid.uuid4().hex}"
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = _schema_engine(schema)
    try:
        statements: list[str] = []

        @event.listens_for(engine, "before_cursor_execute")
        def _capture(_conn, _cursor, statement, _parameters, _context, _executemany):
            statements.append(statement.strip().upper())

        payload = get_ticker_edge("test", user={}, engine=engine)

        assert payload["status"] == "unavailable"
        assert payload["reason"] == "signal_sources_unavailable"
        assert statements and all(statement.startswith("SELECT") for statement in statements)
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        admin.dispose()
