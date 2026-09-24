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
                signal_date DATE NOT NULL,
                signal_value JSONB,
                outcome TEXT,
                trust_score DOUBLE PRECISION DEFAULT 0.5
            )
        """))
        conn.execute(text("""
            INSERT INTO signal_sources
                (source_type, source_id, ticker, signal_type, signal_date, signal_value, outcome, trust_score)
            VALUES (:source_type, :source_id, 'TEST', 'BUY', CURRENT_DATE, CAST(:signal_value AS JSONB), :outcome, :trust_score)
        """), [
            {"source_type": "congressional", "source_id": "Member A", "signal_value": '{"amount":"$1000"}', "outcome": "PENDING", "trust_score": 0.0},
            {"source_type": "insider", "source_id": "Officer B", "signal_value": '{"title":"CEO"}', "outcome": "CORRECT", "trust_score": 0.8},
            {"source_type": "darkpool", "source_id": "Pool C", "signal_value": '{"volume_vs_avg":2.1}', "outcome": "PENDING", "trust_score": 0.6},
        ])
        conn.execute(text("""
            INSERT INTO signal_sources
                (source_type, source_id, ticker, signal_type, signal_date,
                 signal_value, outcome, trust_score)
            VALUES
                ('options_flow', 'whale_TEST_450.0', 'TEST', 'CALL', CURRENT_DATE, NULL, 'WRONG', NULL),
                ('quiverquant:house', 'quiverquant_house_feed', 'TEST', 'BUY', CURRENT_DATE,
                 CAST('{"Representative":"Rep C"}' AS JSONB), 'WRONG', NULL),
                ('other', 'Office of Analyst D', 'TEST', 'BUY', CURRENT_DATE, NULL, 'WRONG', NULL),
                ('social', 'User E', 'TEST', 'BUY', CURRENT_DATE,
                 CAST('{"platform":"forum"}' AS JSONB), 'WRONG', NULL)
        """))
        conn.execute(text("""
            INSERT INTO signal_sources
                (source_type, source_id, ticker, signal_type, signal_date, signal_value)
            VALUES ('social', 'Scalar E', 'SCALAR', 'BUY', CURRENT_DATE,
                    CAST('["not an object"]' AS JSONB))
        """))
        conn.execute(text("""
            CREATE TABLE lever_pullers (
                id SERIAL PRIMARY KEY, source_type TEXT NOT NULL, source_id TEXT NOT NULL,
                name TEXT NOT NULL, category TEXT NOT NULL, motivation_model TEXT
            )
        """))
        conn.execute(text("""
            INSERT INTO lever_pullers (source_type, source_id, name, category, motivation_model)
            VALUES
                ('congressional', 'Member A', 'Member A', 'politician', 'committee overlap'),
                ('options_flow', 'whale_TEST', 'Options tape', 'options', 'flow concentration'),
                ('quiverquant:house', 'Rep C', 'Rep C', 'politician', 'filing context')
        """))
        conn.execute(text("""
            CREATE TABLE actors (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, tier TEXT NOT NULL,
                category TEXT NOT NULL, title TEXT,
                motivation_model TEXT, known_positions JSONB NOT NULL DEFAULT '[]'
            )
        """))
        conn.execute(text("""
            INSERT INTO actors (id, name, tier, category, title, motivation_model, known_positions)
            VALUES
                ('officer-b', 'Officer B', 'individual', 'insider', 'CEO', 'issuer exposure', '[]'),
                ('analyst-d', 'Analyst D', 'individual', 'analyst', 'Analyst', 'issuer research', '[]')
        """))


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
        assert payload["convergence"]["direction"] == "bullish"
        assert payload["convergence"]["direction_basis"] == "inferred_from_signal_types"
        assert payload["convergence"]["source_count"] == 3
        assert payload["convergence"]["non_null_trust_score_count"] == 3
        assert payload["convergence"]["confidence"] is None
        assert payload["convergence"]["confidence_basis"] == "unverified_score_provenance"
        assert payload["convergence"]["persisted_trust_mean"] == 0.47
        assert payload["convergence"]["persisted_trust_basis"] == "mean_non_null_persisted_trust_scores"
        assert payload["congressional"][0]["trust_score"] == 0.0
        assert payload["congressional"][0]["amount"] == "$1000"
        assert payload["insider"][0]["title"] == "CEO"
        assert payload["dark_pool"]["volume_vs_avg"] == 2.1
        assert payload["smart_money"][0]["source"] == "forum"
        assert payload["smart_money"][0]["trust_score"] is None
        assert payload["lever_pullers"] == [
            {"name": "Member A", "action": "BUY", "context": "committee overlap"},
            {"name": "Options tape", "action": "CALL", "context": "flow concentration"},
            {"name": "Rep C", "action": "BUY", "context": "filing context"},
            {"name": "Analyst D", "action": "WATCHING", "context": "Analyst — issuer research"},
            {"name": "Officer B", "action": "WATCHING", "context": "CEO — issuer exposure"},
        ]
        assert payload["availability"]["lever_pullers"]["status"] == "available"
        assert payload["availability"]["actor_context"]["status"] == "available"
        absent = get_ticker_edge("absent", user={}, engine=engine)
        assert absent["availability"]["signal_sources"]["status"] == "available"
        assert absent["congressional"] == []
        assert absent["insider"] == []
        scalar = get_ticker_edge("scalar", user={}, engine=engine)
        assert scalar["availability"]["signal_sources"]["status"] == "available"
        assert scalar["smart_money"][0]["source"] == "unknown"
        assert payload["availability"]["investigation_leads"] == {
            "status": "unsupported", "reason": "no_ticker_association",
        }

        # Schema/writer defaults can persist 0.5 without a scorer. A GET may
        # expose that stored mean, but must not certify it as confidence.
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO signal_sources
                    (source_type, source_id, ticker, signal_type, signal_date, outcome)
                VALUES
                    ('congressional', 'Default A', 'DEFAULT', 'BUY', NOW(), 'PENDING'),
                    ('insider', 'Default B', 'DEFAULT', 'BUY', NOW(), 'PENDING'),
                    ('darkpool', 'Default C', 'DEFAULT', 'BUY', NOW(), 'PENDING')
            """))
        statements.clear()
        defaulted = get_ticker_edge("default", user={}, engine=engine)["convergence"]
        assert defaulted["source_count"] == 3
        assert defaulted["non_null_trust_score_count"] == 3
        assert defaulted["persisted_trust_mean"] == 0.5
        assert defaulted["confidence"] is None
        assert defaulted["confidence_basis"] == "unverified_score_provenance"
        assert statements and all(statement.startswith("SELECT") for statement in statements)

        # Disposable setup changes the persisted measurements between GETs.
        # The route itself must continue to issue SELECTs only.
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE signal_sources SET trust_score = NULL
                WHERE ticker = 'TEST' AND source_type = 'darkpool'
            """))
        statements.clear()
        partially_scored = get_ticker_edge("test", user={}, engine=engine)["convergence"]
        assert partially_scored["source_count"] == 3
        assert partially_scored["non_null_trust_score_count"] == 2
        assert partially_scored["confidence"] is None
        assert partially_scored["persisted_trust_mean"] == 0.4
        assert statements and all(statement.startswith("SELECT") for statement in statements)

        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE signal_sources SET trust_score = NULL
                WHERE ticker = 'TEST' AND source_type = 'insider'
            """))
        statements.clear()
        mixed = get_ticker_edge("test", user={}, engine=engine)["convergence"]
        assert mixed["source_count"] == 3
        assert mixed["non_null_trust_score_count"] == 1
        assert mixed["confidence"] is None
        assert mixed["persisted_trust_mean"] == 0.0
        assert mixed["persisted_trust_basis"] == "mean_non_null_persisted_trust_scores"
        assert statements and all(statement.startswith("SELECT") for statement in statements)

        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE signal_sources SET trust_score = NULL
                WHERE ticker = 'TEST' AND source_type = 'congressional'
            """))
        statements.clear()
        unscored = get_ticker_edge("test", user={}, engine=engine)["convergence"]
        assert unscored["status"] == "detected"
        assert unscored["source_count"] == 3
        assert unscored["non_null_trust_score_count"] == 0
        assert unscored["confidence"] is None
        assert unscored["persisted_trust_mean"] is None
        assert unscored["persisted_trust_basis"] == "unscored"
        assert statements and all(statement.startswith("SELECT") for statement in statements)

        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE signal_sources SET trust_score = 0.0, signal_type = 'SELL'
                WHERE ticker = 'TEST' AND source_type IN ('congressional', 'insider', 'darkpool')
            """))
        statements.clear()
        bearish_zero = get_ticker_edge("test", user={}, engine=engine)["convergence"]
        assert bearish_zero["signal_type"] == "SELL"
        assert bearish_zero["direction"] == "bearish"
        assert bearish_zero["source_count"] == 3
        assert bearish_zero["non_null_trust_score_count"] == 3
        assert bearish_zero["confidence"] is None
        assert bearish_zero["persisted_trust_mean"] == 0.0
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
