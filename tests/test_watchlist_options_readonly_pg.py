"""Disposable PostgreSQL contract for the Watchlist recommendations GET."""

from __future__ import annotations

import os
import uuid
from unittest.mock import patch
from urllib.parse import urlparse

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, text

os.environ.setdefault("DB_PASSWORD", "test-password")

from api.auth import require_auth  # noqa: E402
from api.routers.options import router  # noqa: E402


@pytest.fixture
def isolated_db():
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        pytest.skip("GRID_TEST_DB_URL is required for disposable PostgreSQL proof")
    parsed = urlparse(url)
    assert parsed.hostname in {"localhost", "127.0.0.1"}
    assert "test" in parsed.path.lower()
    admin = create_engine(url)
    schema = "options_get_test_" + uuid.uuid4().hex
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


def _client():
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[require_auth] = lambda: "test-token"
    return TestClient(app)


def _capture(engine):
    statements = []

    def record(_conn, _cursor, statement, _parameters, _context, _executemany):
        statements.append(statement.strip().upper())

    event.listen(engine, "before_cursor_execute", record)
    return statements, record


def test_saved_populated_and_checked_empty_gets_are_select_only(isolated_db):
    engine = isolated_db
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE options_recommendations (
                id SERIAL PRIMARY KEY,
                ticker TEXT, direction TEXT, strike NUMERIC, expiry DATE,
                entry_price NUMERIC, target_price NUMERIC, stop_loss NUMERIC,
                expected_return NUMERIC, kelly_fraction NUMERIC, confidence NUMERIC,
                thesis TEXT, sanity_status JSONB, dealer_context TEXT,
                generated_at TIMESTAMPTZ, outcome TEXT
            )
        """))
        conn.execute(text("""
            INSERT INTO options_recommendations
                (ticker, direction, strike, expiry, entry_price, target_price,
                 stop_loss, expected_return, kelly_fraction, confidence, thesis,
                 generated_at, outcome)
            VALUES
                ('AAPL', 'CALL', 200, '2026-10-16', 10, 12, 8, 0, 0, 0,
                 'Persisted recommendation', NOW(), 'OPEN'),
                ('MSFT', 'PUT', 400, '2026-10-16', 9, 11, 7, 0.2, 0.1, 0.8,
                 'Other ticker', NOW(), 'OPEN'),
                ('AAPL', 'PUT', 180, '2026-10-16', 4, 6, 2, 0.1, 0.1, 0.8,
                 'Closed recommendation', NOW(), 'CLOSED')
        """))
    statements, record = _capture(engine)
    try:
        with patch("api.routers.options.get_db_engine", return_value=engine):
            client = _client()
            populated = client.get("/api/v1/options/recommendations?ticker=AAPL")
            empty = client.get("/api/v1/options/recommendations?ticker=XYZ")
        assert populated.status_code == empty.status_code == 200
        body = populated.json()
        assert len(body["recommendations"]) == 1
        assert body["recommendations"][0]["ticker"] == "AAPL"
        assert body["recommendations"][0]["confidence"] == 0.0
        assert body["recommendations"][0]["expected_return"] == 0.0
        assert body["scan_summary"]["source"] == "persisted"
        assert body["scan_summary"]["fresh_scan"] is False
        assert empty.json()["recommendations"] == []
        assert empty.json()["scan_summary"]["source"] == "persisted"
        assert statements and all(statement.startswith("SELECT") for statement in statements)
    finally:
        event.remove(engine, "before_cursor_execute", record)


def test_missing_table_reports_unavailable_without_bootstrap(isolated_db):
    engine = isolated_db
    statements, record = _capture(engine)
    try:
        with patch("api.routers.options.get_db_engine", return_value=engine):
            response = _client().get("/api/v1/options/recommendations?ticker=AAPL")
        assert response.status_code == 200
        assert response.json()["recommendations"] == []
        assert response.json()["scan_summary"]["source"] == "unavailable"
        assert statements and all(statement.startswith("SELECT") for statement in statements)
    finally:
        event.remove(engine, "before_cursor_execute", record)
