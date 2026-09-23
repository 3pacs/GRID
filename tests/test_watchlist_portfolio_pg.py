"""Portfolio GET contract against an isolated disposable PostgreSQL schema."""

from __future__ import annotations

import os
import uuid
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, text

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")
os.environ.setdefault("GRID_JWT_EXPIRE_HOURS", "1")
os.environ.setdefault("GRID_MASTER_PASSWORD_HASH", "$2b$12$4R0kBT.BssS83RVuCrfYceuiDxodYnH.2qOPgOOmdbo44tgOybgE2")

from api.auth import create_token  # noqa: E402
from api.main import app  # noqa: E402
from api.routers import watchlist_helpers  # noqa: E402


@pytest.fixture
def isolated_watchlist_db():
    if "GRID_TEST_DB_URL" not in os.environ:
        pytest.skip("GRID_TEST_DB_URL is required for isolated PostgreSQL proof")
    admin_engine = create_engine(os.environ["GRID_TEST_DB_URL"])
    schema = "portfolio_test_" + uuid.uuid4().hex
    with admin_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(
        os.environ["GRID_TEST_DB_URL"],
        connect_args={"options": f"-csearch_path={schema}"},
    )
    try:
        yield engine
    finally:
        engine.dispose()
        with admin_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin_engine.dispose()


def _get_portfolio():
    headers = {"Authorization": f"Bearer {create_token(expires_hours=1)}"}
    return TestClient(app).get("/api/v1/watchlist/portfolio", headers=headers)


def _record_statements(engine):
    statements = []

    def record(_conn, _cursor, statement, _parameters, _context, _executemany):
        statements.append(statement.strip())

    event.listen(engine, "before_cursor_execute", record)
    return statements, record


def test_populated_portfolio_reads_actual_positions_and_cached_quotes(isolated_watchlist_db):
    engine = isolated_watchlist_db
    with patch("api.routers.watchlist_helpers.get_db_engine", return_value=engine):
        watchlist_helpers._ensure_watchlist_table()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO watchlist (ticker, display_name, asset_type, weight)
            VALUES ('AAA', 'Alpha', 'stock', 1), ('BBB', 'Beta', 'etf', 3)
        """))
        conn.execute(text("""
            CREATE TABLE options_recommendations (
                outcome TEXT, expiry DATE, actual_return NUMERIC
            )
        """))
        conn.execute(text("""
            INSERT INTO options_recommendations (outcome, expiry, actual_return)
            VALUES ('WIN', CURRENT_DATE, 12.5)
        """))

    statements, recorder = _record_statements(engine)
    prices = {
        "AAA": {"price": 10.0, "pct_1d": 0.01, "pct_1w": 0.02},
        "BBB": {"price": 20.0, "pct_1d": -0.02, "pct_1w": -0.01},
    }
    try:
        with patch("api.routers.watchlist_core.get_db_engine", return_value=engine), \
             patch("api.routers.watchlist_core._init_table") as init, \
             patch("api.routers.watchlist_core._get_cached_prices", return_value=prices), \
             patch("api.routers.watchlist_core._batch_fetch_prices") as fetch:
            response = _get_portfolio()
        assert response.status_code == 200, response.text
        payload = response.json()
        assert payload["total_value"] == 125000
        assert [(p["ticker"], p["weight"], p["price"], p["pnl_1d"])
                for p in payload["positions"]] == [
                    ("AAA", 0.25, 10.0, 312.5),
                    ("BBB", 0.75, 20.0, -1875.0),
                ]
        assert payload["total_pnl_1d"] == -1562.5
        assert payload["allocation"]["by_asset_type"] == {"stock": 0.25, "etf": 0.75}
        assert payload["options_pnl"]["wins"] == 1
        assert payload["options_pnl"]["total_return"] == 12.5
        init.assert_not_called()
        fetch.assert_not_called()
        assert len(statements) == 2
        assert all(s.upper().startswith("SELECT") for s in statements)
    finally:
        event.remove(engine, "before_cursor_execute", recorder)


def test_empty_table_is_zero_and_missing_table_is_unavailable(isolated_watchlist_db):
    engine = isolated_watchlist_db
    with patch("api.routers.watchlist_core.get_db_engine", return_value=engine), \
         patch("api.routers.watchlist_core._init_table") as init, \
         patch("api.routers.watchlist_core._batch_fetch_prices") as fetch:
        statements, recorder = _record_statements(engine)
        try:
            missing = _get_portfolio()
            assert missing.status_code == 503
            assert missing.json() == {"detail": "Portfolio watchlist data is unavailable"}
            assert not statements[0].upper().startswith(("CREATE", "ALTER", "INSERT"))
        finally:
            event.remove(engine, "before_cursor_execute", recorder)

        # A pre-weight schema is unavailable to the reader until the existing
        # initialization path upgrades it; GET itself must not perform ALTER.
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE watchlist (
                    id SERIAL PRIMARY KEY, ticker TEXT NOT NULL UNIQUE,
                    display_name TEXT, asset_type TEXT NOT NULL DEFAULT 'stock',
                    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), notes TEXT
                )
            """))
        legacy = _get_portfolio()
        assert legacy.status_code == 503

        with patch("api.routers.watchlist_helpers.get_db_engine", return_value=engine):
            watchlist_helpers._ensure_watchlist_table()
        statements, recorder = _record_statements(engine)
        try:
            empty = _get_portfolio()
            assert empty.status_code == 200
            assert empty.json()["positions"] == []
            assert empty.json()["total_value"] == 0
            assert len(statements) == 1
            assert statements[0].upper().startswith("SELECT")
        finally:
            event.remove(engine, "before_cursor_execute", recorder)
        init.assert_not_called()
        fetch.assert_not_called()


def test_cache_miss_keeps_quote_fetch_and_process_local_cache(isolated_watchlist_db):
    engine = isolated_watchlist_db
    with patch("api.routers.watchlist_helpers.get_db_engine", return_value=engine):
        watchlist_helpers._ensure_watchlist_table()
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO watchlist (ticker) VALUES ('AAA')"))
    prices = {"AAA": {"price": 42.0, "pct_1d": 0.01}}
    with patch("api.routers.watchlist_core.get_db_engine", return_value=engine), \
         patch("api.routers.watchlist_core._get_cached_prices", return_value=None), \
         patch("api.routers.watchlist_core._batch_fetch_prices", return_value=prices) as fetch, \
         patch("api.routers.watchlist_core._wh._price_cache.set") as cache_set:
        response = _get_portfolio()
    assert response.status_code == 200, response.text
    assert response.json()["positions"][0]["price"] == 42.0
    fetch.assert_called_once_with(["AAA"])
    cache_set.assert_called_once_with("prices", prices)
