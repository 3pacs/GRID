"""Overview GET is useful and read-only on disposable PostgreSQL."""

import os
from datetime import date, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine.url import make_url

from api.auth import require_auth
from api.routers import watchlist_overview


@pytest.mark.xdist_group("postgres")
def test_overview_populated_then_failed_price_read_uses_live_and_keeps_options_without_writes():
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        pytest.skip("GRID_TEST_DB_URL is required for disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("overview proof requires a local disposable test database")

    schema = "overview_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE feature_registry (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"))
        conn.execute(text("""CREATE TABLE resolved_series (
            feature_id INTEGER NOT NULL, obs_date DATE NOT NULL,
            vintage_date DATE NOT NULL, value DOUBLE PRECISION)"""))
        conn.execute(text("""CREATE TABLE options_daily_signals (
            ticker TEXT, signal_date DATE, put_call_ratio DOUBLE PRECISION,
            max_pain DOUBLE PRECISION, iv_atm DOUBLE PRECISION,
            iv_skew DOUBLE PRECISION, spot_price DOUBLE PRECISION,
            total_oi DOUBLE PRECISION)"""))
        conn.execute(text("""CREATE TABLE decision_journal (
            inferred_state TEXT, state_confidence DOUBLE PRECISION,
            grid_recommendation TEXT, decision_timestamp TIMESTAMPTZ)"""))
        conn.execute(text("INSERT INTO feature_registry VALUES (1, 'aapl_close')"))
        conn.execute(text("INSERT INTO resolved_series VALUES (1, :today, :today, 100.0)"),
                     {"today": date.today()})
        conn.execute(text("INSERT INTO resolved_series VALUES (1, :prior, :prior, 90.0)"),
                     {"prior": date.today() - timedelta(days=1)})
        conn.execute(text("""INSERT INTO options_daily_signals
            (ticker, signal_date, put_call_ratio, max_pain, iv_atm, iv_skew, spot_price, total_oi)
            VALUES ('AAPL', :today, 0.5, 105, 0.2, 0.9, 100, 1000)"""), {"today": date.today()})
        conn.execute(text("""INSERT INTO decision_journal
            VALUES ('GROWTH', 0.8, 'risk-on', NOW())"""))

    statements = []

    def record_sql(_conn, _cursor, statement, _params, _context, _many):
        statements.append(statement.strip().upper())

    event.listen(engine, "before_cursor_execute", record_sql)
    app = FastAPI()
    app.include_router(watchlist_overview.router, prefix="/api/v1/watchlist")
    app.dependency_overrides[require_auth] = lambda: "test"
    unavailable_llm = MagicMock(is_available=False)
    try:
        with patch.object(watchlist_overview, "get_db_engine", return_value=engine), \
             patch.object(watchlist_overview, "_fetch_live_price", return_value={
                 "price": 123.0, "pct_1d": 0.02,
             }) as live, \
             patch("llm.router.get_llm", return_value=unavailable_llm), \
             patch("ollama.client.get_client", return_value=unavailable_llm):
            with TestClient(app) as client:
                populated = client.get("/api/v1/watchlist/AAPL/overview")
                assert populated.status_code == 200
                assert populated.json()["sentiment"] == "bullish"
                assert {item["label"]: item["value"] for item in populated.json()["key_levels"]}["Last"] == 100.0
                stored_quote = client.get("/api/v1/watchlist/AAPL/quote")
                assert stored_quote.status_code == 200
                assert stored_quote.json()["price"] == 100.0
                assert stored_quote.json()["source"] == "grid"
                assert stored_quote.json()["put_call_ratio"] == 0.5
                live.assert_not_called()
                assert not any(sql.startswith(("CREATE", "ALTER", "INSERT", "UPDATE", "DELETE"))
                               for sql in statements)

                # A missing price read aborts its transaction; overview must
                # recover it before reading independent options and regime.
                with engine.begin() as conn:
                    conn.execute(text("DROP TABLE resolved_series"))
                statements.clear()
                fallback = client.get("/api/v1/watchlist/AAPL/overview")
                assert fallback.status_code == 200
                assert {item["label"]: item["value"] for item in fallback.json()["key_levels"]}["Last"] == 123.0
                assert {item["label"]: item["value"] for item in fallback.json()["key_levels"]}["Max Pain"] == 105.0
                assert fallback.json()["sentiment"] == "bullish"
                assert any(section["title"] == "Options Flow" for section in fallback.json()["sections"])
                fallback_quote = client.get("/api/v1/watchlist/AAPL/quote")
                assert fallback_quote.status_code == 200
                assert fallback_quote.json()["price"] == 123.0
                assert fallback_quote.json()["source"] == "live"
                assert fallback_quote.json()["put_call_ratio"] == 0.5
                assert live.call_count == 2
                assert not any(sql.startswith(("CREATE", "ALTER", "INSERT", "UPDATE", "DELETE"))
                               for sql in statements)
                assert all("watchlist" not in sql.lower() for sql in statements)
                with engine.connect() as conn:
                    assert conn.execute(text("SELECT to_regclass('watchlist')")).scalar_one() is None
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
        admin.dispose()
