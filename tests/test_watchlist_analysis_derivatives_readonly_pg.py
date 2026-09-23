"""Captured Watchlist analysis and derivatives GETs on disposable PostgreSQL."""

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
from api.routers import derivatives, watchlist_analysis


@pytest.mark.xdist_group("postgres")
def test_analysis_and_flow_keep_measured_data_without_get_writes():
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        pytest.skip("GRID_TEST_DB_URL is required for disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("Watchlist proof requires a local disposable test database")

    schema = "analysis_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    today = date.today()
    prior = today - timedelta(days=1)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE feature_registry (id INTEGER PRIMARY KEY, name TEXT, family TEXT)"))
        conn.execute(text("CREATE TABLE resolved_series (feature_id INTEGER, obs_date DATE, value DOUBLE PRECISION)"))
        conn.execute(text("""CREATE TABLE options_daily_signals (
            ticker TEXT, signal_date DATE, put_call_ratio DOUBLE PRECISION,
            max_pain DOUBLE PRECISION, iv_skew DOUBLE PRECISION,
            total_oi DOUBLE PRECISION, total_volume DOUBLE PRECISION,
            spot_price DOUBLE PRECISION, iv_atm DOUBLE PRECISION,
            iv_25d_put DOUBLE PRECISION, iv_25d_call DOUBLE PRECISION,
            term_structure_slope DOUBLE PRECISION, oi_concentration DOUBLE PRECISION)"""))
        conn.execute(text("""CREATE TABLE decision_journal (
            inferred_state TEXT, state_confidence DOUBLE PRECISION,
            grid_recommendation TEXT, decision_timestamp TIMESTAMPTZ)"""))
        conn.execute(text("CREATE TABLE source_catalog (id INTEGER, name TEXT)"))
        conn.execute(text("""CREATE TABLE raw_series (
            source_id INTEGER, series_id TEXT, pull_timestamp TIMESTAMPTZ,
            value DOUBLE PRECISION, raw_payload JSONB)"""))
        conn.execute(text("INSERT INTO feature_registry VALUES (1, 'aapl_close', 'price')"))
        conn.execute(text("INSERT INTO resolved_series VALUES (1, :d, 100), (1, :p, 90)"), {"d": today, "p": prior})
        conn.execute(text("""INSERT INTO options_daily_signals
            (ticker, signal_date, put_call_ratio, max_pain, iv_atm, spot_price)
            VALUES ('AAPL', :d, 0.5, 105, 0.2, 100),
                   ('AAPL', :p, 0.6, 104, 0.21, 90)"""), {"d": today, "p": prior})
        conn.execute(text("INSERT INTO decision_journal VALUES ('GROWTH', 0.8, 'risk-on', NOW())"))

    statements = []
    event.listen(engine, "before_cursor_execute",
                 lambda _c, _cur, sql, _p, _ctx, _many: statements.append(sql.strip().upper()))
    app = FastAPI()
    app.include_router(watchlist_analysis.router, prefix="/api/v1/watchlist")
    app.include_router(derivatives.router)
    app.dependency_overrides[require_auth] = lambda: "test"
    no_history = MagicMock(empty=True)
    fake_yf = MagicMock()
    fake_yf.Ticker.return_value.history.return_value = no_history

    class FakeGex:
        def compute_gex_profile(self, _ticker, snap_date=None):
            if snap_date == prior:
                return {"error": "No options data", "ticker": "AAPL"}
            return {"gex_aggregate": 42, "regime": "LONG_GAMMA", "spot": 100,
                    "snap_date": str(snap_date or today)}

    try:
        with patch.object(watchlist_analysis, "get_db_engine", return_value=engine), \
             patch.object(watchlist_analysis, "_get_analysis_cached", return_value=None), \
             patch.object(watchlist_analysis, "_set_analysis_cache") as cache, \
             patch.object(watchlist_analysis, "_fetch_live_price", return_value={"price": 123.0}), \
             patch("analysis.market_universe.search_company", return_value=[]), \
             patch("yfinance.Ticker", fake_yf.Ticker), \
             patch.object(derivatives, "get_db_engine", return_value=engine), \
             patch.object(derivatives, "_get_gex_engine", return_value=FakeGex()) as gex_factory, \
             patch.object(derivatives, "_generate_catalysts", return_value=[]):
            with TestClient(app) as client:
                stored = client.get("/api/v1/watchlist/AAPL/analysis")
                assert stored.status_code == 200
                body = stored.json()
                assert body["watchlist_saved"] is False
                assert body["availability"]["watchlist"] == "unavailable"
                assert body["price_source"] == "grid"
                assert body["price_history"][-1]["value"] == 100
                assert body["options"][0]["put_call_ratio"] == 0.5
                assert body["availability"]["options"] == "available"
                cache.assert_not_called()  # partial missing-watchlist result is not frozen

                flow = client.get("/api/v1/derivatives/flow-timeline/AAPL?days=7")
                assert flow.status_code == 200
                assert flow.json()["history_status"] == "partial"
                assert flow.json()["failed_dates"] == 1
                assert [row["net_gex"] for row in flow.json()["history"]] == [42]

                # A missing price table aborts that transaction; later options,
                # regime and TradingView reads must still succeed independently.
                with engine.begin() as conn:
                    conn.execute(text("DROP TABLE resolved_series"))
                statements.clear()
                live = client.get("/api/v1/watchlist/AAPL/analysis")
                assert live.status_code == 200
                body = live.json()
                assert body["price_source"] == "live"
                assert body["live_price"]["price"] == 123.0
                assert body["availability"]["related_features"] == "unavailable"
                assert body["options"][0]["put_call_ratio"] == 0.5
                assert body["availability"]["options"] == "available"
                assert body["regime"]["state"] == "GROWTH"
                assert body["availability"]["tradingview_signals"] == "available"
                assert not any(sql.startswith(("CREATE", "ALTER", "INSERT", "UPDATE", "DELETE"))
                               for sql in statements)
                with engine.connect() as conn:
                    assert conn.execute(text("SELECT to_regclass('watchlist')")).scalar_one() is None

                with engine.begin() as conn:
                    conn.execute(text("DROP TABLE options_daily_signals"))
                statements.clear()
                unavailable = client.get("/api/v1/derivatives/flow-timeline/AAPL?days=7")
                assert unavailable.status_code == 200
                assert unavailable.json()["history_status"] == "fallback"
                assert unavailable.json()["history"][0]["net_gex"] == 42
                assert not any(sql.startswith(("CREATE", "ALTER", "INSERT", "UPDATE", "DELETE"))
                               for sql in statements)

                failed_gex = MagicMock()
                failed_gex.compute_gex_profile.return_value = {"error": "No options data", "ticker": "AAPL"}
                gex_factory.return_value = failed_gex
                no_gex = client.get("/api/v1/derivatives/flow-timeline/AAPL?days=7")
                assert no_gex.status_code == 200
                assert no_gex.json()["history_status"] == "unavailable"
                assert no_gex.json()["history"] == []
                assert no_gex.json()["error"] == "No usable GEX history is available"
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
        admin.dispose()
