"""PWA ticker detail and stream GETs against a disposable PostgreSQL schema."""

import json
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine.url import make_url

import api.routers.dad as dad
from api.auth import require_auth, require_stream_auth


@pytest.mark.xdist_group("postgres")
def test_ticker_detail_stored_cold_error_and_stream_gets_have_no_writes():
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        pytest.skip("GRID_TEST_DB_URL is required for disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("ticker detail proof requires a local disposable test database")

    schema = "dad_detail_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    with engine.begin() as conn:
        conn.execute(text("""CREATE TABLE source_catalog (
            id INTEGER PRIMARY KEY, name TEXT UNIQUE, last_pull_at TIMESTAMPTZ)"""))
        conn.execute(text("""CREATE TABLE raw_series (
            series_id TEXT NOT NULL, source_id INTEGER NOT NULL,
            obs_date DATE NOT NULL, pull_timestamp TIMESTAMPTZ NOT NULL,
            value DOUBLE PRECISION, raw_payload JSONB, pull_status TEXT NOT NULL)"""))
        conn.execute(text("INSERT INTO source_catalog (id, name) VALUES (1, :name)"),
                     {"name": dad.FINVIZ_SOURCE_NAME})
        for ticker, age_days, price in (("FRESH", 0, 11.0), ("STALE", 200, 22.0), ("ERR", 200, 33.0)):
            pulled = datetime.now(timezone.utc) - timedelta(days=age_days)
            conn.execute(text("""INSERT INTO raw_series
                (series_id, source_id, obs_date, pull_timestamp, value, raw_payload, pull_status)
                VALUES (:series, 1, :obs, :pulled, :value, CAST(:payload AS JSONB), 'SUCCESS')"""), {
                "series": f"sec_filed_fundamentals.{ticker}.total_assets", "obs": pulled.date(), "pulled": datetime.now(timezone.utc),
                "value": price, "payload": json.dumps({
                    "ticker": ticker, "cik": "0000320193", "period_end": pulled.date().isoformat(),
                    "filed": pulled.date().isoformat(), "form": "10-K", "unit": "USD",
                    "accession": "0000320193-26-000001",
                    "source_url": "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json",
                }),
            })

    statements = []

    def record_sql(_conn, _cursor, statement, _params, _context, _many):
        statements.append(statement.strip().upper())

    event.listen(engine, "before_cursor_execute", record_sql)
    app = FastAPI()
    app.include_router(dad.router)
    app.dependency_overrides[require_auth] = lambda: "test"
    app.dependency_overrides[require_stream_auth] = lambda: "test"
    workbook = {
        "status": "ready", "summary": {"mentions": 1},
        "workbook": {"files": [], "sheets": [], "evidence": []},
        "source_lanes": [], "dad_stats": [], "fit_signals": [],
        "source": {"attached": True, "db_path": "synthetic"},
    }
    fetched = []

    def fetch(ticker):
        fetched.append(ticker)
        raise AssertionError("retired Finviz provider must never be called")

    dad._GOLD_MEMORY_CACHE.clear()
    dad._FINVIZ_MEMORY_CACHE.clear()
    try:
        with patch.object(dad, "get_db_engine", return_value=engine), \
             patch.object(dad, "_load_workbook_context", return_value=workbook), \
             patch.object(dad, "_fetch_finviz_snapshot", side_effect=fetch):
            with TestClient(app) as client:
                fresh = client.get("/api/v1/dad/ticker/FRESH/finviz?refresh_finviz=true")
                assert fresh.status_code == 200
                assert fresh.json()["finviz"]["fields"]["total_assets"]["parsed"] == 11.0
                assert fresh.json()["finviz"]["source"] == "SEC EDGAR/XBRL"
                assert fresh.json()["finviz"]["refresh_available"] is False

                for ticker in ("COLD", "ERRCOLD"):
                    cold = client.get(f"/api/v1/dad/ticker/{ticker}/finviz?refresh_finviz=true")
                    assert cold.status_code == 200
                    assert cold.json()["finviz"]["status"] == "unavailable"
                    assert cold.json()["finviz"]["fields"] == {}
                    assert cold.json()["finviz"]["rows_inserted"] == 0
                for ticker, value in (("STALE", 22.0), ("ERR", 33.0)):
                    stale = client.get(f"/api/v1/dad/ticker/{ticker}/finviz?refresh_finviz=true")
                    assert stale.status_code == 200
                    assert stale.json()["finviz"]["status"] == "stale"
                    assert stale.json()["finviz"]["fields"]["total_assets"]["parsed"] == value

                # The PWA's normal stream and its four direct fallback GETs all
                # traverse the same read-only contract, including refresh.
                stream = client.get("/api/v1/dad/ticker/COLD/gold/stream?refresh_finviz=true")
                assert stream.status_code == 200
                assert "event: finviz" in stream.text
                assert '"source": "SEC EDGAR/XBRL"' in stream.text
                for path in ("evidence", "chart", "finviz", "options"):
                    response = client.get(f"/api/v1/dad/ticker/COLD/{path}")
                    assert response.status_code == 200, path

        assert fetched == []  # All stored reads, including refresh and stream.
        assert not any(sql.startswith(("INSERT", "UPDATE", "DELETE", "ALTER", "CREATE", "DROP"))
                       for sql in statements)
        with engine.connect() as conn:
            assert conn.execute(text("SELECT COUNT(*) FROM raw_series")).scalar_one() == 3
    finally:
        dad._GOLD_MEMORY_CACHE.clear()
        dad._FINVIZ_MEMORY_CACHE.clear()
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
        admin.dispose()
