"""Pipeline-health behavior on a disposable PostgreSQL schema.

This test must run with GRID_TEST_DB_URL pointing to a throwaway database.
The dedicated CI step treats a skip as a failure.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, event, text

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")
os.environ.setdefault("GRID_JWT_EXPIRE_HOURS", "1")
os.environ.setdefault(
    "GRID_MASTER_PASSWORD_HASH",
    "$2b$12$abcdefghijklmnopqrstuuFb1mY3p5oXq0rN8sxqf6vV2QcVx1zSi",
)

from fastapi.testclient import TestClient  # noqa: E402

from api.auth import create_token  # noqa: E402
from api.main import app  # noqa: E402


@pytest.fixture
def pipeline_pg():
    db_url = os.environ.get("GRID_TEST_DB_URL")
    if not db_url:
        if os.environ.get("REQUIRE_PIPELINE_PG") == "1":
            pytest.fail("GRID_TEST_DB_URL is required for pipeline PostgreSQL proof")
        pytest.skip("GRID_TEST_DB_URL is not configured")
    pg_engine = create_engine(db_url, pool_pre_ping=True)
    schema = f"pipeline_{uuid.uuid4().hex[:12]}"
    try:
        with pg_engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    except Exception:
        pg_engine.dispose()
        raise
    engine = create_engine(
        pg_engine.url,
        pool_size=1,
        max_overflow=0,
        connect_args={"options": f"-csearch_path={schema}"},
    )
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE source_catalog (id integer PRIMARY KEY, name text NOT NULL, last_pull_at timestamptz)"))
            conn.execute(text("CREATE TABLE raw_series (id bigserial PRIMARY KEY, source_id integer, series_id text, pull_timestamp timestamptz, pull_status text)"))
            conn.execute(text("CREATE TABLE feature_registry (id integer PRIMARY KEY, name text, family text, model_eligible boolean)"))
            conn.execute(text("CREATE TABLE resolved_series (feature_id integer, vintage_date date)"))
            conn.execute(text("CREATE TABLE server_log (created_at timestamptz, source text, message text, level text)"))
            conn.execute(text("INSERT INTO source_catalog VALUES (1, 'yfinance', now()), (2, 'Fed_Liquidity', NULL)"))
            conn.execute(text("INSERT INTO raw_series (source_id, series_id, pull_timestamp, pull_status) VALUES (1, 'YF:SPY:close', now(), 'SUCCESS')"))
            conn.execute(text("INSERT INTO feature_registry VALUES (1, 'YF:SPY:close', 'market', true)"))
            conn.execute(text("INSERT INTO resolved_series VALUES (1, CURRENT_DATE)"))
            conn.execute(text("INSERT INTO server_log VALUES (now(), 'fixture', 'sample failure', 'ERROR')"))
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        pg_engine.dispose()


def _response(engine):
    with patch("api.routers.system.get_db_engine", return_value=engine):
        return TestClient(app).get(
            "/api/v1/system/pipeline-health",
            headers={"Authorization": f"Bearer {create_token(expires_hours=1)}"},
        )


def test_representative_postgres_health_is_available(pipeline_pg):
    response = _response(pipeline_pg)
    assert response.status_code == 200
    data = response.json()
    assert data["availability"] == "available"
    assert data["stale_reason"] is None
    assert data["summary"]["total_sources"] == 2
    by_name = {row["name"]: row for row in data["sources"]}
    assert by_name["yfinance"]["rows_last_pull"] == 1
    assert by_name["yfinance"]["status"] == "healthy"
    assert by_name["Fed_Liquidity"]["field_record"]["availability"] == "unavailable"
    assert data["coverage"]["by_family"]["market"]["with_data"] == 1
    assert len(data["recent_errors"]) == 1
    assert data["resolver_status"]["last_run"] == datetime.now(timezone.utc).date().isoformat()


def test_source_cancellation_fails_closed_and_reuses_connection(pipeline_pg):
    with pipeline_pg.connect() as conn:
        backend_pid = conn.execute(text("SELECT pg_backend_pid()" )).scalar_one()

    def slow_source(_conn, _cursor, statement, parameters, _context, _many):
        if "FROM source_catalog sc" in statement:
            return "SELECT pg_sleep(6)", ()
        return statement, parameters

    event.listen(pipeline_pg, "before_cursor_execute", slow_source, retval=True)
    try:
        response = _response(pipeline_pg)
    finally:
        event.remove(pipeline_pg, "before_cursor_execute", slow_source)
    assert response.status_code == 200
    data = response.json()
    assert data["availability"] == "unavailable"
    assert data["stale_reason"] == "fetch_failed"
    assert data["summary"]["healthy"] == 0

    with pipeline_pg.connect() as conn:
        assert conn.execute(text("SELECT pg_backend_pid()" )).scalar_one() == backend_pid
        assert conn.execute(text("SELECT 1")).scalar_one() == 1
    assert _response(pipeline_pg).json()["availability"] == "available"


def test_optional_postgres_failure_is_visible_and_resolver_continues(pipeline_pg):
    with pipeline_pg.begin() as conn:
        conn.execute(text("DROP TABLE server_log"))
        conn.execute(text("INSERT INTO raw_series (source_id, series_id, pull_timestamp, pull_status) VALUES (1, 'unmapped', now(), 'SUCCESS')"))

    response = _response(pipeline_pg)
    assert response.status_code == 200
    data = response.json()
    assert data["availability"] == "unavailable"
    assert data["stale_reason"] == "consumer_query_mismatch"
    assert data["summary"]["total_sources"] == 2
    assert data["coverage"]["by_family"]["market"]["with_data"] == 1
    assert data["resolver_status"]["pending"] == 1
