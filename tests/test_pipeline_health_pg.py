"""Pipeline-health behavior on a disposable PostgreSQL schema.

This test must run with GRID_TEST_DB_URL pointing to a throwaway database.
The dedicated CI step treats a skip as a failure.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
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
            conn.execute(text("CREATE TABLE source_catalog (id integer PRIMARY KEY, name text NOT NULL, last_pull_at timestamptz, active boolean NOT NULL DEFAULT TRUE)"))
            # Mirrors migrations/add_pull_log.sql (columns, CHECK, indexes).
            conn.execute(text(
                "CREATE TABLE pull_log ("
                "  id serial PRIMARY KEY,"
                "  puller_name text NOT NULL,"
                "  source_id integer REFERENCES source_catalog(id),"
                "  started_at timestamptz NOT NULL,"
                "  completed_at timestamptz,"
                "  status text NOT NULL CHECK (status IN ('RUNNING','SUCCESS','PARTIAL','FAILED')),"
                "  rows_inserted integer DEFAULT 0,"
                "  rows_expected integer,"
                "  error_message text,"
                "  features_affected integer[],"
                "  node_name text DEFAULT 'grid-svr'"
                ")"
            ))
            conn.execute(text("CREATE INDEX idx_pull_log_status ON pull_log(status, completed_at DESC)"))
            conn.execute(text("CREATE INDEX idx_pull_log_puller ON pull_log(puller_name, started_at DESC)"))
            conn.execute(text("CREATE TABLE raw_series (id bigserial PRIMARY KEY, source_id integer, series_id text, pull_timestamp timestamptz, pull_status text)"))
            conn.execute(text("CREATE TABLE feature_registry (id integer PRIMARY KEY, name text, family text, model_eligible boolean)"))
            conn.execute(text("CREATE TABLE resolved_series (feature_id integer, vintage_date date)"))
            conn.execute(text("CREATE TABLE server_log (created_at timestamptz, source text, message text, level text)"))
            # Mirrors scripts/freshness_audit_universe.sql's actual shape
            # (data_freshness_audit / data_freshness_universe), not a
            # stand-in -- pipeline_health()/freshness() read this table
            # directly, so the fixture must match column names and types.
            conn.execute(text(
                "CREATE TABLE data_freshness_audit ("
                "  ticker text NOT NULL,"
                "  source_table text NOT NULL,"
                "  last_obs date,"
                "  age_days integer,"
                "  bucket text NOT NULL,"
                "  audited_at timestamptz NOT NULL DEFAULT now(),"
                "  PRIMARY KEY (ticker, source_table, audited_at)"
                ")"
            ))
            conn.execute(text("INSERT INTO source_catalog VALUES (1, 'yfinance', now()), (2, 'Fed_Liquidity', NULL)"))
            conn.execute(text("INSERT INTO raw_series (source_id, series_id, pull_timestamp, pull_status) VALUES (1, 'YF:SPY:close', now(), 'SUCCESS')"))
            conn.execute(text("INSERT INTO feature_registry VALUES (1, 'YF:SPY:close', 'market', true)"))
            conn.execute(text("INSERT INTO resolved_series VALUES (1, CURRENT_DATE)"))
            conn.execute(text("INSERT INTO server_log VALUES (now(), 'fixture', 'sample failure', 'ERROR')"))
            conn.execute(text(
                "INSERT INTO data_freshness_audit (ticker, source_table, last_obs, age_days, bucket, audited_at) VALUES "
                "('SPY', 'ticker_metrics_daily', CURRENT_DATE, 0, 'FRESH', now()), "
                "('OLDCO', 'ticker_metrics_daily', CURRENT_DATE - 40, 40, 'STALE_30+', now())"
            ))
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

    # F3: pipeline-health also carries the pre-computed daily audit
    # (data_freshness_audit), read as a bounded, indexed lookup rather than
    # a live scan -- this must be "available" and reflect the fixture's
    # two rows/buckets from today's audited_at, independent of the
    # per-source sections asserted above.
    audit = data["daily_audit"]
    assert audit["availability"] == "available"
    assert audit["stale_reason"] is None
    assert audit["total_tickers"] == 2
    assert audit["source_tables"] == ["ticker_metrics_daily"]
    buckets_by_name = {b["bucket"]: b["ticker_count"] for b in audit["buckets"]}
    assert buckets_by_name == {"FRESH": 1, "STALE_30+": 1}
    assert audit["field_record"]["availability"] == "available"
    assert audit["field_record"]["provenance"] == "measured"


def test_daily_audit_is_unavailable_when_stale(pipeline_pg):
    """An audit whose last run is more than one full missed cycle old must
    report unavailable/"stale", not silently present last night's (or
    older) numbers as if they were current."""
    old_audited_at = datetime.now(timezone.utc) - timedelta(hours=72)
    with pipeline_pg.begin() as conn:
        conn.execute(text("DELETE FROM data_freshness_audit"))
        conn.execute(
            text(
                "INSERT INTO data_freshness_audit "
                "(ticker, source_table, last_obs, age_days, bucket, audited_at) "
                "VALUES ('SPY', 'ticker_metrics_daily', CURRENT_DATE - 3, 3, 'FRESH', :audited_at)"
            ),
            {"audited_at": old_audited_at},
        )

    response = _response(pipeline_pg)
    assert response.status_code == 200
    data = response.json()
    audit = data["daily_audit"]
    assert audit["availability"] == "unavailable"
    assert audit["stale_reason"] == "stale"
    # The last-known numbers are still surfaced for context, just flagged.
    assert audit["total_tickers"] == 1
    assert audit["audited_at"] is not None
    # This is independent of the rest of the response, which is unaffected.
    assert data["availability"] == "available"


def test_daily_audit_is_unavailable_when_missing(pipeline_pg):
    """An audit table that has never been populated (the timer has not run
    yet) must read as unavailable/"never_configured", never as zero
    buckets that could be misread as "everything is DEAD"."""
    with pipeline_pg.begin() as conn:
        conn.execute(text("DELETE FROM data_freshness_audit"))

    response = _response(pipeline_pg)
    assert response.status_code == 200
    data = response.json()
    audit = data["daily_audit"]
    assert audit["availability"] == "unavailable"
    assert audit["stale_reason"] == "never_configured"
    assert audit["audited_at"] is None
    assert audit["buckets"] == []
    assert audit["total_tickers"] == 0


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


def test_source_query_avoids_sample_and_preserves_health_values(pipeline_pg):
    """Compare the actual route SQL with its former sampled-count query on PG."""
    with pipeline_pg.begin() as conn:
        conn.execute(text("CREATE INDEX idx_raw_series_source_pull ON raw_series (source_id, pull_timestamp DESC)"))
        conn.execute(text(
            "INSERT INTO raw_series (source_id, series_id, pull_timestamp, pull_status) "
            "SELECT 1, 'series-' || (g % 1000), now() - (g % 72) * interval '1 hour', 'SUCCESS' "
            "FROM generate_series(1, 50000) AS g"
        ))
        conn.execute(text(
            "INSERT INTO raw_series (source_id, series_id, pull_timestamp, pull_status) "
            "SELECT 2, 'series-' || (g % 400), now() - (g % 240) * interval '1 hour', 'SUCCESS' "
            "FROM generate_series(1, 20000) AS g"
        ))
        conn.execute(text("ANALYZE raw_series"))

    captured = []

    def capture_source(_conn, _cursor, statement, parameters, _context, _many):
        if "FROM source_catalog sc" in statement:
            captured.append((statement, parameters))

    event.listen(pipeline_pg, "before_cursor_execute", capture_source)
    try:
        response = _response(pipeline_pg)
    finally:
        event.remove(pipeline_pg, "before_cursor_execute", capture_source)
    assert response.status_code == 200
    payload = response.json()
    assert payload["availability"] == "available"
    assert payload["summary"]["total_sources"] == 2
    assert all(source["series_count"] is None for source in payload["sources"])
    assert len(captured) == 1
    candidate_sql, candidate_params = captured[0]
    assert "COUNT(DISTINCT sampled.series_id)" not in candidate_sql

    old_sql = candidate_sql.replace(
        "sc.id AS source_id, sc.active AS catalog_active ",
        "sc.id AS source_id, sc.active AS catalog_active, "
        "COALESCE(series.series_count, 0) AS series_count ",
    ).replace(
        "ORDER BY sc.name",
        "LEFT JOIN LATERAL ("
        "  SELECT COUNT(DISTINCT sampled.series_id) AS series_count "
        "  FROM (SELECT rs.series_id FROM raw_series rs "
        "        WHERE rs.source_id = sc.id ORDER BY rs.pull_timestamp DESC "
        "        LIMIT %(series_limit)s) sampled"
        ") series ON TRUE ORDER BY sc.name",
    )
    with pipeline_pg.connect() as conn:
        candidate_rows = conn.exec_driver_sql(candidate_sql, candidate_params).fetchall()
        original_rows = conn.exec_driver_sql(old_sql, {"series_limit": 50_000}).fetchall()
        assert [tuple(row[:5]) for row in candidate_rows] == [tuple(row[:5]) for row in original_rows]
        assert all(row[5] > 0 for row in original_rows)
        candidate_plan = conn.exec_driver_sql(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + candidate_sql, candidate_params
        ).scalar_one()[0]
        original_plan = conn.exec_driver_sql(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + old_sql,
            {"series_limit": 50_000},
        ).scalar_one()[0]
    print(
        "pipeline_source_pg_plan "
        f"rows={len(candidate_rows)} raw_fixture_rows=70001 "
        f"candidate_ms={candidate_plan['Execution Time']:.3f} "
        f"original_ms={original_plan['Execution Time']:.3f} "
        f"candidate_shared_hit={candidate_plan['Plan'].get('Shared Hit Blocks', 0)} "
        f"original_shared_hit={original_plan['Plan'].get('Shared Hit Blocks', 0)} "
        f"candidate_shared_read={candidate_plan['Plan'].get('Shared Read Blocks', 0)} "
        f"original_shared_read={original_plan['Plan'].get('Shared Read Blocks', 0)}"
    )


def _seed_run_states(engine):
    """One catalog row per pipeline status, driven by real pull_log rows.

    Names are outside _SOURCE_SCHEDULE and PULLER_REGISTRY, so every row
    gets the 168 h default window (recent = 336 h).
    """
    long_tail = "x" * 2000
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO source_catalog (id, name, last_pull_at, active) VALUES "
            "(10, 'pg_options', NULL, true), (11, 'pg_monthly', NULL, true), "
            "(12, 'pg_nokey', NULL, true), (13, 'pg_authfail', NULL, false), "
            "(14, 'pg_failing', NULL, true), (15, 'pg_stopped', NULL, true), "
            "(16, 'pg_inactive', NULL, false), (17, 'pg_stale', NULL, true), "
            "(18, 'pg_byname', NULL, true)"
        ))
        conn.execute(text(
            "INSERT INTO raw_series (source_id, series_id, pull_timestamp, pull_status) "
            "VALUES (17, 'PG:STALE', now() - interval '200 hours', 'SUCCESS')"
        ))
        conn.execute(
            text(
                "INSERT INTO pull_log (puller_name, source_id, started_at, completed_at, status, rows_inserted, error_message) VALUES "
                # healthy: rows landed in its own tables, nothing in raw_series
                "('YFinance_Options', 10, now() - interval '2 hours', now() - interval '2 hours', 'SUCCESS', 75994, NULL), "
                # quiet: old release, latest run found nothing new
                "('pg_monthly', 11, now() - interval '200 hours', now() - interval '200 hours', 'SUCCESS', 500, NULL), "
                "('pg_monthly', 11, now() - interval '1 hour', now() - interval '1 hour', 'SUCCESS', 0, 'NO_NEW_DATA: run completed, 0 rows written'), "
                # skipped_config (SKIPPED marker); a newer orphaned RUNNING row is ignored
                "('pg_nokey', 12, now() - interval '3 hours', now() - interval '3 hours', 'SUCCESS', 0, 'SKIPPED: PG_API_KEY not configured'), "
                "('pg_nokey', 12, now() - interval '1 hour', NULL, 'RUNNING', 0, NULL), "
                # skipped_config (FAILED, CONFIG class)
                "('pg_authfail', 13, now() - interval '1 hour', now() - interval '1 hour', 'FAILED', 0, 'PullerReportedFailure: no_api_key'), "
                # failing: latest run timed out, with a credential in the URL
                "('pg_failing', 14, now() - interval '400 hours', now() - interval '400 hours', 'SUCCESS', 12, NULL), "
                "('pg_failing', 14, now() - interval '1 hour', now() - interval '1 hour', 'FAILED', 0, "
                "  'TIMEOUT: GET https://api.example.com/v1/q?apikey=PGSECRET123&x=1 ' || :tail), "
                # stopped: rows long ago, nothing since
                "('pg_stopped', 15, now() - interval '1000 hours', now() - interval '1000 hours', 'SUCCESS', 7, NULL), "
                # healthy, attributed by name (no source_id, different case)
                "('PG_ByName', NULL, now() - interval '3 hours', now() - interval '3 hours', 'PARTIAL', 4, "
                "  'source coverage incomplete or row count unknown')"
            ),
            {"tail": long_tail},
        )


def test_pull_log_drives_each_status(pipeline_pg):
    _seed_run_states(pipeline_pg)
    response = _response(pipeline_pg)
    assert response.status_code == 200
    data = response.json()
    assert data["availability"] == "available"
    assert data["status_basis"] == "pull_log+raw_series"
    by_name = {row["name"]: row for row in data["sources"]}
    assert {name: row["status"] for name, row in by_name.items()} == {
        "yfinance": "healthy",
        "Fed_Liquidity": "inactive",
        "pg_options": "healthy",
        "pg_byname": "healthy",
        "pg_monthly": "quiet",
        "pg_nokey": "skipped_config",
        "pg_authfail": "skipped_config",
        "pg_failing": "failing",
        "pg_stopped": "stopped",
        "pg_inactive": "inactive",
        "pg_stale": "stale",
    }

    summary = data["summary"]
    assert summary["total_sources"] == 11
    assert (summary["healthy"], summary["stale"], summary["quiet"], summary["skipped_config"]) == (3, 1, 1, 2)
    assert (summary["failing"], summary["stopped"], summary["inactive"]) == (1, 1, 2)
    assert summary["broken"] == summary["failing"] + summary["stopped"] == 2
    assert summary["active_sources"] == 9
    assert sum(summary["by_status"].values()) == 11

    options = by_name["pg_options"]
    assert options["rows_last_pull"] == 0  # nothing in raw_series ...
    assert options["last_rows_inserted"] == 75994  # ... but pull_log saw the rows
    assert options["last_success_with_rows_at"] is not None
    assert options["field_record"]["availability"] == "available"
    assert options["runs_7d"] == 1

    nokey = by_name["pg_nokey"]
    assert nokey["last_status"] == "SUCCESS"  # the RUNNING row is not the latest run
    assert nokey["last_error"] == "SKIPPED: PG_API_KEY not configured"
    assert nokey["error"] == nokey["last_error"]
    assert nokey["field_record"]["stale_reason"] == "never_configured"

    failing = by_name["pg_failing"]
    assert failing["last_status"] == "FAILED"
    assert "PGSECRET123" not in failing["last_error"]
    assert len(failing["last_error"]) <= 300
    assert failing["last_error_class"] == "TIMEOUT"
    assert failing["runs_7d"] == 1  # the 400 h-old run is outside 7 days
    assert failing["field_record"]["stale_reason"] == "fetch_failed"
    assert "PGSECRET123" not in response.text

    monthly = by_name["pg_monthly"]
    assert monthly["field_record"]["stale_reason"] == "not_published_yet"
    assert monthly["freshness"] == "green"
    assert by_name["pg_stopped"]["freshness"] == "red"
    assert by_name["pg_inactive"]["field_record"]["availability"] == "unavailable"
    assert by_name["pg_authfail"]["catalog_active"] is False


def test_missing_pull_log_falls_back_to_raw_series_visibly(pipeline_pg):
    with pipeline_pg.begin() as conn:
        conn.execute(text("DROP TABLE pull_log"))
    response = _response(pipeline_pg)
    assert response.status_code == 200
    data = response.json()
    assert data["availability"] == "unavailable"
    assert data["stale_reason"] == "consumer_query_mismatch"
    assert data["status_basis"] == "raw_series_only"
    by_name = {row["name"]: row for row in data["sources"]}
    assert by_name["yfinance"]["status"] == "healthy"
    # The rest of the response still reads: the savepoint kept the
    # transaction usable after the failed pull_log read.
    assert data["coverage"]["by_family"]["market"]["with_data"] == 1
    assert len(data["recent_errors"]) == 1


def test_pull_log_summary_plan_is_one_scan(pipeline_pg):
    """The run summary reads pull_log once (CTE), not once per catalog row."""
    from ingestion.source_freshness import (
        CATALOG_RUN_ERROR_CHARS,
        CATALOG_RUN_LOOKBACK_DAYS,
        CATALOG_RUN_SUMMARY_SQL,
    )

    _seed_run_states(pipeline_pg)
    with pipeline_pg.begin() as conn:
        conn.execute(text(
            "INSERT INTO pull_log (puller_name, source_id, started_at, completed_at, status, rows_inserted) "
            "SELECT 'pg_bulk', 10 + (g % 9), now() - (g % 2000) * interval '1 hour', "
            "       now() - (g % 2000) * interval '1 hour', 'SUCCESS', g % 3 "
            "FROM generate_series(1, 20000) AS g"
        ))
        conn.execute(text("ANALYZE pull_log"))
    with pipeline_pg.connect() as conn:
        plan = conn.execute(
            text("EXPLAIN (ANALYZE, FORMAT JSON) " + CATALOG_RUN_SUMMARY_SQL.text),
            {"lookback_days": CATALOG_RUN_LOOKBACK_DAYS, "error_chars": CATALOG_RUN_ERROR_CHARS},
        ).scalar_one()[0]

    def scans(node):
        found = 1 if node.get("Relation Name") == "pull_log" else 0
        return found + sum(scans(child) for child in node.get("Plans", []))

    assert scans(plan["Plan"]) == 1
    print(f"pull_log_summary_pg_plan rows=20010 exec_ms={plan['Execution Time']:.3f}")
