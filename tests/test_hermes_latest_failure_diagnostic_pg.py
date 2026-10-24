"""Opted-in native PostgreSQL selection contract; never a default-DB test.

UNRUN during source preparation. Existing pg_engine is requested only after
explicit future isolated target binding. No application modules/provider calls.
"""
from __future__ import annotations

import ast
import contextlib
from datetime import timedelta, timezone
import json
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "scripts/hermes_fixers.py"

SOURCE_CATALOG_TEMP_DDL = "CREATE TEMP TABLE source_catalog (\n    id                SERIAL PRIMARY KEY,\n    name              TEXT NOT NULL UNIQUE,\n    base_url          TEXT NOT NULL,\n    cost_tier         TEXT NOT NULL CHECK (cost_tier IN ('FREE', 'LOW', 'PAID')),\n    -- Seven values mirror the live griddb source_catalog_latency_class_check\n    -- (widened out-of-band before 2026-10-20; re-read 2026-10-20). Readers:\n    -- alerts/cadence.py _METADATA_CADENCE; pinned by tests/test_schema_latency_class_constraint.py.\n    latency_class     TEXT NOT NULL CHECK (latency_class IN ('REALTIME', 'INTRADAY', 'EOD', 'WEEKLY', 'MONTHLY', 'QUARTERLY', 'ANNUAL')),\n    pit_available     BOOLEAN NOT NULL DEFAULT FALSE,\n    revision_behavior TEXT NOT NULL CHECK (revision_behavior IN ('NEVER', 'RARE', 'FREQUENT')),\n    trust_score       TEXT NOT NULL CHECK (trust_score IN ('HIGH', 'MED', 'LOW')),\n    priority_rank     INTEGER NOT NULL,\n    active            BOOLEAN NOT NULL DEFAULT TRUE,\n    last_pull_at      TIMESTAMPTZ,\n    uptime_score      DOUBLE PRECISION NOT NULL DEFAULT 1.0,\n    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()\n) ON COMMIT DROP;"
RAW_SERIES_TEMP_DDL = "CREATE TEMP TABLE raw_series (\n    id                BIGSERIAL PRIMARY KEY,\n    series_id         TEXT NOT NULL,\n    source_id         INTEGER NOT NULL REFERENCES source_catalog(id),\n    obs_date          DATE NOT NULL,\n    pull_timestamp    TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n    value             DOUBLE PRECISION NOT NULL,\n    raw_payload       JSONB,\n    -- QUARANTINED: a once-accepted row later found untrustworthy. It is not an\n    -- observation (readers take SUCCESS only) and not a pull failure. Pullers\n    -- never write it. Production got it via\n    -- migrations/versions/raw_series_quarantined_20260926.py (added NOT VALID,\n    -- same constraint name raw_series_pull_status_check).\n    pull_status       TEXT NOT NULL CHECK (pull_status IN ('SUCCESS', 'PARTIAL', 'FAILED', 'QUARANTINED'))\n) ON COMMIT DROP;"
BASELINE_QUERY_604 = "SELECT sc.name, COUNT(*) AS fail_count,        MAX(rs.pull_timestamp) AS last_fail,        MAX(rs.raw_payload::text) AS last_error FROM raw_series rs JOIN source_catalog sc ON rs.source_id = sc.id WHERE rs.pull_status = 'FAILED' AND rs.pull_timestamp > NOW() - INTERVAL '24 hours' GROUP BY sc.name ORDER BY fail_count DESC"


@pytest.fixture
def latest_failure_pg_connection(request):
    # Do not let conftest's default/fallback target or skip imply qualification.
    if os.environ.get("GRID_HERMES_FAILURE_PG_OPT_IN") != "1":
        pytest.skip("UNRUN: native latest-failure PostgreSQL target not opted in")
    url_text = os.environ.get("GRID_TEST_DB_URL")
    binding_text = os.environ.get("GRID_HERMES_FAILURE_PG_TARGET_BINDING")
    if not url_text or not binding_text:
        pytest.fail("Explicit GRID_TEST_DB_URL and isolated-target binding required", pytrace=False)
    from sqlalchemy import text
    from sqlalchemy.engine import make_url
    try:
        url = make_url(url_text)
        binding = json.loads(binding_text)
        identity = {"host": url.host, "port": url.port or 5432,
                    "database": url.database, "username": url.username}
        expected_keys = {"host", "port", "database", "username", "isolated", "purpose"}
        if (url.get_backend_name() != "postgresql" or url.query
                or not all(identity[k] for k in ("host", "database", "username"))
                or type(identity["port"]) is not int or not 1 <= identity["port"] <= 65535
                or not isinstance(binding, dict) or set(binding) != expected_keys
                or type(binding.get("port")) is not int
                or binding.get("isolated") is not True
                or binding.get("purpose") != "hermes-latest-failure-regression"
                or any(binding.get(k) != v for k, v in identity.items())):
            pytest.fail("Explicit isolated PostgreSQL target identity mismatch", pytrace=False)
    except Exception:
        pytest.fail("Invalid isolated PostgreSQL target binding; values not echoed", pytrace=False)
    # This is the FIRST point at which an existing DB fixture may be requested.
    # Its normal skip becomes a failure when native qualification was opted in.
    try:
        pg_engine = request.getfixturevalue("pg_engine")
    except pytest.skip.Exception:
        pytest.fail("Opted-in native target unavailable; fixture skip is not proof", pytrace=False)
    engine_url = pg_engine.url
    if ({"host": engine_url.host, "port": engine_url.port or 5432,
         "database": engine_url.database, "username": engine_url.username} != identity
            or engine_url.get_backend_name() != "postgresql" or engine_url.query):
        pytest.fail("Existing pg_engine does not match the bound target", pytrace=False)
    with pg_engine.connect() as conn:
        transaction = conn.begin()
        try:
            actual = conn.execute(text("SELECT current_database(), current_user")).one()
            if tuple(actual) != (identity["database"], identity["username"]):
                pytest.fail("Connected database/user differs from isolated binding", pytrace=False)
            conn.execute(text("SET LOCAL search_path = pg_temp, pg_catalog"))
            conn.execute(text("SET LOCAL TIME ZONE 'UTC'"))
            conn.execute(text("SET LOCAL statement_timeout = '5s'"))
            conn.execute(text("SET LOCAL lock_timeout = '1s'"))
            # No IF NOT EXISTS: a collision fails rather than touching another table.
            conn.execute(text(SOURCE_CATALOG_TEMP_DDL))
            conn.execute(text(RAW_SERIES_TEMP_DDL))
            conn.execute(text(
                "CREATE UNIQUE INDEX uq_raw_series_composite "
                "ON raw_series (series_id, source_id, obs_date, pull_timestamp)"))
            # NOW() is stable within this transaction, matching the query's window.
            now = conn.execute(text("SELECT NOW()")).scalar_one()
            yield conn, now
        finally:
            if transaction.is_active:
                transaction.rollback()


def _source(conn, name):
    from sqlalchemy import text
    return conn.execute(text(
        "INSERT INTO source_catalog "
        "(name, base_url, cost_tier, latency_class, revision_behavior, trust_score, priority_rank) "
        "VALUES (:name, 'SYNTHETIC', 'FREE', 'EOD', 'NEVER', 'HIGH', 1) RETURNING id"
    ), {"name": name}).scalar_one()


def _failure(conn, source_id, stamp, error, *, series="SYNTH", status="FAILED", sql_null=False):
    from sqlalchemy import text
    conn.execute(text(
        "INSERT INTO raw_series "
        "(series_id, source_id, obs_date, pull_timestamp, value, raw_payload, pull_status) "
        "VALUES (:series, :sid, :day, :stamp, 0, CAST(:payload AS JSONB), :status)"
    ), {"series": series, "sid": source_id, "day": stamp.astimezone(timezone.utc).date(), "stamp": stamp,
        "payload": None if sql_null else json.dumps(error), "status": status})


class _Log:
    def __init__(self):
        self.records = []
    def info(self, template, **kwargs):
        self.records.append(("INFO", template.format(**kwargs)))
    def warning(self, template, **kwargs):
        self.records.append(("WARNING", template.format(**kwargs)))


class _OneConnection:
    """Use the same TEMP-table connection, preserving the actual SQL and PG result types."""
    def __init__(self, conn):
        self.conn = conn
        self.queries = []
        self.rows = None
    def connect(self):
        return contextlib.nullcontext(self)
    def execute(self, statement):
        self.queries.append(str(statement))
        self.rows = self.conn.execute(statement).fetchall()
        return self
    def fetchall(self):
        return self.rows


def _diagnose(conn):
    # Execute actual function source, not a replacement query or a live import.
    tree = ast.parse(SOURCE.read_bytes())
    fn = next(node for node in tree.body
              if isinstance(node, ast.FunctionDef) and node.name == "diagnose_and_fix_pulls")
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    module = ast.fix_missing_locations(ast.Module(body=[future, fn], type_ignores=[]))
    log = _Log()
    namespace = {"log": log}
    exec(compile(module, str(SOURCE), "exec"), namespace)
    engine = _OneConnection(conn)
    # BOTH flags are necessary: production dry_run is checked after optional LLM diagnosis.
    result = namespace["diagnose_and_fix_pulls"](
        engine, hermes_available=False, state=object(), dry_run=True)
    assert len(engine.queries) == 1
    return engine.rows, result, log.records


def test_newest_failure_jsonb_error_is_bound_to_native_timestamp(latest_failure_pg_connection):
    from sqlalchemy import text
    conn, now = latest_failure_pg_connection
    sid = _source(conn, "SYNTH_SOURCE_A")
    old, latest = now - timedelta(hours=2), now - timedelta(hours=1)
    _failure(conn, sid, old, {"error": "ZZZ_OLDER"}, series="SYNTH_OLD")
    _failure(conn, sid, latest, {"error": "AAA_NEWER"}, series="SYNTH_NEW")
    # Native legacy query is the functional negative control on exactly these rows.
    legacy = conn.execute(text(BASELINE_QUERY_604)).one()
    assert legacy[2] == latest and json.loads(legacy[3]) == {"error": "ZZZ_OLDER"}
    rows, result, logs = _diagnose(conn)
    assert len(rows) == 1 and tuple(rows[0][:3]) == ("SYNTH_SOURCE_A", 2, latest)
    assert json.loads(rows[0][3]) == {"error": "AAA_NEWER"}
    assert result["retried"] == result["diagnosed"] == 0
    warning = next(message for level, message in logs if level == "WARNING")
    assert "AAA_NEWER" in warning and "ZZZ_OLDER" not in warning


def test_equal_time_error_uses_native_text_max_tie_rule(latest_failure_pg_connection):
    from sqlalchemy import text
    conn, now = latest_failure_pg_connection
    sid = _source(conn, "SYNTH_SOURCE_A")
    latest = now - timedelta(hours=1)
    _failure(conn, sid, now - timedelta(hours=2), {"error": "ZZZZ_OLDER"}, series="OLD")
    for i, value in enumerate(({"error": "AAA"}, {"error": "ZZZ"}, None)):
        _failure(conn, sid, latest, value, series=f"TIE_{i}", sql_null=value is None)
    expected = conn.execute(text(
        "SELECT MAX(raw_payload::text) FROM raw_series WHERE pull_timestamp = :latest"
    ), {"latest": latest}).scalar_one()
    rows, _, _ = _diagnose(conn)
    assert tuple(rows[0][:3]) == ("SYNTH_SOURCE_A", 4, latest)
    assert rows[0][3] == expected


def test_latest_sql_null_stays_unknown(latest_failure_pg_connection):
    conn, now = latest_failure_pg_connection
    sid = _source(conn, "SYNTH_SOURCE_A")
    _failure(conn, sid, now - timedelta(hours=2), {"error": "OLDER"}, series="OLD")
    _failure(conn, sid, now - timedelta(hours=1), None, series="NEW", sql_null=True)
    rows, _, _ = _diagnose(conn)
    assert rows[0][1] == 2 and rows[0][3] is None


def test_latest_json_null_retains_jsonb_text_semantics(latest_failure_pg_connection):
    conn, now = latest_failure_pg_connection
    sid = _source(conn, "SYNTH_SOURCE_A")
    _failure(conn, sid, now - timedelta(hours=2), {"error": "OLDER"}, series="OLD")
    _failure(conn, sid, now - timedelta(hours=1), None, series="NEW")
    rows, _, _ = _diagnose(conn)
    assert rows[0][1] == 2 and rows[0][3] == "null"


def test_native_filters_counts_order_and_offset_timestamp(latest_failure_pg_connection):
    conn, now = latest_failure_pg_connection
    a, b = _source(conn, "SYNTH_SOURCE_A"), _source(conn, "SYNTH_SOURCE_B")
    latest = now - timedelta(minutes=30)
    _failure(conn, a, now - timedelta(hours=2), {"error": "ZZZ_OLDER"}, series="A_OLD")
    _failure(conn, a, latest.astimezone(timezone(timedelta(hours=5, minutes=30))),
             {"error": "AAA_NEWER"}, series="A_NEW")
    _failure(conn, a, now - timedelta(hours=24), {"error": "BOUNDARY"}, series="A_BOUNDARY")
    _failure(conn, a, now - timedelta(hours=25), {"error": "TOO_OLD"}, series="A_TOO_OLD")
    _failure(conn, a, now, {"error": "NOT_FAILED"}, series="A_SUCCESS", status="SUCCESS")
    _failure(conn, b, now - timedelta(hours=1), {"error": "B"}, series="B_ONE")
    rows, _, _ = _diagnose(conn)
    assert [(r[0], r[1]) for r in rows] == [("SYNTH_SOURCE_A", 2), ("SYNTH_SOURCE_B", 1)]
    assert rows[0][2] == latest and json.loads(rows[0][3]) == {"error": "AAA_NEWER"}


def test_native_empty_has_no_failed_rows_or_retry(latest_failure_pg_connection):
    conn, _ = latest_failure_pg_connection
    rows, result, logs = _diagnose(conn)
    assert rows == []
    assert result == {"retried": 0, "fixed": 0, "diagnosed": 0, "skipped_cooldown": 0,
                      "skipped_no_handler": 0, "skipped_key_check": 0, "escalated": 0,
                      "abandoned": 0}
    assert logs == [("INFO", "No failed pulls in last 24h")]
