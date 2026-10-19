"""Real-PostgreSQL contract for store/materialization_provenance.py.

Runs the helper's fixed SQL against real (minimal) tables in a disposable
schema: missing / empty / fresh / stale, source_last_ingested from
signal_sources and raw_series, and a *genuine* statement timeout. Offline
coverage of the same rules, plus the endpoint contract, is in
tests/test_flows_provenance.py.

Skipped without GRID_TEST_DB_URL / DB_URL (disposable PostgreSQL).
"""

from __future__ import annotations

import dataclasses
import os
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("DB_PASSWORD", "testpass")

_DB_URL = os.environ.get("GRID_TEST_DB_URL") or os.environ.get("DB_URL")
pytestmark = [
    pytest.mark.skipif(not _DB_URL, reason="requires disposable PostgreSQL via GRID_TEST_DB_URL or DB_URL"),
    pytest.mark.xdist_group("postgres"),
]

from store import materialization_provenance as mp  # noqa: E402

UTC = timezone.utc
NOW = datetime.now(UTC)
TODAY = NOW.date()


@contextmanager
def _schema():
    admin = create_engine(_DB_URL)
    name = "prov_" + uuid.uuid4().hex[:12]
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{name}"'))
    engine = create_engine(_DB_URL, connect_args={"options": f"-csearch_path={name}"})
    try:
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{name}" CASCADE'))
        admin.dispose()


@pytest.fixture()
def engine():
    mp.clear_cache()
    with _schema() as eng:
        with eng.begin() as conn:
            conn.execute(text("CREATE TABLE etf_flows (ticker TEXT, flow_date DATE, created_at TIMESTAMPTZ)"))
            conn.execute(text("CREATE TABLE insider_trades (ticker TEXT, trade_date DATE, created_at TIMESTAMPTZ)"))
            conn.execute(text("CREATE TABLE dark_pool_weekly (ticker TEXT, report_date DATE, created_at TIMESTAMPTZ)"))
            conn.execute(text(
                "CREATE TABLE signal_sources (source_type TEXT, signal_date DATE, created_at TIMESTAMPTZ)"))
            conn.execute(text(
                "CREATE TABLE raw_series (series_id TEXT, obs_date DATE, pull_timestamp TIMESTAMPTZ, "
                "pull_status TEXT)"))
            # junction_point_readings, congressional_trades: deliberately absent.
        yield eng
    mp.clear_cache()


def _insert(engine, table, col, days_ago, n=1, created=NOW):
    with engine.begin() as conn:
        for _ in range(n):
            conn.execute(text(f"INSERT INTO {table} VALUES ('X', :d, :c)"),  # test-only literal identifiers
                         {"d": TODAY - timedelta(days=days_ago), "c": created})


def test_missing_empty_fresh_stale(engine):
    p = mp.table_provenance(engine, "junction_point_readings")
    assert (p["unavailable"], p["reason"]) == (True, "missing_table")

    p = mp.table_provenance(engine, "etf_flows")
    assert (p["unavailable"], p["reason"], p["row_count"]) == (True, "empty", 0)

    mp.clear_cache()
    _insert(engine, "etf_flows", "flow_date", days_ago=1, n=3)
    p = mp.table_provenance(engine, "etf_flows")
    assert p["unavailable"] is False and p["reason"] is None
    assert p["row_count"] == 3 and p["max_obs_date"] == (TODAY - timedelta(days=1)).isoformat()
    assert p["age_days"] == 1 and p["materialized_at"] is not None

    _insert(engine, "insider_trades", "trade_date", days_ago=9)
    p = mp.table_provenance(engine, "insider_trades")
    assert (p["unavailable"], p["reason"], p["age_days"]) == (True, "stale", 9)
    assert p["row_count"] == 1                      # what is there is still reported

    _insert(engine, "dark_pool_weekly", "report_date", days_ago=9)
    assert mp.table_provenance(engine, "dark_pool_weekly")["unavailable"] is False  # weekly: 10 days


def test_source_last_ingested_from_signal_sources_and_raw_series(engine):
    ts = NOW - timedelta(hours=5)
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO signal_sources VALUES ('insider', :d, :c), ('darkpool', :d, :c2)"),
                     {"d": TODAY, "c": ts, "c2": ts - timedelta(days=1)})
        conn.execute(text("INSERT INTO raw_series VALUES ('WALCL', :d, :c, 'SUCCESS'), "
                          "('WALCL', :d, :c2, 'FAILED'), ('OTHER', :d, :c2, 'SUCCESS')"),
                     {"d": TODAY, "c": ts, "c2": NOW})
    _insert(engine, "insider_trades", "trade_date", days_ago=0)
    p = mp.table_provenance(engine, "insider_trades")
    assert datetime.fromisoformat(p["source_last_ingested"]) == ts
    # Tables with no cheaply derivable upstream say so.
    _insert(engine, "etf_flows", "flow_date", days_ago=0)
    assert mp.table_provenance(engine, "etf_flows")["source_last_ingested"] is None


def test_real_statement_timeout_is_unavailable_timeout(engine, monkeypatch):
    _insert(engine, "etf_flows", "flow_date", days_ago=0)
    slow = dataclasses.replace(
        mp.TABLE_SPECS["etf_flows"],
        sql="SELECT count(*), max(flow_date), max(created_at) FROM etf_flows, pg_sleep(3) AS s",
    )
    monkeypatch.setitem(mp.TABLE_SPECS, "etf_flows", slow)
    monkeypatch.setattr(mp, "_SET_TIMEOUT_SQL", "SET LOCAL statement_timeout = 100")
    started = datetime.now(UTC)
    p = mp.table_provenance(engine, "etf_flows")
    assert (datetime.now(UTC) - started).total_seconds() < 2.5
    assert (p["unavailable"], p["reason"]) == (True, "timeout")


def test_timeout_does_not_leak_and_sql_is_read_only(engine):
    _insert(engine, "etf_flows", "flow_date", days_ago=0)
    mp.table_provenance(engine, "etf_flows")
    with engine.connect() as conn:
        assert conn.execute(text("SHOW statement_timeout")).scalar() in ("0", "0ms")
    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM etf_flows")).scalar() == 1


def test_every_spec_sql_executes_against_its_documented_columns(engine):
    """Each fixed statement parses and runs (empty tables -> reason 'empty', never 'error:*')."""
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE congressional_trades (disclosure_date DATE, created_at TIMESTAMPTZ)"))
        conn.execute(text("CREATE TABLE junction_point_readings (obs_date DATE, created_at TIMESTAMPTZ)"))
        conn.execute(text("CREATE TABLE options_daily_signals (signal_date DATE, created_at TIMESTAMPTZ)"))
        conn.execute(text("CREATE TABLE capital_flow_snapshots (snapshot_date DATE, generated_at TIMESTAMPTZ)"))
    for table in mp.TABLE_SPECS:
        p = mp.table_provenance(engine, table, use_cache=False)
        assert p["reason"] == "empty", (table, p)
