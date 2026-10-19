"""Real-PostgreSQL check of the dark-pool / congressional reader fixes.

Same data shapes as the 2026-10-19 trace (CLAUDE-SLICE2-ACCEPTANCE-TRACE cases
B and C) in a disposable schema, through the real SQL of
``api.routers.flows._read_dark_pool_signal`` / ``_read_congressional_activity``
and the connection-graph reads. Offline coverage: tests/test_flows_reader_fixes.py.

Skipped without GRID_TEST_DB_URL / DB_URL (disposable PostgreSQL).
"""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from datetime import date, timedelta

import pytest
from sqlalchemy import create_engine, text

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("DB_PASSWORD", "testpass")

_DB_URL = os.environ.get("GRID_TEST_DB_URL") or os.environ.get("DB_URL")
pytestmark = [
    pytest.mark.skipif(not _DB_URL, reason="requires disposable PostgreSQL via GRID_TEST_DB_URL or DB_URL"),
    pytest.mark.xdist_group("postgres"),
]

from api.routers import flows  # noqa: E402

TODAY = date.today()
TECH = ["AAPL", "AMD", "AMZN", "GOOGL", "META", "MSFT", "NVDA", "PLTR", "TSLA"]


@contextmanager
def _schema():
    admin = create_engine(_DB_URL)
    name = "rfx_" + uuid.uuid4().hex[:12]
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
    with _schema() as eng:
        with eng.begin() as conn:
            conn.execute(text(
                "CREATE TABLE dark_pool_weekly (ticker TEXT, report_date DATE, short_volume DOUBLE PRECISION, "
                "total_volume DOUBLE PRECISION)"))
            conn.execute(text(
                "CREATE TABLE congressional_trades (ticker TEXT, disclosure_date DATE, representative TEXT, "
                "transaction_type TEXT, amount TEXT)"))
        yield eng


def _dp(engine, report_date, short, total=1000.0, tickers=TECH):
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO dark_pool_weekly VALUES (:t, :d, :s, :v)"),
                     [{"t": t, "d": report_date, "s": short, "v": total} for t in tickers])


def test_trace_case_b_null_short_volume_224_days_old(engine):
    """Nine Technology tickers, report_date 224 days back, short_volume NULL -> no signal."""
    _dp(engine, TODAY - timedelta(days=224), None)
    signal, as_of, reason = flows._read_dark_pool_signal(engine, TECH, TODAY)
    assert signal is None and reason == "stale"
    assert as_of == (TODAY - timedelta(days=224)).isoformat()


def test_null_short_volume_in_a_fresh_week_is_not_zero(engine):
    _dp(engine, TODAY - timedelta(days=14), None)
    assert flows._read_dark_pool_signal(engine, TECH, TODAY) == (
        None, (TODAY - timedelta(days=14)).isoformat(), "no_short_volume")


@pytest.mark.parametrize("short,expected", [(300.0, "accumulation"), (500.0, "neutral"), (600.0, "distribution")])
def test_fresh_week_with_short_volume(engine, short, expected):
    _dp(engine, TODAY - timedelta(days=21), short)
    signal, as_of, reason = flows._read_dark_pool_signal(engine, TECH, TODAY)
    assert (signal, reason) == (expected, None)
    assert as_of == (TODAY - timedelta(days=21)).isoformat()


def test_future_dated_week_is_never_the_newest_week(engine):
    """A report_date after today (bad/early-dated row) must not hide the real latest week."""
    _dp(engine, TODAY - timedelta(days=21), 300.0)
    _dp(engine, TODAY + timedelta(days=30), 700.0)
    signal, as_of, reason = flows._read_dark_pool_signal(engine, TECH, TODAY)
    assert (signal, reason) == ("accumulation", None)
    assert as_of == (TODAY - timedelta(days=21)).isoformat()


def test_newest_week_is_scoped_to_the_sectors_tickers(engine):
    _dp(engine, TODAY - timedelta(days=7), 700.0, tickers=["JPM"])        # another sector, newer
    _dp(engine, TODAY - timedelta(days=21), 300.0)
    assert flows._read_dark_pool_signal(engine, TECH, TODAY)[0] == "accumulation"


def test_no_rows(engine):
    assert flows._read_dark_pool_signal(engine, TECH, TODAY) == (None, None, "no_rows")


def test_trace_case_c_future_disclosure_rows_are_not_listed(engine):
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO congressional_trades VALUES (:t, :d, :r, 'BUY', '$1,001 - $15,000')"), [
            {"t": "ADI", "d": TODAY + timedelta(days=34), "r": "Rick Larsen"},
            {"t": "AAPL", "d": TODAY - timedelta(days=27), "r": "Past Member"},
            {"t": "GOOGL", "d": TODAY, "r": "Today Member"},
            {"t": "MSFT", "d": TODAY - timedelta(days=90), "r": "Too Old"},
        ])
    acts = flows._read_congressional_activity(engine, TECH + ["ADI"], TODAY)
    assert [a["representative"] for a in acts] == ["Today Member", "Past Member"]
    assert all(date.fromisoformat(a["date"]) <= TODAY for a in acts)


def test_connection_graph_ignores_future_congress_and_null_short_volume(engine):
    """Two tickers trading together only via a future-dated row, and two with NULL
    short volume, must create no edges."""
    flows._sector_connections_cache.clear()
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO congressional_trades VALUES (:t, :d, 'X', 'BUY', 'a')"), [
            {"t": "AAPL", "d": TODAY + timedelta(days=30)},
            {"t": "AMD", "d": TODAY + timedelta(days=31)},
        ])
    _dp(engine, TODAY - timedelta(days=3), None, tickers=["AAPL", "AMD"])
    subsectors = {"core": {"weight": 0.5, "actors": [
        {"ticker": "AAPL", "name": "Apple", "weight": 0.5},
        {"ticker": "AMD", "name": "AMD", "weight": 0.5},
    ]}}
    out = flows._build_sector_connections(engine, "Technology", ["AAPL", "AMD"], subsectors)
    kinds = {e.get("type") for e in out["edges"]}
    assert {n["id"] for n in out["nodes"]} >= {"AAPL", "AMD"}     # edges can only exist between real nodes
    assert "co_congress_trade" not in kinds
    assert "co_dark_pool_accumulation" not in kinds
    flows._sector_connections_cache.clear()
