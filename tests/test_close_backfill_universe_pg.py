"""PostgreSQL contract for scripts/close_backfill_universe.py.

The unit tests fake the engine; this runs the script's real SQL (the work
queries, the observation reader and the upsert guard) on PostgreSQL.

Runs against a fresh schema on ``GRID_TEST_DB_URL`` (local, disposable
database only) and skips without it.
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

import scripts.close_backfill_universe as mod

pytestmark = pytest.mark.xdist_group("postgres")

TODAY = date(2026, 11, 23)
DDL = (
    "CREATE TABLE source_catalog (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
    "CREATE TABLE raw_series (id BIGSERIAL PRIMARY KEY, series_id TEXT NOT NULL, "
    "source_id INTEGER NOT NULL REFERENCES source_catalog(id), obs_date DATE NOT NULL, "
    "pull_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(), value DOUBLE PRECISION NOT NULL, "
    "raw_payload JSONB, pull_status TEXT NOT NULL)",
    "CREATE TABLE ticker_metrics_daily (id SERIAL PRIMARY KEY, ticker TEXT NOT NULL, obs_date DATE NOT NULL, "
    "shares_outstanding BIGINT, close_price NUMERIC, market_cap_usd NUMERIC, source TEXT, "
    "as_of TIMESTAMPTZ DEFAULT NOW(), UNIQUE (ticker, obs_date))",
    "CREATE TABLE data_freshness_audit (ticker TEXT NOT NULL, source_table TEXT, last_obs DATE, "
    "age_days INT, bucket TEXT NOT NULL, audited_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
    "CREATE TABLE gem_alerts (id SERIAL PRIMARY KEY, backtest_ticker TEXT, subject_kind TEXT, subject_id TEXT)",
    "INSERT INTO source_catalog VALUES (1, 'TIINGO'), (2, 'yfinance')",
)


@pytest.fixture()
def pg_engine():
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        pytest.skip("GRID_TEST_DB_URL is required for disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("the close backfill PG contract requires a local disposable test database")
    schema = "closebf_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    with engine.begin() as conn:
        for ddl in DDL:
            conn.execute(text(ddl))
    yield engine
    engine.dispose()
    with admin.begin() as conn:
        conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    admin.dispose()


def _close(conn, ticker: str, source_id: int, days_ago: int, value: float, status: str = "SUCCESS") -> None:
    conn.execute(
        text("INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) "
             "VALUES (:sid, :src, :d, :v, :st)"),
        {"sid": f"YF:{ticker}:close", "src": source_id, "d": TODAY - timedelta(days=days_ago), "v": value,
         "st": status},
    )


def _metrics(engine) -> dict[tuple[str, date], tuple]:
    with engine.connect() as conn:
        return {(t, d): (float(c) if c is not None else None, shares, src) for t, d, c, shares, src in conn.execute(
            text("SELECT ticker, obs_date, close_price, shares_outstanding, source FROM ticker_metrics_daily"))}


def test_universe_run_on_postgres(pg_engine):
    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO data_freshness_audit (ticker, bucket, audited_at) VALUES "
            "('OLD', 'DEAD', now() - interval '2 days'), "          # an earlier audit: ignored
            "('AAA', 'DEAD', now()), ('BBB', 'STALE_7_30', now()), ('GLEN', 'STALE_30+', now()), "
            "('FINE', 'FRESH', now())"))
        _close(conn, "AAA", 1, 1, 11.0)
        _close(conn, "AAA", 1, 2, 10.0)
        _close(conn, "AAA", 1, 3, 0.0, status="FAILED")           # a failed pull is not an observation
        _close(conn, "AAA", 2, 5, 9.0)                            # yfinance ends earlier: not used
        _close(conn, "BBB", 2, 1, 5.5)
        _close(conn, "BBB", 2, 90, 4.0)                           # outside the 60-day window
        _close(conn, "FINE", 1, 1, 1.0)
        # A row sec_xbrl_shares owns, and one this job may correct.
        conn.execute(text(
            "INSERT INTO ticker_metrics_daily (ticker, obs_date, shares_outstanding, close_price, market_cap_usd, source) "
            "VALUES ('AAA', :d1, 1000, 10.9, 10900, 'sec_xbrl + yfinance'), "
            "('AAA', :d2, NULL, 9.7, NULL, 'twelvedata_universe_backfill')"),
            {"d1": TODAY - timedelta(days=1), "d2": TODAY - timedelta(days=2)})

    summary = mod.run(pg_engine, "universe", today=TODAY)

    assert summary["tickers"] == 3 and summary["filled"] == 2
    assert summary["no_stored_close"] == ["GLEN"] and summary["failed"] == []
    assert summary["by_source"] == {"TIINGO": 1, "yfinance": 1}
    rows = _metrics(pg_engine)
    assert rows[("AAA", TODAY - timedelta(days=1))] == (10.9, 1000, "sec_xbrl + yfinance")     # untouched
    assert rows[("AAA", TODAY - timedelta(days=2))] == (10.0, None, "stored_close_backfill:TIINGO")
    assert rows[("BBB", TODAY - timedelta(days=1))] == (5.5, None, "stored_close_backfill:yfinance")
    assert not any(t in ("OLD", "FINE", "GLEN") for t, _d in rows)
    assert (summary["written"], summary["left_as_is"]) == (2, 1)

    again = mod.run(pg_engine, "universe", today=TODAY)
    assert again["written"] == 0 and again["left_as_is"] == 3 and _metrics(pg_engine) == rows


def test_gem_run_on_postgres(pg_engine):
    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO gem_alerts (backtest_ticker, subject_kind, subject_id) VALUES "
            "('FLUT', 'ticker', 'FLUT'), (NULL, 'ticker', 'HAVE'), ('MACRO', 'macro', 'x y'), (NULL, 'ticker', 'NONE')"))
        conn.execute(text("INSERT INTO ticker_metrics_daily (ticker, obs_date, close_price, source) "
                          "VALUES ('HAVE', :d, 3.0, 'sec_xbrl + yfinance')"), {"d": TODAY - timedelta(days=2)})
        _close(conn, "FLUT", 1, 1, 250.0)
        _close(conn, "HAVE", 1, 1, 3.1)

    summary = mod.run(pg_engine, "gem", today=TODAY)

    assert summary["tickers"] == 1 and summary["filled"] == 1
    assert _metrics(pg_engine)[("FLUT", TODAY - timedelta(days=1))] == (250.0, None, "stored_close_backfill:TIINGO")
