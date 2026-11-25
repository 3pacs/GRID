"""PostgreSQL contract for scripts/close_backfill_universe.py.

The unit tests script the raw rows; this runs the work queries, the real
completed-close reader and the fill-only write on PostgreSQL.

Runs against a fresh schema on ``GRID_TEST_DB_URL`` (local, disposable
database only). Skips without it unless ``REQUIRE_CLOSE_BACKFILL_PG=1``, which
the dedicated CI step sets so the contract cannot pass by skipping.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

import scripts.close_backfill_universe as mod

pytestmark = pytest.mark.xdist_group("postgres")

_REQUIRE_ENV = "REQUIRE_CLOSE_BACKFILL_PG"
EVALUATED_AT = datetime(2026, 11, 24, 1, 0, tzinfo=timezone.utc)      # Mon 20:00 New York
WED, THU, FRI = date(2026, 11, 18), date(2026, 11, 19), date(2026, 11, 20)
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
        if os.environ.get(_REQUIRE_ENV) == "1":
            pytest.fail(f"GRID_TEST_DB_URL is required for the close backfill PG contract ({_REQUIRE_ENV}=1)")
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


def _close(conn, ticker: str, source_id: int, day: date, value: float, *, pulled: datetime | None = None,
           status: str = "SUCCESS") -> None:
    pulled = pulled or datetime(day.year, day.month, day.day, 23, 30, tzinfo=timezone.utc)   # after 17:00 NY
    conn.execute(
        text("INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, pull_status) "
             "VALUES (:sid, :src, :d, :ts, :v, :st)"),
        {"sid": f"YF:{ticker}:close", "src": source_id, "d": day, "ts": pulled, "v": value, "st": status},
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
            "('AMB', 'DEAD', now()), ('BTC-USD', 'DEAD', now()), ('FINE', 'FRESH', now())"))
        _close(conn, "AAA", 1, WED, 9.0)
        _close(conn, "AAA", 1, THU, 10.0)
        _close(conn, "AAA", 1, FRI, 11.0)
        _close(conn, "AAA", 1, FRI, 0.0, status="FAILED")           # a failed pull is not an observation
        _close(conn, "BBB", 2, FRI, 5.5)
        _close(conn, "BBB", 2, THU, 5.4, pulled=datetime(2026, 11, 19, 19, 0, tzinfo=timezone.utc))  # 14:00 NY
        _close(conn, "AMB", 1, FRI, 100.0)
        _close(conn, "AMB", 2, FRI, 50.0)                           # the sources disagree
        _close(conn, "FINE", 1, FRI, 1.0)
        conn.execute(text(
            "INSERT INTO ticker_metrics_daily (ticker, obs_date, shares_outstanding, close_price, market_cap_usd, source) "
            "VALUES ('AAA', :fri, 1000, 10.9, 10900, 'sec_xbrl + yfinance'), "     # sec_xbrl_shares owns it
            "('AAA', :thu, NULL, 9.99, NULL, 'twelvedata_universe_backfill'), "        # an existing close
            "('AAA', :wed, NULL, NULL, NULL, 'placeholder')"),                        # a row with no close
            {"fri": FRI, "thu": THU, "wed": WED})

    receipt = mod.run(pg_engine, "universe", evaluated_at=EVALUATED_AT)

    assert receipt["tickers"] == 5
    assert receipt["eligible_tickers"] == 2 and receipt["eligible_closes"] == 4
    assert receipt["no_completed_close"] == ["AMB", "GLEN"] and receipt["unsupported"] == ["BTC-USD"]
    assert receipt["ambiguous_days_withheld"] == 1 and receipt["failed"] == []
    assert receipt["by_source"] == {"TIINGO": 1, "yfinance": 1}
    rows = _metrics(pg_engine)
    assert rows[("AAA", FRI)] == (10.9, 1000, "sec_xbrl + yfinance")              # untouched
    assert rows[("AAA", THU)] == (9.99, None, "twelvedata_universe_backfill")      # an existing close is kept
    assert rows[("AAA", WED)] == (9.0, None, "stored_close_backfill:TIINGO")      # the missing close is filled
    assert rows[("BBB", FRI)] == (5.5, None, "stored_close_backfill:yfinance")
    assert ("BBB", THU) not in rows                                               # only a provisional price
    assert not any(t in ("OLD", "FINE", "GLEN", "AMB", "BTC-USD") for t, _d in rows)
    assert (receipt["written"], receipt["left_as_is"]) == (2, 2)

    again = mod.run(pg_engine, "universe", evaluated_at=EVALUATED_AT)
    assert again["written"] == 0 and again["left_as_is"] == 4 and _metrics(pg_engine) == rows

    dry = mod.run(pg_engine, "universe", evaluated_at=EVALUATED_AT, dry_run=True)
    assert dry["eligible_tickers"] == 2 and dry["written"] == 0 and _metrics(pg_engine) == rows


def test_basis_conflict_and_unverified_on_postgres(pg_engine):
    """A ticker whose stored closes sit on another basis is withheld whole; one
    with stored closes but no common day is filled and named as unverified."""
    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO data_freshness_audit (ticker, bucket, audited_at) VALUES "
            "('SPL', 'DEAD', now()), ('NOV', 'DEAD', now())"))
        _close(conn, "SPL", 1, THU, 0.30)
        _close(conn, "SPL", 1, FRI, 0.31)
        _close(conn, "NOV", 1, FRI, 7.0)
        conn.execute(text(
            "INSERT INTO ticker_metrics_daily (ticker, obs_date, close_price, source) VALUES "
            "('SPL', :thu, 6.08, 'twelvedata_universe_backfill'), "      # split-adjusted next to a raw close
            "('NOV', :wed, 7.1, 'twelvedata_universe_backfill')"),       # no day in common
            {"thu": THU, "wed": WED})
    receipt = mod.run(pg_engine, "universe", evaluated_at=EVALUATED_AT)
    assert receipt["basis_conflict"] == ["SPL"] and receipt["basis_unverified"] == ["NOV"]
    rows = _metrics(pg_engine)
    assert rows[("SPL", THU)][0] == 6.08 and ("SPL", FRI) not in rows       # withheld whole, nothing written
    assert rows[("NOV", FRI)] == (7.0, None, "stored_close_backfill:TIINGO")


def test_gem_run_on_postgres(pg_engine):
    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO gem_alerts (backtest_ticker, subject_kind, subject_id) VALUES "
            "('FLUT', 'ticker', 'FLUT'), (NULL, 'ticker', 'HAVE'), ('MACRO', 'macro', 'x y'), (NULL, 'ticker', 'NONE')"))
        conn.execute(text("INSERT INTO ticker_metrics_daily (ticker, obs_date, close_price, source) "
                          "VALUES ('HAVE', :d, 3.0, 'sec_xbrl + yfinance')"), {"d": FRI})
        _close(conn, "FLUT", 1, FRI, 250.0)
        _close(conn, "HAVE", 1, FRI, 3.1)

    receipt = mod.run(pg_engine, "gem", evaluated_at=EVALUATED_AT)

    assert receipt["tickers"] == 1 and receipt["eligible_tickers"] == 1
    assert _metrics(pg_engine)[("FLUT", FRI)] == (250.0, None, "stored_close_backfill:TIINGO")
