"""scripts/backfill_resolved_series_from_ticker_metrics.sql stamps the fetch day, not obs_date (E1-V7c).

A bridged row becomes knowable when GRID fetched it into
``ticker_metrics_daily`` (``as_of``). Until 2026-11-21 the bridge wrote
``release_date = obs_date``, so a release-only point-in-time read served each
row up to ~60 days before GRID held it. These tests run the real file against
a scratch schema (session time zone deliberately not UTC) and check the
stamps and what ``store.pit.PITStore`` then serves. Skips without PostgreSQL
(``GRID_TEST_DB_URL``).
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from store.pit import PITStore

SQL_FILE = Path(__file__).resolve().parent.parent / "scripts" / "backfill_resolved_series_from_ticker_metrics.sql"

_DDL = (
    """CREATE TABLE source_catalog (
           id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE, base_url TEXT, cost_tier TEXT,
           latency_class TEXT, pit_available BOOLEAN, revision_behavior TEXT, trust_score TEXT,
           priority_rank INTEGER, active BOOLEAN)""",
    "CREATE TABLE feature_registry (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
    """CREATE TABLE resolved_series (
           id BIGSERIAL PRIMARY KEY, feature_id INTEGER NOT NULL REFERENCES feature_registry(id),
           obs_date DATE NOT NULL, release_date DATE NOT NULL, vintage_date DATE NOT NULL,
           value DOUBLE PRECISION NOT NULL, source_priority_used INTEGER NOT NULL,
           conflict_flag BOOLEAN NOT NULL DEFAULT FALSE, resolution_version INTEGER NOT NULL DEFAULT 1)""",
    "CREATE UNIQUE INDEX uq_resolved_series_composite ON resolved_series (feature_id, obs_date, vintage_date)",
    """CREATE TABLE resolved_series_retractions (
           id BIGSERIAL PRIMARY KEY, feature_id INTEGER NOT NULL, obs_date DATE NOT NULL,
           vintage_date DATE NOT NULL, retracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
           reason TEXT NOT NULL, run_tag TEXT NOT NULL, UNIQUE (feature_id, obs_date, vintage_date))""",
    # migrations/0025_ticker_metrics_daily.sql
    """CREATE TABLE ticker_metrics_daily (
           id SERIAL PRIMARY KEY, ticker TEXT NOT NULL, obs_date DATE NOT NULL,
           shares_outstanding BIGINT, close_price NUMERIC, market_cap_usd NUMERIC, source TEXT,
           as_of TIMESTAMPTZ DEFAULT NOW(), UNIQUE (ticker, obs_date))""",
)

EXISTING = date(2026, 3, 31)  # newest resolved obs_date before the bridge runs
TMD = (
    # (obs_date, close, as_of)
    (date(2026, 3, 31), 99.0, datetime(2026, 5, 17, 14, 0, tzinfo=timezone.utc)),   # not newer: skipped
    (date(2026, 4, 1), 101.0, datetime(2026, 5, 17, 14, 0, tzinfo=timezone.utc)),
    (date(2026, 4, 2), 102.0, datetime(2026, 5, 18, 3, 30, tzinfo=timezone.utc)),   # 05-17 23:30 in New York
    (date(2026, 4, 3), 103.0, None),                                                # fetch time unknown
    (date(2026, 4, 20), 120.0, datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc)),  # as_of before obs (bad clock)
)


@pytest.fixture()
def bridged(pg_engine: Engine):
    schema = f"tmd_bridge_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(
        pg_engine.url, connect_args={"options": f"-csearch_path={schema} -ctimezone=America/New_York"},
    )
    try:
        with engine.begin() as conn:
            for ddl in _DDL:
                conn.execute(text(ddl))
            fid = conn.execute(
                text("INSERT INTO feature_registry (name) VALUES ('aapl_full') RETURNING id")).scalar_one()
            conn.execute(
                text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value, "
                     "source_priority_used) VALUES (:f, :d, :d, :d, 98.0, 1)"),
                {"f": fid, "d": EXISTING},
            )
            for od, close, as_of in TMD:
                conn.execute(
                    text("INSERT INTO ticker_metrics_daily (ticker, obs_date, close_price, source, as_of) "
                         "VALUES ('AAPL', :od, :c, 'twelvedata_backfill', :a)"),
                    {"od": od, "c": close, "a": as_of},
                )
        _run_file(engine)
        yield engine, fid
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _run_file(engine: Engine) -> None:
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            cur.execute(SQL_FILE.read_text(encoding="utf-8"))
        raw.commit()
    finally:
        raw.close()


def _stamps(engine: Engine, fid: int) -> dict[date, tuple[date, date, float]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT obs_date, release_date, vintage_date, value FROM resolved_series "
                 "WHERE feature_id = :f AND obs_date > :e ORDER BY obs_date"),
            {"f": fid, "e": EXISTING},
        ).fetchall()
    return {r[0]: (r[1], r[2], r[3]) for r in rows}


def _utc_today(engine: Engine) -> date:
    with engine.connect() as conn:
        return conn.execute(text("SELECT (now() AT TIME ZONE 'UTC')::date")).scalar_one()


def test_release_and_vintage_are_the_utc_fetch_day(bridged):
    engine, fid = bridged
    today = _utc_today(engine)
    assert _stamps(engine, fid) == {
        date(2026, 4, 1): (date(2026, 5, 17), date(2026, 5, 17), 101.0),
        date(2026, 4, 2): (date(2026, 5, 18), date(2026, 5, 18), 102.0),  # UTC day, not New York's 05-17
        date(2026, 4, 3): (today, today, 103.0),                            # no as_of: the day it is bridged
        date(2026, 4, 20): (date(2026, 4, 20), date(2026, 4, 20), 120.0),   # never before obs_date
    }
    with engine.connect() as conn:
        src = conn.execute(text("SELECT DISTINCT source_priority_used FROM resolved_series "
                                "WHERE obs_date > :e"), {"e": EXISTING}).scalars().all()
        td = conn.execute(text("SELECT id FROM source_catalog WHERE name = 'TWELVEDATA'")).scalar_one()
    assert src == [td]


def test_no_bridged_row_claims_release_before_it_was_held(bridged):
    engine, fid = bridged
    with engine.connect() as conn:
        early = conn.execute(text("SELECT count(*) FROM resolved_series WHERE release_date < vintage_date")).scalar_one()
    assert early == 0


def test_pit_reads_before_the_fetch_day_do_not_see_bridged_rows(bridged):
    """Holds on main's release-only store/pit.py too: the stamps themselves are honest now."""
    engine, fid = bridged
    pit = PITStore(engine)
    before = pit.get_pit([fid], date(2026, 4, 15), "LATEST_AS_OF", start_date=date(2026, 3, 1))
    assert list(before["obs_date"]) == [EXISTING]
    after = pit.get_pit([fid], date(2026, 5, 18), "LATEST_AS_OF", start_date=date(2026, 3, 1))
    assert list(after["obs_date"]) == [EXISTING, date(2026, 4, 1), date(2026, 4, 2), date(2026, 4, 20)]


def test_rerun_is_idempotent(bridged):
    engine, fid = bridged
    first = _stamps(engine, fid)
    _run_file(engine)
    assert _stamps(engine, fid) == first
