"""scripts/backfill_resolved_series_from_ticker_metrics.sql stamps the fetch day, not obs_date (E1-V7c).

A bridged row becomes knowable when GRID fetched it into
``ticker_metrics_daily`` (``as_of``). Until 2026-11-21 the bridge wrote
``release_date = obs_date``, so a release-only point-in-time read served each
row up to ~60 days before GRID held it. These tests run the real file against
a scratch schema (session time zone deliberately not UTC) and check the
stamps and what ``store.pit.PITStore`` then serves. Skips without PostgreSQL
(``GRID_TEST_DB_URL``).

The bridge reads only TwelveData rows (``ticker_metrics_daily.source`` exactly
``twelvedata_backfill`` or ``twelvedata_universe_backfill``) and stamps each with
the TWELVEDATA ``source_catalog`` id; the mixed-source tests check that the
yfinance copy, NULL and every other label never land (Codex hold on #1372).
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


# ── Source filter + provenance (Codex hold on #1372 head 7cadfd78) ──────────

MIXED_FEATURES = ("aapl_full", "msft_full", "nvda_full", "xyz_full", "wxy_full")
MIXED = (
    # (ticker, obs_date, close, source) -- all fetched 2026-05-17 14:00 UTC
    ("AAPL", date(2026, 4, 1), 101.0, "twelvedata_backfill"),
    ("AAPL", date(2026, 4, 2), 202.0, "sec_xbrl + yfinance"),           # newer-source gap day
    ("AAPL", date(2026, 4, 3), 303.0, None),                            # no source recorded
    ("AAPL", date(2026, 4, 6), 106.0, "twelvedata_universe_backfill"),  # lands past the gap
    ("MSFT", date(2026, 4, 1), 401.0, "twelvedata_universe_backfill"),
    ("NVDA", date(2026, 4, 1), 18.0, "sec_xbrl + yfinance"),            # unadjusted YF close
    ("XYZ", date(2026, 4, 1), 5.0, None),
    ("WXY", date(2026, 4, 1), 7.0, "TwelveData"),                       # not the stored label
    ("WXY", date(2026, 4, 2), 7.5, "stored_close_backfill:yfinance"),
)
MIXED_AS_OF = datetime(2026, 5, 17, 14, 0, tzinfo=timezone.utc)


@pytest.fixture()
def mixed(pg_engine: Engine):
    schema = f"tmd_bridge_mix_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        with engine.begin() as conn:
            for ddl in _DDL:
                conn.execute(text(ddl))
            # Other catalog rows first, so a wrong id cannot pass by being 1.
            for name in ("yfinance", "TWELVEDATA_STATS"):
                conn.execute(text("INSERT INTO source_catalog (name) VALUES (:n)"), {"n": name})
            fids = {}
            for name in MIXED_FEATURES:
                fids[name] = conn.execute(
                    text("INSERT INTO feature_registry (name) VALUES (:n) RETURNING id"), {"n": name}).scalar_one()
                conn.execute(
                    text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value, "
                         "source_priority_used) VALUES (:f, :d, :d, :d, 1.0, 1)"),
                    {"f": fids[name], "d": EXISTING},
                )
            for ticker, od, close, source in MIXED:
                conn.execute(
                    text("INSERT INTO ticker_metrics_daily (ticker, obs_date, close_price, source, as_of) "
                         "VALUES (:t, :od, :c, :s, :a)"),
                    {"t": ticker, "od": od, "c": close, "s": source, "a": MIXED_AS_OF},
                )
        _run_file(engine)
        yield engine, fids
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _bridged_rows(engine: Engine) -> set[tuple[str, date, float, str]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT fr.name, rs.obs_date, rs.value, sc.name FROM resolved_series rs "
                 "JOIN feature_registry fr ON fr.id = rs.feature_id "
                 "LEFT JOIN source_catalog sc ON sc.id = rs.source_priority_used "
                 "WHERE rs.obs_date > :e"),
            {"e": EXISTING},
        ).fetchall()
    return {(r[0], r[1], r[2], r[3]) for r in rows}


def test_only_twelvedata_rows_land_with_the_twelvedata_source_id(mixed):
    engine, _fids = mixed
    assert _bridged_rows(engine) == {
        ("aapl_full", date(2026, 4, 1), 101.0, "TWELVEDATA"),
        ("aapl_full", date(2026, 4, 6), 106.0, "TWELVEDATA"),
        ("msft_full", date(2026, 4, 1), 401.0, "TWELVEDATA"),
    }


def test_excluded_sources_leave_their_features_untouched(mixed):
    engine, fids = mixed
    with engine.connect() as conn:
        for name in ("nvda_full", "xyz_full", "wxy_full"):
            n = conn.execute(text("SELECT count(*) FROM resolved_series WHERE feature_id = :f AND obs_date > :e"),
                             {"f": fids[name], "e": EXISTING}).scalar_one()
            assert n == 0, name


def test_mixed_source_rerun_is_idempotent(mixed):
    engine, _fids = mixed
    first = _bridged_rows(engine)
    _run_file(engine)
    assert _bridged_rows(engine) == first
