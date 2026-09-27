"""Regression tests for migration #671 (``pull_status = 'QUARANTINED'``).

Migration #671 adds a ``QUARANTINED`` ``pull_status`` for wrong-instrument
price batches caught after the fact — a row that was inserted as ``SUCCESS``
(so it *looks* like real data) but is later found to belong to the wrong
instrument and gets its status flipped. Every reader fixed by
``fix/raw-series-readers-success-only-20260927`` filters
``pull_status = 'SUCCESS'``, which excludes ``QUARANTINED`` automatically —
these tests prove that for the sanctioned reader (``store/observations.py``),
for a representative fixed raw-SQL analytical read (the pattern used across
``analysis/``, ``intelligence/`` and ``valuation/`` — see
``tests/test_raw_series_read_guard.py``'s updated baseline), and for the
writer-side existence checks that now treat a ``QUARANTINED`` row as absent
rather than as already-pulled data.

2026-09-27 (branch fix/pre-retraction-and-679-followups-20260927, #679
review follow-up): the writer-side existence checks below used to be
hand-copied SQL string constants (``FIXED_ROW_EXISTS``) asserted against
directly — they proved the *query text* was right, not that the real code
path used it. They are now real calls into
``ingestion/base.py::BasePuller._row_exists`` and into the three scripts
that carry their own independent "already pulled" existence-set query
(the ``_resolve_source_id()``/``_row_exists()``-style copy-paste this
codebase already has, per CLAUDE.md's gotchas list):
``scripts/bulk_historical_pull.py::_bulk_insert``,
``scripts/fill_missing_features.py::_insert_computed``, and
``scripts/fix_spy_data.py::main`` (mocked network + engine + API key, since
``main`` isn't refactored into a smaller seam).

Uses the same in-memory SQLite fixture shape as
``tests/test_store_observations.py`` — no live Postgres needed, and no
dependency on the ``pull_status`` CHECK constraint (SQLite does not enforce
it), so this test is valid before migration #671 itself ships.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    text,
)

from ingestion.base import BasePuller
from scripts import bulk_historical_pull, fill_missing_features, fix_spy_data
from store import observations as obs

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

T0 = datetime(2026, 9, 20, 6, 0, 0)
FRED_SRC = 1

SERIES = "YF:WRONGCO:close"


@pytest.fixture()
def conn():
    engine = create_engine("sqlite://")
    md = MetaData()
    source_catalog = Table(
        "source_catalog", md,
        Column("id", Integer, primary_key=True),
        Column("name", String, nullable=False),
    )
    raw = Table(
        "raw_series", md,
        Column("series_id", String, nullable=False),
        Column("source_id", Integer, ForeignKey("source_catalog.id"), nullable=False),
        Column("obs_date", Date, nullable=False),
        Column("pull_timestamp", DateTime, nullable=False),
        Column("value", Float, nullable=False),
        Column("raw_payload", Text),
        Column("pull_status", String, nullable=False),
    )
    md.create_all(engine)

    def row(sid, d, v, status, ts_offset_h, source_id=FRED_SRC):
        return {
            "series_id": sid, "source_id": source_id, "obs_date": d,
            "pull_timestamp": T0 + timedelta(hours=ts_offset_h),
            "value": v, "raw_payload": "{}", "pull_status": status,
        }

    rows = [
        # A real, accepted observation.
        row(SERIES, date(2026, 9, 18), 42.0, "SUCCESS", ts_offset_h=0),
        # A failed pull's zero marker, dated after the good row.
        row(SERIES, date(2026, 9, 19), 0.0, "FAILED", ts_offset_h=10),
        # migration #671: a wrong-instrument batch caught after the fact —
        # inserted as SUCCESS-looking data with a real-looking nonzero
        # value, then flipped to QUARANTINED. Newest row for this series.
        row(SERIES, date(2026, 9, 20), 999.0, "QUARANTINED", ts_offset_h=20),
    ]
    with engine.begin() as c:
        c.execute(source_catalog.insert(), [{"id": FRED_SRC, "name": "yfinance"}])
        c.execute(raw.insert(), rows)
    with engine.connect() as c:
        yield c


def test_observations_reader_never_returns_quarantined_or_failed(conn):
    """The sanctioned reader (store/observations.py) already excludes non-SUCCESS."""
    o = obs.read_latest(conn, SERIES)
    assert o is not None
    assert o.value == 42.0
    assert o.obs_date == date(2026, 9, 18)

    window = obs.read_window(conn, SERIES)
    assert [w.value for w in window] == [42.0]

    n = obs.read_latest_n(conn, SERIES, 5)
    assert [x.value for x in n] == [42.0]


# The pattern applied by fix/raw-series-readers-success-only-20260927 across
# analysis/capital_flows.py, intelligence/*, valuation/* etc.: a plain
# "latest observation" read with an explicit pull_status = 'SUCCESS' filter.
# This is a deliberately representative, hand-written stand-in for that
# repeated inline pattern (there is no single function to call for it) —
# tests/test_raw_series_read_guard.py is the real, codebase-wide guard.
FIXED_ANALYTICAL_READ = text(
    "SELECT value, obs_date FROM raw_series "
    "WHERE series_id = :sid AND pull_status = 'SUCCESS' "
    "ORDER BY obs_date DESC LIMIT 1"
)

# What the same read looked like before this fix (no status filter) — kept
# here to demonstrate the defect the fix closes, mirroring
# tests/test_store_observations.py's "legacy" comparisons.
LEGACY_ANALYTICAL_READ = text(
    "SELECT value, obs_date FROM raw_series "
    "WHERE series_id = :sid "
    "ORDER BY obs_date DESC LIMIT 1"
)


def test_legacy_unfiltered_read_would_have_returned_the_quarantined_row(conn):
    """The defect: without a status filter, the newest row wins even when QUARANTINED."""
    row = conn.execute(LEGACY_ANALYTICAL_READ, {"sid": SERIES}).fetchone()
    assert row is not None
    assert row[0] == 999.0  # the quarantined, wrong-instrument value


def test_fixed_analytical_read_skips_quarantined_and_failed(conn):
    """A representative fixed reader returns the real SUCCESS observation, never
    the newer QUARANTINED or FAILED rows."""
    row = conn.execute(FIXED_ANALYTICAL_READ, {"sid": SERIES}).fetchone()
    assert row is not None
    assert row[0] == 42.0
    assert str(row[1])[:10] == "2026-09-18"


# ── ingestion/base.py::BasePuller._row_exists (real call, not a copied SQL
# string) ────────────────────────────────────────────────────────────────
#
# _row_exists computes its dedup cutoff from datetime.now(timezone.utc)
# internally (it is not an injectable clock), so this fixture anchors
# pull_timestamp to the real wall clock at test time rather than to a fixed
# 2026-09-20 date like the ``conn`` fixture above.


@pytest.fixture()
def row_exists_engine():
    now = datetime.now(timezone.utc)
    engine = create_engine("sqlite://")
    md = MetaData()
    source_catalog = Table(
        "source_catalog", md,
        Column("id", Integer, primary_key=True),
        Column("name", String, nullable=False),
    )
    raw = Table(
        "raw_series", md,
        Column("series_id", String, nullable=False),
        Column("source_id", Integer, ForeignKey("source_catalog.id"), nullable=False),
        Column("obs_date", Date, nullable=False),
        Column("pull_timestamp", DateTime, nullable=False),
        Column("value", Float, nullable=False),
        Column("raw_payload", Text),
        Column("pull_status", String, nullable=False),
    )
    md.create_all(engine)

    def row(d, v, status, ts_offset_h):
        return {
            "series_id": SERIES, "source_id": FRED_SRC, "obs_date": d,
            "pull_timestamp": now + timedelta(hours=ts_offset_h),
            "value": v, "raw_payload": "{}", "pull_status": status,
        }

    rows = [
        row(date(2026, 9, 18), 42.0, "SUCCESS", ts_offset_h=-3),
        row(date(2026, 9, 19), 0.0, "FAILED", ts_offset_h=-2),
        # A wrong-instrument batch caught after the fact, flipped to
        # QUARANTINED. Newest row for this series.
        row(date(2026, 9, 20), 999.0, "QUARANTINED", ts_offset_h=-1),
    ]
    with engine.begin() as c:
        c.execute(source_catalog.insert(), [{"id": FRED_SRC, "name": "yfinance"}])
        c.execute(raw.insert(), rows)
    yield engine
    engine.dispose()


@pytest.fixture()
def row_exists_puller(row_exists_engine):
    puller = BasePuller(row_exists_engine)  # SOURCE_NAME="" skips DB source resolution
    puller.source_id = FRED_SRC
    return puller


def test_row_exists_does_not_count_a_quarantined_row_as_present(
    row_exists_engine, row_exists_puller,
):
    with row_exists_engine.connect() as c:
        # The only row at (SERIES, 2026-09-20) is the QUARANTINED one, so
        # despite a row existing at that (series_id, obs_date), the real
        # _row_exists must report "not present" and let a re-pull proceed.
        exists = row_exists_puller._row_exists(
            SERIES, date(2026, 9, 20), c, dedup_hours=24,
        )
    assert exists is False


def test_row_exists_still_reports_a_success_row_as_present(
    row_exists_engine, row_exists_puller,
):
    with row_exists_engine.connect() as c:
        exists = row_exists_puller._row_exists(
            SERIES, date(2026, 9, 18), c, dedup_hours=24,
        )
    assert exists is True


def test_row_exists_still_reports_a_failed_row_as_present(
    row_exists_engine, row_exists_puller,
):
    """FAILED intentionally still counts as existing (rate-limits retries
    against a failing upstream API) — only QUARANTINED is excluded."""
    with row_exists_engine.connect() as c:
        exists = row_exists_puller._row_exists(
            SERIES, date(2026, 9, 19), c, dedup_hours=24,
        )
    assert exists is True


# ── The three scripts' own "already pulled" existence sets (real calls) ───
#
# fix/raw-series-readers-success-only-20260927 added
# "AND pull_status != 'QUARANTINED'" to an identical
# "SELECT DISTINCT obs_date FROM raw_series WHERE series_id = ... AND
# source_id = ..." existence-set query duplicated across three scripts:
# bulk_historical_pull.py, fill_missing_features.py (three call sites
# sharing the one pattern) and fix_spy_data.py. Each test below calls the
# real function rather than re-typing that SQL.


@pytest.fixture()
def scripts_engine():
    engine = create_engine("sqlite://")
    with engine.begin() as c:
        c.execute(text(
            "CREATE TABLE source_catalog ("
            "id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL, base_url TEXT, "
            "cost_tier TEXT, latency_class TEXT, pit_available BOOLEAN, "
            "revision_behavior TEXT, trust_score TEXT, priority_rank INTEGER, "
            "active BOOLEAN)"
        ))
        c.execute(text(
            "CREATE TABLE raw_series ("
            "series_id TEXT NOT NULL, source_id INTEGER NOT NULL, "
            "obs_date DATE NOT NULL, "
            "pull_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
            "value REAL NOT NULL, raw_payload TEXT, pull_status TEXT NOT NULL)"
        ))
    yield engine
    engine.dispose()


def test_bulk_historical_pull_bulk_insert_repulls_a_quarantined_date(scripts_engine):
    """scripts/bulk_historical_pull.py::_bulk_insert's existence set must not
    treat a QUARANTINED row as already-pulled data."""
    with scripts_engine.begin() as c:
        c.execute(text("INSERT INTO source_catalog (id, name) VALUES (1, 'cboe')"))
        c.execute(text(
            "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) "
            "VALUES ('VIX', 1, '2026-09-20', 999.0, 'QUARANTINED')"
        ))

    inserted = bulk_historical_pull._bulk_insert(
        scripts_engine, source_id=1, series_id="VIX",
        data=[(date(2026, 9, 20), 15.5)],
    )
    assert inserted == 1

    with scripts_engine.connect() as c:
        rows = c.execute(text(
            "SELECT pull_status, value FROM raw_series "
            "WHERE series_id = 'VIX' AND obs_date = '2026-09-20'"
        )).fetchall()
    by_status = {r[0]: r[1] for r in rows}
    # Both rows now coexist: the quarantined one is untouched, and the
    # re-pull went in as a fresh SUCCESS row rather than being skipped.
    assert by_status == {"QUARANTINED": 999.0, "SUCCESS": 15.5}


def test_fill_missing_features_insert_computed_repulls_a_quarantined_date(
    scripts_engine, monkeypatch,
):
    """scripts/fill_missing_features.py::_insert_computed's existence set
    (shared verbatim by pull_fred_extended and pull_yfinance_extended) must
    not treat a QUARANTINED row as already-pulled data."""
    monkeypatch.setattr(
        fill_missing_features, "_ensure_source", lambda engine, name, config: 1,
    )
    with scripts_engine.begin() as c:
        c.execute(text(
            "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) "
            "VALUES ('COMPUTED:spy_macd', 1, '2026-09-20', 999.0, 'QUARANTINED')"
        ))

    results: list = []
    series = pd.Series({date(2026, 9, 20): 42.0})
    fill_missing_features._insert_computed(scripts_engine, "spy_macd", series, results)

    assert results == [{"feature": "spy_macd", "rows": 1, "status": "OK"}]
    with scripts_engine.connect() as c:
        rows = c.execute(text(
            "SELECT pull_status, value FROM raw_series "
            "WHERE series_id = 'COMPUTED:spy_macd' AND obs_date = '2026-09-20'"
        )).fetchall()
    by_status = {r[0]: r[1] for r in rows}
    assert by_status == {"QUARANTINED": 999.0, "SUCCESS": 42.0}


def test_fix_spy_data_main_repulls_a_quarantined_date(scripts_engine, monkeypatch):
    """scripts/fix_spy_data.py::main's existence set must not treat a
    QUARANTINED row as already-pulled data.

    main() isn't factored into a smaller DB-only seam, so this mocks the
    three things that would otherwise need real infrastructure: the
    AlphaVantage API key, the network call, and the engine it connects to
    (get_engine) -- the existence-set query and the insert it guards run
    for real against ``scripts_engine``.
    """
    monkeypatch.setattr(fix_spy_data.settings, "ALPHAVANTAGE_API_KEY", "test-key")
    monkeypatch.setattr(fix_spy_data, "get_engine", lambda: scripts_engine)
    monkeypatch.setattr(fix_spy_data, "PRIORITY_TICKERS", ["SPY"])
    monkeypatch.setattr(fix_spy_data.time, "sleep", lambda *_a, **_k: None)

    csv_body = (
        "timestamp,open,high,low,close,volume\n"
        "2026-09-20,600.0,601.0,599.0,600.5,1000000\n"
    )

    class _FakeResponse:
        text = csv_body

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr(
        fix_spy_data.requests, "get", lambda *a, **k: _FakeResponse(),
    )

    with scripts_engine.begin() as c:
        c.execute(text(
            "INSERT INTO source_catalog (id, name) VALUES (1, 'alphavantage_daily')"
        ))
        c.execute(text(
            "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) "
            "VALUES ('av:daily:SPY', 1, '2026-09-20', 999.0, 'QUARANTINED')"
        ))

    fix_spy_data.main()

    with scripts_engine.connect() as c:
        rows = c.execute(text(
            "SELECT pull_status, value FROM raw_series "
            "WHERE series_id = 'av:daily:SPY' AND obs_date = '2026-09-20'"
        )).fetchall()
    by_status = {r[0]: r[1] for r in rows}
    assert by_status == {"QUARANTINED": 999.0, "SUCCESS": 600.5}
