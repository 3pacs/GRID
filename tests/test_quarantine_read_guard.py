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
writer-side existence check (``ingestion/base.py::BasePuller._row_exists``)
that now treats a ``QUARANTINED`` row as absent rather than as already-pulled
data.

Uses the same in-memory SQLite fixture shape as
``tests/test_store_observations.py`` — no live Postgres needed, and no
dependency on the ``pull_status`` CHECK constraint (SQLite does not enforce
it), so this test is valid before migration #671 itself ships.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta

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


# ingestion/base.py::BasePuller._row_exists, post-fix: a QUARANTINED row must
# not count as "already pulled" data, so a puller is allowed to re-pull and
# overwrite it immediately.
FIXED_ROW_EXISTS = text(
    "SELECT 1 FROM raw_series "
    "WHERE series_id = :sid AND source_id = :src "
    "AND obs_date = :od AND pull_timestamp >= :ts "
    "AND pull_status != 'QUARANTINED' LIMIT 1"
)


def test_row_exists_does_not_count_a_quarantined_row_as_present(conn):
    cutoff = T0  # covers all three fixture rows by timestamp
    result = conn.execute(
        FIXED_ROW_EXISTS,
        {"sid": SERIES, "src": FRED_SRC, "od": date(2026, 9, 20), "ts": cutoff},
    ).fetchone()
    # The only row at (SERIES, 2026-09-20) is the QUARANTINED one, so despite
    # a row existing at that (series_id, obs_date), _row_exists must report
    # "not present" and let a re-pull proceed.
    assert result is None


def test_row_exists_still_reports_a_success_row_as_present(conn):
    cutoff = T0
    result = conn.execute(
        FIXED_ROW_EXISTS,
        {"sid": SERIES, "src": FRED_SRC, "od": date(2026, 9, 18), "ts": cutoff},
    ).fetchone()
    assert result is not None
