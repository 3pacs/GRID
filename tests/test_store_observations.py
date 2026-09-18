"""Executable regression tests for ``store/observations.py``.

These run the real SQL against an in-memory SQLite ``raw_series`` shaped like
production (same columns, same uniqueness), so they demonstrate the defect
rather than mock it: the legacy ``ORDER BY obs_date DESC LIMIT 1`` read
returns a FAILED marker's ``0`` dated today, while the status-aware reader
returns the newest accepted observation.

Fixture shape mirrors what was measured on griddb on 2026-09-17: FRED's
puller writes ``(value=0, pull_status='FAILED', obs_date=date.today())`` on
any failure, successful re-pulls append a second vintage for the same
``obs_date``, and a weekly series' newest real observation can be a day
older than its newest FAILED marker.
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

TODAY = date(2026, 9, 17)
T0 = datetime(2026, 9, 10, 6, 0, 0)


@pytest.fixture()
def conn():
    engine = create_engine("sqlite://")
    md = MetaData()
    raw = Table(
        "raw_series", md,
        Column("series_id", String, nullable=False),
        Column("source_id", String, nullable=False),
        Column("obs_date", Date, nullable=False),
        Column("pull_timestamp", DateTime, nullable=False),
        Column("value", Float, nullable=False),
        Column("raw_payload", Text),
        Column("pull_status", String, nullable=False),
    )
    md.create_all(engine)

    def row(sid, d, v, status="SUCCESS", ts_offset_h=0):
        return {
            "series_id": sid, "source_id": "fred", "obs_date": d,
            "pull_timestamp": T0 + timedelta(hours=ts_offset_h),
            "value": v, "raw_payload": "{}", "pull_status": status,
        }

    rows = [
        # Weekly series: real observations Wednesdays, plus a FAILED zero
        # marker dated the pull day (today) — exactly the WALCL/T10Y2Y case.
        row("WALCL", date(2026, 9, 2), 6_700_000.0, ts_offset_h=0),
        row("WALCL", date(2026, 9, 9), 6_720_000.0, ts_offset_h=24),
        row("WALCL", date(2026, 9, 16), 6_746_548.0, ts_offset_h=48),
        row("WALCL", TODAY, 0.0, status="FAILED", ts_offset_h=72),
        # Daily series with two vintages for one date (revision re-pull) and
        # a FAILED zero on the same date as its newest real value.
        row("RRPONTSYD", date(2026, 9, 15), 0.30, ts_offset_h=0),
        row("RRPONTSYD", date(2026, 9, 16), 0.28, ts_offset_h=10),   # first vintage
        row("RRPONTSYD", date(2026, 9, 16), 0.276, ts_offset_h=20),  # revision
        row("RRPONTSYD", date(2026, 9, 16), 0.0, status="FAILED", ts_offset_h=21),
        # PARTIAL is not an accepted observation either.
        row("RRPONTSYD", TODAY, 0.5, status="PARTIAL", ts_offset_h=30),
    ]
    with engine.begin() as c:
        c.execute(raw.insert(), rows)
    with engine.connect() as c:
        yield c


LEGACY_LATEST = text(
    "SELECT value FROM raw_series WHERE series_id = :sid ORDER BY obs_date DESC LIMIT 1"
)
LEGACY_WINDOW = text(
    "SELECT obs_date, value FROM raw_series WHERE series_id = :sid "
    "AND value IS NOT NULL ORDER BY obs_date ASC"
)


def test_legacy_latest_read_returns_the_failed_zero(conn):
    """The defect: a plain latest-by-obs_date read yields 0 on a failed pull day."""
    assert conn.execute(LEGACY_LATEST, {"sid": "WALCL"}).scalar() == 0.0


def test_read_latest_skips_failed_marker_and_returns_newest_accepted(conn):
    o = obs.read_latest(conn, "WALCL")
    assert o is not None
    assert o.value == 6_746_548.0
    assert o.obs_date == date(2026, 9, 16)
    assert o.pull_timestamp == T0 + timedelta(hours=48)


def test_legacy_window_read_includes_the_failed_zero_and_duplicate_vintages(conn):
    """The defect for windowed readers (FCI, liquidity regime, CoT extremes)."""
    legacy = conn.execute(LEGACY_WINDOW, {"sid": "RRPONTSYD"}).fetchall()
    vals = [float(v) for _, v in legacy]
    assert 0.0 in vals                       # FAILED marker leaks in
    assert vals.count(0.28) == 1 and vals.count(0.276) == 1  # both vintages leak in
    assert 0.5 in vals                       # PARTIAL leaks in


def test_read_window_collapses_vintages_and_drops_failed_and_partial(conn):
    got = obs.read_window(conn, "RRPONTSYD")
    assert [(o.obs_date, o.value) for o in got] == [
        (date(2026, 9, 15), 0.30),
        (date(2026, 9, 16), 0.276),   # latest vintage wins, deterministically
    ]


def test_read_window_start_and_as_of_bounds(conn):
    got = obs.read_window(conn, "WALCL", start=date(2026, 9, 9), as_of=date(2026, 9, 9))
    assert [o.value for o in got] == [6_720_000.0]


def test_point_in_time_excludes_rows_pulled_after_as_of_ts(conn):
    """A revision pulled after the decision instant must not be visible."""
    cutoff = T0 + timedelta(hours=15)  # after the first RRP vintage, before the revision
    o = obs.read_latest(conn, "RRPONTSYD", as_of_ts=cutoff)
    assert o is not None and o.value == 0.28 and o.obs_date == date(2026, 9, 16)
    window = obs.read_window(conn, "RRPONTSYD", as_of_ts=cutoff)
    assert [o.value for o in window] == [0.30, 0.28]


def test_read_latest_n_counts_distinct_observation_dates(conn):
    got = obs.read_latest_n(conn, "WALCL", 2)
    assert [o.obs_date for o in got] == [date(2026, 9, 16), date(2026, 9, 9)]
    got = obs.read_latest_n(conn, "RRPONTSYD", 5)
    assert [(o.obs_date, o.value) for o in got] == [
        (date(2026, 9, 16), 0.276), (date(2026, 9, 15), 0.30),
    ]


def test_missing_series_is_none_or_empty_never_zero(conn):
    assert obs.read_latest(conn, "NOPE") is None
    assert obs.read_window(conn, "NOPE") == []
    assert obs.read_latest_n(conn, "NOPE", 3) == []


def test_observation_carries_provenance():
    o = obs.Observation("X", date(2026, 9, 1), 1.0, datetime(2026, 9, 1, 12))
    assert o.series_id == "X" and o.pull_timestamp is not None
    assert o.age_days >= 0
