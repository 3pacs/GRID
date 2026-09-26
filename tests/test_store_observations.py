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

``source_catalog`` and a real ``source_id`` FK are included (unlike the
pre-#561 fixture) because the reader now joins it for every read: to detect
a mixed-source series_id (``YF:{ticker}:close`` written by both ``yfinance``
and ``tiingo`` in production — see ``store/observations.py``'s module
docstring) and to stamp provenance on every returned ``Observation``.
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

TODAY = date(2026, 9, 17)
T0 = datetime(2026, 9, 10, 6, 0, 0)

FRED_SRC = 1
TIINGO_SRC = 2


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

    def row(sid, d, v, status="SUCCESS", ts_offset_h=0, source_id=FRED_SRC):
        return {
            "series_id": sid, "source_id": source_id, "obs_date": d,
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
        # Mixed-source series_id: yfinance (FRED_SRC standing in for it here;
        # only the id differs from TIINGO_SRC) and tiingo both write
        # "YF:AAPL:close" — the #561 contamination scenario.
        row("YF:AAPL:close", date(2026, 9, 15), 227.5, ts_offset_h=0, source_id=FRED_SRC),
        row("YF:AAPL:close", date(2026, 9, 16), 229.0, ts_offset_h=10, source_id=TIINGO_SRC),
    ]
    with engine.begin() as c:
        c.execute(source_catalog.insert(), [
            {"id": FRED_SRC, "name": "fred"},
            {"id": TIINGO_SRC, "name": "tiingo"},
        ])
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
    assert o.source is None  # positional/legacy construction still works


def test_single_source_read_is_unaffected_and_carries_its_source(conn):
    """A series with exactly one contributing source behaves exactly as before."""
    o = obs.read_latest(conn, "WALCL")
    assert o is not None and o.value == 6_746_548.0 and o.source == "fred"
    window = obs.read_window(conn, "RRPONTSYD")
    assert all(o.source == "fred" for o in window)
    n = obs.read_latest_n(conn, "WALCL", 2)
    assert all(o.source == "fred" for o in n)


def test_mixed_source_read_fails_closed_without_explicit_source(conn):
    """YF:AAPL:close has rows from both fred (standing in for yfinance) and
    tiingo in the fixture — the #561 contamination scenario. Without an
    explicit ``source=``, every reader must refuse rather than silently pick
    whichever source's pull sorts first."""
    with pytest.raises(obs.MixedSourceError):
        obs.read_latest(conn, "YF:AAPL:close")
    with pytest.raises(obs.MixedSourceError):
        obs.read_window(conn, "YF:AAPL:close")
    with pytest.raises(obs.MixedSourceError):
        obs.read_latest_n(conn, "YF:AAPL:close", 5)


def test_explicit_source_disambiguates_a_mixed_series_id(conn):
    """Passing ``source=`` picks exactly that puller's rows, deterministically."""
    fred_latest = obs.read_latest(conn, "YF:AAPL:close", source="fred")
    assert fred_latest is not None
    assert (fred_latest.obs_date, fred_latest.value, fred_latest.source) == (
        date(2026, 9, 15), 227.5, "fred",
    )

    tiingo_latest = obs.read_latest(conn, "YF:AAPL:close", source="tiingo")
    assert tiingo_latest is not None
    assert (tiingo_latest.obs_date, tiingo_latest.value, tiingo_latest.source) == (
        date(2026, 9, 16), 229.0, "tiingo",
    )

    # Case-insensitive, matching the convention in evaluation/prices.py.
    assert obs.read_latest(conn, "YF:AAPL:close", source="TIINGO").value == 229.0

    fred_window = obs.read_window(conn, "YF:AAPL:close", source="fred")
    assert [(o.obs_date, o.value) for o in fred_window] == [(date(2026, 9, 15), 227.5)]

    tiingo_n = obs.read_latest_n(conn, "YF:AAPL:close", 5, source="tiingo")
    assert [(o.obs_date, o.value) for o in tiingo_n] == [(date(2026, 9, 16), 229.0)]


def test_unknown_source_filter_returns_empty_not_mixed_error(conn):
    """An explicit source that matches nothing is a normal empty result, not a mixing error."""
    assert obs.read_latest(conn, "YF:AAPL:close", source="kaggle_bulk") is None
    assert obs.read_window(conn, "YF:AAPL:close", source="kaggle_bulk") == []
