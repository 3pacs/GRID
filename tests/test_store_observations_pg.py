"""Live-PostgreSQL contract for store/observations.py (the SQL behind #535's vintage-safe reads).

tests/test_store_observations.py drives the module through a mocked connection, so the SQL it issues —
``WHERE series_id = :sid AND pull_status = :ok ORDER BY obs_date ASC, pull_timestamp DESC``, the
``as_of`` / ``as_of_ts`` predicates and the ``LIMIT`` — never ran against PostgreSQL. This file runs the
same nine assertions the 2026-09-18 ad-hoc check ran against a disposable PostgreSQL 14 database
(recorded in the vault: ``grid-fake-data-535-obs-db-check/``), through the suite's ``pg_engine`` fixture.
It skips without a live database; CI enables it once GRID_TEST_DB_URL is wired (#560).

Seed (one series, one throwaway source_catalog row; every row deleted again in the fixture teardown):

    2026-09-01  10.0 SUCCESS @T0        11.0 SUCCESS @T0+5h   two vintages: the later pull wins
    2026-09-02   0.0 FAILED  @T0+1d     12.0 SUCCESS @T0+1d1h a failed pull recorded as 0 must never surface
    2026-09-03  13.5 PARTIAL                                   partial pulls are not accepted observations
    2026-09-04   0.0 SUCCESS                                   a *measured* zero is a real observation
    2026-09-05  15.0 SUCCESS
    2026-09-06   0.0 FAILED                                    newest row is a failure: read_latest must skip it
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from store import observations as obs

SID = "OBSPG:TEST:SERIES"
SRC = "test_store_observations_pg_source"
T0 = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)

ROWS = [
    (date(2026, 9, 1), 10.0, "SUCCESS", T0),
    (date(2026, 9, 1), 11.0, "SUCCESS", T0 + timedelta(hours=5)),
    (date(2026, 9, 2), 0.0, "FAILED", T0 + timedelta(days=1)),
    (date(2026, 9, 2), 12.0, "SUCCESS", T0 + timedelta(days=1, hours=1)),
    (date(2026, 9, 3), 13.5, "PARTIAL", T0 + timedelta(days=2)),
    (date(2026, 9, 4), 0.0, "SUCCESS", T0 + timedelta(days=3)),
    (date(2026, 9, 5), 15.0, "SUCCESS", T0 + timedelta(days=4)),
    (date(2026, 9, 6), 0.0, "FAILED", T0 + timedelta(days=5)),
]


@pytest.fixture
def seeded(pg_engine):
    with pg_engine.begin() as c:
        c.execute(text("DELETE FROM raw_series WHERE series_id = :s"), {"s": SID})
        src_id = c.execute(text(
            "INSERT INTO source_catalog (name, base_url, cost_tier, latency_class, pit_available, "
            "revision_behavior, trust_score, priority_rank) "
            "VALUES (:n, 'http://test.invalid', 'FREE', 'EOD', FALSE, 'NEVER', 'LOW', 9999) "
            "ON CONFLICT (name) DO UPDATE SET base_url = EXCLUDED.base_url RETURNING id"
        ), {"n": SRC}).scalar_one()
        for d, v, st, ts in ROWS:
            c.execute(text(
                "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status, pull_timestamp) "
                "VALUES (:s, :src, :d, :v, :st, :ts)"
            ), {"s": SID, "src": src_id, "d": d, "v": v, "st": st, "ts": ts})
    try:
        with pg_engine.connect() as conn:
            yield conn
    finally:
        with pg_engine.begin() as c:
            c.execute(text("DELETE FROM raw_series WHERE series_id = :s"), {"s": SID})
            c.execute(text("DELETE FROM source_catalog WHERE name = :n"), {"n": SRC})


def _pairs(observations):
    return [(o.obs_date.isoformat(), o.value) for o in observations]


def test_window_returns_success_rows_one_per_date_latest_vintage_oldest_first(seeded):
    assert _pairs(obs.read_window(seeded, SID)) == [
        ("2026-09-01", 11.0), ("2026-09-02", 12.0), ("2026-09-04", 0.0), ("2026-09-05", 15.0),
    ]


def test_window_keeps_a_measured_zero_but_never_a_failed_zero(seeded):
    got = _pairs(obs.read_window(seeded, SID))
    assert ("2026-09-04", 0.0) in got
    assert all(d != "2026-09-06" for d, _ in got)
    assert all(d != "2026-09-03" for d, _ in got)  # PARTIAL


def test_latest_is_the_newest_success_not_the_newer_failed_row(seeded):
    latest = obs.read_latest(seeded, SID)
    assert latest is not None
    assert (latest.obs_date, latest.value) == (date(2026, 9, 5), 15.0)


def test_latest_as_of_date_returns_that_dates_success_vintage(seeded):
    got = obs.read_latest(seeded, SID, as_of=date(2026, 9, 2))
    assert got is not None
    assert (got.obs_date, got.value) == (date(2026, 9, 2), 12.0)


def test_latest_as_of_ts_before_the_second_vintage_returns_the_first(seeded):
    got = obs.read_latest(seeded, SID, as_of_ts=T0 + timedelta(hours=1))
    assert got is not None
    assert (got.obs_date, got.value) == (date(2026, 9, 1), 10.0)


def test_latest_n_returns_the_two_most_recent_accepted_dates(seeded):
    dates = sorted(o.obs_date for o in obs.read_latest_n(seeded, SID, 2))
    assert dates == [date(2026, 9, 4), date(2026, 9, 5)]


def test_window_start_bound(seeded):
    assert [o.obs_date for o in obs.read_window(seeded, SID, start=date(2026, 9, 3))] == [
        date(2026, 9, 4), date(2026, 9, 5),
    ]


def test_unknown_series_window_is_empty_not_zeros(seeded):
    assert obs.read_window(seeded, "OBSPG:NO:SUCH") == []


def test_unknown_series_latest_is_none_not_zero(seeded):
    assert obs.read_latest(seeded, "OBSPG:NO:SUCH") is None
