"""Real-PostgreSQL twin of the same-day correction case (SQLite: tests/test_market_state_provisional_cutoff.py).

AGENTS.md "New knowledge: act on now, grade on then": a decision at 10:00, a correction received at 10:07 the same
day. On PostgreSQL the receipt clock is ``raw_series.pull_timestamp TIMESTAMPTZ``, so the reader gets timezone-aware
instants in the session's time zone, not the naive strings the SQLite twin stores. One series, one observation date,
two vintages, read through ``store.observations.read_window_known_at`` with ``as_of_ts``:

* a read at 10:00 returns the original, before and after the correction is stored;
* a read at exactly 10:07:00, the correction's receipt instant, returns the correction (the cutoff is inclusive: "at
  or before ``as_of_ts``"), and a read one microsecond earlier still returns the original;
* a read at 10:08 returns the correction.

Every instant is a fixed timezone-aware constant: the test reads no wall clock. It runs in a throwaway schema (random
name, dropped afterwards) with minimal ``source_catalog`` and ``raw_series`` tables, once with the session time zone
UTC and once west of UTC, where the same receipts come back with another offset and, for the original, another local
date.

Skips without a reachable PostgreSQL (``GRID_TEST_DB_URL``); the CI step fails on a skip.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from store.observations import PublicationLag, read_window_known_at

DDL = (
    "CREATE TABLE source_catalog (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
    "CREATE TABLE raw_series (id BIGSERIAL PRIMARY KEY, series_id TEXT NOT NULL, source_id INTEGER NOT NULL "
    "REFERENCES source_catalog(id), obs_date DATE NOT NULL, value DOUBLE PRECISION, pull_status TEXT NOT NULL, "
    "pull_timestamp TIMESTAMPTZ NOT NULL)",
)
SID, SRC = "CBOE:VIX3M", "CBOE"
DAY, PREV = date(2026, 11, 20), date(2026, 11, 19)
LAG = PublicationLag(1, "business", "next business day")


def utc(hour: int, minute: int) -> datetime:
    return datetime(2026, 11, 20, hour, minute, tzinfo=timezone.utc)


ORIGINAL_AT, CORRECTED_AT = utc(5, 0), utc(10, 7)      # the two receipts
DECIDED, AFTER = utc(10, 0), utc(10, 8)                # the two decisions


@pytest.fixture(params=["UTC", "America/Los_Angeles"])
def scratch(pg_engine, request):
    schema = f"known_at_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={
        "options": f"-csearch_path={schema} -cTimeZone={request.param}"})
    try:
        with engine.begin() as conn:
            for ddl in DDL:
                conn.execute(text(ddl))
            conn.execute(text("INSERT INTO source_catalog (name) VALUES (:n)"), {"n": SRC})
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))


def _store(engine, value: float, pulled: datetime) -> None:
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status, pull_timestamp) "
                          "SELECT :s, id, :d, :v, 'SUCCESS', :p FROM source_catalog WHERE name = :n"),
                     {"s": SID, "d": PREV, "v": value, "p": pulled, "n": SRC})


def _held(engine, ts: datetime):
    with engine.connect() as conn:
        return read_window_known_at(conn, SID, as_of=DAY, lag=LAG, source=SRC, as_of_ts=ts)


def _facts(observations):
    return [(o.obs_date, o.value, o.pull_timestamp, o.known_at_basis) for o in observations]


def test_a_same_day_correction_is_read_only_from_its_receipt_on(scratch):
    original, corrected = [(PREV, 21.0, ORIGINAL_AT, "pulled")], [(PREV, 21.4, CORRECTED_AT, "pulled")]

    _store(scratch, 21.0, ORIGINAL_AT)                 # the original, held at 10:00
    at_decision = _held(scratch, DECIDED)
    assert _facts(at_decision) == original
    assert at_decision[0].pull_timestamp.utcoffset() is not None       # TIMESTAMPTZ: an aware instant

    _store(scratch, 21.4, CORRECTED_AT)                # the correction, received 10:07
    assert _held(scratch, DECIDED) == at_decision      # the replay of 10:00 is unchanged
    assert _facts(_held(scratch, CORRECTED_AT - timedelta(microseconds=1))) == original
    assert _facts(_held(scratch, CORRECTED_AT)) == corrected           # at the receipt instant: inclusive
    assert _facts(_held(scratch, AFTER)) == corrected
