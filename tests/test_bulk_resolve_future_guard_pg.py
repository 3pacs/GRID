"""bulk_resolve.py's obs_date guard, run on PostgreSQL.

tests/test_bulk_resolve_future_guard.py pins the SQL text and its logic in
Python. This file runs that SQL: ``resolve_feature_batch`` (insert and
--dry-run count) against a disposable schema on GRID_TEST_DB_URL, with the
session TimeZone set west of UTC, so a guard that used the session date of
``pull_timestamp`` instead of its UTC date would fail here.

Skips without GRID_TEST_DB_URL unless REQUIRE_BULK_RESOLVE_GUARD_PG=1, which
the dedicated CI step sets so a skip cannot pass for a result.
"""

from __future__ import annotations

import os
import re
from datetime import date, datetime, timezone
from uuid import uuid4

import pytest
from psycopg2 import sql as pg_sql
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

import scripts.bulk_resolve as bulk_resolve

pytestmark = pytest.mark.xdist_group("postgres")

_REQUIRE_ENV = "REQUIRE_BULK_RESOLVE_GUARD_PG"

# 01:00 UTC on the 22nd is 17:00 on the 21st in Los Angeles: the session date
# and the UTC date differ, which is the case the guard must get right.
_SESSION_TZ = "America/Los_Angeles"
_PULL_EARLY_UTC = datetime(2026, 11, 22, 1, 0, tzinfo=timezone.utc)
_PULL_LATE_UTC = datetime(2026, 11, 21, 23, 30, tzinfo=timezone.utc)

_ROWS = [
    # (feature_id, series_id, obs_date, pull_timestamp, value)
    (1, "VIXCLS", date(2026, 11, 21), _PULL_EARLY_UTC, 17.0),   # before the UTC date
    (1, "VIXCLS", date(2026, 11, 22), _PULL_EARLY_UTC, 17.5),   # the UTC date: kept
    (1, "VIXCLS", date(2026, 11, 23), _PULL_EARLY_UTC, 18.0),   # +1, undeclared: out
    (1, "VIXCLS", date(2026, 11, 25), _PULL_EARLY_UTC, 19.0),   # +3: out
    (2, "pla_activity:aircraft_sorties", date(2026, 11, 22), _PULL_LATE_UTC, 12.0),  # declared +1
    (2, "pla_activity:aircraft_sorties", date(2026, 11, 23), _PULL_LATE_UTC, 13.0),  # +2: out
    (3, "EARNINGS:AAPL:estimate", date(2027, 9, 17), _PULL_LATE_UTC, 2.1),  # event-dated
]
_KEPT = {
    1: {date(2026, 11, 21), date(2026, 11, 22)},
    2: {date(2026, 11, 22)},
    3: {date(2027, 9, 17)},
}


@pytest.fixture(scope="module")
def guard_engine():
    """Disposable schema with the three tables bulk_resolve touches."""
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        if os.environ.get(_REQUIRE_ENV) == "1":
            pytest.fail(f"GRID_TEST_DB_URL is required ({_REQUIRE_ENV}=1)")
        pytest.skip("GRID_TEST_DB_URL is required for disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("bulk_resolve guard proof requires a local disposable test database")

    schema = "bulk_guard_" + uuid4().hex[:12]
    assert re.fullmatch(r"bulk_guard_[0-9a-f]{12}", schema)
    admin = create_engine(url)
    raw = admin.raw_connection()
    try:
        with raw.cursor() as cur:
            cur.execute(pg_sql.SQL("CREATE SCHEMA {}").format(pg_sql.Identifier(schema)))
        raw.commit()
    finally:
        raw.close()

    engine = create_engine(url, connect_args={
        "options": f"-csearch_path={schema} -ctimezone={_SESSION_TZ}",
    })
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE source_catalog (id INTEGER PRIMARY KEY, name TEXT, "
            "priority_rank INTEGER NOT NULL)"
        ))
        conn.execute(text(
            "CREATE TABLE raw_series (id BIGSERIAL PRIMARY KEY, series_id TEXT NOT NULL, "
            "source_id INTEGER NOT NULL, obs_date DATE NOT NULL, "
            "pull_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
            "value DOUBLE PRECISION NOT NULL, raw_payload JSONB, pull_status TEXT NOT NULL)"
        ))
        conn.execute(text(
            "CREATE TABLE resolved_series (id BIGSERIAL PRIMARY KEY, "
            "feature_id INTEGER NOT NULL, obs_date DATE NOT NULL, release_date DATE NOT NULL, "
            "vintage_date DATE NOT NULL, value DOUBLE PRECISION NOT NULL, "
            "source_priority_used INTEGER NOT NULL, conflict_flag BOOLEAN NOT NULL DEFAULT FALSE, "
            "UNIQUE (feature_id, obs_date, vintage_date))"
        ))
        conn.execute(text("INSERT INTO source_catalog VALUES (1, 'synthetic', 1)"))
        for _fid, sid, obs, pulled, value in _ROWS:
            conn.execute(
                text("INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, "
                     "value, pull_status) VALUES (:sid, 1, :od, :ts, :val, 'SUCCESS')"),
                {"sid": sid, "od": obs, "ts": pulled, "val": value},
            )

    yield engine

    engine.dispose()
    raw = admin.raw_connection()
    try:
        with raw.cursor() as cur:
            cur.execute(pg_sql.SQL("DROP SCHEMA {} CASCADE").format(pg_sql.Identifier(schema)))
        raw.commit()
    finally:
        raw.close()
    admin.dispose()


def _series_for(feature_id: int) -> list[str]:
    return sorted({sid for fid, sid, *_ in _ROWS if fid == feature_id})


def test_session_date_and_utc_date_really_differ(guard_engine):
    with guard_engine.connect() as conn:
        session_date, utc_date = conn.execute(
            text("SELECT CAST(:ts AS timestamptz)::date, "
                 "(CAST(:ts AS timestamptz) AT TIME ZONE 'UTC')::date"),
            {"ts": _PULL_EARLY_UTC},
        ).one()
    assert (session_date, utc_date) == (date(2026, 11, 21), date(2026, 11, 22))


@pytest.mark.parametrize("feature_id", sorted(_KEPT))
def test_dry_run_counts_only_admitted_dates(guard_engine, feature_id):
    count = bulk_resolve.resolve_feature_batch(
        guard_engine, feature_id, _series_for(feature_id), dry_run=True)
    assert count == len(_KEPT[feature_id])


def test_insert_resolves_only_admitted_rows(guard_engine):
    for feature_id in sorted(_KEPT):
        bulk_resolve.resolve_feature_batch(guard_engine, feature_id, _series_for(feature_id))

    with guard_engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT feature_id, obs_date FROM resolved_series ORDER BY feature_id, obs_date"
        )).fetchall()
    got: dict[int, set[date]] = {}
    for feature_id, obs in rows:
        got.setdefault(feature_id, set()).add(obs)
    assert got == _KEPT
