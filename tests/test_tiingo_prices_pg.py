"""Real PostgreSQL proof for the Tiingo daily-price writer (stale-sources audit 2026-09-29).

1. ``TiingoPuller._insert_rows`` -- the set-based INSERT ... SELECT FROM
   unnest(...) WHERE NOT EXISTS that replaced one round trip per row --
   keeps the old dedupe contract exactly: a row lands only when no SUCCESS
   row exists for (series_id, source_id, obs_date); a FAILED row does not
   block it; in-batch duplicates collapse to one; a re-run inserts nothing.
2. ``_latest_tiingo_obs`` reads only this source's SUCCESS rows.
3. ``pull_incremental`` returns SKIPPED, attempting nothing, while another
   connection (standing in for the other process: grid-scheduler vs
   grid-hermes) holds the Tiingo prices advisory lock.

Runs in a throwaway schema on GRID_TEST_DB_URL; the dedicated CI step treats
a skip as a failure (REQUIRE_TIINGO_PG=1).
"""

from __future__ import annotations

import os
import uuid
from datetime import date

import pytest
from sqlalchemy import create_engine, text

os.environ.setdefault("DB_PASSWORD", "test-password")

from ingestion.tiingo_pull import _ADVISORY_LOCK_KEY, TiingoPuller  # noqa: E402

_DB_URL = os.environ.get("GRID_TEST_DB_URL")


@pytest.fixture
def schema_engine():
    if not _DB_URL:
        if os.environ.get("REQUIRE_TIINGO_PG") == "1":
            pytest.fail("GRID_TEST_DB_URL is required for the Tiingo PostgreSQL proof")
        pytest.skip("GRID_TEST_DB_URL is not configured")
    schema = f"tiingo_pg_{uuid.uuid4().hex[:12]}"
    admin = create_engine(_DB_URL)
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(_DB_URL, connect_args={"options": f"-csearch_path={schema}"})
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE raw_series ("
            " id BIGSERIAL PRIMARY KEY, series_id TEXT NOT NULL, source_id INTEGER NOT NULL,"
            " obs_date DATE NOT NULL, value DOUBLE PRECISION, pull_status TEXT NOT NULL,"
            " pull_timestamp TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())"
        ))
        conn.execute(text(
            "CREATE UNIQUE INDEX uq_raw_series_composite ON raw_series "
            "(series_id, source_id, obs_date, pull_timestamp)"
        ))
    try:
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


def _puller(engine) -> TiingoPuller:
    puller = TiingoPuller.__new__(TiingoPuller)
    puller.engine = engine
    puller.source_id = 524
    return puller


def _rows(engine):
    with engine.connect() as conn:
        return conn.execute(text(
            "SELECT series_id, source_id, obs_date, value, pull_status FROM raw_series "
            "ORDER BY series_id, source_id, obs_date, pull_status"
        )).fetchall()


def test_set_based_insert_keeps_the_dedupe_contract(schema_engine) -> None:
    with schema_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) VALUES "
            "('YF:AAPL:close', 524, '2026-09-25', 1.0, 'SUCCESS'),"  # already stored
            "('YF:AAPL:close', 524, '2026-09-26', 0.0, 'FAILED'),"   # failure row: not a block
            "('YF:AAPL:close', 2,   '2026-09-29', 9.9, 'SUCCESS')"   # other source: not a block
        ))
    puller = _puller(schema_engine)
    batch = [
        {"sid": "YF:AAPL:close", "od": date(2026, 9, 25), "val": 1.5},
        {"sid": "YF:AAPL:close", "od": date(2026, 9, 26), "val": 2.0},
        {"sid": "YF:AAPL:close", "od": date(2026, 9, 29), "val": 3.0},
        {"sid": "YF:AAPL:close", "od": date(2026, 9, 29), "val": 3.0},  # in-batch duplicate
        {"sid": "YF:AAPL:open", "od": date(2026, 9, 29), "val": 2.5},
    ]
    assert puller._insert_rows(batch) == 3
    assert puller._insert_rows(batch) == 0  # idempotent re-run
    success_524 = [
        (r[0], r[2], r[3]) for r in _rows(schema_engine) if r[1] == 524 and r[4] == "SUCCESS"
    ]
    assert success_524 == [
        ("YF:AAPL:close", date(2026, 9, 25), 1.0),  # untouched, not duplicated
        ("YF:AAPL:close", date(2026, 9, 26), 2.0),
        ("YF:AAPL:close", date(2026, 9, 29), 3.0),
        ("YF:AAPL:open", date(2026, 9, 29), 2.5),
    ]


def test_set_based_insert_handles_multi_chunk_histories(schema_engine, monkeypatch) -> None:
    import ingestion.tiingo_pull as tp

    monkeypatch.setattr(tp, "_INSERT_CHUNK_ROWS", 7)
    puller = _puller(schema_engine)
    batch = [{"sid": "YF:MSFT:close", "od": date(2026, 1, d), "val": float(d)} for d in range(1, 31)]
    assert puller._insert_rows(batch) == 30
    assert puller._insert_rows(batch) == 0


def test_latest_obs_reads_only_this_sources_success_rows(schema_engine) -> None:
    with schema_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) VALUES "
            "('YF:AAPL:close', 524, '2026-09-24', 1, 'SUCCESS'),"
            "('YF:AAPL:close', 524, '2026-09-28', 0, 'FAILED'),"
            "('YF:AAPL:close', 2,   '2026-09-29', 1, 'SUCCESS')"
        ))
    puller = _puller(schema_engine)
    assert puller._latest_tiingo_obs("AAPL") == date(2026, 9, 24)
    assert puller._latest_tiingo_obs("NONE") is None


def test_incremental_pull_skips_while_another_connection_holds_the_lock(schema_engine) -> None:
    other = create_engine(_DB_URL)
    holder = other.connect()
    try:
        assert holder.execute(
            text("SELECT pg_try_advisory_lock(:k)"), {"k": _ADVISORY_LOCK_KEY}
        ).scalar() is True
        holder.commit()

        puller = _puller(schema_engine)

        def _must_not_fetch(*_a, **_k):
            raise AssertionError("fetched while another process held the lock")

        puller.pull_ticker = _must_not_fetch
        out = puller.pull_incremental(["AAPL", "MSFT"])
        assert out["status"] == "SKIPPED"
        assert "advisory lock" in out["skipped_reason"]
    finally:
        holder.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": _ADVISORY_LOCK_KEY})
        holder.commit()
        holder.close()
        other.dispose()

    # Released -> the next run takes the lock (and every ticker is simply
    # new here, so it goes to pull_ticker, stubbed to a no-op).
    puller = _puller(schema_engine)
    puller.pull_ticker = lambda t, **_k: {"ticker": t, "status": "PARTIAL", "rows_inserted": 0, "errors": []}
    assert puller.pull_incremental(["AAPL"])["status"] == "SUCCESS"
