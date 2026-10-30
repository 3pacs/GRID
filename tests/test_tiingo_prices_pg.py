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
4. Duplicate prevention (2026-10-01): ``_insert_rows``' per-ticker
   ``pg_advisory_xact_lock`` makes a second concurrent writer for the same
   ticker wait for the first to commit and then insert nothing; a negative
   control shows the same race duplicates without the lock; two simultaneous
   ``pull_ticker`` calls never leave two SUCCESS rows for one (series, date).

Runs in a throwaway schema on GRID_TEST_DB_URL; the dedicated CI step treats
a skip as a failure (REQUIRE_TIINGO_PG=1).
"""

from __future__ import annotations

import os
import time
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


# ── Duplicate prevention: per-ticker pg_advisory_xact_lock (2026-10-01) ──
#
# GRID-TIINGO-DUPLICATES-20260929: 1,675 byte-identical duplicate SUCCESS
# pairs, written 0.3ms-35s apart by two concurrent writers whose NOT EXISTS
# each missed the other's uncommitted rows. #733's run-level lock covers only
# pull_incremental; _insert_rows now takes a per-ticker transaction lock on
# every path (pull_all, pull_ticker, scripts).


def _success_counts(engine, source_id: int = 524) -> dict[tuple[str, date], int]:
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT series_id, obs_date, count(*) FROM raw_series "
            "WHERE source_id = :src AND pull_status = 'SUCCESS' "
            "GROUP BY series_id, obs_date"
        ), {"src": source_id}).fetchall()
    return {(r[0], r[1]): int(r[2]) for r in rows}


def _waiting_on_advisory_key(engine, key: int) -> bool:
    """True when some backend is blocked (not granted) on advisory ``key``."""
    unsigned = key & 0xFFFFFFFFFFFFFFFF
    with engine.connect() as conn:
        return bool(conn.execute(text(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted "
            "AND classid = CAST(:hi AS oid) AND objid = CAST(:lo AS oid) AND objsubid = 1"
        ), {"hi": unsigned >> 32, "lo": unsigned & 0xFFFFFFFF}).scalar())


def _race_second_writer_against_open_first(engine, puller) -> tuple[int, bool]:
    """Writer 1 holds an open txn that has inserted (not committed) AAPL 09-29.

    Writer 2 (puller._insert_rows in a thread) writes the same row. Returns
    (writer 2's inserted count, whether writer 2 was seen blocked on the
    ticker lock before writer 1 committed).
    """
    import threading

    from ingestion.base import _stable_lock_key

    key = _stable_lock_key("TIINGO", "prices", "YF:AAPL")
    batch = [{"sid": "YF:AAPL:close", "od": date(2026, 9, 29), "val": 3.0}]
    first = engine.connect()
    result: dict[str, object] = {}
    try:
        tx = first.begin()
        puller._lock_tickers(first, ["YF:AAPL:close"])  # what _insert_rows does first
        first.execute(text(
            "INSERT INTO raw_series (series_id, source_id, obs_date, value, pull_status) "
            "VALUES ('YF:AAPL:close', 524, '2026-09-29', 3.0, 'SUCCESS')"
        ))

        def _second() -> None:
            try:
                result["n"] = puller._insert_rows(batch)
            except Exception as exc:  # surfaced by the assertion below
                result["err"] = exc

        t = threading.Thread(target=_second, daemon=True)
        t.start()
        blocked = False
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and t.is_alive():
            if _waiting_on_advisory_key(engine, key):
                blocked = True
                break
            time.sleep(0.05)
        if blocked:
            assert t.is_alive(), "writer 2 must still be waiting while writer 1 is open"
        tx.commit()
        t.join(timeout=30)
        assert not t.is_alive()
    finally:
        first.close()
    assert "err" not in result, result.get("err")
    return int(result["n"]), blocked


def test_second_writer_waits_for_the_first_and_then_inserts_nothing(schema_engine) -> None:
    puller = _puller(schema_engine)
    inserted, blocked = _race_second_writer_against_open_first(schema_engine, puller)
    assert blocked, "writer 2 never waited on the per-ticker advisory lock"
    assert inserted == 0
    assert _success_counts(schema_engine) == {("YF:AAPL:close", date(2026, 9, 29)): 1}


def test_control_without_the_ticker_lock_the_same_race_duplicates(schema_engine) -> None:
    """Negative control: proves the race above is real, not an artefact.

    With the lock bypassed for writer 2, its NOT EXISTS cannot see writer 1's
    uncommitted row, so both commit a SUCCESS row -- exactly the 09-29 pairs.
    """
    puller = _puller(schema_engine)
    real_lock = puller._lock_tickers
    calls = {"n": 0}

    def _lock_only_first(conn, sids):
        calls["n"] += 1
        if calls["n"] == 1:  # writer 1 (inside the helper) still locks
            real_lock(conn, sids)

    puller._lock_tickers = _lock_only_first
    inserted, blocked = _race_second_writer_against_open_first(schema_engine, puller)
    assert not blocked and inserted == 1
    assert _success_counts(schema_engine) == {("YF:AAPL:close", date(2026, 9, 29)): 2}


def test_two_simultaneous_pull_ticker_calls_never_write_two_success_rows(
    schema_engine, monkeypatch
) -> None:
    """Acceptance: two concurrent pull_ticker calls, same ticker and dates."""
    import threading
    from unittest.mock import MagicMock

    import ingestion.tiingo_pull as tp

    rounds = 8
    payloads = {
        r: [{"date": f"2026-08-{r + 1:02d}T00:00:00.000Z", "open": 1.0 + r, "high": 2.0 + r,
             "low": 0.5 + r, "close": 1.5 + r, "volume": 100 + r, "adjClose": 1.5 + r}]
        for r in range(rounds)
    }
    current = {"round": 0}
    barrier = threading.Barrier(2, timeout=20)

    def _fake_get(*_a, **_k):
        barrier.wait()  # both writers have fetched before either inserts
        resp = MagicMock(status_code=200, headers={})
        resp.raise_for_status.return_value = None
        resp.json.return_value = payloads[current["round"]]
        return resp

    monkeypatch.setattr(tp.requests, "get", _fake_get)
    puller = _puller(schema_engine)
    for r in range(rounds):
        current["round"] = r
        out: list[dict] = []
        threads = [
            threading.Thread(target=lambda: out.append(puller.pull_ticker("AAPL", start_date="2026-08-01")))
            for _ in range(2)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        assert len(out) == 2 and all(o["status"] == "SUCCESS" for o in out), out
        # Six fields per day: exactly one writer inserted them, the other none.
        assert sorted(o["rows_inserted"] for o in out) == [0, 6]
        barrier.reset()

    counts = _success_counts(schema_engine)
    assert len(counts) == rounds * 6
    assert set(counts.values()) == {1}, {k: v for k, v in counts.items() if v != 1}
