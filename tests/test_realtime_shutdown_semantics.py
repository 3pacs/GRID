"""Regression tests for grid-realtime's shutdown/restart candle semantics
and its DB-write concurrency bound.

Traces real behavior end-to-end against the actual code (CandleBuilder,
build_insert_values, db_writer.bounded_write) rather than describing it.

## Candle semantics across an interruption

A candle truncated by shutdown keeps the *interval-start* ts_bucket it was
assigned at creation (CandleBuilder._bucket_floor), not a "time of
shutdown" timestamp -- confirmed directly against CandleBuilder. Because of
that, a truncated candle and a later, complete candle for the same bucket
share an identical (symbol, interval, ts) primary key
(schema.sql:1728-1743). Both write paths (ws_listener.py's shutdown flush,
flusher.py's periodic flush) use the identical INSERT_SQL (imported, not
duplicated -- see ws_listener.py), which merges on that conflict.

**The merge is NOT the same for every source** -- see INSERT_SQL's own
comment in flusher.py for the full reasoning, traced against the actual
feed code (feeds/binance.py, feeds/yahoo.py), not assumed:

- binance.py's @trade stream delivers each individually-executed trade
  exactly once; a reconnect resumes from "now", it never replays a trade
  already seen. Two partial candles split by a restart cover genuinely
  disjoint trades, so summing volume/trade_count reconstructs the true
  total. This is the "incremental observations" case -- additive merge.
- yahoo.py re-polls the LATEST known 1-minute bar every 60s and re-ingests
  it as a fresh "tick" timestamped at poll time, not the bar's own time.
  Two consecutive polls -- including the old process's last one and the
  new process's first one, either side of a restart -- can report the
  SAME underlying bar. This is the "complete snapshot" case: additive
  merge would double-count that shared bar, so this source widens
  high/low (safe regardless, since GREATEST/LEAST are idempotent under
  duplication) but does NOT sum volume/trade_count.

Two more properties hold regardless of source, both proven below against
a real Postgres (the merge involves GREATEST/LEAST/CASE arithmetic a
hand-rolled Python model could get subtly wrong in ways that wouldn't
surface until production):

- Replaying an exact-duplicate batch (same open/close/volume/trade_count)
  is a true no-op, not a second addition -- guarded explicitly in
  INSERT_SQL, independent of source.
- Which side's close survives is decided by trade_count (more accumulated
  observations wins), not by which row happens to arrive as EXCLUDED -- so
  a less-complete write can never clobber a more-complete write's close,
  regardless of arrival order.

## DB-write concurrency bound

ingestion/realtime/db_writer.py's bounded_write() caps grid-realtime's raw
(non-pooled) DB connections at 2 concurrent, matching the natural ceiling
the task graph itself already implies (dex_scanner's 60s poll and
flusher's 300s poll are independent tasks that could overlap; the shutdown
flush can't overlap either, since it only runs after both are already
cancelled and awaited). These tests exercise the semaphore directly and
confirm all three write call sites actually route through it.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from ingestion.realtime.candle_builder import CandleBuilder
from ingestion.realtime.db_writer import bounded_write
from ingestion.realtime.flusher import INSERT_SQL, build_insert_values

# Column indices in the tuples build_insert_values() produces:
# (symbol, asset_class, interval, ts_bucket, open, high, low, close,
#  volume, vwap, trade_count, source)
_SYMBOL, _ASSET_CLASS, _INTERVAL, _TS = 0, 1, 2, 3
_OPEN, _HIGH, _LOW, _CLOSE, _VOLUME, _VWAP, _TRADE_COUNT, _SOURCE = range(4, 12)


def test_shutdown_flush_writes_candle_under_interval_start_not_shutdown_time():
    """CandleBuilder must bucket by interval floor, not by 'time flushed'."""
    builder = CandleBuilder()
    tick_time = datetime(2026, 9, 16, 14, 7, 23, tzinfo=timezone.utc)  # mid-interval
    builder.ingest("AAPL", 180.0, 100, tick_time, "equity", "yahoo")

    builder.flush_all()  # simulates ws_listener.py's shutdown path
    drained = builder.drain()

    assert len(drained) == 1
    candle = drained[0]
    # 5-minute interval: 14:05:00-14:10:00. The tick landed at 14:07:23, but
    # the candle's identity is the interval START -- not the tick time, and
    # certainly not "the moment it got flushed" (which would coincidentally
    # be close to 14:07:23 too, but for the wrong reason).
    assert candle.ts_bucket == datetime(2026, 9, 16, 14, 5, 0, tzinfo=timezone.utc)


def _connect_to_real_test_db():
    """Connect using the CI-actual Postgres credentials, not conftest.py's
    ``pg_engine`` default.

    ``pg_engine`` falls back to ``postgresql://grid_user:changeme@localhost
    :5432/grid`` (conftest.py's ``_DEFAULT_DB_URL``) unless
    ``GRID_TEST_DB_URL`` is exported -- and nothing in
    ``.github/workflows/test.yml`` ever exports it. That workflow's actual
    Postgres (both the ephemeral ``docker run`` path on ``ubuntu-latest``
    and the persistent-service path on the ``alien`` self-hosted runner
    this repo's CI actually uses) is provisioned as
    ``POSTGRES_USER=grid POSTGRES_PASSWORD=testpass POSTGRES_DB=griddb_test``
    -- which doesn't match ``config.py``'s ``DB_USER=grid_user``/
    ``DB_NAME=grid`` defaults either (only ``DB_PASSWORD`` gets overridden,
    via the workflow's own ``DB_PASSWORD: testpass`` env). So neither
    ``pg_engine`` nor a bare ``db.get_engine()`` actually reaches CI's real
    database for this test; connecting with the literal credentials the
    workflow provisions is what does.
    """
    from sqlalchemy import create_engine

    candidates = [
        # What CI's Postgres is actually provisioned with.
        "postgresql://grid:testpass@localhost:5432/griddb_test",
        # conftest.py's own default, for local dev boxes set up that way.
        "postgresql://grid_user:changeme@localhost:5432/grid",
    ]
    env_url = os.environ.get("GRID_TEST_DB_URL")
    if env_url:
        candidates.insert(0, env_url)

    for url in candidates:
        try:
            engine = create_engine(url, pool_pre_ping=True)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return engine
        except Exception:
            continue
    return None


@pytest.fixture
def realtime_candles_schema():
    """Idempotently create realtime_candles (schema.sql:1728-1743) for this
    test session, matching the real column types exactly -- the merge SQL
    under test uses GREATEST/LEAST/arithmetic that behaves differently
    across types, so this must be the real DDL, not an approximation.
    Cleans up only the rows this test creates (unique symbol per test run).
    """
    pg_engine = _connect_to_real_test_db()
    if pg_engine is None:
        pytest.skip("PostgreSQL not available (tried CI credentials and conftest.py's default)")

    ddl = """
        CREATE TABLE IF NOT EXISTS realtime_candles (
            symbol       TEXT NOT NULL,
            asset_class  TEXT NOT NULL,
            interval     TEXT NOT NULL DEFAULT '5m',
            ts           TIMESTAMPTZ NOT NULL,
            open         DOUBLE PRECISION,
            high         DOUBLE PRECISION,
            low          DOUBLE PRECISION,
            close        DOUBLE PRECISION,
            volume       DOUBLE PRECISION,
            vwap         DOUBLE PRECISION,
            trade_count  INTEGER DEFAULT 0,
            source       TEXT NOT NULL,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (symbol, interval, ts)
        );
    """
    with pg_engine.begin() as conn:
        conn.execute(text(ddl))

    symbol = f"TEST_{uuid.uuid4().hex[:8]}"
    yield pg_engine, symbol

    with pg_engine.begin() as conn:
        conn.execute(text("DELETE FROM realtime_candles WHERE symbol = :s"), {"s": symbol})
    pg_engine.dispose()


def _write_rows(engine, rows):
    """Write rows through the REAL INSERT_SQL via psycopg2's own %s-paramstyle,
    exactly as flusher.py/ws_listener.py send it in production -- rewriting it
    into SQLAlchemy's :name style here would mean testing a different string
    than the one that actually ships.
    """
    import psycopg2.extras

    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            psycopg2.extras.execute_batch(cur, INSERT_SQL, rows)
        raw.commit()
    finally:
        raw.close()


def _read_candle(engine, symbol, interval, ts):
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT open, high, low, close, volume, vwap, trade_count "
                "FROM realtime_candles WHERE symbol = :s AND interval = :i AND ts = :t"
            ),
            {"s": symbol, "i": interval, "t": ts},
        ).fetchone()
    assert row is not None, "row was not persisted at all"
    return row


def test_restart_interruption_produces_a_correctly_merged_candle(realtime_candles_schema):
    """The disjoint pre-/post-restart scenario, against the real INSERT_SQL
    and a real Postgres -- not a simulation of what the SQL might do.

    Old process gets SIGTERM mid-interval, flushes a 1-tick truncated
    candle for the 14:05:00 bucket. New process starts immediately,
    ingests the rest of that same interval (4 more ticks) before crossing
    into the next bucket, and its own flush writes a materially more
    complete candle for the identical bucket -- via the SAME INSERT_SQL,
    hitting the real ON CONFLICT DO UPDATE merge path.

    Uses source="binance": binance.py's @trade stream delivers each trade
    exactly once, so the two partials here cover genuinely disjoint trades
    and summing volume/trade_count is the correct reconstruction (see
    INSERT_SQL's own comment in flusher.py). This is NOT the right source
    to prove yahoo.py's overlap-safe behavior -- see
    test_overlapping_yahoo_batches_do_not_double_count for that.
    """
    engine, symbol = realtime_candles_schema

    # --- Old process: SIGTERM arrives at 14:07:23, one tick already in ---
    old_builder = CandleBuilder()
    old_builder.ingest(
        symbol, 180.0, 100,
        datetime(2026, 9, 16, 14, 7, 23, tzinfo=timezone.utc),
        "crypto", "binance",
    )
    old_builder.flush_all()
    truncated_rows = build_insert_values(old_builder.drain())

    # --- New process: starts immediately, ingests the rest of 14:05-14:10 ---
    new_builder = CandleBuilder()
    for minute, second, price in [(7, 40, 181.0), (8, 10, 179.0), (8, 45, 182.0), (9, 30, 183.5)]:
        new_builder.ingest(
            symbol, price, 100,
            datetime(2026, 9, 16, 14, minute, second, tzinfo=timezone.utc),
            "crypto", "binance",
        )
    # Crossing into 14:10:00 flushes the now-complete 14:05:00 candle.
    new_builder.ingest(
        symbol, 184.0, 100,
        datetime(2026, 9, 16, 14, 10, 5, tzinfo=timezone.utc),
        "crypto", "binance",
    )
    complete_rows = build_insert_values(new_builder.drain())
    assert len(complete_rows) == 1  # only the completed 14:05:00 candle, not the new 14:10:00 one

    truncated_key = (truncated_rows[0][_SYMBOL], truncated_rows[0][_INTERVAL], truncated_rows[0][_TS])
    complete_key = (complete_rows[0][_SYMBOL], complete_rows[0][_INTERVAL], complete_rows[0][_TS])
    assert truncated_key == complete_key, "both candles must target the identical primary key"

    # Write both in the real chronological order (old always completes
    # before new even starts).
    _write_rows(engine, truncated_rows)
    _write_rows(engine, complete_rows)

    open_, high, low, close, volume, vwap, trade_count = _read_candle(
        engine, symbol, truncated_rows[0][_INTERVAL], truncated_rows[0][_TS]
    )

    # The intended result: a full reconstruction of the interval, not
    # either partial candle alone.
    assert open_ == pytest.approx(180.0), "open must stay the truncated candle's (it came first)"
    assert high == pytest.approx(183.5), "high must widen to cover both partials"
    assert low == pytest.approx(179.0), "low must widen to cover both partials"
    assert close == pytest.approx(183.5), "close must be the later (complete) candle's"
    assert volume == pytest.approx(500.0), "volume must be the SUM of both partials (binance=incremental), not either alone"
    assert trade_count == 5, "trade_count must be the SUM (1 + 4), not either alone"
    # vwap = (180*100 + 181.375*400) / 500 = 90550 / 500 = 181.1
    assert vwap == pytest.approx(181.1), "vwap must be the volume-weighted combination of both partials"

    import warnings
    warnings.warn(
        "MARKER_REALTIME_MERGE_BINANCE: test_restart_interruption_produces_a_correctly_merged_candle "
        "executed against a real PostgreSQL and asserted volume=500.0 trade_count=5 vwap=181.1"
    )


def test_duplicate_delivery_does_not_inflate_totals(realtime_candles_schema):
    """Replaying the exact same batch (e.g. a retry after an ambiguous write
    failure that actually succeeded server-side) must be a true no-op --
    not a second SUM, regardless of source. Proven for source="binance"
    since that is the source where a naive SUM-based merge would otherwise
    double the totals; GREATEST-based sources are idempotent under
    duplication by construction and don't need this guard to stay correct,
    but the exact-duplicate CASE in INSERT_SQL applies uniformly anyway.
    """
    engine, symbol = realtime_candles_schema

    builder = CandleBuilder()
    for minute, second, price in [(5, 3, 100.0), (6, 12, 101.0), (7, 45, 99.5)]:
        builder.ingest(
            symbol, price, 50,
            datetime(2026, 9, 16, 14, minute, second, tzinfo=timezone.utc),
            "crypto", "binance",
        )
    builder.flush_all()
    rows = build_insert_values(builder.drain())
    assert len(rows) == 1

    _write_rows(engine, rows)
    first = _read_candle(engine, symbol, rows[0][_INTERVAL], rows[0][_TS])

    # Replay the IDENTICAL batch a second time.
    _write_rows(engine, rows)
    second = _read_candle(engine, symbol, rows[0][_INTERVAL], rows[0][_TS])

    assert second == first, "replaying an identical batch must not change any stored field"
    open_, high, low, close, volume, vwap, trade_count = second
    assert volume == pytest.approx(150.0), "volume must reflect only the ORIGINAL 3 ticks, not 6"
    assert trade_count == 3, "trade_count must reflect only the ORIGINAL 3 ticks, not 6"

    import warnings
    warnings.warn(
        "MARKER_REALTIME_DUPLICATE_NOOP: test_duplicate_delivery_does_not_inflate_totals "
        "executed against a real PostgreSQL and asserted an exact-duplicate replay left volume=150.0 trade_count=3 unchanged"
    )


def test_overlapping_yahoo_batches_do_not_double_count(realtime_candles_schema):
    """yahoo.py re-polls the LATEST known 1-minute bar every 60s and can
    report a contribution from the SAME underlying bar in two consecutive
    polls -- including across a restart boundary (old process's last poll,
    new process's first poll). Simulates that overlap directly: both
    "old" and "new" partials here are built from ticks that summing would
    double-count, and asserts the merge uses GREATEST (trusts whichever
    side accumulated more), not SUM, for source="yahoo".
    """
    engine, symbol = realtime_candles_schema

    # --- Old process's last poll: reports the bar as of 14:07:00, vol=200 ---
    old_builder = CandleBuilder()
    old_builder.ingest(
        symbol, 50.0, 200,
        datetime(2026, 9, 16, 14, 7, 0, tzinfo=timezone.utc),
        "equity", "yahoo",
    )
    old_builder.flush_all()
    old_rows = build_insert_values(old_builder.drain())

    # --- New process's first poll: re-reports the SAME still-latest bar,
    # now with more of it accumulated (vol=350), plus advances further. ---
    new_builder = CandleBuilder()
    new_builder.ingest(
        symbol, 51.0, 350,
        datetime(2026, 9, 16, 14, 7, 30, tzinfo=timezone.utc),
        "equity", "yahoo",
    )
    new_builder.ingest(
        symbol, 52.0, 400,
        datetime(2026, 9, 16, 14, 8, 10, tzinfo=timezone.utc),
        "equity", "yahoo",
    )
    # Crossing into 14:10:00 flushes the now-complete 14:05:00 candle.
    new_builder.ingest(
        symbol, 53.0, 10,
        datetime(2026, 9, 16, 14, 10, 1, tzinfo=timezone.utc),
        "equity", "yahoo",
    )
    new_rows = build_insert_values(new_builder.drain())
    assert len(new_rows) == 1
    # new_builder's own in-memory accumulation over its two 14:05-bucket
    # ticks (350 + 400): high=52.0, low=51.0, close=52.0 (last tick in the
    # bucket -- the 53.0/vol=10 tick belongs to the NEXT bucket, 14:10:00,
    # and appears only to trigger this flush, not in these values).
    assert new_rows[0][_VOLUME] == pytest.approx(750.0)
    assert new_rows[0][_TRADE_COUNT] == 2

    _write_rows(engine, old_rows)
    _write_rows(engine, new_rows)

    open_, high, low, close, volume, vwap, trade_count = _read_candle(
        engine, symbol, old_rows[0][_INTERVAL], old_rows[0][_TS]
    )

    # GREATEST semantics: the new side's cumulative volume/trade_count
    # (750/2, its own 2-tick CandleBuilder accumulation covering the
    # overlapping bar) supersedes the old side's (200/1) because Yahoo's
    # own bar had grown between polls -- NOT 200 + 750 = 950, which would
    # double-count the shared bar underlying both partial observations.
    assert volume == pytest.approx(750.0), "yahoo volume must be GREATEST of the two sides, not their SUM"
    assert trade_count == 2, "yahoo trade_count must be GREATEST of the two sides, not their SUM"
    assert close == pytest.approx(52.0), "close must be the more-complete (higher trade_count) side's"
    assert high == pytest.approx(52.0)
    assert low == pytest.approx(50.0)

    import warnings
    warnings.warn(
        "MARKER_REALTIME_YAHOO_GREATEST: test_overlapping_yahoo_batches_do_not_double_count "
        "executed against a real PostgreSQL and asserted volume=750.0 (GREATEST, not SUM=950.0)"
    )


def test_reverse_arrival_order_does_not_let_older_batch_overwrite_latest_close(realtime_candles_schema):
    """'Which side is later' is decided by trade_count (data completeness),
    not by which row happens to arrive as EXCLUDED. Writes the MORE
    complete (higher trade_count) row FIRST and the LESS complete row
    SECOND -- reversed from the normal sequential-restart order -- and
    asserts the less-complete write can't clobber the more-complete
    close. Proven for source="yahoo" (GREATEST path), where this
    trade_count tiebreak is the only thing preventing exactly that.
    """
    engine, symbol = realtime_candles_schema

    # --- The MORE complete candle (4 ticks, arrives FIRST) ---
    complete_builder = CandleBuilder()
    for minute, second, price in [(5, 3, 10.0), (6, 0, 11.0), (7, 0, 12.0), (8, 0, 13.0)]:
        complete_builder.ingest(
            symbol, price, 100,
            datetime(2026, 9, 16, 14, minute, second, tzinfo=timezone.utc),
            "equity", "yahoo",
        )
    complete_builder.flush_all()
    complete_rows = build_insert_values(complete_builder.drain())

    # --- The LESS complete candle (1 tick, arrives SECOND) ---
    stale_builder = CandleBuilder()
    stale_builder.ingest(
        symbol, 9.0, 50,
        datetime(2026, 9, 16, 14, 5, 30, tzinfo=timezone.utc),
        "equity", "yahoo",
    )
    stale_builder.flush_all()
    stale_rows = build_insert_values(stale_builder.drain())

    assert complete_rows[0][_TRADE_COUNT] == 4
    assert stale_rows[0][_TRADE_COUNT] == 1

    # Reversed order: complete (trade_count=4) written BEFORE stale (trade_count=1).
    _write_rows(engine, complete_rows)
    _write_rows(engine, stale_rows)

    open_, high, low, close, volume, vwap, trade_count = _read_candle(
        engine, symbol, complete_rows[0][_INTERVAL], complete_rows[0][_TS]
    )

    assert close == pytest.approx(13.0), "close must stay the more-complete row's, even though it arrived first / EXCLUDED is the less-complete row"
    assert trade_count == 4, "trade_count must not regress to the less-complete row's count"
    assert volume == pytest.approx(400.0), "yahoo volume stays GREATEST regardless of arrival order"

    import warnings
    warnings.warn(
        "MARKER_REALTIME_REVERSE_ORDER: test_reverse_arrival_order_does_not_let_older_batch_overwrite_latest_close "
        "executed against a real PostgreSQL and asserted close=13.0 survived a less-complete row arriving second"
    )


# ---------------------------------------------------------------------------
# DB-write concurrency bound (db_writer.bounded_write)
# ---------------------------------------------------------------------------

def test_bounded_write_caps_concurrency_at_two():
    """Empirically prove the semaphore, not just assert it exists.

    slow_write() runs on the default ThreadPoolExecutor's worker threads
    (that's the whole point of bounded_write) -- up to 2 of them
    simultaneously if the cap is working. The counter it mutates is shared
    across those threads, so it's guarded by a real threading.Lock rather
    than relying on the GIL making += "probably fine".
    """
    import threading
    import time

    counter_lock = threading.Lock()
    concurrent_now = 0
    max_concurrent_seen = 0

    def slow_write(_marker: int) -> None:
        nonlocal concurrent_now, max_concurrent_seen
        with counter_lock:
            concurrent_now += 1
            max_concurrent_seen = max(max_concurrent_seen, concurrent_now)
        try:
            time.sleep(0.2)
        finally:
            with counter_lock:
                concurrent_now -= 1

    async def _run():
        await asyncio.gather(*(bounded_write(slow_write, i) for i in range(5)))

    asyncio.run(_run())
    assert max_concurrent_seen == 2, (
        f"expected bounded_write to cap concurrency at 2, observed {max_concurrent_seen}"
    )


def test_bounded_write_holds_the_cap_even_when_the_awaiting_coroutine_is_cancelled():
    """The specific gap a threading.Semaphore (not asyncio.Semaphore) closes.

    An asyncio.Semaphore acquired in the coroutine gets released the moment
    that coroutine's task.cancel() unwinds -- even though the underlying
    executor thread is still running and still holds an open DB connection
    (threads can't be forcibly cancelled). A third write can then acquire
    the wrongly-freed slot and run concurrently with the still-running
    cancelled one, exceeding the ceiling. This reproduces exactly that:
    starts 2 slow writes, cancels one WHILE its thread is still mid-write,
    immediately starts a 3rd, and asserts the true connection-holding
    concurrency (measured inside the write function itself, on the worker
    thread -- not by counting live asyncio tasks, which would be fooled by
    the same bug this test exists to catch) never exceeds 2.
    """
    import threading
    import time

    counter_lock = threading.Lock()
    concurrent_now = 0
    max_concurrent_seen = 0
    entered = threading.Event()

    def slow_write(marker: str, release_after: float) -> None:
        nonlocal concurrent_now, max_concurrent_seen
        with counter_lock:
            concurrent_now += 1
            max_concurrent_seen = max(max_concurrent_seen, concurrent_now)
        if marker == "A":
            entered.set()  # let main() know A's thread has actually started
        try:
            time.sleep(release_after)
        finally:
            with counter_lock:
                concurrent_now -= 1

    async def _run():
        task_a = asyncio.create_task(bounded_write(slow_write, "A", 0.5))
        task_b = asyncio.create_task(bounded_write(slow_write, "B", 0.5))

        # Block until A's thread has actually opened its "connection" --
        # not just until the coroutine has been scheduled -- so the
        # cancellation below lands while a real thread is genuinely mid-write.
        await asyncio.get_event_loop().run_in_executor(None, entered.wait, 2.0)

        task_a.cancel()
        try:
            await task_a
        except asyncio.CancelledError:
            pass  # expected -- the coroutine unwinds; A's thread keeps running

        # Immediately try a 3rd write. If cancelling A wrongly freed a slot,
        # this proceeds concurrently with A's still-running thread and B,
        # pushing observed concurrency to 3.
        task_c = asyncio.create_task(bounded_write(slow_write, "C", 0.1))
        await asyncio.gather(task_b, task_c, return_exceptions=True)

    asyncio.run(_run())
    assert max_concurrent_seen <= 2, (
        f"cancelling the awaiting coroutine let a 3rd write run concurrently "
        f"with a still-in-flight one -- observed {max_concurrent_seen} "
        f"simultaneous connection-holding threads, ceiling is 2"
    )


def test_bounded_write_runs_off_the_event_loop():
    """The write must not block the caller's own coroutine from proceeding."""
    import time

    def blocking_write() -> None:
        time.sleep(0.3)

    async def _run():
        t0 = time.monotonic()
        # Two concurrent bounded_write calls; if they ran ON the event loop
        # serially without yielding, this would take >=0.6s. Run off-loop
        # via the executor, they still take >=0.3s (can't beat the sleep
        # itself) but the *loop* isn't blocked meanwhile -- proven by a
        # concurrently-scheduled no-op finishing well before either write.
        marker = {"noop_finished_at": None}

        async def noop_soon():
            await asyncio.sleep(0.05)
            marker["noop_finished_at"] = time.monotonic() - t0

        await asyncio.gather(
            bounded_write(blocking_write),
            bounded_write(blocking_write),
            noop_soon(),
        )
        return marker["noop_finished_at"]

    noop_finished_at = asyncio.run(_run())
    assert noop_finished_at is not None
    assert noop_finished_at < 0.3, (
        "a concurrently-scheduled no-op should finish well before either "
        "0.3s blocking write completes if the writes truly run off the "
        "event loop -- if this fails, bounded_write is blocking the loop"
    )


def test_flusher_write_routes_through_bounded_write():
    """Structural guard: run_flusher's coroutine body must not call
    get_connection() directly -- only its extracted sync helper may, and
    only bounded_write may invoke that helper. Prevents a future edit from
    silently reintroducing a direct, event-loop-blocking write.
    """
    from pathlib import Path
    source = Path("ingestion/realtime/flusher.py").read_text(encoding="utf-8")
    run_flusher_body = source.split("async def run_flusher")[1]
    assert "with get_connection" not in run_flusher_body, (
        "flusher.py::run_flusher calls get_connection() directly again -- "
        "route the write through db_writer.bounded_write instead"
    )
    assert "bounded_write" in run_flusher_body


def test_dex_scanner_write_routes_through_bounded_write():
    from pathlib import Path
    source = Path("ingestion/realtime/feeds/dex_scanner.py").read_text(encoding="utf-8")
    assert "await bounded_write(_write_signals" in source, (
        "dex_scanner.py must call _write_signals via bounded_write, not directly"
    )


def test_ws_listener_final_flush_routes_through_bounded_write_with_timeout():
    from pathlib import Path
    source = Path("ingestion/realtime/ws_listener.py").read_text(encoding="utf-8")
    assert "bounded_write(_write_final_flush_sync" in source
    assert "asyncio.wait_for" in source
    assert "FINAL_FLUSH_TIMEOUT_SECONDS" in source
