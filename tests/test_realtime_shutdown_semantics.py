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
flusher.py's periodic flush) use ON CONFLICT (symbol, interval, ts) DO
NOTHING, so whichever row lands FIRST wins permanently -- and it is
provably the truncated one, since the old process's shutdown flush always
completes before a new process's candle for the same bucket even starts.
"Idempotent" (no duplicate/corrupt rows) is true of this; "the most
complete data wins" is not, and that's the behavior this file pins down so
a future change to either write path can't silently alter it in either
direction without a test noticing.

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
from datetime import datetime, timezone

from ingestion.realtime.candle_builder import CandleBuilder
from ingestion.realtime.db_writer import bounded_write
from ingestion.realtime.flusher import build_insert_values

# Column indices in the tuples build_insert_values() produces:
# (symbol, asset_class, interval, ts_bucket, open, high, low, close,
#  volume, vwap, trade_count, source)
_SYMBOL, _ASSET_CLASS, _INTERVAL, _TS = 0, 1, 2, 3
_OPEN, _HIGH, _LOW, _CLOSE, _VOLUME, _VWAP, _TRADE_COUNT, _SOURCE = range(4, 12)


class _FakeCandlesTable:
    """Minimal stand-in for realtime_candles honoring its real PK/conflict contract."""

    def __init__(self) -> None:
        self.rows: dict[tuple, tuple] = {}

    def insert_on_conflict_do_nothing(self, rows: list[tuple]) -> None:
        for row in rows:
            key = (row[_SYMBOL], row[_INTERVAL], row[_TS])
            if key in self.rows:
                continue  # DO NOTHING -- exact Postgres semantics for this PK
            self.rows[key] = row


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


def test_truncated_candle_blocks_a_later_complete_candle_for_the_same_bucket():
    """The actual interruption/restart scenario, traced through real code.

    Old process gets SIGTERM mid-interval, flushes a 1-tick truncated
    candle for the 14:05:00 bucket. New process starts immediately,
    ingests the rest of that same interval (4 more ticks) before crossing
    into the next bucket, and its own flush tries to write a materially
    more complete candle for the identical bucket. Both go through the
    real build_insert_values() into a fake table enforcing the real
    PRIMARY KEY (symbol, interval, ts) / ON CONFLICT DO NOTHING contract.
    """
    table = _FakeCandlesTable()

    # --- Old process: SIGTERM arrives at 14:07:23, one tick already in ---
    old_builder = CandleBuilder()
    old_builder.ingest(
        "AAPL", 180.0, 100,
        datetime(2026, 9, 16, 14, 7, 23, tzinfo=timezone.utc),
        "equity", "yahoo",
    )
    old_builder.flush_all()
    truncated_rows = build_insert_values(old_builder.drain())
    table.insert_on_conflict_do_nothing(truncated_rows)  # old process's shutdown flush

    # --- New process: starts immediately, ingests the rest of 14:05-14:10 ---
    new_builder = CandleBuilder()
    for minute, second, price in [(7, 40, 181.0), (8, 10, 179.0), (8, 45, 182.0), (9, 30, 183.5)]:
        new_builder.ingest(
            "AAPL", price, 100,
            datetime(2026, 9, 16, 14, minute, second, tzinfo=timezone.utc),
            "equity", "yahoo",
        )
    # Crossing into 14:10:00 flushes the now-complete 14:05:00 candle.
    new_builder.ingest(
        "AAPL", 184.0, 100,
        datetime(2026, 9, 16, 14, 10, 5, tzinfo=timezone.utc),
        "equity", "yahoo",
    )
    complete_rows = build_insert_values(new_builder.drain())
    assert len(complete_rows) == 1  # only the completed 14:05:00 candle, not the new 14:10:00 one
    table.insert_on_conflict_do_nothing(complete_rows)  # new process's next flush

    truncated_key = (truncated_rows[0][_SYMBOL], truncated_rows[0][_INTERVAL], truncated_rows[0][_TS])
    complete_key = (complete_rows[0][_SYMBOL], complete_rows[0][_INTERVAL], complete_rows[0][_TS])
    assert truncated_key == complete_key, "both candles must target the identical primary key"

    # The truncated row won: written first, and DO NOTHING never lets the
    # later, more complete one replace it.
    persisted = table.rows[truncated_key]
    assert persisted[_TRADE_COUNT] == 1, "the surviving row is the 1-tick truncated candle"
    assert persisted[_CLOSE] == 180.0

    # Spell out exactly what was silently discarded.
    assert complete_rows[0][_TRADE_COUNT] == 4, "the more-complete candle had 4 ticks"
    assert complete_rows[0][_TRADE_COUNT] not in (persisted[_TRADE_COUNT],)


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
