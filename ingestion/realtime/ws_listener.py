"""GRID Realtime Market Data Listener.

Main daemon entry point. Launches four async tasks:
1. Binance WebSocket — 31 crypto trade streams
2. Yahoo Finance poller — 31 traditional market symbols
3. DEX scanner — GeckoTerminal + DexScreener liquidity spikes
4. Candle flusher — batch INSERT to realtime_candles every 5 minutes

Run as: python -m ingestion.realtime.ws_listener
"""

from __future__ import annotations

import asyncio
import signal

from loguru import logger as log

from ingestion.realtime.candle_builder import CandleBuilder
from ingestion.realtime.db_writer import bounded_write
from ingestion.realtime.feeds.binance import run_binance_feed
from ingestion.realtime.feeds.dex_scanner import run_dex_scanner
from ingestion.realtime.feeds.yahoo import run_yahoo_feed
# INSERT_SQL is imported, not duplicated: this final flush and flusher.py's
# periodic flush are the two write paths that can conflict on the same
# (symbol, interval, ts) primary key across a restart (see INSERT_SQL's own
# comment in flusher.py for the merge algebra and why it's safe). Importing
# instead of copy-pasting the SQL means they cannot drift out of sync.
from ingestion.realtime.flusher import INSERT_SQL, build_insert_values, run_flusher

# Bounds the final flush so a stalled write can't hang the whole shutdown
# sequence indefinitely -- leaves comfortable margin under grid-realtime's
# systemd TimeoutStopUSec (90s default, unset in the unit) for task
# cancellation and everything else in main() to also complete.
FINAL_FLUSH_TIMEOUT_SECONDS = 30


def _write_final_flush_sync(rows: list[tuple]) -> None:
    """Synchronous final-flush insert -- run off the event loop via bounded_write."""
    from db import get_connection
    from psycopg2.extras import execute_batch

    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, INSERT_SQL, rows, page_size=500)


async def main() -> None:
    """Launch all feeds and the flusher, handle graceful shutdown."""
    builder = CandleBuilder()
    log.info("GRID Realtime Listener starting — 4 async tasks")

    tasks = [
        asyncio.create_task(run_binance_feed(builder), name="binance"),
        asyncio.create_task(run_yahoo_feed(builder), name="yahoo"),
        asyncio.create_task(run_dex_scanner(builder), name="dex_scanner"),
        asyncio.create_task(run_flusher(builder), name="flusher"),
    ]

    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _handle_signal(sig: int) -> None:
        log.info("Received signal {s} — initiating graceful shutdown", s=sig)
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal, sig)

    await shutdown_event.wait()

    log.info("Cancelling feed tasks...")
    for t in tasks:
        t.cancel()

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for t, r in zip(tasks, results):
        if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError):
            log.error("Task {name} failed: {err}", name=t.get_name(), err=str(r))

    # Flush remaining candles.
    #
    # What this guarantees, precisely -- not "nothing is lost":
    #   - flush_all() moves every in-progress candle (including the one
    #     started seconds ago) into the flush queue synchronously, in
    #     memory, before we ever touch the DB. That part cannot fail.
    #   - Each such candle keeps the ts_bucket it was assigned at creation
    #     (CandleBuilder._bucket_floor -- the interval's start, e.g.
    #     14:05:00 for a candle spanning 14:05-14:10), not a "final
    #     timestamp" reflecting when it was cut short. So it is written
    #     truncated (fewer ticks than a full interval), not under some
    #     other identity -- and if the next process (started fresh after
    #     this restart) later builds a *complete* candle for that same
    #     bucket, its own flush uses the identical (symbol, interval, ts)
    #     key. INSERT_SQL (flusher.py) MERGES on that conflict -- but NOT
    #     identically for every source. See INSERT_SQL's own comment for the
    #     full reasoning (binance.py's trades are genuinely non-overlapping
    #     and safe to sum; yahoo.py re-polls the same "latest bar" and can
    #     overlap with itself across the restart boundary, so it widens
    #     high/low but does not sum volume/trade_count), plus the
    #     exact-duplicate-replay no-op guard and the trade_count-based
    #     (not arrival-order-based) rule for which side's close wins. So the
    #     final persisted candle reconstructs as much of the true interval
    #     as the source's own delivery guarantees allow, without ever
    #     inflating totals on a replay or letting a less-complete write
    #     clobber a more-complete one's close.
    #     See tests/test_realtime_shutdown_semantics.py for this traced
    #     end-to-end against real Postgres: the disjoint pre-/post-restart
    #     case, exact-duplicate replay, overlapping (Yahoo-style) batches,
    #     and reverse arrival order.
    #   - The actual DB write below CAN fail (unreachable DB, exhausted
    #     slots outright, a query timeout) or simply run out of time. On
    #     either, the exception/timeout is caught and logged, and the
    #     process still exits -- that batch is genuinely lost, not
    #     retried, because no later flush is coming.
    #   - The write itself runs off the event loop (bounded_write ->
    #     run_in_executor) and under a hard timeout, specifically so a
    #     slow database cannot also delay *this* code from running in the
    #     first place. Traced empirically (see this change's PR
    #     description): unlike a slow synchronous call made directly on
    #     the event loop -- which blocks signal handling itself, the
    #     actual failure mode this bounded_write rollout targets in
    #     flusher.py and dex_scanner.py -- an executor-wrapped call does
    #     NOT block asyncio.gather() or the code after it; cancellation is
    #     delivered promptly regardless of how long the underlying thread
    #     keeps running in the background.
    log.info("Flushing {n} remaining candles...", n=builder.active_symbols)
    builder.flush_all()
    drained = builder.drain()
    if drained:
        rows = build_insert_values(drained)
        try:
            await asyncio.wait_for(
                bounded_write(_write_final_flush_sync, rows),
                timeout=FINAL_FLUSH_TIMEOUT_SECONDS,
            )
            log.info("Final flush: {n} candles written", n=len(rows))
        except asyncio.TimeoutError:
            log.error(
                "Final flush timed out after {s}s -- {n} candles NOT written, not retried",
                s=FINAL_FLUSH_TIMEOUT_SECONDS, n=len(rows),
            )
        except Exception as exc:
            log.error(
                "Final flush failed: {err} -- {n} candles NOT written, not retried",
                err=str(exc), n=len(rows),
            )

    log.info("GRID Realtime Listener shut down cleanly")


if __name__ == "__main__":
    asyncio.run(main())
