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
import sys

from loguru import logger as log

from ingestion.realtime.candle_builder import CandleBuilder
from ingestion.realtime.feeds.binance import run_binance_feed
from ingestion.realtime.feeds.dex_scanner import run_dex_scanner
from ingestion.realtime.feeds.yahoo import run_yahoo_feed
from ingestion.realtime.flusher import run_flusher


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

    # Flush remaining candles
    log.info("Flushing {n} remaining candles...", n=builder.active_symbols)
    builder.flush_all()
    drained = builder.drain()
    if drained:
        try:
            from ingestion.realtime.flusher import build_insert_values
            from db import get_connection

            rows = build_insert_values(drained)
            with get_connection() as conn:
                with conn.cursor() as cur:
                    from psycopg2.extras import execute_batch
                    execute_batch(
                        cur,
                        "INSERT INTO realtime_candles "
                        "(symbol, asset_class, interval, ts, open, high, low, close, volume, vwap, trade_count, source) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                        "ON CONFLICT (symbol, interval, ts) DO NOTHING",
                        rows,
                        page_size=500,
                    )
            log.info("Final flush: {n} candles written", n=len(rows))
        except Exception as exc:
            log.error("Final flush failed: {err}", err=str(exc))

    log.info("GRID Realtime Listener shut down cleanly")


if __name__ == "__main__":
    asyncio.run(main())
