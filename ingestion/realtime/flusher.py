"""Batch DB writer for realtime candles.

Drains the CandleBuilder flush queue every 5 minutes, batch-inserts into
realtime_candles. Buffers in memory on DB failure, alerts after 3
consecutive failures.

INSERT_SQL merges on conflict rather than discarding (see its own comment
for the exact algebra and why it's safe) -- this is the other write path
that can hit the same (symbol, interval, ts) primary key as
ws_listener.py's shutdown final-flush, when a process restart splits one
candle interval across two processes. Both write paths MUST use the
identical merge SQL; keep them in sync if either changes.
"""

from __future__ import annotations

import asyncio

from loguru import logger as log

from ingestion.realtime.candle_builder import CandleBuilder, CandleState
from ingestion.realtime.db_writer import bounded_write

FLUSH_INTERVAL = 300  # 5 minutes
MAX_BUFFER_CYCLES = 12  # 1 hour of candles before dropping oldest
MAX_CONSECUTIVE_FAILURES = 3

# On conflict, MERGE the two partial candles rather than discard the new
# one. Restart interruption is the only realistic way this conflict fires
# (see candle_builder.py: CandleBuilder never re-creates a candle for a
# bucket it has already flushed within one process's lifetime) -- and
# because a shutdown's final flush always completes before the next
# process's first tick, the row ALREADY in the table is always the
# chronologically EARLIER partial candle, and EXCLUDED (the incoming row)
# is always the LATER one. That ordering is what makes this algebra safe:
#   - open: untouched (omitted from SET) -- the existing row's open is
#     always the true interval open, since it was there from the first tick.
#   - high/low: GREATEST/LEAST -- commutative, correct regardless of order.
#   - close: EXCLUDED.close -- the incoming row is always later, so its
#     close is the more recent price. NOT commutative -- this is the one
#     field that depends on the ordering guarantee above.
#   - volume, trade_count: summed -- each partial candle counts genuinely
#     different ticks (before vs. after the restart), so this reconstructs
#     the true total rather than picking one side.
#   - vwap: recomputed from the two partials' own (vwap, volume) pairs --
#     the table stores only the reduced ratio, not vwap_numerator/
#     vwap_denominator separately, but numerator = vwap * volume, so the
#     combined weighted average is recoverable from what's already stored.
#     COALESCEs to 0 contribution when a partial had zero volume (vwap NULL
#     in that case, per CandleState.vwap).
# See tests/test_realtime_shutdown_semantics.py for the traced,
# live-database-verified regression covering this exact scenario.
INSERT_SQL = """
    INSERT INTO realtime_candles
        (symbol, asset_class, interval, ts, open, high, low, close, volume, vwap, trade_count, source)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, interval, ts) DO UPDATE SET
        high = GREATEST(realtime_candles.high, EXCLUDED.high),
        low = LEAST(realtime_candles.low, EXCLUDED.low),
        close = EXCLUDED.close,
        volume = realtime_candles.volume + EXCLUDED.volume,
        vwap = CASE
            WHEN (realtime_candles.volume + EXCLUDED.volume) > 0 THEN
                (COALESCE(realtime_candles.vwap, 0) * realtime_candles.volume
                 + COALESCE(EXCLUDED.vwap, 0) * EXCLUDED.volume)
                / (realtime_candles.volume + EXCLUDED.volume)
            ELSE NULL
        END,
        trade_count = realtime_candles.trade_count + EXCLUDED.trade_count
"""


def build_insert_values(candles: list[CandleState]) -> list[tuple]:
    """Convert CandleState list to INSERT value tuples."""
    rows = []
    for c in candles:
        rows.append((
            c.symbol, c.asset_class, c.interval, c.ts_bucket,
            c.open, c.high, c.low, c.close, c.volume,
            c.vwap,  # property: vwap_numerator/vwap_denominator or None
            c.trade_count, c.source,
        ))
    return rows


def _write_batch_sync(rows: list[tuple]) -> None:
    """Synchronous batch insert -- run off the event loop via bounded_write."""
    from db import get_connection
    from psycopg2.extras import execute_batch

    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, INSERT_SQL, rows, page_size=500)


async def run_flusher(builder: CandleBuilder) -> None:
    """Periodically drain candle builder and batch-insert to DB. Runs forever."""
    buffer: list[CandleState] = []
    consecutive_failures = 0

    while True:
        await asyncio.sleep(FLUSH_INTERVAL)

        try:
            drained = builder.drain()
            if drained:
                buffer.extend(drained)

            if not buffer:
                continue

            # Enforce max buffer size
            max_candles = MAX_BUFFER_CYCLES * 100
            if len(buffer) > max_candles:
                dropped = len(buffer) - max_candles
                buffer = buffer[dropped:]
                log.warning("Dropped {n} oldest buffered candles (buffer overflow)", n=dropped)

            rows = build_insert_values(buffer)
            await bounded_write(_write_batch_sync, rows)

            log.info(
                "Flushed {n} candles to realtime_candles ({syms} symbols)",
                n=len(rows), syms=len({r[0] for r in rows}),
            )
            buffer.clear()
            consecutive_failures = 0

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            consecutive_failures += 1
            log.error(
                "Candle flush failed ({n}/{max}): {err}",
                n=consecutive_failures, max=MAX_CONSECUTIVE_FAILURES, err=str(exc),
            )
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                try:
                    from alerts.email import alert_on_failure
                    alert_on_failure("Realtime candle flusher", str(exc))
                except Exception:
                    pass
