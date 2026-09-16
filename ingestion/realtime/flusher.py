"""Batch DB writer for realtime candles.

Drains the CandleBuilder flush queue every 5 minutes, batch-inserts into
realtime_candles using INSERT ... ON CONFLICT DO NOTHING (idempotent, same
persistence semantics as main -- unchanged by the deployment-path repair
this module is otherwise part of). Buffers in memory on DB failure, alerts
after 3 consecutive failures.

INSERT_SQL is imported (not duplicated) by ws_listener.py's shutdown
final-flush -- this is the other write path that can hit the same
(symbol, interval, ts) primary key as this module's periodic flush, when a
process restart splits one candle interval across two processes. Because
this is DO NOTHING, not a merge, whichever row lands FIRST at that primary
key wins permanently, and it is provably the truncated one (the old
process's shutdown flush always completes, successfully or not, strictly
before a new process's candle for the same bucket even starts) -- a real,
known, PRE-EXISTING risk (see tests/test_realtime_shutdown_semantics.py::
test_truncated_candle_blocks_a_later_complete_candle_for_the_same_bucket
for the proof against real Postgres) that this repair does NOT fix. See
docs/TODO-REALTIME-CANDLE-CORRECTNESS.md for what fixing it would require,
and docs/realtime_candle_merge_proposal_tests.py for a draft merge design
that was explored and then reverted out of this PR after review found real
gaps in it -- kept as a starting point for that follow-up, not shipped
here.
"""

from __future__ import annotations

import asyncio

from loguru import logger as log

from ingestion.realtime.candle_builder import CandleBuilder, CandleState
from ingestion.realtime.db_writer import bounded_write

FLUSH_INTERVAL = 300  # 5 minutes
MAX_BUFFER_CYCLES = 12  # 1 hour of candles before dropping oldest
MAX_CONSECUTIVE_FAILURES = 3

INSERT_SQL = """
    INSERT INTO realtime_candles
        (symbol, asset_class, interval, ts, open, high, low, close, volume, vwap, trade_count, source)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, interval, ts) DO NOTHING
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
