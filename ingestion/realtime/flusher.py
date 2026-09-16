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
# one -- but the correct merge depends on what the source actually delivers,
# and is NOT the same for every source. Traced against the real feed code
# (ingestion/realtime/feeds/binance.py, feeds/yahoo.py):
#
#   - binance.py: each WebSocket message is one individually-executed trade
#     (Binance's own @trade stream semantics -- data["q"] is that ONE
#     trade's own quantity, data["T"] its own execution time; reconnecting
#     never replays a trade already delivered, it only resumes from "now").
#     Two partial candles from the same bucket, split by a restart, cover
#     genuinely DIFFERENT, non-overlapping sets of trades. Summing
#     volume/trade_count reconstructs the true total -- this is the
#     "incremental observations" case.
#
#   - yahoo.py: each poll re-downloads the LATEST known 1-minute bar
#     (`yf.download(..., interval="1m")`, then `.iloc[-1]`) and re-ingests
#     it as a "tick" timestamped at POLL time, not the bar's own time. If
#     Yahoo's data hasn't advanced between two polls (its own refresh
#     lag is often >60s), or if the old process's last poll and the new
#     process's first poll both land within the same still-"latest" bar's
#     window, BOTH partial candles can include a contribution from the
#     SAME underlying bar. This is the "complete snapshot" case: summing
#     volume/trade_count here can double-count that one shared bar. (This
#     poll-vs-bar mismatch is a pre-existing property of yahoo.py's design,
#     not introduced here -- it can already inflate a single process's own
#     candle across two consecutive polls, independent of any restart. Not
#     addressed here: this SQL is about not making the restart-merge case
#     WORSE than what a single process already does, not rearchitecting
#     yahoo.py's poll-to-tick mapping, which is a separate, larger change.)
#
# So: additive (SUM) for source='binance', non-additive (GREATEST -- trust
# whichever side has accumulated more, never both) for every other source.
# GREATEST/LEAST are safe unconditionally because they're idempotent under
# duplication (max of two equal numbers is that same number, no inflation
# risk) -- only a true SUM can inflate, so only the binance branch needs the
# stronger "these are genuinely disjoint" guarantee.
#
# Two more properties, independent of source:
#
#   1. Exact-duplicate resubmission (the SAME batch replayed -- e.g. if
#      flusher.py's own retry-on-ambiguous-failure path resends a buffer
#      whose previous write actually succeeded server-side) must be a true
#      no-op, not a second SUM. Guarded explicitly below: when incoming
#      open/close/volume/trade_count all match what's already stored, every
#      field is left unchanged, regardless of source.
#
#   2. "Which side is later" for `close` is decided by trade_count (more
#      accumulated observations = more complete = more likely to include
#      the true latest tick), NOT by which row happens to be EXCLUDED. In
#      the realistic case (systemd `restart` = stop-then-start, strictly
#      sequential) the existing row is always older and EXCLUDED always has
#      more, so this agrees with "trust EXCLUDED" anyway -- but unlike that
#      simpler rule, this one is also correct if a write is ever reordered
#      or replayed out of sequence: a less-complete row can never overwrite
#      a more-complete row's close. On an exact trade_count tie, prefers
#      the incoming row (a reasonable default when completeness alone can't
#      decide).
#
# See tests/test_realtime_shutdown_semantics.py for the traced,
# live-database-verified regressions covering all of this: the disjoint
# pre-/post-restart case, exact-duplicate replay, overlapping (Yahoo-style)
# batches, and reverse arrival order.
INSERT_SQL = """
    INSERT INTO realtime_candles
        (symbol, asset_class, interval, ts, open, high, low, close, volume, vwap, trade_count, source)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, interval, ts) DO UPDATE SET
        high = GREATEST(realtime_candles.high, EXCLUDED.high),
        low = LEAST(realtime_candles.low, EXCLUDED.low),
        close = CASE
            WHEN realtime_candles.open = EXCLUDED.open
                 AND realtime_candles.close = EXCLUDED.close
                 AND realtime_candles.volume = EXCLUDED.volume
                 AND realtime_candles.trade_count = EXCLUDED.trade_count
                THEN realtime_candles.close  -- exact duplicate: no-op
            WHEN EXCLUDED.trade_count >= realtime_candles.trade_count THEN EXCLUDED.close
            ELSE realtime_candles.close
        END,
        volume = CASE
            WHEN realtime_candles.open = EXCLUDED.open
                 AND realtime_candles.close = EXCLUDED.close
                 AND realtime_candles.volume = EXCLUDED.volume
                 AND realtime_candles.trade_count = EXCLUDED.trade_count
                THEN realtime_candles.volume  -- exact duplicate: no-op
            WHEN realtime_candles.source = 'binance' THEN realtime_candles.volume + EXCLUDED.volume
            ELSE GREATEST(realtime_candles.volume, EXCLUDED.volume)
        END,
        trade_count = CASE
            WHEN realtime_candles.open = EXCLUDED.open
                 AND realtime_candles.close = EXCLUDED.close
                 AND realtime_candles.volume = EXCLUDED.volume
                 AND realtime_candles.trade_count = EXCLUDED.trade_count
                THEN realtime_candles.trade_count  -- exact duplicate: no-op
            WHEN realtime_candles.source = 'binance' THEN realtime_candles.trade_count + EXCLUDED.trade_count
            ELSE GREATEST(realtime_candles.trade_count, EXCLUDED.trade_count)
        END,
        vwap = CASE
            WHEN realtime_candles.open = EXCLUDED.open
                 AND realtime_candles.close = EXCLUDED.close
                 AND realtime_candles.volume = EXCLUDED.volume
                 AND realtime_candles.trade_count = EXCLUDED.trade_count
                THEN realtime_candles.vwap  -- exact duplicate: no-op
            WHEN realtime_candles.source = 'binance' THEN
                -- vwap_numerator = vwap * volume (the table stores only the
                -- reduced ratio, not the numerator/denominator separately,
                -- but this recovers it from what IS stored).
                CASE WHEN (realtime_candles.volume + EXCLUDED.volume) > 0 THEN
                    (COALESCE(realtime_candles.vwap, 0) * realtime_candles.volume
                     + COALESCE(EXCLUDED.vwap, 0) * EXCLUDED.volume)
                    / (realtime_candles.volume + EXCLUDED.volume)
                ELSE NULL END
            WHEN EXCLUDED.trade_count >= realtime_candles.trade_count THEN EXCLUDED.vwap
            ELSE realtime_candles.vwap
        END
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
