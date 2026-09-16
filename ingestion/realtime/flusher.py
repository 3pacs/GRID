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
# and is NOT the same for every source, and is NOT complete even within a
# source. Traced against the real feed code (feeds/binance.py, feeds/
# yahoo.py) and candle_builder.py's CandleState, not assumed:
#
#   - binance.py: each WebSocket message carries its own trade ID (data["t"])
#     and execution timestamp (data["T"]) from Binance's @trade stream. The
#     parser reads data["T"] only to compute the candle bucket and never
#     stores it; data["t"] is never read at all -- neither survives past
#     CandleBuilder.ingest(). So summing volume/trade_count for this source
#     rests on an ASSUMPTION about the upstream feed ("each trade is
#     delivered exactly once, a reconnect only resumes from 'now'"), not on
#     anything this code independently verifies -- there is no trade-ID dedup
#     check that would catch an accidental redelivery, because no trade ID
#     is persisted to check.
#
#   - yahoo.py: each poll re-downloads the LATEST known 1-minute bar and
#     re-ingests it as a tick timestamped at POLL time, not the bar's own
#     time. CandleState tracks only the running 5-minute-bucket total, with
#     no per-minute breakdown -- so a same-minute revision (should replace
#     that minute's prior contribution) and a genuinely different minute
#     within the same bucket (should add to it) look identical at merge
#     time. GREATEST (below) handles the first case correctly by
#     construction (a growing revision of one minute naturally has more
#     volume, so GREATEST picks it) but silently under-counts the second --
#     it returns only the larger side's own total, not the true union of two
#     non-overlapping minutes. Never inflates, but not a full reconstruction
#     either. See docs/TODO-REALTIME-CANDLE-CORRECTNESS.md for what fixing
#     this for real would require (per-minute tracking) -- out of scope here.
#
# So: additive (SUM) for source='binance' (an assumption, not a verified
# guarantee -- see above), non-additive (GREATEST -- trust whichever side
# has accumulated more, never both) for every other source. GREATEST/LEAST
# are safe unconditionally in the sense of never inflating (idempotent under
# duplication, and never claim more than the larger side's own total) --
# only a true SUM can inflate, so only the binance branch needs the
# duplicate guard below to matter.
#
# Two more properties, independent of source:
#
#   1. Exact-duplicate resubmission (the SAME batch replayed -- e.g. if
#      flusher.py's own retry-on-ambiguous-failure path resends a buffer
#      whose previous write actually succeeded server-side) is a no-op for
#      volume/trade_count/vwap when incoming open/close/volume/trade_count
#      all match what's already stored. This is a heuristic proxy for "the
#      same batch was resent," not proof of identical underlying trades --
#      two genuinely different trade sets could in principle produce
#      identical aggregates and be wrongly treated as a duplicate. That
#      failure mode under-counts (a missed real update), never inflates,
#      which is the direction judged acceptable given the alternative is
#      unconditionally double-summing on every retry.
#
#   2. `close` is plain "last write wins" (EXCLUDED.close, unconditionally,
#      not gated by the duplicate check -- a duplicate write has the same
#      close either way). This is correct for the only reachable production
#      ordering: a restart's old-segment shutdown flush provably completes,
#      successfully or not, strictly before the new process's first write is
#      even possible, so EXCLUDED is always the chronologically later side.
#      It is deliberately NOT based on trade_count or any other completeness
#      proxy -- an earlier version of this SQL did exactly that ("more
#      accumulated observations = more likely to include the true latest
#      tick"), which is unsound: trade count reflects how busy a segment's
#      time window was, not when it occurred, so a later, correct segment
#      can easily have FEWER trades than an earlier one and would have been
#      wrongly overridden. Plain last-write-wins also does not claim to
#      solve genuine reverse delivery (the write arriving second is not
#      actually the chronologically later one) -- that is a real, open,
#      tested-as-a-known-limitation gap (see
#      tests/test_realtime_shutdown_semantics.py and
#      docs/TODO-REALTIME-CANDLE-CORRECTNESS.md), not something aggregate
#      values alone (trade count, or anything else derived from them) can
#      resolve without real trade-level identity/order data.
#
# See tests/test_realtime_shutdown_semantics.py for the traced,
# live-database-verified regressions covering all of this.
INSERT_SQL = """
    INSERT INTO realtime_candles
        (symbol, asset_class, interval, ts, open, high, low, close, volume, vwap, trade_count, source)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, interval, ts) DO UPDATE SET
        high = GREATEST(realtime_candles.high, EXCLUDED.high),
        low = LEAST(realtime_candles.low, EXCLUDED.low),
        close = EXCLUDED.close,
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
            -- Non-binance: vwap must stay consistent with whichever side's
            -- volume the GREATEST above actually kept -- reusing that same
            -- comparison, not a new independent rule (see property 2 above
            -- for why introducing another independent completeness proxy
            -- here would repeat the same mistake close's old trade_count
            -- rule made).
            WHEN EXCLUDED.volume >= realtime_candles.volume THEN EXCLUDED.vwap
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
