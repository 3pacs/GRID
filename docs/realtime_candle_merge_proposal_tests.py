"""DRAFT / NOT ACTIVE -- proposed candle-merge SQL and its acceptance tests,
moved out of the active `tests/` suite (this file is NOT under `testpaths`
in pytest.ini and is not collected or run by CI).

## Why this file exists

An earlier round of PR #514 (grid-realtime deployment repair) replaced
`ingestion/realtime/flusher.py`'s `INSERT_SQL` -- which does
`ON CONFLICT (symbol, interval, ts) DO NOTHING` on main -- with a
source-aware merge (`DO UPDATE`, additive for Binance, GREATEST for
everything else). Two real, tested gaps were found in that proposal (see
below), and the deployment repair itself (WorkingDirectory fix, drop-in
backup/rollback, DB-connection-lifetime semaphore, opt-in activation gate,
process-start + freshness verification) does not depend on the merge SQL
landing. Per review, the merge proposal was reverted out of #514 entirely
so the deployment repair could proceed on main's existing, unchanged
persistence semantics (DO NOTHING) without asserting this specific new
algorithm is correct -- see docs/TODO-REALTIME-CANDLE-CORRECTNESS.md for
the tracked follow-up and what a real fix requires.

This file preserves the proposal's SQL and its test suite as a concrete
starting point for whoever picks up that follow-up -- not as a currently
shipped or accepted design.

## What the proposal got right, tested here

- A same-minute Yahoo revision correctly replaces (not adds to) the prior
  partial report, via GREATEST (`test_yahoo_same_minute_revision_is_replaced_not_added_by_greatest`).
- Binance's disjoint pre-/post-restart segments correctly sum
  (`test_restart_interruption_produces_a_correctly_merged_candle`).
- An exact-duplicate batch replay is a true no-op, not a second SUM
  (`test_duplicate_delivery_does_not_inflate_totals`).
- `close` as plain last-write-wins is NOT broken by trade count the way an
  earlier (already-superseded-before-reversion) version of this same
  proposal was -- a later, correct segment with fewer trades than an
  earlier one still wins close correctly
  (`test_binance_close_follows_write_order_even_when_the_later_segment_has_fewer_trades`).

## What the proposal got WRONG or left incomplete -- known limitations,
## asserted deliberately as CURRENT (wrong) behavior, not aspirational

- `test_binance_close_under_genuine_reverse_delivery_is_a_known_limitation`:
  under genuine reverse delivery (not believed reachable via the current
  restart mechanism, but not provably unreachable either), last-write-wins
  picks up the chronologically EARLIER segment's close, not the true
  latest one. INSERT_SQL has no trade-level timestamp to check against --
  confirmed by reading feeds/binance.py directly: it reads the trade's own
  timestamp (`data["T"]`) only to compute the bucket floor, and never reads
  the trade ID (`data["t"]`) at all. Neither survives past
  `CandleBuilder.ingest()`. Fixing this needs `last_trade_id`/
  `last_trade_ts` persisted alongside the candle, not a smarter comparison
  of the aggregates already being computed.
- `test_yahoo_distinct_minutes_are_undercounted_by_greatest_a_known_limitation`:
  two genuinely different minutes within the same 5-minute bucket should
  accumulate (their volumes are both real, non-overlapping contributions),
  but GREATEST cannot tell that apart from a same-minute revision --
  `CandleState` has no per-minute breakdown. GREATEST silently keeps only
  the larger side's own total, under-counting (never inflating) the truth.
  Fixing this needs per-minute state tracked within the bucket, not a
  smarter aggregate comparison.

Also worth carrying forward: the "Binance delivers each trade exactly
once" assumption behind summing its volume/trade_count is a claim about
the *upstream* feed and this parser's own resubscribe behavior, not
something independently verified by a trade-ID dedup check (because no
trade ID is persisted to check against). And the exact-duplicate no-op
guard (equality of open/close/volume/trade_count) is a heuristic proxy for
"the same batch was resent," not proof of identical underlying trades.

## Running this file

Not part of `pytest tests/` (it lives outside `testpaths`). To run it
standalone against a local Postgres:

    pip install -e .  # if not already
    GRID_TEST_DB_URL=postgresql://user:pass@localhost:5432/dbname \\
        python -m pytest docs/realtime_candle_merge_proposal_tests.py -v

It will skip everything except the two `CandleBuilder`-only tests if no
Postgres is reachable, same as the live-DB tests in
tests/test_realtime_shutdown_semantics.py.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from ingestion.realtime.candle_builder import CandleBuilder

# Column indices in the tuples build_insert_values()-shaped rows use here:
# (symbol, asset_class, interval, ts_bucket, open, high, low, close,
#  volume, vwap, trade_count, source)
_SYMBOL, _ASSET_CLASS, _INTERVAL, _TS = 0, 1, 2, 3
_OPEN, _HIGH, _LOW, _CLOSE, _VOLUME, _VWAP, _TRADE_COUNT, _SOURCE = range(4, 12)


def build_insert_values(candles) -> list[tuple]:
    """Local copy of flusher.py's build_insert_values -- this file is
    intentionally decoupled from the shipped module so it keeps working
    unmodified regardless of what flusher.py's own INSERT_SQL does.
    """
    rows = []
    for c in candles:
        rows.append((
            c.symbol, c.asset_class, c.interval, c.ts_bucket,
            c.open, c.high, c.low, c.close, c.volume,
            c.vwap, c.trade_count, c.source,
        ))
    return rows


# The proposed merge SQL, exactly as it stood when reverted out of #514 --
# see the module docstring above for what's proven correct and what's a
# documented, tested-as-such limitation.
PROPOSED_INSERT_SQL = """
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
                CASE WHEN (realtime_candles.volume + EXCLUDED.volume) > 0 THEN
                    (COALESCE(realtime_candles.vwap, 0) * realtime_candles.volume
                     + COALESCE(EXCLUDED.vwap, 0) * EXCLUDED.volume)
                    / (realtime_candles.volume + EXCLUDED.volume)
                ELSE NULL END
            WHEN EXCLUDED.volume >= realtime_candles.volume THEN EXCLUDED.vwap
            ELSE realtime_candles.vwap
        END
"""


def _connect_to_real_test_db():
    from sqlalchemy import create_engine

    candidates = [
        "postgresql://grid:testpass@localhost:5432/griddb_test",
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
    pg_engine = _connect_to_real_test_db()
    if pg_engine is None:
        pytest.skip("PostgreSQL not available")

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
    import psycopg2.extras

    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            psycopg2.extras.execute_batch(cur, PROPOSED_INSERT_SQL, rows)
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
    engine, symbol = realtime_candles_schema

    old_builder = CandleBuilder()
    old_builder.ingest(
        symbol, 180.0, 100,
        datetime(2026, 9, 16, 14, 7, 23, tzinfo=timezone.utc),
        "crypto", "binance",
    )
    old_builder.flush_all()
    truncated_rows = build_insert_values(old_builder.drain())

    new_builder = CandleBuilder()
    for minute, second, price in [(7, 40, 181.0), (8, 10, 179.0), (8, 45, 182.0), (9, 30, 183.5)]:
        new_builder.ingest(
            symbol, price, 100,
            datetime(2026, 9, 16, 14, minute, second, tzinfo=timezone.utc),
            "crypto", "binance",
        )
    new_builder.ingest(
        symbol, 184.0, 100,
        datetime(2026, 9, 16, 14, 10, 5, tzinfo=timezone.utc),
        "crypto", "binance",
    )
    complete_rows = build_insert_values(new_builder.drain())
    assert len(complete_rows) == 1

    _write_rows(engine, truncated_rows)
    _write_rows(engine, complete_rows)

    open_, high, low, close, volume, vwap, trade_count = _read_candle(
        engine, symbol, truncated_rows[0][_INTERVAL], truncated_rows[0][_TS]
    )

    assert open_ == pytest.approx(180.0)
    assert high == pytest.approx(183.5)
    assert low == pytest.approx(179.0)
    assert close == pytest.approx(183.5)
    assert volume == pytest.approx(500.0)
    assert trade_count == 5
    assert vwap == pytest.approx(181.1)


def test_duplicate_delivery_does_not_inflate_totals(realtime_candles_schema):
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

    _write_rows(engine, rows)
    first = _read_candle(engine, symbol, rows[0][_INTERVAL], rows[0][_TS])
    _write_rows(engine, rows)
    second = _read_candle(engine, symbol, rows[0][_INTERVAL], rows[0][_TS])

    assert second == first
    assert second[4] == pytest.approx(150.0)
    assert second[6] == 3


def test_binance_close_follows_write_order_even_when_the_later_segment_has_fewer_trades(realtime_candles_schema):
    engine, symbol = realtime_candles_schema

    busy_builder = CandleBuilder()
    for minute, second, price in [(5, 3, 100.0), (6, 0, 101.0), (7, 0, 102.0)]:
        busy_builder.ingest(
            symbol, price, 10,
            datetime(2026, 9, 16, 14, minute, second, tzinfo=timezone.utc),
            "crypto", "binance",
        )
    busy_builder.flush_all()
    busy_rows = build_insert_values(busy_builder.drain())

    quiet_builder = CandleBuilder()
    quiet_builder.ingest(
        symbol, 999.0, 5,
        datetime(2026, 9, 16, 14, 7, 50, tzinfo=timezone.utc),
        "crypto", "binance",
    )
    quiet_builder.ingest(
        symbol, 1.0, 1,
        datetime(2026, 9, 16, 14, 10, 1, tzinfo=timezone.utc),
        "crypto", "binance",
    )
    quiet_rows = build_insert_values(quiet_builder.drain())

    _write_rows(engine, busy_rows)
    _write_rows(engine, quiet_rows)

    open_, high, low, close, volume, vwap, trade_count = _read_candle(
        engine, symbol, busy_rows[0][_INTERVAL], busy_rows[0][_TS]
    )
    assert close == pytest.approx(999.0)


def test_binance_close_under_genuine_reverse_delivery_is_a_known_limitation(realtime_candles_schema):
    """KNOWN LIMITATION -- asserts the current wrong outcome deliberately.
    Not acceptance evidence that this design is done; a record of the open
    problem for whoever implements the real fix (persist trade-level
    identity/order, don't add another aggregate comparison here).
    """
    engine, symbol = realtime_candles_schema

    later_builder = CandleBuilder()
    later_builder.ingest(
        symbol, 999.0, 10,
        datetime(2026, 9, 16, 14, 9, 0, tzinfo=timezone.utc),
        "crypto", "binance",
    )
    later_builder.flush_all()
    later_rows = build_insert_values(later_builder.drain())

    earlier_builder = CandleBuilder()
    earlier_builder.ingest(
        symbol, 100.0, 10,
        datetime(2026, 9, 16, 14, 6, 0, tzinfo=timezone.utc),
        "crypto", "binance",
    )
    earlier_builder.flush_all()
    earlier_rows = build_insert_values(earlier_builder.drain())

    _write_rows(engine, later_rows)
    _write_rows(engine, earlier_rows)

    open_, high, low, close, volume, vwap, trade_count = _read_candle(
        engine, symbol, later_rows[0][_INTERVAL], later_rows[0][_TS]
    )
    assert close == pytest.approx(100.0), "wrong vs true-latest 999.0 -- see module docstring"


def test_yahoo_same_minute_revision_is_replaced_not_added_by_greatest(realtime_candles_schema):
    engine, symbol = realtime_candles_schema

    partial_builder = CandleBuilder()
    partial_builder.ingest(
        symbol, 50.0, 150,
        datetime(2026, 9, 16, 14, 6, 0, tzinfo=timezone.utc),
        "equity", "yahoo",
    )
    partial_builder.flush_all()
    partial_rows = build_insert_values(partial_builder.drain())

    revised_builder = CandleBuilder()
    revised_builder.ingest(
        symbol, 51.0, 200,
        datetime(2026, 9, 16, 14, 6, 45, tzinfo=timezone.utc),
        "equity", "yahoo",
    )
    revised_builder.flush_all()
    revised_rows = build_insert_values(revised_builder.drain())

    _write_rows(engine, partial_rows)
    _write_rows(engine, revised_rows)

    open_, high, low, close, volume, vwap, trade_count = _read_candle(
        engine, symbol, partial_rows[0][_INTERVAL], partial_rows[0][_TS]
    )
    assert volume == pytest.approx(200.0)


def test_yahoo_distinct_minutes_are_undercounted_by_greatest_a_known_limitation(realtime_candles_schema):
    """KNOWN LIMITATION -- see module docstring. Not acceptance evidence."""
    engine, symbol = realtime_candles_schema

    minute05_builder = CandleBuilder()
    minute05_builder.ingest(
        symbol, 50.0, 100,
        datetime(2026, 9, 16, 14, 5, 10, tzinfo=timezone.utc),
        "equity", "yahoo",
    )
    minute05_builder.flush_all()
    minute05_rows = build_insert_values(minute05_builder.drain())

    minute07_builder = CandleBuilder()
    minute07_builder.ingest(
        symbol, 53.0, 120,
        datetime(2026, 9, 16, 14, 7, 10, tzinfo=timezone.utc),
        "equity", "yahoo",
    )
    minute07_builder.flush_all()
    minute07_rows = build_insert_values(minute07_builder.drain())

    _write_rows(engine, minute05_rows)
    _write_rows(engine, minute07_rows)

    open_, high, low, close, volume, vwap, trade_count = _read_candle(
        engine, symbol, minute05_rows[0][_INTERVAL], minute05_rows[0][_TS]
    )
    true_total = 100.0 + 120.0
    assert volume == pytest.approx(120.0), f"under-counts true total {true_total} -- see module docstring"
