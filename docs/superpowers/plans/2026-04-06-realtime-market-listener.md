# Realtime Market Data Listener — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-[[development]] (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a 24/7 async daemon that streams crypto via Binance WebSocket, polls traditional markets via Yahoo Finance, scans DEX tokens for liquidity spikes, builds 5-minute OHLCV candles in memory, and batch-flushes to [[PostgreSQL]].

**[[architecture|Architecture]]:** Single Python process running `asyncio` with four concurrent tasks (Binance WS, Yahoo poller, DEX scanner, DB flusher). Candles aggregated in an in-memory `CandleBuilder`, flushed every 5 minutes. DEX spikes written to existing `signal_data` table.

**Tech Stack:** Python asyncio, websockets, aiohttp, yfinance, psycopg2, [[SQLAlchemy]], systemd

**Spec:** `docs/superpowers/specs/2026-04-06-realtime-market-listener-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `ingestion/realtime/__init__.py` | Create | Package marker |
| `ingestion/realtime/candle_builder.py` | Create | In-memory OHLCV candle aggregator with flush queue |
| `ingestion/realtime/flusher.py` | Create | Batch DB writer + WebSocket broadcast |
| `ingestion/realtime/feeds/__init__.py` | Create | Package marker |
| `ingestion/realtime/feeds/binance.py` | Create | Binance combined-stream WebSocket client |
| `ingestion/realtime/feeds/yahoo.py` | Create | Yahoo Finance 60s HTTP poller |
| `ingestion/realtime/feeds/dex_scanner.py` | Create | GeckoTerminal + DexScreener liquidity scanner |
| `ingestion/realtime/ws_listener.py` | Create | Main daemon entry point (asyncio orchestrator) |
| `server_setup/grid-realtime.service` | Create | systemd unit file |
| `schema.sql` | Modify | Add `realtime_candles` table + indexes |
| `requirements.txt` | Modify | Add `websockets>=12.0`, `aiohttp>=3.9` |
| `tests/test_candle_builder.py` | Create | Unit tests for candle aggregation |
| `tests/test_dex_scanner.py` | Create | Unit tests for spike detection |
| `tests/test_realtime_flusher.py` | Create | Integration test for flush logic |

---

## Task 1: Database Schema — `realtime_candles` Table

**Files:**
- Modify: `schema.sql` (append after signal_registry block, ~line 1653)

- [ ] **Step 1: Add realtime_candles table to schema.sql**

Append to the end of `schema.sql`:

```sql
-- ============================================================
-- TABLE: realtime_candles
-- 5-minute OHLCV candles from real-time feeds (Binance WS,
-- Yahoo Finance, DEX scanners). Partitioned weekly, 90-day retention.
-- ============================================================
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

CREATE INDEX IF NOT EXISTS idx_rt_candles_ts ON realtime_candles (ts);
CREATE INDEX IF NOT EXISTS idx_rt_candles_asset_class ON realtime_candles (asset_class, ts);
CREATE INDEX IF NOT EXISTS idx_rt_candles_source ON realtime_candles (source, ts);
```

- [ ] **Step 2: Apply schema to local DB**

Run:
```bash
cd ~/dev/GRID && psql -U grid -d griddb -c "
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
CREATE INDEX IF NOT EXISTS idx_rt_candles_ts ON realtime_candles (ts);
CREATE INDEX IF NOT EXISTS idx_rt_candles_asset_class ON realtime_candles (asset_class, ts);
CREATE INDEX IF NOT EXISTS idx_rt_candles_source ON realtime_candles (source, ts);
"
```

Expected: `CREATE TABLE` / `CREATE INDEX` messages, no errors.

- [ ] **Step 3: Commit**

```bash
git add schema.sql
git commit -m "feat: add realtime_candles table schema"
```

---

## Task 2: Dependencies — Add websockets + aiohttp to requirements.txt

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1: Add websockets and aiohttp**

Add these lines to `requirements.txt` (in the appropriate alphabetical section):

```
aiohttp>=3.9
websockets>=12.0
```

- [ ] **Step 2: Install locally**

Run:
```bash
cd ~/dev/GRID && pip install websockets>=12.0 aiohttp>=3.9
```

Expected: Successfully installed.

- [ ] **Step 3: Commit**

```bash
git add requirements.txt
git commit -m "chore: add websockets and aiohttp to requirements"
```

---

## Task 3: CandleBuilder — In-Memory OHLCV Aggregator

**Files:**
- Create: `ingestion/realtime/__init__.py`
- Create: `ingestion/realtime/candle_builder.py`
- Create: `tests/test_candle_builder.py`

- [ ] **Step 1: Create package init**

Create `ingestion/realtime/__init__.py` (empty file).

- [ ] **Step 2: Write failing tests for CandleBuilder**

Create `tests/test_candle_builder.py`:

```python
"""Tests for the in-memory OHLCV candle builder."""

from datetime import datetime, timezone

import pytest

from ingestion.realtime.candle_builder import CandleBuilder, CandleState


class TestCandleBucketFloor:
    """ts_bucket floors timestamps to 5-minute boundaries."""

    def test_exact_boundary(self):
        cb = CandleBuilder()
        ts = datetime(2026, 4, 6, 13, 30, 0, tzinfo=timezone.utc)
        assert cb._bucket_floor(ts) == ts

    def test_mid_bucket(self):
        cb = CandleBuilder()
        ts = datetime(2026, 4, 6, 13, 32, 17, tzinfo=timezone.utc)
        expected = datetime(2026, 4, 6, 13, 30, 0, tzinfo=timezone.utc)
        assert cb._bucket_floor(ts) == expected

    def test_end_of_bucket(self):
        cb = CandleBuilder()
        ts = datetime(2026, 4, 6, 13, 34, 59, tzinfo=timezone.utc)
        expected = datetime(2026, 4, 6, 13, 30, 0, tzinfo=timezone.utc)
        assert cb._bucket_floor(ts) == expected


class TestCandleBuilderIngest:
    """Tick ingestion updates candle state correctly."""

    def test_first_tick_sets_ohlcv(self):
        cb = CandleBuilder()
        ts = datetime(2026, 4, 6, 13, 30, 5, tzinfo=timezone.utc)
        cb.ingest("BTCUSDT", 70000.0, 1.5, ts, "crypto", "binance")

        key = ("BTCUSDT", "5m")
        assert key in cb.candles
        c = cb.candles[key]
        assert c.open == 70000.0
        assert c.high == 70000.0
        assert c.low == 70000.0
        assert c.close == 70000.0
        assert c.volume == 1.5
        assert c.trade_count == 1

    def test_second_tick_updates_high_low_close(self):
        cb = CandleBuilder()
        ts1 = datetime(2026, 4, 6, 13, 30, 5, tzinfo=timezone.utc)
        ts2 = datetime(2026, 4, 6, 13, 30, 10, tzinfo=timezone.utc)
        cb.ingest("BTCUSDT", 70000.0, 1.0, ts1, "crypto", "binance")
        cb.ingest("BTCUSDT", 70500.0, 2.0, ts2, "crypto", "binance")

        c = cb.candles[("BTCUSDT", "5m")]
        assert c.open == 70000.0
        assert c.high == 70500.0
        assert c.low == 70000.0
        assert c.close == 70500.0
        assert c.volume == 3.0
        assert c.trade_count == 2

    def test_low_updates_correctly(self):
        cb = CandleBuilder()
        ts = datetime(2026, 4, 6, 13, 30, 5, tzinfo=timezone.utc)
        cb.ingest("ETHUSDT", 3500.0, 1.0, ts, "crypto", "binance")
        cb.ingest("ETHUSDT", 3400.0, 1.0, ts, "crypto", "binance")

        c = cb.candles[("ETHUSDT", "5m")]
        assert c.low == 3400.0

    def test_vwap_calculation(self):
        cb = CandleBuilder()
        ts = datetime(2026, 4, 6, 13, 30, 5, tzinfo=timezone.utc)
        cb.ingest("BTCUSDT", 100.0, 2.0, ts, "crypto", "binance")
        cb.ingest("BTCUSDT", 200.0, 3.0, ts, "crypto", "binance")

        c = cb.candles[("BTCUSDT", "5m")]
        # vwap = (100*2 + 200*3) / (2 + 3) = 800 / 5 = 160
        assert c.vwap == pytest.approx(160.0)


class TestCandleBuilderBucketRollover:
    """New bucket triggers flush of previous candle."""

    def test_new_bucket_flushes_old_candle(self):
        cb = CandleBuilder()
        ts1 = datetime(2026, 4, 6, 13, 30, 5, tzinfo=timezone.utc)
        ts2 = datetime(2026, 4, 6, 13, 35, 5, tzinfo=timezone.utc)

        cb.ingest("BTCUSDT", 70000.0, 1.0, ts1, "crypto", "binance")
        assert len(cb.flush_queue) == 0

        cb.ingest("BTCUSDT", 71000.0, 2.0, ts2, "crypto", "binance")
        assert len(cb.flush_queue) == 1

        flushed = cb.flush_queue[0]
        assert flushed.open == 70000.0
        assert flushed.close == 70000.0
        assert flushed.ts_bucket == datetime(2026, 4, 6, 13, 30, 0, tzinfo=timezone.utc)

    def test_drain_returns_and_clears_queue(self):
        cb = CandleBuilder()
        ts1 = datetime(2026, 4, 6, 13, 30, 5, tzinfo=timezone.utc)
        ts2 = datetime(2026, 4, 6, 13, 35, 5, tzinfo=timezone.utc)

        cb.ingest("BTCUSDT", 70000.0, 1.0, ts1, "crypto", "binance")
        cb.ingest("BTCUSDT", 71000.0, 2.0, ts2, "crypto", "binance")

        drained = cb.drain()
        assert len(drained) == 1
        assert len(cb.flush_queue) == 0

    def test_flush_all_moves_active_candles_to_queue(self):
        cb = CandleBuilder()
        ts = datetime(2026, 4, 6, 13, 30, 5, tzinfo=timezone.utc)
        cb.ingest("BTCUSDT", 70000.0, 1.0, ts, "crypto", "binance")
        cb.ingest("ETHUSDT", 3500.0, 1.0, ts, "crypto", "binance")

        cb.flush_all()
        assert len(cb.flush_queue) == 2
        assert len(cb.candles) == 0


class TestMultipleSymbols:
    """Multiple symbols tracked independently."""

    def test_independent_candles(self):
        cb = CandleBuilder()
        ts = datetime(2026, 4, 6, 13, 30, 5, tzinfo=timezone.utc)
        cb.ingest("BTCUSDT", 70000.0, 1.0, ts, "crypto", "binance")
        cb.ingest("GC=F", 2350.0, 10.0, ts, "metal", "yahoo")

        assert len(cb.candles) == 2
        assert cb.candles[("BTCUSDT", "5m")].close == 70000.0
        assert cb.candles[("GC=F", "5m")].close == 2350.0
        assert cb.candles[("GC=F", "5m")].asset_class == "metal"
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd ~/dev/GRID && python -m pytest tests/test_candle_builder.py -v`

Expected: FAIL — `ModuleNotFoundError: No module named 'ingestion.realtime'`

- [ ] **Step 4: Implement CandleBuilder**

Create `ingestion/realtime/candle_builder.py`:

```python
"""In-memory OHLCV candle aggregator.

Receives ticks from multiple feeds, builds 5-minute candles, and queues
completed candles for batch DB flush. Thread-safe via asyncio (single-threaded
event loop, no lock needed).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone

from loguru import logger as log


INTERVAL_SECONDS = 300  # 5 minutes


@dataclass
class CandleState:
    """State of a single in-progress candle."""

    symbol: str
    asset_class: str
    source: str
    interval: str
    ts_bucket: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap_numerator: float   # sum(price * volume)
    vwap_denominator: float  # sum(volume)
    trade_count: int

    @property
    def vwap(self) -> float | None:
        if self.vwap_denominator == 0:
            return None
        return self.vwap_numerator / self.vwap_denominator


class CandleBuilder:
    """Aggregates ticks into 5-minute OHLCV candles.

    Usage:
        builder = CandleBuilder()
        builder.ingest("BTCUSDT", 70000.0, 1.5, now, "crypto", "binance")
        completed = builder.drain()  # returns list of finished CandleState
    """

    def __init__(self, interval_seconds: int = INTERVAL_SECONDS) -> None:
        self.interval_seconds = interval_seconds
        self.interval_label = f"{interval_seconds // 60}m"
        self.candles: dict[tuple[str, str], CandleState] = {}
        self.flush_queue: list[CandleState] = []

    def _bucket_floor(self, ts: datetime) -> datetime:
        """Floor a timestamp to the nearest interval boundary."""
        epoch = int(ts.timestamp())
        floored = epoch - (epoch % self.interval_seconds)
        return datetime.fromtimestamp(floored, tz=timezone.utc)

    def ingest(
        self,
        symbol: str,
        price: float,
        volume: float,
        ts: datetime,
        asset_class: str,
        source: str,
    ) -> None:
        """Ingest a single tick (trade or price update)."""
        bucket = self._bucket_floor(ts)
        key = (symbol, self.interval_label)

        existing = self.candles.get(key)

        if existing is not None and existing.ts_bucket != bucket:
            # New bucket — flush the old candle
            self.flush_queue.append(existing)
            existing = None

        if existing is None:
            self.candles[key] = CandleState(
                symbol=symbol,
                asset_class=asset_class,
                source=source,
                interval=self.interval_label,
                ts_bucket=bucket,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=volume,
                vwap_numerator=price * volume,
                vwap_denominator=volume,
                trade_count=1,
            )
        else:
            existing.high = max(existing.high, price)
            existing.low = min(existing.low, price)
            existing.close = price
            existing.volume += volume
            existing.vwap_numerator += price * volume
            existing.vwap_denominator += volume
            existing.trade_count += 1

    def drain(self) -> list[CandleState]:
        """Return and clear all completed candles from the flush queue."""
        result = list(self.flush_queue)
        self.flush_queue.clear()
        return result

    def flush_all(self) -> None:
        """Move all active candles to the flush queue (for graceful shutdown)."""
        for candle in self.candles.values():
            self.flush_queue.append(candle)
        self.candles.clear()

    @property
    def active_symbols(self) -> int:
        return len(self.candles)

    @property
    def pending_flush(self) -> int:
        return len(self.flush_queue)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd ~/dev/GRID && python -m pytest tests/test_candle_builder.py -v`

Expected: All 11 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add ingestion/realtime/__init__.py ingestion/realtime/candle_builder.py tests/test_candle_builder.py
git commit -m "feat: CandleBuilder — in-memory 5-min OHLCV aggregator with tests"
```

---

## Task 4: DB Flusher — Batch Writer

**Files:**
- Create: `ingestion/realtime/flusher.py`
- Create: `tests/test_realtime_flusher.py`

- [ ] **Step 1: Write failing test for flusher**

Create `tests/test_realtime_flusher.py`:

```python
"""Tests for the realtime candle DB flusher."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from ingestion.realtime.candle_builder import CandleBuilder, CandleState
from ingestion.realtime.flusher import build_insert_values, MAX_BUFFER_CYCLES


class TestBuildInsertValues:
    """INSERT value tuples are built correctly from CandleState."""

    def test_single_candle(self):
        candle = CandleState(
            symbol="BTCUSDT",
            asset_class="crypto",
            source="binance",
            interval="5m",
            ts_bucket=datetime(2026, 4, 6, 13, 30, 0, tzinfo=timezone.utc),
            open=70000.0,
            high=70500.0,
            low=69800.0,
            close=70200.0,
            volume=15.3,
            vwap_numerator=70200.0 * 15.3,
            vwap_denominator=15.3,
            trade_count=42,
        )
        rows = build_insert_values([candle])
        assert len(rows) == 1
        row = rows[0]
        assert row[0] == "BTCUSDT"
        assert row[1] == "crypto"
        assert row[2] == "5m"
        assert row[4] == 70000.0  # open
        assert row[5] == 70500.0  # high
        assert row[6] == 69800.0  # low
        assert row[7] == 70200.0  # close
        assert row[8] == 15.3     # volume
        assert row[10] == 42      # trade_count

    def test_empty_list(self):
        rows = build_insert_values([])
        assert rows == []

    def test_vwap_none_when_zero_volume(self):
        candle = CandleState(
            symbol="TEST",
            asset_class="crypto",
            source="binance",
            interval="5m",
            ts_bucket=datetime(2026, 4, 6, 13, 30, 0, tzinfo=timezone.utc),
            open=100.0, high=100.0, low=100.0, close=100.0,
            volume=0.0,
            vwap_numerator=0.0,
            vwap_denominator=0.0,
            trade_count=0,
        )
        rows = build_insert_values([candle])
        assert rows[0][9] is None  # vwap


class TestBufferConfig:
    """Buffer limits are configured correctly."""

    def test_max_buffer_cycles(self):
        assert MAX_BUFFER_CYCLES == 12  # 1 hour at 5-min intervals
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ~/dev/GRID && python -m pytest tests/test_realtime_flusher.py -v`

Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement flusher**

Create `ingestion/realtime/flusher.py`:

```python
"""Batch DB writer for realtime candles.

Drains the CandleBuilder flush queue every 5 minutes, batch-inserts into
realtime_candles using INSERT ... ON CONFLICT DO NOTHING (idempotent).
Buffers in memory on DB failure, alerts after 3 consecutive failures.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from loguru import logger as log

from ingestion.realtime.candle_builder import CandleBuilder, CandleState

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
            c.symbol,
            c.asset_class,
            c.interval,
            c.ts_bucket,
            c.open,
            c.high,
            c.low,
            c.close,
            c.volume,
            c.vwap,
            c.trade_count,
            c.source,
        ))
    return rows


async def run_flusher(builder: CandleBuilder) -> None:
    """Periodically drain candle builder and batch-insert to DB.

    Runs forever. Buffers candles in memory on DB failure.
    """
    from db import get_connection

    buffer: list[CandleState] = []
    consecutive_failures = 0

    while True:
        await asyncio.sleep(FLUSH_INTERVAL)

        try:
            # Drain completed candles
            drained = builder.drain()
            if drained:
                buffer.extend(drained)

            if not buffer:
                continue

            # Enforce max buffer size (drop oldest if over 1 hour)
            max_candles = MAX_BUFFER_CYCLES * 100  # ~100 symbols * 12 cycles
            if len(buffer) > max_candles:
                dropped = len(buffer) - max_candles
                buffer = buffer[dropped:]
                log.warning("Dropped {n} oldest buffered candles (buffer overflow)", n=dropped)

            # Batch insert
            rows = build_insert_values(buffer)
            with get_connection() as conn:
                with conn.cursor() as cur:
                    from psycopg2.extras import execute_batch
                    execute_batch(cur, INSERT_SQL, rows, page_size=500)

            log.info(
                "Flushed {n} candles to realtime_candles ({syms} symbols)",
                n=len(rows),
                syms=len({r[0] for r in rows}),
            )
            buffer.clear()
            consecutive_failures = 0

            # Broadcast to WebSocket clients
            try:
                from api.main import _broadcast_event
                latest = {}
                for c in drained:
                    latest[c.symbol] = {
                        "symbol": c.symbol,
                        "asset_class": c.asset_class,
                        "price": c.close,
                        "ts": c.ts_bucket.isoformat(),
                    }
                if latest:
                    asyncio.create_task(
                        _broadcast_event("candle_update", {"candles": list(latest.values())})
                    )
            except Exception:
                pass  # WebSocket broadcast is best-effort

        except Exception as exc:
            consecutive_failures += 1
            log.error(
                "Candle flush failed ({n}/{max}): {err}",
                n=consecutive_failures,
                max=MAX_CONSECUTIVE_FAILURES,
                err=str(exc),
            )
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                try:
                    from alerts.email import alert_on_failure
                    alert_on_failure("Realtime candle flusher", str(exc))
                except Exception:
                    pass
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd ~/dev/GRID && python -m pytest tests/test_realtime_flusher.py -v`

Expected: All 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add ingestion/realtime/flusher.py tests/test_realtime_flusher.py
git commit -m "feat: realtime candle flusher — batch INSERT with buffer and retry"
```

---

## Task 5: Binance WebSocket Feed

**Files:**
- Create: `ingestion/realtime/feeds/__init__.py`
- Create: `ingestion/realtime/feeds/binance.py`

- [ ] **Step 1: Create feeds package init**

Create `ingestion/realtime/feeds/__init__.py` (empty file).

- [ ] **Step 2: Implement Binance feed**

Create `ingestion/realtime/feeds/binance.py`:

```python
"""Binance combined-stream WebSocket client for real-time crypto trades.

Connects to wss://stream.binance.com:9443/stream and subscribes to
@trade streams for all configured symbols. Parses trade messages and
feeds them into the CandleBuilder.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

from loguru import logger as log

from ingestion.realtime.candle_builder import CandleBuilder

# Binance uses lowercase symbol pairs
CRYPTO_SYMBOLS = [
    "btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt",
    "taousdt", "dogeusdt", "adausdt", "avaxusdt", "linkusdt",
    "dotusdt", "maticusdt", "uniusdt", "aaveusdt", "mkrusdt",
    "snxusdt", "crvusdt", "shibusdt", "ltcusdt", "atomusdt",
    "nearusdt", "pepeusdt", "wifusdt", "arbusdt", "opusdt",
    "suiusdt", "aptusdt", "seiusdt", "fetusdt", "renderusdt",
    "injusdt",
]

BASE_URL = "wss://stream.binance.com:9443/stream"
MAX_BACKOFF = 60


async def run_binance_feed(builder: CandleBuilder) -> None:
    """Connect to Binance combined stream and ingest trades forever."""
    import websockets

    streams = "/".join(f"{s}@trade" for s in CRYPTO_SYMBOLS)
    url = f"{BASE_URL}?streams={streams}"
    backoff = 1

    while True:
        try:
            log.info("Binance WS connecting — {n} streams", n=len(CRYPTO_SYMBOLS))
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                backoff = 1  # Reset on successful connect
                log.info("Binance WS connected")

                async for raw_msg in ws:
                    try:
                        msg = json.loads(raw_msg)
                        data = msg.get("data", {})
                        if data.get("e") != "trade":
                            continue

                        symbol = data["s"]  # e.g., "BTCUSDT"
                        price = float(data["p"])
                        qty = float(data["q"])
                        trade_time = datetime.fromtimestamp(
                            data["T"] / 1000, tz=timezone.utc
                        )

                        builder.ingest(symbol, price, qty, trade_time, "crypto", "binance")

                    except (KeyError, ValueError, TypeError) as exc:
                        log.debug("Binance parse error: {e}", e=str(exc))

        except asyncio.CancelledError:
            log.info("Binance WS feed cancelled — shutting down")
            return
        except Exception as exc:
            log.warning(
                "Binance WS disconnected: {err} — reconnecting in {s}s",
                err=str(exc),
                s=backoff,
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)
```

- [ ] **Step 3: Commit**

```bash
git add ingestion/realtime/feeds/__init__.py ingestion/realtime/feeds/binance.py
git commit -m "feat: Binance WebSocket feed — 31 crypto trade streams"
```

---

## Task 6: Yahoo Finance Poller Feed

**Files:**
- Create: `ingestion/realtime/feeds/yahoo.py`

- [ ] **Step 1: Implement Yahoo feed**

Create `ingestion/realtime/feeds/yahoo.py`:

```python
"""Yahoo Finance HTTP poller for traditional market data.

Polls yfinance every 60 seconds for metals, energy, grains, index futures,
forex, and bond yields. Feeds last price into CandleBuilder.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from loguru import logger as log

from ingestion.realtime.candle_builder import CandleBuilder

POLL_INTERVAL = 60  # seconds

SYMBOLS: dict[str, str] = {
    # Metals
    "GC=F": "metal", "SI=F": "metal", "PL=F": "metal",
    "PA=F": "metal", "HG=F": "metal",
    # Energy
    "CL=F": "energy", "BZ=F": "energy", "NG=F": "energy", "HO=F": "energy",
    # Grains / Softs
    "ZC=F": "grain", "ZS=F": "grain", "ZW=F": "grain",
    "KC=F": "grain", "SB=F": "grain", "CT=F": "grain",
    # Index Futures
    "ES=F": "index", "NQ=F": "index", "YM=F": "index",
    "RTY=F": "index", "NKD=F": "index",
    # Forex
    "EURUSD=X": "forex", "GBPUSD=X": "forex", "USDJPY=X": "forex",
    "USDCHF=X": "forex", "AUDUSD=X": "forex", "USDCAD=X": "forex",
    "NZDUSD=X": "forex", "USDCNH=X": "forex",
    # Bond yields
    "^TNX": "bond", "^TYX": "bond", "^FVX": "bond",
}

# FDAX not available on yfinance; skip if it errors
SYMBOLS["FDAX"] = "index"


async def run_yahoo_feed(builder: CandleBuilder) -> None:
    """Poll Yahoo Finance every 60s and feed prices into CandleBuilder."""
    consecutive_failures = 0

    while True:
        try:
            await asyncio.sleep(POLL_INTERVAL)

            # Run blocking yfinance call in executor
            prices = await asyncio.get_event_loop().run_in_executor(
                None, _fetch_prices
            )

            now = datetime.now(tz=timezone.utc)
            ingested = 0
            for symbol, (price, volume) in prices.items():
                asset_class = SYMBOLS.get(symbol, "other")
                builder.ingest(symbol, price, volume, now, asset_class, "yahoo")
                ingested += 1

            if ingested > 0:
                log.debug("Yahoo poll — {n}/{t} symbols updated", n=ingested, t=len(SYMBOLS))
                consecutive_failures = 0

        except asyncio.CancelledError:
            log.info("Yahoo feed cancelled — shutting down")
            return
        except Exception as exc:
            consecutive_failures += 1
            log.warning(
                "Yahoo poll failed ({n}): {err}",
                n=consecutive_failures,
                err=str(exc),
            )
            if consecutive_failures >= 5:
                log.error("Yahoo feed: 5 consecutive failures, backing off to 120s")
                await asyncio.sleep(120)
                consecutive_failures = 0


def _fetch_prices() -> dict[str, tuple[float, float]]:
    """Synchronous yfinance fetch. Returns {symbol: (price, volume)}."""
    import yfinance as yf

    tickers = list(SYMBOLS.keys())
    result: dict[str, tuple[float, float]] = {}

    try:
        data = yf.download(
            tickers,
            period="1d",
            interval="1m",
            progress=False,
            threads=True,
        )
        if data.empty:
            return result

        for symbol in tickers:
            try:
                if len(tickers) == 1:
                    close_col = data["Close"]
                    vol_col = data["Volume"]
                else:
                    close_col = data["Close"][symbol]
                    vol_col = data["Volume"][symbol]

                last_close = close_col.dropna().iloc[-1]
                last_vol = vol_col.dropna().iloc[-1] if not vol_col.dropna().empty else 0.0

                if last_close > 0:
                    result[symbol] = (float(last_close), float(last_vol))
            except (KeyError, IndexError):
                continue

    except Exception as exc:
        log.debug("yfinance bulk download error: {e}", e=str(exc))

    return result
```

- [ ] **Step 2: Commit**

```bash
git add ingestion/realtime/feeds/yahoo.py
git commit -m "feat: Yahoo Finance poller — 33 metals/energy/grains/futures/forex/bonds"
```

---

## Task 7: DEX Scanner Feed

**Files:**
- Create: `ingestion/realtime/feeds/dex_scanner.py`
- Create: `tests/test_dex_scanner.py`

- [ ] **Step 1: Write failing tests for spike detection**

Create `tests/test_dex_scanner.py`:

```python
"""Tests for DEX liquidity spike detection."""

from ingestion.realtime.feeds.dex_scanner import detect_spikes, PoolData


class TestDetectSpikes:
    """Spike detection applies correct thresholds."""

    def test_volume_spike_detected(self):
        pool = PoolData(
            symbol="SOL:BONK",
            chain="solana",
            dex="raydium",
            pool_address="abc123",
            price=0.00002,
            volume_24h=500_000.0,
            volume_avg_24h=100_000.0,
            liquidity=200_000.0,
            price_change_1h=5.0,
            pool_age_hours=48.0,
        )
        spikes = detect_spikes([pool])
        assert len(spikes) == 1
        assert spikes[0]["direction"] == "spike_volume"
        assert spikes[0]["magnitude"] == 5.0

    def test_no_spike_normal_volume(self):
        pool = PoolData(
            symbol="ETH:UNI",
            chain="ethereum",
            dex="uniswap_v3",
            pool_address="def456",
            price=7.5,
            volume_24h=200_000.0,
            volume_avg_24h=150_000.0,
            liquidity=1_000_000.0,
            price_change_1h=2.0,
            pool_age_hours=720.0,
        )
        spikes = detect_spikes([pool])
        assert len(spikes) == 0

    def test_new_pool_with_liquidity(self):
        pool = PoolData(
            symbol="SOL:NEWCOIN",
            chain="solana",
            dex="raydium",
            pool_address="ghi789",
            price=0.001,
            volume_24h=10_000.0,
            volume_avg_24h=0.0,
            liquidity=75_000.0,
            price_change_1h=0.0,
            pool_age_hours=2.0,
        )
        spikes = detect_spikes([pool])
        assert len(spikes) == 1
        assert spikes[0]["direction"] == "new_pool"

    def test_price_surge(self):
        pool = PoolData(
            symbol="ETH:MEME",
            chain="ethereum",
            dex="uniswap_v3",
            pool_address="jkl012",
            price=0.05,
            volume_24h=50_000.0,
            volume_avg_24h=40_000.0,
            liquidity=100_000.0,
            price_change_1h=25.0,
            pool_age_hours=168.0,
        )
        spikes = detect_spikes([pool])
        assert len(spikes) == 1
        assert spikes[0]["direction"] == "price_surge"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ~/dev/GRID && python -m pytest tests/test_dex_scanner.py -v`

Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement DEX scanner**

Create `ingestion/realtime/feeds/dex_scanner.py`:

```python
"""DEX token scanner — GeckoTerminal + DexScreener liquidity spike detection.

Polls trending pools on Solana and Ethereum every 60 seconds. Detects
volume spikes, new high-liquidity pools, and price surges. Writes
signals to signal_data and feeds hot token prices into CandleBuilder.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone

from loguru import logger as log

from ingestion.realtime.candle_builder import CandleBuilder

POLL_INTERVAL = 60
WATCHED_TOKEN_TTL = 86400  # 24 hours in seconds

# Spike thresholds
VOLUME_SPIKE_MULTIPLIER = 3.0
NEW_POOL_MIN_LIQUIDITY = 50_000.0
NEW_POOL_MAX_AGE_HOURS = 24.0
PRICE_SURGE_THRESHOLD = 20.0  # percent


@dataclass
class PoolData:
    """Normalized pool data from any DEX API."""

    symbol: str          # e.g., "SOL:BONK"
    chain: str           # "solana" or "ethereum"
    dex: str             # "raydium", "uniswap_v3", etc.
    pool_address: str
    price: float
    volume_24h: float
    volume_avg_24h: float
    liquidity: float
    price_change_1h: float
    pool_age_hours: float


def detect_spikes(pools: list[PoolData]) -> list[dict]:
    """Apply spike detection rules. Returns list of signal dicts."""
    signals: list[dict] = []

    for p in pools:
        # Volume spike: current > 3x average
        if p.volume_avg_24h > 0 and p.volume_24h / p.volume_avg_24h >= VOLUME_SPIKE_MULTIPLIER:
            signals.append({
                "signal_type": "dex_liquidity_spike",
                "ticker": p.symbol,
                "direction": "spike_volume",
                "magnitude": round(p.volume_24h / p.volume_avg_24h, 1),
                "data": _pool_metadata(p),
            })
            continue  # One signal per pool per cycle

        # New pool with meaningful liquidity
        if p.pool_age_hours < NEW_POOL_MAX_AGE_HOURS and p.liquidity >= NEW_POOL_MIN_LIQUIDITY:
            signals.append({
                "signal_type": "dex_liquidity_spike",
                "ticker": p.symbol,
                "direction": "new_pool",
                "magnitude": p.liquidity,
                "data": _pool_metadata(p),
            })
            continue

        # Price surge: >20% in 1 hour
        if abs(p.price_change_1h) >= PRICE_SURGE_THRESHOLD:
            signals.append({
                "signal_type": "dex_liquidity_spike",
                "ticker": p.symbol,
                "direction": "price_surge",
                "magnitude": round(p.price_change_1h, 1),
                "data": _pool_metadata(p),
            })

    return signals


def _pool_metadata(p: PoolData) -> dict:
    return {
        "chain": p.chain,
        "dex": p.dex,
        "pool_address": p.pool_address,
        "volume_24h": p.volume_24h,
        "liquidity": p.liquidity,
        "price_change_1h": p.price_change_1h,
        "pool_age_hours": p.pool_age_hours,
        "price": p.price,
    }


async def run_dex_scanner(builder: CandleBuilder) -> None:
    """Poll DEX APIs every 60s, detect spikes, write signals, feed prices."""
    import aiohttp

    watched_tokens: dict[str, float] = {}  # symbol -> expiry timestamp

    while True:
        try:
            await asyncio.sleep(POLL_INTERVAL)

            async with aiohttp.ClientSession() as session:
                pools = await _fetch_all_pools(session)

            if not pools:
                continue

            # Detect spikes
            spikes = detect_spikes(pools)

            # Write spike signals to DB
            if spikes:
                _write_signals(spikes)
                for s in spikes:
                    watched_tokens[s["ticker"]] = (
                        datetime.now(tz=timezone.utc).timestamp() + WATCHED_TOKEN_TTL
                    )
                log.info("DEX scanner — {n} spikes detected", n=len(spikes))

            # Feed prices into CandleBuilder for trending + watched tokens
            now = datetime.now(tz=timezone.utc)
            _expire_watched(watched_tokens, now)

            for p in pools:
                if p.symbol in watched_tokens or p.volume_24h > 100_000:
                    builder.ingest(
                        p.symbol, p.price, p.volume_24h, now, "dex_token", "dex"
                    )

        except asyncio.CancelledError:
            log.info("DEX scanner cancelled — shutting down")
            return
        except Exception as exc:
            log.warning("DEX scanner error: {err}", err=str(exc))


def _expire_watched(watched: dict[str, float], now: datetime) -> None:
    """Remove expired watched tokens."""
    cutoff = now.timestamp()
    expired = [k for k, v in watched.items() if v < cutoff]
    for k in expired:
        del watched[k]


async def _fetch_all_pools(session: aiohttp.ClientSession) -> list[PoolData]:
    """Fetch trending pools from GeckoTerminal + DexScreener."""
    pools: list[PoolData] = []

    # GeckoTerminal trending pools
    for network in ["solana", "eth"]:
        chain = "solana" if network == "solana" else "ethereum"
        try:
            url = f"https://api.geckoterminal.com/api/v2/networks/{network}/trending_pools"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for item in data.get("data", []):
                        pool = _parse_geckoterminal(item, chain)
                        if pool:
                            pools.append(pool)
        except Exception as exc:
            log.debug("GeckoTerminal {net} error: {e}", net=network, e=str(exc))

    # DexScreener latest boosted tokens
    try:
        url = "https://api.dexscreener.com/token-boosts/latest/v1"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                data = await resp.json()
                for item in data if isinstance(data, list) else []:
                    pool = _parse_dexscreener_boost(item)
                    if pool:
                        pools.append(pool)
    except Exception as exc:
        log.debug("DexScreener error: {e}", e=str(exc))

    return pools


def _parse_geckoterminal(item: dict, chain: str) -> PoolData | None:
    """Parse a GeckoTerminal pool item."""
    try:
        attrs = item.get("attributes", {})
        name = attrs.get("name", "")
        # Name format: "TOKEN / USDC" or "TOKEN / SOL"
        base_token = name.split("/")[0].strip() if "/" in name else name
        symbol = f"{chain[:3].upper()}:{base_token}"

        return PoolData(
            symbol=symbol,
            chain=chain,
            dex=attrs.get("dex_id", "unknown"),
            pool_address=attrs.get("address", ""),
            price=float(attrs.get("base_token_price_usd") or 0),
            volume_24h=float(attrs.get("volume_usd", {}).get("h24") or 0),
            volume_avg_24h=float(attrs.get("volume_usd", {}).get("h24") or 0) / 1.5,
            liquidity=float(attrs.get("reserve_in_usd") or 0),
            price_change_1h=float(attrs.get("price_change_percentage", {}).get("h1") or 0),
            pool_age_hours=_pool_age_hours(attrs.get("pool_created_at")),
        )
    except Exception:
        return None


def _parse_dexscreener_boost(item: dict) -> PoolData | None:
    """Parse a DexScreener boosted token item."""
    try:
        chain_id = item.get("chainId", "")
        if chain_id not in ("solana", "ethereum"):
            return None
        chain = chain_id
        symbol = f"{chain[:3].upper()}:{item.get('tokenAddress', '')[:8]}"
        description = item.get("description", "")

        return PoolData(
            symbol=symbol,
            chain=chain,
            dex="dexscreener",
            pool_address=item.get("tokenAddress", ""),
            price=0.0,  # Boost endpoint doesn't include price
            volume_24h=0.0,
            volume_avg_24h=0.0,
            liquidity=0.0,
            price_change_1h=0.0,
            pool_age_hours=0.0,
        )
    except Exception:
        return None


def _pool_age_hours(created_at: str | None) -> float:
    """Convert ISO timestamp to age in hours."""
    if not created_at:
        return 9999.0
    try:
        created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        delta = datetime.now(tz=timezone.utc) - created
        return delta.total_seconds() / 3600
    except Exception:
        return 9999.0


def _write_signals(signals: list[dict]) -> None:
    """Write spike signals to signal_data table."""
    try:
        from db import get_connection

        with get_connection() as conn:
            with conn.cursor() as cur:
                for s in signals:
                    cur.execute(
                        """
                        INSERT INTO signal_data
                            (signal_type, signal_date, ticker, direction, magnitude, data, confidence)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            s["signal_type"],
                            date.today(),
                            s["ticker"],
                            s["direction"],
                            s["magnitude"],
                            json.dumps(s["data"]),
                            "derived",
                        ),
                    )
        log.debug("Wrote {n} DEX signals to signal_data", n=len(signals))
    except Exception as exc:
        log.warning("Failed to write DEX signals: {e}", e=str(exc))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd ~/dev/GRID && python -m pytest tests/test_dex_scanner.py -v`

Expected: All 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add ingestion/realtime/feeds/dex_scanner.py tests/test_dex_scanner.py
git commit -m "feat: DEX scanner — GeckoTerminal + DexScreener spike detection"
```

---

## Task 8: Main Daemon — ws_listener.py

**Files:**
- Create: `ingestion/realtime/ws_listener.py`

- [ ] **Step 1: Implement the daemon entry point**

Create `ingestion/realtime/ws_listener.py`:

```python
"""GRID Realtime Market Data Listener.

Main daemon entry point. Launches four async tasks:
1. Binance WebSocket — 31 crypto trade streams
2. Yahoo Finance poller — 33 traditional market symbols
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

    # Graceful shutdown on SIGTERM/SIGINT
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _handle_signal(sig: int) -> None:
        log.info("Received signal {s} — initiating graceful shutdown", s=sig)
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal, sig)

    # Wait for shutdown signal
    await shutdown_event.wait()

    # Cancel all tasks
    log.info("Cancelling feed tasks...")
    for t in tasks:
        t.cancel()

    # Wait for tasks to finish cancellation
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
```

- [ ] **Step 2: Commit**

```bash
git add ingestion/realtime/ws_listener.py
git commit -m "feat: realtime ws_listener daemon — async orchestrator with graceful shutdown"
```

---

## Task 9: systemd Service + Deploy

**Files:**
- Create: `server_setup/grid-realtime.service`

- [ ] **Step 1: Create service file**

Create `server_setup/grid-realtime.service`:

```ini
[Unit]
Description=GRID Realtime Market Data Listener
After=grid-api.service
Wants=grid-api.service

[Service]
Type=simple
User=grid
WorkingDirectory=/home/grid/grid_v4/grid_repo
EnvironmentFile=/home/grid/grid_v4/grid_repo/.env
ExecStart=/usr/bin/python3 -m ingestion.realtime.ws_listener
Restart=always
RestartSec=10
StandardOutput=append:/data/grid/logs/grid-realtime.log
StandardError=append:/data/grid/logs/grid-realtime.log

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 2: Commit all remaining files**

```bash
git add server_setup/grid-realtime.service
git commit -m "feat: grid-realtime.service systemd unit"
```

- [ ] **Step 3: Push and deploy to server**

```bash
cd ~/dev/GRID && git push
ssh grid@100.75.185.36 "cd ~/grid_v4/grid_repo && git pull --no-rebase"
```

- [ ] **Step 4: Apply schema on server**

```bash
ssh grid@100.75.185.36 "cd ~/grid_v4/grid_repo && psql -U grid -d griddb -c \"
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
CREATE INDEX IF NOT EXISTS idx_rt_candles_ts ON realtime_candles (ts);
CREATE INDEX IF NOT EXISTS idx_rt_candles_asset_class ON realtime_candles (asset_class, ts);
CREATE INDEX IF NOT EXISTS idx_rt_candles_source ON realtime_candles (source, ts);
\""
```

- [ ] **Step 5: Install deps on server**

```bash
ssh grid@100.75.185.36 "pip install websockets>=12.0 aiohttp>=3.9"
```

- [ ] **Step 6: Deploy service (requires sudo — user runs manually)**

```bash
sudo cp server_setup/grid-realtime.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable grid-realtime
sudo systemctl start grid-realtime
sudo systemctl status grid-realtime
```

- [ ] **Step 7: Verify data is flowing**

```bash
# Wait 5-6 minutes for first flush
tail -30 /data/grid/logs/grid-realtime.log
psql -U grid -d griddb -c "SELECT source, asset_class, count(*) FROM realtime_candles GROUP BY source, asset_class;"
```

Expected: Rows from binance/crypto, yahoo/metal, yahoo/energy, etc.

- [ ] **Step 8: Commit deploy verification**

```bash
git commit --allow-empty -m "chore: realtime listener deployed and verified on gridz4"
```

---

## Task 10: Retention Cleanup Job

**Files:**
- Modify: `ingestion/scheduler.py` (add weekly cleanup)

- [ ] **Step 1: Add retention cleanup to scheduler**

In `ingestion/scheduler.py`, inside `start_scheduler()` after the international schedule block, add:

```python
    # Weekly cleanup of old realtime candles (>90 days)
    def _cleanup_realtime_candles() -> None:
        try:
            from db import get_connection
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM realtime_candles WHERE ts < now() - INTERVAL '90 days'"
                    )
                    deleted = cur.rowcount
            if deleted:
                log.info("Realtime candle cleanup — deleted {n} rows older than 90 days", n=deleted)
        except Exception as exc:
            log.warning("Realtime candle cleanup failed: {e}", e=str(exc))

    schedule.every().sunday.at("02:00").do(_cleanup_realtime_candles)
```

- [ ] **Step 2: Commit**

```bash
git add ingestion/scheduler.py
git commit -m "feat: weekly realtime_candles retention cleanup (90 days)"
```
