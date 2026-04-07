# Realtime Market Data Listener — Design Spec

**Date:** 2026-04-06
**Status:** Approved
**Scope:** Single async daemon for real-time crypto, metals, energy, grains, index futures, forex, bonds, and DEX token monitoring

---

## Overview

A 24/7 WebSocket + polling daemon that ingests real-time market data across all asset classes, builds 5-minute OHLCV candles in memory, and batch-flushes to [[PostgreSQL]]. Includes a DEX scanner for Solana/Ethereum token liquidity spike detection.

Runs as a single systemd service on gridz4. Designed for easy decomposition into separate services when a dedicated server is added.

## Architecture

```
Binance WS (30 crypto) ──────┐
Yahoo Finance poll (32 trad) ─┼──▶ CandleBuilder (in-memory) ──▶ batch INSERT every 5min ──▶ realtime_candles table
DEX scanner poll (trending)  ─┘                                                            ──▶ signal_data (spikes)
```

Single Python process, one `asyncio` event loop, three concurrent tasks. Each task has independent error handling and reconnect logic.

## Symbols (62 total)

### Crypto — Binance WebSocket (30)

BTC, ETH, SOL, BNB, XRP, TAO, DOGE, ADA, AVAX, LINK, DOT, MATIC, UNI, AAVE, MKR, SNX, CRV, SHIB, LTC, ATOM, NEAR, PEPE, WIF, ARB, OP, SUI, APT, SEI, FET, RENDER, INJ

Binance stream format: `<symbol>usdt@trade` via combined stream WebSocket.

### Traditional — Yahoo Finance polling (32)

**Metals (5):** GC=F (Gold), SI=F (Silver), PL=F (Platinum), PA=F (Palladium), HG=F (Copper)

**Energy (4):** CL=F (WTI Crude), BZ=F (Brent Crude), NG=F (Natural Gas), HO=F (Heating Oil)

**Grains/Softs (6):** ZC=F (Corn), ZS=F (Soybeans), ZW=F (Wheat), KC=F (Coffee), SB=F (Sugar), CT=F (Cotton)

**Index Futures (6):** ES=F (S&P 500), NQ=F (Nasdaq 100), YM=F (Dow 30), RTY=F (Russell 2000), NKD=F (Nikkei 225), FDAX (DAX)

**Forex (8):** EURUSD=X, GBPUSD=X, USDJPY=X, USDCHF=X, AUDUSD=X, USDCAD=X, NZDUSD=X, USDCNH=X

**Bond Yields (3):** ^TNX (10Y), ^TYX (30Y), ^FVX (5Y)

Poll interval: 60 seconds. Yahoo Finance HTTP, no API key required.

### DEX Tokens — GeckoTerminal + DexScreener polling

Not fixed symbols — scans trending pools and new pairs dynamically.

**GeckoTerminal endpoints:**
- `/networks/solana/trending_pools` — top trending Solana DEX pools
- `/networks/eth/trending_pools` — top trending Ethereum DEX pools
- `/networks/{network}/new_pools` — newly created pools

**DexScreener endpoints:**
- `/latest/dex/tokens/boosted` — tokens with boosted liquidity
- `/latest/dex/pairs/{chainId}` — latest pairs on Solana/Ethereum

Poll interval: 60 seconds. Free, no API key.

## Database

### realtime_candles table

```sql
CREATE TABLE realtime_candles (
    symbol       TEXT NOT NULL,
    asset_class  TEXT NOT NULL,  -- crypto, metal, energy, grain, index, forex, bond
    interval     TEXT NOT NULL DEFAULT '5m',
    ts           TIMESTAMPTZ NOT NULL,
    open         DOUBLE PRECISION,
    high         DOUBLE PRECISION,
    low          DOUBLE PRECISION,
    close        DOUBLE PRECISION,
    volume       DOUBLE PRECISION,
    vwap         DOUBLE PRECISION,
    trade_count  INTEGER,
    source       TEXT NOT NULL,  -- binance, yahoo, geckoterminal, dexscreener
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, interval, ts)
);

CREATE INDEX idx_rt_candles_ts ON realtime_candles (ts);
CREATE INDEX idx_rt_candles_asset_class ON realtime_candles (asset_class, ts);
CREATE INDEX idx_rt_candles_source ON realtime_candles (source, ts);
```

### Partitioning

Weekly range partitions on `ts`. Auto-create next week's partition via the scheduler. Drop partitions older than 90 days via weekly cleanup job.

### DEX signals

Written to existing `signal_data` table:
- `signal_type`: `dex_liquidity_spike`
- `ticker`: token symbol (e.g., `SOL:BONK`, `ETH:PEPE`)
- `direction`: `spike_volume`, `spike_liquidity`, `new_pool`, `price_surge`
- `magnitude`: multiplier (e.g., 3.5 = 3.5x normal volume)
- `data` (JSONB): pool address, chain, DEX name, 24h volume, liquidity, price change %, pool age
- `confidence`: `derived`

### Volume estimates

| Source | Rows/day | Rows/month |
|--------|----------|------------|
| Binance (30 crypto, 5-min) | 8,640 | 259,200 |
| Yahoo (32 trad, 5-min) | 9,216 | 276,480 |
| DEX signals | ~50-200 | ~1,500-6,000 |
| **Total** | ~18,000 | ~540,000 |

At 90-day retention: max ~1.6M rows. Trivial for [[PostgreSQL]].

## Components

### File: `ingestion/realtime/ws_listener.py` — Main daemon

Entry point. Creates the event loop, launches three async tasks, handles graceful shutdown on SIGTERM/SIGINT.

```python
async def main():
    tasks = [
        asyncio.create_task(binance_feed(candle_builder)),
        asyncio.create_task(yahoo_feed(candle_builder)),
        asyncio.create_task(dex_scanner(candle_builder)),
        asyncio.create_task(candle_flusher(candle_builder)),
    ]
    await asyncio.gather(*tasks)
```

### File: `ingestion/realtime/candle_builder.py` — In-memory aggregator

Thread-safe candle builder. Dict of `{(symbol, interval): CandleState}`.

```python
@dataclass
class CandleState:
    symbol: str
    asset_class: str
    source: str
    ts_bucket: datetime       # floor to 5-min boundary
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap_numerator: float     # sum(price * volume)
    vwap_denominator: float   # sum(volume)
    trade_count: int
```

On tick: if same bucket, update HLCV/vwap. If new bucket, move previous candle to flush queue, start fresh.

Flush queue: list of completed `CandleState` objects ready for DB insert.

### File: `ingestion/realtime/feeds/binance.py` — Binance WebSocket

Connects to `wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/...` (combined stream, max 200 streams per connection).

Parses trade messages: `{price, quantity, time}`. Feeds into CandleBuilder.

Reconnect: exponential backoff (1s, 2s, 4s... max 60s). Logs each reconnect attempt.

### File: `ingestion/realtime/feeds/yahoo.py` — Yahoo Finance poller

Uses `yfinance.download(tickers, period="1d", interval="1m")` every 60 seconds to get latest prices. Extracts last row per ticker, feeds into CandleBuilder.

Fallback: if yfinance fails 3x consecutively, switch to direct Yahoo v8 API endpoint.

### File: `ingestion/realtime/feeds/dex_scanner.py` — DEX token scanner

Polls GeckoTerminal and DexScreener every 60 seconds. For each pool:
1. Compare current volume to 24h average
2. If volume > 3x average OR liquidity > $50K on pool < 24h old OR price change > 20% in 1h:
   - Write `dex_liquidity_spike` signal to `signal_data`
   - Also feed price into CandleBuilder for the token (so we track candles for hot tokens)

Maintains a rolling set of "watched tokens" — tokens that triggered a spike stay in the candle builder for 24h, then expire.

### File: `ingestion/realtime/flusher.py` — DB batch writer

Runs on 5-minute interval. Drains the flush queue from CandleBuilder, batch-inserts into `realtime_candles` using `INSERT ... ON CONFLICT DO NOTHING` (idempotent).

Also broadcasts candle updates to [[FastAPI]] WebSocket clients via the existing `_broadcast_event()` pattern.

Retry logic: if DB insert fails, hold candles in memory. After 3 consecutive failures, send alert via existing email system. Buffer up to 1 hour of candles (12 flush cycles) before dropping oldest.

### File: `server_setup/grid-realtime.service` — Systemd unit

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

## Error Handling

| Failure | Response | Recovery |
|---------|----------|----------|
| Binance WS disconnect | Log warning, exponential backoff reconnect | Auto-reconnect, candles resume |
| Yahoo HTTP timeout | Skip cycle, retry next 60s | Automatic |
| DEX API rate limit | Back off to 120s interval | Auto-restore after cooldown |
| DB flush failure | Buffer in memory, retry next cycle | Alert after 3 failures |
| Process crash | systemd `Restart=always` with 10s delay | Automatic |
| Graceful shutdown (SIGTERM) | Flush all pending candles, then exit | Clean |

## Integration Points

1. **Frontend WebSocket:** Flusher broadcasts `candle_update` events to `/ws` clients — live price updates in PWA
2. **signal_data:** DEX spikes written as `dex_liquidity_spike` signals — hypothesis engine picks them up
3. **[[Oracle Engine|Oracle engine]]:** Can query `realtime_candles` for freshest prices during prediction scoring
4. **Existing [[CoinGecko]] puller:** Still runs for daily aggregates; realtime candles supplement with intraday granularity

## Dependencies

Add to `requirements.txt`:
- `websockets>=12.0` — Binance WebSocket client
- `aiohttp>=3.9` — async HTTP for DEX API polling

Both are already used by Discord/Telegram scanners but not listed in requirements.

## Future Expansion Path

1. **Current (Option A):** Single daemon, three async tasks — ships now
2. **Dedicated server (Option B):** Split each feed into its own systemd service — same code, different entry points
3. **Scale (Option C):** Add Redis pub/sub between listeners and flusher — enables tick replay and multi-server fan-out

Each step reuses 90%+ of previous code. No rebuild required.

## Retention Policy

- **0-90 days:** Full 5-minute candles in `realtime_candles`
- **90+ days:** Dropped via weekly partition cleanup
- Daily OHLCV from existing [[CoinGecko]]/yfinance pullers provides long-term history

## Testing

- Unit tests for CandleBuilder: tick ingestion, bucket rollover, VWAP calculation
- Unit tests for DEX scanner: spike detection thresholds
- Integration test: mock WebSocket feed → candle builder → DB flush → verify rows
- Smoke test script: connect to Binance, print 10 trades, verify parsing
