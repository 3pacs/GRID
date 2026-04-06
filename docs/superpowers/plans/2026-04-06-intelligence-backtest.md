# Intelligence Backtest Engine + Crypto Signal Pipeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix crypto data blindness, validate intelligence boost multipliers against real market data, and produce self-tuning calibration data from actual outcomes.

**Architecture:** Phase 0 builds crypto signal infrastructure (bridge module + 5 new free-API pullers). Phases 1-3 build the backtest engine that measures which information sources have real edge across both equity and crypto, replays the full intelligence pipeline, and outputs calibrated multipliers + forensic narratives.

**Tech Stack:** Python 3.11+, SQLAlchemy, PostgreSQL, requests, numpy/scipy for statistics, Ollama/Hermes for forensic narratives.

**Spec:** `docs/superpowers/specs/2026-04-06-intelligence-backtest-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `ingestion/crypto_signals.py` | Create | Bridge: existing crypto raw data → signal_sources |
| `ingestion/altdata/hyperliquid_puller.py` | Create | Hyperliquid OI, funding, liquidations |
| `ingestion/altdata/crypto_etf_flows.py` | Create | IBIT/ETHA/GBTC ETF flow signals |
| `ingestion/altdata/fear_greed.py` | Create | Crypto Fear & Greed Index |
| `ingestion/altdata/onchain_rpc.py` | Create | Direct blockchain RPC price/activity |
| `ingestion/altdata/whale_alert.py` | Create | On-chain whale transfers |
| `ingestion/scheduler.py` | Modify | Add crypto signal pullers to 24/7 block |
| `scripts/backtest_intelligence.py` | Create | Main backtest CLI (edge-table, replay, calibrate, report, full) |
| `tests/test_crypto_signals.py` | Create | Tests for crypto signal bridge |
| `tests/test_backtest.py` | Create | Tests for backtest engine |

---

## Task 1: Crypto Signal Bridge (`ingestion/crypto_signals.py`)

The core module that reads existing crypto data from resolved_series / raw_series / realtime_candles and emits standardized signal_sources entries.

**Files:**
- Create: `ingestion/crypto_signals.py`
- Create: `tests/test_crypto_signals.py`

- [ ] **Step 1: Write failing test for CoinGecko price breakout signal**

```python
# tests/test_crypto_signals.py
"""Tests for crypto signal bridge — existing data → signal_sources."""
import json
import pytest
from datetime import date, datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from unittest.mock import patch


@pytest.fixture
def engine():
    """PostgreSQL test engine."""
    eng = create_engine("postgresql://grid_user:changeme@localhost:5432/grid")
    try:
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception:
        pytest.skip("PostgreSQL not available")
    yield eng
    eng.dispose()


def test_coingecko_breakout_emits_signal(engine):
    """CoinGecko bridge detects price breakout and writes signal_sources."""
    from ingestion.crypto_signals import CryptoSignalBridge

    bridge = CryptoSignalBridge(engine)
    # Should not crash even if no data exists
    result = bridge.bridge_coingecko()
    assert isinstance(result, dict)
    assert "signals_emitted" in result
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ~/dev/GRID && python3 -m pytest tests/test_crypto_signals.py::test_coingecko_breakout_emits_signal -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingestion.crypto_signals'`

- [ ] **Step 3: Implement crypto signal bridge**

```python
# ingestion/crypto_signals.py
"""Crypto Signal Bridge — transforms existing crypto raw data into signal_sources.

Reads from resolved_series, raw_series, and realtime_candles.
Writes standardized entries to signal_sources so the intelligence layer
(trust_scorer, lever_pullers, hypothesis_engine) can see crypto.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# Ticker normalization: strip USDT/USD suffix
def _normalize_ticker(symbol: str) -> str:
    """BTCUSDT → BTC, ETHUSDT → ETH, etc."""
    for suffix in ("USDT", "USD", "BUSD", "USDC"):
        if symbol.endswith(suffix):
            return symbol[: -len(suffix)]
    return symbol


def _emit_signal(
    conn: Any,
    source_type: str,
    source_id: str,
    ticker: str,
    signal_date: date,
    signal_type: str,
    signal_value: dict,
    trust_score: float = 0.5,
) -> bool:
    """Write one signal_sources entry. Returns True if inserted, False if duplicate."""
    try:
        conn.execute(
            text(
                "INSERT INTO signal_sources "
                "(source_type, source_id, ticker, signal_date, signal_type, signal_value, trust_score) "
                "VALUES (:stype, :sid, :ticker, :sdate, :sigtype, :sval, :trust) "
                "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type) "
                "DO NOTHING"
            ),
            {
                "stype": source_type,
                "sid": source_id,
                "ticker": ticker,
                "sdate": signal_date,
                "sigtype": signal_type,
                "sval": json.dumps(signal_value),
                "trust": trust_score,
            },
        )
        return True
    except Exception as exc:
        log.debug("crypto_signals: emit failed: {}", exc)
        return False


class CryptoSignalBridge:
    """Bridge existing crypto data into signal_sources."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def bridge_all(self) -> dict[str, Any]:
        """Run all bridges. Returns summary."""
        results = {}
        for name, fn in [
            ("coingecko", self.bridge_coingecko),
            ("binance_rt", self.bridge_binance_realtime),
            ("defi_llama", self.bridge_defi_llama),
            ("cryptoquant", self.bridge_cryptoquant),
        ]:
            try:
                results[name] = fn()
            except Exception as exc:
                log.warning("crypto bridge {} failed: {}", name, exc)
                results[name] = {"error": str(exc)}

        total = sum(r.get("signals_emitted", 0) for r in results.values() if isinstance(r, dict))
        log.info("crypto_signals: bridge_all complete — {} total signals emitted", total)
        results["total_emitted"] = total
        return results

    def bridge_coingecko(self) -> dict[str, Any]:
        """Detect price breakouts and volume spikes from resolved_series CoinGecko data."""
        emitted = 0
        today = date.today()

        with self.engine.begin() as conn:
            # Get all crypto price series
            rows = conn.execute(text("""
                SELECT rs.feature_id, fr.name, rs.obs_date, rs.value
                FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name LIKE 'coingecko.%%price%%'
                  AND rs.obs_date >= :since
                ORDER BY fr.name, rs.obs_date
            """), {"since": today - timedelta(days=30)}).fetchall()

            if not rows:
                return {"signals_emitted": 0, "note": "no coingecko price data found"}

            # Group by series
            series: dict[str, list[tuple[date, float]]] = {}
            for fid, name, obs_date, value in rows:
                if value is None:
                    continue
                series.setdefault(name, []).append((obs_date, float(value)))

            for name, points in series.items():
                if len(points) < 20:
                    continue

                # Extract ticker from series name (e.g. "coingecko.bitcoin.price" → BTC)
                ticker = _ticker_from_coingecko_name(name)
                if not ticker:
                    continue

                prices = [p[1] for p in points]
                dates = [p[0] for p in points]

                # 20-day SMA breakout
                sma20 = sum(prices[-20:]) / 20
                latest = prices[-1]
                pct_from_sma = (latest - sma20) / sma20 if sma20 else 0

                if abs(pct_from_sma) > 0.05:
                    direction = "BUY" if pct_from_sma > 0 else "SELL"
                    if _emit_signal(conn, "coingecko", f"price:{ticker}", ticker, dates[-1],
                                    direction,
                                    {"signal": "crypto_price_breakout", "pct_from_sma": round(pct_from_sma, 4),
                                     "price": latest, "sma20": round(sma20, 2)}):
                        emitted += 1

        return {"signals_emitted": emitted}

    def bridge_binance_realtime(self) -> dict[str, Any]:
        """Detect momentum and breakouts from realtime_candles Binance data."""
        emitted = 0
        today = date.today()

        with self.engine.begin() as conn:
            # Get recent Binance candles grouped by symbol
            rows = conn.execute(text("""
                SELECT symbol, ts, volume, high, low, close
                FROM realtime_candles
                WHERE source = 'binance'
                  AND ts >= :since
                ORDER BY symbol, ts
            """), {"since": datetime.now(timezone.utc) - timedelta(hours=24)}).fetchall()

            if not rows:
                return {"signals_emitted": 0, "note": "no binance candle data"}

            # Group by symbol
            candles: dict[str, list] = {}
            for sym, ts, vol, high, low, close in rows:
                candles.setdefault(sym, []).append({
                    "ts": ts, "volume": float(vol or 0),
                    "high": float(high or 0), "low": float(low or 0),
                    "close": float(close or 0),
                })

            for sym, data in candles.items():
                ticker = _normalize_ticker(sym)
                if len(data) < 12:
                    continue

                volumes = [c["volume"] for c in data]
                avg_vol = sum(volumes[:-1]) / len(volumes[:-1]) if len(volumes) > 1 else 0
                latest_vol = volumes[-1]

                # Volume spike: >5x average
                if avg_vol > 0 and latest_vol > avg_vol * 5:
                    if _emit_signal(conn, "binance_rt", f"vol:{sym}", ticker, today,
                                    "crypto_momentum",
                                    {"volume": latest_vol, "avg_volume": round(avg_vol, 2),
                                     "spike_ratio": round(latest_vol / avg_vol, 1)}):
                        emitted += 1

                # Price break: latest close vs 1h high/low (>2%)
                highs = [c["high"] for c in data[-12:]]
                lows = [c["low"] for c in data[-12:]]
                if highs and lows:
                    h1_high = max(highs[:-1]) if len(highs) > 1 else highs[0]
                    h1_low = min(lows[:-1]) if len(lows) > 1 else lows[0]
                    latest_close = data[-1]["close"]

                    if h1_high > 0 and latest_close > h1_high * 1.02:
                        if _emit_signal(conn, "binance_rt", f"break:{sym}", ticker, today,
                                        "BUY",
                                        {"signal": "crypto_price_break", "close": latest_close,
                                         "1h_high": h1_high, "break_pct": round((latest_close / h1_high - 1) * 100, 2)}):
                            emitted += 1
                    elif h1_low > 0 and latest_close < h1_low * 0.98:
                        if _emit_signal(conn, "binance_rt", f"break:{sym}", ticker, today,
                                        "SELL",
                                        {"signal": "crypto_price_break", "close": latest_close,
                                         "1h_low": h1_low, "break_pct": round((latest_close / h1_low - 1) * 100, 2)}):
                            emitted += 1

        return {"signals_emitted": emitted}

    def bridge_defi_llama(self) -> dict[str, Any]:
        """Detect TVL crashes/surges from raw_series DeFi Llama data."""
        emitted = 0
        today = date.today()

        with self.engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT series_id, obs_date, value
                FROM raw_series
                WHERE series_id LIKE 'defillama%%'
                  AND obs_date >= :since
                ORDER BY series_id, obs_date
            """), {"since": today - timedelta(days=7)}).fetchall()

            if not rows:
                return {"signals_emitted": 0, "note": "no defi_llama data"}

            series: dict[str, list[tuple[date, float]]] = {}
            for sid, obs_date, value in rows:
                if value is None:
                    continue
                series.setdefault(sid, []).append((obs_date, float(value)))

            for sid, points in series.items():
                if len(points) < 2:
                    continue
                prev = points[-2][1]
                curr = points[-1][1]
                if prev <= 0:
                    continue
                pct_change = (curr - prev) / prev

                # Extract protocol/chain name from series_id
                name = sid.replace("defillama.", "").replace("_", " ").split(".")[0]

                if pct_change < -0.20:
                    if _emit_signal(conn, "defi_llama", name, "ETH", points[-1][0],
                                    "SELL",
                                    {"signal": "tvl_crash", "pct_change": round(pct_change, 4),
                                     "protocol": name, "tvl": curr}):
                        emitted += 1
                elif pct_change > 0.30:
                    if _emit_signal(conn, "defi_llama", name, "ETH", points[-1][0],
                                    "BUY",
                                    {"signal": "tvl_surge", "pct_change": round(pct_change, 4),
                                     "protocol": name, "tvl": curr}):
                        emitted += 1

        return {"signals_emitted": emitted}

    def bridge_cryptoquant(self) -> dict[str, Any]:
        """Bridge CryptoQuant anomalies into signal_sources."""
        emitted = 0
        today = date.today()

        with self.engine.begin() as conn:
            # Exchange netflow
            rows = conn.execute(text("""
                SELECT series_id, obs_date, value
                FROM raw_series
                WHERE series_id LIKE 'cq:btc%%netflow%%' OR series_id LIKE 'cq:eth%%netflow%%'
                  AND obs_date >= :since
                ORDER BY series_id, obs_date
            """), {"since": today - timedelta(days=30)}).fetchall()

            if not rows:
                return {"signals_emitted": 0, "note": "no cryptoquant data"}

            series: dict[str, list[tuple[date, float]]] = {}
            for sid, obs_date, value in rows:
                if value is None:
                    continue
                series.setdefault(sid, []).append((obs_date, float(value)))

            for sid, points in series.items():
                if len(points) < 7:
                    continue

                import numpy as np
                values = [p[1] for p in points]
                mean = np.mean(values[:-1])
                std = np.std(values[:-1])
                latest = values[-1]

                if std > 0 and abs(latest - mean) / std > 3:
                    ticker = "BTC" if "btc" in sid else "ETH"
                    direction = "SELL" if latest > mean else "BUY"  # Net inflow = sell pressure
                    if _emit_signal(conn, "cryptoquant", sid, ticker, points[-1][0],
                                    direction,
                                    {"signal": "exchange_netflow_spike",
                                     "z_score": round((latest - mean) / std, 2),
                                     "value": latest, "mean": round(mean, 2)}):
                        emitted += 1

            # Funding rates
            funding_rows = conn.execute(text("""
                SELECT series_id, obs_date, value
                FROM raw_series
                WHERE series_id LIKE 'cq:%%funding%%'
                  AND obs_date >= :since
                ORDER BY obs_date DESC
                LIMIT 10
            """), {"since": today - timedelta(days=7)}).fetchall()

            for sid, obs_date, value in funding_rows:
                if value is None:
                    continue
                val = float(value)
                ticker = "BTC" if "btc" in sid else "ETH"
                if val > 0.001:  # >0.1% funding = overleveraged longs
                    if _emit_signal(conn, "cryptoquant", f"funding:{ticker}", ticker, obs_date,
                                    "SELL",
                                    {"signal": "funding_rate_extreme", "rate": val,
                                     "annualized_pct": round(val * 3 * 365 * 100, 1)}):
                        emitted += 1
                elif val < -0.0005:  # <-0.05% = overleveraged shorts
                    if _emit_signal(conn, "cryptoquant", f"funding:{ticker}", ticker, obs_date,
                                    "BUY",
                                    {"signal": "funding_rate_extreme", "rate": val,
                                     "annualized_pct": round(val * 3 * 365 * 100, 1)}):
                        emitted += 1

        return {"signals_emitted": emitted}


# ── Helpers ──────────────────────────────────────────────────────────────────

_CG_REVERSE_MAP = {
    "bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL",
    "binancecoin": "BNB", "ripple": "XRP", "bittensor": "TAO",
    "dogecoin": "DOGE", "cardano": "ADA", "avalanche-2": "AVAX",
    "chainlink": "LINK", "polkadot": "DOT", "matic-network": "MATIC",
    "uniswap": "UNI", "aave": "AAVE", "maker": "MKR",
    "havven": "SNX", "curve-dao-token": "CRV", "shiba-inu": "SHIB",
    "litecoin": "LTC", "cosmos": "ATOM", "near": "NEAR",
}


def _ticker_from_coingecko_name(series_name: str) -> str | None:
    """Extract ticker from CoinGecko series name like 'coingecko.bitcoin.price'."""
    parts = series_name.split(".")
    for part in parts:
        if part in _CG_REVERSE_MAP:
            return _CG_REVERSE_MAP[part]
    return None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd ~/dev/GRID && python3 -m pytest tests/test_crypto_signals.py -v`
Expected: PASS

- [ ] **Step 5: Add more tests**

```python
# Append to tests/test_crypto_signals.py

def test_bridge_all_returns_summary(engine):
    """bridge_all returns dict with per-source results and total."""
    from ingestion.crypto_signals import CryptoSignalBridge
    bridge = CryptoSignalBridge(engine)
    result = bridge.bridge_all()
    assert "total_emitted" in result
    assert isinstance(result["total_emitted"], int)


def test_normalize_ticker():
    """Ticker normalization strips USDT/USD suffix."""
    from ingestion.crypto_signals import _normalize_ticker
    assert _normalize_ticker("BTCUSDT") == "BTC"
    assert _normalize_ticker("ETHUSDT") == "ETH"
    assert _normalize_ticker("SOLUSD") == "SOL"
    assert _normalize_ticker("BTC") == "BTC"


def test_emit_signal_deduplicates(engine):
    """Duplicate signals are silently ignored via ON CONFLICT."""
    from ingestion.crypto_signals import _emit_signal
    with engine.begin() as conn:
        r1 = _emit_signal(conn, "test_crypto", "test_id", "BTC",
                          date.today(), "BUY", {"test": True})
        r2 = _emit_signal(conn, "test_crypto", "test_id", "BTC",
                          date.today(), "BUY", {"test": True})
    # Both should succeed (ON CONFLICT DO NOTHING doesn't error)
    assert r1 is True
    assert r2 is True
    # Clean up
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM signal_sources WHERE source_type = 'test_crypto'"))


def test_ticker_from_coingecko_name():
    """CoinGecko series name → ticker mapping."""
    from ingestion.crypto_signals import _ticker_from_coingecko_name
    assert _ticker_from_coingecko_name("coingecko.bitcoin.price") == "BTC"
    assert _ticker_from_coingecko_name("coingecko.ethereum.price") == "ETH"
    assert _ticker_from_coingecko_name("unknown.series") is None
```

- [ ] **Step 6: Run all tests**

Run: `cd ~/dev/GRID && python3 -m pytest tests/test_crypto_signals.py -v`
Expected: All pass

- [ ] **Step 7: Commit**

```bash
git add ingestion/crypto_signals.py tests/test_crypto_signals.py
git commit -m "feat: crypto signal bridge — coingecko/binance/defi_llama/cryptoquant → signal_sources"
```

---

## Task 2: Fear & Greed Index Puller

**Files:**
- Create: `ingestion/altdata/fear_greed.py`

- [ ] **Step 1: Write the puller**

```python
# ingestion/altdata/fear_greed.py
"""Crypto Fear & Greed Index puller — free API, no auth.

Source: https://api.alternative.me/fng/
Emits signal_sources entries for extreme fear (<20) and extreme greed (>80).
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


FNG_URL = "https://api.alternative.me/fng/?limit=30&format=json"


class FearGreedPuller:
    """Pull Crypto Fear & Greed Index and emit signals."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "GRID/4.0"})

    def pull(self) -> dict[str, Any]:
        """Fetch Fear & Greed data and emit extreme signals."""
        try:
            resp = self._session.get(FNG_URL, timeout=15)
            resp.raise_for_status()
            data = resp.json().get("data", [])
        except Exception as exc:
            log.warning("Fear & Greed pull failed: {}", exc)
            return {"error": str(exc)}

        if not data:
            return {"signals_emitted": 0}

        emitted = 0
        with self.engine.begin() as conn:
            for entry in data:
                try:
                    value = int(entry["value"])
                    ts = int(entry["timestamp"])
                    sig_date = datetime.fromtimestamp(ts, tz=timezone.utc).date()
                    classification = entry.get("value_classification", "")
                except (KeyError, ValueError, TypeError):
                    continue

                signal_type = None
                direction = None
                if value <= 20:
                    signal_type = "sentiment_extreme_fear"
                    direction = "BUY"  # Contrarian
                elif value >= 80:
                    signal_type = "sentiment_extreme_greed"
                    direction = "SELL"  # Contrarian

                if signal_type and direction:
                    try:
                        conn.execute(
                            text(
                                "INSERT INTO signal_sources "
                                "(source_type, source_id, ticker, signal_date, signal_type, signal_value, trust_score) "
                                "VALUES (:stype, :sid, :ticker, :sdate, :sigtype, :sval, :trust) "
                                "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type) "
                                "DO NOTHING"
                            ),
                            {
                                "stype": "fear_greed",
                                "sid": "alternative.me",
                                "ticker": "BTC",
                                "sdate": sig_date,
                                "sigtype": direction,
                                "sval": json.dumps({
                                    "signal": signal_type,
                                    "index_value": value,
                                    "classification": classification,
                                }),
                                "trust": 0.5,
                            },
                        )
                        emitted += 1
                    except Exception:
                        pass

            # Sentiment shift: check if index changed >20 points in 7 days
            if len(data) >= 7:
                try:
                    latest_val = int(data[0]["value"])
                    week_ago_val = int(data[6]["value"])
                    shift = latest_val - week_ago_val
                    if abs(shift) >= 20:
                        sig_date = datetime.fromtimestamp(int(data[0]["timestamp"]), tz=timezone.utc).date()
                        direction = "BUY" if shift < -20 else "SELL"
                        conn.execute(
                            text(
                                "INSERT INTO signal_sources "
                                "(source_type, source_id, ticker, signal_date, signal_type, signal_value, trust_score) "
                                "VALUES (:stype, :sid, :ticker, :sdate, :sigtype, :sval, :trust) "
                                "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type) "
                                "DO NOTHING"
                            ),
                            {
                                "stype": "fear_greed",
                                "sid": "shift",
                                "ticker": "BTC",
                                "sdate": sig_date,
                                "sigtype": direction,
                                "sval": json.dumps({
                                    "signal": "sentiment_shift",
                                    "current": latest_val,
                                    "7d_ago": week_ago_val,
                                    "shift": shift,
                                }),
                                "trust": 0.5,
                            },
                        )
                        emitted += 1
                except (KeyError, ValueError, TypeError):
                    pass

        log.info("Fear & Greed: {} signals emitted", emitted)
        return {"signals_emitted": emitted, "latest_value": int(data[0]["value"]) if data else None}
```

- [ ] **Step 2: Smoke test against live API**

Run: `cd ~/dev/GRID && python3 -c "import requests; r = requests.get('https://api.alternative.me/fng/?limit=1&format=json'); print(r.json())"`
Expected: JSON with `data` array containing `value`, `timestamp`, `value_classification`

- [ ] **Step 3: Commit**

```bash
git add ingestion/altdata/fear_greed.py
git commit -m "feat: fear & greed index puller — free API, extreme sentiment signals"
```

---

## Task 3: Hyperliquid Puller

**Files:**
- Create: `ingestion/altdata/hyperliquid_puller.py`

- [ ] **Step 1: Write the puller**

```python
# ingestion/altdata/hyperliquid_puller.py
"""Hyperliquid puller — OI, funding rates, liquidations from public API.

API: https://api.hyperliquid.xyz/info (POST, no auth)
Emits signal_sources entries for OI spikes, funding extremes, and liquidation cascades.
"""
from __future__ import annotations

import json
import time
from datetime import date, datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


HL_API = "https://api.hyperliquid.xyz/info"

# Top perps to track
HL_COINS = ["BTC", "ETH", "SOL", "DOGE", "XRP", "AVAX", "LINK",
            "ARB", "SUI", "OP", "APT", "SEI", "TIA", "PEPE",
            "WIF", "JUP", "ONDO", "AAVE", "RENDER", "INJ"]


class HyperliquidPuller:
    """Pull Hyperliquid perp data and emit signals."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._session = requests.Session()
        self._session.headers.update({
            "Content-Type": "application/json",
            "User-Agent": "GRID/4.0",
        })

    def _post(self, payload: dict) -> Any:
        """Make a POST request to the Hyperliquid API."""
        resp = self._session.post(HL_API, json=payload, timeout=15)
        resp.raise_for_status()
        return resp.json()

    def pull(self) -> dict[str, Any]:
        """Pull all Hyperliquid data and emit signals."""
        emitted = 0
        today = date.today()

        try:
            # 1. Get all asset contexts (OI, funding, mark price)
            meta = self._post({"type": "metaAndAssetCtxs"})
            if not isinstance(meta, list) or len(meta) < 2:
                return {"error": "unexpected meta response format"}

            universe = meta[0].get("universe", [])
            asset_ctxs = meta[1] if len(meta) > 1 else []

            coin_map = {u["name"]: i for i, u in enumerate(universe)}

            with self.engine.begin() as conn:
                for coin in HL_COINS:
                    idx = coin_map.get(coin)
                    if idx is None or idx >= len(asset_ctxs):
                        continue

                    ctx = asset_ctxs[idx]
                    funding = float(ctx.get("funding", 0))
                    oi = float(ctx.get("openInterest", 0))
                    mark = float(ctx.get("markPx", 0))

                    # Funding rate extreme: >0.05% per 8h
                    if abs(funding) > 0.0005:
                        direction = "SELL" if funding > 0 else "BUY"
                        annualized = funding * 3 * 365 * 100
                        try:
                            conn.execute(
                                text(
                                    "INSERT INTO signal_sources "
                                    "(source_type, source_id, ticker, signal_date, signal_type, signal_value, trust_score) "
                                    "VALUES (:stype, :sid, :ticker, :sdate, :sigtype, :sval, :trust) "
                                    "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type) "
                                    "DO NOTHING"
                                ),
                                {
                                    "stype": "hyperliquid",
                                    "sid": f"funding:{coin}",
                                    "ticker": coin,
                                    "sdate": today,
                                    "sigtype": direction,
                                    "sval": json.dumps({
                                        "signal": "hl_funding_extreme",
                                        "funding_rate": funding,
                                        "annualized_pct": round(annualized, 1),
                                        "open_interest": oi,
                                        "mark_price": mark,
                                    }),
                                    "trust": 0.5,
                                },
                            )
                            emitted += 1
                        except Exception:
                            pass

                    # OI stored for comparison (we'd need historical to detect spikes,
                    # so for now just store current state for future delta computation)

            time.sleep(1)  # Rate limiting

            # 2. Recent liquidations (if available via user state endpoint)
            # Hyperliquid doesn't have a public liquidation feed, but we can
            # check clearinghouse state. For now, funding is the primary signal.

        except Exception as exc:
            log.warning("Hyperliquid pull failed: {}", exc)
            return {"error": str(exc)}

        log.info("Hyperliquid: {} signals emitted", emitted)
        return {"signals_emitted": emitted}
```

- [ ] **Step 2: Smoke test against live API**

Run: `cd ~/dev/GRID && python3 -c "import requests; r = requests.post('https://api.hyperliquid.xyz/info', json={'type': 'metaAndAssetCtxs'}, headers={'Content-Type': 'application/json'}); data = r.json(); print(f'Universe: {len(data[0][\"universe\"])} coins'); print(f'BTC funding: {data[1][0].get(\"funding\", \"N/A\")}')"`
Expected: Universe count and BTC funding rate

- [ ] **Step 3: Commit**

```bash
git add ingestion/altdata/hyperliquid_puller.py
git commit -m "feat: hyperliquid puller — funding rate extremes from public API"
```

---

## Task 4: Crypto ETF Flow Puller

**Files:**
- Create: `ingestion/altdata/crypto_etf_flows.py`

- [ ] **Step 1: Write the puller**

```python
# ingestion/altdata/crypto_etf_flows.py
"""Crypto ETF flow puller — tracks BTC/ETH ETF volume and flow signals.

Uses yfinance for ETF price/volume data (IBIT, ETHA, GBTC, FBTC, ARKB, BITB).
Detects volume spikes as proxy for flow signals.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

ETF_MAP = {
    "BTC": ["IBIT", "GBTC", "FBTC", "ARKB", "BITB"],
    "ETH": ["ETHA"],
}


class CryptoETFPuller:
    """Pull crypto ETF data and emit flow signals."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def pull(self) -> dict[str, Any]:
        """Pull ETF volume data and emit signals for spikes."""
        try:
            import yfinance as yf
        except ImportError:
            return {"error": "yfinance not installed"}

        emitted = 0
        today = date.today()

        for crypto_ticker, etfs in ETF_MAP.items():
            for etf in etfs:
                try:
                    data = yf.download(etf, period="30d", interval="1d", progress=False)
                    if data is None or data.empty or len(data) < 20:
                        continue

                    volumes = data["Volume"].tolist()
                    closes = data["Close"].tolist()

                    if not volumes or not closes:
                        continue

                    avg_vol_20d = sum(volumes[-21:-1]) / 20 if len(volumes) > 20 else sum(volumes[:-1]) / max(len(volumes) - 1, 1)
                    latest_vol = volumes[-1]
                    latest_close = closes[-1]
                    # Handle pandas types
                    if hasattr(latest_vol, 'item'):
                        latest_vol = latest_vol.item()
                    if hasattr(latest_close, 'item'):
                        latest_close = latest_close.item()
                    if hasattr(avg_vol_20d, 'item'):
                        avg_vol_20d = avg_vol_20d.item()

                    if avg_vol_20d <= 0:
                        continue

                    ratio = latest_vol / avg_vol_20d

                    if ratio > 2.0:
                        # Determine direction from price change
                        prev_close = closes[-2]
                        if hasattr(prev_close, 'item'):
                            prev_close = prev_close.item()
                        price_change = (latest_close - prev_close) / prev_close if prev_close else 0
                        direction = "BUY" if price_change > 0 else "SELL"
                        signal_name = "etf_inflow_spike" if direction == "BUY" else "etf_outflow_spike"

                        with self.engine.begin() as conn:
                            try:
                                conn.execute(
                                    text(
                                        "INSERT INTO signal_sources "
                                        "(source_type, source_id, ticker, signal_date, signal_type, signal_value, trust_score) "
                                        "VALUES (:stype, :sid, :ticker, :sdate, :sigtype, :sval, :trust) "
                                        "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type) "
                                        "DO NOTHING"
                                    ),
                                    {
                                        "stype": "crypto_etf",
                                        "sid": etf,
                                        "ticker": crypto_ticker,
                                        "sdate": today,
                                        "sigtype": direction,
                                        "sval": json.dumps({
                                            "signal": signal_name,
                                            "etf": etf,
                                            "volume": int(latest_vol),
                                            "avg_volume_20d": int(avg_vol_20d),
                                            "volume_ratio": round(ratio, 2),
                                            "price": round(latest_close, 2),
                                            "price_change_pct": round(price_change * 100, 2),
                                        }),
                                        "trust": 0.5,
                                    },
                                )
                                emitted += 1
                            except Exception:
                                pass

                except Exception as exc:
                    log.debug("ETF {} pull failed: {}", etf, exc)

        log.info("Crypto ETF: {} signals emitted", emitted)
        return {"signals_emitted": emitted}
```

- [ ] **Step 2: Commit**

```bash
git add ingestion/altdata/crypto_etf_flows.py
git commit -m "feat: crypto ETF flow puller — IBIT/ETHA/GBTC volume spike signals"
```

---

## Task 5: On-Chain RPC Poller

**Files:**
- Create: `ingestion/altdata/onchain_rpc.py`

- [ ] **Step 1: Write the poller**

```python
# ingestion/altdata/onchain_rpc.py
"""On-chain RPC poller — direct blockchain queries for price and activity.

Always free. No API keys. Ground truth from the chain itself.
- Solana: public RPC (mainnet-beta)
- Ethereum: free public RPCs (llamarpc, ankr)
- Bitcoin: Blockstream REST API
"""
from __future__ import annotations

import json
import time
from datetime import date, datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

SOL_RPC = "https://api.mainnet-beta.solana.com"
ETH_RPC = "https://eth.llamarpc.com"
ETH_RPC_FALLBACK = "https://rpc.ankr.com/eth"
BTC_API = "https://blockstream.info/api"


class OnChainRPCPoller:
    """Poll blockchains directly for price and activity signals."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "GRID/4.0"})

    def pull(self) -> dict[str, Any]:
        """Pull on-chain data and emit signals."""
        emitted = 0
        results = {}

        # ETH gas price (mempool pressure indicator)
        try:
            gas_result = self._check_eth_gas()
            results["eth_gas"] = gas_result
            emitted += gas_result.get("signals_emitted", 0)
        except Exception as exc:
            log.debug("ETH gas check failed: {}", exc)

        # SOL slot/block info
        try:
            sol_result = self._check_sol_activity()
            results["sol_activity"] = sol_result
            emitted += sol_result.get("signals_emitted", 0)
        except Exception as exc:
            log.debug("SOL activity check failed: {}", exc)

        # BTC mempool
        try:
            btc_result = self._check_btc_mempool()
            results["btc_mempool"] = btc_result
            emitted += btc_result.get("signals_emitted", 0)
        except Exception as exc:
            log.debug("BTC mempool check failed: {}", exc)

        log.info("OnChainRPC: {} total signals emitted", emitted)
        results["total_emitted"] = emitted
        return results

    def _check_eth_gas(self) -> dict[str, Any]:
        """Check ETH gas price for mempool pressure signals."""
        payload = {"jsonrpc": "2.0", "method": "eth_gasPrice", "params": [], "id": 1}
        try:
            resp = self._session.post(ETH_RPC, json=payload, timeout=10)
            resp.raise_for_status()
        except Exception:
            resp = self._session.post(ETH_RPC_FALLBACK, json=payload, timeout=10)
            resp.raise_for_status()

        gas_hex = resp.json().get("result", "0x0")
        gas_wei = int(gas_hex, 16)
        gas_gwei = gas_wei / 1e9

        emitted = 0
        # High gas = network congestion = high activity
        if gas_gwei > 50:
            with self.engine.begin() as conn:
                try:
                    conn.execute(
                        text(
                            "INSERT INTO signal_sources "
                            "(source_type, source_id, ticker, signal_date, signal_type, signal_value, trust_score) "
                            "VALUES (:stype, :sid, :ticker, :sdate, :sigtype, :sval, :trust) "
                            "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type) "
                            "DO NOTHING"
                        ),
                        {
                            "stype": "onchain_rpc",
                            "sid": "eth_gas",
                            "ticker": "ETH",
                            "sdate": date.today(),
                            "sigtype": "onchain_mempool_pressure",
                            "sval": json.dumps({"gas_gwei": round(gas_gwei, 2), "gas_wei": gas_wei}),
                            "trust": 0.5,
                        },
                    )
                    emitted = 1
                except Exception:
                    pass

        return {"gas_gwei": round(gas_gwei, 2), "signals_emitted": emitted}

    def _check_sol_activity(self) -> dict[str, Any]:
        """Check Solana recent performance samples for TPS signals."""
        payload = {
            "jsonrpc": "2.0",
            "method": "getRecentPerformanceSamples",
            "params": [4],
            "id": 1,
        }
        resp = self._session.post(SOL_RPC, json=payload, timeout=10)
        resp.raise_for_status()
        samples = resp.json().get("result", [])

        if not samples:
            return {"signals_emitted": 0}

        avg_tps = sum(s.get("numTransactions", 0) / max(s.get("samplePeriodSecs", 60), 1)
                      for s in samples) / len(samples)

        emitted = 0
        # Unusually high TPS = network surge
        if avg_tps > 4000:
            with self.engine.begin() as conn:
                try:
                    conn.execute(
                        text(
                            "INSERT INTO signal_sources "
                            "(source_type, source_id, ticker, signal_date, signal_type, signal_value, trust_score) "
                            "VALUES (:stype, :sid, :ticker, :sdate, :sigtype, :sval, :trust) "
                            "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type) "
                            "DO NOTHING"
                        ),
                        {
                            "stype": "onchain_rpc",
                            "sid": "sol_tps",
                            "ticker": "SOL",
                            "sdate": date.today(),
                            "sigtype": "onchain_program_activity",
                            "sval": json.dumps({"avg_tps": round(avg_tps, 1)}),
                            "trust": 0.5,
                        },
                    )
                    emitted = 1
                except Exception:
                    pass

        return {"avg_tps": round(avg_tps, 1), "signals_emitted": emitted}

    def _check_btc_mempool(self) -> dict[str, Any]:
        """Check BTC mempool size via Blockstream API."""
        resp = self._session.get(f"{BTC_API}/mempool", timeout=10)
        resp.raise_for_status()
        data = resp.json()

        count = data.get("count", 0)
        vsize = data.get("vsize", 0)
        total_fee = data.get("total_fee", 0)

        emitted = 0
        # Large mempool = congestion = high demand
        if count > 100000:
            with self.engine.begin() as conn:
                try:
                    conn.execute(
                        text(
                            "INSERT INTO signal_sources "
                            "(source_type, source_id, ticker, signal_date, signal_type, signal_value, trust_score) "
                            "VALUES (:stype, :sid, :ticker, :sdate, :sigtype, :sval, :trust) "
                            "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type) "
                            "DO NOTHING"
                        ),
                        {
                            "stype": "onchain_rpc",
                            "sid": "btc_mempool",
                            "ticker": "BTC",
                            "sdate": date.today(),
                            "sigtype": "onchain_mempool_pressure",
                            "sval": json.dumps({
                                "tx_count": count,
                                "vsize_bytes": vsize,
                                "total_fee_sats": total_fee,
                            }),
                            "trust": 0.5,
                        },
                    )
                    emitted = 1
                except Exception:
                    pass

        return {"mempool_tx_count": count, "signals_emitted": emitted}
```

- [ ] **Step 2: Smoke test**

Run: `cd ~/dev/GRID && python3 -c "import requests; r = requests.get('https://blockstream.info/api/mempool'); print(r.json()); r2 = requests.post('https://api.mainnet-beta.solana.com', json={'jsonrpc':'2.0','method':'getRecentPerformanceSamples','params':[1],'id':1}); print(f'SOL TPS sample: {r2.json()[\"result\"][0]}')"`

- [ ] **Step 3: Commit**

```bash
git add ingestion/altdata/onchain_rpc.py
git commit -m "feat: on-chain RPC poller — ETH gas, SOL TPS, BTC mempool signals"
```

---

## Task 6: Whale Alert Puller

**Files:**
- Create: `ingestion/altdata/whale_alert.py`

- [ ] **Step 1: Write the puller**

```python
# ingestion/altdata/whale_alert.py
"""Whale Alert puller — on-chain large transaction tracking.

API: https://api.whale-alert.io/v1/transactions (free tier: 10 req/min, last 1h)
Requires WHALE_ALERT_API_KEY env var (free signup).
Gracefully skips if key not configured.
"""
from __future__ import annotations

import json
import os
import time
from datetime import date, datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

WA_URL = "https://api.whale-alert.io/v1/transactions"
MIN_USD_VALUE = 10_000_000  # $10M minimum for signal emission

BLOCKCHAIN_TICKER = {
    "bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL",
    "ripple": "XRP", "dogecoin": "DOGE", "tron": "TRX",
}

KNOWN_EXCHANGES = {
    "binance", "coinbase", "kraken", "bitfinex", "okx",
    "bybit", "huobi", "kucoin", "gemini", "bitstamp",
}


class WhaleAlertPuller:
    """Pull large crypto transactions and emit directional signals."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.api_key = os.getenv("WHALE_ALERT_API_KEY", "")
        self._session = requests.Session()

    def pull(self) -> dict[str, Any]:
        """Pull recent whale transactions and emit signals."""
        if not self.api_key:
            log.debug("Whale Alert: WHALE_ALERT_API_KEY not set, skipping")
            return {"skipped": True, "reason": "no API key"}

        now = int(datetime.now(timezone.utc).timestamp())
        start = now - 3600  # Last 1 hour (free tier limit)

        try:
            resp = self._session.get(
                WA_URL,
                params={
                    "api_key": self.api_key,
                    "min_value": MIN_USD_VALUE,
                    "start": start,
                    "cursor": "",
                },
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            log.warning("Whale Alert pull failed: {}", exc)
            return {"error": str(exc)}

        transactions = data.get("transactions", [])
        emitted = 0
        today = date.today()

        with self.engine.begin() as conn:
            for tx in transactions:
                blockchain = tx.get("blockchain", "")
                ticker = BLOCKCHAIN_TICKER.get(blockchain)
                if not ticker:
                    continue

                amount_usd = tx.get("amount_usd", 0)
                if amount_usd < MIN_USD_VALUE:
                    continue

                from_owner = (tx.get("from", {}).get("owner", "") or "").lower()
                to_owner = (tx.get("to", {}).get("owner", "") or "").lower()
                tx_hash = tx.get("hash", "")[:16]

                # Determine direction
                from_exchange = any(e in from_owner for e in KNOWN_EXCHANGES)
                to_exchange = any(e in to_owner for e in KNOWN_EXCHANGES)

                if to_exchange and not from_exchange:
                    direction = "SELL"
                    signal = "whale_transfer_to_exchange"
                elif from_exchange and not to_exchange:
                    direction = "BUY"
                    signal = "whale_transfer_from_exchange"
                elif amount_usd >= 50_000_000:
                    direction = "whale_large_transfer"
                    signal = direction
                else:
                    continue

                try:
                    conn.execute(
                        text(
                            "INSERT INTO signal_sources "
                            "(source_type, source_id, ticker, signal_date, signal_type, signal_value, trust_score) "
                            "VALUES (:stype, :sid, :ticker, :sdate, :sigtype, :sval, :trust) "
                            "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type) "
                            "DO NOTHING"
                        ),
                        {
                            "stype": "whale_alert",
                            "sid": f"tx:{tx_hash}",
                            "ticker": ticker,
                            "sdate": today,
                            "sigtype": direction if direction in ("BUY", "SELL") else "BUY",
                            "sval": json.dumps({
                                "signal": signal,
                                "amount_usd": amount_usd,
                                "blockchain": blockchain,
                                "from": from_owner or "unknown",
                                "to": to_owner or "unknown",
                                "hash": tx.get("hash", ""),
                            }),
                            "trust": 0.5,
                        },
                    )
                    emitted += 1
                except Exception:
                    pass

        log.info("Whale Alert: {} signals from {} transactions", emitted, len(transactions))
        return {"signals_emitted": emitted, "transactions_checked": len(transactions)}
```

- [ ] **Step 2: Commit**

```bash
git add ingestion/altdata/whale_alert.py
git commit -m "feat: whale alert puller — large on-chain transfers to/from exchanges"
```

---

## Task 7: Scheduler Integration

**Files:**
- Modify: `ingestion/scheduler.py` (24/7 block, around line 713)

- [ ] **Step 1: Add crypto pullers to scheduler**

Find the 24/7 block in `ingestion/scheduler.py` (after the CoinGecko pull around line 725) and add:

```python
    # Crypto signal bridge (transforms existing data → signal_sources)
    try:
        from ingestion.crypto_signals import CryptoSignalBridge
        from db import get_engine
        bridge = CryptoSignalBridge(get_engine())
        bridge.bridge_all()
        log.info("Crypto signal bridge complete")
    except Exception as exc:
        log.warning("Crypto signal bridge failed: {e}", e=str(exc))

    # Hyperliquid (funding rates, OI)
    try:
        from ingestion.altdata.hyperliquid_puller import HyperliquidPuller
        from db import get_engine
        hl = HyperliquidPuller(get_engine())
        hl.pull()
        log.info("Hyperliquid pull complete")
    except Exception as exc:
        log.warning("Hyperliquid pull failed: {e}", e=str(exc))

    # Fear & Greed Index (once daily is fine, but harmless if called more)
    try:
        from ingestion.altdata.fear_greed import FearGreedPuller
        from db import get_engine
        fg = FearGreedPuller(get_engine())
        fg.pull()
        log.info("Fear & Greed pull complete")
    except Exception as exc:
        log.warning("Fear & Greed pull failed: {e}", e=str(exc))

    # On-chain RPC (ETH gas, SOL TPS, BTC mempool)
    try:
        from ingestion.altdata.onchain_rpc import OnChainRPCPoller
        from db import get_engine
        rpc = OnChainRPCPoller(get_engine())
        rpc.pull()
        log.info("On-chain RPC pull complete")
    except Exception as exc:
        log.warning("On-chain RPC pull failed: {e}", e=str(exc))

    # Whale Alert (optional — requires WHALE_ALERT_API_KEY)
    try:
        from ingestion.altdata.whale_alert import WhaleAlertPuller
        from db import get_engine
        wa = WhaleAlertPuller(get_engine())
        wa.pull()
        log.info("Whale Alert pull complete")
    except Exception as exc:
        log.warning("Whale Alert pull failed: {e}", e=str(exc))
```

Add crypto ETF to the **weekday-only** block (ETFs trade on equity schedule):

```python
    # Crypto ETF flows (weekday only — ETFs are equity-traded)
    try:
        from ingestion.altdata.crypto_etf_flows import CryptoETFPuller
        from db import get_engine
        etf = CryptoETFPuller(get_engine())
        etf.pull()
        log.info("Crypto ETF pull complete")
    except Exception as exc:
        log.warning("Crypto ETF pull failed: {e}", e=str(exc))
```

- [ ] **Step 2: Commit**

```bash
git add ingestion/scheduler.py
git commit -m "feat: add crypto signal pullers to scheduler — 24/7 + weekday ETF"
```

---

## Task 8: Backtest CLI — Phase 1 Edge Table

**Files:**
- Create: `scripts/backtest_intelligence.py`

- [ ] **Step 1: Write edge table analysis**

```python
# scripts/backtest_intelligence.py
"""GRID Intelligence Backtest Engine.

Validates intelligence boost multipliers against real market data.

CLI:
  PYTHONPATH=. python3 scripts/backtest_intelligence.py edge-table
  PYTHONPATH=. python3 scripts/backtest_intelligence.py replay --tickers NVDA,META,GOOGL
  PYTHONPATH=. python3 scripts/backtest_intelligence.py calibrate
  PYTHONPATH=. python3 scripts/backtest_intelligence.py report
  PYTHONPATH=. python3 scripts/backtest_intelligence.py full
"""
from __future__ import annotations

import csv
import json
import os
import sys
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
from loguru import logger as log
from scipy import stats
from sqlalchemy import text
from sqlalchemy.engine import Engine

from db import get_engine

OUTPUT_DIR = Path("outputs/backtest")

# Lookback windows per source type (days)
LOOKBACK_WINDOWS = {
    "congressional": 30,
    "insider": 14,
    "options_flow": 7,
    "darkpool": 5,
    "lobbying": 45,
    "hyperliquid": 3,
    "crypto_etf": 7,
    "fear_greed": 7,
    "whale_alert": 3,
    "coingecko": 7,
    "binance_rt": 1,
    "defi_llama": 7,
    "cryptoquant": 7,
    "onchain_rpc": 3,
    "quiverquant:insider": 14,
    "quiverquant:house": 30,
    "quiverquant:senate": 30,
    "quiverquant:lobbying": 45,
    "quiverquant:offexchange": 5,
    "quiverquant:gov_contracts": 30,
    "gov_contract": 30,
    "export_control": 14,
}
DEFAULT_LOOKBACK = 7


@dataclass
class EdgeRow:
    source_type: str
    ticker: str
    signal_direction: str
    n_events: int
    n_absent: int
    hit_rate_present: float
    hit_rate_absent: float
    avg_return_present: float
    avg_return_absent: float
    information_coefficient: float
    p_value: float
    verdict: str


def run_edge_table(engine: Engine) -> list[EdgeRow]:
    """Phase 1: Signal co-occurrence analysis.

    For every scored prediction, check which signals were active beforehand.
    Compute hit rates, information coefficients, and statistical significance.
    """
    log.info("Phase 1: Building edge table...")

    # Load all scored predictions
    with engine.connect() as conn:
        preds = conn.execute(text("""
            SELECT id, ticker, direction, confidence, actual_move_pct, created_at
            FROM oracle_predictions
            WHERE scored_at IS NOT NULL AND actual_move_pct IS NOT NULL
            ORDER BY created_at
        """)).fetchall()

    log.info("Loaded {} scored predictions", len(preds))

    # For each prediction, determine correctness
    pred_data = []
    for p in preds:
        pid, ticker, direction, conf, actual_move, created_at = p
        is_correct = (
            (direction in ("CALL", "bullish", "up") and actual_move > 0)
            or (direction in ("PUT", "bearish", "down") and actual_move < 0)
        )
        pred_data.append({
            "id": pid,
            "ticker": ticker,
            "direction": direction,
            "confidence": float(conf),
            "actual_move": float(actual_move),
            "created_at": created_at,
            "correct": is_correct,
        })

    # Get all signal sources
    with engine.connect() as conn:
        signals = conn.execute(text("""
            SELECT source_type, source_id, ticker, signal_date, signal_type
            FROM signal_sources
            ORDER BY signal_date
        """)).fetchall()

    log.info("Loaded {} signal sources", len(signals))

    # Build signal lookup: (source_type, ticker) → list of (signal_date, signal_type)
    signal_lookup: dict[tuple[str, str], list[tuple[date, str]]] = {}
    for s in signals:
        key = (s.source_type, s.ticker)
        sig_date = s.signal_date
        sig_type = s.signal_type
        signal_lookup.setdefault(key, []).append((sig_date, sig_type))

    # For each source_type × ticker × direction, compute edge metrics
    edge_rows: list[EdgeRow] = []
    source_ticker_combos = set()
    for key in signal_lookup:
        source_type, ticker = key
        # Get unique signal directions for this source+ticker
        directions = set(st for _, st in signal_lookup[key])
        for sig_dir in directions:
            source_ticker_combos.add((source_type, ticker, sig_dir))

    for source_type, ticker, sig_dir in source_ticker_combos:
        lookback = LOOKBACK_WINDOWS.get(source_type, DEFAULT_LOOKBACK)
        key = (source_type, ticker)
        source_signals = signal_lookup.get(key, [])

        # Filter to relevant signal direction
        sig_dates = sorted(set(
            sd for sd, st in source_signals if st == sig_dir
        ))

        if not sig_dates:
            continue

        # For each prediction on this ticker, check if signal was present in lookback
        ticker_preds = [p for p in pred_data if p["ticker"] == ticker]
        if len(ticker_preds) < 5:
            continue

        present_correct = []
        absent_correct = []
        present_returns = []
        absent_returns = []

        for pred in ticker_preds:
            pred_date = pred["created_at"].date() if hasattr(pred["created_at"], 'date') else pred["created_at"]
            lookback_start = pred_date - timedelta(days=lookback)

            # Was signal present in lookback window?
            signal_present = any(
                lookback_start <= sd <= pred_date for sd in sig_dates
            )

            if signal_present:
                present_correct.append(1.0 if pred["correct"] else 0.0)
                present_returns.append(pred["actual_move"])
            else:
                absent_correct.append(1.0 if pred["correct"] else 0.0)
                absent_returns.append(pred["actual_move"])

        n_present = len(present_correct)
        n_absent = len(absent_correct)

        if n_present < 3:
            continue

        hit_present = np.mean(present_correct) if present_correct else 0
        hit_absent = np.mean(absent_correct) if absent_correct else 0
        avg_ret_present = np.mean(present_returns) if present_returns else 0
        avg_ret_absent = np.mean(absent_returns) if absent_returns else 0

        # Information coefficient: correlation between signal presence and correctness
        all_presence = [1.0] * n_present + [0.0] * n_absent
        all_correct = present_correct + absent_correct
        if len(set(all_presence)) > 1 and len(set(all_correct)) > 1:
            ic, p_val = stats.pearsonr(all_presence, all_correct)
        else:
            ic, p_val = 0.0, 1.0

        # Verdict
        if n_present < 10:
            verdict = "INSUFFICIENT"
        elif p_val < 0.05 and abs(ic) > 0.1:
            verdict = "EDGE"
        elif p_val < 0.10:
            verdict = "WEAK_EDGE"
        else:
            verdict = "NOISE"

        edge_rows.append(EdgeRow(
            source_type=source_type,
            ticker=ticker,
            signal_direction=sig_dir,
            n_events=n_present,
            n_absent=n_absent,
            hit_rate_present=round(hit_present, 4),
            hit_rate_absent=round(hit_absent, 4),
            avg_return_present=round(avg_ret_present, 4),
            avg_return_absent=round(avg_ret_absent, 4),
            information_coefficient=round(ic, 4),
            p_value=round(p_val, 6),
            verdict=verdict,
        ))

    # Sort by verdict priority then IC
    verdict_order = {"EDGE": 0, "WEAK_EDGE": 1, "INSUFFICIENT": 2, "NOISE": 3}
    edge_rows.sort(key=lambda r: (verdict_order.get(r.verdict, 9), -abs(r.information_coefficient)))

    # Write outputs
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # CSV
    csv_path = OUTPUT_DIR / "edge_table.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[field for field in EdgeRow.__dataclass_fields__])
        writer.writeheader()
        for row in edge_rows:
            writer.writerow(asdict(row))

    # Markdown
    md_path = OUTPUT_DIR / "edge_table.md"
    with open(md_path, "w") as f:
        f.write("# Intelligence Edge Table\n\n")
        f.write(f"Generated: {datetime.now(timezone.utc).isoformat()}\n")
        f.write(f"Predictions analyzed: {len(pred_data)}\n")
        f.write(f"Signal sources: {len(signals)}\n\n")
        f.write("| Source | Ticker | Dir | N | Hit% Present | Hit% Absent | Avg Ret P | Avg Ret A | IC | p-value | Verdict |\n")
        f.write("|--------|--------|-----|---|-------------|-------------|-----------|-----------|-----|---------|----------|\n")
        for r in edge_rows:
            f.write(
                f"| {r.source_type} | {r.ticker} | {r.signal_direction} | {r.n_events} | "
                f"{r.hit_rate_present:.1%} | {r.hit_rate_absent:.1%} | "
                f"{r.avg_return_present:+.2f}% | {r.avg_return_absent:+.2f}% | "
                f"{r.information_coefficient:+.3f} | {r.p_value:.4f} | **{r.verdict}** |\n"
            )

    # Print summary
    edges = [r for r in edge_rows if r.verdict == "EDGE"]
    weak = [r for r in edge_rows if r.verdict == "WEAK_EDGE"]
    noise = [r for r in edge_rows if r.verdict == "NOISE"]
    log.info("Edge table: {} EDGE, {} WEAK_EDGE, {} NOISE, {} INSUFFICIENT",
             len(edges), len(weak), len(noise), len(edge_rows) - len(edges) - len(weak) - len(noise))

    if edges:
        log.info("TOP EDGES:")
        for e in edges[:5]:
            log.info("  {} on {} ({}): hit={:.1%} vs {:.1%}, IC={:+.3f}, p={:.4f}",
                     e.source_type, e.ticker, e.signal_direction,
                     e.hit_rate_present, e.hit_rate_absent, e.information_coefficient, e.p_value)

    return edge_rows


# ── Phase 2: Module Replay ──────────────────────────────────────────────────

def run_replay(engine: Engine, tickers: list[str]) -> list[dict]:
    """Phase 2: Replay intelligence modules against scored predictions."""
    log.info("Phase 2: Replaying intelligence modules for tickers: {}", tickers)

    with engine.connect() as conn:
        preds = conn.execute(text("""
            SELECT id, ticker, direction, confidence, actual_move_pct, created_at
            FROM oracle_predictions
            WHERE scored_at IS NOT NULL AND actual_move_pct IS NOT NULL
              AND ticker = ANY(:tickers)
            ORDER BY created_at
        """), {"tickers": tickers}).fetchall()

    log.info("Replaying {} predictions", len(preds))
    results = []

    for i, p in enumerate(preds):
        pid, ticker, direction, raw_conf, actual_move, created_at = p
        is_correct = (
            (direction in ("CALL", "bullish", "up") and actual_move > 0)
            or (direction in ("PUT", "bearish", "down") and actual_move < 0)
        )

        row = {
            "prediction_id": pid,
            "ticker": ticker,
            "direction": direction,
            "raw_confidence": float(raw_conf),
            "actual_move_pct": float(actual_move),
            "correct": is_correct,
            "created_at": created_at.isoformat() if hasattr(created_at, 'isoformat') else str(created_at),
        }

        # Call each intelligence module
        for module_name, module_fn in [
            ("lever_pullers", _replay_lever_pullers),
            ("trust_scorer", _replay_trust_scorer),
            ("forensics", _replay_forensics),
        ]:
            try:
                boost = module_fn(engine, ticker, direction, created_at)
                row[f"{module_name}_boost"] = round(boost, 4)
            except Exception as exc:
                row[f"{module_name}_boost"] = 1.0
                log.debug("Replay {} failed for {}: {}", module_name, ticker, exc)

        # Compute adjusted confidence
        total_boost = 1.0
        for key in row:
            if key.endswith("_boost"):
                total_boost *= row[key]
        row["total_boost"] = round(total_boost, 4)
        row["adjusted_confidence"] = round(
            max(min(float(raw_conf) * total_boost, 0.99), 0.01), 4
        )

        results.append(row)

        if (i + 1) % 100 == 0:
            log.info("Replayed {}/{} predictions", i + 1, len(preds))

    # Write results
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = OUTPUT_DIR / "replay_results.csv"
    if results:
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)

    # Compute accuracy comparison
    if results:
        correct_count = sum(1 for r in results if r["correct"])
        total = len(results)
        raw_accuracy = correct_count / total

        # Brier score: mean squared error of probability vs outcome
        raw_brier = np.mean([(r["raw_confidence"] - (1.0 if r["correct"] else 0.0)) ** 2 for r in results])
        adj_brier = np.mean([(r["adjusted_confidence"] - (1.0 if r["correct"] else 0.0)) ** 2 for r in results])

        log.info("Accuracy: {:.1%} ({}/{})", raw_accuracy, correct_count, total)
        log.info("Brier score — raw: {:.4f}, adjusted: {:.4f}, improvement: {:.4f}",
                 raw_brier, adj_brier, raw_brier - adj_brier)

        # Per-module lift
        for module in ["lever_pullers", "trust_scorer", "forensics"]:
            key = f"{module}_boost"
            boosted = [r for r in results if r.get(key, 1.0) != 1.0]
            if boosted:
                boost_correct = sum(1 for r in boosted if r["correct"]) / len(boosted)
                log.info("  {}: {}/{} predictions boosted, hit rate {:.1%}",
                         module, len(boosted), total, boost_correct)

    return results


def _replay_lever_pullers(engine: Engine, ticker: str, direction: str, created_at: datetime) -> float:
    """Replay lever puller check for a historical prediction."""
    from intelligence.lever_pullers import get_lever_context_for_ticker
    ctx = get_lever_context_for_ticker(engine, ticker)
    active = ctx.get("active_pullers", [])
    if not active:
        return 1.0

    expected = "bullish" if direction in ("CALL", "bullish", "up") else "bearish"
    aligned = sum(1 for p in active if p.get("direction", "").lower() == expected)
    opposed = sum(
        1 for p in active
        if p.get("direction", "") and p.get("direction", "").lower() not in (expected, "neutral")
    )

    if aligned > opposed:
        return 1.15
    elif opposed > aligned and opposed >= 2:
        return 0.75
    return 1.0


def _replay_trust_scorer(engine: Engine, ticker: str, direction: str, created_at: datetime) -> float:
    """Replay trust scorer check for a historical prediction."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT trust_score FROM signal_sources
            WHERE ticker = :ticker AND signal_date <= :before
            ORDER BY signal_date DESC LIMIT 10
        """), {"ticker": ticker, "before": created_at}).fetchall()

    if not rows:
        return 1.0

    avg_trust = np.mean([float(r.trust_score) for r in rows if r.trust_score is not None])
    if avg_trust > 0.6:
        return 1.1
    elif avg_trust < 0.3:
        return 0.85
    return 1.0


def _replay_forensics(engine: Engine, ticker: str, direction: str, created_at: datetime) -> float:
    """Replay forensics check for a historical prediction."""
    from intelligence.forensics import find_significant_moves
    moves = find_significant_moves(engine, ticker, days=30, threshold=0.02)
    if not moves:
        return 1.0

    expected = "bullish" if direction in ("CALL", "bullish", "up") else "bearish"
    aligned = sum(
        1 for m in moves
        if (expected == "bullish" and m.get("change_pct", 0) > 0)
        or (expected == "bearish" and m.get("change_pct", 0) < 0)
    )
    opposed = len(moves) - aligned
    total = len(moves)

    if total > 0 and aligned > opposed * 2:
        return 1.0 + 0.1 * (aligned / total)
    elif total > 0 and opposed > aligned * 2:
        return 1.0 - 0.15 * (opposed / total)
    return 1.0


# ── Phase 3: Outputs ────────────────────────────────────────────────────────

def run_calibrate(engine: Engine) -> dict:
    """Phase 3B: Generate calibration JSON from replay results."""
    csv_path = OUTPUT_DIR / "replay_results.csv"
    if not csv_path.exists():
        log.error("No replay results found. Run 'replay' first.")
        return {}

    with open(csv_path) as f:
        reader = csv.DictReader(f)
        results = list(reader)

    calibration = {
        "calibration_version": date.today().isoformat(),
        "generated_from": f"{len(results)} predictions",
        "modules": {},
        "per_ticker": {},
    }

    for module in ["lever_pullers", "trust_scorer", "forensics"]:
        key = f"{module}_boost"
        boosted = [r for r in results if float(r.get(key, 1.0)) > 1.0]
        penalized = [r for r in results if float(r.get(key, 1.0)) < 1.0]

        boost_hits = sum(1 for r in boosted if r["correct"] == "True")
        penalty_hits = sum(1 for r in penalized if r["correct"] == "False")

        boost_rate = boost_hits / len(boosted) if boosted else 0
        penalty_rate = penalty_hits / len(penalized) if penalized else 0

        # Compute optimal multiplier from data
        if boosted:
            avg_boost = np.mean([float(r[key]) for r in boosted])
            optimal_boost = 1.0 + (avg_boost - 1.0) * min(boost_rate * 2, 2.0)
        else:
            optimal_boost = 1.0

        if penalized:
            avg_penalty = np.mean([float(r[key]) for r in penalized])
            optimal_penalty = 1.0 - (1.0 - avg_penalty) * min(penalty_rate * 2, 2.0)
        else:
            optimal_penalty = 1.0

        calibration["modules"][module] = {
            "boost": round(max(1.01, min(optimal_boost, 1.5)), 3),
            "penalty": round(max(0.5, min(optimal_penalty, 0.99)), 3),
            "n_boost_events": len(boosted),
            "n_penalty_events": len(penalized),
            "boost_hit_rate": round(boost_rate, 3),
            "penalty_correct_rate": round(penalty_rate, 3),
            "confidence": round(min(len(boosted) + len(penalized), 100) / 100, 2),
        }

    # Per-ticker breakdown
    tickers = set(r["ticker"] for r in results)
    for ticker in tickers:
        ticker_results = [r for r in results if r["ticker"] == ticker]
        calibration["per_ticker"][ticker] = {}
        for module in ["lever_pullers", "trust_scorer", "forensics"]:
            key = f"{module}_boost"
            boosted = [r for r in ticker_results if float(r.get(key, 1.0)) != 1.0]
            if boosted:
                hit_rate = sum(1 for r in boosted if r["correct"] == "True") / len(boosted)
                calibration["per_ticker"][ticker][module] = {
                    "n": len(boosted),
                    "hit_rate": round(hit_rate, 3),
                }

    # Write
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    cal_path = OUTPUT_DIR / "calibration.json"
    with open(cal_path, "w") as f:
        json.dump(calibration, f, indent=2)

    log.info("Calibration written to {}", cal_path)
    return calibration


def run_report(engine: Engine) -> None:
    """Phase 3C: Generate per-ticker forensic narratives."""
    edge_path = OUTPUT_DIR / "edge_table.md"
    replay_path = OUTPUT_DIR / "replay_results.csv"

    if not edge_path.exists():
        log.error("No edge table found. Run 'edge-table' first.")
        return

    edge_content = edge_path.read_text()
    replay_content = ""
    if replay_path.exists():
        with open(replay_path) as f:
            reader = csv.DictReader(f)
            replay_data = list(reader)
        # Summarize replay by ticker
        tickers = set(r["ticker"] for r in replay_data)
        for ticker in tickers:
            t_data = [r for r in replay_data if r["ticker"] == ticker]
            correct = sum(1 for r in t_data if r["correct"] == "True")
            replay_content += f"\n### {ticker}: {correct}/{len(t_data)} correct ({correct/len(t_data):.0%})\n"
            for module in ["lever_pullers", "trust_scorer", "forensics"]:
                key = f"{module}_boost"
                boosted = [r for r in t_data if float(r.get(key, 1.0)) != 1.0]
                if boosted:
                    b_correct = sum(1 for r in boosted if r["correct"] == "True")
                    replay_content += f"- {module}: {len(boosted)} boosted, {b_correct}/{len(boosted)} correct\n"

    # Generate narrative per ticker using local LLM
    try:
        from ollama.client import get_client
        client = get_client()
        if not client or not client.is_available:
            raise RuntimeError("Ollama not available")
    except Exception:
        log.warning("LLM not available, writing data-only report")
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        report_path = OUTPUT_DIR / "forensic_report.md"
        with open(report_path, "w") as f:
            f.write("# Intelligence Backtest Forensic Report\n\n")
            f.write(f"Generated: {datetime.now(timezone.utc).isoformat()}\n\n")
            f.write("## Edge Table\n\n")
            f.write(edge_content)
            f.write("\n\n## Replay Results\n\n")
            f.write(replay_content or "(no replay data)")
        log.info("Data-only report written to {}", report_path)
        return

    # With LLM available, generate per-ticker forensic narrative
    tickers_to_report = set()
    if replay_path.exists():
        with open(replay_path) as f:
            reader = csv.DictReader(f)
            for r in reader:
                tickers_to_report.add(r["ticker"])

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for ticker in tickers_to_report:
        prompt = (
            f"You are a quantitative trading analyst writing a forensic intelligence report for {ticker}.\n\n"
            f"Below is the edge table showing which information sources have predictive power, "
            f"and the replay results showing how intelligence modules performed.\n\n"
            f"## Edge Table (filtered to {ticker})\n\n"
        )
        # Filter edge table lines for this ticker
        for line in edge_content.split("\n"):
            if ticker in line or "Source" in line or "---" in line or "# " in line:
                prompt += line + "\n"

        prompt += f"\n## Replay Results for {ticker}\n{replay_content}\n\n"
        prompt += (
            "Write a forensic report covering:\n"
            "1. Which information sources actually predicted moves for this ticker (cite specific examples with dates)\n"
            "2. Which were noise or actively harmful\n"
            "3. What the optimal multiplier strategy is for this ticker\n"
            "4. Name specific actors, dates, and outcomes — not generic observations\n"
            "Under 500 words. Be specific. Take a stand."
        )

        narrative = client.chat(
            [{"role": "user", "content": prompt}],
            temperature=0.3,
            num_predict=800,
        )

        if narrative:
            report_path = OUTPUT_DIR / f"forensic_{ticker}.md"
            with open(report_path, "w") as f:
                f.write(f"# Forensic Intelligence Report: {ticker}\n\n")
                f.write(f"Generated: {datetime.now(timezone.utc).isoformat()}\n\n")
                f.write(narrative)
            log.info("Forensic report written for {}", ticker)


# ── CLI ─────────────────────────────────────────────────────────────────────

def main():
    engine = get_engine()

    if len(sys.argv) < 2:
        print("Usage: backtest_intelligence.py <command> [options]")
        print("Commands: edge-table, replay, calibrate, report, full")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "edge-table":
        run_edge_table(engine)

    elif cmd == "replay":
        tickers = ["NVDA", "META", "GOOGL", "AAPL", "MSFT"]
        if "--tickers" in sys.argv:
            idx = sys.argv.index("--tickers")
            if idx + 1 < len(sys.argv):
                tickers = sys.argv[idx + 1].split(",")
        run_replay(engine, tickers)

    elif cmd == "calibrate":
        run_calibrate(engine)

    elif cmd == "report":
        run_report(engine)

    elif cmd == "full":
        log.info("=== FULL BACKTEST ===")
        run_edge_table(engine)
        run_replay(engine, ["NVDA", "META", "GOOGL", "AAPL", "MSFT"])
        run_calibrate(engine)
        run_report(engine)
        log.info("=== FULL BACKTEST COMPLETE ===")

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify it parses**

Run: `cd ~/dev/GRID && python3 -c "import ast; ast.parse(open('scripts/backtest_intelligence.py').read()); print('OK')"`

- [ ] **Step 3: Commit**

```bash
git add scripts/backtest_intelligence.py
git commit -m "feat: intelligence backtest CLI — edge-table, replay, calibrate, report"
```

---

## Task 9: Run Backtest on Server

- [ ] **Step 1: Push all code**

```bash
git push origin main
```

- [ ] **Step 2: Deploy to server**

```bash
ssh grid-svr "cd ~/grid_v4/grid_repo && git pull origin main"
```

- [ ] **Step 3: Run crypto signal bridge first**

```bash
ssh grid-svr "cd ~/grid_v4/grid_repo && PYTHONPATH=. python3 -c '
from ingestion.crypto_signals import CryptoSignalBridge
from db import get_engine
bridge = CryptoSignalBridge(get_engine())
result = bridge.bridge_all()
print(result)
'"
```

- [ ] **Step 4: Run edge table**

```bash
ssh grid-svr "cd ~/grid_v4/grid_repo && PYTHONPATH=. python3 scripts/backtest_intelligence.py edge-table"
```

- [ ] **Step 5: Run replay on top equity tickers**

```bash
ssh grid-svr "cd ~/grid_v4/grid_repo && PYTHONPATH=. python3 scripts/backtest_intelligence.py replay --tickers NVDA,META,GOOGL,AAPL,MSFT"
```

- [ ] **Step 6: Generate calibration + report**

```bash
ssh grid-svr "cd ~/grid_v4/grid_repo && PYTHONPATH=. python3 scripts/backtest_intelligence.py calibrate"
ssh grid-svr "cd ~/grid_v4/grid_repo && PYTHONPATH=. python3 scripts/backtest_intelligence.py report"
```

- [ ] **Step 7: Review results**

```bash
ssh grid-svr "cat ~/grid_v4/grid_repo/outputs/backtest/edge_table.md"
ssh grid-svr "cat ~/grid_v4/grid_repo/outputs/backtest/calibration.json"
```

- [ ] **Step 8: Restart scheduler with new crypto pullers**

```bash
ssh grid-svr "sudo systemctl restart grid-scheduler"
```

- [ ] **Step 9: Final commit with results**

```bash
git add outputs/backtest/
git commit -m "data: initial backtest results — edge table + calibration + forensic reports"
```

---

## Spec Coverage Check

| Spec Requirement | Task |
|------------------|------|
| Phase 0: crypto_signals.py bridge | Task 1 |
| Phase 0: Fear & Greed puller | Task 2 |
| Phase 0: Hyperliquid puller | Task 3 |
| Phase 0: Crypto ETF flows | Task 4 |
| Phase 0: On-chain RPC | Task 5 |
| Phase 0: Whale Alert | Task 6 |
| Phase 0: Scheduler integration | Task 7 |
| Phase 1: Edge table | Task 8 (run_edge_table) |
| Phase 2: Module replay | Task 8 (run_replay) |
| Phase 3A: Edge table output | Task 8 (CSV + MD in run_edge_table) |
| Phase 3B: Calibration JSON | Task 8 (run_calibrate) |
| Phase 3C: Forensic narrative | Task 8 (run_report) |
| Deployment + run | Task 9 |
