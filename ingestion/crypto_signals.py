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
