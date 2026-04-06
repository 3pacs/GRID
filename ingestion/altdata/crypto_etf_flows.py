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
