# ingestion/altdata/whale_alert.py
"""Whale Alert puller — on-chain large transaction tracking.

API: https://api.whale-alert.io/v1/transactions (free tier: 10 req/min, last 1h)
Requires WHALE_ALERT_API_KEY env var (free signup).
Gracefully skips if key not configured.
"""
from __future__ import annotations

import json
import os
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
