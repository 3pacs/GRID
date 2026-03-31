"""
Kalshi prediction market puller — public API, no auth for market data.

CFTC-regulated prediction exchange. Pulls event contract prices for
economic indicators, elections, climate, tech events.

Source: https://docs.kalshi.com/getting_started/quick_start_market_data
API: https://api.elections.kalshi.com/trade-api/v2
"""

from __future__ import annotations

import hashlib
from datetime import date
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
_REQUEST_TIMEOUT = 30

_MACRO_CATEGORIES = [
    "Economics", "Finance", "Fed", "Climate", "Technology",
]


class KalshiMarketsPuller(BasePuller):
    """Pulls prediction market data from Kalshi public API."""

    SOURCE_NAME: str = "KALSHI"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://api.elections.kalshi.com",
        "cost_tier": "FREE",
        "latency_class": "REALTIME",
        "pit_available": False,
        "revision_behavior": "FREQUENT",
        "trust_score": "HIGH",
        "priority_rank": 14,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)

    @retry_on_failure(max_attempts=2)
    def pull_all(self, max_markets: int = 100, **kwargs) -> list[dict[str, Any]]:
        """Pull active markets from Kalshi."""
        result: dict[str, Any] = {
            "rows_inserted": 0, "markets_found": 0, "status": "SUCCESS",
        }

        try:
            resp = requests.get(
                f"{_API_BASE}/markets",
                params={"limit": 200, "status": "open"},
                timeout=_REQUEST_TIMEOUT,
                headers={"Accept": "application/json"},
            )
            resp.raise_for_status()
            data = resp.json()

            markets = data.get("markets", [])
            if not markets:
                result["status"] = "PARTIAL"
                return [result]

            result["markets_found"] = len(markets)
            inserted = 0
            today = date.today()

            with self.engine.begin() as conn:
                for market in markets:
                    ticker = market.get("ticker", "")
                    title = market.get("title", "")

                    # Get yes_price (probability 0-100 cents → 0-1)
                    yes_price = market.get("yes_bid", 0) or market.get("last_price", 0)
                    if yes_price > 1:
                        yes_price = yes_price / 100.0  # Convert cents to probability

                    if not ticker or yes_price == 0:
                        continue

                    series_id = f"KALSHI:{ticker}"

                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS') "
                            "ON CONFLICT (series_id, source_id, obs_date, pull_timestamp) DO NOTHING"
                        ),
                        {"sid": series_id, "src": self.source_id, "od": today, "val": float(yes_price)},
                    )
                    inserted += 1

                    if inserted >= max_markets:
                        break

            result["rows_inserted"] = inserted
            log.info("Kalshi: {m} markets, {i} inserted", m=result["markets_found"], i=inserted)

        except Exception as exc:
            log.error("Kalshi pull failed: {e}", e=str(exc))
            result["status"] = "FAILED"
            result["error"] = str(exc)

        return [result]
