"""
Polymarket prediction market puller — no auth, real-time odds.

Uses the Gamma API (public, read-only) to pull active market probabilities.
Stores event probabilities as time series for Oracle consumption.

Source: https://docs.polymarket.com/developers/gamma-markets-api/overview
API: https://gamma-api.polymarket.com
"""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

_API_BASE = "https://gamma-api.polymarket.com"
_REQUEST_TIMEOUT = 30

# Keywords to filter for macro-relevant markets
_MACRO_KEYWORDS = [
    "federal reserve", "interest rate", "inflation", "gdp", "recession",
    "unemployment", "election", "president", "congress", "tariff",
    "bitcoin", "crypto", "stock market", "s&p", "oil price",
    "war", "china", "fed", "treasury", "default",
]

# Keywords that identify Bitcoin-specific prediction markets
_BTC_KEYWORDS = ["bitcoin", "btc"]


class PolymarketPuller(BasePuller):
    """Pulls prediction market odds from Polymarket Gamma API."""

    SOURCE_NAME: str = "POLYMARKET"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://gamma-api.polymarket.com",
        "cost_tier": "FREE",
        "latency_class": "REALTIME",
        "pit_available": False,
        "revision_behavior": "FREQUENT",
        "trust_score": "MED",
        "priority_rank": 15,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)

    def _is_macro_relevant(self, title: str) -> bool:
        lower = title.lower()
        return any(kw in lower for kw in _MACRO_KEYWORDS)

    @retry_on_failure(max_attempts=2)
    def pull_all(self, max_markets: int = 100, **kwargs) -> list[dict[str, Any]]:
        """Pull active macro-relevant markets from Polymarket."""
        result: dict[str, Any] = {
            "rows_inserted": 0, "markets_found": 0, "macro_relevant": 0, "status": "SUCCESS",
        }

        try:
            resp = requests.get(
                f"{_API_BASE}/markets",
                params={"closed": "false", "limit": 200, "order": "volume24hr", "ascending": "false"},
                timeout=_REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            markets = resp.json()

            if not markets:
                result["status"] = "PARTIAL"
                return [result]

            result["markets_found"] = len(markets)
            inserted = 0
            today = date.today()

            # Collect BTC market probabilities for aggregation
            btc_probs: list[float] = []

            with self.engine.begin() as conn:
                for market in markets:
                    question = market.get("question", "")
                    if not self._is_macro_relevant(question):
                        continue

                    result["macro_relevant"] = result.get("macro_relevant", 0) + 1

                    # Extract probability from outcomePrices
                    outcome_prices = market.get("outcomePrices", "")
                    if not outcome_prices:
                        continue

                    try:
                        # outcomePrices is a JSON string like "[0.65, 0.35]"
                        import json
                        prices = json.loads(outcome_prices) if isinstance(outcome_prices, str) else outcome_prices
                        yes_prob = float(prices[0]) if prices else 0.5
                    except (json.JSONDecodeError, IndexError, TypeError):
                        yes_prob = 0.5

                    # Track BTC-specific markets for aggregate signal
                    question_lower = question.lower()
                    if any(kw in question_lower for kw in _BTC_KEYWORDS):
                        btc_probs.append(yes_prob)

                    # Create a stable series_id from question hash
                    slug = market.get("slug", "")
                    q_hash = hashlib.md5(slug.encode()).hexdigest()[:12]
                    series_id = f"POLY:{q_hash}"

                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS') "
                            "ON CONFLICT (series_id, source_id, obs_date, pull_timestamp) DO NOTHING"
                        ),
                        {"sid": series_id, "src": self.source_id, "od": today, "val": yes_prob},
                    )
                    inserted += 1

                    if inserted >= max_markets:
                        break

                # Write aggregate BTC market probability
                if btc_probs:
                    avg_btc_prob = sum(btc_probs) / len(btc_probs)
                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS') "
                            "ON CONFLICT (series_id, source_id, obs_date, pull_timestamp) DO NOTHING"
                        ),
                        {"sid": "POLYMARKET:btc", "src": self.source_id, "od": today, "val": avg_btc_prob},
                    )
                    inserted += 1
                    log.info(
                        "Polymarket BTC aggregate: {n} markets, avg_prob={p:.3f}",
                        n=len(btc_probs), p=avg_btc_prob,
                    )

            result["rows_inserted"] = inserted
            log.info(
                "Polymarket: {m} markets, {r} macro-relevant, {i} inserted",
                m=result["markets_found"], r=result["macro_relevant"], i=inserted,
            )

        except Exception as exc:
            log.error("Polymarket pull failed: {e}", e=str(exc))
            result["status"] = "FAILED"
            result["error"] = str(exc)

        return [result]
