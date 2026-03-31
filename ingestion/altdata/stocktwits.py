"""
StockTwits social sentiment puller — no auth, real-time, built-in labels.

Pulls recent messages for tracked tickers with bullish/bearish sentiment.
No API key required for public streams.

Source: https://api.stocktwits.com/api/2/
"""

from __future__ import annotations

import time
from datetime import date, datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

_API_BASE = "https://api.stocktwits.com/api/2"
_REQUEST_TIMEOUT = 15
_RATE_LIMIT_DELAY = 1.0  # StockTwits allows ~200 requests/hour unauthenticated

# Tickers to track sentiment for (top GRID universe)
_DEFAULT_TICKERS = [
    "SPY", "QQQ", "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA",
    "AMD", "AVGO", "GLD", "TLT", "XLE", "XLF", "XLV",
    "BTC.X", "ETH.X", "SOL.X",
]


class StockTwitsPuller(BasePuller):
    """Pulls social sentiment from StockTwits public API."""

    SOURCE_NAME: str = "STOCKTWITS"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://api.stocktwits.com",
        "cost_tier": "FREE",
        "latency_class": "REALTIME",
        "pit_available": False,
        "revision_behavior": "NEVER",
        "trust_score": "LOW",
        "priority_rank": 20,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)

    @retry_on_failure(max_attempts=2)
    def _pull_ticker(self, ticker: str) -> dict[str, Any]:
        """Pull sentiment for a single ticker."""
        result: dict[str, Any] = {"ticker": ticker, "rows_inserted": 0, "status": "SUCCESS"}

        try:
            resp = requests.get(
                f"{_API_BASE}/streams/symbol/{ticker}.json",
                timeout=_REQUEST_TIMEOUT,
                headers={"User-Agent": "GRID/4.0", "Accept": "application/json"},
            )

            if resp.status_code == 404:
                result["status"] = "PARTIAL"
                return result
            if resp.status_code == 429:
                log.warning("StockTwits rate limited on {t}", t=ticker)
                result["status"] = "RATE_LIMITED"
                return result

            resp.raise_for_status()
            data = resp.json()

            messages = data.get("messages", [])
            if not messages:
                result["status"] = "PARTIAL"
                return result

            # Compute aggregate sentiment
            bullish = sum(1 for m in messages if (m.get("entities", {}).get("sentiment") or {}).get("basic") == "Bullish")
            bearish = sum(1 for m in messages if (m.get("entities", {}).get("sentiment") or {}).get("basic") == "Bearish")
            total_labeled = bullish + bearish

            if total_labeled > 0:
                sentiment_score = (bullish - bearish) / total_labeled  # -1 to +1
            else:
                sentiment_score = 0.0

            today = date.today()
            clean_ticker = ticker.replace(".X", "_USD")

            with self.engine.begin() as conn:
                # Store sentiment score
                conn.execute(
                    text(
                        "INSERT INTO raw_series "
                        "(series_id, source_id, obs_date, value, pull_status) "
                        "VALUES (:sid, :src, :od, :val, 'SUCCESS') "
                        "ON CONFLICT (series_id, source_id, obs_date, pull_timestamp) DO NOTHING"
                    ),
                    {
                        "sid": f"ST:{clean_ticker}:sentiment",
                        "src": self.source_id,
                        "od": today,
                        "val": sentiment_score,
                    },
                )
                # Store message volume
                conn.execute(
                    text(
                        "INSERT INTO raw_series "
                        "(series_id, source_id, obs_date, value, pull_status) "
                        "VALUES (:sid, :src, :od, :val, 'SUCCESS') "
                        "ON CONFLICT (series_id, source_id, obs_date, pull_timestamp) DO NOTHING"
                    ),
                    {
                        "sid": f"ST:{clean_ticker}:volume",
                        "src": self.source_id,
                        "od": today,
                        "val": float(len(messages)),
                    },
                )

            result["rows_inserted"] = 2
            result["bullish"] = bullish
            result["bearish"] = bearish
            result["messages"] = len(messages)

        except Exception as exc:
            log.warning("StockTwits {t} failed: {e}", t=ticker, e=str(exc))
            result["status"] = "FAILED"

        return result

    def pull_all(
        self, ticker_list: list[str] | None = None, **kwargs
    ) -> list[dict[str, Any]]:
        """Pull sentiment for all tracked tickers."""
        tickers = ticker_list or _DEFAULT_TICKERS
        results = []
        succeeded = 0

        for ticker in tickers:
            res = self._pull_ticker(ticker)
            results.append(res)
            if res["status"] == "SUCCESS":
                succeeded += 1
            if res["status"] == "RATE_LIMITED":
                log.info("StockTwits rate limited — stopping after {n} tickers", n=len(results))
                break
            time.sleep(_RATE_LIMIT_DELAY)

        log.info("StockTwits: {s}/{t} tickers pulled", s=succeeded, t=len(tickers))
        return results
