"""
CoinGecko crypto spot-price puller — free tier, no API key required.

One ``/simple/price`` request per run returns the USD spot quote and its
``last_updated_at`` time for every tracked coin. Each quote is stored in
``raw_series`` under the ``coingecko`` source as ``CG:<coingecko id>:usd``:

* ``obs_date`` is the UTC date of the quote's own ``last_updated_at``, never
  the date of the pull (a stale quote keeps its real date);
* ``pull_timestamp`` is the schema's ``DEFAULT NOW()``, i.e. when GRID
  actually learned the value;
* ``pull_status`` is ``SUCCESS``;
* raw_series is append-only: a series that already has a SUCCESS row for that
  ``obs_date`` is left alone (the first quote of the UTC day is kept).

Values reach ``resolved_series`` only through the normal resolver
(``normalization.entity_map`` maps ``CG:<id>:usd`` to ``<ticker>_usd_full``,
except BTC and ETH, whose ``*_usd_full`` features carry the yfinance daily
close). Before E1-V6 this module wrote ``resolved_series`` directly
(``source_priority_used = 1``, ``release_date = vintage_date = obs_date``,
``ON CONFLICT ... DO UPDATE``); those rows are left in place pending an
owner decision.

With API key (COINGECKO_API_KEY): demo or pro endpoint headers.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# CoinGecko IDs for our tracked crypto
CRYPTO_MAP = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "BNB": "binancecoin",
    "XRP": "ripple",
    "TAO": "bittensor",
    "DOGE": "dogecoin",
    "ADA": "cardano",
    "AVAX": "avalanche-2",
    "LINK": "chainlink",
    "DOT": "polkadot",
    "MATIC": "matic-network",
    "UNI": "uniswap",
    "AAVE": "aave",
    "MKR": "maker",
    "SNX": "havven",
    "CRV": "curve-dao-token",
    "SHIB": "shiba-inu",
    "LTC": "litecoin",
    "ATOM": "cosmos",
    "NEAR": "near",
}


def spot_series_id(cg_id: str) -> str:
    """raw_series id for a CoinGecko USD spot quote."""
    return f"CG:{cg_id}:usd"


class CoinGeckoPuller(BasePuller):
    """Pull crypto USD spot quotes from CoinGecko into ``raw_series``."""

    SOURCE_NAME = "coingecko"
    # Same catalog payload scripts/bulk_historical_pull.py registers.
    SOURCE_CONFIG = {
        "base_url": "https://api.coingecko.com",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": False,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 25,
    }

    def __init__(self, db_engine: Engine) -> None:
        self.api_key = os.getenv("COINGECKO_API_KEY", "")
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "GRID/4.0"})

        # Use Demo endpoint with key, or free endpoint without
        # Note: Demo keys use api.coingecko.com with x-cg-demo-api-key header.
        # Pro keys use pro-api.coingecko.com with x-cg-pro-api-key header.
        api_tier = os.getenv("COINGECKO_API_TIER", "demo").lower()
        if self.api_key and api_tier == "pro":
            self.base_url = "https://pro-api.coingecko.com/api/v3"
            self._session.headers["x-cg-pro-api-key"] = self.api_key
        elif self.api_key:
            self.base_url = "https://api.coingecko.com/api/v3"
            self._session.headers["x-cg-demo-api-key"] = self.api_key
        else:
            self.base_url = "https://api.coingecko.com/api/v3"
        super().__init__(db_engine)

    def _fetch_quotes(self, cg_ids: list[str]) -> dict[str, dict[str, Any]]:
        """One ``/simple/price`` call for every coin: {cg_id: quote}."""
        resp = self._session.get(
            f"{self.base_url}/simple/price",
            params={
                "ids": ",".join(cg_ids),
                "vs_currencies": "usd",
                "include_market_cap": "true",
                "include_24hr_vol": "true",
                "include_last_updated_at": "true",
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            raise ValueError("CoinGecko simple/price returned a non-object payload")
        return data

    def pull_all(self, tickers: list[str] | None = None) -> dict[str, Any]:
        """Store one spot quote per tracked coin (append-only, deduped per UTC day).

        Returns:
            ``{"status", "rows_inserted", "succeeded", "total", ...}``.
            SUCCESS only when every coin had a dated USD quote; a provider
            error or no usable quote at all is FAILED; some missing is PARTIAL.
        """
        wanted = [t.upper() for t in (tickers or list(CRYPTO_MAP))]
        targets = [(t, CRYPTO_MAP[t]) for t in wanted if t in CRYPTO_MAP]
        missing = [t for t in wanted if t not in CRYPTO_MAP]
        total = len(wanted)
        if not targets:
            return {"status": "FAILED", "rows_inserted": 0, "succeeded": 0, "total": total,
                    "error": f"no CoinGecko id for {missing}"}

        try:
            quotes = self._fetch_quotes([cg_id for _, cg_id in targets])
        except Exception as exc:
            log.warning("CoinGecko simple/price failed: {e}", e=str(exc))
            return {"status": "FAILED", "rows_inserted": 0, "succeeded": 0, "total": total,
                    "error": str(exc)}

        inserted = 0
        unchanged = 0
        with self.engine.begin() as conn:
            for ticker, cg_id in targets:
                quote = quotes.get(cg_id)
                price = quote.get("usd") if isinstance(quote, dict) else None
                updated = quote.get("last_updated_at") if isinstance(quote, dict) else None
                if (
                    isinstance(price, bool) or not isinstance(price, (int, float))
                    or isinstance(updated, bool) or not isinstance(updated, (int, float))
                ):
                    missing.append(ticker)
                    continue
                obs_date = datetime.fromtimestamp(updated, tz=timezone.utc).date()
                series_id = spot_series_id(cg_id)
                if obs_date in self._get_existing_dates(series_id, conn, obs_date, obs_date):
                    unchanged += 1
                    continue
                self._insert_raw(
                    conn, series_id, obs_date, float(price),
                    raw_payload={
                        "kind": "spot",
                        "endpoint": "simple/price",
                        "ticker": ticker,
                        "last_updated_at": int(updated),
                        "market_cap_usd": quote.get("usd_market_cap"),
                        "volume_24h_usd": quote.get("usd_24h_vol"),
                    },
                )
                inserted += 1

        succeeded = total - len(missing)
        result: dict[str, Any] = {
            "status": "SUCCESS" if not missing else ("PARTIAL" if succeeded else "FAILED"),
            "rows_inserted": inserted,
            "succeeded": succeeded,
            "total": total,
            "unchanged": unchanged,
        }
        if missing:
            result["error"] = f"no dated USD quote for {sorted(missing)}"
        log.info(
            "CoinGecko spot: {n} rows, {u} already stored, {m} missing",
            n=inserted, u=unchanged, m=len(missing),
        )
        return result
