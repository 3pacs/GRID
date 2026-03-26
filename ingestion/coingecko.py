"""
CoinGecko crypto price puller — free tier, no API key required.

Pulls current prices + 7d history for tracked crypto assets.
With API key (COINGECKO_API_KEY): higher rate limits via Pro endpoint.
Without: 30 requests/minute on the free endpoint.

Checks freshness before pulling — skips tickers already fresh today.
"""

from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# CoinGecko IDs for our tracked crypto
CRYPTO_MAP = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "TAO": "bittensor",
    "DOGE": "dogecoin",
    "ADA": "cardano",
    "AVAX": "avalanche-2",
    "LINK": "chainlink",
    "DOT": "polkadot",
    "MATIC": "matic-network",
}


class CoinGeckoPuller:
    """Pull crypto prices from CoinGecko free API."""

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.api_key = os.getenv("COINGECKO_API_KEY", "")
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "GRID/4.0"})

        # Use Pro endpoint if key available, else free
        if self.api_key:
            self.base_url = "https://pro-api.coingecko.com/api/v3"
            self._session.headers["x-cg-pro-api-key"] = self.api_key
        else:
            self.base_url = "https://api.coingecko.com/api/v3"

    def _get_fresh_tickers(self) -> set[str]:
        """Return set of crypto feature names already fresh today."""
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT fr.name FROM feature_registry fr "
                    "JOIN resolved_series rs ON rs.feature_id = fr.id "
                    "WHERE fr.family = 'crypto' AND rs.obs_date >= CURRENT_DATE "
                    "GROUP BY fr.name"
                )).fetchall()
                return {r[0] for r in rows}
        except Exception:
            return set()

    def pull_all(self, tickers: list[str] | None = None) -> list[dict]:
        """Pull prices for all tracked crypto, skipping fresh ones.

        Parameters:
            tickers: Optional list of ticker symbols (e.g., ['BTC', 'ETH']).
                     Defaults to all CRYPTO_MAP keys.

        Returns:
            List of result dicts with keys: ticker, price, date, market_cap, volume_24h.
        """
        targets = tickers or list(CRYPTO_MAP.keys())
        fresh = self._get_fresh_tickers()
        results = []

        for ticker in targets:
            cg_id = CRYPTO_MAP.get(ticker.upper())
            if not cg_id:
                log.debug("No CoinGecko ID for {t}", t=ticker)
                continue

            fname = f"{ticker.lower()}_usd_full"
            if fname in fresh:
                log.debug("Skipping {t} — already fresh today", t=ticker)
                continue

            try:
                data = self._fetch_price(cg_id)
                if data:
                    data["ticker"] = ticker.upper()
                    results.append(data)
                    self._save_to_db(ticker, data)
                    log.info("CoinGecko {t}: ${p:,.2f}", t=ticker, p=data["price"])
                time.sleep(2.5)  # Rate limit: 30/min free tier
            except Exception as exc:
                log.warning("CoinGecko {t} failed: {e}", t=ticker, e=str(exc))

        log.info("CoinGecko pull complete: {n}/{total} tickers",
                 n=len(results), total=len(targets))
        return results

    def _fetch_price(self, cg_id: str) -> dict | None:
        """Fetch current price + metadata for a single coin."""
        url = f"{self.base_url}/coins/{cg_id}"
        params = {
            "localization": "false",
            "tickers": "false",
            "market_data": "true",
            "community_data": "false",
            "developer_data": "false",
            "sparkline": "false",
        }
        resp = self._session.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        md = data.get("market_data", {})
        price = md.get("current_price", {}).get("usd")
        if price is None:
            return None

        return {
            "price": float(price),
            "market_cap": md.get("market_cap", {}).get("usd"),
            "volume_24h": md.get("total_volume", {}).get("usd"),
            "price_change_24h_pct": md.get("price_change_percentage_24h"),
            "price_change_7d_pct": md.get("price_change_percentage_7d"),
            "price_change_30d_pct": md.get("price_change_percentage_30d"),
            "ath": md.get("ath", {}).get("usd"),
            "ath_change_pct": md.get("ath_change_percentage", {}).get("usd"),
            "date": date.today().isoformat(),
        }

    def _save_to_db(self, ticker: str, data: dict) -> None:
        """Save price to resolved_series, creating feature if needed."""
        tk = ticker.lower()
        fname = f"{tk}_usd_full"
        today = date.today()

        with self.engine.begin() as conn:
            feat = conn.execute(
                text("SELECT id FROM feature_registry WHERE name = :n"),
                {"n": fname},
            ).fetchone()

            if not feat:
                conn.execute(text(
                    "INSERT INTO feature_registry "
                    "(name, family, description, transformation, normalization, "
                    "missing_data_policy, model_eligible, eligible_from_date) "
                    "VALUES (:name, 'crypto', :desc, 'RAW', 'ZSCORE', "
                    "'FORWARD_FILL', TRUE, :efd) "
                    "ON CONFLICT (name) DO NOTHING"
                ), {
                    "name": fname,
                    "desc": f"CoinGecko {ticker.upper()}/USD daily close",
                    "efd": date(2020, 1, 1),
                })
                feat = conn.execute(
                    text("SELECT id FROM feature_registry WHERE name = :n"),
                    {"n": fname},
                ).fetchone()

            if feat:
                conn.execute(text(
                    "INSERT INTO resolved_series "
                    "(feature_id, obs_date, release_date, vintage_date, value, source_priority_used) "
                    "VALUES (:fid, :d, :d, :d, :v, 1) "
                    "ON CONFLICT (feature_id, obs_date, vintage_date) "
                    "DO UPDATE SET value = EXCLUDED.value"
                ), {"fid": feat[0], "d": today, "v": data["price"]})

    def pull_history(self, ticker: str, days: int = 90) -> int:
        """Pull historical daily prices for a single coin.

        Returns number of rows saved.
        """
        cg_id = CRYPTO_MAP.get(ticker.upper())
        if not cg_id:
            return 0

        url = f"{self.base_url}/coins/{cg_id}/market_chart"
        params = {"vs_currency": "usd", "days": days, "interval": "daily"}
        resp = self._session.get(url, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        prices = data.get("prices", [])
        if not prices:
            return 0

        tk = ticker.lower()
        fname = f"{tk}_usd_full"
        saved = 0

        with self.engine.begin() as conn:
            feat = conn.execute(
                text("SELECT id FROM feature_registry WHERE name = :n"),
                {"n": fname},
            ).fetchone()

            if not feat:
                conn.execute(text(
                    "INSERT INTO feature_registry "
                    "(name, family, description, transformation, normalization, "
                    "missing_data_policy, model_eligible, eligible_from_date) "
                    "VALUES (:name, 'crypto', :desc, 'RAW', 'ZSCORE', "
                    "'FORWARD_FILL', TRUE, :efd) "
                    "ON CONFLICT (name) DO NOTHING"
                ), {
                    "name": fname,
                    "desc": f"CoinGecko {ticker.upper()}/USD daily close",
                    "efd": date(2020, 1, 1),
                })
                feat = conn.execute(
                    text("SELECT id FROM feature_registry WHERE name = :n"),
                    {"n": fname},
                ).fetchone()

            if feat:
                for ts_ms, price in prices:
                    d = datetime.utcfromtimestamp(ts_ms / 1000).date()
                    conn.execute(text(
                        "INSERT INTO resolved_series "
                        "(feature_id, obs_date, release_date, vintage_date, value, source_priority_used) "
                        "VALUES (:fid, :d, :d, :d, :v, 1) "
                        "ON CONFLICT (feature_id, obs_date, vintage_date) "
                        "DO UPDATE SET value = EXCLUDED.value"
                    ), {"fid": feat[0], "d": d, "v": float(price)})
                    saved += 1

        log.info("CoinGecko history {t}: {n} days saved", t=ticker, n=saved)
        return saved
