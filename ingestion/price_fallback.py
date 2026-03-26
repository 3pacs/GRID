"""
Backup price data puller — runs when yfinance is unreliable.

Uses free-tier APIs as fallback sources:
1. Alpha Vantage (ALPHAVANTAGE_API_KEY env var, free: 25 req/day)
2. Twelve Data (TWELVEDATA_API_KEY env var, free: 800 req/day)
3. Stooq.com (no API key needed, CSV download)

Falls through sources in priority order until one succeeds.
"""

from __future__ import annotations

import os
import time
from datetime import date, datetime
from typing import Any

import requests
from loguru import logger as log


class PriceFallbackPuller:
    """Fetch price data from multiple free backup sources."""

    def __init__(self, db_engine: Any = None) -> None:
        self.engine = db_engine
        self.av_key = os.getenv("ALPHAVANTAGE_API_KEY", "")
        self.td_key = os.getenv("TWELVEDATA_API_KEY", "")
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "GRID/4.0"})

    def pull_price(self, ticker: str) -> dict | None:
        """Try multiple sources for a single ticker price.

        Returns dict with keys: ticker, price, date, source
        or None if all sources fail.
        """
        for source_fn in [self._stooq, self._alpha_vantage, self._twelve_data]:
            try:
                result = source_fn(ticker)
                if result and result.get("price"):
                    return result
            except Exception as exc:
                log.debug("Fallback {s} failed for {t}: {e}",
                          s=source_fn.__name__, t=ticker, e=str(exc))
        return None

    def pull_many(self, tickers: list[str]) -> list[dict]:
        """Pull prices for multiple tickers with fallback chain."""
        results = []
        for tk in tickers:
            result = self.pull_price(tk)
            if result:
                results.append(result)
                log.debug("Fallback price for {t}: ${p} via {s}",
                          t=tk, p=result["price"], s=result["source"])
            else:
                log.warning("All fallback sources failed for {t}", t=tk)
            time.sleep(0.5)  # Rate limiting
        return results

    def _stooq(self, ticker: str) -> dict | None:
        """Stooq.com — free, no API key, CSV download."""
        # Stooq uses .US suffix for US stocks
        stooq_ticker = f"{ticker}.US" if not ticker.endswith(".US") else ticker
        url = f"https://stooq.com/q/l/?s={stooq_ticker}&f=sd2t2ohlcv&h&e=csv"
        resp = self._session.get(url, timeout=10)
        resp.raise_for_status()
        lines = resp.text.strip().split("\n")
        if len(lines) < 2:
            return None
        parts = lines[1].split(",")
        if len(parts) < 7 or parts[0] == "N/D":
            return None
        close = float(parts[6])
        if close <= 0:
            return None
        return {
            "ticker": ticker,
            "price": close,
            "date": parts[1] if len(parts) > 1 else date.today().isoformat(),
            "source": "stooq",
        }

    def _alpha_vantage(self, ticker: str) -> dict | None:
        """Alpha Vantage — free tier, 25 requests/day."""
        if not self.av_key:
            return None
        url = "https://www.alphavantage.co/query"
        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": ticker,
            "apikey": self.av_key,
        }
        resp = self._session.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        quote = data.get("Global Quote", {})
        price = quote.get("05. price")
        if not price:
            return None
        return {
            "ticker": ticker,
            "price": float(price),
            "date": quote.get("07. latest trading day", date.today().isoformat()),
            "source": "alpha_vantage",
        }

    def _twelve_data(self, ticker: str) -> dict | None:
        """Twelve Data — free tier, 800 requests/day."""
        if not self.td_key:
            return None
        url = f"https://api.twelvedata.com/price?symbol={ticker}&apikey={self.td_key}"
        resp = self._session.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        price = data.get("price")
        if not price:
            return None
        return {
            "ticker": ticker,
            "price": float(price),
            "date": date.today().isoformat(),
            "source": "twelve_data",
        }

    def save_to_db(self, results: list[dict]) -> int:
        """Save fallback prices to resolved_series."""
        if not self.engine or not results:
            return 0
        from sqlalchemy import text
        saved = 0
        with self.engine.begin() as conn:
            for r in results:
                tk = r["ticker"].lower().replace("-", "_")
                feature_name = f"{tk}_full"
                # Find or skip feature
                feat = conn.execute(
                    text("SELECT id FROM feature_registry WHERE name = :n"),
                    {"n": feature_name},
                ).fetchone()
                if not feat:
                    continue
                conn.execute(
                    text(
                        "INSERT INTO resolved_series (feature_id, obs_date, release_date, value, source_priority_used) "
                        "VALUES (:fid, :d, :d, :v, 'price_fallback') "
                        "ON CONFLICT (feature_id, obs_date, vintage_date) DO UPDATE SET value = :v"
                    ),
                    {"fid": feat[0], "d": r["date"], "v": r["price"]},
                )
                saved += 1
        log.info("Saved {n} fallback prices to resolved_series", n=saved)
        return saved
