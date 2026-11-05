"""
Backup price data puller — runs when yfinance is unreliable.

Uses free-tier APIs as fallback sources:
1. Alpha Vantage (ALPHAVANTAGE_API_KEY env var, free: 25 req/day)
2. Twelve Data (TWELVEDATA_API_KEY env var, free: 800 req/day)
3. Stooq.com (no API key needed, CSV download)

Falls through sources in priority order until one succeeds.

Fetch-only: ``save_to_db`` is retired (it backdated ``resolved_series``
vintages directly, see its docstring) and refuses with
:class:`PriceFallbackRetired`.
"""

from __future__ import annotations

import os
import time
from datetime import date
from typing import Any

import requests
from loguru import logger as log


RETIRED_REASON = (
    "PriceFallbackPuller.save_to_db is retired (E1-V7 / DATA-FIX DFa): it wrote backdated "
    "resolved_series vintages directly; prices reach resolved_series only via raw_series "
    "and normalization/resolver.py"
)


class PriceFallbackRetired(RuntimeError):
    """The fallback writer is retired; nothing is stored."""


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
        """Retired: this writer may not store anything (E1-V7, DATA-FIX DFa).

        It used to insert fallback quotes straight into ``resolved_series``
        with ``release_date = obs_date``, no ``vintage_date`` and
        ``ON CONFLICT ... DO UPDATE SET value``: a backdated vintage that a
        re-run rewrote in place, attributed to a source the resolver never
        saw. ``vintage_date`` is NOT NULL with no default, so on the
        production schema every call failed and its error was swallowed by
        the scheduler. Prices reach ``resolved_series`` only through
        ``raw_series`` plus ``normalization/resolver.py``; a fallback quote
        source would need its own ``raw_series`` source and entity mapping.
        """
        raise PriceFallbackRetired(RETIRED_REASON)
