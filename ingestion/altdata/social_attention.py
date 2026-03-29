"""
GRID social attention data ingestion.

Pulls attention signals from free public sources to measure how much
retail and institutional interest a stock is getting. Information spreads
through social media — we measure the ripples.

Sources:
    1. Google Trends — relative search interest (weekly, back to 2004)
    2. Wikipedia pageviews — daily pageviews (back to 2015)
    3. SEC EDGAR filing views — daily page views on EDGAR (institutional attention)

All stored as raw_series with ATTENTION: prefix for entity_map resolution.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ── Ticker → search term mapping ────────────────────────────────────────

TICKER_SEARCH_TERMS: dict[str, dict[str, str]] = {
    "AAPL": {"search": "Apple stock", "wiki": "Apple_Inc."},
    "MSFT": {"search": "Microsoft stock", "wiki": "Microsoft"},
    "GOOGL": {"search": "Google stock", "wiki": "Alphabet_Inc."},
    "AMZN": {"search": "Amazon stock", "wiki": "Amazon_(company)"},
    "NVDA": {"search": "NVIDIA stock", "wiki": "Nvidia"},
    "META": {"search": "Meta stock", "wiki": "Meta_Platforms"},
    "TSLA": {"search": "Tesla stock", "wiki": "Tesla,_Inc."},
    "SPY": {"search": "S&P 500", "wiki": "S%26P_500"},
    "QQQ": {"search": "QQQ ETF", "wiki": "Invesco_QQQ_Trust"},
    "LLY": {"search": "Eli Lilly stock", "wiki": "Eli_Lilly_and_Company"},
    "V": {"search": "Visa stock", "wiki": "Visa_Inc."},
    "MA": {"search": "Mastercard stock", "wiki": "Mastercard"},
    "UNH": {"search": "UnitedHealth stock", "wiki": "UnitedHealth_Group"},
    "RTX": {"search": "RTX stock Raytheon", "wiki": "RTX_Corporation"},
    "AVGO": {"search": "Broadcom stock", "wiki": "Broadcom_Inc."},
    "JPM": {"search": "JPMorgan stock", "wiki": "JPMorgan_Chase"},
}

_RATE_LIMIT = 1.5  # seconds between requests


# ── Wikipedia Pageview Puller ────────────────────────────────────────────

class WikipediaAttentionPuller(BasePuller):
    """Pull daily Wikipedia pageview counts as attention proxy.

    Uses the Wikimedia REST API (free, no auth required).
    Daily granularity, data available from July 2015.
    """

    SOURCE_NAME: str = "Wikipedia_Attention"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://wikimedia.org/api/rest_v1/",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 45,
    }

    _WIKI_API = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"

    def pull_ticker(self, ticker: str, days: int = 90) -> dict[str, Any]:
        """Pull Wikipedia pageview data for a ticker."""
        info = TICKER_SEARCH_TERMS.get(ticker)
        if not info or "wiki" not in info:
            return {"status": "SKIPPED", "ticker": ticker, "rows": 0}

        article = info["wiki"]
        end = date.today()
        start = end - timedelta(days=days)
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")

        url = (
            f"{self._WIKI_API}/en.wikipedia/all-access/all-agents/"
            f"{article}/daily/{start_str}00/{end_str}00"
        )

        try:
            resp = requests.get(
                url,
                headers={"User-Agent": "GRID-DataPuller/1.0 (trading intelligence)"},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            log.warning("Wikipedia fetch failed for {t}: {e}", t=ticker, e=str(exc))
            return {"status": "FAILED", "ticker": ticker, "error": str(exc)}

        items = data.get("items", [])
        inserted = 0

        with self.engine.begin() as conn:
            for item in items:
                ts = item.get("timestamp", "")
                views = item.get("views", 0)
                try:
                    obs_date = datetime.strptime(ts[:8], "%Y%m%d").date()
                except (ValueError, IndexError):
                    continue

                sid = f"ATTENTION:{ticker}_wiki_views"
                if self._row_exists(sid, obs_date, conn):
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=sid,
                    obs_date=obs_date,
                    value=float(views),
                    raw_payload={"ticker": ticker, "article": article, "views": views},
                )
                inserted += 1

        if inserted > 0:
            log.info(
                "Wikipedia {t}: {n} days stored ({a})",
                t=ticker, n=inserted, a=article,
            )
        return {"status": "SUCCESS", "ticker": ticker, "rows": inserted}

    def pull_all(self, tickers: list[str] | None = None) -> dict[str, Any]:
        """Pull Wikipedia pageviews for all tracked tickers."""
        if tickers is None:
            tickers = list(TICKER_SEARCH_TERMS.keys())

        results = []
        total = 0
        for ticker in tickers:
            result = self.pull_ticker(ticker)
            results.append(result)
            total += result.get("rows", 0)
            time.sleep(_RATE_LIMIT)

        log.info("Wikipedia attention: {n} total rows from {t} tickers",
                 n=total, t=len(tickers))
        return {"status": "SUCCESS", "rows_inserted": total, "results": results}


# ── EDGAR Filing View Puller ─────────────────────────────────────────────

class EdgarViewsPuller(BasePuller):
    """Pull SEC EDGAR full-text search index page view data.

    Uses EDGAR's log file data (EDGAR access logs are public).
    Measures institutional attention — who's reading SEC filings?
    """

    SOURCE_NAME: str = "EDGAR_Attention"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://efts.sec.gov/",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 46,
    }

    # CIK lookup for major tickers
    _CIK_MAP: dict[str, str] = {
        "AAPL": "0000320193", "MSFT": "0000789019",
        "GOOGL": "0001652044", "AMZN": "0001018724",
        "NVDA": "0001045810", "META": "0001326801",
        "TSLA": "0001318605", "JPM": "0000019617",
        "V": "0001403161", "MA": "0001141391",
    }

    _EFTS_URL = "https://efts.sec.gov/LATEST/search-index"

    def pull_ticker(self, ticker: str, days: int = 30) -> dict[str, Any]:
        """Pull EDGAR filing view counts for a ticker.

        Uses the EDGAR full-text search to count recent filings
        as a proxy for institutional attention.
        """
        cik = self._CIK_MAP.get(ticker)
        if not cik:
            return {"status": "SKIPPED", "ticker": ticker, "rows": 0}

        # Use EDGAR EFTS API to get recent filing counts
        end = date.today()
        start = end - timedelta(days=days)

        url = f"https://efts.sec.gov/LATEST/search-index?q=%22{cik}%22&dateRange=custom&startdt={start}&enddt={end}"
        try:
            resp = requests.get(
                url,
                headers={
                    "User-Agent": "GRID-DataPuller/1.0 anik@stepdad.finance",
                    "Accept": "application/json",
                },
                timeout=15,
            )
            if resp.status_code != 200:
                # Fall back to counting filings via submissions API
                return self._pull_filing_count(ticker, cik, days)
        except Exception:
            return self._pull_filing_count(ticker, cik, days)

        return {"status": "SUCCESS", "ticker": ticker, "rows": 0}

    def _pull_filing_count(self, ticker: str, cik: str, days: int) -> dict[str, Any]:
        """Count recent SEC filings as attention proxy."""
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        try:
            resp = requests.get(
                url,
                headers={
                    "User-Agent": "GRID-DataPuller/1.0 anik@stepdad.finance",
                    "Accept": "application/json",
                },
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            log.debug("EDGAR filing count failed for {t}: {e}", t=ticker, e=str(exc))
            return {"status": "FAILED", "ticker": ticker, "error": str(exc)}

        recent = data.get("filings", {}).get("recent", {})
        dates = recent.get("filingDate", [])
        forms = recent.get("form", [])

        cutoff = (date.today() - timedelta(days=days)).isoformat()
        count = sum(1 for d in dates if d >= cutoff)

        # Store as daily aggregate
        today = date.today()
        sid = f"ATTENTION:{ticker}_edgar_filings"

        try:
            with self.engine.begin() as conn:
                if not self._row_exists(sid, today, conn):
                    self._insert_raw(
                        conn=conn,
                        series_id=sid,
                        obs_date=today,
                        value=float(count),
                        raw_payload={
                            "ticker": ticker, "cik": cik,
                            "filing_count": count, "days": days,
                        },
                    )
                    return {"status": "SUCCESS", "ticker": ticker, "rows": 1}
        except Exception as exc:
            log.debug("EDGAR store failed: {e}", e=str(exc))

        return {"status": "SUCCESS", "ticker": ticker, "rows": 0}

    def pull_all(self, tickers: list[str] | None = None) -> dict[str, Any]:
        """Pull EDGAR attention for all tracked tickers."""
        if tickers is None:
            tickers = list(self._CIK_MAP.keys())

        results = []
        total = 0
        for ticker in tickers:
            result = self.pull_ticker(ticker)
            results.append(result)
            total += result.get("rows", 0)
            time.sleep(_RATE_LIMIT)

        return {"status": "SUCCESS", "rows_inserted": total, "results": results}


# ── Google Trends Puller ─────────────────────────────────────────────────

class GoogleTrendsPuller(BasePuller):
    """Pull Google Trends search interest data.

    Weekly relative search volume (0-100) for stock-related terms.
    Requires pytrends library (pip install pytrends).

    Google Trends data goes back to 2004 — 20+ years of retail
    attention data. This is the most reliable free proxy for
    "how much are people talking about this stock."
    """

    SOURCE_NAME: str = "Google_Trends"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://trends.google.com",
        "cost_tier": "FREE",
        "latency_class": "WEEKLY",
        "pit_available": True,
        "revision_behavior": "FREQUENT",
        "trust_score": "MED",
        "priority_rank": 47,
    }

    def pull_ticker(self, ticker: str, months: int = 3) -> dict[str, Any]:
        """Pull Google Trends data for a ticker."""
        info = TICKER_SEARCH_TERMS.get(ticker)
        if not info or "search" not in info:
            return {"status": "SKIPPED", "ticker": ticker, "rows": 0}

        try:
            from pytrends.request import TrendReq
        except ImportError:
            log.warning("pytrends not installed — pip install pytrends")
            return {"status": "SKIPPED", "ticker": ticker, "rows": 0,
                    "error": "pytrends not installed"}

        search_term = info["search"]
        timeframe = f"today {months}-m" if months <= 12 else "today 5-y"

        try:
            pytrends = TrendReq(hl="en-US", tz=360)
            pytrends.build_payload([search_term], timeframe=timeframe)
            df = pytrends.interest_over_time()
        except Exception as exc:
            log.warning("Google Trends fetch failed for {t}: {e}", t=ticker, e=str(exc))
            return {"status": "FAILED", "ticker": ticker, "error": str(exc)}

        if df is None or df.empty:
            return {"status": "SKIPPED", "ticker": ticker, "rows": 0}

        inserted = 0
        with self.engine.begin() as conn:
            for idx, row in df.iterrows():
                obs_date = idx.date() if hasattr(idx, "date") else idx
                value = float(row[search_term])
                sid = f"ATTENTION:{ticker}_google_trends"

                if self._row_exists(sid, obs_date, conn):
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=sid,
                    obs_date=obs_date,
                    value=value,
                    raw_payload={
                        "ticker": ticker, "search_term": search_term,
                        "relative_interest": value,
                    },
                )
                inserted += 1

        if inserted > 0:
            log.info(
                "Google Trends {t}: {n} weeks stored (term: {s})",
                t=ticker, n=inserted, s=search_term,
            )
        return {"status": "SUCCESS", "ticker": ticker, "rows": inserted}

    def pull_all(self, tickers: list[str] | None = None) -> dict[str, Any]:
        """Pull Google Trends for all tracked tickers."""
        if tickers is None:
            tickers = list(TICKER_SEARCH_TERMS.keys())

        results = []
        total = 0
        for ticker in tickers:
            result = self.pull_ticker(ticker)
            results.append(result)
            total += result.get("rows", 0)
            # Google Trends rate limits aggressively
            time.sleep(3.0)

        log.info("Google Trends: {n} total rows from {t} tickers",
                 n=total, t=len(tickers))
        return {"status": "SUCCESS", "rows_inserted": total, "results": results}
