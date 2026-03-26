"""
GRID Alpha Vantage News Sentiment ingestion module.

Pulls per-ticker news sentiment from the Alpha Vantage NEWS_SENTIMENT
endpoint. Each API call returns recent news articles with relevance
scores and sentiment labels per ticker.

Free tier: 25 requests/day. Rate-limited to ~12s between calls.

API docs: https://www.alphavantage.co/documentation/#news-sentiment

Series stored:
- alphavantage_news_sentiment.{TICKER}: Aggregate sentiment score
  for each ticker on each observation date, with full article list
  as raw_payload JSON.
"""

from __future__ import annotations

import os
import time
from datetime import date, datetime, timezone
from typing import Any

import requests
from dotenv import load_dotenv
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

load_dotenv()

# ---- Config ----
_API_KEY: str = os.getenv("ALPHAVANTAGE_API_KEY", "")
_BASE_URL: str = "https://www.alphavantage.co/query"

# Default tickers covering major indices, mega-caps, and crypto
DEFAULT_TICKERS: list[str] = [
    "SPY", "QQQ", "AAPL", "MSFT", "GOOGL",
    "AMZN", "NVDA", "TSLA", "META", "BTC", "ETH",
]

# Series ID prefix
_SERIES_PREFIX: str = "alphavantage_news_sentiment"

# HTTP config
_REQUEST_TIMEOUT: int = 30

# Rate limiting: 12s between calls keeps us well under 25/day on free tier
_RATE_LIMIT_DELAY: float = 12.0


class AlphaVantageSentimentPuller(BasePuller):
    """Pulls news sentiment data from Alpha Vantage NEWS_SENTIMENT endpoint.

    For each ticker, fetches recent news articles with per-ticker
    sentiment scores and relevance. Stores the aggregate (mean)
    sentiment score as the value and the full article list as
    raw_payload JSON.

    Free-tier limit: 25 requests/day. The puller enforces a 12-second
    delay between API calls to stay safely under this cap.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for this puller.
        tickers: List of tickers to pull sentiment for.
    """

    SOURCE_NAME: str = "alphavantage_news_sentiment"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://www.alphavantage.co/query",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 45,
    }

    def __init__(
        self,
        db_engine: Engine,
        tickers: list[str] | None = None,
    ) -> None:
        """Initialise the Alpha Vantage News Sentiment puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
            tickers: Optional list of tickers to pull. Defaults to DEFAULT_TICKERS.
        """
        super().__init__(db_engine)
        self.tickers = tickers or DEFAULT_TICKERS
        if not _API_KEY:
            log.warning(
                "ALPHAVANTAGE_API_KEY not set -- pulls will fail"
            )
        log.info(
            "AlphaVantageSentimentPuller initialised -- source_id={sid}, tickers={n}",
            sid=self.source_id,
            n=len(self.tickers),
        )

    def _series_id(self, ticker: str) -> str:
        """Build the full series_id for a ticker.

        Parameters:
            ticker: Ticker symbol (e.g., 'SPY').

        Returns:
            Full series_id (e.g., 'alphavantage_news_sentiment.SPY').
        """
        return f"{_SERIES_PREFIX}.{ticker}"

    # ------------------------------------------------------------------ #
    # API fetch
    # ------------------------------------------------------------------ #

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.RequestException,
        ),
    )
    def _fetch_sentiment(self, ticker: str) -> dict[str, Any]:
        """Fetch news sentiment for a single ticker from Alpha Vantage.

        Parameters:
            ticker: Ticker symbol.

        Returns:
            Parsed JSON response dict.

        Raises:
            requests.RequestException: On HTTP errors after retries.
            ValueError: If API returns an error message (e.g., rate limit).
        """
        params = {
            "function": "NEWS_SENTIMENT",
            "tickers": ticker,
            "apikey": _API_KEY,
        }
        headers = {
            "User-Agent": "GRID-DataPuller/1.0",
            "Accept": "application/json",
        }
        resp = requests.get(
            _BASE_URL, params=params, headers=headers, timeout=_REQUEST_TIMEOUT
        )
        resp.raise_for_status()
        data = resp.json()

        # Alpha Vantage returns error messages in the JSON body
        if "Error Message" in data:
            raise ValueError(f"Alpha Vantage API error: {data['Error Message']}")
        if "Note" in data:
            raise ValueError(f"Alpha Vantage rate limit: {data['Note']}")
        if "Information" in data:
            raise ValueError(f"Alpha Vantage info: {data['Information']}")

        return data

    # ------------------------------------------------------------------ #
    # Parse response
    # ------------------------------------------------------------------ #

    def _parse_response(
        self, data: dict[str, Any], ticker: str
    ) -> list[dict[str, Any]]:
        """Parse Alpha Vantage NEWS_SENTIMENT response into row dicts.

        Groups articles by date, computes the mean ticker-specific
        sentiment score for each date, and stores the article list
        as raw_payload.

        Parameters:
            data: Raw JSON response from Alpha Vantage.
            ticker: The ticker we queried for.

        Returns:
            List of dicts with keys: obs_date, sentiment_score, raw_payload.
        """
        feed = data.get("feed") or []
        if not feed:
            log.info("No articles returned for {t}", t=ticker)
            return []

        # Group articles by obs_date
        by_date: dict[date, list[dict[str, Any]]] = {}

        for article in feed:
            time_published = article.get("time_published", "")
            if not time_published:
                continue

            # Parse time_published: format is "YYYYMMDDTHHMMSS"
            try:
                obs_dt = datetime.strptime(time_published[:15], "%Y%m%dT%H%M%S")
                obs_dt = obs_dt.replace(tzinfo=timezone.utc)
                obs_date = obs_dt.date()
            except (ValueError, IndexError) as exc:
                log.warning(
                    "Bad time_published {tp} for {t}: {e}",
                    tp=time_published,
                    t=ticker,
                    e=str(exc),
                )
                continue

            # Find the ticker-specific sentiment from the article
            ticker_sentiment = None
            for ts in article.get("ticker_sentiment", []):
                if ts.get("ticker", "").upper() == ticker.upper():
                    ticker_sentiment = ts
                    break

            if ticker_sentiment is None:
                continue

            try:
                score = float(ticker_sentiment.get("ticker_sentiment_score", 0))
            except (ValueError, TypeError):
                continue

            article_summary = {
                "title": article.get("title"),
                "url": article.get("url"),
                "source": article.get("source"),
                "overall_sentiment_score": article.get("overall_sentiment_score"),
                "overall_sentiment_label": article.get("overall_sentiment_label"),
                "ticker_sentiment_score": ticker_sentiment.get("ticker_sentiment_score"),
                "ticker_sentiment_label": ticker_sentiment.get("ticker_sentiment_label"),
                "ticker_relevance_score": ticker_sentiment.get("relevance_score"),
                "time_published": time_published,
            }

            if obs_date not in by_date:
                by_date[obs_date] = []
            by_date[obs_date].append({"score": score, "article": article_summary})

        # Build output rows: one per date with mean sentiment
        rows: list[dict[str, Any]] = []
        for obs_date, articles in sorted(by_date.items()):
            scores = [a["score"] for a in articles]
            mean_score = sum(scores) / len(scores) if scores else 0.0

            rows.append({
                "obs_date": obs_date,
                "sentiment_score": round(mean_score, 6),
                "raw_payload": {
                    "ticker": ticker,
                    "article_count": len(articles),
                    "articles": [a["article"] for a in articles],
                    "source_url": f"{_BASE_URL}?function=NEWS_SENTIMENT&tickers={ticker}",
                },
            })

        return rows

    # ------------------------------------------------------------------ #
    # Pull single ticker
    # ------------------------------------------------------------------ #

    def pull_ticker(self, ticker: str) -> dict[str, Any]:
        """Pull news sentiment for a single ticker and store results.

        Parameters:
            ticker: Ticker symbol (e.g., 'SPY').

        Returns:
            dict with status, rows_inserted count.
        """
        sid = self._series_id(ticker)

        try:
            raw_data = self._fetch_sentiment(ticker)
        except Exception as exc:
            log.error(
                "Alpha Vantage sentiment pull failed for {t}: {e}",
                t=ticker,
                e=str(exc),
            )
            return {
                "status": "FAILED",
                "ticker": ticker,
                "rows_inserted": 0,
                "error": str(exc),
            }

        parsed = self._parse_response(raw_data, ticker)
        if not parsed:
            log.info(
                "Alpha Vantage sentiment: no data for {t}", t=ticker
            )
            return {"status": "SUCCESS", "ticker": ticker, "rows_inserted": 0}

        inserted = 0

        with self.engine.begin() as conn:
            existing = self._get_existing_dates(sid, conn)

            for row in parsed:
                if row["obs_date"] in existing:
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=sid,
                    obs_date=row["obs_date"],
                    value=row["sentiment_score"],
                    raw_payload=row["raw_payload"],
                )
                inserted += 1

        log.info(
            "Alpha Vantage {sid}: {n} rows inserted", sid=sid, n=inserted
        )

        return {
            "status": "SUCCESS",
            "ticker": ticker,
            "rows_inserted": inserted,
        }

    # ------------------------------------------------------------------ #
    # Pull all tickers
    # ------------------------------------------------------------------ #

    def pull_all(self) -> list[dict[str, Any]]:
        """Pull news sentiment for all configured tickers.

        Iterates through the ticker list with rate limiting (12s
        between calls) to stay under the Alpha Vantage free-tier
        limit of 25 requests/day.

        Returns:
            List of result dicts (one per ticker).
        """
        results: list[dict[str, Any]] = []

        for i, ticker in enumerate(self.tickers):
            if i > 0:
                log.debug(
                    "Rate limiting: sleeping {d}s before next call",
                    d=_RATE_LIMIT_DELAY,
                )
                time.sleep(_RATE_LIMIT_DELAY)

            result = self.pull_ticker(ticker)
            results.append(result)

        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        total_rows = sum(r["rows_inserted"] for r in results)
        log.info(
            "Alpha Vantage sentiment pull_all -- {ok}/{total} tickers, {rows} rows",
            ok=succeeded,
            total=len(results),
            rows=total_rows,
        )
        return results
