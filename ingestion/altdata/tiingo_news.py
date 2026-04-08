"""
Tiingo News puller — bulk financial news with tickers, tags, and sources.

Tiingo provides 20M+ articles from 2015+, ticker-tagged, with descriptions.
Uses the /tiingo/news endpoint with pagination (1000 articles/batch).

Series stored:
  tiingo_news.{YYYY-MM-DD}.{hash} — one row per article
  value = 0.0 (no built-in sentiment; run FinBERT separately)
  raw_payload = {title, description, source, tickers, tags, url}

Backfill strategy:
  Pull day-by-day from start_date to today. Each day typically has
  100-2000+ articles. At 1000/batch with pagination, a full year
  is ~365 API calls.
"""

from __future__ import annotations

import hashlib
import json
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

_TIINGO_NEWS_URL: str = "https://api.tiingo.com/tiingo/news"
_BATCH_SIZE: int = 1000  # max per API call
_RATE_DELAY: float = 0.3  # seconds between API calls
_DB_BATCH: int = 500  # rows per DB commit


class TiingoNewsPuller(BasePuller):
    """Pull financial news articles from Tiingo."""

    SOURCE_NAME: str = "tiingo_news"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://api.tiingo.com/tiingo/news",
        "cost_tier": "PAID",
        "latency_class": "REALTIME",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 5,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        from config import settings
        self.api_key = settings.TIINGO_API_KEY
        self._headers = {
            "Content-Type": "application/json",
            "Authorization": f"Token {self.api_key}",
        }
        log.info(
            "TiingoNewsPuller initialised — source_id={sid}",
            sid=self.source_id,
        )

    @retry_on_failure(max_attempts=3, backoff=2.0)
    def _fetch_page(
        self,
        start_date: str,
        end_date: str,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Fetch one page of news articles from Tiingo.

        Parameters:
            start_date: YYYY-MM-DD start.
            end_date: YYYY-MM-DD end.
            offset: Pagination offset.

        Returns:
            List of article dicts.
        """
        params = {
            "startDate": start_date,
            "endDate": end_date,
            "limit": _BATCH_SIZE,
            "offset": offset,
            "sortBy": "date",
        }
        resp = requests.get(
            _TIINGO_NEWS_URL,
            headers=self._headers,
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        time.sleep(_RATE_DELAY)
        return resp.json()

    def pull_day(self, target_date: date) -> dict[str, Any]:
        """Pull all news articles for a single day.

        Paginates through all articles for the day, inserting in batches.

        Parameters:
            target_date: The date to pull.

        Returns:
            Summary dict with counts.
        """
        ds = target_date.isoformat()
        de = (target_date + timedelta(days=1)).isoformat()

        total_fetched = 0
        total_inserted = 0
        offset = 0

        while True:
            try:
                articles = self._fetch_page(ds, de, offset=offset)
            except Exception as exc:
                log.warning(
                    "Tiingo news fetch failed for {d} offset={o}: {e}",
                    d=ds, o=offset, e=str(exc),
                )
                break

            if not articles:
                break

            total_fetched += len(articles)

            with self.engine.begin() as conn:
                for article in articles:
                    try:
                        inserted = self._store_article(conn, article, target_date)
                        if inserted:
                            total_inserted += 1
                    except Exception:
                        pass  # skip individual article errors

            if len(articles) < _BATCH_SIZE:
                break  # no more pages

            offset += _BATCH_SIZE

        return {
            "date": ds,
            "fetched": total_fetched,
            "inserted": total_inserted,
        }

    def _store_article(
        self,
        conn: Any,
        article: dict[str, Any],
        fallback_date: date,
    ) -> bool:
        """Store a single article as a raw_series row.

        Parameters:
            conn: Active DB connection.
            article: Tiingo article dict.
            fallback_date: Date to use if article date is unparseable.

        Returns:
            True if inserted, False if duplicate.
        """
        title = article.get("title", "")
        article_id = str(article.get("id", ""))
        description = article.get("description", "")[:500]
        source = article.get("source", "")
        tickers = article.get("tickers", [])
        tags = article.get("tags", [])
        url = article.get("url", "")

        # Parse date
        pub_str = article.get("publishedDate", "")
        try:
            obs_date = datetime.fromisoformat(
                pub_str.replace("Z", "+00:00")
            ).date()
        except (ValueError, AttributeError):
            obs_date = fallback_date

        # Unique hash from article ID or title+date
        if article_id:
            article_hash = hashlib.md5(article_id.encode()).hexdigest()[:12]
        else:
            article_hash = hashlib.md5(
                f"{title}:{obs_date}".encode()
            ).hexdigest()[:12]

        series_id = f"tiingo_news.{article_hash}"

        conn.execute(
            text(
                "INSERT INTO raw_series "
                "(series_id, source_id, obs_date, value, raw_payload, pull_status) "
                "VALUES (:sid, :src, :od, :val, :payload, 'SUCCESS') "
                "ON CONFLICT DO NOTHING"
            ),
            {
                "sid": series_id,
                "src": self.source_id,
                "od": obs_date,
                "val": 0.0,
                "payload": json.dumps({
                    "title": title[:300],
                    "description": description,
                    "source": source,
                    "tickers": tickers[:20],
                    "tags": tags[:10],
                    "url": url[:300],
                    "tiingo_id": article_id,
                    "published": pub_str,
                }),
            },
        )
        return True

    def pull_range(
        self,
        start_date: date,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        """Pull news for a date range, day by day.

        Parameters:
            start_date: First date to pull.
            end_date: Last date (inclusive). Defaults to today.

        Returns:
            Summary dict with total counts.
        """
        if end_date is None:
            end_date = date.today()

        total_days = (end_date - start_date).days + 1
        log.info(
            "Tiingo news: pulling {n} days ({s} to {e})",
            n=total_days, s=start_date, e=end_date,
        )

        total_fetched = 0
        total_inserted = 0
        errors = 0

        current = start_date
        day_num = 0
        while current <= end_date:
            day_num += 1
            try:
                result = self.pull_day(current)
                total_fetched += result["fetched"]
                total_inserted += result["inserted"]

                if day_num % 30 == 0:
                    log.info(
                        "Tiingo news: day {d}/{t} ({dt}) — {f:,d} fetched, {i:,d} inserted total",
                        d=day_num, t=total_days, dt=current,
                        f=total_fetched, i=total_inserted,
                    )

            except Exception as exc:
                log.warning(
                    "Tiingo news day {d} failed: {e}",
                    d=current, e=str(exc),
                )
                errors += 1

            current += timedelta(days=1)

        summary = {
            "status": "SUCCESS",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "days_pulled": day_num,
            "total_fetched": total_fetched,
            "total_inserted": total_inserted,
            "errors": errors,
        }
        log.info("Tiingo news pull complete: {s}", s=summary)
        return summary

    def pull_recent(self, days_back: int = 7) -> dict[str, Any]:
        """Pull recent news (default: last 7 days).

        Parameters:
            days_back: Number of days to look back.

        Returns:
            Summary dict.
        """
        start = date.today() - timedelta(days=days_back)
        return self.pull_range(start)
