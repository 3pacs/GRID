"""
Wikipedia Pageviews puller -- daily pageview counts for financial topics.

Free Wikimedia REST API (no key required).  Spikes in attention to
"Recession" or "Bank_run" are leading sentiment indicators.

Series stored: wikipedia.{article_slug}.views -- daily pageview count
"""
from __future__ import annotations

import time
from datetime import date, datetime, timedelta
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

_WIKI_API: str = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
    "/en.wikipedia/all-access/all-agents"
)

_ARTICLES: list[str] = [
    "Federal_Reserve", "S%26P_500", "Bitcoin", "Inflation", "Recession",
    "Stock_market_crash", "Bank_run", "Gold_standard",
    "Quantitative_easing", "Interest_rate",
]

_SERIES_PREFIX: str = "wikipedia"
_REQUEST_TIMEOUT: int = 30
_RATE_LIMIT_DELAY: float = 0.5
_LOOKBACK_DAYS: int = 7


class WikipediaPageviewsPuller(BasePuller):
    """Pulls last 7 days of daily Wikipedia pageviews for financial articles."""

    SOURCE_NAME: str = "wikipedia_pageviews"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://wikimedia.org/api/rest_v1/metrics/pageviews",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 45,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("WikipediaPageviewsPuller init -- source_id={sid}", sid=self.source_id)

    @retry_on_failure(
        max_attempts=3, backoff=2.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _fetch_article(self, article: str, start: str, end: str) -> list[dict[str, Any]]:
        """Fetch daily pageviews for one article (start/end are YYYYMMDD)."""
        url = f"{_WIKI_API}/{article}/daily/{start}/{end}"
        resp = requests.get(url, headers={"User-Agent": "GRID-DataPuller/1.0"}, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()

        results: list[dict[str, Any]] = []
        for item in resp.json().get("items", []):
            ts = item.get("timestamp", "")
            if ts and len(ts) >= 8:
                obs_date = datetime.strptime(ts[:8], "%Y%m%d").date()
                results.append({"obs_date": obs_date, "views": int(item.get("views", 0))})
        return results

    @staticmethod
    def _slug(article: str) -> str:
        """URL-encoded article title to clean slug."""
        return article.replace("%26", "and").replace("_", "_").lower()

    def pull(self) -> dict[str, Any]:
        """Fetch last 7 days of pageviews for all tracked articles.

        Returns:
            dict with status, rows_inserted, per_article counts.
        """
        end_dt = date.today() - timedelta(days=1)
        start_dt = end_dt - timedelta(days=_LOOKBACK_DAYS - 1)
        start_str, end_str = start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d")

        total_inserted = 0
        per_article: dict[str, int] = {}
        errors: list[str] = []

        for article in _ARTICLES:
            slug = self._slug(article)
            series_id = f"{_SERIES_PREFIX}.{slug}.views"

            try:
                rows = self._fetch_article(article, start_str, end_str)
            except Exception as exc:
                log.warning("Wikipedia fetch failed for {a}: {e}", a=article, e=str(exc))
                errors.append(f"{article}: {exc}")
                time.sleep(_RATE_LIMIT_DELAY)
                continue

            inserted = 0
            with self.engine.begin() as conn:
                existing = self._get_existing_dates(series_id, conn)
                for row in rows:
                    if row["obs_date"] in existing:
                        continue
                    self._insert_raw(
                        conn=conn, series_id=series_id, obs_date=row["obs_date"],
                        value=float(row["views"]),
                        raw_payload={"article": article, "source_url": _WIKI_API},
                    )
                    inserted += 1

            per_article[slug] = inserted
            total_inserted += inserted
            log.info("Wikipedia {s}: {n} rows inserted", s=series_id, n=inserted)
            time.sleep(_RATE_LIMIT_DELAY)

        status = "SUCCESS" if not errors else "PARTIAL"
        log.info(
            "WikipediaPageviews pull -- {n} rows, {a} articles, {e} errors",
            n=total_inserted, a=len(_ARTICLES), e=len(errors),
        )
        return {
            "status": status,
            "rows_inserted": total_inserted,
            "per_article": per_article,
            "errors": errors if errors else None,
        }
