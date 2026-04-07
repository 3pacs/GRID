"""
GDELT DOC API puller -- financial news sentiment via raw HTTP.

Free API, no key. Pulls finance/market/economy articles with tone scores.

Series stored: gdelt_news.{date}.{hash6} -- tone score per article,
               raw_payload has title, url, source, date, language
"""
from __future__ import annotations

import hashlib
from datetime import date, datetime
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

_GDELT_DOC_URL: str = (
    "https://api.gdeltproject.org/api/v2/doc/doc"
    "?query=finance+market+economy"
    "&mode=artlist&format=json&maxrecords=50&timespan=1d"
)

_SERIES_PREFIX: str = "gdelt_news"
_REQUEST_TIMEOUT: int = 30
_RATE_LIMIT_DELAY: float = 0.5  # 2 req/sec


class GdeltDocPuller(BasePuller):
    """Pulls financial news from GDELT DOC 2.0 artlist endpoint.

    Each article becomes one raw_series row:
      series_id = gdelt_news.{YYYY-MM-DD}.{MD5(url)[:6]}
      value     = GDELT tone score
      raw_payload = {title, url, source, date, language}
    """

    SOURCE_NAME: str = "GDELT_NEWS"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://api.gdeltproject.org/api/v2/doc/doc",
        "cost_tier": "FREE",
        "latency_class": "REALTIME",
        "pit_available": False,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 12,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("GdeltDocPuller init -- source_id={sid}", sid=self.source_id)

    @retry_on_failure(
        max_attempts=3, backoff=2.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _fetch_articles(self) -> list[dict[str, Any]]:
        """Fetch latest financial articles from GDELT DOC API."""
        resp = requests.get(
            _GDELT_DOC_URL,
            headers={"User-Agent": "GRID-DataPuller/1.0"},
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json().get("articles", [])

    @staticmethod
    def _parse_article(article: dict[str, Any]) -> dict[str, Any] | None:
        """Extract series_id, tone, obs_date, and payload from one article."""
        url = article.get("url", "")
        title = article.get("title", "")
        if not url or not title:
            return None

        try:
            tone = float(article.get("tone", 0))
        except (TypeError, ValueError):
            tone = 0.0

        seendate = str(article.get("seendate", ""))
        try:
            obs_date = datetime.strptime(seendate[:8], "%Y%m%d").date()
        except (ValueError, IndexError):
            obs_date = date.today()

        hash6 = hashlib.md5(url.encode()).hexdigest()[:6]
        return {
            "series_id": f"{_SERIES_PREFIX}.{obs_date.isoformat()}.{hash6}",
            "obs_date": obs_date,
            "tone": tone,
            "raw_payload": {
                "title": title[:500],
                "url": url,
                "source": article.get("domain", ""),
                "date": obs_date.isoformat(),
                "language": article.get("language", ""),
            },
        }

    def pull(self) -> dict[str, Any]:
        """Fetch latest GDELT financial articles and store them.

        Returns:
            dict with status, rows_inserted, articles_found.
        """
        try:
            raw_articles = self._fetch_articles()
        except Exception as exc:
            log.error("GDELT DOC pull failed: {e}", e=str(exc))
            return {"status": "FAILED", "rows_inserted": 0, "error": str(exc)}

        if not raw_articles:
            log.info("GDELT DOC: no articles returned")
            return {"status": "SUCCESS", "rows_inserted": 0, "articles_found": 0}

        parsed = [self._parse_article(a) for a in raw_articles]
        parsed = [p for p in parsed if p is not None]

        inserted = 0
        with self.engine.begin() as conn:
            for row in parsed:
                existing = conn.execute(
                    text(
                        "SELECT 1 FROM raw_series "
                        "WHERE series_id = :sid AND source_id = :src LIMIT 1"
                    ),
                    {"sid": row["series_id"], "src": self.source_id},
                ).fetchone()
                if existing:
                    continue
                self._insert_raw(
                    conn=conn, series_id=row["series_id"],
                    obs_date=row["obs_date"], value=row["tone"],
                    raw_payload=row["raw_payload"],
                )
                inserted += 1

        log.info("GDELT DOC: {f} articles, {i} inserted", f=len(parsed), i=inserted)
        return {"status": "SUCCESS", "rows_inserted": inserted, "articles_found": len(parsed)}
