"""
GDELT financial news puller — real-time global news, 15-min updates, no API key.

Uses gdeltdoc library to query the GDELT DOC 2.0 API for financial/market news.
Stores headlines + sentiment for Oracle consumption.

Source: https://www.gdeltproject.org/
API: https://api.gdeltproject.org/api/v2/doc/doc
"""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

try:
    from gdeltdoc import GdeltDoc, Filters
    _HAS_GDELT = True
except ImportError:
    _HAS_GDELT = False

_FINANCE_THEMES = [
    "ECON_STOCKMARKET",
    "ECON_INTEREST_RATE",
    "ECON_INFLATION",
    "ECON_DEBT",
    "ECON_TRADE",
    "TAX_FNCACT",
    "CRISISLEX_T03_DEAD",
]

_FINANCE_KEYWORDS = [
    "stock market", "Federal Reserve", "interest rate", "inflation",
    "earnings", "IPO", "SEC", "bond market", "crude oil", "gold price",
    "bitcoin", "cryptocurrency", "GDP", "unemployment",
]


class GdeltNewsPuller(BasePuller):
    """Pulls financial news from GDELT DOC 2.0 API."""

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
        if not _HAS_GDELT:
            raise ImportError("gdeltdoc not installed — pip install gdeltdoc")
        super().__init__(db_engine)
        self._gd = GdeltDoc()

    @retry_on_failure(max_attempts=2)
    def pull_recent(self, hours_back: int = 24, max_records: int = 250) -> dict[str, Any]:
        """Pull recent financial news articles."""
        result: dict[str, Any] = {"rows_inserted": 0, "articles_found": 0, "status": "SUCCESS"}

        try:
            f = Filters(
                keyword=" OR ".join(_FINANCE_KEYWORDS[:5]),
                start_date=(datetime.now(timezone.utc) - timedelta(hours=hours_back)).strftime("%Y-%m-%d"),
                end_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                num_records=max_records,
                country="US",
            )

            articles = self._gd.article_search(f)

            if articles is None or articles.empty:
                result["status"] = "PARTIAL"
                return result

            result["articles_found"] = len(articles)
            inserted = 0

            with self.engine.begin() as conn:
                for _, row in articles.iterrows():
                    title = str(row.get("title", ""))[:500]
                    url = str(row.get("url", ""))
                    domain = str(row.get("domain", ""))
                    pub_date = row.get("seendate", "")

                    if not title or not url:
                        continue

                    # Deduplicate by URL hash
                    url_hash = hashlib.md5(url.encode()).hexdigest()[:16]
                    series_id = f"GDELT:news:{url_hash}"

                    # Extract tone (GDELT's sentiment score, -100 to +100)
                    tone = float(row.get("tone", 0)) if "tone" in row.index else 0.0

                    try:
                        obs_date = datetime.strptime(str(pub_date)[:10], "%Y%m%d").date() if pub_date else date.today()
                    except (ValueError, TypeError):
                        obs_date = date.today()

                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS') "
                            "ON CONFLICT (series_id, source_id, obs_date, pull_timestamp) DO NOTHING"
                        ),
                        {"sid": series_id, "src": self.source_id, "od": obs_date, "val": tone},
                    )
                    inserted += 1

            result["rows_inserted"] = inserted
            log.info("GDELT news: {n} articles, {i} rows inserted", n=len(articles), i=inserted)

        except Exception as exc:
            log.error("GDELT pull failed: {e}", e=str(exc))
            result["status"] = "FAILED"
            result["error"] = str(exc)

        return result

    def pull_all(self, **kwargs) -> list[dict[str, Any]]:
        return [self.pull_recent(**kwargs)]
