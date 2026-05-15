"""
GRID USPTO patent application search ingestion module.

Pulls patent application counts for 5 key technology keywords from
the USPTO API. Tracks innovation velocity -- rising patent filings
signal upcoming commercialization waves.

Data source: https://developer.uspto.gov/ibd-api/v1/application/publications
No API key required.

Series stored:
- uspto.{keyword_slug}_patent_count: Weekly patent application count
"""

from __future__ import annotations

import time
from datetime import date, timedelta
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, log_pull_failure, retry_on_failure

_USPTO_BASE = "https://developer.uspto.gov/ibd-api/v1/application/publications"
_SERIES_PREFIX = "uspto"
_REQUEST_TIMEOUT: int = 30

# 5 key technology domains per spec
_KEYWORDS: dict[str, str] = {
    "AI": "ai",
    "quantum computing": "quantum_computing",
    "gene therapy": "gene_therapy",
    "blockchain": "blockchain",
    "autonomous vehicles": "autonomous_vehicles",
}


class USPTOPuller(BasePuller):
    """Pulls patent application counts from USPTO API."""

    SOURCE_NAME: str = "USPTO_PV"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": _USPTO_BASE,
        "cost_tier": "FREE",
        "latency_class": "WEEKLY",
        "pit_available": False,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 35,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("USPTOPuller initialised -- source_id={sid}", sid=self.source_id)

    @retry_on_failure(
        max_attempts=3, backoff=3.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _search(self, keyword: str) -> dict[str, Any]:
        """Search USPTO patent publications by keyword."""
        params = {"searchText": keyword, "start": 0, "rows": 1}
        headers = {"Accept": "application/json", "User-Agent": "GRID-DataPuller/1.0"}
        resp = requests.get(_USPTO_BASE, params=params, headers=headers, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    def pull(self) -> dict[str, Any]:
        """Pull patent counts for all 5 tracked keywords (weekly snapshot)."""
        today = date.today()
        obs_date = today - timedelta(days=today.weekday())  # Monday of this week
        total = 0

        with self.engine.begin() as conn:
            for keyword, slug in _KEYWORDS.items():
                sid = f"{_SERIES_PREFIX}.{slug}_patent_count"
                if self._row_exists(sid, obs_date, conn, dedup_hours=168):
                    continue
                try:
                    data = self._search(keyword)
                except Exception as exc:
                    # USPTO's IBD API frequently returns 503 during their
                    # maintenance windows. Route through log_pull_failure
                    # so transient upstream issues stay WARNING and only
                    # code bugs surface as ERROR.
                    log_pull_failure("USPTO", keyword, exc)
                    continue

                num = data.get("numFound", 0)
                if isinstance(data.get("response"), dict):
                    num = data["response"].get("numFound", num)

                self._insert_raw(conn=conn, series_id=sid, obs_date=obs_date,
                                 value=float(num),
                                 raw_payload={"keyword": keyword, "slug": slug})
                total += 1
                log.info("USPTO '{kw}': {n} patents", kw=keyword, n=num)
                time.sleep(2.0)

        log.info("USPTO: {n} rows inserted", n=total)
        return {"status": "SUCCESS", "rows_inserted": total}
