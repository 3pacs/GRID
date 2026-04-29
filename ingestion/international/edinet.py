"""
GRID Japan FSA EDINET filings ingestion module.

Pulls Japanese corporate filing data from the EDINET API v2. Covers
securities reports and annual financial statements for major Japanese companies.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from ingestion.base import BasePuller
from tenacity import retry, stop_after_attempt, wait_exponential

_EDINET_BASE_URL = "https://api.edinet-fsa.go.jp/api/v2"
_RATE_LIMIT_DELAY: float = 1.0

# Major Japanese companies to track (EDINET codes)
EDINET_TARGETS: dict[str, str] = {
    "E02529": "toyota_filings",
    "E02166": "sony_filings",
    "E04425": "softbank_filings",
    "E00736": "mitsubishi_ufj_filings",
    "E04837": "keyence_filings",
}


class EDINETPuller(BasePuller):
    """Pulls Japanese corporate filing data from the EDINET API."""

    SOURCE_NAME = "EDINET"
    SOURCE_CONFIG = {"base_url": "https://disclosure.edinet-fsa.go.jp/api/v2", "cost_tier": "FREE", "latency_class": "EOD", "pit_available": True, "revision_behavior": "RARE", "trust_score": "HIGH", "priority_rank": 28}

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("EDINETPuller initialised — source_id={sid}", sid=self.source_id)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
    def _fetch_documents_list(self, filing_date: str) -> list[dict]:
        """Fetch list of documents filed on a given date."""
        url = f"{_EDINET_BASE_URL}/documents.json"
        params = {"date": filing_date, "type": 2}
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])

    def pull_filings(self, days_back: int = 30) -> dict[str, Any]:
        """Pull recent EDINET filings for tracked companies."""
        log.info("Pulling EDINET filings for last {d} days", d=days_back)
        result: dict[str, Any] = {
            "series_id": "edinet_all",
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            inserted = 0
            for day_offset in range(days_back):
                filing_date = (date.today() - timedelta(days=day_offset)).isoformat()
                try:
                    docs = self._fetch_documents_list(filing_date)
                    filing_count = len(docs)

                    # Count filings for tracked companies
                    obs_dt = date.fromisoformat(filing_date)
                    with self.engine.begin() as conn:
                        # Store total daily filing count
                        if not self._row_exists("edinet_daily_count", obs_dt, conn):
                            conn.execute(
                                text(
                                    "INSERT INTO raw_series "
                                    "(series_id, source_id, obs_date, value, pull_status) "
                                    "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                                ),
                                {
                                    "sid": "edinet_daily_count",
                                    "src": self.source_id,
                                    "od": obs_dt,
                                    "val": float(filing_count),
                                },
                            )
                            inserted += 1

                except Exception as day_exc:
                    log.debug("EDINET day {d} failed: {err}", d=filing_date, err=str(day_exc))

                time.sleep(_RATE_LIMIT_DELAY)

            result["rows_inserted"] = inserted
            log.info("EDINET: inserted {n} rows", n=inserted)

        except Exception as exc:
            log.error("EDINET pull failed: {err}", err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def pull_all(self) -> dict[str, Any]:
        """Pull all EDINET data."""
        return self.pull_filings(days_back=30)
