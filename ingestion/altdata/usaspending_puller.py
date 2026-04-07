"""
GRID USASpending.gov federal spending ingestion module.

Pulls federal agency spending data from the USASpending.gov API v2
via POST to the spending endpoint.

Data source: https://api.usaspending.gov/api/v2/spending/
No API key required.

Series stored:
- usaspending.{agency_slug}_obligations: Total obligations by agency (USD)
"""

from __future__ import annotations

import time
from datetime import date
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

_API_URL = "https://api.usaspending.gov/api/v2/spending/"
_SERIES_PREFIX = "usaspending"
_REQUEST_TIMEOUT: int = 30


class USASpendingPuller(BasePuller):
    """Pulls federal spending obligations from USASpending.gov."""

    SOURCE_NAME: str = "USASPENDING_GOV"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": _API_URL,
        "cost_tier": "FREE",
        "latency_class": "MONTHLY",
        "pit_available": False,
        "revision_behavior": "FREQUENT",
        "trust_score": "HIGH",
        "priority_rank": 30,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("USASpendingPuller initialised -- source_id={sid}", sid=self.source_id)

    @retry_on_failure(
        max_attempts=3, backoff=2.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _fetch_spending(self, fy: str) -> list[dict[str, Any]]:
        """POST to spending endpoint for a fiscal year."""
        payload = {"type": "agency", "filters": {"fy": fy}}
        headers = {"Content-Type": "application/json"}
        resp = requests.post(_API_URL, json=payload, headers=headers, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json().get("results", [])

    def pull(self, fiscal_year: str = "2026") -> dict[str, Any]:
        """Pull agency spending data for a fiscal year."""
        try:
            results = self._fetch_spending(fiscal_year)
        except Exception as exc:
            log.error("USASpending pull failed: {e}", e=str(exc))
            return {"status": "FAILED", "rows_inserted": 0, "error": str(exc)}

        if not results:
            return {"status": "SUCCESS", "rows_inserted": 0}

        obs_date = date.today()
        total = 0
        with self.engine.begin() as conn:
            for item in results:
                name = item.get("name", "")
                amount = item.get("total_obligations") or item.get("amount")
                if amount is None or not name:
                    continue
                slug = name.lower().replace(" ", "_").replace(".", "")[:30]
                sid = f"{_SERIES_PREFIX}.{slug}_obligations"
                try:
                    fv = float(amount)
                except (ValueError, TypeError):
                    continue
                if not self._row_exists(sid, obs_date, conn, dedup_hours=168):
                    self._insert_raw(conn=conn, series_id=sid, obs_date=obs_date,
                                     value=fv, raw_payload={"agency": name, "fy": fiscal_year})
                    total += 1

        log.info("USASpending: {n} rows inserted", n=total)
        return {"status": "SUCCESS", "rows_inserted": total}
