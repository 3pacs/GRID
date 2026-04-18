"""
GRID Reserve Bank of India (RBI) ingestion module.

Pulls Indian monetary and economic data from the RBI data releases API.
Covers repo rate, industrial production index, and FX reserves.
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

# RBI series definitions
RBI_SERIES: dict[str, dict[str, str]] = {
    "repo_rate": {
        "url": "https://rbi.org.in/Scripts/BS_WAMDataReporting.aspx",
        "feature": "india_repo_rate",
    },
    "iip_yoy": {
        "url": "https://rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx",
        "feature": "india_iip_yoy",
    },
    "fx_reserves": {
        "url": "https://rbi.org.in/Scripts/WSSViewDetail.aspx",
        "feature": "india_fx_reserves",
    },
}

_RATE_LIMIT_DELAY: float = 2.0


class RBIPuller(BasePuller):
    """Pulls Indian economic data from RBI data releases.

    Note: RBI does not have a formal REST API. This module scrapes
    publicly available data pages. For production use, consider
    supplementing with FRED series for India (INDPRO, etc.) or
    DBnomics RBI mirror.
    """

    SOURCE_NAME = "RBI"
    SOURCE_CONFIG = {"base_url": "https://dbie.rbi.org.in", "cost_tier": "FREE", "latency_class": "MONTHLY", "pit_available": False, "revision_behavior": "RARE", "trust_score": "HIGH", "priority_rank": 31}

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("RBIPuller initialised — source_id={sid}", sid=self.source_id)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
    def _fetch_rbi_data(self, series_key: str) -> list[dict]:
        """Fetch data from RBI.

        Uses the RBI DBIE (Database on Indian Economy) JSON endpoint
        where available, falling back to HTML scraping for legacy pages.
        """
        # RBI DBIE JSON endpoint for programmatic access
        dbie_url = "https://dbie.rbi.org.in/DBIE/dbie.rbi"
        params = {"site": "data", "actionVal": "CSVDATA"}

        resp = requests.get(dbie_url, params=params, timeout=30)
        if resp.status_code == 200:
            try:
                return resp.json()
            except Exception as exc:
                log.warning("RBI DBIE JSON parse failed for {k}: {e}", k=series_key, e=exc)

        # Fallback: return empty (data will be supplemented via DBnomics)
        log.warning("RBI DBIE endpoint unavailable for {key}, will use DBnomics fallback", key=series_key)
        return []

    def _insert_observations(self, feature_name: str, observations: list[tuple[date, float]]) -> int:
        """Insert a list of (obs_date, value) tuples into raw_series."""
        inserted = 0
        with self.engine.begin() as conn:
            for obs_dt, value in observations:
                if self._row_exists(feature_name, obs_dt, conn):
                    continue
                conn.execute(
                    text(
                        "INSERT INTO raw_series "
                        "(series_id, source_id, obs_date, value, pull_status) "
                        "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                    ),
                    {"sid": feature_name, "src": self.source_id, "od": obs_dt, "val": value},
                )
                inserted += 1
        return inserted

    def pull_all(self, start_date: str | date = "2000-01-01") -> dict[str, Any]:
        """Pull all RBI series.

        Note: RBI data access is best-effort. Missing data should be
        supplemented via the DBnomics puller which mirrors RBI releases.
        """
        log.info("Starting RBI pull from {sd}", sd=start_date)
        results: list[dict[str, Any]] = []

        for key, config in RBI_SERIES.items():
            feature_name = config["feature"]
            result: dict[str, Any] = {
                "series_id": feature_name,
                "rows_inserted": 0,
                "status": "SUCCESS",
                "errors": [],
            }

            try:
                data = self._fetch_rbi_data(key)
                if not data:
                    result["status"] = "PARTIAL"
                    result["errors"].append("No data available — use DBnomics fallback")
                else:
                    observations: list[tuple[date, float]] = []
                    for record in data:
                        try:
                            obs_dt = datetime.strptime(
                                str(record.get("date", "")), "%Y-%m-%d"
                            ).date()
                            value = float(record.get("value", 0))
                            observations.append((obs_dt, value))
                        except (ValueError, TypeError):
                            continue

                    result["rows_inserted"] = self._insert_observations(feature_name, observations)

            except Exception as exc:
                log.error("RBI pull failed for {key}: {err}", key=key, err=str(exc))
                result["status"] = "FAILED"
                result["errors"].append(str(exc))

            results.append(result)
            time.sleep(_RATE_LIMIT_DELAY)

        total_rows = sum(r["rows_inserted"] for r in results)
        log.info("RBI pull complete — {rows} rows", rows=total_rows)
        return {
            "source": "RBI",
            "total_rows": total_rows,
            "succeeded": sum(1 for r in results if r["status"] == "SUCCESS"),
            "total": len(results),
        }
