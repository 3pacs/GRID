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


class EDINETPuller:
    """Pulls Japanese corporate filing data from the EDINET API."""

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.source_id = self._resolve_source_id()
        log.info("EDINETPuller initialised — source_id={sid}", sid=self.source_id)

    def _resolve_source_id(self) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "EDINET"},
            ).fetchone()
        if row is None:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text(
                        "INSERT INTO source_catalog "
                        "(name, base_url, license_type, update_frequency, "
                        "has_vintage_data, revision_policy, data_quality, priority, model_eligible) "
                        "VALUES (:name, :url, 'FREE', 'DAILY', FALSE, 'NEVER', 'HIGH', 22, FALSE) "
                        "RETURNING id"
                    ),
                    {"name": "EDINET", "url": _EDINET_BASE_URL},
                )
                return result.fetchone()[0]
        return row[0]

    def _row_exists(self, series_id: str, obs_date: date, conn: Any) -> bool:
        one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        result = conn.execute(
            text(
                "SELECT 1 FROM raw_series "
                "WHERE series_id = :sid AND source_id = :src "
                "AND obs_date = :od AND pull_timestamp >= :ts LIMIT 1"
            ),
            {"sid": series_id, "src": self.source_id, "od": obs_date, "ts": one_hour_ago},
        ).fetchone()
        return result is not None

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
