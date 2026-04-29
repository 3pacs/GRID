"""
GRID Monetary Authority of Singapore (MAS) ingestion module.

Pulls Singapore SORA rate, SIBOR, FX reserves, and M2 money supply
from the MAS public data API.
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

# MAS resource_id -> feature name
MAS_SERIES: dict[str, str] = {
    "9a0bf149-308c-4bd2-832d-76c8e6cb47ed": "singapore_sora",
    "5f2b18a2-671d-4994-ba42-7a48094adb11": "singapore_sibor_3m",
    "2fe9c5af-57e5-4dd8-9ce6-c9ecff4f4a13": "singapore_fx_reserves",
    "65e62e81-7b80-4fa9-a2bb-9ce4ba3ad3e6": "singapore_m2",
}

_MAS_BASE_URL = "https://eservices.mas.gov.sg/api/action/datastore/search.json"
_RATE_LIMIT_DELAY: float = 1.0


class MASPuller(BasePuller):
    """Pulls monetary and financial data from the MAS API."""

    SOURCE_NAME = "MAS_SG"
    SOURCE_CONFIG = {"base_url": "https://eservices.mas.gov.sg/api", "cost_tier": "FREE", "latency_class": "MONTHLY", "pit_available": True, "revision_behavior": "RARE", "trust_score": "HIGH", "priority_rank": 29}

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("MASPuller initialised — source_id={sid}", sid=self.source_id)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
    def _fetch_page(self, resource_id: str, limit: int, offset: int) -> dict:
        params = {
            "resource_id": resource_id,
            "limit": str(limit),
            "offset": str(offset),
        }
        resp = requests.get(_MAS_BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def pull_resource(
        self,
        resource_id: str,
        limit: int = 1000,
        offset: int = 0,
    ) -> dict[str, Any]:
        """Pull a single MAS resource, paginating through all records."""
        feature_name = MAS_SERIES.get(resource_id, resource_id)
        log.info("Pulling MAS {fn}", fn=feature_name)

        result: dict[str, Any] = {
            "series_id": feature_name,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            total_inserted = 0
            current_offset = offset

            while True:
                data = self._fetch_page(resource_id, limit, current_offset)
                records = data.get("result", {}).get("records", [])

                if not records:
                    break

                with self.engine.begin() as conn:
                    for record in records:
                        try:
                            # MAS records have end_of_day, end_of_month, or similar date fields
                            date_str = (
                                record.get("end_of_day")
                                or record.get("end_of_month")
                                or record.get("end_of_quarter")
                                or record.get("month")
                                or ""
                            )
                            if not date_str:
                                continue

                            obs_dt = self._parse_date(date_str)
                            if obs_dt is None:
                                continue

                            # Find the first numeric value field
                            value = None
                            for key in sorted(record.keys()):
                                if key.startswith("end_of") or key in ("_id", "month", "timestamp"):
                                    continue
                                try:
                                    value = float(record[key])
                                    break
                                except (ValueError, TypeError):
                                    continue

                            if value is None:
                                continue

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
                            total_inserted += 1
                        except Exception as row_exc:
                            log.debug("Skipping MAS row: {err}", err=str(row_exc))
                            continue

                # Check if more pages exist
                total_records = data.get("result", {}).get("total", 0)
                current_offset += limit
                if current_offset >= total_records:
                    break

                time.sleep(0.5)

            result["rows_inserted"] = total_inserted
            log.info("MAS {fn}: inserted {n} rows", fn=feature_name, n=total_inserted)

        except Exception as exc:
            log.error("MAS pull failed for {rid}: {err}", rid=resource_id, err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        time.sleep(_RATE_LIMIT_DELAY)
        return result

    @staticmethod
    def _parse_date(date_str: str) -> date | None:
        """Parse various MAS date formats."""
        for fmt in ("%Y-%m-%d", "%Y-%m", "%Y/%m/%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(date_str[:10], fmt).date()
            except (ValueError, TypeError):
                continue
        return None

    def pull_all(self, start_date: str | date = "2000-01-01") -> dict[str, Any]:
        """Pull all MAS resources."""
        log.info("Starting MAS bulk pull")
        results = [self.pull_resource(rid) for rid in MAS_SERIES]

        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "MAS bulk pull complete — {ok}/{total} succeeded, {rows} rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return {
            "source": "MAS_SG",
            "total_rows": total_rows,
            "succeeded": succeeded,
            "total": len(results),
        }
