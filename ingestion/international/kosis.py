"""
GRID Korea Statistical Information Service (KOSIS) ingestion module.

Pulls South Korean export, industrial production, and CPI data from the
KOSIS OpenAPI. Korea monthly exports are the earliest major economy trade
data release (week 1 of following month).
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

# KOSIS series: (stat_id, item_id) -> feature name
KOSIS_SERIES: dict[str, str] = {
    "TDN006_000_TDN6_2017": "korea_exports_total",
    "TDN006_000_TDN6_2017_semi": "korea_semi_exports",
    "TDN006_000_TDN6_2017_auto": "korea_auto_exports",
    "KOS_IIP001": "korea_iip_yoy",
    "KOSIS_CPI": "korea_cpi_yoy",
}

_KOSIS_BASE_URL = "https://kosis.kr/openapi/statisticsData.do"
_RATE_LIMIT_DELAY: float = 2.0


class KOSISPuller:
    """Pulls Korean economic data from the KOSIS OpenAPI."""

    def __init__(self, db_engine: Engine, api_key: str = "") -> None:
        self.engine = db_engine
        self.api_key = api_key
        if not self.api_key:
            log.warning("KOSISPuller: no KOSIS_API_KEY set — pulls will be skipped")
        self.source_id = self._resolve_source_id()
        log.info("KOSISPuller initialised — source_id={sid}", sid=self.source_id)

    def _resolve_source_id(self) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "KOSIS"},
            ).fetchone()
        if row is None:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text(
                        "INSERT INTO source_catalog "
                        "(name, base_url, license_type, update_frequency, "
                        "has_vintage_data, revision_policy, data_quality, priority, model_eligible) "
                        "VALUES (:name, :url, 'FREE', 'MONTHLY', FALSE, 'RARE', 'HIGH', 18, TRUE) "
                        "RETURNING id"
                    ),
                    {"name": "KOSIS", "url": _KOSIS_BASE_URL},
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
    def _fetch_kosis_data(self, stat_id: str, start_period: str, end_period: str) -> list[dict]:
        """Fetch data from KOSIS OpenAPI."""
        params = {
            "method": "getData",
            "apiKey": self.api_key,
            "format": "json",
            "orgId": "101",
            "tblId": stat_id,
            "startPrdDe": start_period,
            "endPrdDe": end_period,
        }
        resp = requests.get(_KOSIS_BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        return data.get("data", data.get("row", []))

    @staticmethod
    def _get_release_date(obs_date: date) -> date:
        """Korea exports release on the 1st business day of the following month."""
        if obs_date.month == 12:
            release = date(obs_date.year + 1, 1, 1)
        else:
            release = date(obs_date.year, obs_date.month + 1, 1)
        # Adjust for weekends
        while release.weekday() >= 5:
            release += timedelta(days=1)
        return release

    def pull_series(
        self,
        stat_id: str,
        start_period: str = "199001",
        end_period: str | None = None,
    ) -> dict[str, Any]:
        """Pull a single KOSIS series."""
        feature_name = KOSIS_SERIES.get(stat_id, stat_id)
        if end_period is None:
            end_period = datetime.now().strftime("%Y%m")

        log.info("Pulling KOSIS {fn}", fn=feature_name)
        result: dict[str, Any] = {
            "series_id": feature_name,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        if not self.api_key:
            log.warning("KOSIS {fn}: skipped — no API key configured", fn=feature_name)
            result["status"] = "SKIPPED"
            result["errors"].append("No KOSIS_API_KEY configured")
            return result

        try:
            data = self._fetch_kosis_data(stat_id, start_period, end_period)

            if not data:
                result["status"] = "PARTIAL"
                return result

            inserted = 0
            with self.engine.begin() as conn:
                for record in data:
                    try:
                        # KOSIS period format: YYYYMM or YYYYQN
                        prd = str(record.get("PRD_DE", record.get("prdDe", "")))
                        val_str = str(record.get("DT", record.get("dt", "")))

                        if not prd or not val_str:
                            continue

                        if len(prd) == 6:
                            obs_dt = date(int(prd[:4]), int(prd[4:6]), 1)
                        elif len(prd) == 4:
                            obs_dt = date(int(prd), 1, 1)
                        else:
                            continue

                        value = float(val_str.replace(",", ""))

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
                    except (ValueError, TypeError) as row_exc:
                        log.debug("Skipping KOSIS row: {err}", err=str(row_exc))
                        continue

            result["rows_inserted"] = inserted
            log.info("KOSIS {fn}: inserted {n} rows", fn=feature_name, n=inserted)

        except Exception as exc:
            log.error("KOSIS pull failed for {sid}: {err}", sid=stat_id, err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        time.sleep(_RATE_LIMIT_DELAY)
        return result

    def pull_all(self, start_date: str | date = "1990-01-01") -> dict[str, Any]:
        """Pull all KOSIS series."""
        log.info("Starting KOSIS bulk pull")
        start_period = str(start_date)[:4] + str(start_date)[5:7] if len(str(start_date)) >= 7 else "199001"
        results = [self.pull_series(sid, start_period) for sid in KOSIS_SERIES]

        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "KOSIS bulk pull complete — {ok}/{total} succeeded, {rows} rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return {
            "source": "KOSIS",
            "total_rows": total_rows,
            "succeeded": succeeded,
            "total": len(results),
        }
