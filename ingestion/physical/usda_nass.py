"""
GRID USDA NASS QuickStats ingestion module.

Pulls US agricultural data (crop yields, planted acres, conditions, inventories)
from the USDA National Agricultural Statistics Service QuickStats API.
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

# NASS query definitions
NASS_QUERIES: list[dict[str, str]] = [
    {
        "commodity_desc": "CORN",
        "statisticcat_desc": "YIELD",
        "unit_desc": "BU / ACRE",
        "agg_level_desc": "NATIONAL",
        "feature": "corn_yield_forecast",
    },
    {
        "commodity_desc": "WHEAT",
        "statisticcat_desc": "AREA PLANTED",
        "unit_desc": "ACRES",
        "agg_level_desc": "NATIONAL",
        "feature": "wheat_planted_acres",
    },
    {
        "commodity_desc": "SOYBEANS",
        "statisticcat_desc": "YIELD",
        "unit_desc": "BU / ACRE",
        "agg_level_desc": "NATIONAL",
        "feature": "soybean_yield_forecast",
    },
    {
        "commodity_desc": "CORN",
        "statisticcat_desc": "CONDITION",
        "unit_desc": "PCT EXCELLENT",
        "agg_level_desc": "NATIONAL",
        "feature": "crop_progress_corn",
    },
    {
        "commodity_desc": "CATTLE",
        "statisticcat_desc": "INVENTORY",
        "agg_level_desc": "NATIONAL",
        "feature": "cattle_inventory",
    },
]

_NASS_BASE_URL = "https://quickstats.nass.usda.gov/api/api_GET/"
_RATE_LIMIT_DELAY: float = 2.0

# Month name to number mapping for NASS reference_period_desc
_MONTH_MAP: dict[str, int] = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
    "YEAR": 1, "ANNUAL": 1, "MARKETING YEAR": 1,
}


class USDAPuller:
    """Pulls agricultural data from USDA NASS QuickStats API."""

    def __init__(self, db_engine: Engine, api_key: str = "") -> None:
        self.engine = db_engine
        self.api_key = api_key
        self.source_id = self._resolve_source_id()
        log.info("USDAPuller initialised — source_id={sid}", sid=self.source_id)

    def _resolve_source_id(self) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "USDA_NASS"},
            ).fetchone()
        if row is None:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text(
                        "INSERT INTO source_catalog "
                        "(name, base_url, license_type, update_frequency, "
                        "has_vintage_data, revision_policy, data_quality, priority, model_eligible) "
                        "VALUES (:name, :url, 'FREE', 'WEEKLY', FALSE, 'RARE', 'HIGH', 32, TRUE) "
                        "RETURNING id"
                    ),
                    {"name": "USDA_NASS", "url": _NASS_BASE_URL},
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
    def _fetch_nass_data(self, params: dict[str, str]) -> list[dict]:
        """Fetch data from NASS QuickStats API."""
        query_params = {
            "key": self.api_key,
            "source_desc": "SURVEY",
            "format": "JSON",
        }
        query_params.update(params)
        resp = requests.get(_NASS_BASE_URL, params=query_params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", [])

    def _parse_reference_period(self, period_desc: str, year: int) -> date | None:
        """Map NASS reference_period_desc to obs_date.

        Examples: 'AUG' -> {year}-08-01, 'YEAR' -> {year}-01-01
        """
        period_upper = period_desc.strip().upper()
        month = _MONTH_MAP.get(period_upper)
        if month:
            return date(year, month, 1)

        # Handle week-based periods (e.g., 'WEEK #25')
        if "WEEK" in period_upper:
            try:
                week_num = int(period_upper.replace("WEEK", "").replace("#", "").strip())
                # Approximate: week number * 7 days from Jan 1
                jan1 = date(year, 1, 1)
                return jan1 + timedelta(weeks=week_num - 1)
            except ValueError:
                pass

        return None

    def pull_query(
        self,
        params_dict: dict[str, str],
        feature_name: str,
        start_year: int = 1970,
        end_year: int | None = None,
    ) -> dict[str, Any]:
        """Pull a single NASS query."""
        if end_year is None:
            end_year = date.today().year

        log.info("Pulling USDA NASS {fn}", fn=feature_name)
        result: dict[str, Any] = {
            "series_id": feature_name,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            # Build query params excluding 'feature' key
            query = {k: v for k, v in params_dict.items() if k != "feature"}
            query["year__GE"] = str(start_year)
            query["year__LE"] = str(end_year)

            data = self._fetch_nass_data(query)

            if not data:
                result["status"] = "PARTIAL"
                result["errors"].append("No data returned")
                return result

            inserted = 0
            with self.engine.begin() as conn:
                for record in data:
                    try:
                        year = int(record.get("year", 0))
                        period_desc = record.get("reference_period_desc", "YEAR")
                        value_str = record.get("Value", "").replace(",", "").strip()

                        if not value_str or value_str in ("(D)", "(NA)", "(Z)", "(S)"):
                            continue

                        obs_dt = self._parse_reference_period(period_desc, year)
                        if obs_dt is None:
                            continue

                        value = float(value_str)

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
                        log.debug("Skipping NASS row: {err}", err=str(row_exc))

            result["rows_inserted"] = inserted
            log.info("USDA NASS {fn}: inserted {n} rows", fn=feature_name, n=inserted)

        except Exception as exc:
            log.error("USDA NASS pull failed for {fn}: {err}", fn=feature_name, err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        time.sleep(_RATE_LIMIT_DELAY)
        return result

    def pull_all(self, start_year: int = 1970) -> dict[str, Any]:
        """Pull all NASS queries."""
        log.info("Starting USDA NASS bulk pull from {sy}", sy=start_year)
        results: list[dict[str, Any]] = []

        for query_def in NASS_QUERIES:
            feature = query_def["feature"]
            params = {k: v for k, v in query_def.items() if k != "feature"}
            res = self.pull_query(params, feature, start_year)
            results.append(res)

        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "USDA NASS bulk pull complete — {ok}/{total} succeeded, {rows} rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return {
            "source": "USDA_NASS",
            "total_rows": total_rows,
            "succeeded": succeeded,
            "total": len(results),
        }
