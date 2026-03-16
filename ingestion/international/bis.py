"""
GRID BIS Statistics API ingestion module.

Pulls credit-to-GDP gap, cross-border banking flows, and property prices
from the Bank for International Settlements SDMX REST API.
"""

from __future__ import annotations

import json
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from tenacity import retry, stop_after_attempt, wait_exponential

# BIS series: dataset_ref -> feature name
BIS_SERIES_LIST: dict[str, str] = {
    "total_credit/Q/5J/P/N/I/B/770": "bis_credit_gdp_gap_us",
    "total_credit/Q/5J/P/N/I/B/156": "bis_credit_gdp_gap_cn",
    "total_credit/Q/5J/P/N/I/B/276": "bis_credit_gdp_gap_de",
    "lbs/Q/A/F/USD/A/5J": "bis_global_cbflow_usd",
    "pp_ss/Q/5J": "bis_property_prices_g20",
}

_BIS_BASE_URL = "https://stats.bis.org/api/v1/data"
_RATE_LIMIT_DELAY: float = 2.0


class BISPuller:
    """Pulls statistics from the Bank for International Settlements API."""

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.source_id = self._resolve_source_id()
        log.info("BISPuller initialised — source_id={sid}", sid=self.source_id)

    def _resolve_source_id(self) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "BIS"},
            ).fetchone()
        if row is None:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text(
                        "INSERT INTO source_catalog "
                        "(name, base_url, license_type, update_frequency, "
                        "has_vintage_data, revision_policy, data_quality, priority, model_eligible) "
                        "VALUES (:name, :url, 'FREE', 'QUARTERLY', TRUE, 'RARE', 'HIGH', 12, TRUE) "
                        "RETURNING id"
                    ),
                    {"name": "BIS", "url": _BIS_BASE_URL},
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
    def _fetch_bis_json(self, dataset_ref: str, start_period: str | None) -> dict:
        url = f"{_BIS_BASE_URL}/{dataset_ref}"
        params: dict[str, str] = {}
        if start_period:
            params["startPeriod"] = start_period
        headers = {"Accept": "application/json"}
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _parse_observations(self, data: dict) -> list[tuple[date, float]]:
        """Parse BIS SDMX-JSON observations."""
        observations: list[tuple[date, float]] = []
        try:
            datasets = data.get("dataSets", [])
            if not datasets:
                return observations

            structure = data.get("structure", {})
            dimensions = structure.get("dimensions", {})
            obs_dims = dimensions.get("observation", [])

            time_values = []
            for dim in obs_dims:
                if dim.get("id") in ("TIME_PERIOD", "TIME"):
                    time_values = [v.get("id", "") for v in dim.get("values", [])]
                    break

            series_data = datasets[0].get("series", {})
            for _key, series_obj in series_data.items():
                obs_map = series_obj.get("observations", {})
                for idx_str, val_list in obs_map.items():
                    idx = int(idx_str)
                    if idx < len(time_values) and val_list and val_list[0] is not None:
                        period_str = time_values[idx]
                        obs_dt = self._parse_period(period_str)
                        if obs_dt:
                            observations.append((obs_dt, float(val_list[0])))
        except (KeyError, IndexError, TypeError) as exc:
            log.warning("Failed to parse BIS observations: {err}", err=str(exc))
        return observations

    @staticmethod
    def _parse_period(period_str: str) -> date | None:
        try:
            if "-Q" in period_str:
                year, q = period_str.split("-Q")
                return date(int(year), (int(q) - 1) * 3 + 1, 1)
            elif len(period_str) == 7:
                return datetime.strptime(period_str, "%Y-%m").date()
            elif len(period_str) == 4:
                return date(int(period_str), 1, 1)
            elif len(period_str) == 10:
                return datetime.strptime(period_str, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            pass
        return None

    def pull_series(
        self,
        dataset_ref: str,
        start_date: str | date = "1970-01-01",
        end_date: str | date | None = None,
    ) -> dict[str, Any]:
        """Fetch a single BIS series and insert into raw_series."""
        feature_name = BIS_SERIES_LIST.get(dataset_ref, dataset_ref)
        log.info("Pulling BIS series {dr} ({fn})", dr=dataset_ref, fn=feature_name)

        result: dict[str, Any] = {
            "series_id": feature_name,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            start_str = str(start_date)[:7] if len(str(start_date)) > 7 else None
            data = self._fetch_bis_json(dataset_ref, start_str)
            observations = self._parse_observations(data)

            if not observations:
                result["status"] = "PARTIAL"
                result["errors"].append("No data returned")
                return result

            inserted = 0
            with self.engine.begin() as conn:
                for obs_date_val, value in observations:
                    if self._row_exists(feature_name, obs_date_val, conn):
                        continue
                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                        ),
                        {"sid": feature_name, "src": self.source_id, "od": obs_date_val, "val": value},
                    )
                    inserted += 1

            result["rows_inserted"] = inserted
            log.info("BIS {fn}: inserted {n} rows", fn=feature_name, n=inserted)

        except Exception as exc:
            log.error("BIS pull failed for {dr}: {err}", dr=dataset_ref, err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        time.sleep(_RATE_LIMIT_DELAY)
        return result

    def pull_all(self, start_date: str | date = "1970-01-01") -> dict[str, Any]:
        """Pull all BIS series."""
        log.info("Starting BIS bulk pull from {sd}", sd=start_date)
        results = [self.pull_series(dr, start_date) for dr in BIS_SERIES_LIST]

        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "BIS bulk pull complete — {ok}/{total} succeeded, {rows} rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return {
            "source": "BIS",
            "total_rows": total_rows,
            "succeeded": succeeded,
            "total": len(results),
            "results": results,
        }
