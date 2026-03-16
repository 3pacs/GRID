"""
GRID OECD SDMX API ingestion module.

Pulls Composite Leading Indicators (CLI) and Main Economic Indicators (MEI)
from the OECD SDMX-JSON REST API. Includes 3-month CLI slope derivation.
"""

from __future__ import annotations

import json
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from tenacity import retry, stop_after_attempt, wait_exponential

# CLI country codes
OECD_CLI_COUNTRIES: list[str] = [
    "USA", "DEU", "JPN", "GBR", "FRA", "ITA", "CAN", "CHN", "KOR", "G-7", "OECD",
]

# MEI series: SDMX key -> feature name
OECD_MEI_SERIES: dict[str, str] = {
    "MEI.PRINTO01.USA.ST.M": "us_indpro_oecd",
    "MEI.LCEAMN01.USA.ST.M": "us_unit_labor_cost",
    "MEI.CPALTT01.USA.GP.M": "us_cpi_oecd",
    "MEI.LRHUTTTT.USA.ST.M": "us_unemployment_oecd",
    "MEI.PRINTO01.CHN.ST.M": "china_indpro_oecd",
    "MEI.PRINTO01.DEU.ST.M": "germany_indpro_oecd",
}

_OECD_BASE_URL = "https://stats.oecd.org/SDMX-JSON/data"
_RATE_LIMIT_DELAY: float = 5.0


class OECDPuller:
    """Pulls OECD CLI and MEI series from the SDMX-JSON API."""

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.source_id = self._resolve_source_id()
        log.info("OECDPuller initialised — source_id={sid}", sid=self.source_id)

    def _resolve_source_id(self) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "OECD_SDMX"},
            ).fetchone()
        if row is None:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text(
                        "INSERT INTO source_catalog "
                        "(name, base_url, license_type, update_frequency, "
                        "has_vintage_data, revision_policy, data_quality, priority, model_eligible) "
                        "VALUES (:name, :url, 'FREE', 'MONTHLY', TRUE, 'RARE', 'HIGH', 11, TRUE) "
                        "RETURNING id"
                    ),
                    {"name": "OECD_SDMX", "url": _OECD_BASE_URL},
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
    def _fetch_sdmx(self, url: str) -> dict:
        resp = requests.get(url, headers={"Accept": "application/json"}, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _parse_oecd_observations(self, data: dict) -> list[tuple[date, float]]:
        """Parse observations from OECD SDMX-JSON response."""
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
                    time_values = [v.get("id", v.get("name", "")) for v in dim.get("values", [])]
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
            log.warning("Failed to parse OECD observations: {err}", err=str(exc))
        return observations

    @staticmethod
    def _parse_period(period_str: str) -> date | None:
        try:
            if len(period_str) == 7 and "-Q" in period_str:
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

    def pull_cli(
        self,
        country_code: str,
        start_date: str | date = "1970-01-01",
        end_date: str | date | None = None,
    ) -> dict[str, Any]:
        """Pull OECD CLI for a specific country and derive 3-month slope."""
        feature_name = f"oecd_cli_{country_code.lower().replace('-', '')}"
        log.info("Pulling OECD CLI for {cc}", cc=country_code)

        result: dict[str, Any] = {
            "series_id": feature_name,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            start_str = str(start_date)[:7] if len(str(start_date)) > 7 else str(start_date)
            url = f"{_OECD_BASE_URL}/MEI_CLI/LOLITONOSM.{country_code}.M/"
            if start_str:
                url += f"?startTime={start_str}"
            if end_date:
                url += f"&endTime={str(end_date)[:7]}"

            data = self._fetch_sdmx(url)
            observations = self._parse_oecd_observations(data)

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

            # Derive 3-month slope
            if len(observations) >= 3:
                slope_inserted = self._compute_cli_slope(feature_name, observations)
                result["rows_inserted"] += slope_inserted

            log.info("OECD CLI {cc}: inserted {n} rows", cc=country_code, n=result["rows_inserted"])

        except Exception as exc:
            log.error("OECD CLI pull failed for {cc}: {err}", cc=country_code, err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        time.sleep(_RATE_LIMIT_DELAY)
        return result

    def _compute_cli_slope(self, base_feature: str, observations: list[tuple[date, float]]) -> int:
        """Compute 3-month slope of CLI and insert as derived series."""
        slope_feature = f"{base_feature}_slope"
        sorted_obs = sorted(observations, key=lambda x: x[0])
        values = [v for _, v in sorted_obs]
        dates = [d for d, _ in sorted_obs]

        inserted = 0
        with self.engine.begin() as conn:
            for i in range(2, len(values)):
                slope = (values[i] - values[i - 2]) / 2.0
                obs_dt = dates[i]
                if self._row_exists(slope_feature, obs_dt, conn):
                    continue
                conn.execute(
                    text(
                        "INSERT INTO raw_series "
                        "(series_id, source_id, obs_date, value, pull_status) "
                        "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                    ),
                    {"sid": slope_feature, "src": self.source_id, "od": obs_dt, "val": slope},
                )
                inserted += 1
        return inserted

    def pull_mei(
        self,
        series_key: str,
        start_date: str | date = "1970-01-01",
        end_date: str | date | None = None,
    ) -> dict[str, Any]:
        """Pull a single OECD MEI series."""
        feature_name = OECD_MEI_SERIES.get(series_key, series_key)
        log.info("Pulling OECD MEI {sk} ({fn})", sk=series_key, fn=feature_name)

        result: dict[str, Any] = {
            "series_id": feature_name,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            # Convert dotted key to SDMX URL path
            url = f"{_OECD_BASE_URL}/{series_key}/"
            start_str = str(start_date)[:7]
            url += f"?startTime={start_str}"
            if end_date:
                url += f"&endTime={str(end_date)[:7]}"

            data = self._fetch_sdmx(url)
            observations = self._parse_oecd_observations(data)

            if not observations:
                result["status"] = "PARTIAL"
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
            log.info("OECD MEI {fn}: inserted {n} rows", fn=feature_name, n=inserted)

        except Exception as exc:
            log.error("OECD MEI pull failed for {sk}: {err}", sk=series_key, err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        time.sleep(_RATE_LIMIT_DELAY)
        return result

    def pull_all(self, start_date: str | date = "1970-01-01") -> dict[str, Any]:
        """Pull all OECD CLI countries and MEI series."""
        log.info("Starting OECD bulk pull from {sd}", sd=start_date)
        results: list[dict[str, Any]] = []

        # Pull CLI for all countries
        for cc in OECD_CLI_COUNTRIES:
            res = self.pull_cli(cc, start_date)
            results.append(res)

        # Pull all MEI series
        for sk in OECD_MEI_SERIES:
            res = self.pull_mei(sk, start_date)
            results.append(res)

        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "OECD bulk pull complete — {ok}/{total} succeeded, {rows} rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return {
            "source": "OECD_SDMX",
            "total_rows": total_rows,
            "succeeded": succeeded,
            "total": len(results),
            "results": results,
        }
