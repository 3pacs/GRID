"""
GRID NASA VIIRS Nighttime Lights ingestion module.

Pulls nighttime light intensity data from NOAA VIIRS monthly composites.
Uses pre-aggregated country/region sum-of-lights data. Includes
VIIRS-macro divergence computation for data quality flagging.
"""

from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from tenacity import retry, stop_after_attempt, wait_exponential

# Geographic bounding boxes for regional aggregation
VIIRS_BBOXES: dict[str, dict[str, float]] = {
    "us": {"lat_min": 24.5, "lat_max": 49.5, "lon_min": -124.8, "lon_max": -66.9},
    "china": {"lat_min": 18.2, "lat_max": 53.6, "lon_min": 73.5, "lon_max": 134.8},
    "india": {"lat_min": 8.1, "lat_max": 37.1, "lon_min": 68.1, "lon_max": 97.4},
    "eu": {"lat_min": 35.0, "lat_max": 71.0, "lon_min": -10.0, "lon_max": 40.0},
}

# Land area for normalization (sq km, approximate)
_LAND_AREA: dict[str, float] = {
    "us": 9_833_520.0,
    "china": 9_596_961.0,
    "india": 3_287_263.0,
    "eu": 4_233_000.0,
}

_VIIRS_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "viirs")
_RATE_LIMIT_DELAY: float = 2.0


class VIIRSPuller:
    """Pulls nighttime lights data from NOAA VIIRS composites."""

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.source_id = self._resolve_source_id()
        os.makedirs(_VIIRS_DATA_DIR, exist_ok=True)
        log.info("VIIRSPuller initialised — source_id={sid}", sid=self.source_id)

    def _resolve_source_id(self) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "VIIRS"},
            ).fetchone()
        if row is None:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text(
                        "INSERT INTO source_catalog "
                        "(name, base_url, license_type, update_frequency, "
                        "has_vintage_data, revision_policy, data_quality, priority, model_eligible) "
                        "VALUES (:name, :url, 'FREE', 'MONTHLY', FALSE, 'NEVER', 'HIGH', 28, TRUE) "
                        "RETURNING id"
                    ),
                    {"name": "VIIRS", "url": "https://eogdata.mines.edu/nighttime_light"},
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
    def download_monthly_vcmslcfg(self, year: int, month: int) -> pd.DataFrame | None:
        """Download VIIRS monthly composite summary data.

        Uses the pre-aggregated country-level sum-of-lights CSV from NOAA.
        """
        # Try the sum-of-lights summary file
        url = f"https://eogdata.mines.edu/files/sum_of_lights/{year}/sum_of_lights_{year}.csv"
        local_path = os.path.join(_VIIRS_DATA_DIR, f"sum_of_lights_{year}.csv")

        if os.path.exists(local_path):
            return pd.read_csv(local_path)

        try:
            resp = requests.get(url, timeout=60)
            if resp.status_code == 200:
                with open(local_path, "w") as f:
                    f.write(resp.text)
                return pd.read_csv(local_path)
        except Exception as exc:
            log.warning("VIIRS download failed for {y}: {err}", y=year, err=str(exc))

        # Try annual VNL V2 summary
        alt_url = f"https://eogdata.mines.edu/nighttime_light/annual/v22/{year}/"
        log.warning(
            "VIIRS sum-of-lights not available for {y}. "
            "Check {u} for manual download.",
            y=year, u=alt_url,
        )
        return None

    def aggregate_lights(self, region_key: str, year: int) -> float | None:
        """Load regional sum-of-lights and normalize by land area."""
        df = self.download_monthly_vcmslcfg(year, 1)
        if df is None:
            return None

        # Try to find region data in the summary CSV
        # Column names vary; look for the region or country name
        bbox = VIIRS_BBOXES.get(region_key, {})
        if not bbox:
            return None

        # For pre-aggregated data, look for matching country/region column
        for col in df.columns:
            if region_key.lower() in str(col).lower():
                try:
                    raw_value = float(df[col].iloc[0])
                    area = _LAND_AREA.get(region_key, 1.0)
                    return raw_value / area
                except (ValueError, IndexError):
                    continue

        return None

    def compute_viirs_divergence(self, year: int) -> dict[str, Any]:
        """Compare China VIIRS lights vs reported industrial production.

        High divergence = potential data quality flag on official statistics.
        """
        result: dict[str, Any] = {"rows_inserted": 0, "status": "SUCCESS", "errors": []}

        try:
            with self.engine.begin() as conn:
                # Get China VIIRS lights
                viirs_row = conn.execute(
                    text(
                        "SELECT value FROM raw_series "
                        "WHERE series_id = 'viirs_china_lights' AND source_id = :src "
                        "AND obs_date = :od AND pull_status = 'SUCCESS'"
                    ),
                    {"src": self.source_id, "od": date(year, 1, 1)},
                ).fetchone()

                # Get China industrial production (from AKShare or OECD)
                indpro_row = conn.execute(
                    text(
                        "SELECT value FROM raw_series "
                        "WHERE series_id IN ('china_indpro_yoy', 'china_indpro_oecd') "
                        "AND obs_date >= :start AND obs_date < :end "
                        "AND pull_status = 'SUCCESS' "
                        "ORDER BY obs_date DESC LIMIT 1"
                    ),
                    {"start": date(year, 1, 1), "end": date(year + 1, 1, 1)},
                ).fetchone()

                if viirs_row and indpro_row and indpro_row[0] != 0:
                    divergence = viirs_row[0] / indpro_row[0]
                    obs_dt = date(year, 1, 1)
                    if not self._row_exists("china_viirs_macro_divergence", obs_dt, conn):
                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, pull_status) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                            ),
                            {
                                "sid": "china_viirs_macro_divergence",
                                "src": self.source_id,
                                "od": obs_dt,
                                "val": divergence,
                            },
                        )
                        result["rows_inserted"] = 1

        except Exception as exc:
            log.error("VIIRS divergence computation failed: {err}", err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def pull_all(self, start_year: int = 2012) -> dict[str, Any]:
        """Process all available VIIRS years for all regions."""
        log.info("Starting VIIRS pull from {sy}", sy=start_year)
        result: dict[str, Any] = {
            "source": "VIIRS",
            "total_rows": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        inserted = 0
        with self.engine.begin() as conn:
            for year in range(start_year, date.today().year + 1):
                for region_key in VIIRS_BBOXES:
                    intensity = self.aggregate_lights(region_key, year)
                    if intensity is None:
                        continue

                    feature = f"viirs_{region_key}_lights"
                    obs_dt = date(year, 1, 1)
                    if not self._row_exists(feature, obs_dt, conn):
                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, pull_status) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                            ),
                            {"sid": feature, "src": self.source_id, "od": obs_dt, "val": intensity},
                        )
                        inserted += 1

                # Compute divergence for each year
                div_result = self.compute_viirs_divergence(year)
                inserted += div_result.get("rows_inserted", 0)

                time.sleep(_RATE_LIMIT_DELAY)

        result["total_rows"] = inserted
        log.info("VIIRS: inserted {n} rows", n=inserted)
        return result
