"""
GRID Harvard Atlas Economic Complexity Index (ECI) ingestion module.

Pulls annual ECI scores for major economies from the Harvard Growth Lab
Atlas of Economic Complexity. Includes cross-sectional dispersion and
3-year change derivations.
"""

from __future__ import annotations

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

# Countries to track (ISO 3-letter codes)
ECI_COUNTRIES: list[str] = [
    "USA", "CHN", "DEU", "JPN", "GBR", "FRA", "KOR", "IND", "BRA", "RUS", "CAN", "AUS", "MEX",
]

# Country name to ISO mapping for various data sources
_COUNTRY_NAME_MAP: dict[str, str] = {
    "united states": "USA", "china": "CHN", "germany": "DEU",
    "japan": "JPN", "united kingdom": "GBR", "france": "FRA",
    "south korea": "KOR", "korea, rep.": "KOR", "india": "IND",
    "brazil": "BRA", "russia": "RUS", "russian federation": "RUS",
    "canada": "CAN", "australia": "AUS", "mexico": "MEX",
}

_RATE_LIMIT_DELAY: float = 2.0


class AtlasECIPuller:
    """Pulls Economic Complexity Index scores from the Harvard Atlas."""

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.source_id = self._resolve_source_id()
        log.info("AtlasECIPuller initialised — source_id={sid}", sid=self.source_id)

    def _resolve_source_id(self) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "Atlas_ECI"},
            ).fetchone()
        if row is None:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text(
                        "INSERT INTO source_catalog "
                        "(name, base_url, license_type, update_frequency, "
                        "has_vintage_data, revision_policy, data_quality, priority, model_eligible) "
                        "VALUES (:name, :url, 'FREE', 'ANNUAL', FALSE, 'NEVER', 'HIGH', 26, TRUE) "
                        "RETURNING id"
                    ),
                    {"name": "Atlas_ECI", "url": "https://atlas.cid.harvard.edu"},
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
    def download_eci_data(self) -> pd.DataFrame | None:
        """Download ECI data from Harvard Atlas or OEC API.

        Tries multiple sources in order:
        1. Atlas API rankings endpoint
        2. OEC API country ECI endpoint
        3. Harvard Dataverse pre-built CSV
        """
        # Try Atlas rankings page data
        try:
            url = "https://atlas.cid.harvard.edu/rankings/country"
            resp = requests.get(url, timeout=30, headers={"Accept": "application/json"})
            if resp.status_code == 200:
                data = resp.json()
                if data:
                    return pd.DataFrame(data)
        except Exception as exc:
            log.warning("Atlas CID ECI fetch failed: {e}", e=exc)

        # Try OEC API
        try:
            all_data = []
            for iso in ECI_COUNTRIES:
                url = f"https://oec.world/api/stats/country/{iso.lower()}/eci"
                resp = requests.get(url, timeout=30)
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, list):
                        for d in data:
                            d["country"] = iso
                        all_data.extend(data)
                time.sleep(0.5)
            if all_data:
                return pd.DataFrame(all_data)
        except Exception as exc:
            log.warning("OEC ECI fetch failed: {e}", e=exc)

        # Harvard Dataverse fallback
        try:
            url = (
                "https://dataverse.harvard.edu/api/access/datafile/:persistentId"
                "?persistentId=doi:10.7910/DVN/XTEIN9"
            )
            resp = requests.get(url, timeout=60)
            if resp.status_code == 200:
                from io import StringIO
                return pd.read_csv(StringIO(resp.text))
        except Exception as exc:
            log.warning("Harvard Dataverse ECI fetch failed: {e}", e=exc)

        log.warning("Could not download ECI data from any source")
        return None

    def pull_all(self, start_year: int = 1964) -> dict[str, Any]:
        """Pull ECI scores for all tracked countries."""
        log.info("Starting Atlas ECI pull from {sy}", sy=start_year)
        result: dict[str, Any] = {
            "source": "Atlas_ECI",
            "total_rows": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            df = self.download_eci_data()
            if df is None or df.empty:
                result["status"] = "PARTIAL"
                result["errors"].append("No ECI data available")
                return result

            inserted = 0
            # Store per-country ECI scores and compute cross-sectional metrics
            annual_scores: dict[int, dict[str, float]] = {}

            with self.engine.begin() as conn:
                # Process data based on DataFrame structure
                for _, row in df.iterrows():
                    try:
                        # Identify country
                        country = None
                        for col in ["country", "Country", "country_code", "iso3"]:
                            if col in df.columns:
                                raw = str(row[col]).strip().lower()
                                country = _COUNTRY_NAME_MAP.get(raw, raw.upper())
                                break
                        if country not in ECI_COUNTRIES:
                            continue

                        # Identify year and ECI value
                        year_val = None
                        eci_val = None
                        for col in ["year", "Year", "time"]:
                            if col in df.columns:
                                year_val = int(row[col])
                                break
                        for col in ["eci", "ECI", "eci_value", "value"]:
                            if col in df.columns:
                                eci_val = float(row[col])
                                break

                        if year_val is None or eci_val is None or year_val < start_year:
                            continue

                        feature_name = f"eci_{country.lower()}"
                        obs_dt = date(year_val, 1, 1)

                        if not self._row_exists(feature_name, obs_dt, conn):
                            conn.execute(
                                text(
                                    "INSERT INTO raw_series "
                                    "(series_id, source_id, obs_date, value, pull_status) "
                                    "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                                ),
                                {"sid": feature_name, "src": self.source_id, "od": obs_dt, "val": eci_val},
                            )
                            inserted += 1

                        # Track for dispersion calculation
                        if year_val not in annual_scores:
                            annual_scores[year_val] = {}
                        annual_scores[year_val][country] = eci_val

                    except (ValueError, TypeError, KeyError) as row_exc:
                        log.debug("Skipping ECI row: {err}", err=str(row_exc))

                # Compute cross-sectional dispersion
                for year, scores in sorted(annual_scores.items()):
                    if len(scores) >= 3:
                        dispersion = float(np.std(list(scores.values())))
                        obs_dt = date(year, 1, 1)
                        if not self._row_exists("eci_global_dispersion", obs_dt, conn):
                            conn.execute(
                                text(
                                    "INSERT INTO raw_series "
                                    "(series_id, source_id, obs_date, value, pull_status) "
                                    "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                                ),
                                {
                                    "sid": "eci_global_dispersion",
                                    "src": self.source_id,
                                    "od": obs_dt,
                                    "val": dispersion,
                                },
                            )
                            inserted += 1

                # Compute 3-year ECI change per country
                for country in ECI_COUNTRIES:
                    years_available = sorted(
                        [y for y, s in annual_scores.items() if country in s]
                    )
                    for i, y in enumerate(years_available):
                        target_year = y - 3
                        if target_year in annual_scores and country in annual_scores[target_year]:
                            change = annual_scores[y][country] - annual_scores[target_year][country]
                            feature = f"eci_{country.lower()}_3y_change"
                            obs_dt = date(y, 1, 1)
                            if not self._row_exists(feature, obs_dt, conn):
                                conn.execute(
                                    text(
                                        "INSERT INTO raw_series "
                                        "(series_id, source_id, obs_date, value, pull_status) "
                                        "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                                    ),
                                    {"sid": feature, "src": self.source_id, "od": obs_dt, "val": change},
                                )
                                inserted += 1

            result["total_rows"] = inserted
            log.info("Atlas ECI: inserted {n} rows", n=inserted)

        except Exception as exc:
            log.error("Atlas ECI pull failed: {err}", err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result
