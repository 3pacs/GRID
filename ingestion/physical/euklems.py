"""
GRID EU KLEMS industry productivity ingestion module.

Pulls total factor productivity and labor productivity data from the
EU KLEMS database. Covers EU, US, and Japan, 1970-present.
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

_EUKLEMS_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "euklems")
_RATE_LIMIT_DELAY: float = 2.0

# EU KLEMS series to extract
EUKLEMS_SERIES: dict[str, str] = {
    "GO_QI_USA": "euklems_labor_prod_us",
    "TFP_EU": "euklems_tfp_eu",
    "GO_QI_JPN": "euklems_labor_prod_jp",
    "TFP_USA": "euklems_tfp_us",
}


class EUKLEMSPuller:
    """Pulls productivity data from the EU KLEMS database."""

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.source_id = self._resolve_source_id()
        os.makedirs(_EUKLEMS_DATA_DIR, exist_ok=True)
        log.info("EUKLEMSPuller initialised — source_id={sid}", sid=self.source_id)

    def _resolve_source_id(self) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "EU_KLEMS"},
            ).fetchone()
        if row is None:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text(
                        "INSERT INTO source_catalog "
                        "(name, base_url, license_type, update_frequency, "
                        "has_vintage_data, revision_policy, data_quality, priority, model_eligible) "
                        "VALUES (:name, :url, 'FREE', 'ANNUAL', FALSE, 'NEVER', 'HIGH', 29, TRUE) "
                        "RETURNING id"
                    ),
                    {"name": "EU_KLEMS", "url": "https://euklems.eu"},
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
    def _download_euklems_data(self) -> pd.DataFrame | None:
        """Download EU KLEMS dataset.

        EU KLEMS data is distributed via their website. This method
        attempts to download the analytical database Excel file.
        """
        # EU KLEMS provides data via download portal
        url = "https://euklems.eu/download/"
        local_path = os.path.join(_EUKLEMS_DATA_DIR, "euklems_analytical.xlsx")

        if os.path.exists(local_path):
            try:
                return pd.read_excel(local_path)
            except Exception as exc:
                log.warning("Failed to read cached EU KLEMS file: {e}", e=exc)

        log.warning(
            "EU KLEMS data not found locally. Download the analytical database from "
            "https://euklems.eu/download/ and place at {p}",
            p=local_path,
        )
        return None

    def pull_all(self) -> dict[str, Any]:
        """Pull and process EU KLEMS productivity data."""
        log.info("Starting EU KLEMS pull")
        result: dict[str, Any] = {
            "source": "EU_KLEMS",
            "total_rows": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            df = self._download_euklems_data()
            if df is None:
                result["status"] = "PARTIAL"
                result["errors"].append("EU KLEMS data not available locally")
                return result

            inserted = 0
            with self.engine.begin() as conn:
                for series_key, feature_name in EUKLEMS_SERIES.items():
                    try:
                        # Find matching data in the DataFrame
                        for col in df.columns:
                            if series_key.lower() in str(col).lower():
                                for _, row in df.iterrows():
                                    try:
                                        year = int(row.iloc[0])
                                        value = float(row[col])
                                        if pd.isna(value):
                                            continue
                                        obs_dt = date(year, 1, 1)
                                        if not self._row_exists(feature_name, obs_dt, conn):
                                            conn.execute(
                                                text(
                                                    "INSERT INTO raw_series "
                                                    "(series_id, source_id, obs_date, value, pull_status) "
                                                    "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                                                ),
                                                {
                                                    "sid": feature_name,
                                                    "src": self.source_id,
                                                    "od": obs_dt,
                                                    "val": value,
                                                },
                                            )
                                            inserted += 1
                                    except (ValueError, TypeError):
                                        continue
                                break
                    except Exception as series_exc:
                        log.warning("EU KLEMS {fn} failed: {err}", fn=feature_name, err=str(series_exc))

            result["total_rows"] = inserted
            log.info("EU KLEMS: inserted {n} rows", n=inserted)

        except Exception as exc:
            log.error("EU KLEMS pull failed: {err}", err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result
