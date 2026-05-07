"""
GRID EU KLEMS industry productivity ingestion module.

Pulls total factor productivity and labor productivity data from the
EU KLEMS database. Covers EU, US, and Japan, 1970-present.
"""

from __future__ import annotations

import os
from datetime import date
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from ingestion.base import BasePuller
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


class EUKLEMSPuller(BasePuller):
    """Pulls productivity data from the EU KLEMS database."""

    SOURCE_NAME = "EU_KLEMS"
    SOURCE_CONFIG = {"base_url": "https://euklems-intanprod-llee.luiss.it", "cost_tier": "FREE", "latency_class": "MONTHLY", "pit_available": False, "revision_behavior": "RARE", "trust_score": "HIGH", "priority_rank": 36}

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        os.makedirs(_EUKLEMS_DATA_DIR, exist_ok=True)
        log.info("EUKLEMSPuller initialised — source_id={sid}", sid=self.source_id)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
    def _download_euklems_data(self) -> pd.DataFrame | None:
        """Download EU KLEMS dataset.

        EU KLEMS data is distributed via their website. This method
        attempts to download the analytical database Excel file.
        """
        # EU KLEMS provides data via download portal
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
