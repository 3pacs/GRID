"""
NY Fed Global Supply Chain Pressure Index (GSCPI) puller.

Downloads the official Excel file directly from the NY Fed website.
Single best free supply chain indicator — composite of Baltic shipping,
airfreight, and PMI data from 7 countries.

Source: https://www.newyorkfed.org/research/policy/gscpi
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

_GSCPI_URL = "https://www.newyorkfed.org/medialibrary/research/interactives/gscpi/downloads/gscpi_data.xlsx"
_REQUEST_TIMEOUT = 30


class NYFedGSCPIPuller(BasePuller):
    """Pulls the NY Fed Global Supply Chain Pressure Index."""

    SOURCE_NAME: str = "NYFED_GSCPI"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://www.newyorkfed.org/research/policy/gscpi",
        "cost_tier": "FREE",
        "latency_class": "MONTHLY",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 8,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)

    @retry_on_failure(max_attempts=2)
    def pull_all(self, **kwargs) -> list[dict[str, Any]]:
        """Download GSCPI Excel and insert into raw_series."""
        result: dict[str, Any] = {"rows_inserted": 0, "status": "SUCCESS"}

        try:
            log.info("Downloading NY Fed GSCPI from {u}", u=_GSCPI_URL)
            resp = requests.get(
                _GSCPI_URL,
                timeout=_REQUEST_TIMEOUT,
                headers={"User-Agent": "GRID/4.0 (research; stepdadfinance@gmail.com)"},
            )
            resp.raise_for_status()

            from io import BytesIO
            content_type = resp.headers.get("content-type", "")
            excel_engine = "xlrd" if "ms-excel" in content_type else "openpyxl"

            # Data is in "GSCPI Monthly Data" sheet, cols 0-1, starting ~row 5
            df = pd.read_excel(
                BytesIO(resp.content),
                engine=excel_engine,
                sheet_name="GSCPI Monthly Data",
                header=None,
                usecols=[0, 1],
                names=["Date", "GSCPI"],
            )
            date_col = "Date"
            val_col = "GSCPI"

            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
            df = df.dropna(subset=[date_col, val_col])

            inserted = 0
            with self.engine.begin() as conn:
                for _, row in df.iterrows():
                    obs_date = row[date_col].date()
                    value = float(row[val_col])

                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS') "
                            "ON CONFLICT (series_id, source_id, obs_date, pull_timestamp) DO NOTHING"
                        ),
                        {"sid": "NYFED:gscpi", "src": self.source_id, "od": obs_date, "val": value},
                    )
                    inserted += 1

            result["rows_inserted"] = inserted
            log.info("NY Fed GSCPI: {n} monthly observations inserted", n=inserted)

        except Exception as exc:
            log.error("GSCPI pull failed: {e}", e=str(exc))
            result["status"] = "FAILED"
            result["error"] = str(exc)

        return [result]
