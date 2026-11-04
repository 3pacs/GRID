"""
NY Fed Global Supply Chain Pressure Index (GSCPI) puller.

Downloads the official Excel file directly from the NY Fed website.
Single best free supply chain indicator — composite of Baltic shipping,
airfreight, and PMI data from 7 countries.

Source: https://www.newyorkfed.org/research/policy/gscpi
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

_GSCPI_URL = "https://www.newyorkfed.org/medialibrary/research/interactives/gscpi/downloads/gscpi_data.xlsx"
_REQUEST_TIMEOUT = 30


def _raw_series_success_exists(
    conn: Any,
    *,
    series_id: str,
    source_id: int,
    obs_date: date,
) -> bool:
    """Return True when raw_series already has a successful row for this key."""
    row = conn.execute(
        text(
            "SELECT 1 FROM raw_series "
            "WHERE series_id = :sid AND source_id = :src "
            "AND obs_date = :od AND pull_status = 'SUCCESS' "
            "LIMIT 1"
        ),
        {"sid": series_id, "src": source_id, "od": obs_date},
    ).fetchone()
    return row is not None


_GSCPI_DATE_FORMAT = "%d-%b-%Y"


def _parse_gscpi_date(value: Any) -> Any:
    """Parse one GSCPI date cell; anything unparseable becomes NaT."""
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return pd.Timestamp(value)
    if isinstance(value, str):
        return pd.to_datetime(value.strip(), format=_GSCPI_DATE_FORMAT, errors="coerce")
    return pd.NaT


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

            # The sheet stores dates as "31-Jan-1998" text below a banner
            # block. An explicit format parses them without pandas'
            # per-element "Could not infer format" UserWarning (20 journal
            # lines/day on grid-hermes 2026-11-03); banner/header rows coerce
            # to NaT and are dropped below, as before. Real Excel date cells
            # (an older workbook) are kept as-is.
            df[date_col] = df[date_col].map(_parse_gscpi_date)
            df = df.dropna(subset=[date_col, val_col])

            inserted = 0
            with self.engine.begin() as conn:
                for _, row in df.iterrows():
                    obs_date = row[date_col].date()
                    value = float(row[val_col])
                    series_id = "NYFED:gscpi"

                    if _raw_series_success_exists(
                        conn,
                        series_id=series_id,
                        source_id=self.source_id,
                        obs_date=obs_date,
                    ):
                        continue

                    insert_result = conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                        ),
                        {"sid": series_id, "src": self.source_id, "od": obs_date, "val": value},
                    )
                    rowcount = getattr(insert_result, "rowcount", 1)
                    if rowcount and rowcount > 0:
                        inserted += 1

            result["rows_inserted"] = inserted
            log.info("NY Fed GSCPI: {n} monthly observations inserted", n=inserted)

        except Exception as exc:
            log.error("GSCPI pull failed: {e}", e=str(exc))
            result["status"] = "FAILED"
            result["error"] = str(exc)

        return [result]
