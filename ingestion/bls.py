"""
GRID BLS data ingestion module.

Pulls economic time series from the Bureau of Labor Statistics (BLS)
Public Data API v2 and stores raw observations in ``raw_series``.
"""

from __future__ import annotations

import json
from datetime import date
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# Default BLS series to pull
BLS_SERIES_LIST: list[str] = [
    "CES0000000001",   # Total Nonfarm Payrolls
    "LNS14000000",     # Unemployment Rate
    "CUUR0000SA0",     # CPI-U All Items
    "PRS85006092",     # Nonfarm Business Productivity
    "JTS00000000JOL",  # JOLTS Job Openings
]

# BLS API endpoint
_BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

# BLS rate limits
_MAX_SERIES_PER_QUERY = 50  # API max is 50 per request
_MAX_QUERIES_NO_KEY = 25
_MAX_QUERIES_WITH_KEY = 500

# Month name to number mapping
_MONTH_MAP: dict[str, int] = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12,
    "M01": 1, "M02": 2, "M03": 3, "M04": 4,
    "M05": 5, "M06": 6, "M07": 7, "M08": 8,
    "M09": 9, "M10": 10, "M11": 11, "M12": 12,
    "M13": 12,  # Annual average — map to December
    "Q01": 1, "Q02": 4, "Q03": 7, "Q04": 10,  # Quarterly → first month
    "S01": 1, "S02": 7,  # Semi-annual → first month
}


class BLSPuller(BasePuller):
    """Pulls time series data from the BLS Public Data API v2 into ``raw_series``.

    Attributes:
        engine: SQLAlchemy engine for database writes.
        api_key: Optional BLS API registration key.
        source_id: The ``source_catalog.id`` for the BLS source.
        query_count: Running count of API queries in this session.
    """

    SOURCE_NAME: str = "BLS"

    def __init__(self, db_engine: Engine, api_key: str | None = None) -> None:
        """Initialise the BLS puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
            api_key: Optional BLS API key. Increases rate limits from 25
                     to 500 queries per day.
        """
        self.api_key = api_key
        self.query_count: int = 0
        self._max_queries = _MAX_QUERIES_WITH_KEY if api_key else _MAX_QUERIES_NO_KEY
        super().__init__(db_engine)
        log.info(
            "BLSPuller initialised — source_id={sid}, has_key={k}, max_queries={mq}",
            sid=self.source_id,
            k=bool(api_key),
            mq=self._max_queries,
        )

    def _parse_period_to_date(self, year: str, period: str) -> date | None:
        """Convert a BLS year + period string into a Python date.

        Parameters:
            year: Four-digit year string (e.g. '2024').
            period: BLS period string (e.g. 'M01' for January).

        Returns:
            date: The first day of the observation month, or None if
                  the period is not a monthly observation.
        """
        month = _MONTH_MAP.get(period)
        if month is None:
            log.warning("Unknown BLS period: {p}", p=period)
            return None
        return date(int(year), month, 1)

    def pull_series(
        self,
        series_ids: list[str] | None = None,
        start_year: int = 2000,
        end_year: int | None = None,
    ) -> dict[str, Any]:
        """Fetch one or more BLS series and insert into raw_series.

        Parameters:
            series_ids: List of BLS series identifiers.
                        Defaults to BLS_SERIES_LIST.
            start_year: Start year for the data request.
            end_year: End year (default: current year).

        Returns:
            dict: Result with keys ``series_count``, ``rows_inserted``,
                  ``status``, ``errors``.

        Raises:
            RuntimeError: If the daily query limit is reached.
        """
        if series_ids is None:
            series_ids = BLS_SERIES_LIST
        if end_year is None:
            end_year = date.today().year

        log.info(
            "Pulling BLS series — {n} series, {sy}–{ey}",
            n=len(series_ids),
            sy=start_year,
            ey=end_year,
        )

        result: dict[str, Any] = {
            "series_count": len(series_ids),
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        # BLS limits span to 20 years per request
        year_ranges: list[tuple[int, int]] = []
        current_start = start_year
        while current_start <= end_year:
            current_end = min(current_start + 19, end_year)
            year_ranges.append((current_start, current_end))
            current_start = current_end + 1

        # Chunk series into batches of _MAX_SERIES_PER_QUERY
        series_chunks: list[list[str]] = []
        for i in range(0, len(series_ids), _MAX_SERIES_PER_QUERY):
            series_chunks.append(series_ids[i : i + _MAX_SERIES_PER_QUERY])

        total_inserted = 0

        for chunk in series_chunks:
            for yr_start, yr_end in year_ranges:
                if self.query_count >= self._max_queries:
                    msg = (
                        f"BLS daily query limit reached ({self._max_queries}). "
                        "Consider using an API key for higher limits."
                    )
                    log.warning(msg)
                    result["errors"].append(msg)
                    result["status"] = "PARTIAL"
                    result["rows_inserted"] = total_inserted
                    return result

                inserted = self._fetch_and_store(chunk, yr_start, yr_end, result)
                total_inserted += inserted

        result["rows_inserted"] = total_inserted
        log.info("BLS pull complete — {n} rows inserted", n=total_inserted)
        return result

    def _fetch_and_store(
        self,
        series_ids: list[str],
        start_year: int,
        end_year: int,
        result: dict[str, Any],
    ) -> int:
        """Make a single BLS API request and store the results.

        Parameters:
            series_ids: Series to fetch in this request.
            start_year: Start year.
            end_year: End year.
            result: Mutable result dict to append errors to.

        Returns:
            int: Number of rows inserted.
        """
        payload: dict[str, Any] = {
            "seriesid": series_ids,
            "startyear": str(start_year),
            "endyear": str(end_year),
        }
        if self.api_key:
            payload["registrationkey"] = self.api_key

        headers = {"Content-type": "application/json"}

        self.query_count += 1
        log.debug(
            "BLS API request #{n}: {ids} {sy}–{ey}",
            n=self.query_count,
            ids=series_ids,
            sy=start_year,
            ey=end_year,
        )

        try:
            resp = requests.post(_BLS_API_URL, json=payload, headers=headers, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            log.error("BLS API request failed: {err}", err=str(exc))
            result["errors"].append(str(exc))
            result["status"] = "PARTIAL"
            return 0

        if data.get("status") != "REQUEST_SUCCEEDED":
            msg = f"BLS API error: {data.get('message', 'Unknown error')}"
            log.error(msg)
            result["errors"].append(msg)
            result["status"] = "PARTIAL"
            return 0

        inserted = 0
        with self.engine.begin() as conn:
            for series_data in data.get("Results", {}).get("series", []):
                sid = series_data.get("seriesID", "UNKNOWN")
                for obs in series_data.get("data", []):
                    obs_date_val = self._parse_period_to_date(
                        obs.get("year", "2000"),
                        obs.get("period", "M01"),
                    )
                    if obs_date_val is None:
                        continue

                    try:
                        value = float(obs.get("value", "0").replace(",", ""))
                    except (ValueError, TypeError):
                        log.warning(
                            "Invalid BLS value for {sid} {yr}/{p}: {v}",
                            sid=sid,
                            yr=obs.get("year"),
                            p=obs.get("period"),
                            v=obs.get("value"),
                        )
                        continue

                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, "
                            "raw_payload, pull_status) "
                            "VALUES (:sid, :src, :od, :val, :payload, 'SUCCESS') "
                            "ON CONFLICT (series_id, source_id, obs_date, pull_timestamp) "
                            "DO NOTHING"
                        ),
                        {
                            "sid": sid,
                            "src": self.source_id,
                            "od": obs_date_val,
                            "val": value,
                            "payload": json.dumps(obs),
                        },
                    )
                    inserted += 1

        log.debug("BLS batch inserted {n} rows", n=inserted)
        return inserted


if __name__ == "__main__":
    from db import get_engine

    puller = BLSPuller(db_engine=get_engine())
    result = puller.pull_series(start_year=2020)
    print(f"BLS pull: {result['status']} — {result['rows_inserted']} rows")
