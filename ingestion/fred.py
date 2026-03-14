"""
GRID FRED data ingestion module.

Pulls economic time series from the Federal Reserve Economic Data (FRED) API
and stores raw observations in the ``raw_series`` table.  Includes deduplication,
rate limiting, release-date retrieval, and full error handling.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
from fredapi import Fred
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# Default FRED series to pull
FRED_SERIES_LIST: list[str] = [
    "T10Y2Y",
    "T10Y3M",
    "DFF",
    "VIXCLS",
    "USSLIND",
    "CPIAUCSL",
    "MANEMP",
    "UNRATE",
    "HOUST",
    "DSPIC96",
    "M2SL",
    "WALCL",
    "BAMLH0A0HYM2",
    "BAMLC0A0CM",
    "TEDRATE",
    "T5YIE",
    "UMCSENT",
    "ICSA",
    "RETAILSMNSA",
    "INDPRO",
]

# Minimum delay between FRED API calls (seconds)
_RATE_LIMIT_DELAY: float = 0.25


class FREDPuller:
    """Pulls time series data from the FRED API into ``raw_series``.

    Attributes:
        fred: fredapi.Fred client instance.
        engine: SQLAlchemy engine for database writes.
        source_id: The ``source_catalog.id`` for the FRED source.
    """

    def __init__(self, api_key: str, db_engine: Engine) -> None:
        """Initialise the FRED puller.

        Parameters:
            api_key: FRED API key.
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        self.fred = Fred(api_key=api_key)
        self.engine = db_engine
        self.source_id = self._resolve_source_id()
        log.info("FREDPuller initialised — source_id={sid}", sid=self.source_id)

    def _resolve_source_id(self) -> int:
        """Look up the source_catalog id for FRED.

        Returns:
            int: The source_catalog.id for the 'FRED' row.

        Raises:
            RuntimeError: If the FRED source is not found in source_catalog.
        """
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "FRED"},
            ).fetchone()
        if row is None:
            raise RuntimeError("FRED source not found in source_catalog. Run schema.sql first.")
        return row[0]

    def _row_exists(self, series_id: str, obs_date: date, conn: Any) -> bool:
        """Check whether a duplicate row already exists within 1 hour.

        Parameters:
            series_id: FRED series identifier.
            obs_date: Observation date.
            conn: Active SQLAlchemy connection.

        Returns:
            bool: True if a matching row was inserted within the last hour.
        """
        one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        result = conn.execute(
            text(
                "SELECT 1 FROM raw_series "
                "WHERE series_id = :sid AND source_id = :src "
                "AND obs_date = :od AND pull_timestamp >= :ts "
                "LIMIT 1"
            ),
            {
                "sid": series_id,
                "src": self.source_id,
                "od": obs_date,
                "ts": one_hour_ago,
            },
        ).fetchone()
        return result is not None

    def pull_series(
        self,
        series_id: str,
        start_date: str | date,
        end_date: str | date | None = None,
    ) -> dict[str, Any]:
        """Fetch a single series from FRED and insert into raw_series.

        Parameters:
            series_id: FRED series identifier (e.g. 'T10Y2Y').
            start_date: Earliest observation date to fetch.
            end_date: Latest observation date (default: today).

        Returns:
            dict: Result with keys ``series_id``, ``rows_inserted``,
                  ``status``, ``errors``.
        """
        log.info("Pulling FRED series {sid} from {sd}", sid=series_id, sd=start_date)
        result: dict[str, Any] = {
            "series_id": series_id,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            data: pd.Series = self.fred.get_series(
                series_id,
                observation_start=str(start_date),
                observation_end=str(end_date) if end_date else None,
            )

            if data is None or data.empty:
                log.warning("FRED returned no data for {sid}", sid=series_id)
                result["status"] = "PARTIAL"
                result["errors"].append("No data returned")
                return result

            # Drop NaN values
            data = data.dropna()
            inserted = 0

            with self.engine.begin() as conn:
                for obs_dt, value in data.items():
                    obs_date_val = obs_dt.date() if hasattr(obs_dt, "date") else obs_dt
                    if self._row_exists(series_id, obs_date_val, conn):
                        continue
                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                        ),
                        {
                            "sid": series_id,
                            "src": self.source_id,
                            "od": obs_date_val,
                            "val": float(value),
                        },
                    )
                    inserted += 1

            result["rows_inserted"] = inserted
            log.info(
                "FRED {sid}: inserted {n} rows",
                sid=series_id,
                n=inserted,
            )

        except Exception as exc:
            log.error("FRED pull failed for {sid}: {err}", sid=series_id, err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

            # Record the failure row
            try:
                import json

                with self.engine.begin() as conn:
                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, "
                            "raw_payload, pull_status) "
                            "VALUES (:sid, :src, :od, 0, :payload, 'FAILED')"
                        ),
                        {
                            "sid": series_id,
                            "src": self.source_id,
                            "od": date.today(),
                            "payload": json.dumps({"error": str(exc)}),
                        },
                    )
            except Exception as insert_exc:
                log.error(
                    "Failed to record error row for {sid}: {err}",
                    sid=series_id,
                    err=str(insert_exc),
                )

        # Rate limiting
        time.sleep(_RATE_LIMIT_DELAY)
        return result

    def pull_all(
        self,
        series_list: list[str] | None = None,
        start_date: str | date = "1990-01-01",
        end_date: str | date | None = None,
    ) -> list[dict[str, Any]]:
        """Pull multiple FRED series sequentially.

        Never stops on a single-series failure — logs and continues.

        Parameters:
            series_list: List of FRED series IDs.  Defaults to FRED_SERIES_LIST.
            start_date: Earliest observation date.
            end_date: Latest observation date (default: today).

        Returns:
            list[dict]: One result dict per series.
        """
        if series_list is None:
            series_list = FRED_SERIES_LIST

        log.info(
            "Starting FRED bulk pull — {n} series from {sd}",
            n=len(series_list),
            sd=start_date,
        )
        results: list[dict[str, Any]] = []
        for sid in series_list:
            res = self.pull_series(sid, start_date, end_date)
            results.append(res)
        log.info(
            "FRED bulk pull complete — {ok}/{total} succeeded",
            ok=sum(1 for r in results if r["status"] == "SUCCESS"),
            total=len(results),
        )
        return results

    def get_release_dates(self, series_id: str) -> dict[date, date]:
        """Retrieve release-date metadata for a FRED series.

        Attempts to use the FRED release dates endpoint.  If unavailable,
        falls back to using ``pull_timestamp`` as the release-date proxy.

        Parameters:
            series_id: FRED series identifier.

        Returns:
            dict: Mapping of observation date to release date.
        """
        log.info("Fetching release dates for {sid}", sid=series_id)
        mapping: dict[date, date] = {}

        try:
            # fredapi provides get_series_all_releases for some series
            releases = self.fred.get_series_all_releases(series_id)
            if releases is not None and not releases.empty:
                for _, row in releases.iterrows():
                    obs_dt = row["date"].date() if hasattr(row["date"], "date") else row["date"]
                    rel_dt = (
                        row["realtime_start"].date()
                        if hasattr(row["realtime_start"], "date")
                        else row["realtime_start"]
                    )
                    # Keep the earliest release date for each obs_date
                    if obs_dt not in mapping or rel_dt < mapping[obs_dt]:
                        mapping[obs_dt] = rel_dt
                log.info(
                    "Got {n} release dates for {sid} via FRED API",
                    n=len(mapping),
                    sid=series_id,
                )
                return mapping
        except Exception as exc:
            log.warning(
                "Could not fetch release dates for {sid} from FRED: {err}. "
                "Falling back to pull_timestamp.",
                sid=series_id,
                err=str(exc),
            )

        # Fallback: use pull_timestamp from raw_series
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT obs_date, pull_timestamp::date AS release_date "
                    "FROM raw_series "
                    "WHERE series_id = :sid AND source_id = :src "
                    "AND pull_status = 'SUCCESS' "
                    "ORDER BY obs_date"
                ),
                {"sid": series_id, "src": self.source_id},
            ).fetchall()
            for row in rows:
                mapping[row[0]] = row[1]

        log.info(
            "Got {n} release dates for {sid} via pull_timestamp fallback",
            n=len(mapping),
            sid=series_id,
        )
        return mapping


if __name__ == "__main__":
    from config import settings
    from db import get_engine

    puller = FREDPuller(api_key=settings.FRED_API_KEY, db_engine=get_engine())
    results = puller.pull_all(start_date="2020-01-01")
    for r in results:
        print(f"  {r['series_id']}: {r['status']} ({r['rows_inserted']} rows)")
