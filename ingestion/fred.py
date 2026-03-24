"""
GRID FRED data ingestion module.

Pulls economic time series from the Federal Reserve Economic Data (FRED) API
using the ``fedfred`` library and stores raw observations in ``raw_series``.
Includes deduplication, rate limiting, release-date retrieval, and full error
handling.
"""

from __future__ import annotations

import json
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
from fedfred import FredAPI
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

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


class FREDPuller(BasePuller):
    """Pulls time series data from the FRED API into ``raw_series``.

    Attributes:
        fred: fedfred.FredAPI client instance.
        engine: SQLAlchemy engine for database writes.
        source_id: The ``source_catalog.id`` for the FRED source.
    """

    SOURCE_NAME: str = "FRED"

    def __init__(self, api_key: str, db_engine: Engine) -> None:
        """Initialise the FRED puller.

        Parameters:
            api_key: FRED API key.
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        self.fred = FredAPI(api_key)
        super().__init__(db_engine)
        log.info("FREDPuller initialised — source_id={sid}", sid=self.source_id)

    def pull_series(
        self,
        series_id: str,
        start_date: str | date = "1990-01-01",
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
            obs_kwargs: dict[str, Any] = {
                "observation_start": str(start_date),
            }
            if end_date:
                obs_kwargs["observation_end"] = str(end_date)

            data: pd.DataFrame = self.fred.get_series_observations(
                series_id, **obs_kwargs
            )

            if data is None or data.empty:
                log.warning("FRED returned no data for {sid}", sid=series_id)
                result["status"] = "PARTIAL"
                result["errors"].append("No data returned")
                return result

            # fedfred returns a DataFrame with 'date' and 'value' columns
            # Normalise column names (may vary by version)
            if "date" in data.columns and "value" in data.columns:
                pass
            elif "observation_date" in data.columns:
                data = data.rename(columns={"observation_date": "date"})
            else:
                # Fallback: try index as date
                if data.index.name == "date" or hasattr(data.index, "date"):
                    data = data.reset_index()

            # Drop rows where value is NaN or '.'
            data = data[data["value"].apply(
                lambda v: v != "." and pd.notna(v)
            )].copy()
            pre_coerce_count = len(data)
            data["value"] = pd.to_numeric(data["value"], errors="coerce")
            coerced_count = data["value"].isna().sum()
            if coerced_count > 0:
                log.warning(
                    "Coerced {n} non-numeric values to NaN for series {sid}",
                    n=int(coerced_count),
                    sid=series_id,
                )
            data = data.dropna(subset=["value"])

            inserted = 0

            with self.engine.begin() as conn:
                # Batch fetch all existing dates — one query instead of N
                existing_dates = self._get_existing_dates(series_id, conn)
                skipped = 0
                for _, row in data.iterrows():
                    obs_date_val = (
                        row["date"].date()
                        if hasattr(row["date"], "date") and callable(row["date"].date)
                        else pd.Timestamp(row["date"]).date()
                    )
                    if obs_date_val in existing_dates:
                        skipped += 1
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
                            "val": float(row["value"]),
                        },
                    )
                    inserted += 1
                if skipped:
                    log.debug("FRED {sid}: skipped {n} existing rows", sid=series_id, n=skipped)

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
            # Use incremental start: only fetch from last known date - 7 day overlap
            latest = self._get_latest_date(sid)
            effective_start = start_date
            if latest is not None:
                incremental = latest - timedelta(days=7)
                # Use whichever is more recent
                start_as_date = date.fromisoformat(str(start_date)) if isinstance(start_date, str) else start_date
                if incremental > start_as_date:
                    effective_start = incremental.isoformat()
                    log.info("FRED {sid}: incremental from {d} (last={l})", sid=sid, d=effective_start, l=latest)
            res = self.pull_series(sid, effective_start, end_date)
            results.append(res)
        log.info(
            "FRED bulk pull complete — {ok}/{total} succeeded",
            ok=sum(1 for r in results if r["status"] == "SUCCESS"),
            total=len(results),
        )
        return results

    def get_release_dates(self, series_id: str) -> dict[date, date]:
        """Retrieve release-date metadata for a FRED series.

        Uses the FRED vintage dates endpoint via fedfred. Falls back to
        pull_timestamp from raw_series if unavailable.

        Parameters:
            series_id: FRED series identifier.

        Returns:
            dict: Mapping of observation date to release date.
        """
        log.info("Fetching release dates for {sid}", sid=series_id)
        mapping: dict[date, date] = {}

        try:
            # fedfred supports vintage dates via get_series_vintagedates
            vintages = self.fred.get_series_vintagedates(series_id)
            if vintages is not None and not vintages.empty:
                # vintages is a DataFrame/Series of realtime dates
                # For each vintage, pull observations to build obs_date -> release_date map
                for vdate in vintages.head(50).values:
                    vd = pd.Timestamp(vdate).date() if not isinstance(vdate, date) else vdate
                    try:
                        obs = self.fred.get_series_observations(
                            series_id,
                            realtime_start=str(vd),
                            realtime_end=str(vd),
                        )
                        if obs is not None and not obs.empty:
                            for _, row in obs.iterrows():
                                od = pd.Timestamp(row["date"]).date()
                                if od not in mapping or vd < mapping[od]:
                                    mapping[od] = vd
                    except Exception:
                        continue
                    time.sleep(_RATE_LIMIT_DELAY)

                if mapping:
                    log.info(
                        "Got {n} release dates for {sid} via fedfred vintages",
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
