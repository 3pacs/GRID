"""
GRID Baltic Dry Index and shipping indices ingestion module.

Pulls the Baltic Dry Index (BDI) and related sub-indices from the FRED API.
The BDI is a key global trade activity and commodity demand indicator issued
by the Baltic Exchange. FRED mirrors the series with IDs DBDI, BCPI, BPTI,
and BSI.

Data source: FRED (Federal Reserve Economic Data) via the fedfred library.
"""

from __future__ import annotations

import json
import time
from datetime import date, timedelta
from typing import Any

import pandas as pd
from fedfred import FredAPI
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# Baltic Exchange series available on FRED
# Maps internal GRID series_id -> FRED series code
BALTIC_SERIES: dict[str, dict[str, str]] = {
    "baltic.bdi": {
        "fred_id": "DBDI",
        "description": "Baltic Dry Index — composite shipping cost benchmark",
    },
    "baltic.capesize": {
        "fred_id": "BCPI",
        "description": "Baltic Capesize Index — large bulk carriers (100K+ DWT)",
    },
    "baltic.panamax": {
        "fred_id": "BPTI",
        "description": "Baltic Panamax Index — mid-size bulk carriers (60-80K DWT)",
    },
    "baltic.supramax": {
        "fred_id": "BSI",
        "description": "Baltic Supramax Index — handymax bulk carriers (45-60K DWT)",
    },
}

# Minimum delay between FRED API calls (seconds)
_RATE_LIMIT_DELAY: float = 0.25


class BalticDryPuller(BasePuller):
    """Pulls Baltic Dry Index and sub-indices from FRED into ``raw_series``.

    Uses the FRED API via fedfred to fetch BDI, Capesize, Panamax, and
    Supramax indices. Stores each under the ``baltic.*`` series namespace
    with Baltic_Exchange as the logical source.

    Attributes:
        fred: fedfred.FredAPI client instance.
        engine: SQLAlchemy engine for database writes.
        source_id: The ``source_catalog.id`` for Baltic_Exchange.
    """

    SOURCE_NAME: str = "Baltic_Exchange"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://www.balticexchange.com/",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 30,
    }

    def __init__(self, api_key: str, db_engine: Engine) -> None:
        """Initialise the Baltic Dry puller.

        Parameters:
            api_key: FRED API key (used to fetch Baltic series from FRED).
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        self.fred = FredAPI(api_key)
        super().__init__(db_engine)
        log.info("BalticDryPuller initialised — source_id={sid}", sid=self.source_id)

    def pull_series(
        self,
        grid_series_id: str,
        start_date: str | date = "2000-01-01",
        end_date: str | date | None = None,
    ) -> dict[str, Any]:
        """Fetch a single Baltic series from FRED and insert into raw_series.

        Parameters:
            grid_series_id: Internal GRID series ID (e.g. 'baltic.bdi').
            start_date: Earliest observation date to fetch.
            end_date: Latest observation date (default: today).

        Returns:
            dict: Result with keys ``series_id``, ``rows_inserted``,
                  ``status``, ``errors``.
        """
        if grid_series_id not in BALTIC_SERIES:
            return {
                "series_id": grid_series_id,
                "rows_inserted": 0,
                "status": "FAILED",
                "errors": [f"Unknown series: {grid_series_id}"],
            }

        fred_id = BALTIC_SERIES[grid_series_id]["fred_id"]
        log.info(
            "Pulling {gsid} (FRED {fid}) from {sd}",
            gsid=grid_series_id,
            fid=fred_id,
            sd=start_date,
        )

        result: dict[str, Any] = {
            "series_id": grid_series_id,
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
                fred_id, **obs_kwargs
            )

            if data is None or data.empty:
                log.warning("FRED returned no data for {fid}", fid=fred_id)
                result["status"] = "PARTIAL"
                result["errors"].append("No data returned")
                return result

            # Normalise column names (fedfred may vary by version)
            if "date" in data.columns and "value" in data.columns:
                pass
            elif "observation_date" in data.columns:
                data = data.rename(columns={"observation_date": "date"})
            else:
                if data.index.name == "date" or hasattr(data.index, "date"):
                    data = data.reset_index()

            # Drop rows where value is NaN or FRED's '.' missing marker
            data = data[data["value"].apply(
                lambda v: v != "." and pd.notna(v)
            )].copy()
            data["value"] = pd.to_numeric(data["value"], errors="coerce")
            coerced_count = data["value"].isna().sum()
            if coerced_count > 0:
                log.warning(
                    "Coerced {n} non-numeric values to NaN for {gsid}",
                    n=int(coerced_count),
                    gsid=grid_series_id,
                )
            data = data.dropna(subset=["value"])

            inserted = 0

            with self.engine.begin() as conn:
                # Batch dedup: one query instead of per-row checks
                existing_dates = self._get_existing_dates(grid_series_id, conn)
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
                            "(series_id, source_id, obs_date, value, "
                            "raw_payload, pull_status) "
                            "VALUES (:sid, :src, :od, :val, :payload, 'SUCCESS')"
                        ),
                        {
                            "sid": grid_series_id,
                            "src": self.source_id,
                            "od": obs_date_val,
                            "val": float(row["value"]),
                            "payload": json.dumps({
                                "fred_series": fred_id,
                                "source": "Baltic_Exchange_via_FRED",
                            }),
                        },
                    )
                    inserted += 1

                if skipped:
                    log.debug(
                        "{gsid}: skipped {n} existing rows",
                        gsid=grid_series_id,
                        n=skipped,
                    )

            result["rows_inserted"] = inserted
            log.info(
                "{gsid}: inserted {n} rows",
                gsid=grid_series_id,
                n=inserted,
            )

        except Exception as exc:
            log.error(
                "Baltic pull failed for {gsid} (FRED {fid}): {err}",
                gsid=grid_series_id,
                fid=fred_id,
                err=str(exc),
            )
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

            # Record the failure row for observability
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
                            "sid": grid_series_id,
                            "src": self.source_id,
                            "od": date.today(),
                            "payload": json.dumps({"error": str(exc)}),
                        },
                    )
            except Exception as insert_exc:
                log.error(
                    "Failed to record error row for {gsid}: {err}",
                    gsid=grid_series_id,
                    err=str(insert_exc),
                )

        # Rate limiting between FRED API calls
        time.sleep(_RATE_LIMIT_DELAY)
        return result

    def pull_all(
        self,
        series_list: list[str] | None = None,
        start_date: str | date = "2000-01-01",
        end_date: str | date | None = None,
    ) -> list[dict[str, Any]]:
        """Pull all Baltic shipping indices sequentially.

        Never stops on a single-series failure -- logs and continues.

        Parameters:
            series_list: List of GRID series IDs to pull. Defaults to all
                         four Baltic indices.
            start_date: Earliest observation date.
            end_date: Latest observation date (default: today).

        Returns:
            list[dict]: One result dict per series.
        """
        if series_list is None:
            series_list = list(BALTIC_SERIES.keys())

        log.info(
            "Starting Baltic bulk pull — {n} series from {sd}",
            n=len(series_list),
            sd=start_date,
        )

        results: list[dict[str, Any]] = []
        for gsid in series_list:
            # Incremental pull: start from last known date minus 7-day overlap
            latest = self._get_latest_date(gsid)
            effective_start = start_date
            if latest is not None:
                incremental = latest - timedelta(days=7)
                start_as_date = (
                    date.fromisoformat(str(start_date))
                    if isinstance(start_date, str)
                    else start_date
                )
                if incremental > start_as_date:
                    effective_start = incremental.isoformat()
                    log.info(
                        "{gsid}: incremental from {d} (last={l})",
                        gsid=gsid,
                        d=effective_start,
                        l=latest,
                    )

            res = self.pull_series(gsid, effective_start, end_date)
            results.append(res)

        log.info(
            "Baltic bulk pull complete — {ok}/{total} succeeded",
            ok=sum(1 for r in results if r["status"] == "SUCCESS"),
            total=len(results),
        )
        return results


if __name__ == "__main__":
    from config import settings
    from db import get_engine

    puller = BalticDryPuller(
        api_key=settings.FRED_API_KEY,
        db_engine=get_engine(),
    )
    results = puller.pull_all(start_date="2015-01-01")
    for r in results:
        print(f"  {r['series_id']}: {r['status']} ({r['rows_inserted']} rows)")
