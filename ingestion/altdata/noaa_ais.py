"""
GRID NOAA AIS vessel traffic ingestion module.

Pulls vessel traffic data from NOAA AIS (Automatic Identification System)
pre-aggregated port call statistics. Computes port congestion indices
from arrival counts relative to seasonal baselines.

Note: Full AIS data is 1-1.5TB. This module uses pre-aggregated monthly
summary statistics rather than raw vessel tracks.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.base import BasePuller

# Port bounding boxes for vessel counting
AIS_PORTS: dict[str, dict[str, Any]] = {
    "los_angeles": {
        "lat_min": 33.5, "lat_max": 33.9,
        "lon_min": -118.4, "lon_max": -117.8,
        "feature": "ais_port_arrivals_la",
    },
    "rotterdam": {
        "lat_min": 51.8, "lat_max": 52.0,
        "lon_min": 3.9, "lon_max": 4.6,
        "feature": "ais_port_arrivals_rotterdam",
    },
    "shanghai": {
        "lat_min": 30.6, "lat_max": 31.5,
        "lon_min": 121.3, "lon_max": 122.3,
        "feature": "ais_port_arrivals_shanghai",
    },
    "singapore": {
        "lat_min": 1.0, "lat_max": 1.5,
        "lon_min": 103.5, "lon_max": 104.2,
        "feature": "ais_port_arrivals_singapore",
    },
}

_RATE_LIMIT_DELAY: float = 2.0


class NOAAAISPuller(BasePuller):
    """Pulls vessel traffic data from NOAA AIS summary statistics."""

    SOURCE_NAME = "NOAA_AIS"
    SOURCE_CONFIG = {
        "base_url": "https://marinecadastre.gov/ais",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": False,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 35,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("NOAAAISPuller initialised — source_id={sid}", sid=self.source_id)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
    def _fetch_ais_summary(self, year: int, month: int) -> dict[str, int]:
        """Fetch pre-aggregated AIS vessel count data.

        NOAA publishes monthly zone summaries. This method attempts to
        download from the MarineCadastre AIS data portal.
        """
        # MarineCadastre AIS summary endpoint
        url = f"https://marinecadastre.gov/ais/AIS_{year}_{month:02d}_Zone_Summary.csv"

        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                from io import StringIO
                df = pd.read_csv(StringIO(resp.text))
                port_counts: dict[str, int] = {}
                for port_name, port_info in AIS_PORTS.items():
                    # Filter by bounding box coordinates
                    mask = (
                        (df.get("lat", df.get("latitude", pd.Series())) >= port_info["lat_min"])
                        & (df.get("lat", df.get("latitude", pd.Series())) <= port_info["lat_max"])
                        & (df.get("lon", df.get("longitude", pd.Series())) >= port_info["lon_min"])
                        & (df.get("lon", df.get("longitude", pd.Series())) <= port_info["lon_max"])
                    )
                    port_counts[port_name] = int(mask.sum()) if mask.any() else 0
                return port_counts
        except Exception as exc:
            log.warning("AIS data parsing failed for {y}-{m:02d}: {e}", y=year, m=month, e=exc)

        # Return empty dict if data not available
        log.debug("AIS summary not available for {y}-{m:02d}", y=year, m=month)
        return {}

    def pull_monthly_summary(self, year: int, month: int) -> dict[str, Any]:
        """Pull monthly vessel arrival counts by port zone."""
        log.info("Pulling NOAA AIS for {y}-{m:02d}", y=year, m=month)
        result: dict[str, Any] = {
            "series_id": "ais_monthly",
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            port_counts = self._fetch_ais_summary(year, month)
            obs_dt = date(year, month, 1)

            inserted = 0
            with self.engine.begin() as conn:
                for port_name, port_info in AIS_PORTS.items():
                    count = port_counts.get(port_name, 0)
                    if count == 0:
                        continue

                    feature = port_info["feature"]
                    if not self._row_exists(feature, obs_dt, conn):
                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, pull_status) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                            ),
                            {"sid": feature, "src": self.source_id, "od": obs_dt, "val": float(count)},
                        )
                        inserted += 1

            result["rows_inserted"] = inserted
            if not port_counts:
                result["status"] = "PARTIAL"
                result["errors"].append("No AIS data available for this period")

        except Exception as exc:
            log.error("AIS pull failed for {y}-{m}: {err}", y=year, m=month, err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def compute_congestion_index(self) -> dict[str, Any]:
        """Compute port congestion indices relative to seasonal baselines."""
        result: dict[str, Any] = {"rows_inserted": 0, "status": "SUCCESS", "errors": []}

        try:
            with self.engine.begin() as conn:
                global_congestion_by_date: dict[date, list[float]] = {}

                for port_name, port_info in AIS_PORTS.items():
                    feature = port_info["feature"]
                    rows = conn.execute(
                        text(
                            "SELECT obs_date, value FROM raw_series "
                            "WHERE series_id = :sid AND source_id = :src "
                            "AND pull_status = 'SUCCESS' ORDER BY obs_date"
                        ),
                        {"sid": feature, "src": self.source_id},
                    ).fetchall()

                    if len(rows) < 12:
                        continue

                    # Compute seasonal average by month
                    monthly_avg: dict[int, list[float]] = {}
                    for obs_dt, value in rows:
                        m = obs_dt.month
                        if m not in monthly_avg:
                            monthly_avg[m] = []
                        monthly_avg[m].append(value)

                    seasonal_baseline = {m: sum(v) / len(v) for m, v in monthly_avg.items()}

                    # Compute congestion index
                    inserted = 0
                    for obs_dt, value in rows:
                        baseline = seasonal_baseline.get(obs_dt.month, value)
                        if baseline > 0:
                            congestion = value / baseline
                            cong_feature = f"port_congestion_{port_name}"
                            if not self._row_exists(cong_feature, obs_dt, conn):
                                conn.execute(
                                    text(
                                        "INSERT INTO raw_series "
                                        "(series_id, source_id, obs_date, value, pull_status) "
                                        "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                                    ),
                                    {"sid": cong_feature, "src": self.source_id, "od": obs_dt, "val": congestion},
                                )
                                inserted += 1

                            if obs_dt not in global_congestion_by_date:
                                global_congestion_by_date[obs_dt] = []
                            global_congestion_by_date[obs_dt].append(congestion)

                # Compute global weighted average congestion
                for obs_dt, values in sorted(global_congestion_by_date.items()):
                    global_cong = sum(values) / len(values)
                    if not self._row_exists("global_port_congestion", obs_dt, conn):
                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, pull_status) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                            ),
                            {"sid": "global_port_congestion", "src": self.source_id, "od": obs_dt, "val": global_cong},
                        )
                        inserted += 1

                result["rows_inserted"] = inserted

        except Exception as exc:
            log.error("Congestion index computation failed: {err}", err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def pull_all(self, start_year: int = 2009) -> dict[str, Any]:
        """Pull all AIS data and compute congestion indices."""
        log.info("Starting NOAA AIS pull from {sy}", sy=start_year)
        results: list[dict[str, Any]] = []

        for year in range(start_year, date.today().year + 1):
            for month in range(1, 13):
                if year == date.today().year and month > date.today().month:
                    break
                res = self.pull_monthly_summary(year, month)
                results.append(res)
                time.sleep(_RATE_LIMIT_DELAY)

        # Compute congestion indices after pulling all data
        cong_result = self.compute_congestion_index()
        results.append(cong_result)

        total_rows = sum(r["rows_inserted"] for r in results)
        log.info("NOAA AIS pull complete — {rows} total rows", rows=total_rows)
        return {
            "source": "NOAA_AIS",
            "total_rows": total_rows,
            "succeeded": sum(1 for r in results if r["status"] == "SUCCESS"),
            "total": len(results),
        }
