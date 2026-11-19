"""
GRID Eurostat bulk download ingestion module.

Pulls Euro area HICP inflation, unemployment, and industrial production
from the Eurostat JSON API.
"""

from __future__ import annotations

import time
from datetime import date
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from ingestion.base import BasePuller
from tenacity import retry, stop_after_attempt, wait_exponential

# Eurostat dataset/series mapping
#
# Checked against the live API from grid-svr 2026-11-07:
# * prc_hicp_manr stopped at 2025-12 (last update 2026-02-06): Eurostat moved
#   HICP to the ECOICOP ver.2 dataset prc_hicp_minr (unit RCH_A = annual rate,
#   coicop18 TOTAL). geo=EA stays the changing-composition euro area.
# * une_rt_m has no EA19 rows any more (euro area is EA21 from 2026) and
#   carries two units, so unit=PC_ACT is required. eurozone_unemployment had
#   never stored a row.
# * sts_inpr_m renamed indic_bt PROD -> PRD and has no NSA rows for B-D;
#   unit I21 (2021=100), seasonally and calendar adjusted. eu_industrial_output
#   had never stored a row either.
EUROSTAT_SERIES: dict[str, str] = {
    "prc_hicp_minr?geo=EA&unit=RCH_A&coicop18=TOTAL&freq=M": "eurozone_hicp_yoy",
    "une_rt_m?geo=EA21&sex=T&age=TOTAL&s_adj=SA&unit=PC_ACT&freq=M": "eurozone_unemployment",
    "sts_inpr_m?geo=EU27_2020&indic_bt=PRD&nace_r2=B-D&s_adj=SCA&unit=I21&freq=M": "eu_industrial_output",
}

_EUROSTAT_BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
_RATE_LIMIT_DELAY: float = 2.0


class EurostatPuller(BasePuller):
    """Pulls Euro area statistics from the Eurostat JSON API."""

    SOURCE_NAME = "Eurostat"
    SOURCE_CONFIG = {"base_url": "https://ec.europa.eu/eurostat/api/dissemination", "cost_tier": "FREE", "latency_class": "MONTHLY", "pit_available": True, "revision_behavior": "RARE", "trust_score": "HIGH", "priority_rank": 14}

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("EurostatPuller initialised — source_id={sid}", sid=self.source_id)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
    def _fetch_data(self, dataset_query: str) -> dict:
        parts = dataset_query.split("?", 1)
        dataset = parts[0]
        query_params = parts[1] if len(parts) > 1 else ""
        url = f"{_EUROSTAT_BASE_URL}/{dataset}?{query_params}&format=JSON&lang=EN"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _parse_observations(self, data: dict) -> list[tuple[date, float]]:
        """Parse Eurostat JSON-stat format observations."""
        observations: list[tuple[date, float]] = []
        try:
            dimension = data.get("dimension", {})
            time_dim = dimension.get("time", {}).get("category", {}).get("index", {})
            values = data.get("value", {})

            # Sort time periods by their index
            sorted_periods = sorted(time_dim.items(), key=lambda x: x[1])

            for period_str, idx in sorted_periods:
                val = values.get(str(idx))
                if val is None:
                    continue
                obs_dt = self._parse_period(period_str)
                if obs_dt:
                    observations.append((obs_dt, float(val)))
        except (KeyError, TypeError) as exc:
            log.warning("Failed to parse Eurostat observations: {err}", err=str(exc))
        return observations

    @staticmethod
    def _parse_period(period_str: str) -> date | None:
        try:
            if "M" in period_str and len(period_str) == 7:
                # Legacy format: 1997M01
                year, month = period_str.split("M")
                return date(int(year), int(month), 1)
            elif "-" in period_str and len(period_str) == 7:
                # Current API format: 1997-01
                year, month = period_str.split("-")
                return date(int(year), int(month), 1)
            elif "Q" in period_str:
                year, q = period_str.split("Q")
                return date(int(year), (int(q) - 1) * 3 + 1, 1)
            elif len(period_str) == 4:
                return date(int(period_str), 1, 1)
        except (ValueError, TypeError):
            pass
        return None

    def _latest_stored_values(self, series_id: str, conn: Any) -> dict[date, float]:
        """Latest SUCCESS value per obs_date this source stored for ``series_id``."""
        rows = conn.execute(
            text(
                "SELECT DISTINCT ON (obs_date) obs_date, value FROM raw_series "
                "WHERE series_id = :sid AND source_id = :src AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date, pull_timestamp DESC"
            ),
            {"sid": series_id, "src": self.source_id},
        ).fetchall()
        return {r[0]: float(r[1]) for r in rows if r[1] is not None}

    def pull_series(self, dataset_query: str, feature_name: str) -> dict[str, Any]:
        """Pull a single Eurostat series."""
        log.info("Pulling Eurostat {fn}", fn=feature_name)
        result: dict[str, Any] = {
            "series_id": feature_name,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            data = self._fetch_data(dataset_query)
            observations = self._parse_observations(data)

            if not observations:
                result["status"] = "PARTIAL"
                return result

            inserted = 0
            with self.engine.begin() as conn:
                # Insert a vintage only when the value is new or revised. The
                # 1-hour _row_exists window re-inserted the whole history on
                # every run: eurozone_hicp_yoy held 85,608 rows for 348 dates
                # on 2026-11-07.
                stored = self._latest_stored_values(feature_name, conn)
                for obs_date_val, value in observations:
                    if obs_date_val in stored and stored[obs_date_val] == value:
                        continue
                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                        ),
                        {"sid": feature_name, "src": self.source_id, "od": obs_date_val, "val": value},
                    )
                    inserted += 1

            result["rows_inserted"] = inserted
            log.info("Eurostat {fn}: inserted {n} rows", fn=feature_name, n=inserted)

        except Exception as exc:
            # Provider/network failure, reported in the result: not a GRID bug.
            log.warning("Eurostat pull failed for {fn}: {err}", fn=feature_name, err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        time.sleep(_RATE_LIMIT_DELAY)
        return result

    def pull_all(self) -> dict[str, Any]:
        """Pull all Eurostat series."""
        log.info("Starting Eurostat bulk pull")
        results = [self.pull_series(dq, fn) for dq, fn in EUROSTAT_SERIES.items()]

        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "Eurostat bulk pull complete — {ok}/{total} succeeded, {rows} rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return {
            "source": "Eurostat",
            "total_rows": total_rows,
            "succeeded": succeeded,
            "total": len(results),
        }
