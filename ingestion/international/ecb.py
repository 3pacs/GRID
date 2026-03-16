"""
GRID ECB Statistical Data Warehouse ingestion module.

Pulls Euro area monetary, credit, and yield data from the ECB SDW SDMX-JSON
REST API. Includes BTP-Bund spread derivation and tenacity retry logic.
"""

from __future__ import annotations

import json
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from tenacity import retry, stop_after_attempt, wait_exponential

# ECB SDW series mapping: SDMX flow_ref -> canonical feature name
ECB_SERIES_LIST: dict[str, str] = {
    "BSI.M.U2.Y.V.M30.X.1.U2.2300.Z01.A": "ecb_m3_yoy",
    "BSI.M.U2.Y.U.A20.A.1.U2.2250.Z01.A": "ecb_bank_lending_yoy",
    "FM.M.DE.EUR.FR.BB.GVT.YLD.10Y": "euro_bund_10y",
    "FM.M.IT.EUR.FR.BB.GVT.YLD.10Y": "italy_btp_10y",
    "IRS.M.IT.L.L40.CI.0000.EUR.N.Z": "italy_btp_spread_ecb",
    "EXR.D.USD.EUR.SP00.A": "eurusd_ecb_daily",
}

# Base URL for ECB SDW REST API
_ECB_BASE_URL = "https://sdw-wsrest.ecb.europa.eu/service/data"

# Rate limit: 1 request per second
_RATE_LIMIT_DELAY: float = 1.0


class ECBPuller:
    """Pulls Euro area time series from the ECB Statistical Data Warehouse.

    Attributes:
        engine: SQLAlchemy engine for database writes.
        source_id: The source_catalog.id for ECB_SDW.
    """

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.source_id = self._resolve_source_id()
        log.info("ECBPuller initialised — source_id={sid}", sid=self.source_id)

    def _resolve_source_id(self) -> int:
        """Look up or create the ECB_SDW source in source_catalog."""
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "ECB_SDW"},
            ).fetchone()
        if row is None:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text(
                        "INSERT INTO source_catalog "
                        "(name, base_url, license_type, update_frequency, "
                        "has_vintage_data, revision_policy, data_quality, priority, model_eligible) "
                        "VALUES (:name, :url, 'FREE', 'DAILY', TRUE, 'RARE', 'HIGH', 10, TRUE) "
                        "RETURNING id"
                    ),
                    {"name": "ECB_SDW", "url": _ECB_BASE_URL},
                )
                return result.fetchone()[0]
        return row[0]

    def _row_exists(self, series_id: str, obs_date: date, conn: Any) -> bool:
        """Check whether a duplicate row already exists within 1 hour."""
        one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        result = conn.execute(
            text(
                "SELECT 1 FROM raw_series "
                "WHERE series_id = :sid AND source_id = :src "
                "AND obs_date = :od AND pull_timestamp >= :ts "
                "LIMIT 1"
            ),
            {"sid": series_id, "src": self.source_id, "od": obs_date, "ts": one_hour_ago},
        ).fetchone()
        return result is not None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
    def _fetch_sdmx_json(self, flow_ref: str, start_period: str, end_period: str | None) -> dict:
        """Fetch SDMX-JSON data from ECB SDW with retry logic."""
        # Split flow_ref into dataset key components
        parts = flow_ref.split(".")
        dataset = parts[0]
        key = ".".join(parts[1:])

        url = f"{_ECB_BASE_URL}/{dataset}/{key}"
        params: dict[str, str] = {
            "startPeriod": start_period,
            "detail": "dataonly",
        }
        if end_period:
            params["endPeriod"] = end_period

        headers = {"Accept": "application/json"}
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _parse_sdmx_observations(self, data: dict) -> list[tuple[date, float]]:
        """Parse observations from ECB SDMX-JSON response format.

        Returns a list of (obs_date, value) tuples.
        """
        observations: list[tuple[date, float]] = []
        try:
            datasets = data.get("dataSets", [])
            if not datasets:
                return observations

            # Navigate the SDMX-JSON structure
            structure = data.get("structure", {})
            dimensions = structure.get("dimensions", {})
            obs_dims = dimensions.get("observation", [])

            # Find the time dimension values
            time_values = []
            for dim in obs_dims:
                if dim.get("id") in ("TIME_PERIOD", "TIME"):
                    time_values = [v.get("id", v.get("name", "")) for v in dim.get("values", [])]
                    break

            # Extract series data
            series_data = datasets[0].get("series", {})
            for _series_key, series_obj in series_data.items():
                obs_map = series_obj.get("observations", {})
                for idx_str, val_list in obs_map.items():
                    idx = int(idx_str)
                    if idx < len(time_values) and val_list:
                        period_str = time_values[idx]
                        value = val_list[0]
                        if value is None:
                            continue
                        # Parse period string (YYYY-MM, YYYY-MM-DD, or YYYY-QN)
                        obs_dt = self._parse_period(period_str)
                        if obs_dt:
                            observations.append((obs_dt, float(value)))
        except (KeyError, IndexError, TypeError) as exc:
            log.warning("Failed to parse SDMX observations: {err}", err=str(exc))

        return observations

    @staticmethod
    def _parse_period(period_str: str) -> date | None:
        """Parse ECB period strings into date objects."""
        try:
            if len(period_str) == 10:
                return datetime.strptime(period_str, "%Y-%m-%d").date()
            elif len(period_str) == 7 and "-Q" in period_str:
                year, q = period_str.split("-Q")
                month = (int(q) - 1) * 3 + 1
                return date(int(year), month, 1)
            elif len(period_str) == 7:
                return datetime.strptime(period_str, "%Y-%m").date()
            elif len(period_str) == 4:
                return date(int(period_str), 1, 1)
        except (ValueError, TypeError):
            pass
        return None

    def pull_series(
        self,
        flow_ref: str,
        start_date: str | date = "1999-01-01",
        end_date: str | date | None = None,
    ) -> dict[str, Any]:
        """Fetch a single ECB SDW series and insert into raw_series.

        Parameters:
            flow_ref: ECB SDMX flow reference (e.g. 'BSI.M.U2.Y.V.M30.X.1.U2.2300.Z01.A').
            start_date: Earliest observation date.
            end_date: Latest observation date.

        Returns:
            dict with series_id, rows_inserted, status, errors.
        """
        feature_name = ECB_SERIES_LIST.get(flow_ref, flow_ref)
        log.info("Pulling ECB series {fr} ({fn})", fr=flow_ref, fn=feature_name)

        result: dict[str, Any] = {
            "series_id": flow_ref,
            "feature_name": feature_name,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            start_str = str(start_date)[:7] if len(str(start_date)) > 7 else str(start_date)
            end_str = str(end_date)[:7] if end_date else None

            data = self._fetch_sdmx_json(flow_ref, start_str, end_str)
            observations = self._parse_sdmx_observations(data)

            if not observations:
                log.warning("ECB returned no data for {fr}", fr=flow_ref)
                result["status"] = "PARTIAL"
                result["errors"].append("No data returned")
                return result

            inserted = 0
            with self.engine.begin() as conn:
                for obs_date_val, value in observations:
                    if self._row_exists(feature_name, obs_date_val, conn):
                        continue
                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                        ),
                        {
                            "sid": feature_name,
                            "src": self.source_id,
                            "od": obs_date_val,
                            "val": value,
                        },
                    )
                    inserted += 1

            result["rows_inserted"] = inserted
            log.info("ECB {fn}: inserted {n} rows", fn=feature_name, n=inserted)

        except Exception as exc:
            log.error("ECB pull failed for {fr}: {err}", fr=flow_ref, err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))
            self._record_failure(flow_ref, exc)

        time.sleep(_RATE_LIMIT_DELAY)
        return result

    def _compute_btp_bund_spread(self) -> dict[str, Any]:
        """Compute BTP-Bund spread from the two yield series.

        Reads the most recent pulled values for italy_btp_10y and euro_bund_10y,
        computes the difference, and inserts as euro_btp_bund_spread.
        """
        result: dict[str, Any] = {"rows_inserted": 0, "status": "SUCCESS", "errors": []}

        try:
            with self.engine.begin() as conn:
                # Get matching dates where both series exist
                rows = conn.execute(
                    text(
                        "SELECT a.obs_date, a.value AS btp, b.value AS bund "
                        "FROM raw_series a "
                        "JOIN raw_series b ON a.obs_date = b.obs_date "
                        "WHERE a.series_id = 'italy_btp_10y' AND a.source_id = :src "
                        "AND b.series_id = 'euro_bund_10y' AND b.source_id = :src "
                        "AND a.pull_status = 'SUCCESS' AND b.pull_status = 'SUCCESS' "
                        "ORDER BY a.obs_date"
                    ),
                    {"src": self.source_id},
                ).fetchall()

                inserted = 0
                for row in rows:
                    spread = row[1] - row[2]  # BTP - Bund
                    if self._row_exists("euro_btp_bund_spread", row[0], conn):
                        continue
                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                        ),
                        {
                            "sid": "euro_btp_bund_spread",
                            "src": self.source_id,
                            "od": row[0],
                            "val": spread,
                        },
                    )
                    inserted += 1

                result["rows_inserted"] = inserted
                log.info("BTP-Bund spread: inserted {n} rows", n=inserted)

        except Exception as exc:
            log.error("BTP-Bund spread computation failed: {err}", err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def pull_all(self, start_date: str | date = "1999-01-01") -> dict[str, Any]:
        """Pull all ECB SDW series and compute derived spreads.

        Parameters:
            start_date: Earliest observation date.

        Returns:
            Summary dict with total rows and per-series results.
        """
        log.info("Starting ECB bulk pull from {sd}", sd=start_date)
        results: list[dict[str, Any]] = []

        for flow_ref in ECB_SERIES_LIST:
            res = self.pull_series(flow_ref, start_date)
            results.append(res)

        # Compute BTP-Bund spread after pulling both yield series
        spread_result = self._compute_btp_bund_spread()
        results.append(spread_result)

        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "ECB bulk pull complete — {ok}/{total} succeeded, {rows} total rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return {
            "source": "ECB_SDW",
            "total_rows": total_rows,
            "succeeded": succeeded,
            "total": len(results),
            "results": results,
        }

    def _record_failure(self, series_id: str, exc: Exception) -> None:
        """Record a failed pull attempt in raw_series."""
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO raw_series "
                        "(series_id, source_id, obs_date, value, raw_payload, pull_status) "
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
            log.error("Failed to record error row: {err}", err=str(insert_exc))
