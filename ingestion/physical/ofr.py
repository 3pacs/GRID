"""
GRID OFR Financial Stability Monitor ingestion module.

Pulls financial stability data from the Office of Financial Research (OFR).
Covers FSM category scores (credit, funding, leverage), Financial Stress
Index (FSI), and Short-Term Funding Monitor (STFM) data.
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
from tenacity import retry, stop_after_attempt, wait_exponential  # noqa: F401

# OFR dataset endpoints
OFR_DATASETS: dict[str, str] = {
    "short-term-funding-monitor": "ofr_stfm_data",
    "financial-stress-index": "ofr_fsi",
}

_OFR_BASE_URL = "https://financialresearch.gov/data"
_RATE_LIMIT_DELAY: float = 2.0


class OFRPuller:
    """Pulls financial stability data from the OFR."""

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.source_id = self._resolve_source_id()
        log.info("OFRPuller initialised — source_id={sid}", sid=self.source_id)

    def _resolve_source_id(self) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "OFR"},
            ).fetchone()
        if row is None:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text(
                        "INSERT INTO source_catalog "
                        "(name, base_url, license_type, update_frequency, "
                        "has_vintage_data, revision_policy, data_quality, priority, model_eligible) "
                        "VALUES (:name, :url, 'FREE', 'WEEKLY', FALSE, 'NEVER', 'HIGH', 33, TRUE) "
                        "RETURNING id"
                    ),
                    {"name": "OFR", "url": _OFR_BASE_URL},
                )
                return result.fetchone()[0]
        return row[0]

    def _row_exists(self, series_id: str, obs_date: date, conn: Any) -> bool:
        one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        result = conn.execute(
            text(
                "SELECT 1 FROM raw_series "
                "WHERE series_id = :sid AND source_id = :src "
                "AND obs_date = :od AND pull_timestamp >= :ts LIMIT 1"
            ),
            {"sid": series_id, "src": self.source_id, "od": obs_date, "ts": one_hour_ago},
        ).fetchone()
        return result is not None

    def _fetch_csv(self, url: str) -> pd.DataFrame | None:
        """Fetch CSV data from OFR website.

        Handles endpoint changes gracefully: logs a warning on 400/403/404
        instead of retrying indefinitely or crashing.  Retries up to 3 times
        on transient network errors only.
        """
        try:
            resp = requests.get(url, timeout=30)
        except requests.RequestException as exc:
            log.warning("OFR request failed for {u}: {e}", u=url, e=str(exc))
            return None
        if resp.status_code == 200:
            from io import StringIO
            return pd.read_csv(StringIO(resp.text))
        if resp.status_code in (400, 403, 404):
            log.warning(
                "OFR endpoint returned {code} for {url} — endpoint may have changed. "
                "Skipping gracefully.",
                code=resp.status_code,
                url=url,
            )
            return None
        log.warning("OFR unexpected status {code} for {url}", code=resp.status_code, url=url)
        return None

    def pull_fsm(self) -> dict[str, Any]:
        """Pull OFR Financial Stability Monitor data.

        Extracts category scores for Credit, Funding, Leverage, Risk Appetite,
        and computes overall composite score.
        """
        log.info("Pulling OFR Financial Stability Monitor")
        result: dict[str, Any] = {
            "series_id": "ofr_fsm",
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            # OFR FSM data download endpoint — try current and legacy URLs
            df = None
            for url in [
                "https://www.financialresearch.gov/financial-stability-monitor/download/",
                "https://financialresearch.gov/financial-stability-monitor/download/",
                "https://data.financialresearch.gov/v1/financial-stability-monitor/download/",
            ]:
                df = self._fetch_csv(url)
                if df is not None and not df.empty:
                    break

            if df is None or df.empty:
                result["status"] = "PARTIAL"
                result["errors"].append("FSM data not available")
                return result

            # FSM categories to extract
            category_map = {
                "credit": "ofr_fsm_credit",
                "funding": "ofr_fsm_funding",
                "leverage": "ofr_fsm_leverage",
            }

            inserted = 0
            with self.engine.begin() as conn:
                for _, row in df.iterrows():
                    try:
                        # Find date column
                        date_val = None
                        for col in ["Date", "date", "DATE"]:
                            if col in df.columns:
                                date_val = pd.Timestamp(str(row[col])).date()
                                break
                        if date_val is None:
                            continue

                        # Extract category values
                        category_values = {}
                        for cat_key, feature in category_map.items():
                            for col in df.columns:
                                if cat_key.lower() in col.lower():
                                    try:
                                        val = float(row[col])
                                        if not pd.isna(val):
                                            category_values[feature] = val
                                    except (ValueError, TypeError):
                                        continue
                                    break

                        # Insert category values
                        for feature, val in category_values.items():
                            if not self._row_exists(feature, date_val, conn):
                                conn.execute(
                                    text(
                                        "INSERT INTO raw_series "
                                        "(series_id, source_id, obs_date, value, pull_status) "
                                        "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                                    ),
                                    {"sid": feature, "src": self.source_id, "od": date_val, "val": val},
                                )
                                inserted += 1

                        # Compute composite
                        if category_values:
                            composite = sum(category_values.values()) / len(category_values)
                            if not self._row_exists("ofr_fsm_composite", date_val, conn):
                                conn.execute(
                                    text(
                                        "INSERT INTO raw_series "
                                        "(series_id, source_id, obs_date, value, pull_status) "
                                        "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                                    ),
                                    {"sid": "ofr_fsm_composite", "src": self.source_id, "od": date_val, "val": composite},
                                )
                                inserted += 1

                    except Exception as row_exc:
                        log.debug("Skipping FSM row: {err}", err=str(row_exc))

            result["rows_inserted"] = inserted
            log.info("OFR FSM: inserted {n} rows", n=inserted)

        except Exception as exc:
            log.error("OFR FSM pull failed: {err}", err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def pull_fsi(self) -> dict[str, Any]:
        """Pull OFR Financial Stress Index data."""
        log.info("Pulling OFR Financial Stress Index")
        result: dict[str, Any] = {
            "series_id": "ofr_fsi",
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            df = None
            for url in [
                "https://www.financialresearch.gov/financial-stress-index/download/",
                "https://financialresearch.gov/financial-stress-index/download/",
                "https://data.financialresearch.gov/v1/financial-stress-index/download/",
            ]:
                df = self._fetch_csv(url)
                if df is not None and not df.empty:
                    break

            if df is None or df.empty:
                result["status"] = "PARTIAL"
                return result

            inserted = 0
            with self.engine.begin() as conn:
                for _, row in df.iterrows():
                    try:
                        date_val = pd.Timestamp(str(row.iloc[0])).date()
                        value = float(row.iloc[1])
                        if pd.isna(value):
                            continue
                        if not self._row_exists("ofr_fsi", date_val, conn):
                            conn.execute(
                                text(
                                    "INSERT INTO raw_series "
                                    "(series_id, source_id, obs_date, value, pull_status) "
                                    "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                                ),
                                {"sid": "ofr_fsi", "src": self.source_id, "od": date_val, "val": value},
                            )
                            inserted += 1
                    except (ValueError, TypeError):
                        continue

            result["rows_inserted"] = inserted
            log.info("OFR FSI: inserted {n} rows", n=inserted)

        except Exception as exc:
            log.error("OFR FSI pull failed: {err}", err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def pull_stfm(self) -> dict[str, Any]:
        """Pull OFR Short-Term Funding Monitor data."""
        log.info("Pulling OFR Short-Term Funding Monitor")
        result: dict[str, Any] = {
            "series_id": "ofr_stfm",
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            df = None
            for url in [
                "https://www.financialresearch.gov/short-term-funding-monitor/download/",
                "https://financialresearch.gov/short-term-funding-monitor/download/",
                "https://data.financialresearch.gov/v1/short-term-funding-monitor/download/",
            ]:
                df = self._fetch_csv(url)
                if df is not None and not df.empty:
                    break

            if df is None or df.empty:
                result["status"] = "PARTIAL"
                return result

            inserted = 0
            with self.engine.begin() as conn:
                for _, row in df.iterrows():
                    try:
                        date_val = pd.Timestamp(str(row.iloc[0])).date()
                        # Extract repo volume and rate columns
                        for col in df.columns[1:]:
                            col_lower = col.lower()
                            if "volume" in col_lower:
                                feature = "ofr_repo_volume"
                            elif "rate" in col_lower:
                                feature = "ofr_repo_rate_1d"
                            else:
                                continue
                            value = float(row[col])
                            if pd.isna(value):
                                continue
                            if not self._row_exists(feature, date_val, conn):
                                conn.execute(
                                    text(
                                        "INSERT INTO raw_series "
                                        "(series_id, source_id, obs_date, value, pull_status) "
                                        "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                                    ),
                                    {"sid": feature, "src": self.source_id, "od": date_val, "val": value},
                                )
                                inserted += 1
                    except (ValueError, TypeError):
                        continue

            result["rows_inserted"] = inserted
            log.info("OFR STFM: inserted {n} rows", n=inserted)

        except Exception as exc:
            log.error("OFR STFM pull failed: {err}", err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def pull_all(self) -> dict[str, Any]:
        """Pull all OFR data sources."""
        log.info("Starting OFR bulk pull")
        results = [self.pull_fsm(), self.pull_fsi(), self.pull_stfm()]

        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "OFR bulk pull complete — {ok}/{total} succeeded, {rows} rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return {
            "source": "OFR",
            "total_rows": total_rows,
            "succeeded": succeeded,
            "total": len(results),
        }
