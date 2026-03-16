"""
GRID Opportunity Insights Economic Tracker ingestion module.

Pulls high-frequency US economic indicators from the Opportunity Insights
Economic Tracker GitHub repository. Covers consumer spending by income
quartile, employment, and K-shape recovery metrics.
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

# OI file definitions: (filename, date_col, value_col, feature)
# Note: multiple features from same file handled via list
OI_FILES: list[dict[str, str]] = [
    {
        "filename": "Affinity - National - Daily.csv",
        "date_col": "date",
        "value_col": "spend_all",
        "feature": "oi_consumer_spend",
    },
    {
        "filename": "Affinity - National - Daily.csv",
        "date_col": "date",
        "value_col": "spend_all_q1",
        "feature": "oi_spend_low_income",
    },
    {
        "filename": "Affinity - National - Daily.csv",
        "date_col": "date",
        "value_col": "spend_all_q4",
        "feature": "oi_spend_high_income",
    },
    {
        "filename": "Employment - National - Daily.csv",
        "date_col": "date",
        "value_col": "emp_combined",
        "feature": "oi_employment_overall",
    },
]

_OI_BASE_URL = (
    "https://raw.githubusercontent.com/OpportunityInsights/EconomicTracker/main/data/"
)
_RATE_LIMIT_DELAY: float = 1.0


class OppInsightsPuller:
    """Pulls high-frequency economic data from Opportunity Insights."""

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.source_id = self._resolve_source_id()
        log.info("OppInsightsPuller initialised — source_id={sid}", sid=self.source_id)

    def _resolve_source_id(self) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": "OppInsights"},
            ).fetchone()
        if row is None:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text(
                        "INSERT INTO source_catalog "
                        "(name, base_url, license_type, update_frequency, "
                        "has_vintage_data, revision_policy, data_quality, priority, model_eligible) "
                        "VALUES (:name, :url, 'FREE', 'WEEKLY', FALSE, 'RARE', 'HIGH', 34, TRUE) "
                        "RETURNING id"
                    ),
                    {"name": "OppInsights", "url": _OI_BASE_URL},
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

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
    def _download_csv(self, filename: str) -> pd.DataFrame | None:
        """Download CSV file from OI GitHub repository."""
        url = f"{_OI_BASE_URL}{filename}"
        resp = requests.get(url, timeout=30)
        if resp.status_code != 200:
            return None
        from io import StringIO
        return pd.read_csv(StringIO(resp.text))

    def _parse_oi_date(self, row: pd.Series, date_col: str) -> date | None:
        """Parse OI date format.

        Some OI files use year/month/day as separate columns, others use
        a combined date column.
        """
        try:
            if date_col in row.index:
                return pd.Timestamp(str(row[date_col])).date()
        except (ValueError, TypeError):
            pass

        # Try separate year/month/day columns
        try:
            year = int(row.get("year", 0))
            month = int(row.get("month", 0))
            day = int(row.get("day", 1))
            if year and month:
                return date(year, month, day)
        except (ValueError, TypeError):
            pass

        return None

    def pull_file(
        self,
        filename: str,
        date_col: str,
        value_col: str,
        feature_name: str,
    ) -> dict[str, Any]:
        """Pull a single OI file and extract the specified column."""
        log.info("Pulling OI {fn} from {f}", fn=feature_name, f=filename)
        result: dict[str, Any] = {
            "series_id": feature_name,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            df = self._download_csv(filename)
            if df is None or df.empty:
                result["status"] = "PARTIAL"
                result["errors"].append(f"File {filename} not available")
                return result

            if value_col not in df.columns:
                result["status"] = "PARTIAL"
                result["errors"].append(f"Column {value_col} not found in {filename}")
                return result

            inserted = 0
            with self.engine.begin() as conn:
                for _, row in df.iterrows():
                    try:
                        obs_dt = self._parse_oi_date(row, date_col)
                        if obs_dt is None:
                            continue

                        val = row.get(value_col)
                        if val is None or pd.isna(val):
                            continue
                        value = float(val)

                        if self._row_exists(feature_name, obs_dt, conn):
                            continue

                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, pull_status) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                            ),
                            {"sid": feature_name, "src": self.source_id, "od": obs_dt, "val": value},
                        )
                        inserted += 1
                    except (ValueError, TypeError) as row_exc:
                        log.debug("Skipping OI row: {err}", err=str(row_exc))

            result["rows_inserted"] = inserted
            log.info("OI {fn}: inserted {n} rows", fn=feature_name, n=inserted)

        except Exception as exc:
            log.error("OI pull failed for {fn}: {err}", fn=feature_name, err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def compute_k_shape(self) -> dict[str, Any]:
        """Compute K-shape divergence: high minus low income spend."""
        result: dict[str, Any] = {"rows_inserted": 0, "status": "SUCCESS", "errors": []}

        try:
            with self.engine.begin() as conn:
                q4_rows = conn.execute(
                    text(
                        "SELECT obs_date, value FROM raw_series "
                        "WHERE series_id = 'oi_spend_high_income' AND source_id = :src "
                        "AND pull_status = 'SUCCESS' ORDER BY obs_date"
                    ),
                    {"src": self.source_id},
                ).fetchall()

                q1_rows = conn.execute(
                    text(
                        "SELECT obs_date, value FROM raw_series "
                        "WHERE series_id = 'oi_spend_low_income' AND source_id = :src "
                        "AND pull_status = 'SUCCESS' ORDER BY obs_date"
                    ),
                    {"src": self.source_id},
                ).fetchall()

                q4_dict = {r[0]: r[1] for r in q4_rows}
                q1_dict = {r[0]: r[1] for r in q1_rows}

                common_dates = set(q4_dict.keys()) & set(q1_dict.keys())
                inserted = 0

                for obs_dt in sorted(common_dates):
                    k_shape = q4_dict[obs_dt] - q1_dict[obs_dt]
                    if not self._row_exists("oi_k_shape_ratio", obs_dt, conn):
                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, pull_status) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                            ),
                            {"sid": "oi_k_shape_ratio", "src": self.source_id, "od": obs_dt, "val": k_shape},
                        )
                        inserted += 1

                result["rows_inserted"] = inserted
                log.info("K-shape ratio: inserted {n} rows", n=inserted)

        except Exception as exc:
            log.error("K-shape computation failed: {err}", err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def pull_all(self) -> dict[str, Any]:
        """Pull all OI files and compute K-shape ratio."""
        log.info("Starting Opportunity Insights pull")
        results: list[dict[str, Any]] = []

        # Track which files we've already downloaded (avoid duplicate downloads)
        downloaded_files: dict[str, pd.DataFrame] = {}

        for file_def in OI_FILES:
            res = self.pull_file(
                file_def["filename"],
                file_def["date_col"],
                file_def["value_col"],
                file_def["feature"],
            )
            results.append(res)
            time.sleep(_RATE_LIMIT_DELAY)

        # Compute K-shape after pulling Q1 and Q4 spending
        k_result = self.compute_k_shape()
        results.append(k_result)

        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "OI pull complete — {ok}/{total} succeeded, {rows} rows",
            ok=succeeded, total=len(results), rows=total_rows,
        )
        return {
            "source": "OppInsights",
            "total_rows": total_rows,
            "succeeded": succeeded,
            "total": len(results),
        }
