"""
GRID Indeed Hiring Lab ingestion module.

Pulls free job postings data from Indeed Hiring Lab's public GitHub:
- Aggregate US job postings index (indexed to Feb 1, 2020 = 100)
- Sector-level breakdowns

Detects anomalies: flags when aggregate index drops >10% month-over-month
(hiring freeze signal).

Series stored:
- indeed:us:aggregate_postings
- indeed:us:sector:{sector_name}

Source: Indeed Hiring Lab (GitHub CSV)
Schedule: Weekly
"""

from __future__ import annotations

import io
from datetime import date, datetime
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure
from intelligence.actor_ingest import ingest_actor

# ── Configuration ────────────────────────────────────────────────────

_REQUEST_TIMEOUT: int = 30

_AGGREGATE_URL: str = (
    "https://raw.githubusercontent.com/hiring-lab/"
    "hiring-lab-data-library/main/US/aggregate_job_postings_US.csv"
)

_SECTOR_URL: str = (
    "https://raw.githubusercontent.com/hiring-lab/"
    "hiring-lab-data-library/main/US/job_postings_by_sector_US.csv"
)

# Anomaly threshold: >10% MoM drop in aggregate index
_HIRING_FREEZE_THRESHOLD: float = -0.10


def _normalise_sector(sector: str) -> str:
    """Lowercase and clean a sector name for series_id construction."""
    return (
        sector.strip()
        .lower()
        .replace(" ", "_")
        .replace(",", "")
        .replace("&", "and")
        .replace("/", "_")
        .replace("(", "")
        .replace(")", "")
        .replace("'", "")
    )


class IndeedHiringPuller(BasePuller):
    """Pulls Indeed Hiring Lab job postings data into raw_series.

    Downloads aggregate and sector-level CSV files from the Hiring
    Lab public GitHub repository. Stores index values and detects
    hiring freeze signals.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for Indeed_Hiring_Lab.
    """

    SOURCE_NAME: str = "Indeed_Hiring_Lab"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://github.com/hiring-lab/hiring-lab-data-library",
        "cost_tier": "FREE",
        "latency_class": "WEEKLY",
        "pit_available": True,
        "revision_behavior": "FREQUENT",
        "trust_score": "HIGH",
        "priority_rank": 36,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info(
            "IndeedHiringPuller initialised — source_id={sid}",
            sid=self.source_id,
        )

    # ------------------------------------------------------------------ #
    # Data fetch
    # ------------------------------------------------------------------ #

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.RequestException,
        ),
    )
    def _fetch_csv(self, url: str) -> pd.DataFrame:
        """Download and parse a CSV from the Hiring Lab GitHub repo.

        Parameters:
            url: URL of the CSV file.

        Returns:
            DataFrame with parsed CSV data.

        Raises:
            requests.RequestException: On HTTP errors after retries.
        """
        headers = {
            "User-Agent": "GRID-DataPuller/1.0",
            "Accept": "text/csv",
        }

        resp = requests.get(url, headers=headers, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()

        df = pd.read_csv(io.StringIO(resp.text))

        log.info(
            "Indeed CSV downloaded from {u}: {rows} rows, {cols} columns",
            u=url.split("/")[-1],
            rows=len(df),
            cols=len(df.columns),
        )
        return df

    # ------------------------------------------------------------------ #
    # Aggregate index
    # ------------------------------------------------------------------ #

    def _pull_aggregate(
        self,
        start_date: date,
    ) -> dict[str, Any]:
        """Pull aggregate US job postings index.

        Parameters:
            start_date: Earliest observation date.

        Returns:
            Result dict with status and rows_inserted.
        """
        try:
            df = self._fetch_csv(_AGGREGATE_URL)
        except Exception as exc:
            log.error("Indeed aggregate CSV download failed: {e}", e=str(exc))
            return {
                "source": "indeed_aggregate",
                "status": "FAILED",
                "rows_inserted": 0,
                "error": str(exc),
            }

        if df.empty:
            return {
                "source": "indeed_aggregate",
                "status": "PARTIAL",
                "rows_inserted": 0,
                "errors": ["Empty CSV"],
            }

        # Normalise column names
        df.columns = [c.strip().lower() for c in df.columns]

        # Identify date column
        date_col = None
        for candidate in ("date", "day", "period"):
            if candidate in df.columns:
                date_col = candidate
                break

        # Identify value column
        value_col = None
        for candidate in (
            "indeed_job_postings_index",
            "job_postings_index",
            "index",
            "value",
        ):
            if candidate in df.columns:
                value_col = candidate
                break

        if date_col is None or value_col is None:
            # Fall back: use first column as date, second as value
            if len(df.columns) >= 2:
                date_col = df.columns[0]
                value_col = df.columns[1]
            else:
                return {
                    "source": "indeed_aggregate",
                    "status": "FAILED",
                    "rows_inserted": 0,
                    "errors": [f"Cannot identify columns: {list(df.columns)}"],
                }

        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
        coerced_count = df[value_col].isna().sum()
        if coerced_count > 0:
            log.warning(
                "Coerced {n} non-numeric values to NaN for indeed aggregate",
                n=int(coerced_count),
            )
        df = df.dropna(subset=[date_col, value_col])

        series_id = "indeed:us:aggregate_postings"
        inserted = 0
        anomalies: list[dict[str, Any]] = []

        df = df.sort_values(date_col)

        with self.engine.begin() as conn:
            existing = self._get_existing_dates(series_id, conn)
            prev_value: float | None = None

            for _, row in df.iterrows():
                obs_date = row[date_col].date()
                if obs_date < start_date or obs_date in existing:
                    prev_value = float(row[value_col])
                    continue

                val = float(row[value_col])

                # Anomaly detection: >10% MoM drop
                anomaly_payload: dict[str, Any] | None = None
                if prev_value is not None and prev_value > 0:
                    pct_change = (val - prev_value) / prev_value
                    if pct_change < _HIRING_FREEZE_THRESHOLD:
                        anomaly_payload = {
                            "signal": "hiring_freeze",
                            "pct_change": round(pct_change * 100, 2),
                            "previous_value": prev_value,
                        }
                        anomalies.append({
                            "date": str(obs_date),
                            "value": val,
                            "previous_value": prev_value,
                            "pct_change": round(pct_change * 100, 2),
                            "signal": "hiring_freeze",
                        })
                        log.warning(
                            "ANOMALY: Indeed aggregate dropped {p:.1f}% "
                            "({prev} -> {curr}) on {d}",
                            p=pct_change * 100,
                            prev=prev_value,
                            curr=val,
                            d=obs_date,
                        )

                payload: dict[str, Any] = {
                    "source": "Indeed_Hiring_Lab",
                    "metric": "aggregate_job_postings_index",
                    "base_date": "2020-02-01",
                    "base_value": 100,
                }
                if anomaly_payload:
                    payload["anomaly"] = anomaly_payload

                self._insert_raw(
                    conn=conn,
                    series_id=series_id,
                    obs_date=obs_date,
                    value=val,
                    raw_payload=payload,
                )
                inserted += 1
                prev_value = val

        log.info(
            "Indeed aggregate: {n} rows inserted, {a} anomalies",
            n=inserted,
            a=len(anomalies),
        )
        return {
            "source": "indeed_aggregate",
            "status": "SUCCESS" if inserted > 0 else "PARTIAL",
            "rows_inserted": inserted,
            "anomalies": anomalies,
        }

    # ------------------------------------------------------------------ #
    # Sector breakdowns
    # ------------------------------------------------------------------ #

    def _pull_sectors(
        self,
        start_date: date,
    ) -> dict[str, Any]:
        """Pull sector-level job postings data.

        Parameters:
            start_date: Earliest observation date.

        Returns:
            Result dict with status and rows_inserted.
        """
        try:
            df = self._fetch_csv(_SECTOR_URL)
        except Exception as exc:
            log.error("Indeed sector CSV download failed: {e}", e=str(exc))
            return {
                "source": "indeed_sectors",
                "status": "FAILED",
                "rows_inserted": 0,
                "error": str(exc),
            }

        if df.empty:
            return {
                "source": "indeed_sectors",
                "status": "PARTIAL",
                "rows_inserted": 0,
                "errors": ["Empty CSV"],
            }

        # Normalise column names
        df.columns = [c.strip().lower() for c in df.columns]

        # Identify date column
        date_col = None
        for candidate in ("date", "day", "period"):
            if candidate in df.columns:
                date_col = candidate
                break

        if date_col is None:
            date_col = df.columns[0]

        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=[date_col])

        # All non-date columns are sector data
        sector_cols = [c for c in df.columns if c != date_col]

        if not sector_cols:
            return {
                "source": "indeed_sectors",
                "status": "FAILED",
                "rows_inserted": 0,
                "errors": ["No sector columns found"],
            }

        total_inserted = 0
        sectors_processed: list[str] = []

        df = df.sort_values(date_col)

        with self.engine.begin() as conn:
            for sector_col in sector_cols:
                sector_norm = _normalise_sector(sector_col)
                series_id = f"indeed:us:sector:{sector_norm}"

                df[sector_col] = pd.to_numeric(df[sector_col], errors="coerce")
                sector_data = df.dropna(subset=[sector_col])

                if sector_data.empty:
                    continue

                existing = self._get_existing_dates(series_id, conn)
                inserted = 0

                for _, row in sector_data.iterrows():
                    obs_date = row[date_col].date()
                    if obs_date < start_date or obs_date in existing:
                        continue

                    val = float(row[sector_col])

                    self._insert_raw(
                        conn=conn,
                        series_id=series_id,
                        obs_date=obs_date,
                        value=val,
                        raw_payload={
                            "source": "Indeed_Hiring_Lab",
                            "sector": sector_col,
                            "metric": "job_postings_index",
                            "base_date": "2020-02-01",
                            "base_value": 100,
                        },
                    )
                    inserted += 1

                total_inserted += inserted
                sectors_processed.append(sector_col)

                # Ingest sector as an actor
                ingest_actor(
                    self.engine,
                    name=sector_col,
                    actor_type="entity",
                    source="indeed_hiring_lab",
                    country="US",
                    confidence="confirmed",
                    metadata={"type": "employment_sector"},
                )

        log.info(
            "Indeed sectors: {n} rows inserted across {s} sectors",
            n=total_inserted,
            s=len(sectors_processed),
        )
        return {
            "source": "indeed_sectors",
            "status": "SUCCESS" if total_inserted > 0 else "PARTIAL",
            "rows_inserted": total_inserted,
            "sectors": sectors_processed,
        }

    # ------------------------------------------------------------------ #
    # Main pull
    # ------------------------------------------------------------------ #

    def pull_all(
        self,
        start_date: str | date = "2020-01-01",
    ) -> list[dict[str, Any]]:
        """Pull all Indeed Hiring Lab data.

        Never stops on a single-source failure -- logs and continues.

        Parameters:
            start_date: Earliest observation date (str or date).

        Returns:
            List of per-source result dicts.
        """
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)

        log.info(
            "Starting Indeed Hiring Lab pull from {sd}",
            sd=start_date,
        )

        results: list[dict[str, Any]] = []

        # 1. Aggregate index
        try:
            agg_result = self._pull_aggregate(start_date)
            results.append(agg_result)
        except Exception as exc:
            log.error("Indeed aggregate pull failed: {e}", e=str(exc))
            results.append({
                "source": "indeed_aggregate",
                "status": "FAILED",
                "error": str(exc),
            })

        # 2. Sector breakdowns
        try:
            sector_result = self._pull_sectors(start_date)
            results.append(sector_result)
        except Exception as exc:
            log.error("Indeed sector pull failed: {e}", e=str(exc))
            results.append({
                "source": "indeed_sectors",
                "status": "FAILED",
                "error": str(exc),
            })

        # Ingest Indeed as a known data actor
        ingest_actor(
            self.engine,
            name="Indeed",
            actor_type="company",
            source="indeed_hiring_lab",
            country="US",
            confidence="confirmed",
            metadata={"type": "employment_data_provider"},
        )

        total_inserted = sum(r.get("rows_inserted", 0) for r in results)
        succeeded = sum(1 for r in results if r.get("status") == "SUCCESS")
        log.info(
            "Indeed Hiring Lab pull complete — {ok}/{total} sources succeeded, "
            "{n} total rows inserted",
            ok=succeeded,
            total=len(results),
            n=total_inserted,
        )

        return results


if __name__ == "__main__":
    from db import get_engine

    puller = IndeedHiringPuller(db_engine=get_engine())
    results = puller.pull_all(start_date="2020-01-01")
    for r in results:
        print(
            f"  {r.get('source', '?')}: {r.get('status')} — "
            f"{r.get('rows_inserted', 0)} rows"
        )
