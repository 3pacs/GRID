"""
GRID Redfin Housing Data ingestion module.

Pulls free weekly housing market data from Redfin's public S3 bucket:
- Median sale price, list price
- Homes sold, new listings, inventory
- Days on market

Covers national-level and top 20 metro areas.
Detects anomalies: flags metros where inventory jumped >30% month-over-month.

Series stored:
- redfin:{region}:{metric}  e.g. redfin:national:median_sale_price

Source: Redfin (public TSV on S3)
Schedule: Weekly
"""

from __future__ import annotations

import io
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure
from intelligence.actor_ingest import ingest_actor

# ── Configuration ────────────────────────────────────────────────────

_REQUEST_TIMEOUT: int = 60  # Large TSV file

_REDFIN_TSV_URL: str = (
    "https://redfin-public-data.s3.us-west-2.amazonaws.com/"
    "redfin_covid19/weekly_housing_market_data_most_recent.tsv"
)

# Metrics to extract from the TSV
METRICS: list[str] = [
    "median_sale_price",
    "inventory",
    "days_on_market",
    "homes_sold",
    "new_listings",
]

# Top 20 US metro areas by population (names as they appear in Redfin data)
TOP_METROS: list[str] = [
    "New York",
    "Los Angeles",
    "Chicago",
    "Dallas",
    "Houston",
    "Washington, D.C.",
    "Philadelphia",
    "Miami",
    "Atlanta",
    "Boston",
    "Phoenix",
    "San Francisco",
    "Riverside",
    "Detroit",
    "Seattle",
    "Minneapolis",
    "San Diego",
    "Tampa",
    "Denver",
    "St. Louis",
]

# Inventory anomaly threshold: >30% month-over-month jump
_INVENTORY_ANOMALY_THRESHOLD: float = 0.30


def _normalise_region(region: str) -> str:
    """Lowercase and strip a region name for series_id construction."""
    return (
        region.strip()
        .lower()
        .replace(" ", "_")
        .replace(",", "")
        .replace(".", "")
    )


class RedfinPuller(BasePuller):
    """Pulls Redfin weekly housing market data into raw_series.

    Downloads the public TSV, filters to national and top-20 metros,
    stores key housing metrics, and flags inventory anomalies.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for Redfin.
    """

    SOURCE_NAME: str = "Redfin"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://redfin-public-data.s3.us-west-2.amazonaws.com/",
        "cost_tier": "FREE",
        "latency_class": "WEEKLY",
        "pit_available": True,
        "revision_behavior": "FREQUENT",
        "trust_score": "HIGH",
        "priority_rank": 35,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info(
            "RedfinPuller initialised — source_id={sid}",
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
    def _fetch_tsv(self) -> pd.DataFrame:
        """Download and parse the Redfin weekly housing TSV.

        Returns:
            DataFrame with raw TSV data.

        Raises:
            requests.RequestException: On HTTP errors after retries.
        """
        headers = {
            "User-Agent": "GRID-DataPuller/1.0",
            "Accept": "text/tab-separated-values",
        }

        resp = requests.get(
            _REDFIN_TSV_URL,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()

        df = pd.read_csv(
            io.StringIO(resp.text),
            sep="\t",
            low_memory=False,
        )

        log.info(
            "Redfin TSV downloaded: {rows} rows, {cols} columns",
            rows=len(df),
            cols=len(df.columns),
        )
        return df

    # ------------------------------------------------------------------ #
    # Filtering
    # ------------------------------------------------------------------ #

    def _filter_regions(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter to national-level and top 20 metro areas.

        Identifies national rows and metro rows using the 'region'
        and 'region_type' (or similar) columns.

        Parameters:
            df: Raw Redfin DataFrame.

        Returns:
            Filtered DataFrame.
        """
        # Normalise column names to lowercase
        df.columns = [c.strip().lower() for c in df.columns]

        # Identify the region column (Redfin uses various names)
        region_col = None
        for candidate in ("region", "region_name", "metro_area"):
            if candidate in df.columns:
                region_col = candidate
                break

        if region_col is None:
            log.warning("Redfin: no region column found — using all rows")
            return df

        # Identify region_type column if present
        type_col = None
        for candidate in ("region_type", "region_type_id"):
            if candidate in df.columns:
                type_col = candidate
                break

        # Filter national rows
        national_mask = df[region_col].str.contains(
            "national|united states", case=False, na=False
        )
        if type_col is not None:
            national_mask = national_mask | df[type_col].str.contains(
                "national|country", case=False, na=False
            )

        # Filter metro rows — partial match on top metro names
        metro_pattern = "|".join(
            m.split(",")[0].strip() for m in TOP_METROS
        )
        metro_mask = df[region_col].str.contains(
            metro_pattern, case=False, na=False
        )

        filtered = df[national_mask | metro_mask].copy()

        log.info(
            "Redfin: filtered to {n} rows (national + top metros)",
            n=len(filtered),
        )
        return filtered

    # ------------------------------------------------------------------ #
    # Anomaly detection
    # ------------------------------------------------------------------ #

    def _detect_inventory_anomalies(
        self,
        df: pd.DataFrame,
    ) -> list[dict[str, Any]]:
        """Detect metros where inventory jumped >30% month-over-month.

        Parameters:
            df: Filtered DataFrame with date and region columns.

        Returns:
            List of anomaly dicts with region, date, pct_change.
        """
        anomalies: list[dict[str, Any]] = []

        df.columns = [c.strip().lower() for c in df.columns]

        # Identify columns
        region_col = None
        for candidate in ("region", "region_name", "metro_area"):
            if candidate in df.columns:
                region_col = candidate
                break

        date_col = None
        for candidate in ("period_begin", "period_end", "date"):
            if candidate in df.columns:
                date_col = candidate
                break

        if region_col is None or date_col is None or "inventory" not in df.columns:
            return anomalies

        df_work = df[[region_col, date_col, "inventory"]].copy()
        df_work["inventory"] = pd.to_numeric(df_work["inventory"], errors="coerce")
        df_work[date_col] = pd.to_datetime(df_work[date_col], errors="coerce")
        df_work = df_work.dropna(subset=["inventory", date_col])

        if df_work.empty:
            return anomalies

        # Sort and compute MoM change per region
        df_work = df_work.sort_values([region_col, date_col])

        for region, group in df_work.groupby(region_col):
            if len(group) < 2:
                continue

            group = group.sort_values(date_col)

            # Compare last two periods
            latest = group.iloc[-1]
            previous = group.iloc[-2]

            prev_inv = previous["inventory"]
            curr_inv = latest["inventory"]

            if prev_inv > 0:
                pct_change = (curr_inv - prev_inv) / prev_inv
                if pct_change > _INVENTORY_ANOMALY_THRESHOLD:
                    anomaly = {
                        "region": str(region),
                        "date": str(latest[date_col].date())
                        if hasattr(latest[date_col], "date")
                        else str(latest[date_col]),
                        "inventory_previous": float(prev_inv),
                        "inventory_current": float(curr_inv),
                        "pct_change": round(pct_change * 100, 2),
                        "signal": "housing_stress",
                    }
                    anomalies.append(anomaly)
                    log.warning(
                        "ANOMALY: {r} inventory jumped {p:.1f}% "
                        "({prev} -> {curr})",
                        r=region,
                        p=pct_change * 100,
                        prev=prev_inv,
                        curr=curr_inv,
                    )

        return anomalies

    # ------------------------------------------------------------------ #
    # Storage
    # ------------------------------------------------------------------ #

    def _store_metrics(
        self,
        df: pd.DataFrame,
        start_date: date,
        anomalies: list[dict[str, Any]],
    ) -> int:
        """Store filtered housing metrics into raw_series.

        Parameters:
            df: Filtered DataFrame.
            start_date: Earliest observation date.
            anomalies: Detected anomalies to attach to payloads.

        Returns:
            Number of rows inserted.
        """
        df.columns = [c.strip().lower() for c in df.columns]

        # Identify columns
        region_col = None
        for candidate in ("region", "region_name", "metro_area"):
            if candidate in df.columns:
                region_col = candidate
                break

        date_col = None
        for candidate in ("period_begin", "period_end", "date"):
            if candidate in df.columns:
                date_col = candidate
                break

        if region_col is None or date_col is None:
            log.error("Redfin: missing region or date column")
            return 0

        # Build anomaly lookup
        anomaly_regions = {a["region"] for a in anomalies}

        total_inserted = 0

        # Convert dates
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                obs_dt = row[date_col]
                if pd.isna(obs_dt):
                    continue
                obs_date = obs_dt.date() if hasattr(obs_dt, "date") else obs_dt

                if obs_date < start_date:
                    continue

                region_raw = str(row[region_col]).strip()
                region_norm = _normalise_region(region_raw)

                # Use "national" for national-level data
                if "national" in region_raw.lower() or "united states" in region_raw.lower():
                    region_norm = "national"

                for metric in METRICS:
                    if metric not in df.columns:
                        continue

                    val = row.get(metric)
                    if pd.isna(val):
                        continue

                    try:
                        val_float = float(val)
                    except (ValueError, TypeError):
                        continue

                    series_id = f"redfin:{region_norm}:{metric}"

                    if self._row_exists(series_id, obs_date, conn):
                        continue

                    payload: dict[str, Any] = {
                        "region": region_raw,
                        "metric": metric,
                        "source": "Redfin",
                    }

                    # Tag anomalous regions
                    if metric == "inventory" and region_raw in anomaly_regions:
                        payload["anomaly"] = "housing_stress"
                        payload["anomaly_details"] = next(
                            (a for a in anomalies if a["region"] == region_raw),
                            None,
                        )

                    self._insert_raw(
                        conn=conn,
                        series_id=series_id,
                        obs_date=obs_date,
                        value=val_float,
                        raw_payload=payload,
                    )
                    total_inserted += 1

                # Ingest region as an actor (geographic entity)
                if region_norm != "national":
                    ingest_actor(
                        self.engine,
                        name=region_raw,
                        actor_type="entity",
                        source="redfin",
                        country="US",
                        confidence="confirmed",
                        metadata={"type": "metro_area"},
                    )

        return total_inserted

    # ------------------------------------------------------------------ #
    # Main pull
    # ------------------------------------------------------------------ #

    def pull_all(
        self,
        start_date: str | date = "2020-01-01",
    ) -> dict[str, Any]:
        """Pull all Redfin housing data.

        Parameters:
            start_date: Earliest observation date (str or date).

        Returns:
            Result dict with status, rows_inserted, anomalies.
        """
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)

        log.info(
            "Starting Redfin housing pull from {sd}",
            sd=start_date,
        )

        try:
            df = self._fetch_tsv()
        except Exception as exc:
            log.error("Redfin TSV download failed: {e}", e=str(exc))
            return {
                "source": "redfin",
                "status": "FAILED",
                "rows_inserted": 0,
                "error": str(exc),
            }

        if df.empty:
            return {
                "source": "redfin",
                "status": "PARTIAL",
                "rows_inserted": 0,
                "errors": ["Empty TSV"],
            }

        # Filter to national + top metros
        filtered = self._filter_regions(df)

        if filtered.empty:
            log.warning("Redfin: no matching regions after filter")
            return {
                "source": "redfin",
                "status": "PARTIAL",
                "rows_inserted": 0,
                "errors": ["No matching regions"],
            }

        # Detect inventory anomalies
        anomalies = self._detect_inventory_anomalies(filtered)

        # Store metrics
        rows_inserted = self._store_metrics(filtered, start_date, anomalies)

        # Ingest Redfin as a known data actor
        ingest_actor(
            self.engine,
            name="Redfin",
            actor_type="company",
            source="redfin",
            country="US",
            confidence="confirmed",
            metadata={"type": "real_estate_data_provider"},
        )

        log.info(
            "Redfin pull complete — {n} rows inserted, {a} anomalies",
            n=rows_inserted,
            a=len(anomalies),
        )

        return {
            "source": "redfin",
            "status": "SUCCESS" if rows_inserted > 0 else "PARTIAL",
            "rows_inserted": rows_inserted,
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
        }


if __name__ == "__main__":
    from db import get_engine

    puller = RedfinPuller(db_engine=get_engine())
    result = puller.pull_all(start_date="2020-01-01")
    print(
        f"Redfin: {result['status']} — {result['rows_inserted']} rows, "
        f"{result.get('anomaly_count', 0)} anomalies"
    )
