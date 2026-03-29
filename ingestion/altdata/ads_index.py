"""
GRID ADS Business Conditions Index ingestion module.

Pulls the Aruoba-Diebold-Scotti (ADS) Business Conditions Index from the
Federal Reserve Bank of Philadelphia. The ADS index is designed to track
real business conditions at high frequency -- it is updated daily and
incorporates six underlying economic indicators (weekly initial jobless
claims, monthly payroll employment, industrial production, real personal
income less transfers, real manufacturing and trade sales, and quarterly
real GDP).

The index value is normalised so that 0 represents average economic
conditions, negative values indicate below-average, and positive values
above-average. Large negative spikes correspond to recessions.

Historical daily data is available back to 1960.

Data source: https://www.philadelphiafed.org/surveys-and-data/real-time-data-research/ads
Direct download: https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/ads/ads_index_most_current_vintage.xlsx

Series produced:
    ads.business_conditions_index   ADS Business Conditions Index (daily)
"""

from __future__ import annotations

import io
import math
from datetime import date, datetime
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_REQUEST_TIMEOUT: int = 60  # Generous timeout for large Excel download
_USER_AGENT: str = "GRID-Ingestion/1.0 (research; +https://grid.local)"

# Philadelphia Fed ADS Index download URL (Excel, most current vintage)
_ADS_XLSX_URL: str = (
    "https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/"
    "ads/ads_index_most_current_vintage.xlsx"
)

# Fallback CSV URL in case the Excel endpoint changes
_ADS_CSV_URL: str = (
    "https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/"
    "ads/ads_index_most_current_vintage.csv"
)

# Series ID
_SERIES_ID: str = "ads.business_conditions_index"

# Series ID prefix
_PFX: str = "ads"

# All series managed by this puller
ALL_SERIES: list[str] = [_SERIES_ID]


class ADSIndexPuller(BasePuller):
    """Pulls the ADS Business Conditions Index from the Philadelphia Fed.

    The Aruoba-Diebold-Scotti index tracks real business conditions at
    daily frequency, combining six economic indicators into a single
    composite. The index is normalised to zero-mean so that negative
    values represent below-average conditions and positive values
    above-average conditions.

    Historical data extends back to March 1960 and is updated daily.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for this puller.
    """

    SOURCE_NAME: str = "ADS_Index"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://www.philadelphiafed.org/surveys-and-data/real-time-data-research/ads",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "FREQUENT",
        "trust_score": "HIGH",
        "priority_rank": 20,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the ADS Business Conditions Index puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": _USER_AGENT})
        log.info(
            "ADSIndexPuller initialised -- source_id={sid}",
            sid=self.source_id,
        )

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.exceptions.RequestException,
        ),
    )
    def _fetch_xlsx(self) -> bytes:
        """Download the ADS Index Excel file from the Philadelphia Fed.

        Returns:
            Raw Excel file bytes.

        Raises:
            requests.exceptions.HTTPError: On non-2xx response.
        """
        resp = self._session.get(_ADS_XLSX_URL, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.content

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.exceptions.RequestException,
        ),
    )
    def _fetch_csv(self) -> str:
        """Download the ADS Index CSV file as fallback.

        Returns:
            CSV text content.

        Raises:
            requests.exceptions.HTTPError: On non-2xx response.
        """
        resp = self._session.get(_ADS_CSV_URL, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.text

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_xlsx(self, raw_bytes: bytes) -> list[tuple[date, float]]:
        """Parse the ADS Index Excel file into (date, value) pairs.

        The Excel file typically has two columns: a date column and the
        ADS index value. Column names vary across vintages, so we use
        position-based detection with fallback to name matching.

        Parameters:
            raw_bytes: Raw Excel file bytes.

        Returns:
            List of (observation_date, ads_value) tuples with valid data.
        """
        import pandas as pd

        xls = pd.ExcelFile(io.BytesIO(raw_bytes))

        # Try common sheet names
        sheet_name: str | None = None
        for candidate in ("ADS Index", "ADS_Index", "ADS", "Sheet1", xls.sheet_names[0]):
            if candidate in xls.sheet_names:
                sheet_name = candidate
                break
        if sheet_name is None:
            sheet_name = xls.sheet_names[0]

        df = pd.read_excel(xls, sheet_name=sheet_name)
        log.debug(
            "ADS Excel sheet '{s}' -- {r} rows, columns: {c}",
            s=sheet_name,
            r=len(df),
            c=list(df.columns),
        )

        # Normalise column names
        df.columns = [str(c).strip().lower() for c in df.columns]

        # Identify date column
        date_col: str | None = None
        for col in ("date", "observation date", "obs_date", "period"):
            if col in df.columns:
                date_col = col
                break
        if date_col is None:
            # Fall back to first column (usually the date)
            date_col = df.columns[0]

        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=[date_col])

        # Identify value column
        value_col: str | None = None
        for col in df.columns:
            if col == date_col:
                continue
            col_lower = col.lower()
            if any(kw in col_lower for kw in ("ads", "index", "value", "business")):
                value_col = col
                break
        if value_col is None:
            # Fall back to first numeric column that is not the date
            numeric_cols = [
                c for c in df.columns
                if c != date_col and pd.api.types.is_numeric_dtype(df[c])
            ]
            if numeric_cols:
                value_col = numeric_cols[0]
            else:
                log.error("ADS: no numeric value column found")
                return []

        log.debug(
            "ADS: using date_col='{d}', value_col='{v}'",
            d=date_col,
            v=value_col,
        )

        records: list[tuple[date, float]] = []
        for _, row in df.iterrows():
            obs_date = row[date_col].date()
            val = row[value_col]

            if pd.isna(val):
                continue

            try:
                fval = float(val)
            except (ValueError, TypeError):
                log.warning(
                    "ADS: non-numeric value on {d}: {v}", d=obs_date, v=val
                )
                continue

            if math.isnan(fval) or math.isinf(fval):
                log.warning(
                    "ADS: NaN/inf value on {d}, skipping", d=obs_date
                )
                continue

            records.append((obs_date, fval))

        log.info("ADS: parsed {n} valid records from Excel", n=len(records))
        return records

    def _parse_csv(self, csv_text: str) -> list[tuple[date, float]]:
        """Parse the ADS Index CSV file as fallback.

        Parameters:
            csv_text: Raw CSV text content.

        Returns:
            List of (observation_date, ads_value) tuples with valid data.
        """
        import pandas as pd

        df = pd.read_csv(io.StringIO(csv_text))
        log.debug(
            "ADS CSV -- {r} rows, columns: {c}",
            r=len(df),
            c=list(df.columns),
        )

        df.columns = [str(c).strip().lower() for c in df.columns]

        # Identify date column
        date_col: str | None = None
        for col in ("date", "observation date", "obs_date", "period"):
            if col in df.columns:
                date_col = col
                break
        if date_col is None:
            date_col = df.columns[0]

        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=[date_col])

        # Identify value column
        value_col: str | None = None
        for col in df.columns:
            if col == date_col:
                continue
            if any(kw in col.lower() for kw in ("ads", "index", "value", "business")):
                value_col = col
                break
        if value_col is None:
            numeric_cols = [
                c for c in df.columns
                if c != date_col and pd.api.types.is_numeric_dtype(df[c])
            ]
            if numeric_cols:
                value_col = numeric_cols[0]
            else:
                log.error("ADS CSV: no numeric value column found")
                return []

        records: list[tuple[date, float]] = []
        for _, row in df.iterrows():
            obs_date = row[date_col].date()
            val = row[value_col]
            if pd.isna(val):
                continue
            try:
                fval = float(val)
            except (ValueError, TypeError):
                continue
            if math.isnan(fval) or math.isinf(fval):
                continue
            records.append((obs_date, fval))

        log.info("ADS CSV: parsed {n} valid records", n=len(records))
        return records

    # ------------------------------------------------------------------
    # Pull methods
    # ------------------------------------------------------------------

    def pull_ads_index(
        self,
        start_date: str | date = "1960-01-01",
    ) -> dict[str, Any]:
        """Pull the ADS Business Conditions Index.

        Downloads the latest vintage from the Philadelphia Fed, parses
        the data, and stores new observations using batch deduplication
        via _get_existing_dates(). Supports incremental updates using
        _get_latest_date() to skip already-stored history.

        Parameters:
            start_date: Earliest observation date to store. Defaults to
                1960-01-01 to capture full history.

        Returns:
            Result dict with status, rows_inserted, total_parsed, and
            latest_date fields.
        """
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)

        # Check latest stored date for incremental efficiency
        latest_stored = self._get_latest_date(_SERIES_ID)
        if latest_stored is not None:
            # Only process records after the latest stored date
            effective_start = max(start_date, latest_stored)
            log.info(
                "ADS: incremental pull from {d} (latest stored: {ls})",
                d=effective_start,
                ls=latest_stored,
            )
        else:
            effective_start = start_date
            log.info("ADS: full history pull from {d}", d=effective_start)

        # Fetch and parse data -- try Excel first, fall back to CSV
        records: list[tuple[date, float]] = []
        try:
            log.info("ADS: downloading Excel from Philadelphia Fed...")
            raw_bytes = self._fetch_xlsx()
            records = self._parse_xlsx(raw_bytes)
        except Exception as exc:
            log.warning(
                "ADS: Excel download/parse failed ({e}), trying CSV fallback",
                e=str(exc),
            )
            try:
                csv_text = self._fetch_csv()
                records = self._parse_csv(csv_text)
            except Exception as csv_exc:
                log.error(
                    "ADS: both Excel and CSV downloads failed: {e}",
                    e=str(csv_exc),
                )
                return {
                    "feature": _SERIES_ID,
                    "status": "FAILED",
                    "rows_inserted": 0,
                    "error": f"Excel: {exc}; CSV: {csv_exc}",
                }

        if not records:
            log.warning("ADS: no valid records parsed")
            return {
                "feature": _SERIES_ID,
                "status": "NO_DATA",
                "rows_inserted": 0,
                "total_parsed": 0,
            }

        # Filter to records after effective_start
        records = [
            (d, v) for d, v in records if d >= effective_start
        ]

        if not records:
            log.info("ADS: no new records after {d}", d=effective_start)
            return {
                "feature": _SERIES_ID,
                "status": "SUCCESS",
                "rows_inserted": 0,
                "total_parsed": 0,
                "latest_date": str(latest_stored) if latest_stored else None,
            }

        # Batch dedup and insert
        rows_inserted = 0
        with self.engine.begin() as conn:
            existing_dates = self._get_existing_dates(_SERIES_ID, conn)
            log.debug(
                "ADS: {n} existing dates in DB, {m} candidate records",
                n=len(existing_dates),
                m=len(records),
            )

            for obs_date, value in records:
                if obs_date in existing_dates:
                    continue
                self._insert_raw(
                    conn=conn,
                    series_id=_SERIES_ID,
                    obs_date=obs_date,
                    value=value,
                    raw_payload={
                        "source": "Philadelphia Fed ADS Index",
                        "vintage": "most_current",
                    },
                )
                rows_inserted += 1

        latest_record_date = max(d for d, _ in records)
        log.info(
            "ADS pull complete -- {n} new rows inserted (latest: {d})",
            n=rows_inserted,
            d=latest_record_date,
        )

        return {
            "feature": _SERIES_ID,
            "status": "SUCCESS",
            "rows_inserted": rows_inserted,
            "total_parsed": len(records),
            "latest_date": str(latest_record_date),
        }

    def pull_all(
        self,
        start_date: str | date = "1960-01-01",
    ) -> list[dict[str, Any]]:
        """Pull all ADS Index series.

        Convenience method matching the pull_all() pattern used by
        other GRID pullers.

        Parameters:
            start_date: Earliest observation date to store.

        Returns:
            List containing the single result dict.
        """
        result = self.pull_ads_index(start_date=start_date)
        return [result]
