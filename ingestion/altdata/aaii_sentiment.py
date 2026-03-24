"""
GRID AAII Sentiment Survey ingestion module.

Pulls the weekly AAII Investor Sentiment Survey data, which tracks
the percentage of individual investors who are bullish, bearish, or
neutral on the stock market over the next six months.

Data source: AAII Sentiment Survey public CSV export.
Published weekly (typically Thursday). History available from 1987.

Series stored:
- aaii.bullish_pct: Percentage of investors reporting bullish sentiment
- aaii.bearish_pct: Percentage of investors reporting bearish sentiment
- aaii.neutral_pct: Percentage of investors reporting neutral sentiment
- aaii.bull_bear_spread: Bullish minus bearish (contrarian signal)
"""

from __future__ import annotations

import io
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# AAII sentiment survey CSV URL (public data export)
_AAII_CSV_URL: str = "https://www.aaii.com/files/surveys/sentiment.xls"

# Alternative URL if the primary is blocked or unavailable
_AAII_ALT_URL: str = "https://www.aaii.com/sentimentsurvey/sent_results"

# Series ID prefix for all AAII sentiment features
_SERIES_PREFIX: str = "aaii"

# Feature definitions: series_id suffix -> description
AAII_FEATURES: dict[str, str] = {
    "bullish_pct": "AAII % bullish (6-month stock market outlook)",
    "bearish_pct": "AAII % bearish (6-month stock market outlook)",
    "neutral_pct": "AAII % neutral (6-month stock market outlook)",
    "bull_bear_spread": "AAII bull-bear spread (bullish_pct - bearish_pct)",
}

# HTTP request timeout (seconds)
_REQUEST_TIMEOUT: int = 30

# Minimum delay between requests to AAII (seconds)
_RATE_LIMIT_DELAY: float = 2.0


class AAIISentimentPuller(BasePuller):
    """Pulls the AAII weekly Investor Sentiment Survey.

    Data source: https://www.aaii.com/sentimentsurvey

    The AAII sentiment survey is a widely-followed contrarian indicator.
    Extreme bullish readings (>50%) often precede corrections; extreme
    bearish readings (>50%) often precede rallies.

    Features:
    - aaii.bullish_pct: % bullish investors
    - aaii.bearish_pct: % bearish investors
    - aaii.neutral_pct: % neutral investors
    - aaii.bull_bear_spread: bullish - bearish (contrarian signal)

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for AAII.
    """

    SOURCE_NAME: str = "AAII_Sentiment"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://www.aaii.com/sentimentsurvey",
        "cost_tier": "FREE",
        "latency_class": "WEEKLY",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 30,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the AAII Sentiment puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info(
            "AAIISentimentPuller initialised -- source_id={sid}",
            sid=self.source_id,
        )

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
    def _download_sentiment_data(self) -> pd.DataFrame:
        """Download AAII sentiment data and return as DataFrame.

        Attempts the XLS/CSV export first. If blocked, falls back to
        scraping the HTML results page.

        Returns:
            DataFrame with columns: date, bullish, bearish, neutral.

        Raises:
            requests.RequestException: On HTTP errors after retries.
            ValueError: If data cannot be parsed from any source.
        """
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (compatible; GRID-DataPuller/1.0; "
                "+https://github.com/grid-trading)"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,text/csv,*/*;q=0.8"
            ),
        }

        # Attempt 1: Try the XLS/CSV export
        try:
            resp = requests.get(
                _AAII_CSV_URL, headers=headers, timeout=_REQUEST_TIMEOUT
            )
            resp.raise_for_status()

            # AAII exports as .xls but it may be HTML-table or actual Excel
            try:
                df = pd.read_excel(io.BytesIO(resp.content))
            except Exception:
                # Sometimes the .xls is actually a CSV/TSV
                df = pd.read_csv(io.StringIO(resp.text))

            df = self._normalize_dataframe(df)
            if len(df) > 0:
                log.info(
                    "AAII sentiment: parsed {n} rows from XLS export",
                    n=len(df),
                )
                return df

        except (requests.RequestException, ValueError) as exc:
            log.warning(
                "AAII XLS download failed, trying HTML fallback: {e}",
                e=str(exc),
            )

        # Attempt 2: Try scraping the HTML results page
        try:
            resp = requests.get(
                _AAII_ALT_URL, headers=headers, timeout=_REQUEST_TIMEOUT
            )
            resp.raise_for_status()

            tables = pd.read_html(io.StringIO(resp.text))
            if not tables:
                raise ValueError("No tables found on AAII results page")

            # Find the table with sentiment data (look for Bullish column)
            for table in tables:
                cols_lower = [str(c).lower() for c in table.columns]
                if any("bullish" in c for c in cols_lower):
                    df = self._normalize_dataframe(table)
                    if len(df) > 0:
                        log.info(
                            "AAII sentiment: parsed {n} rows from HTML",
                            n=len(df),
                        )
                        return df

            raise ValueError(
                "No sentiment table found in AAII HTML response"
            )

        except (requests.RequestException, ValueError) as exc:
            log.error(
                "AAII HTML scrape also failed: {e}", e=str(exc)
            )
            raise

    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize an AAII DataFrame to standard columns.

        Handles various column naming conventions from AAII exports.
        Produces a DataFrame with: date, bullish, bearish, neutral.

        Parameters:
            df: Raw DataFrame from AAII (XLS, CSV, or HTML table).

        Returns:
            Normalized DataFrame with date, bullish, bearish, neutral columns.
        """
        df = df.copy()

        # Strip whitespace from column names
        df.columns = [str(c).strip() for c in df.columns]

        # Map column names (AAII uses various formats)
        col_map: dict[str, list[str]] = {
            "date": ["Date", "DATE", "date", "Reported Date", "Survey Date"],
            "bullish": [
                "Bullish", "BULLISH", "bullish", "Bullish %",
                "Bull %", "% Bullish",
            ],
            "bearish": [
                "Bearish", "BEARISH", "bearish", "Bearish %",
                "Bear %", "% Bearish",
            ],
            "neutral": [
                "Neutral", "NEUTRAL", "neutral", "Neutral %",
                "% Neutral",
            ],
        }

        renamed: dict[str, str] = {}
        for target, candidates in col_map.items():
            for candidate in candidates:
                if candidate in df.columns:
                    renamed[candidate] = target
                    break
            else:
                # Try case-insensitive partial match
                for col in df.columns:
                    if target in str(col).lower():
                        renamed[col] = target
                        break

        df = df.rename(columns=renamed)

        # Check we have required columns
        required = {"date", "bullish", "bearish"}
        if not required.issubset(set(df.columns)):
            missing = required - set(df.columns)
            log.warning(
                "AAII data missing columns: {m} (have: {h})",
                m=missing,
                h=list(df.columns),
            )
            return pd.DataFrame(columns=["date", "bullish", "bearish", "neutral"])

        # Parse dates
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        nat_count = int(df["date"].isna().sum())
        if nat_count > 0:
            log.warning(
                "AAII date parsing: {n} values coerced to NaT", n=nat_count
            )
        df = df.dropna(subset=["date"])

        # Parse percentage values -- AAII may report as 0.45 or 45.0
        for col in ["bullish", "bearish", "neutral"]:
            if col not in df.columns:
                continue
            # Strip '%' sign if present in string values
            if df[col].dtype == object:
                df[col] = df[col].astype(str).str.replace("%", "", regex=False)
            df[col] = pd.to_numeric(df[col], errors="coerce")

            # Log coerced NaN values (ATTENTION.md #13)
            nan_count = int(df[col].isna().sum())
            if nan_count > 0:
                log.warning(
                    "AAII {c} parsing: {n} values coerced to NaN",
                    c=col,
                    n=nan_count,
                )

        # Determine if values are in decimal (0-1) or percentage (0-100) form
        # If max of bullish > 1, assume already percentage; otherwise multiply
        bullish_max = df["bullish"].max()
        if pd.notna(bullish_max) and bullish_max <= 1.0:
            for col in ["bullish", "bearish", "neutral"]:
                if col in df.columns:
                    df[col] = df[col] * 100.0

        # If neutral column is missing, derive it
        if "neutral" not in df.columns or df["neutral"].isna().all():
            df["neutral"] = 100.0 - df["bullish"] - df["bearish"]

        # Drop rows where both bullish and bearish are NaN
        df = df.dropna(subset=["bullish", "bearish"])

        return df[["date", "bullish", "bearish", "neutral"]].reset_index(
            drop=True
        )

    def _series_id(self, feature: str) -> str:
        """Build the full series_id for a feature.

        Parameters:
            feature: Feature suffix (e.g., 'bullish_pct').

        Returns:
            Full series_id (e.g., 'aaii.bullish_pct').
        """
        return f"{_SERIES_PREFIX}.{feature}"

    def pull_sentiment(
        self,
        start_date: str | date = "1987-01-01",
        days_back: int | None = None,
    ) -> dict[str, Any]:
        """Pull AAII sentiment data and store all four features.

        Uses _get_existing_dates() for efficient batch deduplication
        and _get_latest_date() to determine incremental start.

        Parameters:
            start_date: Earliest observation date to store.
            days_back: If set, only store data from this many days ago.

        Returns:
            dict with status, rows_inserted (total across features),
            and per-feature counts.
        """
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)

        if days_back is not None:
            cutoff = date.today() - timedelta(days=days_back)
            start_date = max(start_date, cutoff)

        # Check latest date for incremental pull
        latest = self._get_latest_date(self._series_id("bullish_pct"))
        if latest is not None and latest >= start_date:
            start_date = latest + timedelta(days=1)
            log.info(
                "AAII incremental pull from {d}", d=start_date.isoformat()
            )

        try:
            df = self._download_sentiment_data()
        except Exception as exc:
            log.error("AAII sentiment pull failed: {e}", e=str(exc))
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "error": str(exc),
            }

        if df.empty:
            log.warning("AAII sentiment: no data returned")
            return {"status": "SUCCESS", "rows_inserted": 0}

        # Filter by start_date
        df = df[df["date"].dt.date >= start_date].copy()
        if df.empty:
            log.info("AAII sentiment: no new data after {d}", d=start_date)
            return {"status": "SUCCESS", "rows_inserted": 0}

        # Compute bull-bear spread
        df["bull_bear_spread"] = df["bullish"] - df["bearish"]

        # Build feature-to-column mapping
        feature_cols: dict[str, str] = {
            "bullish_pct": "bullish",
            "bearish_pct": "bearish",
            "neutral_pct": "neutral",
            "bull_bear_spread": "bull_bear_spread",
        }

        total_inserted = 0
        per_feature: dict[str, int] = {}

        with self.engine.begin() as conn:
            for feature, col in feature_cols.items():
                series_id = self._series_id(feature)
                existing_dates = self._get_existing_dates(series_id, conn)
                inserted = 0

                for _, row in df.iterrows():
                    obs_date = row["date"].date()

                    if obs_date in existing_dates:
                        continue

                    value = row[col]
                    if pd.isna(value):
                        continue

                    self._insert_raw(
                        conn=conn,
                        series_id=series_id,
                        obs_date=obs_date,
                        value=float(value),
                        raw_payload={
                            "bullish": float(row["bullish"]) if pd.notna(row["bullish"]) else None,
                            "bearish": float(row["bearish"]) if pd.notna(row["bearish"]) else None,
                            "neutral": float(row["neutral"]) if pd.notna(row["neutral"]) else None,
                            "source_url": _AAII_CSV_URL,
                        },
                    )
                    inserted += 1

                per_feature[feature] = inserted
                total_inserted += inserted
                log.info(
                    "AAII {feat}: {n} rows inserted",
                    feat=series_id,
                    n=inserted,
                )

        log.info(
            "AAII sentiment pull complete -- {n} total rows inserted",
            n=total_inserted,
        )

        return {
            "status": "SUCCESS",
            "rows_inserted": total_inserted,
            "per_feature": per_feature,
        }

    def pull_all(
        self,
        start_date: str | date = "1987-01-01",
        days_back: int | None = None,
    ) -> list[dict[str, Any]]:
        """Pull all AAII sentiment features.

        Convenience method matching the pull_all() pattern used by
        other GRID pullers. Delegates to pull_sentiment() which
        handles all four features in a single pass.

        Parameters:
            start_date: Earliest observation date.
            days_back: If set, only store recent data.

        Returns:
            List containing the single result dict.
        """
        result = self.pull_sentiment(
            start_date=start_date,
            days_back=days_back,
        )
        return [result]
