"""
GRID Google Trends data ingestion module.

Pulls Google Trends search interest for economically relevant terms using
the pytrends library (unofficial API — no key needed). Search volume data
leads economic indicators by weeks, making it a valuable sentiment signal.

Rate-limited to 1 request per 2 seconds to avoid 429 errors.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# Search terms and their corresponding feature names
TRENDS_QUERIES: dict[str, str] = {
    "recession": "gt_recession_interest",
    "unemployment": "gt_unemployment_interest",
    "inflation": "gt_inflation_interest",
    "stock market crash": "gt_stock_market_crash",
    "housing bubble": "gt_housing_bubble",
    "rate cut": "gt_fed_rate_cut",
}

# Minimum delay between pytrends API calls (seconds)
_RATE_LIMIT_DELAY: float = 2.0


class GoogleTrendsPuller(BasePuller):
    """Pulls Google Trends search interest for economically relevant terms.

    Uses the pytrends library (unofficial API — no key needed).

    Features:
    - gt_recession_interest: Search interest for "recession" (0-100)
    - gt_unemployment_interest: Search interest for "unemployment"
    - gt_inflation_interest: Search interest for "inflation"
    - gt_stock_market_crash: Search interest for "stock market crash"
    - gt_housing_bubble: Search interest for "housing bubble"
    - gt_fed_rate_cut: Search interest for "rate cut"
    - gt_economic_composite: Average of above (fear/anxiety index)

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for Google Trends.
    """

    SOURCE_NAME: str = "GoogleTrends"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://trends.google.com",
        "cost_tier": "FREE",
        "latency_class": "WEEKLY",
        "pit_available": False,
        "revision_behavior": "RARE",
        "trust_score": "MED",
        "priority_rank": 40,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the Google Trends puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info(
            "GoogleTrendsPuller initialised — source_id={sid}", sid=self.source_id
        )

    @retry_on_failure(
        max_attempts=3,
        backoff=5.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, Exception),
    )
    def _fetch_trend(
        self, keyword: str, timeframe: str = "today 12-m"
    ) -> pd.DataFrame:
        """Fetch Google Trends data for a single keyword.

        Parameters:
            keyword: Search term to query.
            timeframe: Pytrends timeframe string.

        Returns:
            DataFrame with date index and interest values.
        """
        try:
            from pytrends.request import TrendReq
        except ImportError:
            log.error(
                "pytrends not installed — run: pip install pytrends"
            )
            raise ImportError("pytrends library required for GoogleTrendsPuller")

        pytrends = TrendReq(hl="en-US", tz=360, timeout=(10, 30))
        pytrends.build_payload([keyword], cat=0, timeframe=timeframe, geo="US")
        df = pytrends.interest_over_time()

        if df.empty:
            log.warning(
                "No Google Trends data returned for keyword={kw}", kw=keyword
            )
            return pd.DataFrame()

        # Drop the 'isPartial' column if present
        if "isPartial" in df.columns:
            df = df.drop(columns=["isPartial"])

        return df

    def _interpolate_weekly_to_daily(self, df: pd.DataFrame) -> pd.DataFrame:
        """Interpolate weekly Google Trends data to daily frequency.

        Parameters:
            df: Weekly DataFrame with date index.

        Returns:
            Daily DataFrame with linearly interpolated values.
        """
        if df.empty:
            return df

        # Resample to daily and interpolate
        daily = df.resample("D").interpolate(method="linear")
        return daily

    def pull_keyword(
        self,
        keyword: str,
        feature_name: str,
        start_date: str | date = "2004-01-01",
        days_back: int = 365,
    ) -> dict[str, Any]:
        """Pull Google Trends data for a single keyword and store in raw_series.

        Parameters:
            keyword: Search term to query.
            feature_name: Feature name for raw_series.series_id.
            start_date: Earliest date (ignored if days_back covers less).
            days_back: Number of days back to fetch.

        Returns:
            dict with status, rows_inserted, keyword.
        """
        rows_inserted = 0

        try:
            # Determine timeframe
            if days_back <= 90:
                timeframe = "today 3-m"
            elif days_back <= 365:
                timeframe = "today 12-m"
            else:
                timeframe = "today 5-y"

            df = self._fetch_trend(keyword, timeframe=timeframe)
            if df.empty:
                return {
                    "status": "NO_DATA",
                    "rows_inserted": 0,
                    "keyword": keyword,
                }

            # Interpolate to daily
            daily_df = self._interpolate_weekly_to_daily(df)

            with self.engine.begin() as conn:
                for idx_date, row in daily_df.iterrows():
                    obs_date = idx_date.date()
                    value = row.iloc[0]

                    # Check for NaN from coercion (ATTENTION.md #13)
                    if pd.isna(value):
                        log.warning(
                            "NaN value for {kw} on {d} — skipping",
                            kw=keyword,
                            d=obs_date,
                        )
                        continue

                    value = float(value)

                    if self._row_exists(feature_name, obs_date, conn):
                        continue

                    self._insert_raw(
                        conn=conn,
                        series_id=feature_name,
                        obs_date=obs_date,
                        value=value,
                        raw_payload={"keyword": keyword, "source": "pytrends"},
                    )
                    rows_inserted += 1

            log.info(
                "Google Trends pull for {kw} — {n} rows inserted",
                kw=keyword,
                n=rows_inserted,
            )

        except ImportError:
            log.error("pytrends not available — skipping Google Trends pull")
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "keyword": keyword,
                "error": "pytrends not installed",
            }
        except Exception as exc:
            log.error(
                "Google Trends pull failed for {kw}: {e}",
                kw=keyword,
                e=str(exc),
            )
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "keyword": keyword,
                "error": str(exc),
            }

        return {
            "status": "SUCCESS",
            "rows_inserted": rows_inserted,
            "keyword": keyword,
        }

    def pull_all(
        self,
        start_date: str | date = "2004-01-01",
        days_back: int = 365,
    ) -> list[dict[str, Any]]:
        """Pull all configured Google Trends keywords.

        Also computes the gt_economic_composite (average of all terms).

        Parameters:
            start_date: Earliest date to fetch.
            days_back: Number of days back to pull.

        Returns:
            List of result dicts per keyword.
        """
        results: list[dict[str, Any]] = []

        for keyword, feature_name in TRENDS_QUERIES.items():
            result = self.pull_keyword(
                keyword=keyword,
                feature_name=feature_name,
                start_date=start_date,
                days_back=days_back,
            )
            results.append(result)

            # Rate limit between requests
            time.sleep(_RATE_LIMIT_DELAY)

        # Compute composite: average of all individual keyword scores
        try:
            self._compute_composite(days_back=days_back)
        except Exception as exc:
            log.error(
                "Google Trends composite computation failed: {e}", e=str(exc)
            )

        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        total_rows = sum(r["rows_inserted"] for r in results)
        log.info(
            "Google Trends pull_all — {ok}/{total} keywords, {rows} rows",
            ok=succeeded,
            total=len(results),
            rows=total_rows,
        )
        return results

    def _compute_composite(self, days_back: int = 365) -> None:
        """Compute gt_economic_composite as average of all keyword features.

        Reads recently inserted values and stores the daily average.
        """
        from sqlalchemy import text

        cutoff = date.today() - timedelta(days=days_back)
        feature_names = list(TRENDS_QUERIES.values())

        with self.engine.begin() as conn:
            # Get all recent values for our features
            rows = conn.execute(
                text(
                    "SELECT obs_date, AVG(value) as avg_val "
                    "FROM raw_series "
                    "WHERE series_id = ANY(:features) "
                    "AND source_id = :src "
                    "AND obs_date >= :cutoff "
                    "GROUP BY obs_date "
                    "HAVING COUNT(DISTINCT series_id) >= 3"
                ),
                {
                    "features": feature_names,
                    "src": self.source_id,
                    "cutoff": cutoff,
                },
            ).fetchall()

            composite_inserted = 0
            for row in rows:
                obs_date = row[0]
                avg_val = float(row[1])

                if self._row_exists("gt_economic_composite", obs_date, conn):
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id="gt_economic_composite",
                    obs_date=obs_date,
                    value=avg_val,
                    raw_payload={"method": "mean_of_keywords"},
                )
                composite_inserted += 1

            log.info(
                "Google Trends composite — {n} rows inserted",
                n=composite_inserted,
            )
