"""
GRID full US Treasury yield curve ingestion module.

The existing yield curve coverage is limited to 2s10s and 3m10y spreads.
This module pulls the full curve (1Y, 5Y, 30Y), TIPS real yields,
breakeven inflation expectations, and the ACM term premium estimate --
all from FRED.

These additional signals capture long-end steepness, curvature (butterfly),
real rates, and term premium that are critical for regime classification.
"""

from __future__ import annotations

import time
from datetime import date, timedelta
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# FRED series IDs for yield curve components
YC_SERIES: dict[str, dict[str, str]] = {
    "yc_1y": {
        "fred_id": "DGS1",
        "description": "1-year Treasury constant maturity yield",
    },
    "yc_5y": {
        "fred_id": "DGS5",
        "description": "5-year Treasury constant maturity yield",
    },
    "yc_30y": {
        "fred_id": "DGS30",
        "description": "30-year Treasury constant maturity yield",
    },
    "yc_real_10y": {
        "fred_id": "DFII10",
        "description": "10-year TIPS yield (real rate)",
    },
    "yc_breakeven_10y": {
        "fred_id": "T10YIE",
        "description": "10-year breakeven inflation rate",
    },
    "yc_term_premium": {
        "fred_id": "THREEFYTP10",
        "description": "ACM 10-year term premium estimate",
    },
}

# Additional FRED series needed for derived features (already pulled elsewhere)
_SUPPORTING_SERIES: dict[str, str] = {
    "yc_2y": "DGS2",   # 2-year yield (for butterfly)
    "yc_10y": "DGS10",  # 10-year yield (for butterfly, 5s30s)
}

# Derived features computed from raw series
DERIVED_FEATURES: list[str] = [
    "yc_5s30s_spread",      # 30Y minus 5Y
    "yc_butterfly_2_5_10",  # 2*5Y - (2Y + 10Y) curvature
]

# Rate limit between FRED API calls
_RATE_LIMIT_DELAY: float = 0.3


class FullYieldCurvePuller(BasePuller):
    """Pulls the full US Treasury yield curve and derived metrics from FRED.

    Features:
    - yc_1y: 1-year Treasury yield
    - yc_5y: 5-year Treasury yield
    - yc_30y: 30-year yield
    - yc_5s30s_spread: 5s30s slope (long end steepness)
    - yc_butterfly_2_5_10: 2*5Y - (2Y + 10Y) curvature
    - yc_real_10y: 10Y TIPS yield (real rate)
    - yc_breakeven_10y: 10Y nominal minus TIPS (inflation expectations)
    - yc_term_premium: ACM term premium estimate

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id (FRED).
        fred: FredAPI client instance.
    """

    SOURCE_NAME: str = "FRED"  # Reuses existing FRED source entry

    def __init__(self, api_key: str, db_engine: Engine) -> None:
        """Initialise the full yield curve puller.

        Parameters:
            api_key: FRED API key.
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        if not api_key:
            log.warning(
                "FullYieldCurvePuller: FRED_API_KEY not set — pulls will fail"
            )
        self._api_key = api_key
        super().__init__(db_engine)
        log.info(
            "FullYieldCurvePuller initialised — source_id={sid}",
            sid=self.source_id,
        )

    @retry_on_failure(
        max_attempts=3,
        backoff=2.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, Exception),
    )
    def _fetch_fred_series(
        self, series_id: str, start_date: date, end_date: date
    ) -> pd.DataFrame:
        """Fetch a single FRED series via the API.

        Parameters:
            series_id: FRED series identifier.
            start_date: Start date for observations.
            end_date: End date for observations.

        Returns:
            DataFrame with 'date' and 'value' columns.
        """
        try:
            from fedfred import FredAPI
        except ImportError:
            log.error("fedfred not installed — run: pip install fedfred")
            raise ImportError("fedfred library required for FullYieldCurvePuller")

        fred = FredAPI(self._api_key)
        df = fred.get_series_observations(
            series_id,
            observation_start=str(start_date),
            observation_end=str(end_date),
        )

        if df is None or (hasattr(df, "empty") and df.empty):
            return pd.DataFrame(columns=["date", "value"])

        if isinstance(df, pd.DataFrame):
            result = pd.DataFrame()

            # Find date column
            for col in ("date", "Date", "observation_date"):
                if col in df.columns:
                    result["date"] = pd.to_datetime(df[col], errors="coerce")
                    break
            if "date" not in result.columns and df.index.name in ("date", "Date"):
                result["date"] = pd.to_datetime(df.index, errors="coerce")
            elif "date" not in result.columns:
                result["date"] = pd.to_datetime(df.iloc[:, 0], errors="coerce")

            # Find value column
            for col in ("value", "Value", series_id):
                if col in df.columns:
                    result["value"] = pd.to_numeric(df[col], errors="coerce")
                    break
            if "value" not in result.columns:
                numeric_cols = df.select_dtypes(include=["number"]).columns
                if len(numeric_cols) > 0:
                    result["value"] = df[numeric_cols[0]]
                else:
                    result["value"] = pd.to_numeric(df.iloc[:, -1], errors="coerce")

            # Log coerced values (ATTENTION.md #13)
            nan_count = result["value"].isna().sum()
            if nan_count > 0:
                log.warning(
                    "FRED {sid}: {n} values coerced to NaN",
                    sid=series_id,
                    n=nan_count,
                )

            result = result.dropna(subset=["date", "value"])
            return result

        return pd.DataFrame(columns=["date", "value"])

    def _pull_raw_series(
        self,
        feature_name: str,
        fred_id: str,
        start_date: date,
        end_date: date,
    ) -> dict[str, Any]:
        """Pull a single FRED series and store in raw_series.

        Parameters:
            feature_name: GRID feature name for series_id.
            fred_id: FRED series identifier.
            start_date: Earliest observation date.
            end_date: Latest observation date.

        Returns:
            Result dict with status and rows_inserted.
        """
        rows_inserted = 0

        try:
            df = self._fetch_fred_series(fred_id, start_date, end_date)
            if df.empty:
                return {
                    "feature": feature_name,
                    "status": "NO_DATA",
                    "rows_inserted": 0,
                }

            with self.engine.begin() as conn:
                for _, row in df.iterrows():
                    obs_date = row["date"].date()
                    value = float(row["value"])

                    if self._row_exists(feature_name, obs_date, conn):
                        continue

                    self._insert_raw(
                        conn=conn,
                        series_id=feature_name,
                        obs_date=obs_date,
                        value=value,
                        raw_payload={"fred_series": fred_id},
                    )
                    rows_inserted += 1

            log.info(
                "YieldCurve {feat} ({fid}) — {n} rows inserted",
                feat=feature_name,
                fid=fred_id,
                n=rows_inserted,
            )

        except ImportError:
            return {
                "feature": feature_name,
                "status": "FAILED",
                "rows_inserted": 0,
                "error": "fedfred not installed",
            }
        except Exception as exc:
            log.error(
                "YieldCurve {feat} pull failed: {e}", feat=feature_name, e=str(exc)
            )
            return {
                "feature": feature_name,
                "status": "FAILED",
                "rows_inserted": 0,
                "error": str(exc),
            }

        return {
            "feature": feature_name,
            "status": "SUCCESS",
            "rows_inserted": rows_inserted,
        }

    def _compute_derived(
        self, start_date: date, end_date: date
    ) -> list[dict[str, Any]]:
        """Compute derived yield curve features from raw series.

        Calculates 5s30s spread and butterfly from stored raw yields.

        Parameters:
            start_date: Earliest date for computation.
            end_date: Latest date for computation.

        Returns:
            List of result dicts per derived feature.
        """
        from sqlalchemy import text as sql_text

        results: list[dict[str, Any]] = []

        with self.engine.begin() as conn:
            # Fetch raw series for the computation window
            raw_data: dict[str, dict[date, float]] = {}
            for feature_name in ("yc_5y", "yc_30y", "yc_2y", "yc_10y"):
                # yc_2y and yc_10y may come from other pullers — search
                # by series_id across all sources
                rows = conn.execute(
                    sql_text(
                        "SELECT DISTINCT ON (obs_date) obs_date, value "
                        "FROM raw_series "
                        "WHERE series_id = :sid "
                        "AND obs_date >= :start AND obs_date <= :end "
                        "ORDER BY obs_date, pull_timestamp DESC"
                    ),
                    {"sid": feature_name, "start": start_date, "end": end_date},
                ).fetchall()
                raw_data[feature_name] = {r[0]: float(r[1]) for r in rows}

            y5 = raw_data.get("yc_5y", {})
            y30 = raw_data.get("yc_30y", {})
            y2 = raw_data.get("yc_2y", {})
            y10 = raw_data.get("yc_10y", {})

            # yc_5s30s_spread: 30Y minus 5Y
            spread_rows = 0
            for obs_date in sorted(set(y30.keys()) & set(y5.keys())):
                spread = y30[obs_date] - y5[obs_date]
                if not self._row_exists("yc_5s30s_spread", obs_date, conn):
                    self._insert_raw(
                        conn=conn,
                        series_id="yc_5s30s_spread",
                        obs_date=obs_date,
                        value=spread,
                        raw_payload={"y30": y30[obs_date], "y5": y5[obs_date]},
                    )
                    spread_rows += 1
            results.append({
                "feature": "yc_5s30s_spread",
                "status": "SUCCESS",
                "rows_inserted": spread_rows,
            })

            # yc_butterfly_2_5_10: 2*5Y - (2Y + 10Y)
            butterfly_rows = 0
            common_dates = sorted(set(y2.keys()) & set(y5.keys()) & set(y10.keys()))
            for obs_date in common_dates:
                butterfly = 2.0 * y5[obs_date] - (y2[obs_date] + y10[obs_date])
                if not self._row_exists("yc_butterfly_2_5_10", obs_date, conn):
                    self._insert_raw(
                        conn=conn,
                        series_id="yc_butterfly_2_5_10",
                        obs_date=obs_date,
                        value=butterfly,
                        raw_payload={
                            "y2": y2[obs_date],
                            "y5": y5[obs_date],
                            "y10": y10[obs_date],
                        },
                    )
                    butterfly_rows += 1
            results.append({
                "feature": "yc_butterfly_2_5_10",
                "status": "SUCCESS",
                "rows_inserted": butterfly_rows,
            })

        return results

    def _pull_supporting_series(
        self, start_date: date, end_date: date
    ) -> list[dict[str, Any]]:
        """Pull supporting series (2Y, 10Y) needed for derived features.

        These may already exist from other pullers but we ensure coverage.

        Parameters:
            start_date: Earliest observation date.
            end_date: Latest observation date.

        Returns:
            List of result dicts.
        """
        results: list[dict[str, Any]] = []
        for feature_name, fred_id in _SUPPORTING_SERIES.items():
            result = self._pull_raw_series(
                feature_name=feature_name,
                fred_id=fred_id,
                start_date=start_date,
                end_date=end_date,
            )
            results.append(result)
            time.sleep(_RATE_LIMIT_DELAY)
        return results

    def pull_all(
        self,
        start_date: str | date = "1990-01-01",
        days_back: int = 365,
    ) -> list[dict[str, Any]]:
        """Pull all yield curve features (raw + derived).

        Parameters:
            start_date: Earliest date to fetch.
            days_back: Number of days back to pull.

        Returns:
            List of result dicts per feature.
        """
        if not self._api_key:
            log.warning("FullYieldCurvePuller: no FRED_API_KEY — skipping")
            return [{"feature": "all", "status": "SKIPPED", "rows_inserted": 0}]

        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)

        cutoff = date.today() - timedelta(days=days_back)
        effective_start = max(start_date, cutoff)
        end_date = date.today()

        results: list[dict[str, Any]] = []

        # Pull primary yield curve series
        for feature_name, config in YC_SERIES.items():
            result = self._pull_raw_series(
                feature_name=feature_name,
                fred_id=config["fred_id"],
                start_date=effective_start,
                end_date=end_date,
            )
            results.append(result)
            time.sleep(_RATE_LIMIT_DELAY)

        # Pull supporting series for derived features
        supporting = self._pull_supporting_series(effective_start, end_date)
        results.extend(supporting)

        # Compute derived features
        try:
            derived = self._compute_derived(effective_start, end_date)
            results.extend(derived)
        except Exception as exc:
            log.error(
                "Yield curve derived computation failed: {e}", e=str(exc)
            )

        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "FullYieldCurve pull_all — {ok}/{total} features, {rows} rows",
            ok=succeeded,
            total=len(results),
            rows=total_rows,
        )
        return results
