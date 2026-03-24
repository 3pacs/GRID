"""
GRID repo and money market stress indicator ingestion module.

The repo market is the plumbing of the financial system -- stress here
precedes broader market dislocations. This puller fetches SOFR, reverse
repo facility usage, and Treasury bill spreads from FRED.

All series are publicly available via the FRED API (requires FRED_API_KEY).
"""

from __future__ import annotations

import time
from datetime import date, timedelta
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# FRED series IDs mapped to GRID feature names
REPO_SERIES: dict[str, dict[str, str]] = {
    "sofr_rate": {
        "fred_id": "SOFR",
        "description": "Secured Overnight Financing Rate",
    },
    "reverse_repo_usage": {
        "fred_id": "RRPONTSYD",
        "description": "Fed ON RRP facility usage (billions)",
    },
    "treasury_bill_3m": {
        "fred_id": "DTB3",
        "description": "3-Month Treasury Bill secondary market rate",
    },
    "fed_funds_effective": {
        "fred_id": "DFF",
        "description": "Effective Federal Funds Rate (for spread calc)",
    },
}

# Derived features computed from the raw series above
DERIVED_FEATURES: list[str] = [
    "sofr_spread_to_ffr",     # SOFR minus effective FFR
    "rrp_as_pct_of_peak",     # Current RRP / peak RRP
    "treasury_bill_spread",   # 3M T-bill minus SOFR
]

# Rate limit between FRED API calls
_RATE_LIMIT_DELAY: float = 0.3
_REQUEST_TIMEOUT: int = 30


class RepoMarketPuller(BasePuller):
    """Pulls repo and money market stress indicators from FRED.

    Features:
    - sofr_rate: Secured Overnight Financing Rate
    - sofr_spread_to_ffr: SOFR minus effective FFR (positive = funding stress)
    - reverse_repo_usage: Fed ON RRP facility usage (billions)
    - rrp_as_pct_of_peak: current usage / peak usage
    - treasury_bill_spread: 3M T-bill minus SOFR

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for this puller.
        fred: FredAPI client instance.
    """

    SOURCE_NAME: str = "FRED"  # Reuses existing FRED source entry

    def __init__(self, api_key: str, db_engine: Engine) -> None:
        """Initialise the repo market puller.

        Parameters:
            api_key: FRED API key.
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        if not api_key:
            log.warning(
                "RepoMarketPuller: FRED_API_KEY not set — pulls will fail"
            )
        self._api_key = api_key
        super().__init__(db_engine)
        log.info(
            "RepoMarketPuller initialised — source_id={sid}", sid=self.source_id
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
            series_id: FRED series identifier (e.g., 'SOFR').
            start_date: Start date for observations.
            end_date: End date for observations.

        Returns:
            DataFrame with 'date' and 'value' columns.
        """
        try:
            from fedfred import FredAPI
        except ImportError:
            log.error("fedfred not installed — run: pip install fedfred")
            raise ImportError("fedfred library required for RepoMarketPuller")

        fred = FredAPI(self._api_key)
        df = fred.get_series_observations(
            series_id,
            observation_start=str(start_date),
            observation_end=str(end_date),
        )

        if df is None or (hasattr(df, "empty") and df.empty):
            return pd.DataFrame(columns=["date", "value"])

        # fedfred returns a DataFrame — normalise column names
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
                "Repo {feat} ({fid}) — {n} rows inserted",
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
                "Repo {feat} pull failed: {e}", feat=feature_name, e=str(exc)
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
        """Compute derived repo market features from raw series.

        Reads recently stored raw values and computes spreads / ratios.

        Parameters:
            start_date: Earliest date for computation.
            end_date: Latest date for computation.

        Returns:
            List of result dicts per derived feature.
        """
        from sqlalchemy import text as sql_text

        results: list[dict[str, Any]] = []

        with self.engine.begin() as conn:
            # Fetch raw series for the window
            raw_data: dict[str, dict[date, float]] = {}
            for feature_name in ("sofr_rate", "fed_funds_effective",
                                 "reverse_repo_usage", "treasury_bill_3m"):
                rows = conn.execute(
                    sql_text(
                        "SELECT obs_date, value FROM raw_series "
                        "WHERE series_id = :sid AND source_id = :src "
                        "AND obs_date >= :start AND obs_date <= :end "
                        "ORDER BY obs_date"
                    ),
                    {
                        "sid": feature_name,
                        "src": self.source_id,
                        "start": start_date,
                        "end": end_date,
                    },
                ).fetchall()
                raw_data[feature_name] = {r[0]: float(r[1]) for r in rows}

            sofr = raw_data.get("sofr_rate", {})
            ffr = raw_data.get("fed_funds_effective", {})
            rrp = raw_data.get("reverse_repo_usage", {})
            tbill = raw_data.get("treasury_bill_3m", {})

            # Peak RRP for ratio calculation
            rrp_peak = max(rrp.values()) if rrp else 1.0
            if rrp_peak == 0:
                rrp_peak = 1.0

            # sofr_spread_to_ffr
            spread_rows = 0
            for obs_date in sorted(set(sofr.keys()) & set(ffr.keys())):
                spread = sofr[obs_date] - ffr[obs_date]
                if not self._row_exists("sofr_spread_to_ffr", obs_date, conn):
                    self._insert_raw(
                        conn=conn,
                        series_id="sofr_spread_to_ffr",
                        obs_date=obs_date,
                        value=spread,
                        raw_payload={"sofr": sofr[obs_date], "ffr": ffr[obs_date]},
                    )
                    spread_rows += 1
            results.append({
                "feature": "sofr_spread_to_ffr",
                "status": "SUCCESS",
                "rows_inserted": spread_rows,
            })

            # rrp_as_pct_of_peak
            pct_rows = 0
            for obs_date, val in sorted(rrp.items()):
                pct = val / rrp_peak
                if not self._row_exists("rrp_as_pct_of_peak", obs_date, conn):
                    self._insert_raw(
                        conn=conn,
                        series_id="rrp_as_pct_of_peak",
                        obs_date=obs_date,
                        value=pct,
                        raw_payload={"rrp": val, "peak": rrp_peak},
                    )
                    pct_rows += 1
            results.append({
                "feature": "rrp_as_pct_of_peak",
                "status": "SUCCESS",
                "rows_inserted": pct_rows,
            })

            # treasury_bill_spread: 3M T-bill minus SOFR
            tb_spread_rows = 0
            for obs_date in sorted(set(tbill.keys()) & set(sofr.keys())):
                spread = tbill[obs_date] - sofr[obs_date]
                if not self._row_exists("treasury_bill_spread", obs_date, conn):
                    self._insert_raw(
                        conn=conn,
                        series_id="treasury_bill_spread",
                        obs_date=obs_date,
                        value=spread,
                        raw_payload={
                            "tbill_3m": tbill[obs_date],
                            "sofr": sofr[obs_date],
                        },
                    )
                    tb_spread_rows += 1
            results.append({
                "feature": "treasury_bill_spread",
                "status": "SUCCESS",
                "rows_inserted": tb_spread_rows,
            })

        return results

    def pull_all(
        self,
        start_date: str | date = "2018-01-01",
        days_back: int = 365,
    ) -> list[dict[str, Any]]:
        """Pull all repo market features (raw + derived).

        Parameters:
            start_date: Earliest date to fetch.
            days_back: Number of days back to pull.

        Returns:
            List of result dicts per feature.
        """
        if not self._api_key:
            log.warning("RepoMarketPuller: no FRED_API_KEY — skipping")
            return [{"feature": "all", "status": "SKIPPED", "rows_inserted": 0}]

        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)

        cutoff = date.today() - timedelta(days=days_back)
        effective_start = max(start_date, cutoff)
        end_date = date.today()

        results: list[dict[str, Any]] = []

        # Pull raw FRED series
        for feature_name, config in REPO_SERIES.items():
            result = self._pull_raw_series(
                feature_name=feature_name,
                fred_id=config["fred_id"],
                start_date=effective_start,
                end_date=end_date,
            )
            results.append(result)
            time.sleep(_RATE_LIMIT_DELAY)

        # Compute derived features
        try:
            derived = self._compute_derived(effective_start, end_date)
            results.extend(derived)
        except Exception as exc:
            log.error("Repo derived feature computation failed: {e}", e=str(exc))

        total_rows = sum(r["rows_inserted"] for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "RepoMarket pull_all — {ok}/{total} features, {rows} rows",
            ok=succeeded,
            total=len(results),
            rows=total_rows,
        )
        return results
