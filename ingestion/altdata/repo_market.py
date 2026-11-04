"""
GRID repo and money market stress indicator ingestion module.

The repo market is the plumbing of the financial system -- stress here
precedes broader market dislocations. This puller fetches SOFR, reverse
repo facility usage, and Treasury bill spreads from FRED.

All series are publicly available via the FRED API (requires FRED_API_KEY).
"""

from __future__ import annotations

import time
import math
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
    # rrp_as_pct_of_peak is no longer derived here (2026-11-04): this window's
    # peak disagreed with fed_liquidity's all-history peak, which now feeds
    # the feature (COMPUTED:reverse_repo_pct_of_peak -> rrp_as_pct_of_peak).
    "treasury_bill_spread",   # 3M T-bill minus SOFR
]

# Rate limit between FRED API calls
_RATE_LIMIT_DELAY: float = 0.3
_REQUEST_TIMEOUT: int = 30


# FRED marks a date with no observation (market holiday on a daily series)
# with value "."; fedfred coerces it to NaN before we see it. A handful per
# year is expected and logged at DEBUG. A window where more than this share of
# rows is unusable is not holiday noise and is logged at WARNING
# (ATTENTION.md #13).
_MISSING_WARN_SHARE: float = 0.25


def _normalise_fred_frame(df: Any, series_id: str) -> Any:
    """Return a ``date``/``value`` frame from a fedfred observations frame.

    Rows whose value is missing ("." on FRED) or non-finite are skipped -- never
    stored, never fatal -- so a series whose latest observation is "." still
    stores every valid one. Only a non-empty response with no usable row at all
    raises ``ValueError`` (docs/puller_outcome_contracts.md).
    """
    result = pd.DataFrame()
    idx = df.index
    if isinstance(idx, pd.DatetimeIndex):
        result["date"] = pd.Series(idx.to_numpy())
    elif idx.name in ("date", "Date", "observation_date"):
        result["date"] = pd.Series(pd.to_datetime(idx, errors="coerce").to_numpy())
    for col in ("date", "Date", "observation_date"):
        if "date" not in result.columns and col in df.columns:
            result["date"] = pd.to_datetime(df[col], errors="coerce").to_numpy()
            break
    if "date" not in result.columns:
        result["date"] = pd.to_datetime(df.iloc[:, 0], errors="coerce").to_numpy()

    for col in ("value", "Value", series_id):
        if col in df.columns:
            result["value"] = pd.to_numeric(df[col], errors="coerce").to_numpy()
            break
    if "value" not in result.columns:
        numeric_cols = df.select_dtypes(include=["number"]).columns
        source = df[numeric_cols[0]] if len(numeric_cols) > 0 else df.iloc[:, -1]
        result["value"] = pd.to_numeric(source, errors="coerce").to_numpy()

    total = len(result)
    kept_dates: list[Any] = []
    kept_values: list[float] = []
    missing = 0
    non_finite = 0
    for obs_date, value in zip(result["date"], result["value"]):
        if pd.isna(obs_date) or pd.isna(value):
            missing += 1
            continue
        if not math.isfinite(float(value)):
            non_finite += 1
            continue
        kept_dates.append(obs_date)
        kept_values.append(float(value))

    if missing or non_finite:
        skipped = missing + non_finite
        emit = log.warning if non_finite or skipped > total * _MISSING_WARN_SHARE else log.debug
        emit(
            "FRED {sid}: skipped {m} missing ('.') and {nf} non-finite "
            "observations; kept {k}/{t}",
            sid=series_id, m=missing, nf=non_finite, k=len(kept_dates), t=total,
        )
    if total > 0 and not kept_dates:
        raise ValueError(
            "Invalid FRED observation date or value: all "
            f"{total} observations for {series_id} unusable"
        )
    return pd.DataFrame({"date": kept_dates, "value": kept_values})


class RepoMarketPuller(BasePuller):
    """Pulls repo and money market stress indicators from FRED.

    Features:
    - sofr_rate: Secured Overnight Financing Rate
    - sofr_spread_to_ffr: SOFR minus effective FFR (positive = funding stress)
    - reverse_repo_usage: Fed ON RRP facility usage (billions)
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
        # Transport errors only: a deterministic parse failure returns the same
        # answer on every attempt, so retrying it only triples the log lines.
        retryable_exceptions=(ConnectionError, TimeoutError, OSError),
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

        if not isinstance(df, pd.DataFrame):
            raise ValueError("Unsupported FRED response")
        if df.empty:
            if not any(c in df.columns for c in ("date", "Date", "observation_date")) or not any(c in df.columns for c in ("value", "Value", series_id)):
                raise ValueError("Empty FRED response lacks required columns")
            return pd.DataFrame(columns=["date", "value"])

        # fedfred 3.x (fedfred/helpers.py ``to_pd_df``) returns the
        # observation date as a DatetimeIndex named "date" and has already
        # coerced FRED's "." missing-value marker to NaN. Every column below is
        # assigned positionally (``.to_numpy()``): the previous code assigned
        # the index-bearing value Series onto a RangeIndex frame, pandas
        # aligned on index, and EVERY value became NaN (grid-hermes
        # 2026-11-03 18:02Z "FRED SOFR: 261 values coerced to NaN" ->
        # "Invalid FRED observation date or value", retried 3x, ERROR for all
        # four repo series every run). Same fix as yield_curve_full.py.
        return _normalise_fred_frame(df, series_id)

        raise ValueError("Unsupported FRED response")

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

        transaction_started = False
        try:
            df = self._fetch_fred_series(fred_id, start_date, end_date)
            if not isinstance(df, pd.DataFrame):
                raise ValueError("Unsupported normalized FRED response")
            if "date" not in df.columns or "value" not in df.columns:
                raise ValueError("Malformed normalized FRED columns")
            if df.empty:
                return {
                    "feature": feature_name,
                    "status": "NO_NEW_DATA",
                    "rows_inserted": 0,
                }

            transaction_started = True
            with self.engine.begin() as conn:
                for _, row in df.iterrows():
                    obs_date = row["date"].date()
                    value = float(row["value"])

                    if self._row_exists(feature_name, obs_date, conn):
                        continue

                    insert_result = self._insert_raw(
                        conn=conn,
                        series_id=feature_name,
                        obs_date=obs_date,
                        value=value,
                        raw_payload={"fred_series": fred_id},
                        return_result=True,
                    )
                    affected = getattr(insert_result, "rowcount", None)
                    if type(affected) is not int or affected < 0:
                        raise ValueError("Unknown affected row count")
                    rows_inserted += affected

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
                "rows_inserted": None if transaction_started else 0,
                "error": "fedfred not installed",
            }
        except Exception as exc:
            log.error(
                "Repo {feat} pull failed: {e}", feat=feature_name, e=str(exc)
            )
            return {
                "feature": feature_name,
                "status": "FAILED",
                "rows_inserted": None if transaction_started else 0,
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
                        "AND pull_status = 'SUCCESS' "
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
            tbill = raw_data.get("treasury_bill_3m", {})

            # sofr_spread_to_ffr
            spread_rows = 0
            for obs_date in sorted(set(sofr.keys()) & set(ffr.keys())):
                spread = sofr[obs_date] - ffr[obs_date]
                if not self._row_exists("sofr_spread_to_ffr", obs_date, conn):
                    insert_result = self._insert_raw(
                        conn=conn,
                        series_id="sofr_spread_to_ffr",
                        obs_date=obs_date,
                        value=spread,
                        raw_payload={"sofr": sofr[obs_date], "ffr": ffr[obs_date]},
                        return_result=True,
                    )
                    affected = getattr(insert_result, "rowcount", None)
                    if type(affected) is not int or affected < 0:
                        raise ValueError("Unknown affected row count")
                    spread_rows += affected
            results.append({
                "feature": "sofr_spread_to_ffr",
                "status": "SUCCESS",
                "rows_inserted": spread_rows,
            })

            # treasury_bill_spread: 3M T-bill minus SOFR
            tb_spread_rows = 0
            for obs_date in sorted(set(tbill.keys()) & set(sofr.keys())):
                spread = tbill[obs_date] - sofr[obs_date]
                if not self._row_exists("treasury_bill_spread", obs_date, conn):
                    insert_result = self._insert_raw(
                        conn=conn,
                        series_id="treasury_bill_spread",
                        obs_date=obs_date,
                        value=spread,
                        raw_payload={
                            "tbill_3m": tbill[obs_date],
                            "sofr": sofr[obs_date],
                        },
                        return_result=True,
                    )
                    affected = getattr(insert_result, "rowcount", None)
                    if type(affected) is not int or affected < 0:
                        raise ValueError("Unknown affected row count")
                    tb_spread_rows += affected
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

            results.append({"feature": "derived_features", "status": "FAILED",
                            "rows_inserted": None, "error": str(exc)})

        total_rows = (None if any(r.get("rows_inserted") is None for r in results) else sum(r["rows_inserted"] for r in results))
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "RepoMarket pull_all — {ok}/{total} features, {rows} rows",
            ok=succeeded,
            total=len(results),
            rows=total_rows,
        )
        return results
