"""
GRID CBOE volatility and strategy indices ingestion module.

Pulls free CBOE index closes: VIX, VIX3M and VIX9D (the originator's own
daily closes — FRED's ``VIXCLS`` is a republication of ``CBOE:VIX``), plus
SKEW, VVIX, PUT/CALL ratio and implied correlation for tail risk,
vol-of-vol and positioning.

``CBOE:VIX`` is the backup feed for the regime vector's VIX dimensions:
FRED posts VIXCLS the next business day and its API times out often enough
that the Sept 23-25 2026 closes only landed on Sept 29. Cboe's CSV has the
close the same evening. The series ids continue the ``scripts/
bulk_historical_pull.py`` history (``CBOE:VIX`` 1990→2026-03-25, source
CBOE) and are mapped to ``vix_spot`` / ``vix3m_spot`` / ``vix9d_spot`` in
``normalization/entity_map.py``.

Data source: CBOE Datashop CSV downloads (public, no API key needed).
"""

from __future__ import annotations

import io
import time
from datetime import date, timedelta
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, _http_status_from_exc, retry_on_failure

# CBOE index download URLs and feature mappings. Keys are raw_series ids.
CBOE_INDICES: dict[str, dict[str, str]] = {
    "CBOE:VIX": {
        "url": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv",
        "description": "CBOE VIX daily close (authoritative source of FRED VIXCLS)",
        "value_col": "CLOSE",
    },
    "CBOE:VIX3M": {
        "url": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX3M_History.csv",
        "description": "CBOE 3-month VIX daily close",
        "value_col": "CLOSE",
    },
    "CBOE:VIX9D": {
        "url": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX9D_History.csv",
        "description": "CBOE 9-day VIX daily close",
        "value_col": "CLOSE",
    },
    "skew_index": {
        "url": "https://cdn.cboe.com/api/global/us_indices/daily_prices/SKEW_History.csv",
        "description": "CBOE SKEW index (tail risk pricing, >130 = elevated)",
        "value_col": "SKEW",
    },
    "vvix": {
        "url": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VVIX_History.csv",
        "description": "VIX of VIX (vol-of-vol)",
        "value_col": "VVIX",
    },
    "put_call_ratio": {
        "url": "https://cdn.cboe.com/api/global/us_options/market_statistics/daily_put_call_volume/totalpc.csv",
        "fallback_url": "https://cdn.cboe.com/data/us/exchange_traded_products/market_statistics/volume_put_call_ratios/total_exchange_pcr.csv",
        "description": "CBOE total exchange PUT/CALL volume ratio",
        "value_col": "PCR",
    },
    "correlation_index": {
        "url": "https://cdn.cboe.com/api/global/us_indices/daily_prices/ICJ_History.csv",
        "description": "CBOE implied correlation index (ICJ)",
        "value_col": "ICJ",
    },
}

# Minimum delay between CBOE requests (seconds)
_RATE_LIMIT_DELAY: float = 1.0

# HTTP request timeout
_REQUEST_TIMEOUT: int = 30


class CBOEPermanentError(RuntimeError):
    """Raised on permanent 4xx responses from CBOE's CDN.

    Kept outside the retry decorator's retryable_exceptions tuple so a
    single 403/404 short-circuits the attempt loop instead of hammering a
    dead URL three times per scheduler tick.
    """


class CBOEIndicesPuller(BasePuller):
    """Pulls CBOE volatility and strategy indices.

    Data source: https://www.cboe.com/tradable_products/ (CSV downloads)

    Features:
    - CBOE:VIX / CBOE:VIX3M / CBOE:VIX9D: daily closes (VIX backup feed)
    - skew_index: CBOE SKEW (tail risk pricing, >130 = elevated)
    - vvix: VIX of VIX (vol-of-vol)
    - put_call_ratio: CBOE PUT/CALL ratio for exchange options
    - correlation_index: ICJ implied correlation

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for CBOE.
    """

    SOURCE_NAME: str = "CBOE"

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the CBOE indices puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info(
            "CBOEIndicesPuller initialised — source_id={sid}", sid=self.source_id
        )

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _download_csv(self, url: str) -> pd.DataFrame:
        """Download a CSV from CBOE and return as DataFrame.

        Parameters:
            url: Full URL to the CSV file.

        Returns:
            DataFrame with parsed CSV data.

        Raises:
            requests.RequestException: On HTTP errors.
        """
        headers = {
            # CBOE's CDN returns 403 for generic bot User-Agents. A mainstream
            # browser string gets past the WAF without bypassing any real
            # protection (the CSVs are publicly linked from cboe.com).
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/csv,application/csv,*/*",
            "Referer": "https://www.cboe.com/",
        }
        resp = requests.get(url, headers=headers, timeout=_REQUEST_TIMEOUT)
        # 4xx responses are permanent for this URL — don't burn retries.
        if resp.status_code in (400, 401, 403, 404, 410):
            raise CBOEPermanentError(
                f"CBOE CSV {url} returned {resp.status_code}"
            )
        resp.raise_for_status()

        df = pd.read_csv(io.StringIO(resp.text))
        return df

    def _parse_date_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Find and parse the date column in a CBOE CSV.

        CBOE CSVs use various date column names. This method finds the
        date column and standardises it.

        Parameters:
            df: Raw DataFrame from CSV.

        Returns:
            DataFrame with a 'date' column of type datetime.
        """
        date_candidates = ["Date", "DATE", "date", "Trade Date", "TRADE DATE"]
        date_col = None
        for candidate in date_candidates:
            if candidate in df.columns:
                date_col = candidate
                break

        if date_col is None:
            # Try the first column
            date_col = df.columns[0]
            log.warning(
                "No standard date column found, using first column: {c}",
                c=date_col,
            )

        df = df.copy()
        df["date"] = pd.to_datetime(df[date_col], errors="coerce")

        # Log coerced NaT values (ATTENTION.md #13)
        nat_count = df["date"].isna().sum()
        if nat_count > 0:
            log.warning(
                "CBOE date parsing: {n} values coerced to NaT", n=nat_count
            )

        df = df.dropna(subset=["date"])
        return df

    def _extract_value(
        self, df: pd.DataFrame, value_col: str
    ) -> pd.DataFrame:
        """Extract the value column from a CBOE DataFrame.

        Parameters:
            df: DataFrame with date column already parsed.
            value_col: Expected column name for the value.

        Returns:
            DataFrame with 'date' and 'value' columns.
        """
        # Try exact match first, then case-insensitive search
        actual_col = None
        if value_col in df.columns:
            actual_col = value_col
        else:
            for col in df.columns:
                if col.strip().upper() == value_col.upper():
                    actual_col = col
                    break

        if actual_col is None:
            # Fallback: use the last numeric column
            numeric_cols = df.select_dtypes(include=["number"]).columns
            if len(numeric_cols) > 0:
                actual_col = numeric_cols[-1]
                log.warning(
                    "Value column {vc} not found, using fallback: {fc}",
                    vc=value_col,
                    fc=actual_col,
                )
            else:
                raise ValueError(
                    f"No numeric column found in CBOE data for {value_col}"
                )

        result = df[["date"]].copy()
        result["value"] = pd.to_numeric(df[actual_col], errors="coerce")

        # Log coerced NaN values (ATTENTION.md #13)
        nan_count = result["value"].isna().sum()
        if nan_count > 0:
            log.warning(
                "CBOE value parsing ({vc}): {n} values coerced to NaN",
                vc=value_col,
                n=nan_count,
            )

        result = result.dropna(subset=["value"])
        return result

    def pull_index(
        self,
        feature_name: str,
        start_date: str | date = "1990-01-01",
        days_back: int | None = None,
    ) -> dict[str, Any]:
        """Pull a single CBOE index and store in raw_series.

        Parameters:
            feature_name: Key in CBOE_INDICES (e.g., 'skew_index').
            start_date: Earliest observation date to store.
            days_back: If set, only store data from this many days ago.

        Returns:
            dict with status, rows_inserted, feature_name.
        """
        if feature_name not in CBOE_INDICES:
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "feature_name": feature_name,
                "error": f"Unknown CBOE index: {feature_name}",
            }

        config = CBOE_INDICES[feature_name]
        rows_inserted = 0

        try:
            urls = [config["url"]]
            if "fallback_url" in config:
                urls.append(config["fallback_url"])

            df = None
            last_err = None
            for url in urls:
                try:
                    df = self._download_csv(url)
                    break
                except Exception as dl_err:
                    last_err = dl_err
                    log.warning(
                        "CBOE {feat} download failed for {u}: {e} — trying next",
                        feat=feature_name,
                        u=url,
                        e=str(dl_err),
                    )
            if df is None:
                raise last_err  # type: ignore[misc]

            df = self._parse_date_column(df)
            df = self._extract_value(df, config["value_col"])

            # Apply date filters
            if isinstance(start_date, str):
                start_date = date.fromisoformat(start_date)

            if days_back is not None:
                cutoff = date.today() - timedelta(days=days_back)
                start_date = max(start_date, cutoff)

            with self.engine.begin() as conn:
                # One set lookup for the whole window. The previous per-row
                # ``_row_exists`` check only looked back one hour, so every
                # daily run re-inserted the full CSV history (vvix: 553,970
                # rows for 5,118 dates by 2026-10-06). History already on
                # file is a vintage, not a new observation.
                existing = self._get_existing_dates(
                    feature_name, conn, start_date=start_date,
                )
                for _, row in df.iterrows():
                    obs_date = row["date"].date()
                    if obs_date < start_date or obs_date in existing:
                        continue

                    value = float(row["value"])
                    self._insert_raw(
                        conn=conn,
                        series_id=feature_name,
                        obs_date=obs_date,
                        value=value,
                        raw_payload={"source_url": config["url"]},
                    )
                    existing.add(obs_date)
                    rows_inserted += 1

            log.info(
                "CBOE {feat} pull — {n} rows inserted",
                feat=feature_name,
                n=rows_inserted,
            )

        except CBOEPermanentError as exc:
            log.warning(
                "CBOE {feat}: permanent failure, skipping — {e}",
                feat=feature_name, e=str(exc),
            )
            return {
                "status": "SKIPPED",
                "rows_inserted": 0,
                "feature_name": feature_name,
                "error": str(exc),
            }
        except Exception as exc:
            # 4xx upstream failures (e.g. 403 on the public CSV CDN) are
            # outside our control; demote so they don't drown the log.
            status = _http_status_from_exc(exc)
            if status is not None and 400 <= status < 500 and status != 429:
                log.warning(
                    "CBOE {feat} unavailable (HTTP {s}) — skipping",
                    feat=feature_name,
                    s=status,
                )
            else:
                log.error(
                    "CBOE {feat} pull failed: {e}", feat=feature_name, e=str(exc)
                )
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "feature_name": feature_name,
                "error": str(exc),
            }

        return {
            "status": "SUCCESS",
            "rows_inserted": rows_inserted,
            "feature_name": feature_name,
        }

    def pull_all(
        self,
        start_date: str | date = "1990-01-01",
        days_back: int | None = None,
    ) -> list[dict[str, Any]]:
        """Pull all configured CBOE indices.

        Parameters:
            start_date: Earliest observation date.
            days_back: If set, only store recent data.

        Returns:
            List of result dicts per index.
        """
        results: list[dict[str, Any]] = []

        for feature_name in CBOE_INDICES:
            result = self.pull_index(
                feature_name=feature_name,
                start_date=start_date,
                days_back=days_back,
            )
            results.append(result)
            time.sleep(_RATE_LIMIT_DELAY)

        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        total_rows = sum(r["rows_inserted"] for r in results)
        log.info(
            "CBOE pull_all — {ok}/{total} indices, {rows} rows",
            ok=succeeded,
            total=len(results),
            rows=total_rows,
        )
        return results
