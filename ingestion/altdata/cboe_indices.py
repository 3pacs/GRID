"""
GRID CBOE volatility and strategy indices ingestion module.

Pulls free CBOE index closes: VIX, VIX3M and VIX9D (the originator's own
daily closes — FRED's ``VIXCLS`` is a republication of ``CBOE:VIX``), plus
SKEW, VVIX, the 3-month implied correlation index COR3M and the total
exchange and equity PUT/CALL ratios for tail risk, vol-of-vol and positioning.

Two feeds were rebuilt on 2026-11-04, when both old URLs answered 403 on
every run (so the source never finished better than PARTIAL):

* implied correlation: Cboe discontinued ICJ (``correlation_index``, last
  observation 2021-11-19). Its successor COR3M is published as
  ``COR3M_History.csv`` (probed from grid-svr: DATE,OPEN,HIGH,LOW,CLOSE from
  2006-01-03) and stored as ``CBOE:COR3M`` -> ``implied_correlation_3m``, a
  new series rather than a continuation, because the methodology and level
  differ.
* put/call: ``totalpc.csv`` stopped in 2019. Cboe now serves one JSON page
  per trading day, ``data/us/options/market_statistics/daily/<date>_daily_options``
  (probed: ``ratios[]`` with "TOTAL PUT/CALL RATIO", plus put/call volumes;
  403 on non-trading days). Stored as ``put_call_ratio`` (registered, 0 rows
  before), one obs per trading day, recent days only.
* equity put/call (2026-11-23): the same page's "EQUITY PUT/CALL RATIO"
  (confirmed on the 2026-11-20 page from grid-svr: 0.77, matching that page's
  EQUITY OPTIONS volume, 2,576,245 puts / 3,360,800 calls) is stored as
  ``equity_put_call_ratio``. Cboe's ``equitypc.csv`` ended with
  ``totalpc.csv``, and nothing ever wrote the ``CBOE:equitypc`` id the
  conviction scorer read, so this is a new series, not a continuation.

Put/call clock: ``raw_series.pull_timestamp`` is the schema default
``NOW()``, the insert transaction's start (``_insert_raw`` sends no
timestamp). ``pull_put_call_ratio`` reads every page before that
transaction opens, so a row is never stamped before its page was read; the
stamp can be later than the read, by up to the rest of the run, which makes
it a proxy for the acquisition time, not that time. Before #1397 the
transaction opened before the fetches, so a page read after the UTC day-end
could carry the previous day's stamp; ``put_call_ratio`` rows written then
keep it. The conviction scorer shows both ratios and scores neither.

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
import math
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
    "CBOE:COR3M": {
        "url": "https://cdn.cboe.com/api/global/us_indices/daily_prices/COR3M_History.csv",
        "description": "CBOE 3-month implied correlation index (successor to ICJ)",
        "value_col": "CLOSE",
    },
}

# Put/call ratios: one JSON page per trading day, read once for every series.
PUT_CALL_SERIES_ID: str = "put_call_ratio"
EQUITY_PUT_CALL_SERIES_ID: str = "equity_put_call_ratio"
PUT_CALL_URL: str = (
    "https://cdn.cboe.com/data/us/options/market_statistics/daily/{day}_daily_options"
)
PUT_CALL_RATIO_NAME: str = "TOTAL PUT/CALL RATIO"
EQUITY_PUT_CALL_RATIO_NAME: str = "EQUITY PUT/CALL RATIO"
# raw_series id -> (the ``ratios[].name`` it stores, the page block whose
# VOLUME row goes in raw_payload as put_volume / call_volume).
PUT_CALL_PAGE_SERIES: dict[str, tuple[str, str]] = {
    PUT_CALL_SERIES_ID: (PUT_CALL_RATIO_NAME, "SUM OF ALL PRODUCTS"),
    EQUITY_PUT_CALL_SERIES_ID: (EQUITY_PUT_CALL_RATIO_NAME, "EQUITY OPTIONS"),
}
# Calendar days looked back on a scheduled run. Only missing weekdays are
# requested, so a normal run asks for one or two pages.
PUT_CALL_DEFAULT_LOOKBACK_DAYS: int = 10
# If at least this many dates were requested and every one was refused, the
# CDN is blocking us rather than describing holidays: FAILED, not SUCCESS.
PUT_CALL_ALL_REFUSED_MIN: int = 3
_PUT_CALL_DELAY: float = 0.3

_HEADERS: dict[str, str] = {
    # CBOE's CDN returns 403 for generic bot User-Agents. A mainstream
    # browser string gets past the WAF without bypassing any real
    # protection (the files are publicly linked from cboe.com).
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.cboe.com/",
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
    - CBOE:COR3M: 3-month implied correlation (ICJ's successor)
    - put_call_ratio: total exchange PUT/CALL ratio (daily JSON pages)
    - equity_put_call_ratio: equity PUT/CALL ratio (same daily pages)

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
        headers = {**_HEADERS, "Accept": "text/csv,application/csv,*/*"}
        resp = requests.get(url, headers=headers, timeout=_REQUEST_TIMEOUT)
        # 4xx responses are permanent for this URL — don't burn retries.
        if resp.status_code in (400, 401, 403, 404, 410):
            raise CBOEPermanentError(
                f"CBOE CSV {url} returned {resp.status_code}"
            )
        resp.raise_for_status()

        df = pd.read_csv(io.StringIO(resp.text))
        return df

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _download_json(self, url: str) -> dict[str, Any] | None:
        """Fetch a CBOE JSON page; None when the CDN has no page (403/404).

        For the per-day put/call pages a 403 is how Cboe answers a date with
        no session (weekend, holiday, today before publication), so it is not
        an error here; ``pull_put_call_ratio`` decides what an all-refused
        window means.
        """
        headers = {**_HEADERS, "Accept": "application/json,*/*"}
        resp = requests.get(url, headers=headers, timeout=_REQUEST_TIMEOUT)
        if resp.status_code in (403, 404):
            return None
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, dict) else None

    @staticmethod
    def _page_ratio(page: dict[str, Any], name: str) -> float | None:
        """The page's published ``ratios[]`` value called ``name``, or None if absent/invalid."""
        for item in page.get("ratios") or []:
            if isinstance(item, dict) and item.get("name") == name:
                try:
                    value = float(item.get("value"))
                except (TypeError, ValueError):
                    return None
                return value if math.isfinite(value) and value > 0 else None
        return None

    @classmethod
    def _total_put_call(cls, page: dict[str, Any]) -> float | None:
        """The published TOTAL PUT/CALL RATIO, or None if absent/invalid."""
        return cls._page_ratio(page, PUT_CALL_RATIO_NAME)

    @staticmethod
    def _page_volume(page: dict[str, Any], block: str) -> dict[str, Any]:
        """``block``'s VOLUME row as raw_payload fields (None where the page has none)."""
        for item in page.get(block) or []:
            if isinstance(item, dict) and item.get("name") == "VOLUME":
                return {"put_volume": item.get("put"), "call_volume": item.get("call")}
        return {"put_volume": None, "call_volume": None}

    def pull_put_call_ratio(self, days_back: int | None = None) -> list[dict[str, Any]]:
        """Store the total and equity put/call ratios for recent trading days.

        A weekday in the window is requested when any series in
        PUT_CALL_PAGE_SERIES lacks it, once for all of them; each series
        stores only the dates it lacks. obs_date is the session date in the
        page URL. Three steps, so no transaction is open while pages are
        read: look up the stored dates (a read-only connection), read the
        pages, then insert in one transaction that opens after the last read.
        The insert transaction looks the dates up again and skips any a
        concurrent run stored meanwhile. pull_timestamp (DEFAULT NOW()) is
        that transaction's start: at or after every page's read, a proxy for
        the acquisition time (module docstring). Returns one result per
        series, so a page that loses one ratio fails that series alone.
        """
        lookback = days_back if days_back is not None else PUT_CALL_DEFAULT_LOOKBACK_DAYS
        end = date.today()
        start = end - timedelta(days=max(lookback, 0))
        series = list(PUT_CALL_PAGE_SERIES)
        rows = dict.fromkeys(series, 0)
        requested = dict.fromkeys(series, 0)
        refused = dict.fromkeys(series, 0)
        unparsed = dict.fromkeys(series, 0)
        pages_requested = 0
        read: list[tuple[str, date, float, dict[str, Any]]] = []
        try:
            with self.engine.connect() as conn:
                existing = {
                    sid: self._get_existing_dates(sid, conn, start_date=start, end_date=end)
                    for sid in series
                }
            day = start
            while day <= end:
                missing = [sid for sid in series if day not in existing[sid]]
                if day.weekday() < 5 and missing:
                    url = PUT_CALL_URL.format(day=day.isoformat())
                    pages_requested += 1
                    page = self._download_json(url)
                    time.sleep(_PUT_CALL_DELAY)
                    for sid in missing:
                        requested[sid] += 1
                        if page is None:
                            refused[sid] += 1
                            continue
                        name, block = PUT_CALL_PAGE_SERIES[sid]
                        value = self._page_ratio(page, name)
                        if value is None:
                            unparsed[sid] += 1
                            log.warning(
                                "CBOE put/call {d}: page has no valid {n}", d=day, n=name,
                            )
                            continue
                        read.append((sid, day, value, {"source_url": url, **self._page_volume(page, block)}))
                day += timedelta(days=1)
            if read:
                # Opens after the last page was read: NOW() is at or after
                # every read below.
                with self.engine.begin() as conn:
                    stored = {
                        sid: self._get_existing_dates(sid, conn, start_date=start, end_date=end)
                        for sid in {sid for sid, *_ in read}
                    }
                    for sid, day, value, payload in read:
                        if day in stored[sid]:
                            continue
                        self._insert_raw(
                            conn=conn,
                            series_id=sid,
                            obs_date=day,
                            value=value,
                            raw_payload=payload,
                        )
                        rows[sid] += 1
        except Exception as exc:
            log.warning("CBOE put/call pull failed: {e}", e=str(exc))
            return [{"status": "FAILED", "rows_inserted": 0, "feature_name": sid, "error": str(exc)}
                    for sid in series]

        log.info(
            "CBOE put/call — {n} rows inserted ({r} pages requested, {x} no session page)",
            n=sum(rows.values()), r=pages_requested,
            x=max(refused.values(), default=0),
        )
        results: list[dict[str, Any]] = []
        for sid in series:
            name = PUT_CALL_PAGE_SERIES[sid][0]
            if requested[sid] >= PUT_CALL_ALL_REFUSED_MIN and refused[sid] == requested[sid]:
                results.append({"status": "FAILED", "rows_inserted": 0, "feature_name": sid,
                                "error": f"all {requested[sid]} daily put/call pages refused (403/404)"})
            elif unparsed[sid] and not rows[sid]:
                results.append({"status": "FAILED", "rows_inserted": 0, "feature_name": sid,
                                "error": f"{unparsed[sid]} put/call page(s) without {name}"})
            else:
                results.append({"status": "SUCCESS", "rows_inserted": rows[sid], "feature_name": sid})
        return results

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

        results.extend(self.pull_put_call_ratio(days_back=days_back))

        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        total_rows = sum(r["rows_inserted"] for r in results)
        log.info(
            "CBOE pull_all — {ok}/{total} indices, {rows} rows",
            ok=succeeded,
            total=len(results),
            rows=total_rows,
        )
        return results
