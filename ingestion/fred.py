"""
GRID FRED data ingestion module.

Pulls economic time series from the Federal Reserve Economic Data (FRED) API
using the ``fedfred`` library and stores raw observations in ``raw_series``.
Includes deduplication, rate limiting, release-date retrieval, and full error
handling.
"""

from __future__ import annotations

import json
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
from fedfred import FredAPI
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# Default FRED series to pull
FRED_SERIES_LIST: list[str] = [
    "T10Y2Y",
    "T10Y3M",
    "DFF",
    "VIXCLS",
    "USSLIND",
    "CPIAUCSL",
    "CPALTT01USM657N",  # CPI YoY % change (actual inflation rate)
    "MANEMP",
    "UNRATE",
    "HOUST",
    "DSPIC96",
    "M2SL",
    "WALCL",
    "BAMLH0A0HYM2",
    "BAMLC0A0CM",
    "TEDRATE",
    "T5YIE",
    "UMCSENT",
    "ICSA",
    "RETAILSMNSA",
    "INDPRO",
    "DEXUSEU",
    "DEXJPUS",
    "DEXCAUS",
    "DEXSZUS",
    "DEXUSUK",
    "PAYEMS",
    "RSAFS",
    "BOPGTB",
    "WTREGEN",
    "PERMIT",
    "CCSA",
    "PCEPI",
    "PCEPILFE",
    "TCU",
    # "NAPM",  # Discontinued — ISM revoked FRED redistribution Jan 2024
    # Yield curve tenors
    "DGS1",
    "DGS2",
    "DGS5",
    "DGS30",
    "DFII10",
    "T10YIE",
    # Fed liquidity equation components (used by altdata/fed_liquidity.py)
    "RRPONTSYD",
    "WSHOSHO",
    "SWPT",
    "H8B1023NCBCMG",
    "TOTRESNS",
    # ── Capital flow pipeline: credit layer ──
    "TOTBKCR",          # Total bank credit, all commercial banks (weekly)
    "BUSLOANS",         # Commercial and Industrial loans
    "DRTSCIS",          # Sr Loan Officer Survey: tightening standards on C&I
    "WDTOTAL",          # Total public debt outstanding (proxy for Treasury issuance)
    "CCLACBW027SBOG",   # Consumer loans, all commercial banks (weekly)
    "RHEACBW027SBOG",   # Real estate loans, all commercial banks (weekly)
    # ── Capital flow pipeline: sovereign/cross-border layer ──
    "BOGZ1FL263061103Q",  # Foreign official holdings of US Treasuries (quarterly)
    # ── Capital flow pipeline: additional FX ──
    "DEXCHUS",          # Chinese Yuan per USD
    # ── CDS proxy / credit spread granularity ──
    "BAMLC0A4CBBB",     # ICE BofA BBB Corporate OAS (~CDX NA IG proxy)
    "BAMLH0A1HYBB",     # ICE BofA BB US High Yield OAS
    "BAMLH0A2HYB",      # ICE BofA B US High Yield OAS
    "BAMLH0A3HYC",      # ICE BofA CCC & Lower OAS (deep distress)
    "BAMLHE00EHYIOAS",  # ICE BofA Euro High Yield OAS
    "DRTSCILM",         # Net % banks tightening C&I large/medium (quarterly)
    # ── Bug fixes: series queried by layers but never ingested ──
    "SOFR",             # Secured Overnight Financing Rate (layer_credit.py)
    "FEDFUNDS",         # Effective Fed Funds Rate monthly (layer_monetary.py)
    # ── Financial conditions / stress indices ──
    "NFCI",             # Chicago Fed National Financial Conditions Index
    "STLFSI2",          # St. Louis Fed Financial Stress Index
    # ── Dollar / FX ──
    "DTWEXBGS",         # Trade-Weighted USD Index (Broad)
    # ── Consumer credit ──
    "TOTALSL",          # Total Consumer Credit Outstanding
    "REVOLSL",          # Revolving Consumer Credit (credit cards)
    # ── Monetary depth ──
    "M2V",              # Velocity of M2 Money Stock
    "BOGMBASE",         # Monetary Base (Total)
    # ── Corporate / industrial ──
    "NEWORDER",         # Manufacturers' New Orders
    "CPATAX",           # Corporate Profits After Tax (quarterly)
    # ── Real rates ──
    "REAINTRATREARAT1YE",  # 1-Year Real Interest Rate
    # ── Breadth ──
    # FRED does not publish NYSE advance/decline issues under ADVFN/DECFN.
    # Keep breadth on the market-data path instead of hammering FRED with
    # invalid series IDs every scheduler cycle.
    # ── Consumer credit health ──
    "DRCCLACBS",           # Credit card delinquency rate
    "DRSFRMACBS",          # Mortgage delinquency rate
    "TDSP",                # Household debt service ratio
    "DRBLACBS",            # Business loan delinquency
    # ── Labor depth (JOLTS) ──
    "JTSJOL",              # Job openings
    "JTSQUR",              # Quits rate
    "JTSHIR",              # Hiring rate
    # ── Housing ──
    "CSUSHPINSA",          # Case-Shiller Home Price Index
    "MORTGAGE30US",        # 30-Year Mortgage Rate
    "MSACSR",              # Monthly Supply of New Houses
    # ── EM bonds ──
    "BAMLEMHBHYCRPIOAS",   # EM High Yield OAS
    "BAMLEMCLLOTRUSD",     # EM Corporate Total Return
    # ── Activity ──
    "CFNAI",               # Chicago Fed National Activity Index
    # ── Margin debt / leverage ──
    "BOGZ1FL663067003Q",   # Security brokers/dealers margin accounts (quarterly)
    # ── Money market / liquidity ──
    "MMMFFAQ027S",         # Money market fund total assets (quarterly)
    "WRMFNS",              # Retail money market funds (weekly)
    "RRPONTSYD",           # Overnight reverse repo (daily)
]

# Minimum delay between FRED API calls (seconds)
_RATE_LIMIT_DELAY: float = 0.25


def _extract_http_status_code(exc: BaseException) -> int | None:
    """Best-effort status extraction across HTTP and retry wrappers."""
    direct_status = getattr(exc, "status_code", None)
    if isinstance(direct_status, int):
        return direct_status

    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    if isinstance(status_code, int):
        return status_code
    status_code = getattr(response, "status", None)
    if isinstance(status_code, int):
        return status_code

    last_attempt = getattr(exc, "last_attempt", None)
    if last_attempt is not None:
        try:
            inner = last_attempt.exception()
        except Exception:
            inner = None
        if isinstance(inner, BaseException) and inner is not exc:
            status_code = _extract_http_status_code(inner)
            if status_code is not None:
                return status_code

    for inner in (getattr(exc, "__cause__", None), getattr(exc, "__context__", None)):
        if isinstance(inner, BaseException) and inner is not exc:
            status_code = _extract_http_status_code(inner)
            if status_code is not None:
                return status_code

    for arg in getattr(exc, "args", ()):
        if isinstance(arg, BaseException) and arg is not exc:
            status_code = _extract_http_status_code(arg)
            if status_code is not None:
                return status_code

    match = re.search(r"\b(400|401|403|404|429|500|502|503|504)\b", f"{exc!r} {exc}")
    if match:
        return int(match.group(1))

    return None


def _contains_http_status_error(exc: BaseException) -> bool:
    """Return true if a retry wrapper contains an HTTP status exception."""
    if "HTTPStatusError" in type(exc).__name__ or "HTTPError" in type(exc).__name__:
        return True
    if "HTTPStatusError" in f"{exc!r} {exc}" or "HTTPError" in f"{exc!r} {exc}":
        return True

    last_attempt = getattr(exc, "last_attempt", None)
    if last_attempt is not None:
        try:
            inner = last_attempt.exception()
        except Exception:
            inner = None
        if isinstance(inner, BaseException) and inner is not exc:
            return _contains_http_status_error(inner)

    for inner in (getattr(exc, "__cause__", None), getattr(exc, "__context__", None)):
        if isinstance(inner, BaseException) and inner is not exc:
            if _contains_http_status_error(inner):
                return True

    for arg in getattr(exc, "args", ()):
        if isinstance(arg, BaseException) and arg is not exc:
            if _contains_http_status_error(arg):
                return True

    return False


def _normalise_observation_frame(data: pd.DataFrame, series_id: str) -> pd.DataFrame | None:
    """Return a FRED observation frame with canonical date/value columns.

    fedfred may put the actual observation date in the index and use a column
    named ``date`` for the realtime vintage date. Prefer a date-like index so
    monthly/weekly series do not collapse onto the pull/vintage date.
    """
    result = pd.DataFrame()
    date_col_names = ("date", "Date", "observation_date")
    value_col_names = ("value", "Value", series_id)

    idx = data.index
    if isinstance(idx, pd.DatetimeIndex):
        result["date"] = pd.Series(idx.to_numpy())
    elif idx.name in date_col_names:
        result["date"] = pd.Series(pd.to_datetime(idx, errors="coerce").to_numpy())

    for col in date_col_names:
        if "date" not in result.columns and col in data.columns:
            result["date"] = pd.to_datetime(data[col], errors="coerce").to_numpy()
            break

    if (
        "date" not in result.columns
        and idx.name is not None
        and not pd.api.types.is_integer_dtype(idx.dtype)
    ):
        parsed_index = pd.to_datetime(idx, errors="coerce")
        if pd.Series(parsed_index).notna().any():
            result["date"] = pd.Series(parsed_index.to_numpy())

    if (
        "date" not in result.columns
        and len(data.columns) == 1
        and not isinstance(idx, pd.RangeIndex)
        and not pd.api.types.is_integer_dtype(idx.dtype)
    ):
        parsed_index = pd.to_datetime(idx, errors="coerce")
        if pd.Series(parsed_index).notna().any():
            result["date"] = pd.Series(parsed_index.to_numpy())

    for col in value_col_names:
        if col in data.columns:
            result["value"] = pd.to_numeric(data[col], errors="coerce").to_numpy()
            break

    if "value" not in result.columns:
        excluded = set(date_col_names) | {"realtime_start", "realtime_end"}
        numeric_cols = [c for c in data.select_dtypes(include=["number"]).columns if c not in excluded]
        if numeric_cols:
            result["value"] = pd.to_numeric(data[numeric_cols[0]], errors="coerce").to_numpy()
        elif len(data.columns) > 0:
            result["value"] = pd.to_numeric(data.iloc[:, -1], errors="coerce").to_numpy()

    if "date" not in result.columns or "value" not in result.columns:
        return None

    return result


class FREDPuller(BasePuller):
    """Pulls time series data from the FRED API into ``raw_series``.

    Attributes:
        fred: fedfred.FredAPI client instance.
        engine: SQLAlchemy engine for database writes.
        source_id: The ``source_catalog.id`` for the FRED source.
    """

    SOURCE_NAME: str = "FRED"

    def __init__(self, api_key: str, db_engine: Engine) -> None:
        """Initialise the FRED puller.

        Parameters:
            api_key: FRED API key.
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        self.fred = FredAPI(api_key)
        super().__init__(db_engine)
        log.info("FREDPuller initialised — source_id={sid}", sid=self.source_id)

    def pull_series(
        self,
        series_id: str,
        start_date: str | date = "1990-01-01",
        end_date: str | date | None = None,
    ) -> dict[str, Any]:
        """Fetch a single series from FRED and insert into raw_series.

        Parameters:
            series_id: FRED series identifier (e.g. 'T10Y2Y').
            start_date: Earliest observation date to fetch.
            end_date: Latest observation date (default: today).

        Returns:
            dict: Result with keys ``series_id``, ``rows_inserted``,
                  ``status``, ``errors``.
        """
        log.info("Pulling FRED series {sid} from {sd}", sid=series_id, sd=start_date)
        result: dict[str, Any] = {
            "series_id": series_id,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            obs_kwargs: dict[str, Any] = {
                "observation_start": str(start_date),
            }
            if end_date:
                obs_kwargs["observation_end"] = str(end_date)

            data: pd.DataFrame = self.fred.get_series_observations(
                series_id, **obs_kwargs
            )

            if data is None or data.empty:
                log.warning("FRED returned no data for {sid}", sid=series_id)
                result["status"] = "PARTIAL"
                result["errors"].append("No data returned")
                return result

            normalised = _normalise_observation_frame(data, series_id)
            if normalised is None:
                msg = f"Unknown column layout: {list(data.columns)}"
                log.warning(
                    "FRED {sid}: {msg}; skipping without failure row",
                    sid=series_id,
                    msg=msg,
                )
                result["status"] = "SKIPPED"
                result["errors"].append(msg)
                return result
            data = normalised

            # Defence-in-depth: normalise contract is "frame has date+value or
            # is None" but if anything mutates it after, we want a clean log
            # rather than a cryptic KeyError.
            missing = [c for c in ("date", "value") if c not in data.columns]
            if missing:
                msg = f"normalised frame missing columns {missing}; cols={list(data.columns)}"
                log.warning("FRED {sid}: {msg}; skipping", sid=series_id, msg=msg)
                result["status"] = "SKIPPED"
                result["errors"].append(msg)
                return result

            # fedfred may return dates in the value column (columns swapped or
            # both columns contain dates).  Detect this by checking if the value
            # column looks like dates and the date column looks numeric.
            if "date" in data.columns and "value" in data.columns and len(data) > 0:
                sample_val = data["value"].dropna().iloc[0] if not data["value"].dropna().empty else None
                sample_date = data["date"].dropna().iloc[0] if not data["date"].dropna().empty else None
                # If value looks like a date string and date looks numeric, swap them
                if sample_val is not None and isinstance(sample_val, str):
                    val_looks_datelike = any(c in str(sample_val) for c in ["-", "/"]) and len(str(sample_val)) >= 8
                    date_is_numeric = pd.to_numeric(pd.Series([sample_date]), errors="coerce").notna().iloc[0]
                    if val_looks_datelike and date_is_numeric:
                        log.warning(
                            "FRED {sid}: detected date/value column swap — correcting",
                            sid=series_id,
                        )
                        data = data.rename(columns={"date": "value", "value": "date"})

            # Drop rows where value is NaN or '.'
            data = data[data["value"].apply(
                lambda v: v != "." and pd.notna(v)
            )].copy()
            pre_coerce_count = len(data)
            data["value"] = pd.to_numeric(data["value"], errors="coerce")
            coerced_count = data["value"].isna().sum()
            if coerced_count > 0:
                log.warning(
                    "Coerced {n} non-numeric values to NaN for series {sid}",
                    n=int(coerced_count),
                    sid=series_id,
                )
            data = data.dropna(subset=["value"])

            inserted = 0

            with self.engine.begin() as conn:
                # Batch fetch all existing dates — one query instead of N
                existing_dates = self._get_existing_dates(series_id, conn)
                skipped = 0
                for _, row in data.iterrows():
                    try:
                        obs_date_val = (
                            row["date"].date()
                            if hasattr(row["date"], "date") and callable(row["date"].date)
                            else pd.Timestamp(row["date"]).date()
                        )
                    except Exception as e:
                        bad_date = row["date"] if "date" in row else None
                        log.warning(
                            "FRED {sid}: bad date value {v}: {e}, skipping row",
                            sid=series_id, v=repr(bad_date), e=str(e),
                        )
                        continue
                    if obs_date_val in existing_dates:
                        skipped += 1
                        continue
                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                        ),
                        {
                            "sid": series_id,
                            "src": self.source_id,
                            "od": obs_date_val,
                            "val": float(row["value"]),
                        },
                    )
                    inserted += 1
                if skipped:
                    log.debug("FRED {sid}: skipped {n} existing rows", sid=series_id, n=skipped)

            result["rows_inserted"] = inserted
            log.info(
                "FRED {sid}: inserted {n} rows",
                sid=series_id,
                n=inserted,
            )

        except Exception as exc:
            status_code = _extract_http_status_code(exc)
            if status_code in (400, 403, 404, 429) or (
                status_code is None and _contains_http_status_error(exc)
            ):
                status_desc = f"HTTP {status_code}" if status_code else "HTTP rejection"
                message = (
                    f"FRED series unavailable or not entitled "
                    f"({status_desc})"
                )
                log.warning(
                    "FRED {sid}: {msg}; skipping without failure row",
                    sid=series_id,
                    msg=message,
                )
                result["status"] = "SKIPPED"
                result["errors"].append(message)
                return result

            # KeyError on 'date' / 'value' indicates fedfred returned a frame
            # shape we didn't anticipate — log a WARNING with the actual
            # column layout so we can fix the normaliser, but don't flood
            # errors.jsonl with the bare repr (was ~1,500 ERRORs/cycle).
            if isinstance(exc, KeyError) and str(exc) in ("'date'", "'value'"):
                log.warning(
                    "FRED {sid}: unexpected frame shape (missing {k}); "
                    "skipping without failure row",
                    sid=series_id,
                    k=str(exc),
                )
                result["status"] = "SKIPPED"
                result["errors"].append(f"missing column {exc}")
                return result

            # Use opt(exception=True) so the GitSink captures the full
            # traceback — without it, KeyError('date') logs as just "'date'"
            # and the underlying root cause is invisible (was 2010 mystery
            # entries in errors.jsonl).
            log.opt(exception=True).error(
                "FRED pull failed for {sid}: {err}",
                sid=series_id, err=str(exc),
            )
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

            # Record the failure row
            try:
                with self.engine.begin() as conn:
                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, "
                            "raw_payload, pull_status) "
                            "VALUES (:sid, :src, :od, 0, :payload, 'FAILED')"
                        ),
                        {
                            "sid": series_id,
                            "src": self.source_id,
                            "od": date.today(),
                            "payload": json.dumps({"error": str(exc)}),
                        },
                    )
            except Exception as insert_exc:
                log.error(
                    "Failed to record error row for {sid}: {err}",
                    sid=series_id,
                    err=str(insert_exc),
                )

        # Rate limiting
        time.sleep(_RATE_LIMIT_DELAY)
        return result

    def pull_all(
        self,
        series_list: list[str] | None = None,
        start_date: str | date = "1990-01-01",
        end_date: str | date | None = None,
    ) -> list[dict[str, Any]]:
        """Pull multiple FRED series sequentially.

        Never stops on a single-series failure — logs and continues.

        Parameters:
            series_list: List of FRED series IDs.  Defaults to FRED_SERIES_LIST.
            start_date: Earliest observation date.
            end_date: Latest observation date (default: today).

        Returns:
            list[dict]: One result dict per series.
        """
        if series_list is None:
            series_list = FRED_SERIES_LIST

        log.info(
            "Starting FRED bulk pull — {n} series from {sd}",
            n=len(series_list),
            sd=start_date,
        )
        results: list[dict[str, Any]] = []
        for sid in series_list:
            # Use incremental start: only fetch from last known date - 7 day overlap
            latest = self._get_latest_date(sid)
            effective_start = start_date
            if latest is not None:
                incremental = latest - timedelta(days=7)
                # Use whichever is more recent
                start_as_date = date.fromisoformat(str(start_date)) if isinstance(start_date, str) else start_date
                if incremental > start_as_date:
                    effective_start = incremental.isoformat()
                    log.info("FRED {sid}: incremental from {d} (last={l})", sid=sid, d=effective_start, l=latest)
            res = self.pull_series(sid, effective_start, end_date)
            results.append(res)
        log.info(
            "FRED bulk pull complete — {ok}/{total} succeeded",
            ok=sum(1 for r in results if r["status"] == "SUCCESS"),
            total=len(results),
        )
        return results

    def get_release_dates(self, series_id: str) -> dict[date, date]:
        """Retrieve release-date metadata for a FRED series.

        Uses the FRED vintage dates endpoint via fedfred. Falls back to
        pull_timestamp from raw_series if unavailable.

        Parameters:
            series_id: FRED series identifier.

        Returns:
            dict: Mapping of observation date to release date.
        """
        log.info("Fetching release dates for {sid}", sid=series_id)
        mapping: dict[date, date] = {}

        try:
            # fedfred supports vintage dates via get_series_vintagedates
            vintages = self.fred.get_series_vintagedates(series_id)
            if vintages is not None and not vintages.empty:
                # vintages is a DataFrame/Series of realtime dates
                # For each vintage, pull observations to build obs_date -> release_date map
                for vdate in vintages.head(50).values:
                    vd = pd.Timestamp(vdate).date() if not isinstance(vdate, date) else vdate
                    try:
                        obs = self.fred.get_series_observations(
                            series_id,
                            realtime_start=str(vd),
                            realtime_end=str(vd),
                        )
                        if obs is not None and not obs.empty:
                            for _, row in obs.iterrows():
                                od = pd.Timestamp(row["date"]).date()
                                if od not in mapping or vd < mapping[od]:
                                    mapping[od] = vd
                    except Exception as exc:
                        log.warning("FRED vintage fetch failed for {s}: {e}", s=series_id, e=exc)
                        continue
                    time.sleep(_RATE_LIMIT_DELAY)

                if mapping:
                    log.info(
                        "Got {n} release dates for {sid} via fedfred vintages",
                        n=len(mapping),
                        sid=series_id,
                    )
                    return mapping
        except Exception as exc:
            log.warning(
                "Could not fetch release dates for {sid} from FRED: {err}. "
                "Falling back to pull_timestamp.",
                sid=series_id,
                err=str(exc),
            )

        # Fallback: use pull_timestamp from raw_series
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT obs_date, pull_timestamp::date AS release_date "
                    "FROM raw_series "
                    "WHERE series_id = :sid AND source_id = :src "
                    "AND pull_status = 'SUCCESS' "
                    "ORDER BY obs_date"
                ),
                {"sid": series_id, "src": self.source_id},
            ).fetchall()
            for row in rows:
                mapping[row[0]] = row[1]

        log.info(
            "Got {n} release dates for {sid} via pull_timestamp fallback",
            n=len(mapping),
            sid=series_id,
        )
        return mapping


if __name__ == "__main__":
    from config import settings
    from db import get_engine

    puller = FREDPuller(api_key=settings.FRED_API_KEY, db_engine=get_engine())
    results = puller.pull_all(start_date="2020-01-01")
    for r in results:
        print(f"  {r['series_id']}: {r['status']} ({r['rows_inserted']} rows)")
