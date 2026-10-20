"""
GRID FINRA ATS (Dark Pool) volume ingestion module.

Pulls dark pool trading data from FINRA's OTC Transparency API:
1. ATS (Alternative Trading System) weekly volume data -- total dark pool
   volume and dark pool percentage of total volume (DIX proxy).
2. Consolidated short interest -- bimonthly short interest reports.

Data sources:
- ATS volume: https://api.finra.org/data/group/otcMarket/name/weeklySummary
- Short interest: https://api.finra.org/data/group/otcMarket/name/consolidatedShortInterest

Series stored:
- finra.ats_total_volume: Total ATS (dark pool) volume across tracked tickers
- finra.ats_dark_pct: Dark pool volume as percentage of total volume (DIX proxy)
- finra.short_interest_total: Consolidated short interest across tracked tickers

ATS request history (2026-10-19): this module used to POST to
``otcMarket/name/weeklyDownloadDetails`` with ``sortFields`` and failed every
run with HTTP 400. ``weeklyDownloadDetails`` is not a dataset in FINRA's API
catalog or its OTC Transparency API spec (v0.4), where the weekly ATS data is
``otcMarket/name/weeklySummary``. FINRA's Query API docs also say sorting
needs an EQUAL compareFilter on every partition field (weeklySummary's are
weekStartDate and tierIdentifier). The ATS request now matches
``dark_pool.py``, which was fixed for the same 400 on the same dataset:
``dateRangeFilters`` on weekStartDate plus ``domainFilters`` on
issueSymbolIdentifier, and no ``sortFields``.
"""

from __future__ import annotations

import hashlib
import json
import time
from datetime import date, timedelta
from typing import Any, NamedTuple

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.altdata.dark_pool import parse_ats_weekly_records
from ingestion.base import BasePuller, retry_on_failure

# ---- URLs ----
_ATS_VOLUME_URL: str = (
    "https://api.finra.org/data/group/otcMarket/name/weeklySummary"
)
_SHORT_INTEREST_URL: str = (
    "https://api.finra.org/data/group/otcMarket/name/consolidatedShortInterest"
)

# Series ID prefix
_SERIES_PREFIX: str = "finra"

# Tickers to track (major ETFs + mega-cap equities)
TRACKED_TICKERS: list[str] = ["SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "TSLA"]

# Feature definitions
FINRA_ATS_FEATURES: dict[str, str] = {
    "ats_total_volume": (
        "Total ATS (dark pool) volume across major tickers "
        "(SPY, QQQ, IWM, AAPL, MSFT, NVDA, TSLA)"
    ),
    "ats_dark_pct": (
        "Dark pool volume as pct of total volume (DIX proxy, 0-100)"
    ),
    "short_interest_total": (
        "Consolidated short interest across major tickers"
    ),
}

# HTTP config
_REQUEST_TIMEOUT: int = 45
_RATE_LIMIT_DELAY: float = 2.0

# FINRA API pagination
_PAGE_SIZE: int = 5000

# weeklySummary holds a rolling 12 months (older weeks live in
# weeklySummaryHistoric), so the ATS fetch window never reaches further back.
_ATS_WINDOW_DAYS: int = 365

# Upper bound on weeklySummary pages per pull. FINRA's synchronous API can
# return fewer rows than ``limit`` (response-size cap) while more remain, so
# pages are counted rather than assumed full. FINRA caps ``offset`` at 500,000,
# i.e. 100 full pages of 5000; a 7-ticker, 12-month pull is ~20k rows.
_MAX_ATS_PAGES: int = 100

# Response header carrying the total number of matching records.
_RECORD_TOTAL_HEADER: str = "Record-Total"


class _ATSPage(NamedTuple):
    """One weeklySummary page plus the server's total match count.

    ``total`` is the validated ``Record-Total`` header, or None when the
    header is missing or not a non-negative integer.
    """

    records: Any
    total: int | None


class _ATSPaginationError(RuntimeError):
    """Coverage of a paginated weeklySummary pull could not be proven."""


def _record_total(headers: Any) -> int | None:
    """Validated ``Record-Total`` response header, or None."""
    try:
        value = headers.get(_RECORD_TOTAL_HEADER)
    except AttributeError:
        return None
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


class FINRAATSPuller(BasePuller):
    """Pulls FINRA ATS dark pool volume and short interest data.

    FINRA publishes weekly ATS volume data showing how much trading
    occurs in dark pools vs lit exchanges. High dark pool activity
    (high DIX) has historically been a contrarian bullish signal,
    as institutional investors tend to accumulate in dark pools
    before price moves.

    Short interest data is published bimonthly and indicates
    aggregate short positioning across major securities.

    Features:
    - finra.ats_total_volume: Aggregate dark pool volume
    - finra.ats_dark_pct: Dark pool % of total volume
    - finra.short_interest_total: Aggregate short interest

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for FINRA_ATS.
    """

    SOURCE_NAME: str = "FINRA_ATS"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://api.finra.org/data/group/otcMarket",
        "cost_tier": "FREE",
        "latency_class": "WEEKLY",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 35,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the FINRA ATS puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info(
            "FINRAATSPuller initialised -- source_id={sid}",
            sid=self.source_id,
        )

    def _series_id(self, feature: str) -> str:
        """Build the full series_id for a feature.

        Parameters:
            feature: Feature suffix (e.g., 'ats_total_volume').

        Returns:
            Full series_id (e.g., 'finra.ats_total_volume').
        """
        return f"{_SERIES_PREFIX}.{feature}"

    # ------------------------------------------------------------------ #
    # ATS Volume Data
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
    def _fetch_ats_page(
        self,
        offset: int = 0,
        limit: int = _PAGE_SIZE,
        tickers: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> _ATSPage:
        """Fetch a page of weeklySummary ATS data from the FINRA API.

        Uses the same request shape as ``dark_pool.py``: a
        ``dateRangeFilters`` on weekStartDate and a ``domainFilters`` on
        issueSymbolIdentifier, without ``sortFields`` (sorting needs EQUAL
        compareFilters on every partition key, otherwise FINRA returns 400).

        Parameters:
            offset: Pagination offset.
            limit: Page size.
            tickers: List of ticker symbols to filter on.
            start_date: Start of the weekStartDate range (inclusive).
                Defaults to ``_ATS_WINDOW_DAYS`` before today.
            end_date: End of the weekStartDate range (inclusive).
                Defaults to today.

        Returns:
            ``_ATSPage``: the page's records (empty on HTTP 204) and the
            validated ``Record-Total`` header (None if missing/malformed).

        Raises:
            requests.RequestException: On HTTP errors after retries.
        """
        headers = {
            "User-Agent": "GRID-DataPuller/1.0",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        if end_date is None:
            end_date = date.today()
        if start_date is None:
            start_date = end_date - timedelta(days=_ATS_WINDOW_DAYS)

        query: dict[str, Any] = {
            "offset": offset,
            "limit": limit,
            "dateRangeFilters": [
                {
                    "fieldName": "weekStartDate",
                    "startDate": start_date.isoformat(),
                    "endDate": end_date.isoformat(),
                }
            ],
        }

        if tickers:
            query["domainFilters"] = [
                {
                    "fieldName": "issueSymbolIdentifier",
                    "values": tickers,
                }
            ]

        resp = requests.post(
            _ATS_VOLUME_URL,
            json=query,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
        )

        total = _record_total(resp.headers)

        # 204 = No Content (valid response, just no data for this range)
        if resp.status_code == 204:
            return _ATSPage([], total)

        resp.raise_for_status()
        return _ATSPage(resp.json(), total)

    def _parse_ats_records(
        self,
        records: list[dict[str, Any]],
    ) -> dict[date, dict[str, float]]:
        """Aggregate weeklySummary ATS records by week across tracked tickers.

        Per-ticker ATS volume comes from
        :func:`ingestion.altdata.dark_pool.parse_ats_weekly_records`: ATS
        rows only (no non-ATS OTC volume), with the symbol-level
        ``ATS_W_SMBL`` aggregate preferred over summed per-firm rows, never
        both. Those per-ticker volumes are then summed per week.

        weeklySummary carries no consolidated (exchange + OTC) volume, so
        ``total_volume`` stays 0 and ``dark_pct`` is 0.0; the caller only
        stores ``ats_dark_pct`` when ``total_volume`` is positive.

        Parameters:
            records: Raw FINRA weeklySummary records.

        Returns:
            Dict mapping obs_date to aggregated metrics:
            {date: {'ats_volume': float, 'total_volume': float,
                     'dark_pct': float, 'raw_records': int}}, where
            raw_records is the number of tickers contributing that week.
        """
        by_week: dict[date, dict[str, float]] = {}

        for weeks in parse_ats_weekly_records(records).values():
            for obs_date, metrics in weeks.items():
                agg = by_week.setdefault(
                    obs_date,
                    {"ats_volume": 0.0, "total_volume": 0.0, "raw_records": 0},
                )
                agg["ats_volume"] += metrics["volume"]
                agg["raw_records"] += 1

        for agg in by_week.values():
            agg["dark_pct"] = 0.0

        return by_week

    def pull_ats_volume(
        self,
        start_date: str | date = "2020-01-01",
        days_back: int | None = None,
        tickers: list[str] | None = None,
    ) -> dict[str, Any]:
        """Pull ATS dark pool volume data and store ats_total_volume + ats_dark_pct.

        Uses _get_existing_dates() for efficient batch deduplication.

        Pagination: FINRA's synchronous API may return fewer rows than
        ``limit`` while more remain, so a short page is not treated as the
        last one. The offset advances by the rows actually returned, and
        the pull is complete only when it reaches the ``Record-Total``
        header. A non-empty page without a valid total, a changing total,
        rows past the total, an empty page before the total, a repeated
        page, or more than ``_MAX_ATS_PAGES`` pages all fail the pull, and
        nothing is stored (incomplete coverage is never written as
        success). An empty first page (HTTP 204) with no or zero total is
        an empty result.

        Parameters:
            start_date: Earliest observation date to store.
            days_back: If set, only store data from this many days ago.
            tickers: Ticker symbols to track (default: TRACKED_TICKERS).

        Returns:
            dict with status, rows_inserted, per_feature counts, and the
            coverage evidence ``records_fetched`` / ``record_total``.
        """
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)

        if days_back is not None:
            cutoff = date.today() - timedelta(days=days_back)
            start_date = max(start_date, cutoff)

        if tickers is None:
            tickers = TRACKED_TICKERS

        # weeklySummary only holds a rolling 12 months; never ask for more.
        fetch_end = date.today()
        fetch_start = max(start_date, fetch_end - timedelta(days=_ATS_WINDOW_DAYS))

        # Fetch all pages; see the docstring for the completion rule.
        all_records: list[dict[str, Any]] = []
        offset = 0
        record_total: int | None = None
        previous_page_sha: str | None = None

        try:
            for _ in range(_MAX_ATS_PAGES):
                page = self._fetch_ats_page(
                    offset=offset,
                    limit=_PAGE_SIZE,
                    tickers=tickers,
                    start_date=fetch_start,
                    end_date=fetch_end,
                )
                records = page.records

                if not isinstance(records, list):
                    raise _ATSPaginationError(
                        f"weeklySummary body at offset {offset} is not a JSON list"
                    )

                if not records:
                    if offset == 0 and page.total in (None, 0):
                        break  # empty result for this window
                    raise _ATSPaginationError(
                        f"empty page at offset {offset} before "
                        f"{_RECORD_TOTAL_HEADER} "
                        f"{record_total if record_total is not None else page.total}"
                    )

                if page.total is None:
                    raise _ATSPaginationError(
                        f"missing or malformed {_RECORD_TOTAL_HEADER} header at "
                        f"offset {offset}; coverage of a {len(records)}-row page "
                        f"cannot be proven"
                    )
                if record_total is None:
                    record_total = page.total
                elif page.total != record_total:
                    raise _ATSPaginationError(
                        f"{_RECORD_TOTAL_HEADER} changed from {record_total} to "
                        f"{page.total} at offset {offset}"
                    )
                if offset + len(records) > record_total:
                    raise _ATSPaginationError(
                        f"{offset + len(records)} rows exceed "
                        f"{_RECORD_TOTAL_HEADER} {record_total}"
                    )

                page_sha = hashlib.sha256(
                    json.dumps(records, sort_keys=True, default=str).encode()
                ).hexdigest()
                if page_sha == previous_page_sha:
                    raise _ATSPaginationError(
                        f"page at offset {offset} repeats the previous page"
                    )
                previous_page_sha = page_sha

                all_records.extend(records)
                offset += len(records)
                log.debug(
                    "FINRA ATS: fetched {n} records, {o} of {t}",
                    n=len(records),
                    o=offset,
                    t=record_total,
                )

                if offset == record_total:
                    break

                time.sleep(_RATE_LIMIT_DELAY)
            else:
                raise _ATSPaginationError(
                    f"page budget of {_MAX_ATS_PAGES} pages exhausted at "
                    f"offset {offset} of {record_total}"
                )

        except Exception as exc:
            log.error("FINRA ATS volume pull failed: {e}", e=str(exc))
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "error": str(exc),
            }

        coverage = {"records_fetched": len(all_records), "record_total": record_total}

        if not all_records:
            log.warning("FINRA ATS: no records returned from API")
            return {"status": "SUCCESS", "rows_inserted": 0, **coverage}

        log.info(
            "FINRA ATS: fetched {n} total records", n=len(all_records)
        )

        # Parse and aggregate by week
        by_week = self._parse_ats_records(all_records)
        if not by_week:
            log.warning("FINRA ATS: no data parsed from records")
            return {"status": "SUCCESS", "rows_inserted": 0, **coverage}

        total_inserted = 0
        per_feature: dict[str, int] = {}

        with self.engine.begin() as conn:
            # --- ats_total_volume ---
            sid_vol = self._series_id("ats_total_volume")
            existing_vol = self._get_existing_dates(sid_vol, conn)
            inserted_vol = 0

            for obs_date, agg in sorted(by_week.items()):
                if obs_date < start_date:
                    continue
                if obs_date in existing_vol:
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=sid_vol,
                    obs_date=obs_date,
                    value=agg["ats_volume"],
                    raw_payload={
                        "raw_records": int(agg["raw_records"]),
                        "tickers": tickers,
                        "source_url": _ATS_VOLUME_URL,
                    },
                )
                inserted_vol += 1

            per_feature["ats_total_volume"] = inserted_vol
            total_inserted += inserted_vol
            log.info(
                "FINRA {sid}: {n} rows inserted",
                sid=sid_vol,
                n=inserted_vol,
            )

            # --- ats_dark_pct ---
            sid_pct = self._series_id("ats_dark_pct")
            existing_pct = self._get_existing_dates(sid_pct, conn)
            inserted_pct = 0

            for obs_date, agg in sorted(by_week.items()):
                if obs_date < start_date:
                    continue
                if obs_date in existing_pct:
                    continue
                # Only store if we have a meaningful percentage
                if agg["total_volume"] <= 0:
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=sid_pct,
                    obs_date=obs_date,
                    value=agg["dark_pct"],
                    raw_payload={
                        "ats_volume": agg["ats_volume"],
                        "total_volume": agg["total_volume"],
                        "tickers": tickers,
                        "source_url": _ATS_VOLUME_URL,
                    },
                )
                inserted_pct += 1

            per_feature["ats_dark_pct"] = inserted_pct
            total_inserted += inserted_pct
            log.info(
                "FINRA {sid}: {n} rows inserted",
                sid=sid_pct,
                n=inserted_pct,
            )

        return {
            "status": "SUCCESS",
            "rows_inserted": total_inserted,
            "per_feature": per_feature,
            **coverage,
        }

    # ------------------------------------------------------------------ #
    # Short Interest Data
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
    def _fetch_short_interest_page(
        self,
        offset: int = 0,
        limit: int = _PAGE_SIZE,
        tickers: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch a page of consolidated short interest data from FINRA.

        Parameters:
            offset: Pagination offset.
            limit: Page size.
            tickers: List of ticker symbols to filter on.

        Returns:
            List of record dicts from FINRA API.

        Raises:
            requests.RequestException: On HTTP errors after retries.
        """
        headers = {
            "User-Agent": "GRID-DataPuller/1.0",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        query: dict[str, Any] = {
            "offset": offset,
            "limit": limit,
            "sortFields": ["-settlementDate"],
        }

        if tickers:
            query["domainFilters"] = [
                {
                    "fieldName": "symbolCode",
                    "values": tickers,
                }
            ]

        resp = requests.post(
            _SHORT_INTEREST_URL,
            json=query,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def _parse_short_interest_records(
        self,
        records: list[dict[str, Any]],
    ) -> dict[date, dict[str, Any]]:
        """Aggregate short interest records by settlement date.

        Sums currentShortPositionQuantity across all tracked tickers
        for each settlement date.

        Parameters:
            records: Raw FINRA short interest records.

        Returns:
            Dict mapping obs_date to aggregated short interest data.
        """
        by_date: dict[date, dict[str, Any]] = {}

        for rec in records:
            date_str = rec.get("settlementDate")
            if not date_str:
                continue

            try:
                obs_date = date.fromisoformat(date_str[:10])
            except (ValueError, TypeError) as exc:
                log.warning(
                    "FINRA SI: bad date {d}: {e}",
                    d=date_str,
                    e=str(exc),
                )
                continue

            short_qty = rec.get("currentShortPositionQuantity")
            if short_qty is None:
                continue

            try:
                si_val = float(short_qty)
            except (ValueError, TypeError):
                log.warning(
                    "FINRA SI: bad short interest value {v} for {d}",
                    v=short_qty,
                    d=date_str,
                )
                continue

            if obs_date not in by_date:
                by_date[obs_date] = {
                    "short_interest": 0.0,
                    "raw_records": 0,
                    "symbols": [],
                }

            by_date[obs_date]["short_interest"] += si_val
            by_date[obs_date]["raw_records"] += 1

            symbol = rec.get("symbolCode") or rec.get("issueSymbolIdentifier")
            if symbol and symbol not in by_date[obs_date]["symbols"]:
                by_date[obs_date]["symbols"].append(symbol)

        return by_date

    def pull_short_interest(
        self,
        start_date: str | date = "2020-01-01",
        days_back: int | None = None,
        tickers: list[str] | None = None,
    ) -> dict[str, Any]:
        """Pull consolidated short interest and store short_interest_total.

        Uses _get_existing_dates() for efficient batch deduplication.

        Parameters:
            start_date: Earliest observation date to store.
            days_back: If set, only store data from this many days ago.
            tickers: Ticker symbols to track (default: TRACKED_TICKERS).

        Returns:
            dict with status, rows_inserted, per_feature counts.
        """
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)

        if days_back is not None:
            cutoff = date.today() - timedelta(days=days_back)
            start_date = max(start_date, cutoff)

        if tickers is None:
            tickers = TRACKED_TICKERS

        # Fetch all pages
        all_records: list[dict[str, Any]] = []
        offset = 0

        try:
            while True:
                page = self._fetch_short_interest_page(
                    offset=offset,
                    limit=_PAGE_SIZE,
                    tickers=tickers,
                )

                if not page:
                    break

                all_records.extend(page)
                log.debug(
                    "FINRA SI: fetched page at offset {o}, {n} records",
                    o=offset,
                    n=len(page),
                )

                if len(page) < _PAGE_SIZE:
                    break

                offset += _PAGE_SIZE
                time.sleep(_RATE_LIMIT_DELAY)

        except Exception as exc:
            log.error("FINRA short interest pull failed: {e}", e=str(exc))
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "error": str(exc),
            }

        if not all_records:
            log.warning("FINRA SI: no records returned from API")
            return {"status": "SUCCESS", "rows_inserted": 0}

        log.info(
            "FINRA SI: fetched {n} total records", n=len(all_records)
        )

        # Parse and aggregate by date
        by_date = self._parse_short_interest_records(all_records)
        if not by_date:
            log.warning("FINRA SI: no data parsed from records")
            return {"status": "SUCCESS", "rows_inserted": 0}

        inserted = 0
        sid = self._series_id("short_interest_total")

        with self.engine.begin() as conn:
            existing = self._get_existing_dates(sid, conn)

            for obs_date, agg in sorted(by_date.items()):
                if obs_date < start_date:
                    continue
                if obs_date in existing:
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=sid,
                    obs_date=obs_date,
                    value=agg["short_interest"],
                    raw_payload={
                        "raw_records": int(agg["raw_records"]),
                        "symbols": agg["symbols"],
                        "tickers_requested": tickers,
                        "source_url": _SHORT_INTEREST_URL,
                    },
                )
                inserted += 1

        log.info(
            "FINRA {sid}: {n} rows inserted", sid=sid, n=inserted
        )

        return {
            "status": "SUCCESS",
            "rows_inserted": inserted,
            "per_feature": {"short_interest_total": inserted},
        }

    # ------------------------------------------------------------------ #
    # Combined pull
    # ------------------------------------------------------------------ #

    def pull_all(
        self,
        start_date: str | date = "2020-01-01",
        days_back: int | None = None,
        tickers: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Pull all FINRA ATS features (volume + short interest).

        Parameters:
            start_date: Earliest observation date.
            days_back: If set, only store recent data.
            tickers: Ticker symbols to track (default: TRACKED_TICKERS).

        Returns:
            List of result dicts (one per data source).
        """
        results: list[dict[str, Any]] = []

        ats_result = self.pull_ats_volume(
            start_date=start_date,
            days_back=days_back,
            tickers=tickers,
        )
        results.append(ats_result)

        time.sleep(_RATE_LIMIT_DELAY)

        si_result = self.pull_short_interest(
            start_date=start_date,
            days_back=days_back,
            tickers=tickers,
        )
        results.append(si_result)

        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        total_rows = sum(r["rows_inserted"] for r in results)
        log.info(
            "FINRA ATS pull_all -- {ok}/{total} sources, {rows} rows",
            ok=succeeded,
            total=len(results),
            rows=total_rows,
        )
        return results
