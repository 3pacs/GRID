"""
GRID FINRA ATS (Dark Pool) volume ingestion module.

Pulls dark pool trading data from FINRA's OTC Transparency API:
1. ATS (Alternative Trading System) weekly volume data -- total dark pool
   volume and dark pool percentage of total volume (DIX proxy).
2. Consolidated short interest -- bimonthly short interest reports.

Data sources:
- ATS volume: https://api.finra.org/data/group/otcMarket/name/weeklyDownloadDetails
- Short interest: https://api.finra.org/data/group/otcMarket/name/consolidatedShortInterest

Series stored:
- finra.ats_total_volume: Total ATS (dark pool) volume across tracked tickers
- finra.ats_dark_pct: Dark pool volume as percentage of total volume (DIX proxy)
- finra.short_interest_total: Consolidated short interest across tracked tickers
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ---- URLs ----
_ATS_VOLUME_URL: str = (
    "https://api.finra.org/data/group/otcMarket/name/weeklyDownloadDetails"
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
    ) -> list[dict[str, Any]]:
        """Fetch a page of ATS volume data from FINRA API.

        The FINRA API accepts JSON POST queries with filtering,
        pagination, and sorting.

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

        # Build FINRA query -- filter by tracked tickers
        query: dict[str, Any] = {
            "offset": offset,
            "limit": limit,
            "sortFields": ["-weekStartDate"],
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
        resp.raise_for_status()
        return resp.json()

    def _parse_ats_records(
        self,
        records: list[dict[str, Any]],
    ) -> dict[date, dict[str, float]]:
        """Aggregate ATS records by week into volume and dark pct.

        Groups records by weekStartDate, sums totalWeeklyShareQuantity
        (ATS volume) and totalWeeklyTradeCount, and computes dark pool
        percentage where possible.

        Parameters:
            records: Raw FINRA ATS records.

        Returns:
            Dict mapping obs_date to aggregated metrics:
            {date: {'ats_volume': float, 'total_volume': float,
                     'dark_pct': float, 'raw_records': int}}.
        """
        by_week: dict[date, dict[str, float]] = {}

        for rec in records:
            # Parse the week start date
            week_str = rec.get("weekStartDate")
            if not week_str:
                continue

            try:
                obs_date = date.fromisoformat(week_str[:10])
            except (ValueError, TypeError) as exc:
                log.warning(
                    "FINRA ATS: bad date {d}: {e}",
                    d=week_str,
                    e=str(exc),
                )
                continue

            # Extract volume fields
            ats_volume = rec.get("totalWeeklyShareQuantity")
            total_volume = rec.get("lastUpdateDate")  # not always available

            if ats_volume is None:
                continue

            try:
                ats_vol = float(ats_volume)
            except (ValueError, TypeError):
                log.warning(
                    "FINRA ATS: bad volume value {v} for {d}",
                    v=ats_volume,
                    d=week_str,
                )
                continue

            if obs_date not in by_week:
                by_week[obs_date] = {
                    "ats_volume": 0.0,
                    "total_volume": 0.0,
                    "raw_records": 0,
                }

            by_week[obs_date]["ats_volume"] += ats_vol
            by_week[obs_date]["raw_records"] += 1

            # Some records include totalShareQuantity for overall volume
            total_qty = rec.get("totalShareQuantity")
            if total_qty is not None:
                try:
                    by_week[obs_date]["total_volume"] += float(total_qty)
                except (ValueError, TypeError):
                    pass

        # Compute dark pool percentage
        for obs_date, agg in by_week.items():
            if agg["total_volume"] > 0:
                agg["dark_pct"] = (
                    agg["ats_volume"] / agg["total_volume"]
                ) * 100.0
            else:
                # Use ATS volume alone -- mark as incomplete
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
        Paginates through FINRA API to retrieve all matching records.

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
                page = self._fetch_ats_page(
                    offset=offset,
                    limit=_PAGE_SIZE,
                    tickers=tickers,
                )

                if not page:
                    break

                all_records.extend(page)
                log.debug(
                    "FINRA ATS: fetched page at offset {o}, {n} records",
                    o=offset,
                    n=len(page),
                )

                if len(page) < _PAGE_SIZE:
                    break

                offset += _PAGE_SIZE
                time.sleep(_RATE_LIMIT_DELAY)

        except Exception as exc:
            log.error("FINRA ATS volume pull failed: {e}", e=str(exc))
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "error": str(exc),
            }

        if not all_records:
            log.warning("FINRA ATS: no records returned from API")
            return {"status": "SUCCESS", "rows_inserted": 0}

        log.info(
            "FINRA ATS: fetched {n} total records", n=len(all_records)
        )

        # Parse and aggregate by week
        by_week = self._parse_ats_records(all_records)
        if not by_week:
            log.warning("FINRA ATS: no data parsed from records")
            return {"status": "SUCCESS", "rows_inserted": 0}

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
