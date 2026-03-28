"""
GRID FINRA ADF/ATS dark pool transparency data ingestion module.

Pulls dark pool volume and trade count data from FINRA's OTC
transparency API, which publishes weekly summaries with a ~2-week lag.

This module focuses on per-ticker dark pool activity for signal
generation, complementing the existing finra_ats.py module which
tracks aggregate dark pool metrics (DIX proxy).

Key signal: unusual dark pool volume spikes vs 20-day average indicate
institutional accumulation/distribution before price moves.

Series stored with patterns:
- DARKPOOL:{ticker}:volume — weekly dark pool share volume
- DARKPOOL:{ticker}:trades — weekly dark pool trade count

Source: FINRA ATS Transparency Data
URL: https://api.finra.org/data/group/otcMarket/name/weeklySummary
Schedule: Weekly (data published weekly with ~2-week lag)
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ---- API URLs ----
_WEEKLY_SUMMARY_URL: str = (
    "https://api.finra.org/data/group/otcMarket/name/weeklySummary"
)

# HTTP config
_REQUEST_TIMEOUT: int = 45
_RATE_LIMIT_DELAY: float = 2.0
_PAGE_SIZE: int = 5000

# Tickers to track — major ETFs + liquid mega-caps
TRACKED_TICKERS: list[str] = [
    "SPY", "QQQ", "IWM", "DIA", "HYG", "TLT", "XLF", "XLE", "XLK",
    "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOGL", "META", "AMD",
    "JPM", "BAC", "GS", "NFLX", "COIN", "PLTR", "SOFI",
]

# Volume spike detection: ratio to 20-day average that counts as unusual
_VOLUME_SPIKE_THRESHOLD: float = 2.0  # 2x average


class DarkPoolPuller(BasePuller):
    """Pulls FINRA dark pool transparency data per ticker.

    Tracks weekly dark pool volume and trade counts for a watchlist
    of major tickers. Detects unusual volume spikes relative to the
    20-day (4-week) moving average, which can signal institutional
    accumulation or distribution ahead of price moves.

    Series patterns:
    - DARKPOOL:{ticker}:volume — weekly ATS share volume
    - DARKPOOL:{ticker}:trades — weekly ATS trade count

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for DARKPOOL.
    """

    SOURCE_NAME: str = "DARKPOOL"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://api.finra.org/data/group/otcMarket",
        "cost_tier": "FREE",
        "latency_class": "WEEKLY",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 36,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the dark pool puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info(
            "DarkPoolPuller initialised — source_id={sid}",
            sid=self.source_id,
        )

    # ------------------------------------------------------------------ #
    # FINRA API interaction
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
    def _fetch_weekly_page(
        self,
        offset: int = 0,
        limit: int = _PAGE_SIZE,
        tickers: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch a page of weekly dark pool summary data from FINRA.

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
            _WEEKLY_SUMMARY_URL,
            json=query,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def _parse_weekly_records(
        self,
        records: list[dict[str, Any]],
    ) -> dict[str, dict[date, dict[str, float]]]:
        """Parse FINRA weekly summary records by ticker and date.

        Groups records by ticker symbol and week start date,
        summing volume and trade counts across all ATSs.

        Parameters:
            records: Raw FINRA weekly summary records.

        Returns:
            Nested dict: {ticker: {obs_date: {'volume': float, 'trades': float}}}.
        """
        by_ticker: dict[str, dict[date, dict[str, float]]] = {}

        for rec in records:
            ticker = rec.get("issueSymbolIdentifier") or ""
            ticker = ticker.strip().upper()
            if not ticker:
                continue

            week_str = rec.get("weekStartDate")
            if not week_str:
                continue

            try:
                obs_date = date.fromisoformat(week_str[:10])
            except (ValueError, TypeError) as exc:
                log.warning(
                    "DarkPool: bad date {d}: {e}", d=week_str, e=str(exc)
                )
                continue

            # Extract volume and trade count
            volume = rec.get("totalWeeklyShareQuantity")
            trades = rec.get("totalWeeklyTradeCount")

            if volume is None and trades is None:
                continue

            if ticker not in by_ticker:
                by_ticker[ticker] = {}

            if obs_date not in by_ticker[ticker]:
                by_ticker[ticker][obs_date] = {"volume": 0.0, "trades": 0.0}

            if volume is not None:
                try:
                    by_ticker[ticker][obs_date]["volume"] += float(volume)
                except (ValueError, TypeError):
                    pass

            if trades is not None:
                try:
                    by_ticker[ticker][obs_date]["trades"] += float(trades)
                except (ValueError, TypeError):
                    pass

        return by_ticker

    # ------------------------------------------------------------------ #
    # Volume spike detection
    # ------------------------------------------------------------------ #

    def _detect_volume_spikes(
        self,
        ticker_data: dict[date, dict[str, float]],
        ticker: str,
    ) -> list[dict[str, Any]]:
        """Detect unusual volume spikes vs 4-week (20-day) average.

        Parameters:
            ticker_data: {obs_date: {'volume': float, 'trades': float}}.
            ticker: Ticker symbol for logging.

        Returns:
            List of spike dicts with date, volume, average, ratio.
        """
        sorted_dates = sorted(ticker_data.keys())
        if len(sorted_dates) < 5:
            return []

        spikes: list[dict[str, Any]] = []

        for i in range(4, len(sorted_dates)):
            current_date = sorted_dates[i]
            current_vol = ticker_data[current_date]["volume"]

            # Compute 4-week rolling average
            lookback = sorted_dates[max(0, i - 4):i]
            avg_vol = sum(
                ticker_data[d]["volume"] for d in lookback
            ) / len(lookback)

            if avg_vol > 0 and current_vol > 0:
                ratio = current_vol / avg_vol
                if ratio >= _VOLUME_SPIKE_THRESHOLD:
                    spikes.append({
                        "ticker": ticker,
                        "date": current_date,
                        "volume": current_vol,
                        "avg_volume_4w": avg_vol,
                        "spike_ratio": ratio,
                    })
                    log.info(
                        "DARKPOOL SPIKE: {t} on {d} — {r:.1f}x average "
                        "({v:,.0f} vs {a:,.0f} avg)",
                        t=ticker,
                        d=current_date,
                        r=ratio,
                        v=current_vol,
                        a=avg_vol,
                    )

        return spikes

    # ------------------------------------------------------------------ #
    # Signal emission for trust scoring
    # ------------------------------------------------------------------ #

    def _emit_spike_signal(
        self,
        conn: Any,
        spike: dict[str, Any],
    ) -> None:
        """Emit an UNUSUAL_VOLUME signal for trust scoring.

        Parameters:
            conn: Active database connection (within a transaction).
            spike: Volume spike detection result.
        """
        import json

        conn.execute(
            text(
                "INSERT INTO signal_sources "
                "(source_type, source_id, ticker, signal_date, signal_type, signal_value) "
                "VALUES (:stype, :sid, :ticker, :sdate, :stype2, :sval) "
                "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type) "
                "DO NOTHING"
            ),
            {
                "stype": "darkpool",
                "sid": f"dp_{spike['ticker'].lower()}",
                "ticker": spike["ticker"],
                "sdate": spike["date"],
                "stype2": "UNUSUAL_VOLUME",
                "sval": json.dumps({
                    "volume": spike["volume"],
                    "avg_volume_4w": spike["avg_volume_4w"],
                    "spike_ratio": spike["spike_ratio"],
                }),
            },
        )

    # ------------------------------------------------------------------ #
    # Main pull methods
    # ------------------------------------------------------------------ #

    def pull_weekly(
        self,
        start_date: str | date = "2020-01-01",
        weeks_back: int | None = None,
        tickers: list[str] | None = None,
    ) -> dict[str, Any]:
        """Pull weekly dark pool data for tracked tickers.

        Fetches per-ticker ATS volume and trade counts, stores in
        raw_series, detects volume spikes, and emits signals.

        Parameters:
            start_date: Earliest observation date to store.
            weeks_back: If set, only store data from this many weeks ago.
            tickers: Ticker symbols to track (default: TRACKED_TICKERS).

        Returns:
            dict with status, rows_inserted, tickers_with_data, spikes_detected.
        """
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)

        if weeks_back is not None:
            cutoff = date.today() - timedelta(weeks=weeks_back)
            start_date = max(start_date, cutoff)

        if tickers is None:
            tickers = TRACKED_TICKERS

        # Fetch all pages
        all_records: list[dict[str, Any]] = []
        offset = 0

        try:
            while True:
                page = self._fetch_weekly_page(
                    offset=offset,
                    limit=_PAGE_SIZE,
                    tickers=tickers,
                )

                if not page:
                    break

                all_records.extend(page)
                log.debug(
                    "DarkPool: fetched page at offset {o}, {n} records",
                    o=offset,
                    n=len(page),
                )

                if len(page) < _PAGE_SIZE:
                    break

                offset += _PAGE_SIZE
                time.sleep(_RATE_LIMIT_DELAY)

        except Exception as exc:
            log.error("DarkPool weekly pull failed: {e}", e=str(exc))
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "error": str(exc),
            }

        if not all_records:
            log.warning("DarkPool: no records returned from FINRA API")
            return {"status": "SUCCESS", "rows_inserted": 0}

        log.info(
            "DarkPool: fetched {n} total records", n=len(all_records)
        )

        # Parse by ticker and date
        by_ticker = self._parse_weekly_records(all_records)
        if not by_ticker:
            log.warning("DarkPool: no data parsed from records")
            return {"status": "SUCCESS", "rows_inserted": 0}

        total_inserted = 0
        tickers_with_data = 0
        all_spikes: list[dict[str, Any]] = []

        with self.engine.begin() as conn:
            for ticker, dates_data in sorted(by_ticker.items()):
                tickers_with_data += 1

                # --- Volume series ---
                sid_vol = f"DARKPOOL:{ticker}:volume"
                existing_vol = self._get_existing_dates(sid_vol, conn)

                for obs_date, agg in sorted(dates_data.items()):
                    if obs_date < start_date:
                        continue
                    if obs_date in existing_vol:
                        continue
                    if agg["volume"] <= 0:
                        continue

                    self._insert_raw(
                        conn=conn,
                        series_id=sid_vol,
                        obs_date=obs_date,
                        value=agg["volume"],
                        raw_payload={
                            "ticker": ticker,
                            "metric": "volume",
                            "trades": agg["trades"],
                            "source_url": _WEEKLY_SUMMARY_URL,
                        },
                    )
                    total_inserted += 1

                # --- Trade count series ---
                sid_trades = f"DARKPOOL:{ticker}:trades"
                existing_trades = self._get_existing_dates(sid_trades, conn)

                for obs_date, agg in sorted(dates_data.items()):
                    if obs_date < start_date:
                        continue
                    if obs_date in existing_trades:
                        continue
                    if agg["trades"] <= 0:
                        continue

                    self._insert_raw(
                        conn=conn,
                        series_id=sid_trades,
                        obs_date=obs_date,
                        value=agg["trades"],
                        raw_payload={
                            "ticker": ticker,
                            "metric": "trades",
                            "volume": agg["volume"],
                            "source_url": _WEEKLY_SUMMARY_URL,
                        },
                    )
                    total_inserted += 1

                # --- Volume spike detection ---
                spikes = self._detect_volume_spikes(dates_data, ticker)
                all_spikes.extend(spikes)

                for spike in spikes:
                    try:
                        self._emit_spike_signal(conn, spike)
                    except Exception as exc:
                        log.warning(
                            "DarkPool: spike signal emission failed for {t}: {e}",
                            t=ticker,
                            e=str(exc),
                        )

        log.info(
            "DarkPool: {ins} rows inserted across {t} tickers, "
            "{s} volume spikes detected",
            ins=total_inserted,
            t=tickers_with_data,
            s=len(all_spikes),
        )

        return {
            "status": "SUCCESS",
            "rows_inserted": total_inserted,
            "tickers_with_data": tickers_with_data,
            "spikes_detected": len(all_spikes),
        }

    def pull_all(
        self,
        start_date: str | date = "2020-01-01",
        weeks_back: int | None = None,
        tickers: list[str] | None = None,
    ) -> dict[str, Any]:
        """Pull all dark pool data.

        Alias for pull_weekly().

        Parameters:
            start_date: Earliest observation date to store.
            weeks_back: If set, only store recent data.
            tickers: Ticker symbols to track (default: TRACKED_TICKERS).

        Returns:
            dict with status, rows_inserted, etc.
        """
        return self.pull_weekly(
            start_date=start_date,
            weeks_back=weeks_back,
            tickers=tickers,
        )
