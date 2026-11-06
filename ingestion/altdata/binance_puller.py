"""
GRID Binance public market data ingestion module.

Pulls daily OHLCV klines and 24hr ticker stats for major crypto pairs.
No API key required. Series: binance.{SYMBOL}.{field}
"""

from __future__ import annotations

import math
import time
from datetime import date, datetime, timedelta, timezone
from datetime import time as dtime
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from binance_close_contract import (
    DAY_MS,
    PUBLIC_DATA_HOST,
    completed_kline_payload,
    is_completed_canonical_close,
    last_completed_obs_date,
)
from ingestion.base import BasePuller, retry_on_failure

# Binance's documented unauthenticated spot market-data host. The source and
# BTCUSDT/ETHUSDT series identities are unchanged.
_DATA_BASE = f"https://{PUBLIC_DATA_HOST}/api/v3"
_KLINE_URL = f"{_DATA_BASE}/klines"
_TICKER_URL = f"{_DATA_BASE}/ticker/24hr"
_SYMBOLS: list[str] = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
_KLINE_FIELDS: list[str] = ["open", "high", "low", "close", "volume"]
_RATE_LIMIT: float = 0.5
_TIMEOUT: int = 30
_HEADERS = {"User-Agent": "GRID-DataPuller/1.0"}
_BACKFILL_PAGE_LIMIT: int = 1000  # Binance's maximum klines per request
# Backfill writes OHLC, not volume: binance.*USDT.volume is base-asset units
# but resolves into *_total_volume beside yfinance's USD volume, so history
# would deepen that unit mix. The scheduled pull is unchanged.
BACKFILL_DEFAULT_FIELDS: tuple[str, ...] = ("open", "high", "low", "close")


def _day_ms(day: date) -> int:
    """Epoch milliseconds of ``day`` 00:00 UTC."""
    return int(datetime.combine(day, dtime.min, timezone.utc).timestamp() * 1000)

# Stop this cycle on access denial rather than hammering the other endpoints.
# HTTP 403 can also be a WAF block, so it does not prove a geographic cause.
_ACCESS_BLOCKED_STATUSES = frozenset({451, 403})


class BinanceAccessBlocked(RuntimeError):
    """Raised when Binance denies this market-data request."""


class BinancePuller(BasePuller):
    """Pulls daily OHLCV and 24hr ticker data from Binance public API."""

    SOURCE_NAME: str = "binance"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": _DATA_BASE,
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 25,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info("BinancePuller initialised -- source_id={sid}", sid=self.source_id)

    @retry_on_failure(
        max_attempts=3, backoff=2.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _fetch_klines(self, symbol: str) -> list[list]:
        """Fetch yesterday's close and today's open bar, with no backfill."""
        resp = requests.get(
            _KLINE_URL,
            params={"symbol": symbol, "interval": "1d", "timeZone": "0", "limit": 2},
            headers=_HEADERS, timeout=_TIMEOUT,
        )
        if resp.status_code in _ACCESS_BLOCKED_STATUSES:
            raise BinanceAccessBlocked(
                f"Binance market-data host returned access status {resp.status_code}"
            )
        resp.raise_for_status()
        return resp.json()

    @retry_on_failure(
        max_attempts=3, backoff=2.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _fetch_ticker(self, symbol: str) -> dict[str, Any]:
        """Fetch 24hr ticker stats for a symbol."""
        resp = requests.get(
            _TICKER_URL, params={"symbol": symbol},
            headers=_HEADERS, timeout=_TIMEOUT,
        )
        if resp.status_code in _ACCESS_BLOCKED_STATUSES:
            raise BinanceAccessBlocked(
                f"Binance market-data host returned access status {resp.status_code}"
            )
        resp.raise_for_status()
        return resp.json()

    @retry_on_failure(
        max_attempts=3, backoff=2.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _fetch_kline_page(self, symbol: str, start_ms: int, end_ms: int) -> list[list]:
        """Fetch one page of UTC 1d klines opening in ``[start_ms, end_ms]``."""
        resp = requests.get(
            _KLINE_URL,
            params={"symbol": symbol, "interval": "1d", "timeZone": "0",
                    "startTime": start_ms, "endTime": end_ms,
                    "limit": _BACKFILL_PAGE_LIMIT},
            headers=_HEADERS, timeout=_TIMEOUT,
        )
        if resp.status_code in _ACCESS_BLOCKED_STATUSES:
            raise BinanceAccessBlocked(
                f"Binance market-data host returned access status {resp.status_code}"
            )
        resp.raise_for_status()
        return resp.json()

    def _fetch_kline_range(self, symbol: str, start_day: date, end_day: date) -> list[list]:
        """All UTC 1d klines opening on ``start_day``..``end_day``, paged."""
        start_ms = _day_ms(start_day)
        end_ms = _day_ms(end_day) + DAY_MS - 1
        klines: list[list] = []
        while start_ms <= end_ms:
            page = self._fetch_kline_page(symbol, start_ms, end_ms)
            if not page:
                break
            klines.extend(page)
            next_start = int(page[-1][0]) + DAY_MS
            if len(page) < _BACKFILL_PAGE_LIMIT or next_start <= start_ms:
                break
            start_ms = next_start
            time.sleep(_RATE_LIMIT)
        return klines

    @staticmethod
    def _validated_completed_bar(
        symbol: str, kline: list[Any], captured_at: datetime,
    ) -> tuple[date, dict[str, Any], list[float]] | None:
        """A completed bar's (obs_date, evidence, OHLCV), or None if still open.

        Malformed, non-UTC, non-finite or inconsistent bars raise ValueError
        so the pull fails rather than writing a bad close.
        """
        close_evidence = completed_kline_payload(kline, captured_at)
        if close_evidence is None:
            return None  # the current UTC day's kline is still open
        obs_date, evidence = close_evidence
        values = [float(kline[i]) for i in range(1, 6)]
        if any(not math.isfinite(value) for value in values):
            raise ValueError(f"Binance returned non-finite daily kline for {symbol}")
        if any(value <= 0 for value in values[:4]) or values[4] < 0:
            raise ValueError(f"Binance returned invalid daily kline for {symbol}")
        opening, high, low, closing, _volume = values
        if high < max(opening, closing) or low > min(opening, closing):
            raise ValueError(f"Binance returned inconsistent daily kline for {symbol}")
        return obs_date, evidence, values

    def _completed_days_by_field(
        self, conn: Any, symbol: str, first_day: date, last_day: date,
        fields: tuple[str, ...] = tuple(_KLINE_FIELDS),
    ) -> dict[str, set[date]]:
        """UTC days that already hold a completed-contract row, per kline field.

        An old provisional or unmarked same-day row does not count, so it
        cannot suppress the first completed bar.
        """
        existing: dict[str, set[date]] = {}
        for field in fields:
            sid = f"binance.{symbol}.{field}"
            raw_rows = conn.execute(text("""
                SELECT obs_date, pull_timestamp, raw_payload
                FROM raw_series
                WHERE series_id = :sid AND source_id = :src
                  AND obs_date BETWEEN :first_day AND :last_day
                  AND pull_status = 'SUCCESS'
            """), {"sid": sid, "src": self.source_id,
                   "first_day": first_day, "last_day": last_day}).fetchall()
            existing[field] = {
                row[0] for row in raw_rows
                if is_completed_canonical_close(row[2], row[0], row[1])
            }
        return existing

    def _append_completed(
        self, conn: Any, symbol: str,
        completed: list[tuple[date, dict[str, Any], list[float]]],
        fields: tuple[str, ...] = tuple(_KLINE_FIELDS),
    ) -> int:
        """Append completed bars not already stored (INSERT only, idempotent)."""
        if not completed:
            return 0
        first_day = min(row[0] for row in completed)
        last_day = max(row[0] for row in completed)
        existing = self._completed_days_by_field(conn, symbol, first_day, last_day, fields)
        inserted = 0
        for field in fields:
            field_idx = _KLINE_FIELDS.index(field)
            sid = f"binance.{symbol}.{field}"
            for obs_date, evidence, values in completed:
                if obs_date in existing[field]:
                    continue
                self._insert_raw(
                    conn=conn, series_id=sid, obs_date=obs_date,
                    value=values[field_idx],
                    raw_payload={"symbol": symbol, "field": field, **evidence},
                )
                inserted += 1
        return inserted

    def _pull_klines(self, symbol: str) -> int:
        """Append only the last completed UTC 1d bar for one symbol.

        The scheduled pull is forward-only; history is filled by
        ``backfill_klines`` (scripts/backfill_crypto_daily_closes.py).
        """
        klines = self._fetch_klines(symbol)
        if not klines:
            raise ValueError(f"Binance returned no daily klines for {symbol}")
        captured_at = datetime.now(timezone.utc)
        expected_day = captured_at.date() - timedelta(days=1)
        completed = []
        for k in klines:
            bar = self._validated_completed_bar(symbol, k, captured_at)
            if bar is None or bar[0] != expected_day:
                continue  # open bar, or not the last completed day
            completed.append(bar)
        if not completed:
            raise ValueError(f"Binance returned no completed UTC close for {symbol}/{expected_day}")
        with self.engine.begin() as conn:
            return self._append_completed(conn, symbol, completed)

    def backfill_klines(
        self, symbol: str, start_day: date, end_day: date, *,
        apply: bool = False, now: datetime | None = None,
        fields: tuple[str, ...] = BACKFILL_DEFAULT_FIELDS,
    ) -> dict[str, Any]:
        """Fill missing completed UTC daily bars for ``symbol`` (append-only).

        Fetches every 1d bar opening on ``start_day``..``end_day`` (clamped to
        the last completed UTC day), keeps completed bars under the
        ``binance_utc_close_v1`` contract, and appends the ones with no
        completed-contract row yet. Existing rows are never updated; a re-run
        inserts nothing. ``apply=False`` reads only and reports what would be
        written. Each backfilled row's ``pull_timestamp`` is the capture time,
        so its resolved release date is the backfill date (never back-dated).
        ``fields`` must include ``close``; volume is excluded by default (see
        ``BACKFILL_DEFAULT_FIELDS``).
        """
        fields = tuple(fields)
        if "close" not in fields or any(f not in _KLINE_FIELDS for f in fields):
            raise ValueError(f"backfill fields must include close and be in {_KLINE_FIELDS}")
        captured_at = now or datetime.now(timezone.utc)
        end_day = min(end_day, last_completed_obs_date(captured_at))
        summary: dict[str, Any] = {
            "symbol": symbol, "start": start_day.isoformat(), "end": end_day.isoformat(),
            "mode": "apply" if apply else "dry_run", "fields": list(fields),
            "days_in_range": 0, "missing_close_days_before": 0,
            "bars_completed": 0, "close_days_to_fill": 0,
            "rows_to_insert": 0, "rows_inserted": 0,
            "missing_close_days_after": 0, "unfillable_days": [],
            "stored_close_days": [],
        }
        if end_day < start_day:
            return summary
        days = [start_day + timedelta(days=i) for i in range((end_day - start_day).days + 1)]
        summary["days_in_range"] = len(days)
        completed = []
        for k in self._fetch_kline_range(symbol, start_day, end_day):
            bar = self._validated_completed_bar(symbol, k, captured_at)
            if bar is not None and start_day <= bar[0] <= end_day:
                completed.append(bar)
        if len({bar[0] for bar in completed}) != len(completed):
            raise ValueError(f"Binance returned duplicate daily klines for {symbol}")
        summary["bars_completed"] = len(completed)
        bar_days = {bar[0] for bar in completed}

        connect = self.engine.begin if apply else self.engine.connect
        with connect() as conn:
            existing = self._completed_days_by_field(conn, symbol, start_day, end_day, fields)
            have_close = existing["close"]
            summary["missing_close_days_before"] = sum(1 for d in days if d not in have_close)
            summary["close_days_to_fill"] = len(bar_days - have_close)
            summary["rows_to_insert"] = sum(
                1 for field in fields for bar in completed
                if bar[0] not in existing[field]
            )
            if apply:
                summary["rows_inserted"] = self._append_completed(conn, symbol, completed, fields)
        still_missing = [d for d in days if d not in have_close and d not in bar_days]
        summary["missing_close_days_after"] = len(still_missing)
        summary["unfillable_days"] = [d.isoformat() for d in still_missing]
        summary["stored_close_days"] = sorted(d.isoformat() for d in have_close)
        return summary

    def _pull_ticker(self, symbol: str) -> int:
        """Pull 24hr ticker for one symbol. Returns rows inserted."""
        inserted = 0
        ticker = self._fetch_ticker(symbol)
        obs_date = datetime.now(timezone.utc).date()
        with self.engine.begin() as conn:
            for field, key in [("volume_24h", "volume"), ("price_change_pct", "priceChangePercent")]:
                sid = f"binance.{symbol}.{field}"
                existing = self._get_existing_dates(sid, conn)
                if obs_date not in existing:
                    self._insert_raw(
                        conn=conn, series_id=sid, obs_date=obs_date,
                        value=float(ticker[key]),
                        raw_payload={
                            "symbol": symbol,
                            "lastPrice": ticker.get("lastPrice"),
                            "weightedAvgPrice": ticker.get("weightedAvgPrice"),
                        },
                    )
                    inserted += 1
        return inserted

    def pull(self) -> dict[str, Any]:
        """Pull klines + 24hr ticker for all tracked symbols.

        Returns:
            dict with status, rows_inserted, per_symbol breakdown.
        """
        total_inserted = 0
        per_symbol: dict[str, int] = {}
        errors: list[str] = []

        access_blocked = False

        for symbol in _SYMBOLS:
            sym_inserted = 0
            if access_blocked:
                per_symbol[symbol] = 0
                continue

            for label, fn in [("klines", self._pull_klines), ("ticker", self._pull_ticker)]:
                try:
                    sym_inserted += fn(symbol)
                except BinanceAccessBlocked as exc:
                    log.warning(
                        "Binance access denied at {s}/{l}: {e}; skipping "
                        "remaining symbols this cycle",
                        s=symbol, l=label, e=str(exc),
                    )
                    errors.append(f"{symbol}_{label}: {exc}")
                    access_blocked = True
                    break
                except Exception as exc:
                    log.error("Binance {l} {s}: {e}", l=label, s=symbol, e=str(exc))
                    errors.append(f"{symbol}_{label}: {exc}")
                time.sleep(_RATE_LIMIT)

            per_symbol[symbol] = sym_inserted
            total_inserted += sym_inserted

        log.info("BinancePuller: {n} rows, {e} errors", n=total_inserted, e=len(errors))
        if errors:
            # The scheduler otherwise records SUCCESS and advances
            # source_catalog.last_pull_at for a zero-row access failure.
            raise RuntimeError(
                f"Binance pull incomplete ({len(errors)} endpoint errors): {errors[0]}"
            )
        return {
            "status": "SUCCESS",
            "rows_inserted": total_inserted,
            "per_symbol": per_symbol,
            "errors": errors or None,
        }
