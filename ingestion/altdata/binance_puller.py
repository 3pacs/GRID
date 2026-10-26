"""
GRID Binance public market data ingestion module.

Pulls daily OHLCV klines and 24hr ticker stats for major crypto pairs.
No API key required. Series: binance.{SYMBOL}.{field}
"""

from __future__ import annotations

import math
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from binance_close_contract import (
    PUBLIC_DATA_HOST, completed_kline_payload, is_completed_canonical_close,
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

    def _pull_klines(self, symbol: str) -> int:
        """Append only completed UTC 1d bars for one symbol."""
        inserted = 0
        klines = self._fetch_klines(symbol)
        if not klines:
            raise ValueError(f"Binance returned no daily klines for {symbol}")
        captured_at = datetime.now(timezone.utc)
        expected_day = captured_at.date() - timedelta(days=1)
        completed = []
        for k in klines:
            close_evidence = completed_kline_payload(k, captured_at)
            if close_evidence is None:
                continue  # the current UTC day's kline is still open
            obs_date, evidence = close_evidence
            if obs_date != expected_day:
                continue  # only the last completed day, never historical fill
            values = [float(k[i]) for i in range(1, 6)]
            if any(not math.isfinite(value) for value in values):
                raise ValueError(f"Binance returned non-finite daily kline for {symbol}")
            if any(value <= 0 for value in values[:4]) or values[4] < 0:
                raise ValueError(f"Binance returned invalid daily kline for {symbol}")
            opening, high, low, closing, _volume = values
            if high < max(opening, closing) or low > min(opening, closing):
                raise ValueError(f"Binance returned inconsistent daily kline for {symbol}")
            completed.append((obs_date, evidence, values))
        if not completed:
            raise ValueError(f"Binance returned no completed UTC close for {symbol}/{expected_day}")
        first_day = min(row[0] for row in completed)
        last_day = max(row[0] for row in completed)
        with self.engine.begin() as conn:
            for field_idx, field in enumerate(_KLINE_FIELDS):
                sid = f"binance.{symbol}.{field}"
                # An old provisional or unmarked same-day row cannot suppress
                # the first completed bar. Future cycles dedupe the new marker.
                raw_rows = conn.execute(text("""
                    SELECT obs_date, pull_timestamp, raw_payload
                    FROM raw_series
                    WHERE series_id = :sid AND source_id = :src
                      AND obs_date BETWEEN :first_day AND :last_day
                      AND pull_status = 'SUCCESS'
                """), {"sid": sid, "src": self.source_id,
                       "first_day": first_day, "last_day": last_day}).fetchall()
                existing = {
                    row[0] for row in raw_rows
                    if is_completed_canonical_close(row[2], row[0], row[1])
                }
                for obs_date, evidence, values in completed:
                    if obs_date in existing:
                        continue
                    self._insert_raw(
                        conn=conn, series_id=sid, obs_date=obs_date,
                        value=values[field_idx],
                        raw_payload={"symbol": symbol, "field": field, **evidence},
                    )
                    inserted += 1
        return inserted

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
