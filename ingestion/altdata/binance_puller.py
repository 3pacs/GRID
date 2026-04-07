"""
GRID Binance public market data ingestion module.

Pulls daily OHLCV klines and 24hr ticker stats for major crypto pairs.
No API key required. Series: binance.{SYMBOL}.{field}
"""

from __future__ import annotations

import time
from datetime import date, datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

_KLINE_URL = "https://api.binance.com/api/v3/klines"
_TICKER_URL = "https://api.binance.com/api/v3/ticker/24hr"
_SYMBOLS: list[str] = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
_KLINE_FIELDS: list[str] = ["open", "high", "low", "close", "volume"]
_RATE_LIMIT: float = 0.5
_TIMEOUT: int = 30
_HEADERS = {"User-Agent": "GRID-DataPuller/1.0"}


class BinancePuller(BasePuller):
    """Pulls daily OHLCV and 24hr ticker data from Binance public API."""

    SOURCE_NAME: str = "binance"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://api.binance.com/api/v3",
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
        """Fetch 7-day daily klines for a symbol."""
        resp = requests.get(
            _KLINE_URL, params={"symbol": symbol, "interval": "1d", "limit": 7},
            headers=_HEADERS, timeout=_TIMEOUT,
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
        resp.raise_for_status()
        return resp.json()

    def _pull_klines(self, symbol: str) -> int:
        """Pull klines for one symbol. Returns rows inserted."""
        inserted = 0
        klines = self._fetch_klines(symbol)
        with self.engine.begin() as conn:
            for field_idx, field in enumerate(_KLINE_FIELDS):
                sid = f"binance.{symbol}.{field}"
                existing = self._get_existing_dates(sid, conn)
                col = field_idx + 1  # Kline: [open_time, O, H, L, C, vol, ...]
                for k in klines:
                    obs_date = datetime.fromtimestamp(k[0] / 1000.0, tz=timezone.utc).date()
                    if obs_date in existing:
                        continue
                    self._insert_raw(
                        conn=conn, series_id=sid, obs_date=obs_date,
                        value=float(k[col]),
                        raw_payload={"symbol": symbol, "field": field},
                    )
                    inserted += 1
        return inserted

    def _pull_ticker(self, symbol: str) -> int:
        """Pull 24hr ticker for one symbol. Returns rows inserted."""
        inserted = 0
        ticker = self._fetch_ticker(symbol)
        obs_date = date.today()
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

        for symbol in _SYMBOLS:
            sym_inserted = 0
            for label, fn in [("klines", self._pull_klines), ("ticker", self._pull_ticker)]:
                try:
                    sym_inserted += fn(symbol)
                except Exception as exc:
                    log.error("Binance {l} {s}: {e}", l=label, s=symbol, e=str(exc))
                    errors.append(f"{symbol}_{label}: {exc}")
                time.sleep(_RATE_LIMIT)

            per_symbol[symbol] = sym_inserted
            total_inserted += sym_inserted

        status = "SUCCESS" if not errors else ("PARTIAL" if total_inserted > 0 else "FAILED")
        log.info("BinancePuller: {n} rows, {e} errors", n=total_inserted, e=len(errors))
        return {
            "status": status,
            "rows_inserted": total_inserted,
            "per_symbol": per_symbol,
            "errors": errors or None,
        }
