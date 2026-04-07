"""
Kalshi prediction market puller — public API, no auth needed.

CFTC-regulated prediction exchange.  Pulls economic/financial event
contract prices for Fed rate, CPI, GDP and similar macro series.

The v2 API moved prices to ``*_dollars`` fields and volume/OI to
``*_fp`` fields.  This puller iterates target *series*, fetches their
open *events*, then fetches individual *markets* per event and writes
the highest-volume contract's yes-probability plus metadata into
``raw_series``.

Source: https://docs.kalshi.com/getting_started/quick_start_market_data
API:    https://api.elections.kalshi.com/trade-api/v2
"""

from __future__ import annotations

import json as _json
import time as _time
from datetime import date
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
_REQUEST_TIMEOUT = 30

# Economic / financial series we care about.
# The puller dynamically discovers events under each series.
_TARGET_SERIES: list[str] = [
    "KXFED",       # Fed funds rate
    "KXCPI",       # CPI
    "KXGDP",       # GDP
    "KXUNEMPLOY",  # Unemployment
    "KXBITCOIN",   # Bitcoin price
    "KXTARIFF",    # Tariffs
    "KXRECESSION", # Recession odds
]


def _parse_dollar(raw: Any) -> float:
    """Convert a dollar-string like '0.6500' to float, default 0."""
    if raw is None:
        return 0.0
    try:
        return float(raw)
    except (ValueError, TypeError):
        return 0.0


def _parse_fp(raw: Any) -> float:
    """Convert a volume/OI string like '30621.00' to float, default 0."""
    return _parse_dollar(raw)


class KalshiMarketsPuller(BasePuller):
    """Pulls prediction market data from Kalshi public API."""

    SOURCE_NAME: str = "KALSHI"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://api.elections.kalshi.com",
        "cost_tier": "FREE",
        "latency_class": "REALTIME",
        "pit_available": False,
        "revision_behavior": "FREQUENT",
        "trust_score": "HIGH",
        "priority_rank": 14,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_events(self, series_ticker: str) -> list[dict]:
        """Return open events for a series, newest first."""
        resp = requests.get(
            f"{_API_BASE}/events",
            params={"series_ticker": series_ticker, "status": "open"},
            timeout=_REQUEST_TIMEOUT,
            headers={"Accept": "application/json"},
        )
        resp.raise_for_status()
        return resp.json().get("events", [])

    def _fetch_markets(self, event_ticker: str) -> list[dict]:
        """Return all markets for a given event."""
        _time.sleep(0.35)  # rate-limit: ~3 req/s keeps us under 10/s
        resp = requests.get(
            f"{_API_BASE}/markets",
            params={"event_ticker": event_ticker, "limit": 50},
            timeout=_REQUEST_TIMEOUT,
            headers={"Accept": "application/json"},
        )
        resp.raise_for_status()
        return resp.json().get("markets", [])

    @staticmethod
    def _best_market(markets: list[dict]) -> dict | None:
        """Pick the market with the highest total volume."""
        best, best_vol = None, -1.0
        for m in markets:
            vol = _parse_fp(m.get("volume_fp"))
            if vol > best_vol:
                best, best_vol = m, vol
        return best

    @staticmethod
    def _extract_price(market: dict) -> float:
        """Extract a yes-probability (0-1) from a market dict.

        Prefers last_price_dollars, falls back to yes_bid_dollars or
        yes_ask_dollars.  Returns 0 only when every price field is empty.
        """
        lp = _parse_dollar(market.get("last_price_dollars"))
        if lp > 0:
            return lp
        yb = _parse_dollar(market.get("yes_bid_dollars"))
        if yb > 0:
            return yb
        ya = _parse_dollar(market.get("yes_ask_dollars"))
        if ya > 0:
            return ya
        return 0.0

    @staticmethod
    def _build_payload(event: dict, market: dict, all_markets: list[dict]) -> dict:
        """Build a JSON payload with useful metadata."""
        strikes: list[dict] = []
        for m in all_markets:
            strikes.append({
                "ticker": m.get("ticker"),
                "title": m.get("title"),
                "yes_price": _parse_dollar(m.get("last_price_dollars")),
                "yes_bid": _parse_dollar(m.get("yes_bid_dollars")),
                "yes_ask": _parse_dollar(m.get("yes_ask_dollars")),
                "volume": _parse_fp(m.get("volume_fp")),
                "volume_24h": _parse_fp(m.get("volume_24h_fp")),
                "open_interest": _parse_fp(m.get("open_interest_fp")),
            })
        return {
            "event_ticker": event.get("event_ticker"),
            "event_title": event.get("title"),
            "series_ticker": event.get("series_ticker"),
            "strike_date": event.get("strike_date"),
            "best_market_ticker": market.get("ticker"),
            "best_market_title": market.get("title"),
            "close_time": market.get("close_time"),
            "yes_price": _parse_dollar(market.get("last_price_dollars")),
            "yes_bid": _parse_dollar(market.get("yes_bid_dollars")),
            "yes_ask": _parse_dollar(market.get("yes_ask_dollars")),
            "volume": _parse_fp(market.get("volume_fp")),
            "volume_24h": _parse_fp(market.get("volume_24h_fp")),
            "open_interest": _parse_fp(market.get("open_interest_fp")),
            "all_strikes": strikes,
        }

    # ------------------------------------------------------------------
    # Main pull
    # ------------------------------------------------------------------

    @retry_on_failure(max_attempts=2)
    def pull_all(self, **kwargs) -> list[dict[str, Any]]:
        """Pull active economic/financial markets from Kalshi."""
        result: dict[str, Any] = {
            "rows_inserted": 0,
            "events_found": 0,
            "series_checked": 0,
            "status": "SUCCESS",
        }

        today = date.today()
        inserted = 0
        total_events = 0

        try:
            with self.engine.begin() as conn:
                for series_ticker in _TARGET_SERIES:
                    result["series_checked"] += 1
                    try:
                        events = self._fetch_events(series_ticker)
                    except Exception as exc:
                        log.warning(
                            "Kalshi: failed to fetch events for {s}: {e}",
                            s=series_ticker, e=str(exc),
                        )
                        continue

                    if not events:
                        continue

                    total_events += len(events)

                    for event in events:
                        event_ticker = event.get("event_ticker", "")
                        if not event_ticker:
                            continue

                        try:
                            markets = self._fetch_markets(event_ticker)
                        except Exception as exc:
                            log.warning(
                                "Kalshi: failed to fetch markets for {e}: {err}",
                                e=event_ticker, err=str(exc),
                            )
                            continue

                        if not markets:
                            continue

                        best = self._best_market(markets)
                        if best is None:
                            continue

                        yes_price = self._extract_price(best)
                        volume = _parse_fp(best.get("volume_fp"))
                        open_interest = _parse_fp(best.get("open_interest_fp"))

                        # Accept the row if we have *any* signal:
                        # price, volume, or open interest.
                        if yes_price == 0 and volume == 0 and open_interest == 0:
                            continue

                        # Use the event ticker as series_id so each
                        # meeting/report date gets its own row.
                        series_id = f"KALSHI:{event_ticker}"
                        payload = self._build_payload(event, best, markets)

                        # Use yes_price as value when available;
                        # fall back to volume as a proxy signal.
                        value = yes_price if yes_price > 0 else volume

                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, "
                                " raw_payload, pull_status) "
                                "VALUES (:sid, :src, :od, :val, "
                                " :payload, 'SUCCESS') "
                                "ON CONFLICT (series_id, source_id, "
                                " obs_date, pull_timestamp) "
                                "DO NOTHING"
                            ),
                            {
                                "sid": series_id,
                                "src": self.source_id,
                                "od": today,
                                "val": float(value),
                                "payload": _json.dumps(payload),
                            },
                        )
                        inserted += 1

            result["rows_inserted"] = inserted
            result["events_found"] = total_events
            log.info(
                "Kalshi: {s} series, {e} events, {i} rows inserted",
                s=result["series_checked"], e=total_events, i=inserted,
            )

        except Exception as exc:
            log.error("Kalshi pull failed: {e}", e=str(exc))
            result["status"] = "FAILED"
            result["error"] = str(exc)

        return [result]
