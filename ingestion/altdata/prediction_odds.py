"""
GRID Prediction Market Rapid-Change Detector.

Monitors Polymarket for rapid probability shifts (>10% in 24 hours) on
active markets, focusing on economic events, policy decisions, and
company events. Maps detected shifts to impacted tickers where possible.

Leverages the existing Polymarket integration in trading/prediction_markets.py
but reads from the public CLOB API (no trading key needed for market reads).

Series stored:
- PREDICTION:{market_slug}:{direction}

Source: Polymarket CLOB API (public, no key required for reads)
Schedule: Daily
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ── API Configuration ────────────────────────────────────────────────

_POLYMARKET_GAMMA_URL: str = "https://gamma-api.polymarket.com"
_CLOB_URL: str = "https://clob.polymarket.com"

_REQUEST_TIMEOUT: int = 30
_RATE_LIMIT_DELAY: float = 1.0
_PAGE_LIMIT: int = 100

# Minimum probability shift in 24h to flag (absolute, 0-1 scale)
_MIN_SHIFT_THRESHOLD: float = 0.10

# Keywords that identify economically relevant markets
_ECONOMIC_KEYWORDS: list[str] = [
    "fed", "fomc", "rate cut", "rate hike", "interest rate",
    "inflation", "cpi", "pce", "gdp", "recession",
    "unemployment", "nonfarm", "jobs report",
    "tariff", "trade war", "sanctions",
    "debt ceiling", "government shutdown", "default",
    "election", "president", "congress", "senate",
    "bitcoin", "crypto", "etf approval",
    "earnings", "revenue", "profit",
    "oil", "opec", "energy",
    "war", "invasion", "ceasefire",
    "bank", "svb", "credit",
]

# Mapping from market topic patterns to impacted tickers
# Each entry: (regex_pattern, list_of_tickers)
_TOPIC_TICKER_MAP: list[tuple[str, list[str]]] = [
    (r"fed.*rate|fomc|interest rate", ["TLT", "SPY", "XLF", "IEF"]),
    (r"recession", ["SPY", "TLT", "HYG", "IWM"]),
    (r"inflation|cpi|pce", ["TLT", "TIP", "GLD", "SPY"]),
    (r"gdp", ["SPY", "IWM", "EEM"]),
    (r"unemployment|nonfarm|jobs", ["SPY", "XLF", "IWM"]),
    (r"tariff|trade war", ["EEM", "FXI", "SPY", "KWEB"]),
    (r"debt ceiling|shutdown|default", ["TLT", "SPY", "GLD"]),
    (r"bitcoin|crypto", ["COIN", "MSTR", "BITO"]),
    (r"oil|opec|energy", ["USO", "XLE", "OXY"]),
    (r"election|president", ["SPY", "IWM", "XLF"]),
    (r"bank|credit", ["XLF", "KRE", "JPM", "BAC"]),
    (r"war|invasion|ceasefire", ["GLD", "USO", "SPY"]),
    (r"earnings.*apple|apple.*earnings", ["AAPL"]),
    (r"earnings.*nvidia|nvidia.*earnings", ["NVDA"]),
    (r"earnings.*tesla|tesla.*earnings", ["TSLA"]),
    (r"earnings.*microsoft|microsoft.*earnings", ["MSFT"]),
    (r"earnings.*amazon|amazon.*earnings", ["AMZN"]),
    (r"earnings.*google|alphabet.*earnings", ["GOOGL"]),
    (r"earnings.*meta|meta.*earnings", ["META"]),
]


def _make_slug(title: str) -> str:
    """Convert a market title into a series-safe slug.

    Parameters:
        title: Raw market title/question.

    Returns:
        Lowercase alphanumeric slug with underscores.
    """
    slug = title.lower().strip()
    cleaned = []
    for ch in slug:
        if ch.isalnum() or ch in ("_", " "):
            cleaned.append(ch)
    result = "_".join("".join(cleaned).split())
    # Truncate to keep series_id reasonable
    if len(result) > 80:
        hash_suffix = hashlib.md5(result.encode()).hexdigest()[:8]
        result = result[:70] + "_" + hash_suffix
    return result


def _is_economically_relevant(title: str, description: str = "") -> bool:
    """Check if a market is economically relevant.

    Parameters:
        title: Market title.
        description: Market description.

    Returns:
        True if the market matches any economic keyword.
    """
    combined = f"{title} {description}".lower()
    return any(kw in combined for kw in _ECONOMIC_KEYWORDS)


def _map_to_tickers(title: str, description: str = "") -> list[str]:
    """Map a market to impacted tickers based on topic.

    Parameters:
        title: Market title.
        description: Market description.

    Returns:
        List of potentially impacted ticker symbols.
    """
    combined = f"{title} {description}".lower()
    tickers: list[str] = []
    for pattern, ticker_list in _TOPIC_TICKER_MAP:
        if re.search(pattern, combined):
            for t in ticker_list:
                if t not in tickers:
                    tickers.append(t)
    return tickers


class PredictionOddsPuller(BasePuller):
    """Detects rapid probability shifts on Polymarket.

    Monitors active prediction markets for >10% probability changes
    in 24 hours. These rapid shifts often front-run traditional
    market moves on related assets.

    Series pattern: PREDICTION:{market_slug}:{direction}
    Direction is 'UP' or 'DOWN' based on probability movement.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for Polymarket.
    """

    SOURCE_NAME: str = "Polymarket"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://polymarket.com/",
        "cost_tier": "FREE",
        "latency_class": "INTRADAY",
        "pit_available": True,
        "revision_behavior": "FREQUENT",
        "trust_score": "HIGH",
        "priority_rank": 22,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the prediction odds puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info(
            "PredictionOddsPuller initialised — source_id={sid}",
            sid=self.source_id,
        )

    # ------------------------------------------------------------------ #
    # Polymarket Gamma API interaction
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
    def _fetch_markets_page(
        self,
        offset: int = 0,
        limit: int = _PAGE_LIMIT,
        active: bool = True,
    ) -> list[dict[str, Any]]:
        """Fetch a page of markets from the Polymarket Gamma API.

        Parameters:
            offset: Pagination offset.
            limit: Page size.
            active: Only fetch active/open markets.

        Returns:
            List of market dicts.

        Raises:
            requests.RequestException: On HTTP errors after retries.
        """
        headers = {
            "User-Agent": "GRID-DataPuller/1.0",
            "Accept": "application/json",
        }
        params: dict[str, Any] = {
            "offset": offset,
            "limit": limit,
            "closed": "false" if active else "true",
            "order": "volume24hr",
            "ascending": "false",
        }

        resp = requests.get(
            f"{_POLYMARKET_GAMMA_URL}/markets",
            headers=headers,
            params=params,
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    @retry_on_failure(
        max_attempts=3,
        backoff=2.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.RequestException,
        ),
    )
    def _fetch_market_history(
        self,
        condition_id: str,
    ) -> list[dict[str, Any]]:
        """Fetch price history for a market from Polymarket CLOB.

        Parameters:
            condition_id: The market's condition ID (token ID).

        Returns:
            List of price history entries, or empty list on failure.
        """
        headers = {
            "User-Agent": "GRID-DataPuller/1.0",
            "Accept": "application/json",
        }

        try:
            resp = requests.get(
                f"{_CLOB_URL}/prices-history",
                headers=headers,
                params={
                    "market": condition_id,
                    "interval": "1d",
                    "fidelity": 24,
                },
                timeout=_REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("history", []) if isinstance(data, dict) else data
        except Exception as exc:
            log.debug(
                "Polymarket history fetch failed for {cid}: {e}",
                cid=condition_id,
                e=str(exc),
            )
            return []

    def _fetch_all_active_markets(self) -> list[dict[str, Any]]:
        """Fetch all active markets with economic relevance.

        Paginates through the Gamma API, filtering for economically
        relevant markets. Safety limit of 10 pages.

        Returns:
            List of filtered market dicts.
        """
        relevant_markets: list[dict[str, Any]] = []
        max_pages = 10

        for page in range(max_pages):
            offset = page * _PAGE_LIMIT
            markets = self._fetch_markets_page(offset=offset)

            if not markets:
                break

            for market in markets:
                title = market.get("question", "") or market.get("title", "")
                desc = market.get("description", "")
                if _is_economically_relevant(title, desc):
                    relevant_markets.append(market)

            if len(markets) < _PAGE_LIMIT:
                break

            time.sleep(_RATE_LIMIT_DELAY)

        log.info(
            "Polymarket: found {n} economically relevant active markets",
            n=len(relevant_markets),
        )
        return relevant_markets

    # ------------------------------------------------------------------ #
    # Shift detection
    # ------------------------------------------------------------------ #

    def _detect_rapid_shifts(
        self,
        market: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Detect if a market had a rapid probability shift.

        Uses the outcomePrices field from the Gamma API for current price,
        and attempts to get 24h-ago price from history. Falls back to
        comparing current price against the market's initial probability
        if history is unavailable.

        Parameters:
            market: Market dict from Polymarket Gamma API.

        Returns:
            Shift detection dict, or None if no significant shift.
        """
        title = market.get("question", "") or market.get("title", "")

        # Parse current probability from outcomePrices
        outcome_prices = market.get("outcomePrices", "")
        if isinstance(outcome_prices, str):
            try:
                outcome_prices = json.loads(outcome_prices)
            except (json.JSONDecodeError, TypeError):
                outcome_prices = []

        if not outcome_prices or not isinstance(outcome_prices, list):
            return None

        try:
            current_prob = float(outcome_prices[0])
        except (ValueError, TypeError, IndexError):
            return None

        if math.isnan(current_prob) or math.isinf(current_prob):
            return None

        # Try to get 24h-ago probability from the CLOB price history
        previous_prob: float | None = None
        condition_id = market.get("conditionId") or market.get("condition_id", "")

        if condition_id:
            history = self._fetch_market_history(condition_id)
            if history and len(history) >= 2:
                # History entries are ordered chronologically
                # Look for entry ~24h ago
                try:
                    prev_entry = history[-2] if len(history) >= 2 else history[0]
                    previous_prob = float(prev_entry.get("price", prev_entry.get("p", 0)))
                except (ValueError, TypeError):
                    previous_prob = None

        # Fallback: use initial probability if available
        if previous_prob is None:
            initial = market.get("initialProb")
            if initial is not None:
                try:
                    previous_prob = float(initial)
                except (ValueError, TypeError):
                    pass

        if previous_prob is None:
            return None

        if math.isnan(previous_prob) or math.isinf(previous_prob):
            return None

        shift = current_prob - previous_prob
        abs_shift = abs(shift)

        if abs_shift < _MIN_SHIFT_THRESHOLD:
            return None

        direction = "UP" if shift > 0 else "DOWN"
        slug = _make_slug(title)
        impacted_tickers = _map_to_tickers(title, market.get("description", ""))

        return {
            "market_slug": slug,
            "title": title,
            "direction": direction,
            "current_prob": current_prob,
            "previous_prob": previous_prob,
            "shift": shift,
            "abs_shift": abs_shift,
            "impacted_tickers": impacted_tickers,
            "condition_id": condition_id,
            "volume_24h": market.get("volume24hr", 0),
            "liquidity": market.get("liquidity", 0),
        }

    # ------------------------------------------------------------------ #
    # Storage
    # ------------------------------------------------------------------ #

    def _store_shift(
        self,
        conn: Any,
        shift: dict[str, Any],
        obs_date: date,
    ) -> bool:
        """Store a detected probability shift in raw_series.

        Parameters:
            conn: Active database connection (within a transaction).
            shift: Detected shift dict.
            obs_date: Observation date.

        Returns:
            True if inserted, False if duplicate.
        """
        series_id = f"PREDICTION:{shift['market_slug']}:{shift['direction']}"

        if self._row_exists(series_id, obs_date, conn):
            return False

        self._insert_raw(
            conn=conn,
            series_id=series_id,
            obs_date=obs_date,
            value=shift["abs_shift"],
            raw_payload={
                "title": shift["title"],
                "direction": shift["direction"],
                "current_prob": shift["current_prob"],
                "previous_prob": shift["previous_prob"],
                "shift": shift["shift"],
                "impacted_tickers": shift["impacted_tickers"],
                "condition_id": shift["condition_id"],
                "volume_24h": shift["volume_24h"],
                "liquidity": shift["liquidity"],
            },
        )
        return True

    def _emit_shift_signal(
        self,
        conn: Any,
        shift: dict[str, Any],
        obs_date: date,
    ) -> None:
        """Emit prediction shift signals for each impacted ticker.

        Parameters:
            conn: Active database connection.
            shift: Detected shift dict.
            obs_date: Signal date.
        """
        for ticker in shift.get("impacted_tickers", []):
            try:
                conn.execute(
                    text(
                        "INSERT INTO signal_sources "
                        "(source_type, source_id, ticker, signal_date, signal_type, signal_value) "
                        "VALUES (:stype, :sid, :ticker, :sdate, :stype2, :sval) "
                        "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type) "
                        "DO NOTHING"
                    ),
                    {
                        "stype": "prediction_market",
                        "sid": f"poly_{shift['market_slug'][:50]}",
                        "ticker": ticker,
                        "sdate": obs_date,
                        "stype2": "RAPID_SHIFT",
                        "sval": json.dumps({
                            "title": shift["title"],
                            "direction": shift["direction"],
                            "shift": shift["shift"],
                            "current_prob": shift["current_prob"],
                        }),
                    },
                )
            except Exception as exc:
                log.debug(
                    "Prediction shift signal emission failed for {t}: {e}",
                    t=ticker,
                    e=str(exc),
                )

    # ------------------------------------------------------------------ #
    # Main pull methods
    # ------------------------------------------------------------------ #

    def pull_shifts(self) -> dict[str, Any]:
        """Scan all active Polymarket markets for rapid probability shifts.

        Returns:
            Dict with status, shifts_detected, rows_inserted, impacted_tickers.
        """
        today = date.today()

        try:
            markets = self._fetch_all_active_markets()
        except Exception as exc:
            log.error("Polymarket markets fetch failed: {e}", e=str(exc))
            return {
                "status": "FAILED",
                "shifts_detected": 0,
                "rows_inserted": 0,
                "error": str(exc),
            }

        if not markets:
            log.info("Polymarket: no economically relevant markets found")
            return {
                "status": "SUCCESS",
                "shifts_detected": 0,
                "rows_inserted": 0,
            }

        shifts: list[dict[str, Any]] = []
        for market in markets:
            try:
                shift = self._detect_rapid_shifts(market)
                if shift is not None:
                    shifts.append(shift)
                    log.info(
                        "PREDICTION SHIFT: '{t}' {d} {s:+.1%} "
                        "(now {c:.1%}, was {p:.1%}) — tickers: {tk}",
                        t=shift["title"][:60],
                        d=shift["direction"],
                        s=shift["shift"],
                        c=shift["current_prob"],
                        p=shift["previous_prob"],
                        tk=", ".join(shift["impacted_tickers"]) or "none",
                    )
            except Exception as exc:
                log.debug(
                    "Polymarket: shift detection failed for market: {e}",
                    e=str(exc),
                )

            time.sleep(_RATE_LIMIT_DELAY * 0.5)

        if not shifts:
            log.info("Polymarket: no rapid shifts detected today")
            return {
                "status": "SUCCESS",
                "shifts_detected": 0,
                "rows_inserted": 0,
                "markets_scanned": len(markets),
            }

        inserted = 0
        all_tickers: set[str] = set()

        with self.engine.begin() as conn:
            for shift in shifts:
                try:
                    if self._store_shift(conn, shift, today):
                        inserted += 1
                except Exception as exc:
                    log.warning(
                        "Prediction: failed to store shift for {s}: {e}",
                        s=shift["market_slug"][:40],
                        e=str(exc),
                    )

                try:
                    self._emit_shift_signal(conn, shift, today)
                except Exception as exc:
                    log.debug(
                        "Prediction: signal emission failed: {e}",
                        e=str(exc),
                    )

                all_tickers.update(shift.get("impacted_tickers", []))

        log.info(
            "Prediction odds pull complete — {n} shifts detected, "
            "{i} rows stored, {t} tickers impacted",
            n=len(shifts),
            i=inserted,
            t=len(all_tickers),
        )

        return {
            "status": "SUCCESS",
            "shifts_detected": len(shifts),
            "rows_inserted": inserted,
            "markets_scanned": len(markets),
            "impacted_tickers": sorted(all_tickers),
        }

    def pull_all(self) -> list[dict[str, Any]]:
        """Pull all prediction market shift data.

        Convenience method matching the pull_all() pattern.

        Returns:
            List containing the single result dict.
        """
        result = self.pull_shifts()
        return [result]


if __name__ == "__main__":
    from db import get_engine

    puller = PredictionOddsPuller(db_engine=get_engine())
    results = puller.pull_all()
    for r in results:
        print(
            f"  Status: {r.get('status')} — "
            f"{r.get('shifts_detected', 0)} shifts, "
            f"{r.get('rows_inserted', 0)} stored"
        )
