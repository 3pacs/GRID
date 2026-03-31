"""
GRID Kalshi Prediction Markets ingestion module.

Pulls event contract data from the Kalshi prediction markets API for
macro-relevant events: recession, Fed rate decisions, inflation,
GDP, unemployment, government shutdown, and elections.

Prediction market prices are real-money-backed probability estimates
and provide forward-looking sentiment that complements survey-based
indicators. Kalshi is a CFTC-regulated exchange.

Data source: https://api.elections.kalshi.com/trade-api/v2/markets

Series stored (per matched event):
- kalshi.{event_slug}.probability: Yes price as implied probability (0-1)
- kalshi.{event_slug}.volume: Contract trading volume
"""

from __future__ import annotations

import math
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ---- API ----
_KALSHI_MARKETS_URL: str = (
    "https://api.elections.kalshi.com/trade-api/v2/markets"
)

# Series ID prefix
_SERIES_PREFIX: str = "kalshi"

# Keywords used to filter macro-relevant event contracts.
# Each tuple is (slug_fragment, description).
MACRO_EVENT_KEYWORDS: list[tuple[str, str]] = [
    ("recession", "US recession probability"),
    ("fed-rate", "Federal Reserve rate decision"),
    ("fed-funds", "Federal funds rate target"),
    ("fomc", "FOMC meeting outcome"),
    ("inflation", "Inflation / CPI expectations"),
    ("cpi", "Consumer Price Index outcome"),
    ("gdp", "GDP growth expectations"),
    ("unemployment", "Unemployment rate outcome"),
    ("nonfarm", "Nonfarm payrolls outcome"),
    ("government-shutdown", "US government shutdown probability"),
    ("shutdown", "US government shutdown probability"),
    ("election", "US election outcome"),
    ("presidential", "US presidential election"),
    ("debt-ceiling", "Debt ceiling resolution"),
]

# Set of keyword fragments for fast lookup
_KEYWORD_FRAGMENTS: set[str] = {kw for kw, _ in MACRO_EVENT_KEYWORDS}

# Feature definitions
KALSHI_FEATURES: dict[str, str] = {
    "probability": "Implied probability from yes_price (0-1 scale)",
    "volume": "Contract trading volume (number of contracts traded)",
}

# HTTP config
_REQUEST_TIMEOUT: int = 30
_RATE_LIMIT_DELAY: float = 1.0
_PAGE_LIMIT: int = 200  # Kalshi API max per page


def _is_macro_relevant(ticker: str, title: str) -> bool:
    """Check if a Kalshi market is macro-relevant based on ticker/title.

    Parameters:
        ticker: Market ticker string (e.g., 'RECESSION-2024').
        title: Market title / question text.

    Returns:
        True if the market matches any macro event keyword.
    """
    combined = f"{ticker} {title}".lower()
    return any(fragment in combined for fragment in _KEYWORD_FRAGMENTS)


def _make_event_slug(ticker: str) -> str:
    """Normalise a Kalshi ticker into a series-safe slug.

    Converts to lowercase, replaces non-alphanumeric chars with
    underscores, and strips leading/trailing underscores.

    Parameters:
        ticker: Raw Kalshi market ticker.

    Returns:
        Normalised slug safe for use in series_id.
    """
    slug = ticker.lower()
    cleaned = []
    for ch in slug:
        if ch.isalnum() or ch == "_":
            cleaned.append(ch)
        else:
            cleaned.append("_")
    result = "".join(cleaned).strip("_")
    # Collapse multiple underscores
    while "__" in result:
        result = result.replace("__", "_")
    return result


class KalshiPuller(BasePuller):
    """Pulls macro-relevant prediction market data from Kalshi.

    Kalshi is a CFTC-regulated event contract exchange. Yes-prices
    represent real-money-backed implied probabilities for binary
    outcomes (recession, rate hikes, inflation thresholds, etc.).

    The puller fetches all active markets, filters for macro-relevant
    events using keyword matching, and stores implied probability and
    volume for each matched contract.

    Features (per event slug):
    - kalshi.{slug}.probability: Yes price as implied probability (0-1)
    - kalshi.{slug}.volume: Contract trading volume

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for Kalshi.
    """

    SOURCE_NAME: str = "Kalshi"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://api.elections.kalshi.com/trade-api/v2",
        "cost_tier": "FREE",
        "latency_class": "INTRADAY",
        "pit_available": True,
        "revision_behavior": "FREQUENT",
        "trust_score": "HIGH",
        "priority_rank": 25,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the Kalshi prediction markets puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info(
            "KalshiPuller initialised -- source_id={sid}",
            sid=self.source_id,
        )

    @staticmethod
    def _infer_tickers_from_title(title: str) -> list[str]:
        """Infer impacted stock tickers from a market title."""
        title_lower = title.lower()
        mapping = {
            "recession": ["SPY", "TLT"], "fed rate": ["SPY", "TLT", "XLF"],
            "inflation": ["TIP", "GLD", "SPY"], "cpi": ["SPY", "TLT"],
            "gdp": ["SPY"], "unemployment": ["SPY", "XLF"],
            "nonfarm": ["SPY"], "government shutdown": ["SPY"],
            "debt ceiling": ["SPY", "TLT"], "election": ["SPY"],
            "bitcoin": ["COIN", "MSTR"], "crypto": ["COIN"],
            "oil": ["USO", "XLE"], "gold": ["GLD"],
        }
        tickers = set()
        for keyword, tkrs in mapping.items():
            if keyword in title_lower:
                tickers.update(tkrs)
        return list(tickers) if tickers else ["SPY"]  # default to SPY for macro events

    def _series_id(self, slug: str, feature: str) -> str:
        """Build the full series_id for an event feature.

        Parameters:
            slug: Normalised event slug (e.g., 'recession_2024').
            feature: Feature suffix ('probability' or 'volume').

        Returns:
            Full series_id (e.g., 'kalshi.recession_2024.probability').
        """
        return f"{_SERIES_PREFIX}.{slug}.{feature}"

    # ------------------------------------------------------------------ #
    # API interaction
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
        cursor: str | None = None,
        status: str = "open",
    ) -> dict[str, Any]:
        """Fetch a single page of markets from the Kalshi API.

        Parameters:
            cursor: Pagination cursor for next page (None for first).
            status: Market status filter ('open', 'closed', etc.).

        Returns:
            Parsed JSON response dict with 'markets' and 'cursor' keys.

        Raises:
            requests.RequestException: On HTTP errors after retries.
        """
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (compatible; GRID-DataPuller/1.0; "
                "+https://github.com/grid-trading)"
            ),
            "Accept": "application/json",
        }
        params: dict[str, Any] = {
            "limit": _PAGE_LIMIT,
            "status": status,
        }
        if cursor:
            params["cursor"] = cursor

        resp = requests.get(
            _KALSHI_MARKETS_URL,
            headers=headers,
            params=params,
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def _fetch_all_macro_markets(self) -> list[dict[str, Any]]:
        """Fetch all macro-relevant markets, paginating through results.

        Iterates through the Kalshi markets API, filtering each page
        for macro-relevant events. Stops when no more pages are available
        or after a safety limit of pages to prevent runaway requests.

        Returns:
            List of market dicts that match macro event keywords.
        """
        macro_markets: list[dict[str, Any]] = []
        cursor: str | None = None
        max_pages = 25  # Safety limit

        for page_num in range(1, max_pages + 1):
            data = self._fetch_markets_page(cursor=cursor)
            markets = data.get("markets") or []

            if not markets:
                log.debug("Kalshi: no markets on page {p}, stopping", p=page_num)
                break

            for market in markets:
                ticker = market.get("ticker", "")
                title = market.get("title", "")
                if _is_macro_relevant(ticker, title):
                    macro_markets.append(market)

            # Check for next page
            next_cursor = data.get("cursor")
            if not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor

            if page_num < max_pages:
                time.sleep(_RATE_LIMIT_DELAY)

        log.info(
            "Kalshi: found {n} macro-relevant markets",
            n=len(macro_markets),
        )
        return macro_markets

    # ------------------------------------------------------------------ #
    # Parsing
    # ------------------------------------------------------------------ #

    def _parse_market(
        self, market: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Parse a single Kalshi market into storable fields.

        Extracts yes_price (probability), volume, open_interest, and
        metadata. Validates numeric values and skips markets with
        missing or invalid data.

        Parameters:
            market: Raw market dict from the Kalshi API.

        Returns:
            Parsed dict with slug, obs_date, probability, volume,
            open_interest, and raw_payload; or None if invalid.
        """
        ticker = market.get("ticker", "")
        if not ticker:
            return None

        slug = _make_event_slug(ticker)

        # Extract yes_price as probability (Kalshi uses cents: 0-100)
        yes_price = market.get("yes_ask") or market.get("last_price")
        if yes_price is None:
            yes_price = market.get("yes_bid")
        if yes_price is None:
            log.debug("Kalshi {t}: no price data, skipping", t=ticker)
            return None

        try:
            probability = float(yes_price) / 100.0
        except (ValueError, TypeError):
            log.warning(
                "Kalshi {t}: invalid yes_price {v}",
                t=ticker,
                v=yes_price,
            )
            return None

        # Validate probability is in [0, 1] and not NaN/inf
        if math.isnan(probability) or math.isinf(probability):
            log.warning(
                "Kalshi {t}: probability is NaN/inf, skipping", t=ticker
            )
            return None
        probability = max(0.0, min(1.0, probability))

        # Volume and open interest
        volume = market.get("volume", 0)
        try:
            volume = int(volume) if volume is not None else 0
        except (ValueError, TypeError):
            volume = 0

        open_interest = market.get("open_interest", 0)
        try:
            open_interest = int(open_interest) if open_interest is not None else 0
        except (ValueError, TypeError):
            open_interest = 0

        # Determine observation date from close_time or use today
        close_time = market.get("close_time") or market.get("expiration_time")
        obs_date = date.today()

        raw_payload = {
            "ticker": ticker,
            "title": market.get("title", ""),
            "subtitle": market.get("subtitle", ""),
            "status": market.get("status", ""),
            "yes_ask": market.get("yes_ask"),
            "yes_bid": market.get("yes_bid"),
            "last_price": market.get("last_price"),
            "volume": volume,
            "open_interest": open_interest,
            "close_time": close_time,
            "category": market.get("category", ""),
            "source_url": _KALSHI_MARKETS_URL,
        }

        return {
            "slug": slug,
            "obs_date": obs_date,
            "probability": probability,
            "volume": float(volume),
            "open_interest": open_interest,
            "raw_payload": raw_payload,
        }

    # ------------------------------------------------------------------ #
    # Pull methods
    # ------------------------------------------------------------------ #

    def pull_markets(
        self,
        start_date: str | date = "2021-01-01",
        days_back: int | None = None,
    ) -> dict[str, Any]:
        """Pull macro-relevant Kalshi prediction market data.

        Fetches all active macro-relevant markets, extracts probability
        and volume for each, and stores with batch deduplication via
        _get_existing_dates().

        Parameters:
            start_date: Earliest observation date to store.
            days_back: If set, only store data from this many days ago.

        Returns:
            dict with status, rows_inserted (total), per_feature counts,
            and markets_found.
        """
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)

        if days_back is not None:
            cutoff = date.today() - timedelta(days=days_back)
            start_date = max(start_date, cutoff)

        try:
            raw_markets = self._fetch_all_macro_markets()
        except Exception as exc:
            log.error("Kalshi markets pull failed: {e}", e=str(exc))
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "error": str(exc),
            }

        if not raw_markets:
            log.warning("Kalshi: no macro-relevant markets found")
            return {
                "status": "SUCCESS",
                "rows_inserted": 0,
                "markets_found": 0,
            }

        # Parse all markets
        parsed: list[dict[str, Any]] = []
        for market in raw_markets:
            result = self._parse_market(market)
            if result is not None and result["obs_date"] >= start_date:
                parsed.append(result)

        if not parsed:
            log.info("Kalshi: no valid markets after filtering")
            return {
                "status": "SUCCESS",
                "rows_inserted": 0,
                "markets_found": len(raw_markets),
            }

        total_inserted = 0
        per_feature: dict[str, int] = {}

        with self.engine.begin() as conn:
            for record in parsed:
                slug = record["slug"]
                obs_date = record["obs_date"]

                # -- probability --
                sid_prob = self._series_id(slug, "probability")
                existing_prob = self._get_existing_dates(sid_prob, conn)

                if obs_date not in existing_prob:
                    self._insert_raw(
                        conn=conn,
                        series_id=sid_prob,
                        obs_date=obs_date,
                        value=record["probability"],
                        raw_payload=record["raw_payload"],
                    )
                    per_feature[sid_prob] = per_feature.get(sid_prob, 0) + 1
                    total_inserted += 1

                # -- volume --
                sid_vol = self._series_id(slug, "volume")
                existing_vol = self._get_existing_dates(sid_vol, conn)

                if obs_date not in existing_vol:
                    self._insert_raw(
                        conn=conn,
                        series_id=sid_vol,
                        obs_date=obs_date,
                        value=record["volume"],
                        raw_payload=record["raw_payload"],
                    )
                    per_feature[sid_vol] = per_feature.get(sid_vol, 0) + 1
                    total_inserted += 1

        # Register high-conviction markets as signals for trust scoring
        signals_registered = 0
        try:
            from intelligence.trust_scorer import register_signal
            for record in parsed:
                prob = record.get("probability", 0.5)
                title = record.get("raw_payload", {}).get("title", record.get("slug", ""))
                # High probability (>0.75) or low (<0.25) = directional signal
                if prob > 0.75 or prob < 0.25:
                    # Map to impacted tickers from title keywords
                    impacted = self._infer_tickers_from_title(title)
                    direction = "BUY" if prob > 0.75 else "SELL"
                    for ticker in impacted:
                        register_signal(
                            engine=self.engine,
                            source_type="prediction_market",
                            source_id=f"kalshi:{record.get('slug', 'unknown')}",
                            ticker=ticker,
                            signal_type=direction,
                            signal_value=prob,
                            metadata={"title": title, "probability": prob, "platform": "kalshi"},
                        )
                        signals_registered += 1
        except Exception as exc:
            log.debug("Kalshi signal registration failed: {e}", e=str(exc))

        log.info(
            "Kalshi pull complete -- {n} rows inserted across {m} markets, {s} signals registered",
            n=total_inserted,
            m=len(parsed),
            s=signals_registered,
        )

        return {
            "status": "SUCCESS",
            "rows_inserted": total_inserted,
            "markets_found": len(raw_markets),
            "markets_stored": len(parsed),
            "per_feature": per_feature,
            "signals_registered": signals_registered,
        }

    def pull_all(
        self,
        start_date: str | date = "2021-01-01",
        days_back: int | None = None,
    ) -> list[dict[str, Any]]:
        """Pull all Kalshi prediction market features.

        Convenience method matching the pull_all() pattern used by
        other GRID pullers.

        Parameters:
            start_date: Earliest observation date.
            days_back: If set, only store recent data.

        Returns:
            List containing the single result dict.
        """
        result = self.pull_markets(
            start_date=start_date,
            days_back=days_back,
        )
        return [result]
