"""
GRID Prediction Market Multi-Platform Puller via pmxt SDK.

Pulls prediction market data from ALL platforms supported by the pmxt
unified SDK (Polymarket, Kalshi, Limitless, Probable Markets, Myriad
Markets, Opinion Trade). Focuses on macro-relevant events using keyword
filtering.

Series stored:
- pmxt.{platform}.{event_slug}.{outcome}

Source: pmxt SDK (unified prediction market API)
Schedule: Daily
"""

from __future__ import annotations

import hashlib
import math
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import settings
from ingestion.base import BasePuller

# ── Graceful import of pmxt SDK ──────────────────────────────────────────

try:
    import pmxt

    _PMXT_AVAILABLE = True
except ImportError:
    pmxt = None  # type: ignore[assignment]
    _PMXT_AVAILABLE = False
    log.info("pmxt SDK not installed — run: pip install pmxt")


# ── Configuration ────────────────────────────────────────────────────────

_RATE_LIMIT_DELAY: float = 1.5
_MAX_EVENTS_PER_PLATFORM: int = 200

# Keywords that identify economically relevant markets (shared with
# prediction_odds.py — kept in sync manually).
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

# Platforms to iterate over when pulling
_PLATFORMS: list[str] = [
    "polymarket",
    "kalshi",
    "limitless",
    "probable",
    "myriad",
    "opiniontrade",
]


# ── Helpers ──────────────────────────────────────────────────────────────

def _sanitize_slug(raw: str, max_len: int = 80) -> str:
    """Convert raw text into a series-safe lowercase slug.

    Parameters:
        raw: Raw text (event title, outcome label, etc.).
        max_len: Maximum slug length before truncation.

    Returns:
        Lowercase alphanumeric slug with underscores.
    """
    slug = raw.lower().strip()
    cleaned = []
    for ch in slug:
        if ch.isalnum() or ch in ("_", " "):
            cleaned.append(ch)
    result = "_".join("".join(cleaned).split())
    if len(result) > max_len:
        hash_suffix = hashlib.md5(result.encode()).hexdigest()[:8]
        result = result[: max_len - 9] + "_" + hash_suffix
    return result


def _is_economically_relevant(title: str, description: str = "") -> bool:
    """Check if an event is economically relevant.

    Parameters:
        title: Event title.
        description: Event description.

    Returns:
        True if the event matches any economic keyword.
    """
    combined = f"{title} {description}".lower()
    return any(kw in combined for kw in _ECONOMIC_KEYWORDS)


def _safe_float(value: Any) -> float | None:
    """Safely convert a value to float, returning None on failure or NaN/inf.

    Parameters:
        value: Input value to convert.

    Returns:
        Float value or None if invalid.
    """
    if value is None:
        return None
    try:
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (ValueError, TypeError):
        return None


# ── Puller ───────────────────────────────────────────────────────────────


class PmxtPredictionPuller(BasePuller):
    """Multi-platform prediction market puller using the pmxt SDK.

    Iterates over all pmxt-supported platforms, fetches events matching
    economic keywords, and stores probability and volume data.

    Series pattern: pmxt.{platform}.{event_slug}.{outcome}

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for pmxt.
    """

    SOURCE_NAME: str = "pmxt"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://github.com/pmxt/pmxt",
        "cost_tier": "FREE",
        "latency_class": "INTRADAY",
        "pit_available": True,
        "revision_behavior": "FREQUENT",
        "trust_score": "HIGH",
        "priority_rank": 23,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the pmxt prediction puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info(
            "PmxtPredictionPuller initialised — source_id={sid}, pmxt_available={a}",
            sid=self.source_id,
            a=_PMXT_AVAILABLE,
        )

    # ------------------------------------------------------------------ #
    # Platform event fetching
    # ------------------------------------------------------------------ #

    def _fetch_platform_events(
        self,
        platform: str,
    ) -> list[dict[str, Any]]:
        """Fetch economically relevant events from a single pmxt platform.

        Parameters:
            platform: Platform name (e.g. 'polymarket', 'kalshi').

        Returns:
            List of relevant event dicts with normalised fields.
        """
        if not _PMXT_AVAILABLE:
            return []

        relevant: list[dict[str, Any]] = []

        for keyword in _ECONOMIC_KEYWORDS:
            try:
                events = pmxt.fetch_events(query=keyword, platform=platform)
                if not events:
                    continue

                for event in events:
                    title = getattr(event, "title", "") or ""
                    description = getattr(event, "description", "") or ""

                    if not _is_economically_relevant(title, description):
                        continue

                    markets = getattr(event, "markets", []) or []
                    event_slug = _sanitize_slug(title)

                    for market in markets:
                        outcomes = getattr(market, "outcomes", []) or []
                        for outcome in outcomes:
                            outcome_name = getattr(outcome, "name", "") or "yes"
                            yes_price = _safe_float(
                                getattr(outcome, "yes_price", None)
                                or getattr(outcome, "price", None)
                            )
                            volume = _safe_float(
                                getattr(market, "volume", None)
                                or getattr(outcome, "volume", None)
                            )

                            if yes_price is None:
                                continue

                            relevant.append({
                                "platform": platform,
                                "event_slug": event_slug,
                                "title": title,
                                "description": description[:200],
                                "outcome": _sanitize_slug(outcome_name),
                                "yes_price": yes_price,
                                "volume": volume or 0.0,
                                "event_id": getattr(event, "id", "") or "",
                                "market_id": getattr(market, "id", "") or "",
                            })

                time.sleep(_RATE_LIMIT_DELAY)

            except Exception as exc:
                log.debug(
                    "pmxt fetch failed for {p}/{kw}: {e}",
                    p=platform,
                    kw=keyword,
                    e=str(exc),
                )
                continue

            if len(relevant) >= _MAX_EVENTS_PER_PLATFORM:
                break

        return relevant

    # ------------------------------------------------------------------ #
    # Storage
    # ------------------------------------------------------------------ #

    def _store_event(
        self,
        conn: Any,
        event: dict[str, Any],
        obs_date: date,
    ) -> bool:
        """Store a single event outcome in raw_series.

        Parameters:
            conn: Active database connection (within a transaction).
            event: Normalised event dict.
            obs_date: Observation date.

        Returns:
            True if inserted, False if duplicate.
        """
        series_id = (
            f"pmxt.{event['platform']}.{event['event_slug']}.{event['outcome']}"
        )

        if self._row_exists(series_id, obs_date, conn):
            return False

        self._insert_raw(
            conn=conn,
            series_id=series_id,
            obs_date=obs_date,
            value=event["yes_price"],
            raw_payload={
                "platform": event["platform"],
                "title": event["title"],
                "outcome": event["outcome"],
                "yes_price": event["yes_price"],
                "volume": event["volume"],
                "event_id": event["event_id"],
                "market_id": event["market_id"],
            },
        )
        return True

    # ------------------------------------------------------------------ #
    # Main pull
    # ------------------------------------------------------------------ #

    def pull(self, engine: Engine | None = None) -> dict[str, Any]:
        """Pull prediction market data from all pmxt platforms.

        Parameters:
            engine: Optional engine override (unused, kept for BasePuller compat).

        Returns:
            Dict with status, events_scanned, series_stored, per-platform counts.
        """
        if not _PMXT_AVAILABLE:
            log.warning("pmxt SDK not installed — skipping prediction pull")
            return {
                "status": "SKIPPED",
                "reason": "pmxt not installed",
                "events_scanned": 0,
                "series_stored": 0,
            }

        if not settings.PMXT_ENABLED:
            log.info("PMXT_ENABLED=False — skipping prediction pull")
            return {
                "status": "SKIPPED",
                "reason": "PMXT_ENABLED is False",
                "events_scanned": 0,
                "series_stored": 0,
            }

        today = date.today()
        total_scanned = 0
        total_stored = 0
        platform_counts: dict[str, int] = {}

        for platform in _PLATFORMS:
            try:
                events = self._fetch_platform_events(platform)
                scanned = len(events)
                total_scanned += scanned
                stored = 0

                if events:
                    with self.engine.begin() as conn:
                        for event in events:
                            try:
                                if self._store_event(conn, event, today):
                                    stored += 1
                            except Exception as exc:
                                log.warning(
                                    "pmxt: failed to store event {p}/{s}: {e}",
                                    p=platform,
                                    s=event.get("event_slug", "?")[:40],
                                    e=str(exc),
                                )

                total_stored += stored
                platform_counts[platform] = stored

                log.info(
                    "pmxt {p}: {s} events scanned, {i} stored",
                    p=platform,
                    s=scanned,
                    i=stored,
                )

            except Exception as exc:
                log.error(
                    "pmxt: platform {p} pull failed: {e}",
                    p=platform,
                    e=str(exc),
                )
                platform_counts[platform] = 0

        log.info(
            "pmxt pull complete — {s} events scanned, {i} series stored across {n} platforms",
            s=total_scanned,
            i=total_stored,
            n=len(_PLATFORMS),
        )

        return {
            "status": "SUCCESS",
            "events_scanned": total_scanned,
            "series_stored": total_stored,
            "platform_counts": platform_counts,
        }

    def pull_all(self) -> list[dict[str, Any]]:
        """Pull all prediction market data.

        Convenience method matching the pull_all() pattern.

        Returns:
            List containing the single result dict.
        """
        result = self.pull()
        return [result]


if __name__ == "__main__":
    from db import get_engine

    puller = PmxtPredictionPuller(db_engine=get_engine())
    results = puller.pull_all()
    for r in results:
        print(
            f"  Status: {r.get('status')} — "
            f"{r.get('events_scanned', 0)} scanned, "
            f"{r.get('series_stored', 0)} stored"
        )
