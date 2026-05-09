"""Breaking news monitor — detects high-impact events in near-real-time.

Polls GDELT every 60 seconds, detects article volume spikes for key topics,
and injects signals when breaking events are detected. This closes the
latency gap between world events and GRID's intelligence pipeline.

Usage:
    python -m intelligence.breaking_news          # run forever (daemon)
    python -m intelligence.breaking_news --once    # single check cycle
"""

from __future__ import annotations

import json
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# Ensure project root is on sys.path when run as module
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

POLL_INTERVAL_SECONDS = 60
SPIKE_MULTIPLIER = 3.0          # articles must exceed baseline * multiplier
COOLDOWN_SECONDS = 15 * 60      # 15 min cooldown per query after detection
GDELT_TIMESPAN_MINUTES = 60
GDELT_TIMEOUT_SECONDS = 10
GDELT_REQUEST_SPACING = 12.0   # seconds between GDELT requests (defensive after sustained 429s)
CACHE_INVALIDATION_FILE = Path("/tmp/grid_cache_invalidation")

# Positive and negative keyword sets for rudimentary direction inference
_BULLISH_WORDS = frozenset({
    "surge", "rally", "jump", "gain", "soar", "boom", "record high",
    "approval", "deal", "merger", "ceasefire", "peace", "stimulus",
    "cut", "easing", "bullish", "upgrade", "buy",
})
_BEARISH_WORDS = frozenset({
    "crash", "plunge", "drop", "fall", "collapse", "crisis", "fail",
    "default", "war", "invasion", "strike", "hack", "breach",
    "bearish", "downgrade", "sell", "recession", "layoff",
})

# ---------------------------------------------------------------------------
# Watchlist — topics to monitor with baseline article rates
# ---------------------------------------------------------------------------

WATCHLIST: list[dict[str, Any]] = [
    {"query": "ceasefire OR peace deal OR truce",
     "category": "geopolitical", "baseline_per_hour": 5},
    {"query": "tariff OR trade war OR sanctions",
     "category": "trade", "baseline_per_hour": 10},
    {"query": "rate cut OR rate hike OR emergency meeting",
     "category": "central_bank", "baseline_per_hour": 8},
    {"query": "invasion OR military strike OR war",
     "category": "geopolitical", "baseline_per_hour": 3},
    {"query": "bank failure OR bank run OR systemic",
     "category": "credit", "baseline_per_hour": 2},
    {"query": "default OR debt crisis OR restructuring",
     "category": "credit", "baseline_per_hour": 3},
    {"query": "crash OR circuit breaker OR flash crash",
     "category": "market", "baseline_per_hour": 2},
    {"query": "FDA approval OR clinical trial OR drug approval",
     "category": "biotech", "baseline_per_hour": 5},
    {"query": "merger OR acquisition OR takeover bid",
     "category": "corporate", "baseline_per_hour": 8},
    {"query": "hack OR data breach OR cyber attack",
     "category": "cyber", "baseline_per_hour": 4},
    {"query": "earthquake OR hurricane OR natural disaster",
     "category": "physical", "baseline_per_hour": 3},
    {"query": "Trump OR Biden OR executive order",
     "category": "political", "baseline_per_hour": 15},
]


# ---------------------------------------------------------------------------
# GDELT DOC API — article count retrieval
# ---------------------------------------------------------------------------

def check_gdelt(query: str, minutes: int = GDELT_TIMESPAN_MINUTES) -> int:
    """Hit GDELT DOC API and return article count in the last *minutes* minutes.

    Returns 0 on any error (graceful degradation).
    """
    url = "https://api.gdeltproject.org/api/v2/doc/doc"
    # GDELT requires OR'd queries to be wrapped in parentheses.
    safe_query = f"({query})" if " OR " in query and not query.startswith("(") else query
    params = {
        "query": safe_query,
        "mode": "TimelineVolRaw",
        "timespan": f"{minutes}min",
        "format": "json",
    }
    try:
        resp = requests.get(url, params=params, timeout=GDELT_TIMEOUT_SECONDS)
        if resp.status_code == 429:
            log.warning("GDELT 429 rate-limited for '{q}' — backing off", q=query[:40])
            time.sleep(GDELT_REQUEST_SPACING)
            return 0
        resp.raise_for_status()
        # GDELT sometimes returns text body even on 200 (e.g., 'Invalid mode.',
        # 'Timespan is too short.'). Detect that before json().
        body = resp.text.lstrip()
        if not body.startswith('{') and not body.startswith('['):
            log.warning("GDELT non-JSON 200 for '{q}': {b}", q=query[:40], b=body[:120])
            return 0
        data = resp.json()
        # GDELT artcount returns {"artcount": N} or a timeline array.
        # Handle both formats.
        if isinstance(data, dict):
            if "artcount" in data:
                return int(data["artcount"])
            # Sometimes returns {"timeline": [{"data": [...]}]}
            timeline = data.get("timeline", [])
            if timeline:
                series = timeline[0].get("data", [])
                return sum(int(pt.get("value", pt.get("count", 0))) for pt in series)
        if isinstance(data, list):
            return sum(int(pt.get("value", pt.get("count", 0))) for pt in data)
        return 0
    except requests.RequestException as exc:
        log.warning("GDELT request failed for '{q}': {e}", q=query, e=str(exc))
        return 0
    except (ValueError, KeyError, TypeError) as exc:
        log.warning("GDELT response parse error for '{q}': {e}", q=query, e=str(exc))
        return 0


# ---------------------------------------------------------------------------
# Spike detection
# ---------------------------------------------------------------------------

def detect_spike(
    article_count: int,
    baseline_per_hour: float,
    timespan_minutes: int = GDELT_TIMESPAN_MINUTES,
    multiplier: float = SPIKE_MULTIPLIER,
) -> tuple[bool, float]:
    """Return (is_spike, spike_ratio) for a given article count.

    The expected count in *timespan_minutes* is baseline_per_hour / 60 * timespan_minutes.
    A spike is detected when actual exceeds expected * multiplier.
    """
    expected = baseline_per_hour / 60.0 * timespan_minutes
    if expected <= 0:
        return False, 0.0
    ratio = article_count / expected
    return ratio >= multiplier, ratio


# ---------------------------------------------------------------------------
# Direction inference from article titles / query text
# ---------------------------------------------------------------------------

def infer_direction(query: str, titles: list[str] | None = None) -> str:
    """Infer bullish/bearish/neutral from query keywords and optional titles."""
    combined = query.lower()
    if titles:
        combined += " " + " ".join(t.lower() for t in titles)

    bull_hits = sum(1 for w in _BULLISH_WORDS if w in combined)
    bear_hits = sum(1 for w in _BEARISH_WORDS if w in combined)

    if bull_hits > bear_hits:
        return "buy"
    if bear_hits > bull_hits:
        return "sell"
    return "neutral"


# ---------------------------------------------------------------------------
# Signal injection
# ---------------------------------------------------------------------------

_INSERT_SIGNAL = text("""
    INSERT INTO signal_data
        (signal_type, signal_date, ticker, actor, direction,
         magnitude, description, data, confidence, source_id, created_at)
    VALUES (:signal_type, :signal_date, :ticker, :actor, :direction,
            :magnitude, :description, CAST(:data AS jsonb),
            :confidence, :source_id, NOW())
""")


def inject_signal(engine: Engine, event: dict[str, Any]) -> None:
    """Write a breaking-news signal into signal_data."""
    metadata = json.dumps({
        "query": event["query"],
        "category": event["category"],
        "article_count": event["article_count"],
        "spike_ratio": round(event["spike_ratio"], 2),
    })
    confidence = "derived" if event["spike_ratio"] < 5.0 else "estimated"
    magnitude = min(10.0, event["spike_ratio"])  # cap at 10

    with engine.begin() as conn:
        conn.execute(_INSERT_SIGNAL, {
            "signal_type": "breaking_news",
            "signal_date": date.today(),
            "ticker": "MACRO",
            "actor": f"gdelt:{event['category']}",
            "direction": event.get("direction", "neutral"),
            "magnitude": magnitude,
            "description": (
                f"Breaking: {event['category']} spike "
                f"({event['article_count']} articles in {GDELT_TIMESPAN_MINUTES}min, "
                f"{event['spike_ratio']:.1f}x baseline) — {event['query'][:80]}"
            ),
            "data": metadata,
            "confidence": confidence,
            "source_id": "gdelt_breaking",
        })
    log.info(
        "Injected breaking_news signal: {cat} — {cnt} articles, {ratio:.1f}x spike",
        cat=event["category"],
        cnt=event["article_count"],
        ratio=event["spike_ratio"],
    )


# ---------------------------------------------------------------------------
# Cache invalidation (file-based for cross-process signalling)
# ---------------------------------------------------------------------------

def invalidate_caches() -> None:
    """Signal the API process to clear cached intelligence data.

    Writes a timestamp file that the API can poll. This avoids importing
    API internals from a separate daemon process.
    """
    try:
        CACHE_INVALIDATION_FILE.write_text(
            datetime.now(timezone.utc).isoformat(), encoding="utf-8"
        )
        log.debug("Cache invalidation flag written to {p}", p=CACHE_INVALIDATION_FILE)
    except OSError as exc:
        log.warning("Failed to write cache invalidation flag: {e}", e=str(exc))


# ---------------------------------------------------------------------------
# Main monitor loop
# ---------------------------------------------------------------------------

def run_monitor(interval: int = POLL_INTERVAL_SECONDS, once: bool = False) -> None:
    """Poll GDELT for all watchlist queries and inject signals on spikes.

    Parameters:
        interval: Seconds between poll cycles.
        once: If True, run a single cycle and exit.
    """
    from db import get_engine
    engine = get_engine()

    # Cooldown tracker: query_text -> last_detection_timestamp
    cooldowns: dict[str, float] = {}

    log.info(
        "Breaking news monitor started — {n} watchlist queries, "
        "poll every {s}s, spike threshold {m}x",
        n=len(WATCHLIST), s=interval, m=SPIKE_MULTIPLIER,
    )

    while True:
        cycle_start = time.monotonic()
        detections = 0

        for i, item in enumerate(WATCHLIST):
            query = item["query"]
            now = time.time()

            # Skip if in cooldown
            last_hit = cooldowns.get(query, 0.0)
            if now - last_hit < COOLDOWN_SECONDS:
                remaining = int(COOLDOWN_SECONDS - (now - last_hit))
                log.debug(
                    "Skipping '{q}' — cooldown {r}s remaining",
                    q=query[:40], r=remaining,
                )
                continue

            # Rate limit: space out GDELT requests to stay under free tier
            if i > 0:
                time.sleep(GDELT_REQUEST_SPACING)

            article_count = check_gdelt(query)
            is_spike, ratio = detect_spike(article_count, item["baseline_per_hour"])

            if is_spike:
                direction = infer_direction(query)
                event = {
                    "query": query,
                    "category": item["category"],
                    "article_count": article_count,
                    "spike_ratio": ratio,
                    "direction": direction,
                }
                try:
                    inject_signal(engine, event)
                    invalidate_caches()
                    cooldowns[query] = now
                    detections += 1
                except Exception as exc:
                    log.error(
                        "Failed to inject signal for '{q}': {e}",
                        q=query[:40], e=str(exc),
                    )
            else:
                log.debug(
                    "No spike for '{q}': {cnt} articles, {r:.1f}x",
                    q=query[:40], cnt=article_count, r=ratio,
                )

        elapsed = time.monotonic() - cycle_start
        log.info(
            "Breaking news cycle complete — {d} detections in {t:.1f}s",
            d=detections, t=elapsed,
        )

        if once:
            break

        # Sleep the remainder of the interval
        sleep_time = max(1.0, interval - elapsed)
        time.sleep(sleep_time)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="GRID Breaking News Monitor")
    parser.add_argument("--once", action="store_true", help="Run a single cycle and exit")
    parser.add_argument("--interval", type=int, default=POLL_INTERVAL_SECONDS,
                        help=f"Seconds between polls (default: {POLL_INTERVAL_SECONDS})")
    args = parser.parse_args()

    run_monitor(interval=args.interval, once=args.once)
