"""Signal Backlinker — closes the loop between signals and the actor graph.

The pipeline today is one-directional:
    ingest → resolve → feature → signal → hypothesis → score

This module adds the return path:
    signal → actor_resolve → actor_connections → influence_update

For every signal with an actor name, it:
    1. Finds or creates the actor in the actors table
    2. Creates a connection (actor → ticker) in actor_connections
    3. Updates trust/influence scores based on signal outcomes

Runs as a post-processor after each puller cycle, or as a standalone daemon.
"""

from __future__ import annotations

import json
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Signal types that contain real actor names (not Reddit usernames) ──
REAL_ACTOR_SIGNAL_TYPES = {
    "whale_flow",
    "unusual_options",
    "gov_contract",
    "news_event",
    "breaking_news",
    "congressional",
    "insider",
    "darkpool",
    "foreign_lobbying",
    "campaign_finance",
    "offshore_leak",
}

# Signal types that are noise (Reddit/social handles, not real actors)
NOISE_SIGNAL_TYPES = {"social_sentiment", "dex_liquidity_spike"}

# ── Relationship mapping from signal type ──
SIGNAL_TO_RELATIONSHIP = {
    "whale_flow": "traded",
    "unusual_options": "options_activity",
    "gov_contract": "government_contract",
    "news_event": "mentioned_in_news",
    "breaking_news": "breaking_news_subject",
    "congressional": "congressional_trade",
    "insider": "insider_trade",
    "darkpool": "darkpool_trade",
    "foreign_lobbying": "lobbying_target",
    "campaign_finance": "campaign_donor",
    "offshore_leak": "offshore_connection",
}

# ── Category inference from signal context ──
ACTOR_CATEGORY_HINTS = {
    "congressional": "politician",
    "insider": "insider",
    "foreign_lobbying": "government",
    "campaign_finance": "politician",
    "whale_flow": "fund",
    "gov_contract": "corporation",
}

# Minimum actor name length to filter out garbage
MIN_ACTOR_NAME_LEN = 3

# Regex to detect Reddit-style usernames and garbage
_NOISE_PATTERN = re.compile(
    r"^[A-Za-z]+\d{2,}$|"       # CoolGuy1234
    r"^[a-z]+[-_][a-z]+$|"       # some-handle
    r"^u/|"                       # Reddit u/ prefix
    r"^@|"                        # Twitter @handle
    r"^\d+$|"                     # Pure numbers
    r"^[A-Z]{1,5}$",             # Ticker symbols mistaken as actors
)


def is_real_actor(name: str, signal_type: str) -> bool:
    """Filter out noise — Reddit handles, bot names, short garbage."""
    if not name or len(name) < MIN_ACTOR_NAME_LEN:
        return False
    if signal_type in NOISE_SIGNAL_TYPES:
        return False
    if _NOISE_PATTERN.match(name):
        return False
    # Must contain at least one space or be a known org pattern
    # Single-word names are often tickers or handles
    if " " not in name and len(name) < 8 and signal_type not in {"gov_contract"}:
        return False
    return True


def _normalize_actor_id(name: str) -> str:
    """Generate a stable actor ID from a name."""
    return name.strip().lower().replace(" ", "_").replace(".", "").replace(",", "")


def backlink_signals(engine: Engine, batch_size: int = 1000, since_minutes: int = 60) -> dict[str, int]:
    """Process recent signals and create actor graph backlinks.

    Args:
        engine: SQLAlchemy engine.
        batch_size: Max signals to process per run.
        since_minutes: Only process signals created in the last N minutes.

    Returns:
        Stats dict with counts of actors created, connections made, etc.
    """
    stats = {
        "signals_scanned": 0,
        "actors_created": 0,
        "actors_found": 0,
        "connections_created": 0,
        "skipped_noise": 0,
        "errors": 0,
    }

    with engine.connect() as conn:
        # Fetch recent signals with actors
        rows = conn.execute(text("""
            SELECT id, signal_type, ticker, actor, direction, magnitude,
                   confidence, signal_date, data, created_at
            FROM signal_data
            WHERE actor IS NOT NULL AND actor != ''
              AND created_at >= NOW() - (:mins || ' minutes')::INTERVAL
            ORDER BY created_at DESC
            LIMIT :lim
        """), {"mins": since_minutes, "lim": batch_size}).fetchall()

        stats["signals_scanned"] = len(rows)
        log.info("Backlinker: scanning {n} signals from last {m} min",
                 n=len(rows), m=since_minutes)

        for row in rows:
            sig_id, sig_type, ticker, actor_name, direction, magnitude, \
                confidence, sig_date, sig_data, created_at = row

            # Filter noise
            if not is_real_actor(actor_name, sig_type):
                stats["skipped_noise"] += 1
                continue

            try:
                actor_id = _normalize_actor_id(actor_name)

                # 1. Find or create actor
                existing = conn.execute(text(
                    "SELECT id FROM actors WHERE id = :aid OR LOWER(name) = LOWER(:name) LIMIT 1"
                ), {"aid": actor_id, "name": actor_name}).fetchone()

                if existing:
                    actor_id = existing[0]
                    stats["actors_found"] += 1
                else:
                    category = ACTOR_CATEGORY_HINTS.get(sig_type, "unknown")
                    conn.execute(text("""
                        INSERT INTO actors (id, name, tier, category, influence_score,
                            trust_score, degree, source, credibility, data_sources, updated_at)
                        VALUES (:id, :name, 'individual', :cat, 0.3, 0.5, 0,
                            :source, 'inferred', :ds, NOW())
                        ON CONFLICT (id) DO NOTHING
                    """), {
                        "id": actor_id,
                        "name": actor_name.strip(),
                        "cat": category,
                        "source": f"signal_{sig_type}",
                        "ds": json.dumps([sig_type]),
                    })
                    stats["actors_created"] += 1

                # 2. Create connection: actor → ticker
                if ticker:
                    relationship = SIGNAL_TO_RELATIONSHIP.get(sig_type, "signal_linked")
                    evidence = {
                        "signal_id": sig_id,
                        "signal_type": sig_type,
                        "direction": direction,
                        "magnitude": float(magnitude) if magnitude else None,
                        "confidence": str(confidence) if confidence else None,
                        "date": str(sig_date) if sig_date else str(created_at)[:10],
                    }

                    conn.execute(text("""
                        INSERT INTO actor_connections
                            (actor_a, actor_b, relationship, strength, evidence, discovered_at)
                        VALUES (:a, :b, :rel, :strength, :evidence, NOW())
                        ON CONFLICT (actor_a, actor_b, relationship)
                        DO UPDATE SET
                            strength = GREATEST(actor_connections.strength, EXCLUDED.strength),
                            evidence = actor_connections.evidence || EXCLUDED.evidence,
                            discovered_at = NOW()
                    """), {
                        "a": actor_id,
                        "b": ticker,
                        "rel": relationship,
                        "strength": min(0.9, 0.3 + (float(magnitude) if magnitude else 0) * 0.1),
                        "evidence": json.dumps([evidence]),
                    })
                    stats["connections_created"] += 1

                # 3. Update actor data_sources to include this signal type
                conn.execute(text("""
                    UPDATE actors SET
                        data_sources = COALESCE(data_sources, '[]'::jsonb) || :new_src,
                        updated_at = NOW()
                    WHERE id = :aid
                      AND NOT (COALESCE(data_sources, '[]'::jsonb) @> :new_src)
                """), {
                    "aid": actor_id,
                    "new_src": json.dumps([sig_type]),
                })

            except Exception as exc:
                stats["errors"] += 1
                log.debug("Backlink error for signal {s}: {e}", s=sig_id, e=str(exc))

        conn.commit()

    log.info(
        "Backlinker complete: scanned={sc} created={cr} found={fo} "
        "connected={co} noise={no} errors={er}",
        sc=stats["signals_scanned"],
        cr=stats["actors_created"],
        fo=stats["actors_found"],
        co=stats["connections_created"],
        no=stats["skipped_noise"],
        er=stats["errors"],
    )
    return stats


def update_trust_from_signal_density(engine: Engine) -> dict[str, int]:
    """Update actor influence scores based on signal activity density.

    Actors who generate more signals across more tickers get higher influence.
    Actors with consistent directional signals get higher trust.
    """
    stats = {"actors_updated": 0}

    with engine.connect() as conn:
        # Find actors with signal activity and compute metrics
        rows = conn.execute(text("""
            SELECT actor, COUNT(*) as sig_count,
                   COUNT(DISTINCT ticker) as ticker_count,
                   SUM(CASE WHEN direction IN ('bullish', 'buy', 'long') THEN 1 ELSE 0 END) as bull,
                   SUM(CASE WHEN direction IN ('bearish', 'sell', 'short') THEN 1 ELSE 0 END) as bear
            FROM signal_data
            WHERE actor IS NOT NULL AND actor != ''
              AND created_at >= NOW() - INTERVAL '90 days'
            GROUP BY actor
            HAVING COUNT(*) >= 3
            ORDER BY COUNT(*) DESC
            LIMIT 2000
        """)).fetchall()

        for actor_name, sig_count, ticker_count, bull, bear in rows:
            aid = _normalize_actor_id(actor_name)

            # Influence: log-scaled signal density
            influence = min(0.95, 0.3 + 0.1 * min(6, (sig_count / 10.0)))

            # Trust: directional consistency (strong lean = higher trust)
            total = bull + bear
            if total > 0:
                consistency = max(bull, bear) / total
                trust = round(0.3 + consistency * 0.5, 3)
            else:
                trust = 0.5

            conn.execute(text("""
                UPDATE actors SET
                    influence_score = GREATEST(influence_score, :inf),
                    trust_score = :trust,
                    updated_at = NOW()
                WHERE id = :aid
            """), {"inf": round(influence, 3), "trust": trust, "aid": aid})
            stats["actors_updated"] += 1

        conn.commit()

    log.info("Trust/influence update: {u} actors updated", u=stats["actors_updated"])
    return stats


def run_backlinker(interval: int = 300, lookback_minutes: int = 60) -> None:
    """Main loop — run backlinker every `interval` seconds."""
    sys.path.insert(0, ".")
    from db import get_engine

    engine = get_engine()
    log.info("Signal backlinker starting — interval={i}s, lookback={m}min",
             i=interval, m=lookback_minutes)

    # First run: process ALL historical signals
    log.info("Backlinker: initial catch-up (all time)")
    backlink_signals(engine, batch_size=50000, since_minutes=999999)
    update_trust_from_signal_density(engine)

    while True:
        try:
            stats = backlink_signals(engine, since_minutes=lookback_minutes)
            if stats["signals_scanned"] > 0:
                update_trust_from_signal_density(engine)
        except Exception as exc:
            log.error("Backlinker cycle failed: {e}", e=str(exc))

        time.sleep(interval)


if __name__ == "__main__":
    log.remove()
    log.add(sys.stderr, level="INFO")
    run_backlinker()
