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


BACKLINKER_SERVICE = "signal_backlinker"
BACKLINKER_CURSOR = "signal_data_id"


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


def ensure_backlinker_state(engine: Engine) -> None:
    """Create the lightweight cursor table used by daemon-style workers."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS service_cursors (
                service_name TEXT NOT NULL,
                cursor_name TEXT NOT NULL,
                cursor_value BIGINT NOT NULL DEFAULT 0,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (service_name, cursor_name)
            )
        """))


def _get_cursor(engine: Engine) -> int | None:
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT cursor_value
            FROM service_cursors
            WHERE service_name = :service AND cursor_name = :cursor
        """), {"service": BACKLINKER_SERVICE, "cursor": BACKLINKER_CURSOR}).fetchone()
    return int(row[0]) if row else None


def _set_cursor(engine: Engine, signal_id: int) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO service_cursors (service_name, cursor_name, cursor_value, updated_at)
            VALUES (:service, :cursor, :value, NOW())
            ON CONFLICT (service_name, cursor_name)
            DO UPDATE SET cursor_value = EXCLUDED.cursor_value, updated_at = NOW()
        """), {
            "service": BACKLINKER_SERVICE,
            "cursor": BACKLINKER_CURSOR,
            "value": signal_id,
        })


def _initialize_cursor(engine: Engine, since_minutes: int) -> int:
    """Start from recent work only; never all-time catch up by default."""
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT COALESCE(
                (
                    SELECT MIN(id) - 1
                    FROM signal_data
                    WHERE actor IS NOT NULL AND actor != ''
                      AND created_at >= NOW() - (:mins || ' minutes')::INTERVAL
                ),
                (
                    SELECT COALESCE(MAX(id), 0)
                    FROM signal_data
                    WHERE actor IS NOT NULL AND actor != ''
                ),
                0
            )
        """), {"mins": since_minutes}).fetchone()
    cursor = int(row[0]) if row and row[0] is not None else 0
    _set_cursor(engine, cursor)
    log.info("Backlinker cursor initialized at signal_data.id={id}", id=cursor)
    return cursor


def backlink_signals(engine: Engine, batch_size: int = 500, since_minutes: int = 1440) -> dict[str, Any]:
    """Process recent signals and create actor graph backlinks.

    Args:
        engine: SQLAlchemy engine.
        batch_size: Max signals to process per run.
        since_minutes: Cursor bootstrap lookback and safety window.

    Returns:
        Stats dict with counts of actors created, connections made, etc.
    """
    ensure_backlinker_state(engine)
    cursor = _get_cursor(engine)
    if cursor is None:
        cursor = _initialize_cursor(engine, since_minutes)

    stats: dict[str, Any] = {
        "signals_scanned": 0,
        "actors_created": 0,
        "actors_found": 0,
        "connections_created": 0,
        "skipped_noise": 0,
        "errors": 0,
        "actors_touched": 0,
        "last_signal_id": cursor,
        "touched_actors": {},
    }
    touched_actors: dict[str, str] = {}

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, signal_type, ticker, actor, direction, magnitude,
                   confidence, signal_date, data, created_at
            FROM signal_data
            WHERE id > :cursor
              AND actor IS NOT NULL AND actor != ''
            ORDER BY id ASC
            LIMIT :lim
        """), {"cursor": cursor, "lim": batch_size}).fetchall()

    stats["signals_scanned"] = len(rows)
    if not rows:
        return stats

    log.info(
        "Backlinker: processing {n} signals after id={id}",
        n=len(rows),
        id=cursor,
    )

    last_consumed_id = cursor
    for row in rows:
        sig_id = int(row[0])
        result = _process_signal(engine, row)
        for key in (
            "actors_created",
            "actors_found",
            "connections_created",
            "skipped_noise",
            "errors",
        ):
            stats[key] += result.get(key, 0)
        actor_id = result.get("actor_id")
        actor_name = result.get("actor_name")
        if actor_id and actor_name:
            touched_actors[str(actor_id)] = str(actor_name)
        if result.get("errors", 0):
            log.warning(
                "Backlinker stopping at failed signal id={id}; cursor remains at {cursor}",
                id=sig_id,
                cursor=last_consumed_id,
            )
            break
        last_consumed_id = sig_id

    if last_consumed_id != cursor:
        _set_cursor(engine, last_consumed_id)
    stats["actors_touched"] = len(touched_actors)
    stats["last_signal_id"] = last_consumed_id
    stats["touched_actors"] = touched_actors

    log.info(
        "Backlinker complete: scanned={sc} created={cr} found={fo} "
        "connected={co} noise={no} touched={ta} errors={er} cursor={cu}",
        sc=stats["signals_scanned"],
        cr=stats["actors_created"],
        fo=stats["actors_found"],
        co=stats["connections_created"],
        no=stats["skipped_noise"],
        ta=stats["actors_touched"],
        er=stats["errors"],
        cu=stats["last_signal_id"],
    )
    return stats


def _process_signal(engine: Engine, row: Any) -> dict[str, Any]:
    """Backlink one signal using one short transaction."""
    stats: dict[str, Any] = {
        "actors_created": 0,
        "actors_found": 0,
        "connections_created": 0,
        "skipped_noise": 0,
        "errors": 0,
    }

    sig_id, sig_type, ticker, actor_name, direction, magnitude, \
        confidence, sig_date, _sig_data, created_at = row

    if not is_real_actor(actor_name, sig_type):
        stats["skipped_noise"] = 1
        return stats

    try:
        actor_id = _normalize_actor_id(actor_name)
        with engine.begin() as conn:
            existing = conn.execute(text(
                "SELECT id FROM actors WHERE id = :aid OR LOWER(name) = LOWER(:name) LIMIT 1"
            ), {"aid": actor_id, "name": actor_name}).fetchone()

            if existing:
                actor_id = existing[0]
                stats["actors_found"] = 1
            else:
                category = ACTOR_CATEGORY_HINTS.get(sig_type, "unknown")
                conn.execute(text("""
                    INSERT INTO actors (id, name, tier, category, influence_score,
                        trust_score, degree, source, credibility, data_sources, updated_at)
                    VALUES (:id, :name, 'individual', :cat, 0.3, 0.5, 0,
                        :source, 'inferred', CAST(:ds AS jsonb), NOW())
                    ON CONFLICT (id) DO NOTHING
                """), {
                    "id": actor_id,
                    "name": actor_name.strip(),
                    "cat": category,
                    "source": f"signal_{sig_type}",
                    "ds": json.dumps([sig_type]),
                })
                stats["actors_created"] = 1

            if ticker:
                relationship = SIGNAL_TO_RELATIONSHIP.get(sig_type, "signal_linked")
                evidence = {
                    "signal_id": int(sig_id),
                    "signal_type": sig_type,
                    "direction": direction,
                    "magnitude": float(magnitude) if magnitude else None,
                    "confidence": str(confidence) if confidence else None,
                    "date": str(sig_date) if sig_date else str(created_at)[:10],
                    "source": "signal_backlinker",
                }
                evidence_json = json.dumps([evidence], sort_keys=True)

                conn.execute(text("""
                    INSERT INTO actor_connections
                        (actor_a, actor_b, relationship, strength, evidence, discovered_at)
                    VALUES (:a, :b, :rel, :strength, CAST(:evidence AS jsonb), NOW())
                    ON CONFLICT (actor_a, actor_b, relationship)
                    DO UPDATE SET
                        strength = GREATEST(actor_connections.strength, EXCLUDED.strength),
                        evidence = CASE
                            WHEN COALESCE(actor_connections.evidence, '[]'::jsonb)
                                 @> CAST(:evidence AS jsonb)
                            THEN actor_connections.evidence
                            ELSE COALESCE(actor_connections.evidence, '[]'::jsonb)
                                 || EXCLUDED.evidence
                        END,
                        discovered_at = NOW()
                """), {
                    "a": actor_id,
                    "b": ticker,
                    "rel": relationship,
                    "strength": min(0.9, 0.3 + (float(magnitude) if magnitude else 0) * 0.1),
                    "evidence": evidence_json,
                })
                stats["connections_created"] = 1

            conn.execute(text("""
                UPDATE actors SET
                    data_sources = COALESCE(data_sources, '[]'::jsonb) || CAST(:new_src AS jsonb),
                    updated_at = NOW()
                WHERE id = :aid
                  AND NOT (COALESCE(data_sources, '[]'::jsonb) @> CAST(:new_src AS jsonb))
            """), {
                "aid": actor_id,
                "new_src": json.dumps([sig_type]),
            })

        stats["actor_id"] = actor_id
        stats["actor_name"] = actor_name
        return stats
    except Exception as exc:
        stats["errors"] = 1
        log.debug("Backlink error for signal {s}: {e}", s=sig_id, e=str(exc))
        return stats


def update_trust_from_signal_density(
    engine: Engine,
    actors: dict[str, str] | None = None,
    *,
    days: int = 90,
    limit: int = 500,
) -> dict[str, int]:
    """Update actor influence scores based on signal activity density.

    Actors who generate more signals across more tickers get higher influence.
    Actors with consistent directional signals get higher trust.
    """
    stats = {"actors_updated": 0}
    if not actors:
        return stats

    with engine.begin() as conn:
        for aid, actor_name in list(actors.items())[:limit]:
            row = conn.execute(text("""
                SELECT COUNT(*) as sig_count,
                       COUNT(DISTINCT ticker) as ticker_count,
                       SUM(CASE WHEN direction IN ('bullish', 'buy', 'long') THEN 1 ELSE 0 END) as bull,
                       SUM(CASE WHEN direction IN ('bearish', 'sell', 'short') THEN 1 ELSE 0 END) as bear
                FROM signal_data
                WHERE actor = :actor
                  AND created_at >= NOW() - (:days || ' days')::INTERVAL
            """), {"actor": actor_name, "days": days}).fetchone()
            if not row or int(row[0] or 0) < 3:
                continue

            sig_count = int(row[0] or 0)
            bull = int(row[2] or 0)
            bear = int(row[3] or 0)

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

    log.info("Trust/influence update: {u} actors updated", u=stats["actors_updated"])
    return stats


def run_backlinker(
    interval: int = 300,
    lookback_minutes: int = 1440,
    batch_size: int = 500,
    once: bool = False,
    trust_every: int = 12,
) -> None:
    """Main loop — run backlinker every `interval` seconds."""
    sys.path.insert(0, ".")
    from db import get_engine

    engine = get_engine()
    ensure_backlinker_state(engine)
    log.info(
        "Signal backlinker starting — interval={i}s, lookback={m}min, batch={b}, once={once}",
        i=interval,
        m=lookback_minutes,
        b=batch_size,
        once=once,
    )

    cycle = 0
    while True:
        cycle += 1
        try:
            stats = backlink_signals(
                engine,
                batch_size=batch_size,
                since_minutes=lookback_minutes,
            )
            should_update_trust = once or (trust_every > 0 and cycle % trust_every == 0)
            if stats["signals_scanned"] > 0 and should_update_trust:
                update_trust_from_signal_density(
                    engine,
                    stats.get("touched_actors", {}),
                )
        except Exception as exc:
            log.error("Backlinker cycle failed: {e}", e=str(exc))

        if once:
            break
        time.sleep(interval)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Backlink signal_data actors into actor_connections.")
    parser.add_argument("--interval", type=int, default=300, help="Seconds between daemon cycles.")
    parser.add_argument("--lookback-minutes", type=int, default=1440, help="Cursor bootstrap/safety window.")
    parser.add_argument("--batch-size", type=int, default=500, help="Maximum signals per cycle.")
    parser.add_argument("--once", action="store_true", help="Run one bounded cycle and exit.")
    parser.add_argument("--trust-every", type=int, default=12, help="Trust update cadence in cycles; 0 disables.")
    args = parser.parse_args()

    log.remove()
    log.add(sys.stderr, level="INFO")
    run_backlinker(
        interval=args.interval,
        lookback_minutes=args.lookback_minutes,
        batch_size=args.batch_size,
        once=args.once,
        trust_every=args.trust_every,
    )
