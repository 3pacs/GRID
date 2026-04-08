"""Actor Signal Bridge — injects actor intelligence into the prediction pipeline.

This module is the missing link between the actor graph and the analytical
engines. It provides functions that the oracle, trust scorer, and causation
engine call to get actor-aware context.

Without this, the prediction says "whale bought NVDA calls" but doesn't know
it was Pelosi, who has a 73% hit rate on tech trades, sits on the oversight
committee, and just visited Taiwan last month.

Functions:
    get_actor_signals_for_ticker — returns actor activity for a ticker
    get_actor_trust_weights — returns trust-weighted actor signals
    get_actor_context — returns who moved what and why (for causation)
    enrich_signal_with_actor — adds actor context to a raw signal
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


def get_actor_signals_for_ticker(
    engine: Engine,
    ticker: str,
    days: int = 30,
) -> list[dict[str, Any]]:
    """Get all actor-attributed signals for a ticker in the last N days.

    Returns signals enriched with actor trust_score, influence_score,
    and category from the actors table. This is what the oracle should
    call to know WHO is trading a ticker, not just WHAT is happening.
    """
    cutoff = date.today() - timedelta(days=days)

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT sd.signal_type, sd.actor, sd.direction, sd.magnitude,
                   sd.confidence, sd.signal_date, sd.data,
                   a.trust_score, a.influence_score, a.category, a.tier
            FROM signal_data sd
            LEFT JOIN actors a ON LOWER(sd.actor) = LOWER(a.name)
                               OR a.id = LOWER(REPLACE(sd.actor, ' ', '_'))
            WHERE sd.ticker = :ticker
              AND sd.actor IS NOT NULL AND sd.actor != ''
              AND sd.signal_date >= :cutoff
            ORDER BY COALESCE(a.influence_score, 0) DESC, sd.signal_date DESC
            LIMIT 100
        """), {"ticker": ticker, "cutoff": cutoff}).fetchall()

    signals = []
    for r in rows:
        signals.append({
            "signal_type": r[0],
            "actor": r[1],
            "direction": r[2],
            "magnitude": float(r[3]) if r[3] else 0.0,
            "confidence": str(r[4]) if r[4] else "unknown",
            "signal_date": str(r[5]),
            "data": r[6] if r[6] else {},
            "actor_trust": float(r[7]) if r[7] else 0.5,
            "actor_influence": float(r[8]) if r[8] else 0.3,
            "actor_category": r[9] or "unknown",
            "actor_tier": r[10] or "unknown",
        })

    return signals


def get_actor_trust_weights(
    engine: Engine,
    ticker: str,
    days: int = 90,
) -> dict[str, float]:
    """Get actor-weighted trust scores for a ticker.

    Returns a dict of source_module → trust_weight that the oracle's
    signal_aggregator can use. Actors with higher trust_score and
    influence_score get more weight.

    This replaces the flat trust_scores dict with one that knows
    Pelosi's congressional trades are more predictive than random_whale.
    """
    cutoff = date.today() - timedelta(days=days)

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT sd.signal_type, sd.actor,
                   COALESCE(a.trust_score, 0.5) as trust,
                   COALESCE(a.influence_score, 0.3) as influence
            FROM signal_data sd
            LEFT JOIN actors a ON LOWER(sd.actor) = LOWER(a.name)
                               OR a.id = LOWER(REPLACE(sd.actor, ' ', '_'))
            WHERE sd.ticker = :ticker
              AND sd.actor IS NOT NULL AND sd.actor != ''
              AND sd.signal_date >= :cutoff
        """), {"ticker": ticker, "cutoff": cutoff}).fetchall()

    # Weight = trust × influence for each signal type
    weights: dict[str, list[float]] = {}
    for sig_type, actor, trust, influence in rows:
        key = sig_type
        if key not in weights:
            weights[key] = []
        weights[key].append(float(trust) * float(influence))

    # Average per signal type
    return {k: sum(v) / len(v) for k, v in weights.items() if v}


def get_actor_context_for_causation(
    engine: Engine,
    ticker: str,
    days: int = 14,
) -> list[dict[str, Any]]:
    """Get the causal actor chain for a ticker move.

    Returns actors who traded/influenced this ticker recently,
    ordered by influence × recency. This is what the causation engine
    should call to answer "who caused this move?"

    Each entry includes the actor's connections to other entities
    (board seats, lobbying targets, political affiliations).
    """
    cutoff = date.today() - timedelta(days=days)

    with engine.connect() as conn:
        # Actor signals for this ticker
        actors = conn.execute(text("""
            SELECT DISTINCT sd.actor,
                   sd.signal_type, sd.direction, sd.signal_date,
                   a.trust_score, a.influence_score, a.category,
                   a.title, a.tier, a.id as actor_id
            FROM signal_data sd
            LEFT JOIN actors a ON LOWER(sd.actor) = LOWER(a.name)
                               OR a.id = LOWER(REPLACE(sd.actor, ' ', '_'))
            WHERE sd.ticker = :ticker
              AND sd.actor IS NOT NULL AND sd.actor != ''
              AND sd.signal_date >= :cutoff
            ORDER BY COALESCE(a.influence_score, 0) DESC
            LIMIT 20
        """), {"ticker": ticker, "cutoff": cutoff}).fetchall()

        results = []
        for a in actors:
            actor_id = a[9]
            connections = []

            # Get this actor's other connections (what else are they involved in?)
            if actor_id:
                conns = conn.execute(text("""
                    SELECT actor_b, relationship, strength
                    FROM actor_connections
                    WHERE actor_a = :aid
                    ORDER BY strength DESC
                    LIMIT 10
                """), {"aid": actor_id}).fetchall()
                connections = [
                    {"target": c[0], "relationship": c[1], "strength": float(c[2]) if c[2] else 0}
                    for c in conns
                ]

            results.append({
                "actor": a[0],
                "signal_type": a[1],
                "direction": a[2],
                "signal_date": str(a[3]),
                "trust_score": float(a[4]) if a[4] else 0.5,
                "influence_score": float(a[5]) if a[5] else 0.3,
                "category": a[6] or "unknown",
                "title": a[7] or "",
                "tier": a[8] or "unknown",
                "other_connections": connections,
            })

    return results


def enrich_signals_with_actors(
    engine: Engine,
    signals: list[dict[str, Any]],
    ticker: str,
) -> list[dict[str, Any]]:
    """Enrich a list of signals with actor context.

    Called by the oracle engine before aggregation. Adds actor_trust
    and actor_influence fields to each signal that has an actor.
    Signals without actors pass through unchanged.
    """
    actor_signals = get_actor_signals_for_ticker(engine, ticker, days=30)

    # Build lookup: (signal_type, direction) → best actor info
    actor_lookup: dict[str, dict] = {}
    for asig in actor_signals:
        key = f"{asig['signal_type']}:{asig['direction']}"
        if key not in actor_lookup or asig["actor_influence"] > actor_lookup[key].get("actor_influence", 0):
            actor_lookup[key] = asig

    enriched = []
    for sig in signals:
        sig_copy = {**sig}
        key = f"{sig.get('source_module', '')}:{sig.get('direction', '')}"
        actor_info = actor_lookup.get(key)
        if actor_info:
            sig_copy["actor_name"] = actor_info["actor"]
            sig_copy["actor_trust"] = actor_info["actor_trust"]
            sig_copy["actor_influence"] = actor_info["actor_influence"]
            sig_copy["actor_category"] = actor_info["actor_category"]
            # Boost trust_score by actor credibility
            existing_trust = float(sig_copy.get("trust_score", 0.5))
            sig_copy["trust_score"] = min(0.95, existing_trust * (0.5 + actor_info["actor_trust"]))
        enriched.append(sig_copy)

    return enriched


def sync_actor_trust_to_signal_sources(engine: Engine) -> int:
    """Push actor trust scores into signal_sources for the trust scorer.

    The trust_scorer reads from signal_sources.trust_score but never
    checks the actors table. This bridges the gap by copying
    actor trust scores back into signal_sources rows.
    """
    with engine.connect() as conn:
        result = conn.execute(text("""
            UPDATE signal_sources ss
            SET trust_score = a.trust_score
            FROM actors a
            WHERE LOWER(ss.source_id) = LOWER(a.name)
              AND a.trust_score IS NOT NULL
              AND a.trust_score != COALESCE(ss.trust_score, 0.5)
        """))
        updated = result.rowcount
        conn.commit()

    if updated > 0:
        log.info("Synced {n} actor trust scores to signal_sources", n=updated)
    return updated
