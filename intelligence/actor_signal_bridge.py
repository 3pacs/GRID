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
        # Two-step: get signals first, then batch-lookup actors
        rows = conn.execute(text("""
            SELECT signal_type, actor, direction, magnitude,
                   confidence, signal_date, data
            FROM signal_data
            WHERE ticker = :ticker
              AND actor IS NOT NULL AND actor != ''
              AND signal_date >= :cutoff
            ORDER BY signal_date DESC
            LIMIT 100
        """), {"ticker": ticker, "cutoff": cutoff}).fetchall()

        # Batch lookup actors by normalized ID
        actor_names = list({r[1] for r in rows if r[1]})
        actor_lookup: dict[str, dict] = {}
        if actor_names:
            actor_ids = [n.strip().lower().replace(" ", "_").replace(".", "").replace(",", "") for n in actor_names]
            actor_rows = conn.execute(text(
                "SELECT id, trust_score, influence_score, category, tier "
                "FROM actors WHERE id = ANY(:ids)"
            ), {"ids": actor_ids}).fetchall()
            for ar in actor_rows:
                actor_lookup[ar[0]] = {
                    "trust": float(ar[1]) if ar[1] else 0.5,
                    "influence": float(ar[2]) if ar[2] else 0.3,
                    "category": ar[3] or "unknown",
                    "tier": ar[4] or "unknown",
                }

    signals = []
    for r in rows:
        actor_name = r[1]
        aid = actor_name.strip().lower().replace(" ", "_").replace(".", "").replace(",", "")
        ainfo = actor_lookup.get(aid, {})
        signals.append({
            "signal_type": r[0],
            "actor": actor_name,
            "direction": r[2],
            "magnitude": float(r[3]) if r[3] else 0.0,
            "confidence": str(r[4]) if r[4] else "unknown",
            "signal_date": str(r[5]),
            "data": r[6] if r[6] else {},
            "actor_trust": ainfo.get("trust", 0.5),
            "actor_influence": ainfo.get("influence", 0.3),
            "actor_category": ainfo.get("category", "unknown"),
            "actor_tier": ainfo.get("tier", "unknown"),
        })

    # Sort by influence
    signals.sort(key=lambda s: s["actor_influence"], reverse=True)
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
            SELECT signal_type, actor
            FROM signal_data
            WHERE ticker = :ticker
              AND actor IS NOT NULL AND actor != ''
              AND signal_date >= :cutoff
        """), {"ticker": ticker, "cutoff": cutoff}).fetchall()

        # Batch actor lookup
        actor_names = list({r[1] for r in rows if r[1]})
        actor_lookup: dict[str, tuple] = {}
        if actor_names:
            aids = [n.strip().lower().replace(" ", "_").replace(".", "").replace(",", "") for n in actor_names]
            arows = conn.execute(text(
                "SELECT id, trust_score, influence_score FROM actors WHERE id = ANY(:ids)"
            ), {"ids": aids}).fetchall()
            for ar in arows:
                actor_lookup[ar[0]] = (float(ar[1]) if ar[1] else 0.5, float(ar[2]) if ar[2] else 0.3)

    # Weight = trust × influence for each signal type
    weights: dict[str, list[float]] = {}
    for sig_type, actor_name in rows:
        aid = actor_name.strip().lower().replace(" ", "_").replace(".", "").replace(",", "")
        trust, influence = actor_lookup.get(aid, (0.5, 0.3))
        key = sig_type
        if key not in weights:
            weights[key] = []
        weights[key].append(trust * influence)

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
        # Step 1: Get signals
        sig_rows = conn.execute(text("""
            SELECT DISTINCT actor, signal_type, direction, signal_date
            FROM signal_data
            WHERE ticker = :ticker
              AND actor IS NOT NULL AND actor != ''
              AND signal_date >= :cutoff
            ORDER BY signal_date DESC
            LIMIT 20
        """), {"ticker": ticker, "cutoff": cutoff}).fetchall()

        # Step 2: Batch actor lookup
        actor_names = list({r[0] for r in sig_rows})
        actor_ids = [n.strip().lower().replace(" ", "_").replace(".", "").replace(",", "") for n in actor_names]
        actor_lookup: dict[str, dict] = {}
        if actor_ids:
            arows = conn.execute(text(
                "SELECT id, trust_score, influence_score, category, title, tier "
                "FROM actors WHERE id = ANY(:ids)"
            ), {"ids": actor_ids}).fetchall()
            for ar in arows:
                actor_lookup[ar[0]] = {
                    "trust": float(ar[1]) if ar[1] else 0.5,
                    "influence": float(ar[2]) if ar[2] else 0.3,
                    "category": ar[3] or "unknown",
                    "title": ar[4] or "",
                    "tier": ar[5] or "unknown",
                }

        # Step 3: Batch connection lookup for all actors
        conn_lookup: dict[str, list] = {}
        if actor_ids:
            crows = conn.execute(text(
                "SELECT actor_a, actor_b, relationship, strength "
                "FROM actor_connections WHERE actor_a = ANY(:ids) "
                "ORDER BY strength DESC"
            ), {"ids": actor_ids}).fetchall()
            for c in crows:
                if c[0] not in conn_lookup:
                    conn_lookup[c[0]] = []
                if len(conn_lookup[c[0]]) < 10:
                    conn_lookup[c[0]].append({
                        "target": c[1], "relationship": c[2],
                        "strength": float(c[3]) if c[3] else 0,
                    })

        results = []
        for r in sig_rows:
            actor_name = r[0]
            aid = actor_name.strip().lower().replace(" ", "_").replace(".", "").replace(",", "")
            ainfo = actor_lookup.get(aid, {})
            results.append({
                "actor": actor_name,
                "signal_type": r[1],
                "direction": r[2],
                "signal_date": str(r[3]),
                "trust_score": ainfo.get("trust", 0.5),
                "influence_score": ainfo.get("influence", 0.3),
                "category": ainfo.get("category", "unknown"),
                "title": ainfo.get("title", ""),
                "tier": ainfo.get("tier", "unknown"),
                "other_connections": conn_lookup.get(aid, []),
            })

        results.sort(key=lambda x: x["influence_score"], reverse=True)

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
