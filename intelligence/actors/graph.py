"""
GRID Intelligence — Actor graph construction and network traversal.

Contains:
    _resolve_dynamic_insiders   — query trust scorer for top insiders
    _compute_influence_propagation — propagate influence through graph
    build_actor_graph           — main entry point for D3 visualization
    find_connected_actions      — correlated actions within an actor's network
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.actors.db import (
    _ensure_tables,
    _load_actors_from_db,
    _seed_known_actors,
)
from intelligence.actors.models import Actor


def _resolve_dynamic_insiders(engine: Engine) -> list[dict]:
    """Query trust_scorer data to find the top insiders by accuracy.

    Returns up to 10 highest-trust insiders as dicts suitable for
    merging into the actor graph.
    """
    insiders: list[dict] = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT source_id, trust_score,
                       COUNT(*) AS total_signals
                FROM signal_sources
                WHERE source_type = 'insider'
                  AND trust_score IS NOT NULL
                GROUP BY source_id, trust_score
                HAVING COUNT(*) >= 3
                ORDER BY trust_score DESC
                LIMIT 10
            """)).fetchall()
            for i, r in enumerate(rows):
                insiders.append({
                    "id": f"insider_dynamic_{i}",
                    "name": r[0],
                    "tier": "individual",
                    "category": "insider",
                    "title": f"Corporate Insider (trust={float(r[1]):.2f}, signals={r[2]})",
                    "influence_score": min(0.4 + float(r[1]) * 0.4, 0.80),
                    "trust_score": float(r[1]),
                    "data_sources": ["form4", "trust_scorer"],
                    "credibility": "hard_data",
                    "motivation_model": "informed",
                })
    except Exception as exc:
        log.debug("Could not resolve dynamic insiders: {e}", e=str(exc))
    return insiders


def _compute_influence_propagation(
    actors: dict[str, Actor],
) -> dict[str, float]:
    """Propagate influence through the actor graph.

    If actor A controls fund B which holds stock C, A's influence
    propagates to C with decay.

    Returns:
        Dict of actor_id -> propagated_influence_score.
    """
    propagated: dict[str, float] = {}
    decay = 0.5  # each hop reduces influence by half

    for actor_id, actor in actors.items():
        propagated[actor_id] = actor.influence_score

        # Walk connections
        for conn in actor.connections:
            target = conn.get("actor_id", conn.get("actor", ""))
            strength = float(conn.get("strength", 0.5))
            if target in actors:
                current = propagated.get(target, actors[target].influence_score)
                contribution = actor.influence_score * strength * decay
                propagated[target] = min(current + contribution, 1.0)

    return propagated


def build_actor_graph(engine: Engine) -> dict:
    """Build the complete actor network graph.

    Loads all actors + their connections, computes influence propagation,
    and returns a graph structure suitable for D3 force-directed visualization.

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        dict with keys: nodes, links, metadata.
        - nodes: list of dicts with id, label, tier, category, influence, size
        - links: list of dicts with source, target, relationship, strength
        - metadata: summary statistics
    """
    _ensure_tables(engine)

    # Load base actors from DB (fall back to _KNOWN_ACTORS if empty)
    actors = _load_actors_from_db(engine)
    if not actors:
        _seed_known_actors(engine)
        actors = _load_actors_from_db(engine)

    # Merge dynamic insiders
    dynamic_insiders = _resolve_dynamic_insiders(engine)
    for ins in dynamic_insiders:
        aid = ins["id"]
        if aid not in actors:
            actors[aid] = Actor(
                id=aid,
                name=ins["name"],
                tier=ins["tier"],
                category=ins["category"],
                title=ins["title"],
                influence_score=ins["influence_score"],
                trust_score=ins.get("trust_score", 0.5),
                data_sources=ins.get("data_sources", []),
                credibility=ins.get("credibility", "hard_data"),
                motivation_model=ins.get("motivation_model", "informed"),
            )

    # Compute propagated influence
    propagated = _compute_influence_propagation(actors)

    # Build nodes
    nodes: list[dict] = []
    for actor_id, actor in actors.items():
        effective_influence = propagated.get(actor_id, actor.influence_score)
        nodes.append({
            "id": actor_id,
            "label": actor.name,
            "tier": actor.tier,
            "category": actor.category,
            "title": actor.title,
            "influence": round(effective_influence, 3),
            "trust_score": round(actor.trust_score, 3),
            "net_worth": actor.net_worth_estimate,
            "aum": actor.aum,
            "motivation": actor.motivation_model,
            "credibility": actor.credibility,
            # D3 sizing: scale radius by influence
            "size": max(4, int(effective_influence * 30)),
        })

    # Build links from connections
    links: list[dict] = []
    seen_links: set[tuple[str, str]] = set()
    for actor_id, actor in actors.items():
        for conn_info in actor.connections:
            target = conn_info.get("actor_id", conn_info.get("actor", ""))
            if target in actors and (actor_id, target) not in seen_links:
                links.append({
                    "source": actor_id,
                    "target": target,
                    "relationship": conn_info.get("relationship", "connected"),
                    "strength": float(conn_info.get("strength", 0.5)),
                })
                seen_links.add((actor_id, target))
                seen_links.add((target, actor_id))

    # Tier breakdown
    tier_counts: dict[str, int] = defaultdict(int)
    for actor in actors.values():
        tier_counts[actor.tier] += 1

    metadata = {
        "total_actors": len(actors),
        "total_links": len(links),
        "tier_breakdown": dict(tier_counts),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    log.info(
        "Actor graph built: {n} nodes, {l} links",
        n=len(nodes), l=len(links),
    )

    return {"nodes": nodes, "links": links, "metadata": metadata}


def find_connected_actions(
    engine: Engine,
    actor_id: str,
) -> list[dict]:
    """Find correlated actions within an actor's network.

    When actor X acts, who else in their network also acted recently?
    E.g., "3 board members of Company Y all sold within 2 weeks."

    Parameters:
        engine: SQLAlchemy engine.
        actor_id: The actor to investigate.

    Returns:
        List of dicts describing connected actions, sorted by recency.
    """
    _ensure_tables(engine)
    results: list[dict] = []

    # Load the target actor
    actors = _load_actors_from_db(engine)
    target = actors.get(actor_id)
    if not target:
        log.warning("Actor {a} not found", a=actor_id)
        return results

    # Get the target's recent actions from signal_sources
    cutoff = date.today() - timedelta(days=30)
    target_tickers: set[str] = set()

    try:
        with engine.connect() as conn:
            # Find tickers this actor recently acted on
            rows = conn.execute(text("""
                SELECT DISTINCT ticker, signal_type, signal_date
                FROM signal_sources
                WHERE source_id = :sid
                  AND signal_date >= :cutoff
                ORDER BY signal_date DESC
                LIMIT 20
            """), {"sid": target.name, "cutoff": cutoff}).fetchall()

            for r in rows:
                target_tickers.add(str(r[0]))

            if not target_tickers:
                return results

            # Find other actors who acted on the same tickers in a 14-day window
            ticker_list = list(target_tickers)

            related_rows = conn.execute(text("""
                SELECT source_type, source_id, ticker, signal_type,
                       signal_date, trust_score
                FROM signal_sources
                WHERE ticker = ANY(:tickers)
                  AND signal_date >= :cutoff
                  AND source_id != :exclude
                ORDER BY signal_date DESC
                LIMIT 100
            """), {
                "tickers": ticker_list,
                "cutoff": cutoff,
                "exclude": target.name,
            }).fetchall()

            # Group by ticker to find clusters
            by_ticker: dict[str, list[dict]] = defaultdict(list)
            for r in related_rows:
                by_ticker[str(r[2])].append({
                    "source_type": r[0],
                    "source_id": r[1],
                    "ticker": r[2],
                    "direction": r[3],
                    "signal_date": str(r[4]),
                    "trust_score": float(r[5]) if r[5] else 0.5,
                })

            for ticker, actions in by_ticker.items():
                if len(actions) >= 2:
                    # Multiple actors acting on the same ticker = connected action
                    directions = {a["direction"] for a in actions}
                    alignment = "aligned" if len(directions) == 1 else "mixed"
                    results.append({
                        "ticker": ticker,
                        "primary_actor": actor_id,
                        "connected_actors": [
                            {
                                "source_type": a["source_type"],
                                "name": a["source_id"],
                                "direction": a["direction"],
                                "date": a["signal_date"],
                                "trust": a["trust_score"],
                            }
                            for a in actions
                        ],
                        "total_actors": len(actions) + 1,  # +1 for primary
                        "alignment": alignment,
                        "dominant_direction": actions[0]["direction"],
                        "conviction": (
                            "high"
                            if len(actions) >= 3 and alignment == "aligned"
                            else "moderate"
                        ),
                    })
    except Exception as exc:
        log.warning("Connected action search failed: {e}", e=str(exc))

    # Sort by number of connected actors (most coordinated first)
    results.sort(key=lambda x: x.get("total_actors", 0), reverse=True)
    log.info(
        "Found {n} connected action clusters for {a}",
        n=len(results), a=actor_id,
    )
    return results
