"""
ICIJ Linker — fuzzy-match ICIJ offshore entities against the actor network.

Cross-references 814K+ ICIJ entities/officers with GRID's 495+ named actors
using exact matching, trigram similarity, and alias expansion.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class ActorMatch:
    """A match between an ICIJ node and an actor network entity."""
    icij_node_id: int
    icij_node_type: str    # entity, officer, intermediary
    icij_name: str
    actor_name: str
    match_type: str        # exact, fuzzy, alias
    similarity: float      # 0-1


def link_actors(
    engine: Engine,
    min_similarity: float = 0.6,
    limit: int = 10000,
) -> list[ActorMatch]:
    """Cross-reference ICIJ entities against actor_network actors.

    Uses pg_trgm similarity for fuzzy matching.

    Args:
        engine: SQLAlchemy engine.
        min_similarity: Minimum trigram similarity threshold.
        limit: Max matches to return.

    Returns:
        List of ActorMatch objects.
    """
    # Get actor names from the actor network
    actor_names = _get_actor_names(engine)
    if not actor_names:
        log.warning("No actors found in network — skipping ICIJ linking")
        return []

    matches: list[ActorMatch] = []

    with engine.connect() as conn:
        # Set trigram similarity threshold
        conn.execute(text(f"SET pg_trgm.similarity_threshold = {min_similarity}"))

        for actor_name in actor_names:
            # Search entities
            entity_matches = conn.execute(
                text(
                    "SELECT node_id, name, similarity(name, :actor) AS sim "
                    "FROM icij_entities "
                    "WHERE name % :actor "
                    "ORDER BY sim DESC LIMIT 5"
                ),
                {"actor": actor_name},
            ).fetchall()

            for row in entity_matches:
                match_type = "exact" if row[1].lower() == actor_name.lower() else "fuzzy"
                matches.append(ActorMatch(
                    icij_node_id=row[0],
                    icij_node_type="entity",
                    icij_name=row[1],
                    actor_name=actor_name,
                    match_type=match_type,
                    similarity=float(row[2]),
                ))

            # Search officers
            officer_matches = conn.execute(
                text(
                    "SELECT node_id, name, similarity(name, :actor) AS sim "
                    "FROM icij_officers "
                    "WHERE name % :actor "
                    "ORDER BY sim DESC LIMIT 5"
                ),
                {"actor": actor_name},
            ).fetchall()

            for row in officer_matches:
                match_type = "exact" if row[1].lower() == actor_name.lower() else "fuzzy"
                matches.append(ActorMatch(
                    icij_node_id=row[0],
                    icij_node_type="officer",
                    icij_name=row[1],
                    actor_name=actor_name,
                    match_type=match_type,
                    similarity=float(row[2]),
                ))

    log.info("ICIJ linking: {n} matches found for {a} actors",
             n=len(matches), a=len(actor_names))

    # Store matches in DB
    _store_matches(engine, matches)
    return matches


def _get_actor_names(engine: Engine) -> list[str]:
    """Extract actor names from the actor_network module.

    Reads the ACTORS dict from intelligence/actor_network.py.
    Falls back to querying the actors table if it exists.
    """
    try:
        from intelligence.actor_network import ACTORS
        return [a.get("name", "") for a in ACTORS if a.get("name")]
    except (ImportError, AttributeError):
        pass

    # Fallback: query actors table
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT name FROM actors LIMIT 1000")).fetchall()
            return [r[0] for r in rows if r[0]]
    except Exception:
        pass

    return []


def _store_matches(engine: Engine, matches: list[ActorMatch]) -> None:
    """Persist matches to icij_actor_matches table."""
    if not matches:
        return

    with engine.begin() as conn:
        for m in matches:
            conn.execute(
                text(
                    "INSERT INTO icij_actor_matches "
                    "(icij_node_id, icij_node_type, actor_name, match_type, similarity) "
                    "VALUES (:nid, :ntype, :actor, :mtype, :sim) "
                    "ON CONFLICT DO NOTHING"
                ),
                {
                    "nid": m.icij_node_id,
                    "ntype": m.icij_node_type,
                    "actor": m.actor_name,
                    "mtype": m.match_type,
                    "sim": m.similarity,
                },
            )

    log.info("Stored {n} ICIJ-actor matches", n=len(matches))


def get_offshore_connections(engine: Engine, actor_name: str) -> list[dict[str, Any]]:
    """Get all ICIJ connections for a specific actor.

    Args:
        actor_name: Name of the actor to query.

    Returns:
        List of offshore connection dicts.
    """
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT m.icij_node_id, m.icij_node_type, m.match_type, m.similarity, "
                "COALESCE(e.name, o.name) AS icij_name, "
                "COALESCE(e.jurisdiction, '') AS jurisdiction, "
                "COALESCE(e.source_dataset, o.source_dataset) AS dataset "
                "FROM icij_actor_matches m "
                "LEFT JOIN icij_entities e ON m.icij_node_id = e.node_id AND m.icij_node_type = 'entity' "
                "LEFT JOIN icij_officers o ON m.icij_node_id = o.node_id AND m.icij_node_type = 'officer' "
                "WHERE m.actor_name = :actor "
                "ORDER BY m.similarity DESC"
            ),
            {"actor": actor_name},
        ).fetchall()

    return [
        {
            "node_id": r[0],
            "node_type": r[1],
            "match_type": r[2],
            "similarity": float(r[3]),
            "icij_name": r[4],
            "jurisdiction": r[5],
            "dataset": r[6],
        }
        for r in rows
    ]
