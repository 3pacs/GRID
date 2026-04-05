"""
ICIJ Actor Discovery — automatically discover and add new actors from ICIJ data.

When ICIJ entities are loaded, this module:
1. Identifies entities with many connections (high-degree nodes)
2. Cross-references with existing actors to avoid duplicates
3. Auto-creates new actor entries for significant offshore players
4. Follows relationship chains to discover hidden networks

The rule: if an entity has >= N connections, they're an actor worth tracking.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class DiscoveredActor:
    """A newly discovered actor from ICIJ data."""
    name: str
    node_id: int
    node_type: str            # entity, officer, intermediary
    connection_count: int
    jurisdictions: list[str]
    source_datasets: list[str]
    relationship_types: list[str]
    confidence: str           # confirmed, derived, inferred


def discover_actors(
    engine: Engine,
    min_connections: int = 5,
    limit: int = 1000,
) -> list[DiscoveredActor]:
    """Find high-connection entities in ICIJ data not yet in actor_network.

    Scans ICIJ relationships for entities with >= min_connections edges,
    filters out those already tracked, and returns new discoveries.

    Args:
        engine: SQLAlchemy engine.
        min_connections: Minimum relationship count to qualify.
        limit: Max actors to discover per run.

    Returns:
        List of DiscoveredActor objects.
    """
    # Get existing actor names
    existing = _get_existing_actor_names(engine)
    existing_lower = {n.lower() for n in existing}

    discoveries: list[DiscoveredActor] = []

    # Find high-degree officers (people controlling many entities)
    with engine.connect() as conn:
        officer_rows = conn.execute(text("""
            SELECT o.node_id, o.name, o.country_codes, o.source_dataset,
                   COUNT(r.id) AS rel_count,
                   array_agg(DISTINCT r.rel_type) AS rel_types,
                   array_agg(DISTINCT e.jurisdiction) FILTER (WHERE e.jurisdiction IS NOT NULL) AS jurisdictions
            FROM icij_officers o
            JOIN icij_relationships r ON o.node_id = r.from_node OR o.node_id = r.to_node
            LEFT JOIN icij_entities e ON (
                CASE WHEN r.from_node = o.node_id THEN r.to_node ELSE r.from_node END
            ) = e.node_id
            GROUP BY o.node_id, o.name, o.country_codes, o.source_dataset
            HAVING COUNT(r.id) >= :min_conn
            ORDER BY rel_count DESC
            LIMIT :lim
        """), {"min_conn": min_connections, "lim": limit}).fetchall()

    for row in officer_rows:
        name = row[1].strip()
        if not name or name.lower() in existing_lower:
            continue

        discoveries.append(DiscoveredActor(
            name=name,
            node_id=row[0],
            node_type="officer",
            connection_count=row[4],
            jurisdictions=row[6] or [],
            source_datasets=[row[3]] if row[3] else [],
            relationship_types=row[5] or [],
            confidence="derived",
        ))
        existing_lower.add(name.lower())

    # Find high-degree entities (shell companies with many officers)
    with engine.connect() as conn:
        entity_rows = conn.execute(text("""
            SELECT e.node_id, e.name, e.jurisdiction, e.source_dataset,
                   COUNT(r.id) AS rel_count,
                   array_agg(DISTINCT r.rel_type) AS rel_types
            FROM icij_entities e
            JOIN icij_relationships r ON e.node_id = r.from_node OR e.node_id = r.to_node
            GROUP BY e.node_id, e.name, e.jurisdiction, e.source_dataset
            HAVING COUNT(r.id) >= :min_conn
            ORDER BY rel_count DESC
            LIMIT :lim
        """), {"min_conn": min_connections, "lim": limit}).fetchall()

    for row in entity_rows:
        name = row[1].strip()
        if not name or name.lower() in existing_lower:
            continue

        discoveries.append(DiscoveredActor(
            name=name,
            node_id=row[0],
            node_type="entity",
            connection_count=row[4],
            jurisdictions=[row[2]] if row[2] else [],
            source_datasets=[row[3]] if row[3] else [],
            relationship_types=row[5] or [],
            confidence="derived",
        ))
        existing_lower.add(name.lower())

    log.info("ICIJ actor discovery: {n} new actors found (min_conn={m})",
             n=len(discoveries), m=min_connections)
    return discoveries


def auto_add_actors(
    engine: Engine,
    min_connections: int = 5,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Discover and automatically add new actors to the actors table.

    Args:
        engine: SQLAlchemy engine.
        min_connections: Minimum connections to qualify.
        limit: Max actors to add per run.

    Returns:
        List of added actor dicts.
    """
    discoveries = discover_actors(engine, min_connections=min_connections, limit=limit)
    added: list[dict[str, Any]] = []

    for actor in discoveries:
        try:
            with engine.begin() as conn:
                # Check if actors table exists and has the right schema
                result = conn.execute(text(
                    "INSERT INTO actors (name, actor_type, country, source, confidence, metadata) "
                    "VALUES (:name, :atype, :country, :source, :conf, :meta) "
                    "ON CONFLICT (name) DO NOTHING "
                    "RETURNING id"
                ), {
                    "name": actor.name,
                    "atype": "person" if actor.node_type == "officer" else "entity",
                    "country": actor.jurisdictions[0] if actor.jurisdictions else None,
                    "source": "icij_discovery",
                    "conf": actor.confidence,
                    "meta": str({
                        "icij_node_id": actor.node_id,
                        "icij_node_type": actor.node_type,
                        "connection_count": actor.connection_count,
                        "source_datasets": actor.source_datasets,
                        "relationship_types": actor.relationship_types,
                        "discovered_at": datetime.now(timezone.utc).isoformat(),
                    }),
                })
                new_row = result.fetchone()
                if new_row:
                    added.append({
                        "id": new_row[0],
                        "name": actor.name,
                        "connections": actor.connection_count,
                        "type": actor.node_type,
                    })
        except Exception as exc:
            # Table might not have the right schema — fall back to logging
            log.debug("Could not add actor {n}: {e}", n=actor.name, e=str(exc))

    log.info("ICIJ auto-add: {n} new actors added to network", n=len(added))
    return added


def follow_connections(
    engine: Engine,
    node_id: int,
    depth: int = 2,
    max_per_level: int = 50,
) -> list[dict[str, Any]]:
    """Follow relationship chains from a given ICIJ node.

    BFS traversal to discover connected entities up to N hops away.

    Args:
        engine: SQLAlchemy engine.
        node_id: Starting ICIJ node ID.
        depth: Maximum hops to follow.
        max_per_level: Max nodes to explore per level.

    Returns:
        List of discovered connection dicts.
    """
    visited: set[int] = {node_id}
    connections: list[dict[str, Any]] = []
    current_level = [node_id]

    for level in range(depth):
        next_level: list[int] = []

        with engine.connect() as conn:
            for nid in current_level[:max_per_level]:
                rows = conn.execute(text(
                    "SELECT r.from_node, r.to_node, r.rel_type, "
                    "COALESCE(e.name, o.name, i.name) AS target_name, "
                    "CASE "
                    "  WHEN e.node_id IS NOT NULL THEN 'entity' "
                    "  WHEN o.node_id IS NOT NULL THEN 'officer' "
                    "  WHEN i.node_id IS NOT NULL THEN 'intermediary' "
                    "  ELSE 'unknown' "
                    "END AS target_type "
                    "FROM icij_relationships r "
                    "LEFT JOIN icij_entities e ON (CASE WHEN r.from_node = :nid THEN r.to_node ELSE r.from_node END) = e.node_id "
                    "LEFT JOIN icij_officers o ON (CASE WHEN r.from_node = :nid THEN r.to_node ELSE r.from_node END) = o.node_id "
                    "LEFT JOIN icij_intermediaries i ON (CASE WHEN r.from_node = :nid THEN r.to_node ELSE r.from_node END) = i.node_id "
                    "WHERE r.from_node = :nid OR r.to_node = :nid "
                    "LIMIT :max_nodes"
                ), {"nid": nid, "max_nodes": max_per_level}).fetchall()

                for row in rows:
                    target_nid = row[1] if row[0] == nid else row[0]
                    if target_nid in visited:
                        continue

                    visited.add(target_nid)
                    next_level.append(target_nid)
                    connections.append({
                        "node_id": target_nid,
                        "name": row[3] or f"node:{target_nid}",
                        "node_type": row[4],
                        "rel_type": row[2],
                        "depth": level + 1,
                        "from_node": nid,
                    })

        current_level = next_level

    log.info("Follow connections from {nid}: {n} nodes found at depth {d}",
             nid=node_id, n=len(connections), d=depth)
    return connections


def _get_existing_actor_names(engine: Engine) -> set[str]:
    """Get all existing actor names from both actor_network and actors table."""
    names: set[str] = set()

    # From actor_network module
    try:
        from intelligence.actor_network import ACTORS
        names.update(a.get("name", "") for a in ACTORS if a.get("name"))
    except (ImportError, AttributeError):
        pass

    # From actors DB table
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT name FROM actors LIMIT 10000")).fetchall()
            names.update(r[0] for r in rows if r[0])
    except Exception:
        pass

    # From ICIJ actor matches
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT DISTINCT actor_name FROM icij_actor_matches LIMIT 10000"
            )).fetchall()
            names.update(r[0] for r in rows if r[0])
    except Exception:
        pass

    return names
