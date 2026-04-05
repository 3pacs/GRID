"""
Universal Actor Ingestion — auto-discover and log actors from ANY data source.

Every puller, intelligence module, and data integration should call
`ingest_actor()` when it encounters a named entity. This builds the
actor network organically from all data flowing through the system.

Usage from any module:
    from intelligence.actor_ingest import ingest_actor, ingest_actors_batch

    # Single actor
    ingest_actor(engine, "Warren Buffett", "person", "opensecrets",
                 metadata={"role": "CEO", "org": "Berkshire Hathaway"})

    # Batch from a data pull
    actors = [("BlackRock", "company"), ("Larry Fink", "person")]
    ingest_actors_batch(engine, actors, source="littlesis")
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# In-memory dedup cache to avoid hammering the DB
_seen_actors: set[str] = set()


def ingest_actor(
    engine: Engine,
    name: str,
    actor_type: str = "unknown",
    source: str = "auto",
    country: str | None = None,
    confidence: str = "derived",
    metadata: dict[str, Any] | None = None,
) -> bool:
    """Ingest a single actor into the actor network.

    Deduplicates by name (case-insensitive). If the actor already exists,
    updates the metadata with new source info.

    Args:
        engine: SQLAlchemy engine.
        name: Actor name.
        actor_type: person, company, government, fund, entity, unknown.
        source: Data source that discovered this actor.
        country: Country code or name.
        confidence: confirmed, derived, estimated, inferred.
        metadata: Additional metadata dict.

    Returns:
        True if new actor was added, False if already existed.
    """
    if not name or not name.strip():
        return False

    name = name.strip()
    key = name.lower()

    # In-memory dedup
    if key in _seen_actors:
        return False
    _seen_actors.add(key)

    meta = {
        "source": source,
        "discovered_at": datetime.now(timezone.utc).isoformat(),
        **(metadata or {}),
    }

    try:
        import uuid
        # Server actors schema: id, name, tier, category, title, credibility,
        # metadata, data_sources, source, degree, icij_node_id, + more
        actor_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, name.lower()))
        category_map = {
            "person": "individual", "company": "institution",
            "government": "government", "fund": "institution",
            "entity": "institution", "unknown": "other",
        }
        cat = category_map.get(actor_type, "other")
        data_sources_entry = [{"source": source, "discovered": datetime.now(timezone.utc).isoformat()}]

        with engine.begin() as conn:
            result = conn.execute(text("""
                INSERT INTO actors (id, name, tier, category, credibility, source, metadata, data_sources)
                VALUES (:id, :name, '3', :cat, :cred, :source, :meta, :dsrc)
                ON CONFLICT (id) DO UPDATE SET
                    metadata = actors.metadata || :meta_update,
                    data_sources = actors.data_sources || :dsrc_update,
                    source = CASE
                        WHEN actors.source NOT LIKE :source_pattern
                        THEN actors.source || ',' || :source
                        ELSE actors.source
                    END,
                    updated_at = NOW()
                RETURNING id, (xmax = 0) AS is_new
            """), {
                "id": actor_id,
                "name": name,
                "cat": cat,
                "cred": confidence,
                "source": source,
                "source_pattern": f"%{source}%",
                "meta": json.dumps(meta),
                "dsrc": json.dumps(data_sources_entry),
                "meta_update": json.dumps({"sources": {source: datetime.now(timezone.utc).isoformat()}}),
                "dsrc_update": json.dumps(data_sources_entry),
            })
            row = result.fetchone()
            if row and row[1]:  # is_new
                log.info("NEW ACTOR: {n} ({t}) from {s}", n=name, t=actor_type, s=source)
                return True
    except Exception as exc:
        log.debug("Actor ingest failed for {n}: {e}", n=name, e=str(exc))

    return False


def ingest_actors_batch(
    engine: Engine,
    actors: list[tuple[str, str]],
    source: str = "auto",
    country: str | None = None,
) -> int:
    """Batch ingest multiple actors.

    Args:
        engine: SQLAlchemy engine.
        actors: List of (name, actor_type) tuples.
        source: Data source.
        country: Default country.

    Returns:
        Number of new actors added.
    """
    added = 0
    for name, actor_type in actors:
        if ingest_actor(engine, name, actor_type, source=source, country=country):
            added += 1
    return added


def extract_actors_from_payload(
    engine: Engine,
    payload: dict[str, Any],
    source: str,
    name_fields: list[str] | None = None,
) -> int:
    """Extract and ingest actors from a raw data payload.

    Scans the payload for fields that look like actor names and
    ingests them. Smart enough to skip numeric values, dates, etc.

    Args:
        engine: SQLAlchemy engine.
        payload: Raw data payload dict.
        source: Data source name.
        name_fields: List of field names to extract from. If None, uses defaults.

    Returns:
        Number of actors ingested.
    """
    if name_fields is None:
        name_fields = [
            "name", "person", "company", "organization", "org_name",
            "owner", "subsidiary", "parent", "head", "tail",
            "entity1_name", "entity2_name", "officer_name",
            "donor", "recipient", "lobbyist", "client",
            "board_member", "director", "ceo", "cfo",
            "description1", "description2",
        ]

    added = 0
    for field in name_fields:
        value = payload.get(field)
        if not value or not isinstance(value, str):
            continue

        value = value.strip()
        if len(value) < 3 or len(value) > 200:
            continue

        # Skip obvious non-names
        if value.replace(".", "").replace(",", "").isdigit():
            continue

        # Guess actor type from field name
        if field in ("person", "officer_name", "board_member", "director", "ceo", "cfo", "donor", "lobbyist"):
            actor_type = "person"
        elif field in ("company", "organization", "org_name", "subsidiary", "parent", "client"):
            actor_type = "company"
        else:
            actor_type = "unknown"

        if ingest_actor(engine, value, actor_type, source=source):
            added += 1

    return added


def get_actor_count(engine: Engine) -> int:
    """Get total actor count from the database."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text("SELECT COUNT(*) FROM actors")).fetchone()
            return row[0] if row else 0
    except Exception:
        return 0


def get_actor_sources(engine: Engine) -> dict[str, int]:
    """Get actor counts by category."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT category, COUNT(*) FROM actors GROUP BY category ORDER BY COUNT(*) DESC"
            )).fetchall()
            return {r[0]: r[1] for r in rows}
    except Exception:
        return {}
