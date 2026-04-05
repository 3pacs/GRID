"""
LittleSis puller — power-mapping database of who-knows-who in US politics/business.

API docs: https://littlesis.org/api
Free, no auth required for basic queries.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

LITTLESIS_API = "https://littlesis.org/api"


class LittleSisPuller(BasePuller):
    """Pull entity relationships from LittleSis power-mapping database."""

    SOURCE_NAME = "littlesis"
    SOURCE_CONFIG = {
        "base_url": LITTLESIS_API,
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": False,
        "revision_behavior": "FREQUENT",
        "trust_score": "HIGH",
        "priority_rank": 35,
    }

    @retry_on_failure(max_attempts=3)
    def search_entities(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        """Search LittleSis for entities by name.

        Args:
            query: Search query.
            limit: Max results.

        Returns:
            List of entity dicts.
        """
        resp = requests.get(
            f"{LITTLESIS_API}/entities/search",
            params={"q": query, "per_page": limit},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", [])

    @retry_on_failure(max_attempts=3)
    def get_entity_relationships(self, entity_id: int) -> list[dict[str, Any]]:
        """Get all relationships for a LittleSis entity.

        Args:
            entity_id: LittleSis entity ID.

        Returns:
            List of relationship dicts.
        """
        resp = requests.get(
            f"{LITTLESIS_API}/entities/{entity_id}/relationships",
            params={"per_page": 100},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", [])

    def pull(self, actor_names: list[str] | None = None) -> dict[str, Any]:
        """Pull relationships for actor_network entities from LittleSis.

        Args:
            actor_names: List of names to search. Defaults to actor_network.

        Returns:
            Summary with entity and relationship counts.
        """
        if actor_names is None:
            actor_names = self._get_actor_names()

        total_entities = 0
        total_relationships = 0

        for name in actor_names:
            try:
                entities = self.search_entities(name, limit=3)
                if not entities:
                    continue

                for entity in entities:
                    eid = entity.get("id")
                    attrs = entity.get("attributes", {})
                    entity_name = attrs.get("name", name)

                    # Store entity as raw series
                    with self.engine.begin() as conn:
                        self._insert_raw(
                            conn,
                            series_id=f"littlesis:entity:{eid}",
                            obs_date=date.today(),
                            value=1.0,
                            raw_payload={
                                "id": eid,
                                "name": entity_name,
                                "blurb": attrs.get("blurb", ""),
                                "primary_ext": attrs.get("primary_ext", ""),
                                "types": attrs.get("types", []),
                            },
                        )
                    total_entities += 1

                    # Auto-discover actor
                    try:
                        from intelligence.actor_ingest import ingest_actor, extract_actors_from_payload
                        ingest_actor(self.engine, entity_name,
                                    attrs.get("primary_ext", "unknown"),
                                    source="littlesis")
                    except Exception:
                        pass

                    # Get relationships
                    rels = self.get_entity_relationships(eid)
                    for rel in rels:
                        rel_attrs = rel.get("attributes", {})
                        with self.engine.begin() as conn:
                            self._insert_raw(
                                conn,
                                series_id=f"littlesis:rel:{rel.get('id')}",
                                obs_date=date.today(),
                                value=1.0,
                                raw_payload={
                                    "id": rel.get("id"),
                                    "entity1_id": rel_attrs.get("entity1_id"),
                                    "entity2_id": rel_attrs.get("entity2_id"),
                                    "category_id": rel_attrs.get("category_id"),
                                    "description1": rel_attrs.get("description1", ""),
                                    "description2": rel_attrs.get("description2", ""),
                                    "amount": rel_attrs.get("amount"),
                                    "is_current": rel_attrs.get("is_current"),
                                },
                            )
                        total_relationships += 1

                        # Extract actors from relationship payload
                        try:
                            from intelligence.actor_ingest import extract_actors_from_payload
                            extract_actors_from_payload(self.engine, rel_attrs, source="littlesis")
                        except Exception:
                            pass

            except Exception as exc:
                log.debug("LittleSis pull failed for {n}: {e}", n=name, e=str(exc))

        log.info("LittleSis: {e} entities, {r} relationships",
                 e=total_entities, r=total_relationships)
        return {"entities": total_entities, "relationships": total_relationships}

    def _get_actor_names(self) -> list[str]:
        try:
            from intelligence.actor_network import ACTORS
            return [a.get("name", "") for a in ACTORS if a.get("name")][:100]
        except (ImportError, AttributeError):
            return []
