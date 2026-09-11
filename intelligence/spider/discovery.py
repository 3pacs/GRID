"""Discovery orchestrator — fans out to source adapters and deduplicates results."""

from __future__ import annotations

from typing import Any

from loguru import logger as log

from intelligence.actor_identity import build_person_index, resolve_canonical_actor_id
from intelligence.entity_resolver import SpiderEntityResolver as EntityResolver
from intelligence.spider.graph_engine import GraphEngine
from intelligence.spider.models import ConnectionMeta, DiscoveredConnection

# Actor-id prefixes that hold natural persons, for the SEC person-filer rule
# ("SCRIVNER DOUGLAS G" -> the insider actor already in the graph).
_PERSON_ID_PREFIXES: tuple[str, ...] = ("insider_", "ins_", "congress_", "gov_")


class DiscoveryOrchestrator:
    """Orchestrates connection discovery across all source adapters."""

    def __init__(self, graph: GraphEngine, resolver: EntityResolver, adapters: list[Any]) -> None:
        self._graph = graph
        self._resolver = resolver
        self._adapters: list[Any] = adapters

    def _build_person_index(self) -> dict[str, str]:
        """Name key -> person actor id, over the person-shaped ids only.

        Restricting to person ids keeps a company from ever answering a person
        key. One O(actors) pass, built at most once per expansion and only when
        a person filer actually turns up.
        """
        return build_person_index(
            (actor_id, data.get("name", ""))
            for actor_id, data in self._graph.iter_actors()
            if actor_id.startswith(_PERSON_ID_PREFIXES)
        )

    def expand(self, actor_id: str) -> tuple[list[dict[str, Any]], list[tuple[str, str, ConnectionMeta]]]:
        """Expand an actor's connections using all adapters.

        Returns (new_actors, new_connections) where:
        - new_actors: list of actor dicts that were created
        - new_connections: list of (source_id, target_id, ConnectionMeta)
        """
        actor_data = self._graph.get_actor(actor_id)
        if not actor_data:
            log.warning("Cannot expand unknown actor: {a}", a=actor_id)
            return [], []

        actor_name = actor_data.get("name", "")
        actor_hint = {
            "category": actor_data.get("category", ""),
            "tier": actor_data.get("tier", ""),
            "title": actor_data.get("title", ""),
        }

        all_discovered: list[DiscoveredConnection] = []
        for adapter in self._adapters:
            try:
                results = adapter.discover(actor_name, actor_hint)
                all_discovered.extend(results)
            except Exception as exc:
                log.debug("Adapter {n} failed for {a}: {e}", n=getattr(adapter, "name", "?"), a=actor_name, e=str(exc))

        # Deduplicate by (target_name, relationship)
        seen: set[tuple[str, str]] = set()
        unique: list[DiscoveredConnection] = []
        for dc in all_discovered:
            key = (dc.target_name.lower().strip(), dc.relationship.lower().strip())
            if key not in seen:
                seen.add(key)
                unique.append(dc)

        new_actors: list[dict[str, Any]] = []
        new_connections: list[tuple[str, str, ConnectionMeta]] = []

        # Built on first use: most expansions turn up no person filer at all.
        person_index: dict[str, str] | None = None

        def person_lookup(key: str) -> str | None:
            nonlocal person_index
            if person_index is None:
                person_index = self._build_person_index()
            return person_index.get(key)

        for dc in unique:
            # SEC filer display names ("BlackRock Inc.  (BLK)  (CIK 0001364742)")
            # name an entity the graph usually already holds as a ticker or
            # insider actor. Resolve that first — it is a hard-data identity,
            # where the resolver below is a fuzzy edit-distance guess — and
            # attach the edge to the canonical node instead of minting a
            # corporation_<slug> twin that only ever connects to other twins.
            canonical = resolve_canonical_actor_id(
                dc.target_name,
                exists=self._graph.has_actor,
                person_lookup=person_lookup,
            )
            if canonical is not None:
                target_id, rule = canonical
                self._graph.add_alias(target_id, dc.target_name)
                log.debug(
                    "Folded filer '{n}' into {t} via {r}",
                    n=dc.target_name, t=target_id, r=rule,
                )
            else:
                target_id = self._resolver.resolve(dc.target_name, dc.target_hint)

            if target_id is None:
                category = dc.target_hint.get("category", "corporation")
                target_id = self._resolver.generate_id(dc.target_name, category)
                actor_degree = actor_data.get("degree", 0)
                new_actor = {
                    "name": dc.target_name,
                    "tier": "institutional",
                    "category": category,
                    "title": dc.target_hint.get("description", ""),
                    "influence_score": 0.3,
                    "trust_score": 0.5,
                    "degree": actor_degree + 1,
                    "source": dc.evidence[0].get("source", "unknown") if dc.evidence else "unknown",
                    "credibility": _tier_to_credibility(dc.confidence_tier),
                    "data_sources": [dc.evidence[0].get("source", "unknown")] if dc.evidence else [],
                }
                self._graph.add_actor(target_id, new_actor)
                new_actors.append({**new_actor, "id": target_id})

            sources = [e.get("source", "unknown") for e in dc.evidence if isinstance(e, dict)]
            meta = ConnectionMeta(
                relationship=dc.relationship,
                strength=dc.strength,
                confidence_tier=dc.confidence_tier,
                sources=sources,
            )
            self._graph.add_connection(actor_id, target_id, meta)
            new_connections.append((actor_id, target_id, meta))

        log.info("Expanded {a}: {d} discovered, {n} new actors, {c} connections",
                 a=actor_name, d=len(unique), n=len(new_actors), c=len(new_connections))
        return new_actors, new_connections


def _tier_to_credibility(tier: int) -> str:
    return {1: "hard_data", 2: "public_record", 3: "inferred", 4: "rumor"}.get(tier, "inferred")
