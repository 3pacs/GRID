"""Google Knowledge Graph adapter — discovers structured entity relationships.

Queries the Google Knowledge Graph Search API for entities matching
an actor name and extracts schema.org typed relationships.
Confidence tier: 2 (public record).
Requires GOOGLE_KG_API_KEY env var (skips gracefully if missing).
"""

from __future__ import annotations

import os
from typing import Any

import requests
from loguru import logger as log

from intelligence.spider.models import DiscoveredConnection

KG_SEARCH = "https://kgsearch.googleapis.com/v1/entities:search"

_SCHEMA_RELATIONSHIPS = {
    "worksFor": "worksFor",
    "alumniOf": "alumniOf",
    "spouse": "spouse",
    "memberOf": "memberOf",
    "affiliation": "memberOf",
    "foundedBy": "founded",
    "parentOrganization": "subsidiary_of",
    "subOrganization": "parent_of",
}


class GoogleKgAdapter:
    """Discover actor connections via Google Knowledge Graph API."""

    name = "google_kg"

    def discover(
        self, actor_name: str, actor_hint: dict[str, Any]
    ) -> list[DiscoveredConnection]:
        api_key = os.environ.get("GOOGLE_KG_API_KEY", "")
        if not api_key:
            log.debug("Google KG: GOOGLE_KG_API_KEY not set, skipping")
            return []

        try:
            resp = requests.get(
                KG_SEARCH,
                params={
                    "query": actor_name,
                    "key": api_key,
                    "limit": 10,
                    "indent": "false",
                },
                headers={"User-Agent": "GRID-Spider/1.0"},
                timeout=15,
            )
            if resp.status_code != 200:
                log.debug("Google KG failed for {a}: HTTP {s}", a=actor_name, s=resp.status_code)
                return []

            data = resp.json()
            elements = data.get("itemListElement", [])
            connections: list[DiscoveredConnection] = []
            seen: set[str] = set()

            for element in elements:
                result = element.get("result", {})
                result_name = result.get("name", "")
                description = result.get("description", "")
                detailed = result.get("detailedDescription", {})
                detail_body = detailed.get("articleBody", "")
                detail_url = detailed.get("url", "")
                types = result.get("@type", [])

                if not result_name or result_name.lower() == actor_name.lower():
                    continue

                # Extract relationships from description text
                relationships_found = self._extract_relationships(
                    actor_name, result_name, description, detail_body
                )

                for rel in relationships_found:
                    key = f"{result_name}:{rel}"
                    if key in seen:
                        continue
                    seen.add(key)

                    connections.append(DiscoveredConnection(
                        target_name=result_name,
                        relationship=rel,
                        strength=0.6,
                        confidence_tier=2,
                        target_hint={
                            "description": description,
                            "types": types if isinstance(types, list) else [types],
                        },
                        evidence=[{
                            "source": "google_knowledge_graph",
                            "url": detail_url or f"https://www.google.com/search?kgmid={result.get('@id', '')}",
                            "excerpt": detail_body[:300] if detail_body else description,
                        }],
                    ))

                # If no specific relationship found, still link if it appears related
                if not relationships_found and result_name not in seen:
                    seen.add(result_name)
                    connections.append(DiscoveredConnection(
                        target_name=result_name,
                        relationship="associated_with",
                        strength=0.5,
                        confidence_tier=2,
                        target_hint={"description": description},
                        evidence=[{
                            "source": "google_knowledge_graph",
                            "url": detail_url or "https://www.google.com/",
                            "excerpt": description[:300] if description else result_name,
                        }],
                    ))

            log.debug("Google KG: {n} connections for {a}", n=len(connections), a=actor_name)
            return connections

        except Exception as exc:
            log.debug("Google KG adapter error for {a}: {e}", a=actor_name, e=str(exc))
            return []

    @staticmethod
    def _extract_relationships(
        actor: str, target: str, description: str, body: str
    ) -> list[str]:
        """Extract schema.org-like relationships from description text."""
        text = f"{description} {body}".lower()
        rels: list[str] = []

        if "spouse" in text or "married" in text:
            rels.append("spouse")
        if "alumni" in text or "graduated" in text or "studied" in text:
            rels.append("alumniOf")
        if "works for" in text or "employed" in text or "ceo of" in text:
            rels.append("worksFor")
        if "member of" in text or "joined" in text:
            rels.append("memberOf")
        if "founded" in text or "co-founded" in text:
            rels.append("founded")

        return rels
