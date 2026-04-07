"""Wikidata SPARQL adapter — discovers structured relationships for public figures.

Queries Wikidata for board seats, employers, education, family, political party.
Confidence tier: 2 (public record).
"""

from __future__ import annotations

from typing import Any

import requests
from loguru import logger as log

from intelligence.spider.models import DiscoveredConnection

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"

_RELATIONSHIP_PROPS = [
    "P108",   # employer
    "P463",   # member of
    "P39",    # position held
    "P102",   # member of political party
    "P69",    # educated at
    "P22",    # father
    "P25",    # mother
    "P26",    # spouse
    "P1037",  # director/manager
    "P3320",  # board member
]


class WikidataAdapter:
    """Discover actor connections via Wikidata SPARQL."""

    name = "wikidata"

    def discover(
        self, actor_name: str, actor_hint: dict[str, Any]
    ) -> list[DiscoveredConnection]:
        props_filter = " ".join(f"wd:{p}" for p in _RELATIONSHIP_PROPS)
        query = f"""
        SELECT ?relatedLabel ?relatedDescription ?propLabel WHERE {{
          ?person rdfs:label "{actor_name}"@en .
          VALUES ?prop {{ {props_filter} }}
          ?person ?prop ?related .
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }}
        LIMIT 50
        """
        try:
            resp = requests.get(
                WIKIDATA_SPARQL,
                params={"query": query, "format": "json"},
                headers={"User-Agent": "GRID-Spider/1.0"},
                timeout=15,
            )
            if resp.status_code != 200:
                log.debug("Wikidata query failed for {a}: HTTP {s}", a=actor_name, s=resp.status_code)
                return []

            bindings = resp.json().get("results", {}).get("bindings", [])
            connections: list[DiscoveredConnection] = []
            for b in bindings:
                target = b.get("relatedLabel", {}).get("value", "")
                rel = b.get("propLabel", {}).get("value", "")
                desc = b.get("relatedDescription", {}).get("value", "")
                if not target or not rel:
                    continue
                connections.append(DiscoveredConnection(
                    target_name=target,
                    relationship=rel,
                    strength=0.7,
                    confidence_tier=2,
                    target_hint={"description": desc} if desc else {},
                    evidence=[{
                        "source": "wikidata",
                        "url": f"https://www.wikidata.org/wiki/Special:Search/{actor_name}",
                        "excerpt": f"{actor_name} → {rel} → {target}",
                    }],
                ))
            log.debug("Wikidata: {n} connections for {a}", n=len(connections), a=actor_name)
            return connections

        except Exception as exc:
            log.debug("Wikidata adapter error for {a}: {e}", a=actor_name, e=str(exc))
            return []
