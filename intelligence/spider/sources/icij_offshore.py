"""ICIJ Offshore Leaks adapter — discovers offshore entity connections.

Searches locally downloaded ICIJ data (Panama/Pandora Papers) for actor
matches, falling back to the public ICIJ API if local data is missing.
Confidence tier: 2 (public record).
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import requests
from loguru import logger as log

from intelligence.spider.models import DiscoveredConnection

_ICIJ_DATA_DIR = Path("/Users/anikdang/dev/GRID/data/icij/csv")
ICIJ_API = "https://offshoreleaks.icij.org/api/v1/search"

_REL_MAP = {
    "officer of": "officer_of_entity",
    "shareholder of": "shareholder_of",
    "intermediary of": "intermediary_of",
    "registered address": "offshore_entity_of",
    "similar name and target address as": "offshore_entity_of",
    "beneficial owner of": "shareholder_of",
}


class IcijOffshoreAdapter:
    """Discover actor connections via ICIJ Offshore Leaks data."""

    name = "icij_offshore"

    def discover(
        self, actor_name: str, actor_hint: dict[str, Any]
    ) -> list[DiscoveredConnection]:
        if _ICIJ_DATA_DIR.exists():
            return self._search_local(actor_name)
        return self._search_api(actor_name)

    def _search_local(self, actor_name: str) -> list[DiscoveredConnection]:
        """Search locally downloaded ICIJ CSV files."""
        try:
            connections: list[DiscoveredConnection] = []
            needle = actor_name.lower()

            # Build officer → node_id mapping
            officer_ids: set[str] = set()
            officers_file = _ICIJ_DATA_DIR / "nodes-officers.csv"
            if officers_file.exists():
                with open(officers_file, encoding="utf-8", errors="replace") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        name_col = row.get("name", "")
                        if needle in name_col.lower():
                            officer_ids.add(row.get("node_id", ""))

            # Build entity name lookup
            entities: dict[str, str] = {}
            entities_file = _ICIJ_DATA_DIR / "nodes-entities.csv"
            if entities_file.exists():
                with open(entities_file, encoding="utf-8", errors="replace") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        nid = row.get("node_id", "")
                        entities[nid] = row.get("name", "")
                        # Also check if entity name matches actor
                        if needle in row.get("name", "").lower():
                            officer_ids.add(nid)

            # Walk relationships to find connections
            rels_file = _ICIJ_DATA_DIR / "relationships.csv"
            if rels_file.exists() and officer_ids:
                with open(rels_file, encoding="utf-8", errors="replace") as f:
                    reader = csv.DictReader(f)
                    seen: set[str] = set()
                    for row in reader:
                        start = row.get("node_id_start", "")
                        end = row.get("node_id_end", "")
                        rel_type = row.get("rel_type", "").lower()

                        target_id = None
                        if start in officer_ids:
                            target_id = end
                        elif end in officer_ids:
                            target_id = start

                        if target_id is None:
                            continue

                        target_name = entities.get(target_id, f"entity_{target_id}")
                        if target_name in seen or target_name.lower() == needle:
                            continue
                        seen.add(target_name)

                        relationship = _REL_MAP.get(rel_type, "offshore_entity_of")
                        connections.append(DiscoveredConnection(
                            target_name=target_name,
                            relationship=relationship,
                            strength=0.8,
                            confidence_tier=2,
                            target_hint={"data_source": "icij_local"},
                            evidence=[{
                                "source": "icij_offshore_leaks",
                                "url": f"https://offshoreleaks.icij.org/search?q={actor_name}",
                                "excerpt": f"{actor_name} → {relationship} → {target_name}",
                                "rel_type": rel_type,
                            }],
                        ))

            log.debug("ICIJ local: {n} connections for {a}", n=len(connections), a=actor_name)
            return connections

        except Exception as exc:
            log.debug("ICIJ local search error for {a}: {e}", a=actor_name, e=str(exc))
            return []

    def _search_api(self, actor_name: str) -> list[DiscoveredConnection]:
        """Fall back to ICIJ public API."""
        try:
            resp = requests.get(
                ICIJ_API,
                params={"q": actor_name},
                headers={"User-Agent": "GRID-Spider/1.0"},
                timeout=15,
            )
            if resp.status_code != 200:
                log.debug("ICIJ API failed for {a}: HTTP {s}", a=actor_name, s=resp.status_code)
                return []

            data = resp.json()
            results = data if isinstance(data, list) else data.get("results", [])
            connections: list[DiscoveredConnection] = []
            seen: set[str] = set()

            for item in results[:30]:
                target = item.get("name", "") or item.get("entity", "")
                if not target or target.lower() == actor_name.lower() or target in seen:
                    continue
                seen.add(target)

                connections.append(DiscoveredConnection(
                    target_name=target,
                    relationship="offshore_entity_of",
                    strength=0.8,
                    confidence_tier=2,
                    target_hint={"data_source": "icij_api"},
                    evidence=[{
                        "source": "icij_offshore_leaks",
                        "url": f"https://offshoreleaks.icij.org/search?q={actor_name}",
                        "excerpt": f"{actor_name} linked to {target} in offshore leaks",
                    }],
                ))

            log.debug("ICIJ API: {n} connections for {a}", n=len(connections), a=actor_name)
            return connections

        except Exception as exc:
            log.debug("ICIJ API error for {a}: {e}", a=actor_name, e=str(exc))
            return []
