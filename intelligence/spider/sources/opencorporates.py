"""OpenCorporates adapter — discovers corporate registry connections.

Searches the free-tier OpenCorporates API for companies and officers
linked to an actor name.
Confidence tier: 2 (public record).
"""

from __future__ import annotations

from typing import Any

import requests
from loguru import logger as log

from intelligence.spider.models import DiscoveredConnection

OC_COMPANIES = "https://api.opencorporates.com/v0.4/companies/search"
OC_OFFICERS = "https://api.opencorporates.com/v0.4/officers/search"


class OpenCorporatesAdapter:
    """Discover actor connections via OpenCorporates corporate registries."""

    name = "opencorporates"

    def discover(
        self, actor_name: str, actor_hint: dict[str, Any]
    ) -> list[DiscoveredConnection]:
        connections: list[DiscoveredConnection] = []
        seen: set[str] = set()

        connections.extend(self._search_companies(actor_name, seen))
        connections.extend(self._search_officers(actor_name, seen))

        log.debug("OpenCorporates: {n} connections for {a}", n=len(connections), a=actor_name)
        return connections

    def _search_companies(
        self, actor_name: str, seen: set[str]
    ) -> list[DiscoveredConnection]:
        try:
            resp = requests.get(
                OC_COMPANIES,
                params={"q": actor_name, "format": "json"},
                headers={"User-Agent": "GRID-Spider/1.0"},
                timeout=15,
            )
            if resp.status_code != 200:
                log.debug("OpenCorporates companies failed for {a}: HTTP {s}", a=actor_name, s=resp.status_code)
                return []

            data = resp.json()
            companies = data.get("results", {}).get("companies", [])
            connections: list[DiscoveredConnection] = []

            for item in companies[:20]:
                company = item.get("company", {})
                name = company.get("name", "")
                jurisdiction = company.get("jurisdiction_code", "")
                oc_url = company.get("opencorporates_url", "")

                if not name or name.lower() == actor_name.lower() or name in seen:
                    continue
                seen.add(name)

                connections.append(DiscoveredConnection(
                    target_name=name,
                    relationship="incorporated_in",
                    strength=0.7,
                    confidence_tier=2,
                    target_hint={
                        "jurisdiction": jurisdiction,
                        "type": "company",
                    },
                    evidence=[{
                        "source": "opencorporates",
                        "url": oc_url or f"https://opencorporates.com/companies?q={actor_name}",
                        "excerpt": f"{actor_name} linked to {name} ({jurisdiction})",
                    }],
                ))

            return connections

        except Exception as exc:
            log.debug("OpenCorporates companies error for {a}: {e}", a=actor_name, e=str(exc))
            return []

    def _search_officers(
        self, actor_name: str, seen: set[str]
    ) -> list[DiscoveredConnection]:
        try:
            resp = requests.get(
                OC_OFFICERS,
                params={"q": actor_name, "format": "json"},
                headers={"User-Agent": "GRID-Spider/1.0"},
                timeout=15,
            )
            if resp.status_code != 200:
                log.debug("OpenCorporates officers failed for {a}: HTTP {s}", a=actor_name, s=resp.status_code)
                return []

            data = resp.json()
            officers = data.get("results", {}).get("officers", [])
            connections: list[DiscoveredConnection] = []

            for item in officers[:20]:
                officer = item.get("officer", {})
                company = officer.get("company", {})
                company_name = company.get("name", "")
                position = officer.get("position", "officer")
                oc_url = officer.get("opencorporates_url", "")

                if not company_name or company_name in seen:
                    continue
                seen.add(company_name)

                # Map position to relationship
                pos_lower = (position or "").lower()
                if "director" in pos_lower:
                    relationship = "director_of"
                elif "agent" in pos_lower:
                    relationship = "registered_agent_of"
                else:
                    relationship = "officer_of"

                connections.append(DiscoveredConnection(
                    target_name=company_name,
                    relationship=relationship,
                    strength=0.7,
                    confidence_tier=2,
                    target_hint={
                        "position": position,
                        "type": "company",
                    },
                    evidence=[{
                        "source": "opencorporates",
                        "url": oc_url or f"https://opencorporates.com/officers?q={actor_name}",
                        "excerpt": f"{actor_name} serves as {position} of {company_name}",
                    }],
                ))

            return connections

        except Exception as exc:
            log.debug("OpenCorporates officers error for {a}: {e}", a=actor_name, e=str(exc))
            return []
