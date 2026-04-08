"""SEC EDGAR cross-reference adapter — discovers entity relationships from SEC filings.

Queries the EDGAR full-text search API for Forms 4, SC 13D/G, DEF 14A
to find officers, directors, and 5%+ holders linked to an actor.
Confidence tier: 1 (hard data).
"""

from __future__ import annotations

from typing import Any

import requests
from loguru import logger as log

from intelligence.spider.models import DiscoveredConnection

EDGAR_SEARCH = "https://efts.sec.gov/LATEST/search-index"

_FORM_RELATIONSHIP = {
    "4": "officer_of",
    "SC 13D": "5pct_holder",
    "SC 13G": "5pct_holder",
    "DEF 14A": "proxy_filing",
}


class SecCrossRefAdapter:
    """Discover actor connections via SEC EDGAR filing cross-references."""

    name = "sec_crossref"

    def discover(
        self, actor_name: str, actor_hint: dict[str, Any]
    ) -> list[DiscoveredConnection]:
        try:
            resp = requests.get(
                EDGAR_SEARCH,
                params={
                    "q": f'"{actor_name}"',
                    "dateRange": "custom",
                    "startdt": "2024-01-01",
                    "forms": "4,SC 13D,SC 13G,DEF 14A",
                },
                headers={"User-Agent": "GRID-Spider/1.0 grid@stepdad.finance"},
                timeout=15,
            )
            if resp.status_code != 200:
                log.debug("SEC EDGAR query failed for {a}: HTTP {s}", a=actor_name, s=resp.status_code)
                return []

            data = resp.json()
            hits = data.get("hits", {}).get("hits", [])
            connections: list[DiscoveredConnection] = []
            seen: set[str] = set()

            for hit in hits[:50]:
                source = hit.get("_source", {})
                form_type = source.get("form_type", "")
                entity_name = source.get("entity_name", "")
                display_names = source.get("display_names", [])
                filing_date = source.get("file_date", "")

                # Extract related entities from display_names
                for name in display_names:
                    name = name.strip()
                    if not name or name.lower() == actor_name.lower():
                        continue
                    if name in seen:
                        continue
                    seen.add(name)

                    relationship = _FORM_RELATIONSHIP.get(form_type, "filing_related")
                    filing_url = (
                        "https://www.sec.gov/cgi-bin/browse-edgar"
                        f"?action=getcompany&company={entity_name}&type={form_type}"
                    )

                    connections.append(DiscoveredConnection(
                        target_name=name,
                        relationship=relationship,
                        strength=0.85,
                        confidence_tier=1,
                        target_hint={"form_type": form_type, "filing_date": filing_date},
                        evidence=[{
                            "source": "sec_edgar",
                            "url": filing_url,
                            "excerpt": f"{actor_name} linked via {form_type} filing ({filing_date})",
                            "form_type": form_type,
                        }],
                    ))

                # Also link the entity_name itself if different from actor
                if (
                    entity_name
                    and entity_name.lower() != actor_name.lower()
                    and entity_name not in seen
                ):
                    seen.add(entity_name)
                    relationship = _FORM_RELATIONSHIP.get(form_type, "filing_related")
                    connections.append(DiscoveredConnection(
                        target_name=entity_name,
                        relationship=relationship,
                        strength=0.85,
                        confidence_tier=1,
                        target_hint={"form_type": form_type},
                        evidence=[{
                            "source": "sec_edgar",
                            "url": f"https://www.sec.gov/cgi-bin/browse-edgar?company={entity_name}",
                            "excerpt": f"{actor_name} named in {form_type} for {entity_name}",
                        }],
                    ))

            log.debug("SEC EDGAR: {n} connections for {a}", n=len(connections), a=actor_name)
            return connections

        except Exception as exc:
            log.debug("SEC EDGAR adapter error for {a}: {e}", a=actor_name, e=str(exc))
            return []
