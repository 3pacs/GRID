"""
Wikidata puller — structured entity data via SPARQL.

Pulls board memberships, corporate hierarchies, ownership structures,
and subsidiary relationships from Wikidata's knowledge graph.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"


class WikidataPuller(BasePuller):
    """Pull structured entity relationships from Wikidata SPARQL endpoint."""

    SOURCE_NAME = "wikidata"
    SOURCE_CONFIG = {
        "base_url": WIKIDATA_SPARQL,
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": False,
        "revision_behavior": "FREQUENT",
        "trust_score": "HIGH",
        "priority_rank": 40,
    }

    @retry_on_failure(max_attempts=3)
    def _sparql_query(self, query: str) -> list[dict[str, Any]]:
        """Execute a SPARQL query against Wikidata.

        Args:
            query: SPARQL query string.

        Returns:
            List of result binding dicts.
        """
        resp = requests.get(
            WIKIDATA_SPARQL,
            params={"query": query, "format": "json"},
            timeout=60,
            headers={"User-Agent": "GRID/1.0 (intelligence platform)"},
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", {}).get("bindings", [])

    def pull_board_members(self, company_name: str) -> list[dict[str, Any]]:
        """Get board members/directors for a company.

        Args:
            company_name: Company name to search.

        Returns:
            List of board member dicts.
        """
        query = f"""
        SELECT ?company ?companyLabel ?person ?personLabel ?positionLabel WHERE {{
          ?company rdfs:label "{company_name}"@en .
          ?company wdt:P3320 ?person .
          OPTIONAL {{ ?person wdt:P39 ?position . }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
        }} LIMIT 100
        """
        results = self._sparql_query(query)
        return [
            {
                "company": r.get("companyLabel", {}).get("value", ""),
                "person": r.get("personLabel", {}).get("value", ""),
                "position": r.get("positionLabel", {}).get("value", ""),
            }
            for r in results
        ]

    def pull_subsidiaries(self, company_name: str) -> list[dict[str, Any]]:
        """Get subsidiaries of a company.

        Args:
            company_name: Parent company name.

        Returns:
            List of subsidiary dicts.
        """
        query = f"""
        SELECT ?parent ?parentLabel ?subsidiary ?subsidiaryLabel WHERE {{
          ?parent rdfs:label "{company_name}"@en .
          ?subsidiary wdt:P749 ?parent .
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
        }} LIMIT 200
        """
        results = self._sparql_query(query)
        return [
            {
                "parent": r.get("parentLabel", {}).get("value", ""),
                "subsidiary": r.get("subsidiaryLabel", {}).get("value", ""),
            }
            for r in results
        ]

    def pull_ownership(self, company_name: str) -> list[dict[str, Any]]:
        """Get ownership/shareholder info for a company.

        Args:
            company_name: Company name.

        Returns:
            List of owner dicts.
        """
        query = f"""
        SELECT ?company ?companyLabel ?owner ?ownerLabel WHERE {{
          ?company rdfs:label "{company_name}"@en .
          ?company wdt:P127 ?owner .
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
        }} LIMIT 100
        """
        results = self._sparql_query(query)
        return [
            {
                "company": r.get("companyLabel", {}).get("value", ""),
                "owner": r.get("ownerLabel", {}).get("value", ""),
            }
            for r in results
        ]

    def pull(self, company_names: list[str] | None = None) -> dict[str, Any]:
        """Pull all relationship types for a list of companies.

        Args:
            company_names: Companies to query. Defaults to actor_network.

        Returns:
            Summary counts.
        """
        if company_names is None:
            company_names = self._get_company_names()

        total_board = 0
        total_subs = 0
        total_owners = 0

        for company in company_names:
            try:
                # Board members
                board = self.pull_board_members(company)
                for b in board:
                    with self.engine.begin() as conn:
                        self._insert_raw(
                            conn,
                            series_id=f"wikidata:board:{company}:{b['person']}",
                            obs_date=date.today(),
                            value=1.0,
                            raw_payload={"type": "board_member", **b},
                        )
                # Auto-discover actors from board members
                try:
                    from intelligence.actor_ingest import ingest_actor
                    for b in board:
                        ingest_actor(self.engine, b["person"], "person", source="wikidata")
                except Exception:
                    pass
                total_board += len(board)

                # Subsidiaries
                subs = self.pull_subsidiaries(company)
                for s in subs:
                    with self.engine.begin() as conn:
                        self._insert_raw(
                            conn,
                            series_id=f"wikidata:subsidiary:{company}:{s['subsidiary']}",
                            obs_date=date.today(),
                            value=1.0,
                            raw_payload={"type": "subsidiary", **s},
                        )
                try:
                    from intelligence.actor_ingest import ingest_actor
                    for s in subs:
                        ingest_actor(self.engine, s["subsidiary"], "company", source="wikidata")
                except Exception:
                    pass
                total_subs += len(subs)

                # Ownership
                owners = self.pull_ownership(company)
                for o in owners:
                    with self.engine.begin() as conn:
                        self._insert_raw(
                            conn,
                            series_id=f"wikidata:owner:{company}:{o['owner']}",
                            obs_date=date.today(),
                            value=1.0,
                            raw_payload={"type": "ownership", **o},
                        )
                try:
                    from intelligence.actor_ingest import ingest_actor
                    for o in owners:
                        ingest_actor(self.engine, o["owner"], "unknown", source="wikidata")
                except Exception:
                    pass
                total_owners += len(owners)

            except Exception as exc:
                log.debug("Wikidata pull failed for {c}: {e}", c=company, e=str(exc))

        log.info("Wikidata: {b} board, {s} subsidiaries, {o} owners",
                 b=total_board, s=total_subs, o=total_owners)
        return {"board_members": total_board, "subsidiaries": total_subs, "owners": total_owners}

    def _get_company_names(self) -> list[str]:
        try:
            from intelligence.actor_network import ACTORS
            return [
                a.get("name", "") for a in ACTORS
                if a.get("type") in ("company", "corporation", "org") and a.get("name")
            ][:50]
        except (ImportError, AttributeError):
            return []
