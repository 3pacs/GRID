"""
OpenSecrets puller — political donations, lobbying expenditures, revolving door.

Tracks money flows from corporations/individuals to politicians.
API: https://www.opensecrets.org/api
Requires API key (free registration).
"""

from __future__ import annotations

from datetime import date
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

OPENSECRETS_API = "https://www.opensecrets.org/api/"


class OpenSecretsPuller(BasePuller):
    """Pull political money flow data from OpenSecrets."""

    SOURCE_NAME = "opensecrets"
    SOURCE_CONFIG = {
        "base_url": OPENSECRETS_API,
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": False,
        "revision_behavior": "FREQUENT",
        "trust_score": "HIGH",
        "priority_rank": 32,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        from config import settings
        self.api_key = getattr(settings, "OPENSECRETS_API_KEY", "")

    @retry_on_failure(max_attempts=3)
    def _api_call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        """Make an OpenSecrets API call.

        Args:
            method: API method name.
            params: Query parameters.

        Returns:
            Response JSON.
        """
        if not self.api_key:
            log.warning("No OPENSECRETS_API_KEY set — skipping")
            return {}

        params.update({"method": method, "output": "json", "apikey": self.api_key})
        resp = requests.get(OPENSECRETS_API, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def pull_top_contributors(self, candidate_id: str, cycle: str = "2024", candidate_name: str = "") -> list[dict[str, Any]]:
        """Get top contributors to a candidate.

        Args:
            candidate_id: OpenSecrets CID.
            cycle: Election cycle year.

        Returns:
            List of contributor dicts.
        """
        data = self._api_call("candContrib", {"cid": candidate_id, "cycle": cycle})
        contributors = data.get("response", {}).get("contributors", {}).get("contributor", [])
        results = [
            {
                "org_name": c.get("@attributes", {}).get("org_name", ""),
                "total": float(c.get("@attributes", {}).get("total", 0)),
                "pacs": float(c.get("@attributes", {}).get("pacs", 0)),
                "indivs": float(c.get("@attributes", {}).get("indivs", 0)),
            }
            for c in contributors
        ]

        # Auto-discover actors from contributors
        try:
            from intelligence.actor_ingest import ingest_actor
            for r in results:
                if r["org_name"]:
                    ingest_actor(self.engine, r["org_name"], "company", source="opensecrets",
                                metadata={"total_donated": r["total"], "candidate": candidate_name})
        except Exception:
            pass

        return results

    def pull_org_summary(self, org_id: str) -> dict[str, Any]:
        """Get lobbying summary for an organization.

        Args:
            org_id: OpenSecrets organization ID.

        Returns:
            Org summary dict.
        """
        data = self._api_call("orgSummary", {"id": org_id})
        return data.get("response", {}).get("organization", {}).get("@attributes", {})

    def pull(self) -> dict[str, Any]:
        """Pull donation and lobbying data for tracked actors.

        Returns:
            Summary with counts.
        """
        if not self.api_key:
            log.warning("OPENSECRETS_API_KEY not configured — skipping pull")
            return {"error": "no_api_key", "records": 0}

        records = 0
        # Example: pull top contributors for key congressional committees
        # In production, this would iterate over tracked candidate IDs
        log.info("OpenSecrets puller ready — configure candidate/org IDs for full pull")
        return {"records": records, "status": "configured"}
