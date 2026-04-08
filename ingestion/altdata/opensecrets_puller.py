"""
OpenSecrets puller — political donations, lobbying expenditures, revolving door.

Tracks money flows from corporations/individuals to politicians.
API: https://www.opensecrets.org/api
Requires API key (free registration at opensecrets.org/api).

Series stored:
  opensecrets.contrib.{cid}  — top contributors to a candidate (value = total $)
  opensecrets.industry.{cid} — industry breakdown for a candidate
  opensecrets.lobby.{org_id} — lobbying spend by an organization
"""

from __future__ import annotations

import json
import time
from datetime import date
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

OPENSECRETS_API = "https://www.opensecrets.org/api/"

# Key congressional members who sit on finance/markets/trade committees.
# CID = OpenSecrets Candidate ID (stable across cycles).
TRACKED_CANDIDATES: list[dict[str, str]] = [
    {"cid": "N00007360", "name": "Nancy Pelosi", "role": "House (CA-11)"},
    {"cid": "N00003389", "name": "Mitch McConnell", "role": "Senate (KY)"},
    {"cid": "N00033085", "name": "Kevin McCarthy", "role": "House (CA-20)"},
    {"cid": "N00009638", "name": "Chuck Schumer", "role": "Senate (NY)"},
    {"cid": "N00033492", "name": "Elizabeth Warren", "role": "Senate (MA)"},
    {"cid": "N00030612", "name": "Sherrod Brown", "role": "Senate Banking Chair"},
    {"cid": "N00006249", "name": "Patrick McHenry", "role": "House Financial Services"},
    {"cid": "N00001489", "name": "Maxine Waters", "role": "House Financial Services"},
]

# Major corporations/industries to track lobbying spend
TRACKED_ORGS: list[dict[str, str]] = [
    {"id": "D000000461", "name": "Goldman Sachs"},
    {"id": "D000000103", "name": "JPMorgan Chase"},
    {"id": "D000000082", "name": "Lockheed Martin"},
    {"id": "D000000085", "name": "Boeing"},
    {"id": "D000067336", "name": "Meta/Facebook"},
    {"id": "D000067401", "name": "Alphabet/Google"},
    {"id": "D000021754", "name": "Exxon Mobil"},
    {"id": "D000000091", "name": "Pfizer"},
]

_RATE_LIMIT_DELAY: float = 1.0  # OpenSecrets rate limit: ~200 req/day


class OpenSecretsPuller(BasePuller):
    """Pull political money flow data from OpenSecrets.

    Iterates over tracked congressional members and major corporations
    to pull contribution and lobbying data. Stores as raw_series for
    thesis scoring integration.
    """

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

        Parameters:
            method: API method name.
            params: Query parameters.

        Returns:
            Response JSON.
        """
        if not self.api_key:
            return {}

        params.update({"method": method, "output": "json", "apikey": self.api_key})
        resp = requests.get(OPENSECRETS_API, params=params, timeout=30)
        resp.raise_for_status()
        time.sleep(_RATE_LIMIT_DELAY)
        return resp.json()

    def pull_top_contributors(
        self,
        candidate_id: str,
        cycle: str = "2024",
        candidate_name: str = "",
    ) -> list[dict[str, Any]]:
        """Get top contributors to a candidate.

        Parameters:
            candidate_id: OpenSecrets CID.
            cycle: Election cycle year.
            candidate_name: Display name for logging.

        Returns:
            List of contributor dicts.
        """
        data = self._api_call("candContrib", {"cid": candidate_id, "cycle": cycle})
        if not data:
            return []

        contributors = data.get("response", {}).get("contributors", {}).get("contributor", [])
        if isinstance(contributors, dict):
            contributors = [contributors]

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
                    ingest_actor(
                        self.engine, r["org_name"], "company",
                        source="opensecrets",
                        metadata={"total_donated": r["total"], "candidate": candidate_name},
                    )
        except Exception:
            pass

        return results

    def pull_candidate_industries(
        self,
        candidate_id: str,
        cycle: str = "2024",
    ) -> list[dict[str, Any]]:
        """Get industry breakdown for a candidate.

        Parameters:
            candidate_id: OpenSecrets CID.
            cycle: Election cycle year.

        Returns:
            List of industry contribution dicts.
        """
        data = self._api_call("candIndustry", {"cid": candidate_id, "cycle": cycle})
        if not data:
            return []

        industries = data.get("response", {}).get("industries", {}).get("industry", [])
        if isinstance(industries, dict):
            industries = [industries]

        return [
            {
                "industry_name": ind.get("@attributes", {}).get("industry_name", ""),
                "total": float(ind.get("@attributes", {}).get("total", 0)),
                "indivs": float(ind.get("@attributes", {}).get("indivs", 0)),
                "pacs": float(ind.get("@attributes", {}).get("pacs", 0)),
            }
            for ind in industries
        ]

    def pull_org_summary(self, org_id: str) -> dict[str, Any]:
        """Get lobbying summary for an organization.

        Parameters:
            org_id: OpenSecrets organization ID.

        Returns:
            Org summary dict.
        """
        data = self._api_call("orgSummary", {"id": org_id})
        if not data:
            return {}
        return data.get("response", {}).get("organization", {}).get("@attributes", {})

    def pull(self) -> dict[str, Any]:
        """Pull donation and lobbying data for all tracked actors.

        Iterates over TRACKED_CANDIDATES and TRACKED_ORGS, storing
        results as raw_series rows for thesis scoring integration.

        Returns:
            Summary with counts per category.
        """
        if not self.api_key:
            log.warning("OPENSECRETS_API_KEY not configured — skipping pull")
            return {"error": "no_api_key", "records": 0}

        today = date.today()
        records = 0
        errors = 0

        with self.engine.begin() as conn:
            # Pull top contributors for each tracked candidate
            for candidate in TRACKED_CANDIDATES:
                cid = candidate["cid"]
                name = candidate["name"]
                try:
                    contribs = self.pull_top_contributors(cid, candidate_name=name)
                    if contribs:
                        total_raised = sum(c["total"] for c in contribs)
                        series_id = f"opensecrets.contrib.{cid}"

                        self._insert_raw(
                            conn=conn,
                            series_id=series_id,
                            obs_date=today,
                            value=total_raised,
                            raw_payload={
                                "candidate": name,
                                "role": candidate["role"],
                                "top_contributors": contribs[:5],
                                "total_raised_from_top": total_raised,
                            },
                        )
                        records += 1
                        log.debug(
                            "OpenSecrets: {name} — ${total:,.0f} from top contributors",
                            name=name, total=total_raised,
                        )

                    # Also pull industry breakdown
                    industries = self.pull_candidate_industries(cid)
                    if industries:
                        industry_total = sum(i["total"] for i in industries)
                        series_id = f"opensecrets.industry.{cid}"

                        self._insert_raw(
                            conn=conn,
                            series_id=series_id,
                            obs_date=today,
                            value=industry_total,
                            raw_payload={
                                "candidate": name,
                                "top_industries": industries[:10],
                            },
                        )
                        records += 1

                except Exception as exc:
                    log.warning(
                        "OpenSecrets candidate pull failed for {n}: {e}",
                        n=name, e=str(exc),
                    )
                    errors += 1

            # Pull lobbying data for tracked organizations
            for org in TRACKED_ORGS:
                org_id = org["id"]
                org_name = org["name"]
                try:
                    summary = self.pull_org_summary(org_id)
                    if summary:
                        lobby_total = float(summary.get("lobbying", 0) or 0)
                        series_id = f"opensecrets.lobby.{org_id}"

                        self._insert_raw(
                            conn=conn,
                            series_id=series_id,
                            obs_date=today,
                            value=lobby_total,
                            raw_payload={
                                "org_name": org_name,
                                "org_id": org_id,
                                "summary": summary,
                            },
                        )
                        records += 1
                        log.debug(
                            "OpenSecrets: {name} — ${total:,.0f} lobbying",
                            name=org_name, total=lobby_total,
                        )

                except Exception as exc:
                    log.warning(
                        "OpenSecrets org pull failed for {n}: {e}",
                        n=org_name, e=str(exc),
                    )
                    errors += 1

        log.info(
            "OpenSecrets pull complete: {r} records, {e} errors",
            r=records, e=errors,
        )
        return {"records": records, "errors": errors, "status": "SUCCESS"}
