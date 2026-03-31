"""
GRID OpenCorporates API ingestion module.

Pulls corporate registry data from the OpenCorporates API to cross-reference
ICIJ offshore leak officers with their public company registrations.

Source: https://api.opencorporates.com/
Free tier: 50 search requests/month, unlimited direct company lookups.
No API key required for basic access.

Series stored with pattern: OPENCORP:{jurisdiction}:{company_number}:{field}
Signal source_type: 'corporate_registry'

This module:
    1. Searches companies and officers via the OpenCorporates API
    2. Cross-references ICIJ officers from the actors table
    3. Stores company registration data in raw_series + signal_sources
    4. Rate-limited to 1 request per 2 seconds to respect free tier
"""

from __future__ import annotations

import json
import re
import time
from datetime import date, datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# -- API Configuration --------------------------------------------------------

_BASE_URL: str = "https://api.opencorporates.com/v0.4"
_SEARCH_COMPANIES: str = f"{_BASE_URL}/companies/search"
_GET_COMPANY: str = f"{_BASE_URL}/companies/{{jurisdiction_code}}/{{company_number}}"
_SEARCH_OFFICERS: str = f"{_BASE_URL}/officers/search"

_REQUEST_TIMEOUT: int = 30
_RATE_LIMIT_DELAY: float = 2.0  # seconds between requests


# -- Rate Limiter -------------------------------------------------------------

class _RateLimiter:
    """Simple rate limiter: ensures minimum delay between API calls."""

    def __init__(self, min_interval: float = _RATE_LIMIT_DELAY) -> None:
        self._min_interval = min_interval
        self._last_call: float = 0.0

    def wait(self) -> None:
        """Block until the rate limit window has passed."""
        elapsed = time.monotonic() - self._last_call
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_call = time.monotonic()


# -- Puller Class -------------------------------------------------------------

class OpenCorporatesPuller(BasePuller):
    """Pulls corporate registry data from the OpenCorporates API.

    Cross-references ICIJ offshore leak officers with their public
    company registrations to build a fuller picture of corporate
    structures and beneficial ownership.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for OpenCorporates.
    """

    SOURCE_NAME: str = "OpenCorporates"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://api.opencorporates.com",
        "cost_tier": "FREE",
        "latency_class": "REALTIME",
        "pit_available": False,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 55,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialize the OpenCorporates puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        self._limiter = _RateLimiter()
        self._search_calls_used: int = 0
        log.info(
            "OpenCorporatesPuller initialised -- source_id={sid}",
            sid=self.source_id,
        )

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    @retry_on_failure(max_attempts=2, backoff=3.0)
    def _get(self, url: str, params: dict[str, Any] | None = None) -> dict | None:
        """Make a rate-limited GET request to the OpenCorporates API.

        Parameters:
            url: Full URL to request.
            params: Optional query parameters.

        Returns:
            Parsed JSON response dict, or None on failure.
        """
        self._limiter.wait()

        try:
            resp = requests.get(url, params=params, timeout=_REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            log.warning("OpenCorporates request failed: {e}", e=str(exc))
            return None

        if resp.status_code == 403:
            log.warning("OpenCorporates rate limit hit (403). Backing off.")
            time.sleep(10.0)
            return None
        if resp.status_code == 404:
            log.debug("OpenCorporates 404: {u}", u=url)
            return None
        if resp.status_code != 200:
            log.warning(
                "OpenCorporates HTTP {s} for {u}",
                s=resp.status_code,
                u=url,
            )
            return None

        try:
            return resp.json()
        except ValueError:
            log.warning("OpenCorporates returned non-JSON response")
            return None

    # ------------------------------------------------------------------
    # Public API methods
    # ------------------------------------------------------------------

    def search_company(self, name: str) -> list[dict]:
        """Search for companies by name.

        Uses a search request (counts against 50/month free tier limit).

        Parameters:
            name: Company name to search for.

        Returns:
            List of company result dicts with keys: name, company_number,
            jurisdiction_code, incorporation_date, company_type, status,
            registered_address, opencorporates_url.
        """
        if not name or len(name.strip()) < 2:
            return []

        self._search_calls_used += 1
        log.debug(
            "OpenCorporates company search: {q} (search call #{n})",
            q=name,
            n=self._search_calls_used,
        )

        data = self._get(_SEARCH_COMPANIES, params={"q": name.strip()})
        if not data:
            return []

        results: list[dict] = []
        try:
            companies = (
                data.get("results", {})
                .get("companies", [])
            )
            for item in companies:
                company = item.get("company", {})
                results.append({
                    "name": company.get("name", ""),
                    "company_number": company.get("company_number", ""),
                    "jurisdiction_code": company.get("jurisdiction_code", ""),
                    "incorporation_date": company.get("incorporation_date", ""),
                    "company_type": company.get("company_type", ""),
                    "status": company.get("current_status", ""),
                    "registered_address": (
                        company.get("registered_address_in_full", "")
                    ),
                    "opencorporates_url": company.get("opencorporates_url", ""),
                })
        except (KeyError, TypeError) as exc:
            log.warning("Failed to parse company search results: {e}", e=str(exc))

        log.info(
            "OpenCorporates company search '{q}': {n} results",
            q=name,
            n=len(results),
        )
        return results

    def get_company(
        self,
        jurisdiction_code: str,
        company_number: str,
    ) -> dict | None:
        """Get a specific company by jurisdiction and number.

        Direct lookups are unlimited on the free tier.

        Parameters:
            jurisdiction_code: e.g. 'us_de', 'gb', 'ky'.
            company_number: Company registration number.

        Returns:
            Company detail dict, or None if not found.
        """
        url = _GET_COMPANY.format(
            jurisdiction_code=jurisdiction_code,
            company_number=company_number,
        )
        data = self._get(url)
        if not data:
            return None

        try:
            company = data.get("results", {}).get("company", {})
            officers_raw = company.get("officers", [])
            officers = []
            for o in officers_raw:
                officer = o.get("officer", {})
                officers.append({
                    "name": officer.get("name", ""),
                    "position": officer.get("position", ""),
                    "start_date": officer.get("start_date", ""),
                    "end_date": officer.get("end_date", ""),
                })

            return {
                "name": company.get("name", ""),
                "company_number": company.get("company_number", ""),
                "jurisdiction_code": company.get("jurisdiction_code", ""),
                "incorporation_date": company.get("incorporation_date", ""),
                "dissolution_date": company.get("dissolution_date", ""),
                "company_type": company.get("company_type", ""),
                "status": company.get("current_status", ""),
                "registered_address": (
                    company.get("registered_address_in_full", "")
                ),
                "agent_name": company.get("agent_name", ""),
                "agent_address": company.get("agent_address", ""),
                "officers": officers,
                "opencorporates_url": company.get("opencorporates_url", ""),
                "source": company.get("source", {}),
            }
        except (KeyError, TypeError) as exc:
            log.warning("Failed to parse company detail: {e}", e=str(exc))
            return None

    def search_officer(self, name: str) -> list[dict]:
        """Search for officers (directors/secretaries) by name.

        Uses a search request (counts against 50/month free tier limit).

        Parameters:
            name: Person name to search across all jurisdictions.

        Returns:
            List of officer result dicts with keys: name, position,
            company_name, company_number, jurisdiction_code, start_date,
            end_date, opencorporates_url.
        """
        if not name or len(name.strip()) < 3:
            return []

        self._search_calls_used += 1
        log.debug(
            "OpenCorporates officer search: {q} (search call #{n})",
            q=name,
            n=self._search_calls_used,
        )

        data = self._get(_SEARCH_OFFICERS, params={"q": name.strip()})
        if not data:
            return []

        results: list[dict] = []
        try:
            officers = (
                data.get("results", {})
                .get("officers", [])
            )
            for item in officers:
                officer = item.get("officer", {})
                company = officer.get("company", {})
                results.append({
                    "name": officer.get("name", ""),
                    "position": officer.get("position", ""),
                    "company_name": company.get("name", ""),
                    "company_number": company.get("company_number", ""),
                    "jurisdiction_code": company.get("jurisdiction_code", ""),
                    "start_date": officer.get("start_date", ""),
                    "end_date": officer.get("end_date", ""),
                    "opencorporates_url": officer.get("opencorporates_url", ""),
                })
        except (KeyError, TypeError) as exc:
            log.warning("Failed to parse officer search results: {e}", e=str(exc))

        log.info(
            "OpenCorporates officer search '{q}': {n} results",
            q=name,
            n=len(results),
        )
        return results

    # ------------------------------------------------------------------
    # Series ID builder
    # ------------------------------------------------------------------

    @staticmethod
    def _build_series_id(
        jurisdiction: str,
        company_number: str,
        field: str,
    ) -> str:
        """Build an OPENCORP series_id.

        Parameters:
            jurisdiction: Jurisdiction code (e.g. 'us_de', 'gb').
            company_number: Company registration number.
            field: Data field name (e.g. 'status', 'officer_match').

        Returns:
            Series ID string in pattern OPENCORP:{jurisdiction}:{number}:{field}.
        """
        jur_clean = re.sub(r"[^a-zA-Z0-9_]", "_", jurisdiction)[:20]
        num_clean = re.sub(r"[^a-zA-Z0-9_]", "_", company_number)[:40]
        field_clean = re.sub(r"[^a-zA-Z0-9_]", "_", field)[:30]
        return f"OPENCORP:{jur_clean}:{num_clean}:{field_clean}"

    # ------------------------------------------------------------------
    # Cross-reference ICIJ officers
    # ------------------------------------------------------------------

    def cross_reference_icij(self, engine: Engine) -> list[dict]:
        """Cross-reference top ICIJ officers with OpenCorporates.

        Queries the actors table for individuals sourced from ICIJ
        offshore leaks, then searches OpenCorporates for their public
        company registrations to build a complete corporate picture.

        Parameters:
            engine: SQLAlchemy engine (used for actor queries).

        Returns:
            List of cross-reference result dicts with keys: actor_name,
            actor_id, opencorporates_results, match_count.
        """
        # Fetch ICIJ-sourced individuals from the database
        icij_officers: list[dict] = []

        try:
            with engine.connect() as conn:
                # Look for raw_series entries with OFFSHORE: prefix
                # that have actor names in their payloads
                rows = conn.execute(text("""
                    SELECT DISTINCT
                        raw_payload->>'actor_name' AS actor_name,
                        raw_payload->>'actor_id' AS actor_id
                    FROM raw_series
                    WHERE series_id LIKE 'OFFSHORE:%'
                      AND raw_payload->>'actor_name' IS NOT NULL
                    ORDER BY raw_payload->>'actor_name'
                    LIMIT 50
                """)).fetchall()

                for row in rows:
                    actor_name = row[0]
                    actor_id = row[1]
                    if actor_name and len(actor_name.strip()) >= 3:
                        icij_officers.append({
                            "name": actor_name.strip(),
                            "actor_id": actor_id or "",
                        })
        except Exception as exc:
            log.warning(
                "Failed to query ICIJ officers from raw_series: {e}",
                e=str(exc),
            )

        if not icij_officers:
            log.info("No ICIJ officers found in raw_series for cross-reference")
            return []

        log.info(
            "Cross-referencing {n} ICIJ officers with OpenCorporates",
            n=len(icij_officers),
        )

        # Search each officer in OpenCorporates (conservative: officer search
        # uses search quota, so we limit to what we have)
        cross_refs: list[dict] = []

        for officer in icij_officers:
            name = officer["name"]

            # Guard against burning through the 50/month search quota
            if self._search_calls_used >= 45:
                log.warning(
                    "Approaching OpenCorporates search limit "
                    "({n}/50 used). Stopping cross-reference.",
                    n=self._search_calls_used,
                )
                break

            results = self.search_officer(name)

            if results:
                cross_ref = {
                    "actor_name": name,
                    "actor_id": officer["actor_id"],
                    "opencorporates_results": results,
                    "match_count": len(results),
                }
                cross_refs.append(cross_ref)

                # Store each matched company registration
                self._store_cross_ref(cross_ref)

                log.info(
                    "ICIJ officer '{name}' found in {n} company registrations",
                    name=name,
                    n=len(results),
                )
            else:
                log.debug(
                    "ICIJ officer '{name}' not found in OpenCorporates",
                    name=name,
                )

        log.info(
            "OpenCorporates cross-reference complete: {matched}/{total} "
            "ICIJ officers found in public registries",
            matched=len(cross_refs),
            total=len(icij_officers),
        )
        return cross_refs

    # ------------------------------------------------------------------
    # Store cross-reference results
    # ------------------------------------------------------------------

    def _store_cross_ref(self, cross_ref: dict) -> int:
        """Store cross-reference results in raw_series.

        Parameters:
            cross_ref: Dict with actor_name, actor_id, opencorporates_results.

        Returns:
            Number of rows inserted.
        """
        inserted = 0
        today = date.today()
        actor_name = cross_ref["actor_name"]
        actor_id = cross_ref.get("actor_id", "")

        with self.engine.begin() as conn:
            for result in cross_ref.get("opencorporates_results", []):
                jurisdiction = result.get("jurisdiction_code", "unknown")
                company_number = result.get("company_number", "unknown")

                series_id = self._build_series_id(
                    jurisdiction, company_number, "officer_match",
                )

                # Deduplicate: skip if we already have this match (30-day window)
                if self._row_exists(series_id, today, conn, dedup_hours=720):
                    continue

                payload = {
                    "actor_name": actor_name,
                    "actor_id": actor_id,
                    "officer_name": result.get("name", ""),
                    "officer_position": result.get("position", ""),
                    "company_name": result.get("company_name", ""),
                    "company_number": company_number,
                    "jurisdiction_code": jurisdiction,
                    "start_date": result.get("start_date", ""),
                    "end_date": result.get("end_date", ""),
                    "opencorporates_url": result.get("opencorporates_url", ""),
                    "confidence": "derived",
                    "source_cross_ref": "ICIJ_OFFSHORE -> OpenCorporates",
                }

                try:
                    self._insert_raw(
                        conn=conn,
                        series_id=series_id,
                        obs_date=today,
                        value=1.0,  # binary flag: match exists
                        raw_payload=payload,
                        pull_status="SUCCESS",
                    )
                    inserted += 1
                except Exception as exc:
                    log.debug(
                        "Failed to insert cross-ref for {name}/{co}: {e}",
                        name=actor_name,
                        co=company_number,
                        e=str(exc),
                    )

        if inserted:
            log.info(
                "Stored {n} OpenCorporates registrations for '{name}'",
                n=inserted,
                name=actor_name,
            )
        return inserted

    # ------------------------------------------------------------------
    # pull_all: main entry point
    # ------------------------------------------------------------------

    def pull_all(self) -> dict[str, Any]:
        """Run full OpenCorporates ingestion.

        Cross-references top 50 ICIJ individuals against OpenCorporates
        public company registrations.

        Returns:
            Dict with status, officers_checked, matches_found,
            registrations_stored.
        """
        log.info("Starting OpenCorporates pull_all")

        cross_refs = self.cross_reference_icij(self.engine)

        total_registrations = sum(
            cr.get("match_count", 0) for cr in cross_refs
        )

        result = {
            "status": "SUCCESS" if cross_refs else "NO_MATCHES",
            "officers_checked": len(cross_refs),
            "matches_found": len([cr for cr in cross_refs if cr["match_count"] > 0]),
            "total_registrations": total_registrations,
            "search_calls_used": self._search_calls_used,
            "search_calls_remaining": max(0, 50 - self._search_calls_used),
        }

        log.info(
            "OpenCorporates pull complete: {m} officers matched, "
            "{r} registrations found, {u}/50 search calls used",
            m=result["matches_found"],
            r=total_registrations,
            u=self._search_calls_used,
        )
        return result
