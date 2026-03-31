"""
GRID UK Companies House ingestion module.

Pulls beneficial ownership, officer, and company data from the UK Companies
House API (free, requires API key from
https://developer.company-information.service.gov.uk/).

Endpoints:
    - Search companies by name
    - Get company profile
    - Get Persons with Significant Control (PSC) — beneficial owners
    - Get officers (directors, secretaries)

Series stored with pattern: UKCH:{company_number}:{field}
Signal source_type: 'beneficial_ownership'

This module:
    1. Searches Companies House for entities matching watchlist tickers
    2. Pulls company profile, PSC register, and officer list
    3. Stores results in raw_series
    4. Emits signal_sources entries for significant PSC changes
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import date, datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ── API Configuration ────────────────────────────────────────────────────

_BASE_URL: str = "https://api.company-information.service.gov.uk"
_REQUEST_TIMEOUT: int = 30

# Rate limit: 600 requests per 5 minutes = 2 requests/second average.
# Use 0.55s between requests to stay safely under.
_RATE_LIMIT_DELAY: float = 0.55

# Default ticker-to-company-name mapping for watchlist pulls.
# Companies House is UK-focused, so this maps tickers to their UK
# registered entity names where applicable.
_TICKER_TO_NAME: dict[str, str] = {
    "AAPL": "Apple",
    "MSFT": "Microsoft",
    "GOOGL": "Alphabet",
    "AMZN": "Amazon",
    "META": "Meta Platforms",
    "TSLA": "Tesla",
    "NVDA": "Nvidia",
    "JPM": "JPMorgan",
    "GS": "Goldman Sachs",
    "MS": "Morgan Stanley",
    "BAC": "Bank of America",
    "C": "Citigroup",
    "HSBA.L": "HSBC Holdings",
    "BARC.L": "Barclays",
    "LLOY.L": "Lloyds Banking Group",
    "NWG.L": "NatWest Group",
    "STAN.L": "Standard Chartered",
    "AZN.L": "AstraZeneca",
    "GSK.L": "GlaxoSmithKline",
    "SHEL.L": "Shell",
    "BP.L": "BP",
    "RIO.L": "Rio Tinto",
    "BHP.L": "BHP Group",
    "GLEN.L": "Glencore",
    "VOD.L": "Vodafone",
    "ARM.L": "ARM Holdings",
}


# ══════════════════════════════════════════════════════════════════════════
# PULLER CLASS
# ══════════════════════════════════════════════════════════════════════════

class UKCompaniesHousePuller(BasePuller):
    """Pulls company, PSC, and officer data from UK Companies House.

    Uses HTTP Basic auth with the API key as the username and an empty
    password, per the Companies House API specification.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for UK_Companies_House.
        api_key: Companies House API key.
    """

    SOURCE_NAME: str = "UK_Companies_House"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": _BASE_URL,
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": False,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 45,
    }

    def __init__(self, api_key: str, db_engine: Engine) -> None:
        """Initialize the UK Companies House puller.

        Parameters:
            api_key: Companies House API key. Falls back to
                     UK_COMPANIES_HOUSE_KEY env var if empty.
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        self.api_key: str = api_key or os.environ.get(
            "UK_COMPANIES_HOUSE_KEY", "",
        )
        if not self.api_key:
            log.warning(
                "UK_COMPANIES_HOUSE_KEY not set — Companies House pulls "
                "will fail. Get a free key from "
                "https://developer.company-information.service.gov.uk/"
            )
        self._request_count: int = 0
        self._window_start: float = time.monotonic()
        log.info(
            "UKCompaniesHousePuller initialised — source_id={sid}",
            sid=self.source_id,
        )

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------
    def _rate_limit(self) -> None:
        """Enforce rate limit: 600 requests per 5 minutes.

        Tracks requests in a rolling window and sleeps if approaching
        the limit. Also applies a per-request delay for safety.
        """
        now = time.monotonic()
        elapsed = now - self._window_start

        # Reset window every 5 minutes
        if elapsed >= 300:
            self._request_count = 0
            self._window_start = now

        # If approaching limit, sleep until window resets
        if self._request_count >= 580:
            sleep_time = 300 - elapsed + 1.0
            if sleep_time > 0:
                log.info(
                    "Companies House rate limit approaching — "
                    "sleeping {s:.0f}s",
                    s=sleep_time,
                )
                time.sleep(sleep_time)
                self._request_count = 0
                self._window_start = time.monotonic()

        # Per-request delay
        time.sleep(_RATE_LIMIT_DELAY)
        self._request_count += 1

    # ------------------------------------------------------------------
    # HTTP helper
    # ------------------------------------------------------------------
    @retry_on_failure(max_attempts=3, backoff=2.0)
    def _get(self, path: str, params: dict | None = None) -> dict | list | None:
        """Make an authenticated GET request to the Companies House API.

        Uses HTTP Basic auth with api_key as username, empty password.

        Parameters:
            path: API path (e.g. '/company/12345678').
            params: Optional query parameters.

        Returns:
            Parsed JSON response, or None on error.
        """
        if not self.api_key:
            log.warning("No API key — skipping Companies House request")
            return None

        self._rate_limit()

        url = f"{_BASE_URL}{path}"
        try:
            resp = requests.get(
                url,
                params=params,
                auth=(self.api_key, ""),
                timeout=_REQUEST_TIMEOUT,
            )
            if resp.status_code == 404:
                log.debug("Companies House 404: {p}", p=path)
                return None
            if resp.status_code == 429:
                log.warning(
                    "Companies House rate limited — backing off 60s"
                )
                time.sleep(60)
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as exc:
            log.warning(
                "Companies House HTTP error: {s} {p}",
                s=getattr(exc.response, "status_code", "?"),
                p=path,
            )
            return None
        except requests.exceptions.RequestException as exc:
            log.warning(
                "Companies House request failed: {e}",
                e=str(exc),
            )
            raise ConnectionError(str(exc)) from exc

    # ------------------------------------------------------------------
    # Search companies
    # ------------------------------------------------------------------
    def search_companies(
        self,
        query: str,
        items_per_page: int = 10,
    ) -> list[dict]:
        """Search for companies by name.

        Parameters:
            query: Company name search string.
            items_per_page: Number of results (max 100).

        Returns:
            List of company result dicts with company_number, title, etc.
        """
        data = self._get(
            "/search/companies",
            params={"q": query, "items_per_page": items_per_page},
        )
        if not data or not isinstance(data, dict):
            return []
        return data.get("items", [])

    # ------------------------------------------------------------------
    # Get company profile
    # ------------------------------------------------------------------
    def get_company(self, company_number: str) -> dict | None:
        """Get full company profile.

        Parameters:
            company_number: 8-character Companies House number.

        Returns:
            Company profile dict, or None if not found.
        """
        data = self._get(f"/company/{company_number}")
        if isinstance(data, dict):
            return data
        return None

    # ------------------------------------------------------------------
    # Get PSC (Persons with Significant Control)
    # ------------------------------------------------------------------
    def get_psc(self, company_number: str) -> list[dict]:
        """Get Persons with Significant Control for a company.

        PSC data reveals beneficial ownership — who really controls the
        company, including ownership percentages and control types.

        Parameters:
            company_number: 8-character Companies House number.

        Returns:
            List of PSC dicts.
        """
        data = self._get(
            f"/company/{company_number}/persons-with-significant-control",
        )
        if not data or not isinstance(data, dict):
            return []
        return data.get("items", [])

    # ------------------------------------------------------------------
    # Get officers
    # ------------------------------------------------------------------
    def get_officers(self, company_number: str) -> list[dict]:
        """Get officers (directors, secretaries) for a company.

        Parameters:
            company_number: 8-character Companies House number.

        Returns:
            List of officer dicts.
        """
        data = self._get(f"/company/{company_number}/officers")
        if not data or not isinstance(data, dict):
            return []
        return data.get("items", [])

    # ------------------------------------------------------------------
    # Pull a single company (profile + PSC + officers)
    # ------------------------------------------------------------------
    def pull_company(self, company_number: str) -> dict[str, Any]:
        """Pull full details for a single company.

        Fetches profile, PSC register, and officers, then stores all
        data in raw_series.

        Parameters:
            company_number: 8-character Companies House number.

        Returns:
            Dict with status and counts.
        """
        company_number = company_number.strip().upper()
        log.info(
            "Pulling Companies House data for {cn}",
            cn=company_number,
        )

        profile = self.get_company(company_number)
        if not profile:
            log.warning(
                "Company {cn} not found on Companies House",
                cn=company_number,
            )
            return {"status": "NOT_FOUND", "company_number": company_number}

        psc_list = self.get_psc(company_number)
        officers = self.get_officers(company_number)

        raw_count = 0
        signal_count = 0
        today = date.today()
        now = datetime.now(timezone.utc)

        with self.engine.begin() as conn:
            # ── Store company profile ──
            profile_sid = f"UKCH:{company_number}:profile"
            if not self._row_exists(profile_sid, today, conn, dedup_hours=24):
                self._insert_raw(
                    conn,
                    series_id=profile_sid,
                    obs_date=today,
                    value=1.0,
                    raw_payload=profile,
                )
                raw_count += 1

            # ── Store PSC entries ──
            for i, psc in enumerate(psc_list):
                psc_name = _sanitize_name(
                    psc.get("name", psc.get("name_elements", {}).get(
                        "surname", f"psc_{i}",
                    )),
                )
                psc_sid = f"UKCH:{company_number}:psc:{psc_name}"

                if not self._row_exists(psc_sid, today, conn, dedup_hours=24):
                    # Determine ownership percentage from natures_of_control
                    ownership_pct = _extract_ownership_pct(
                        psc.get("natures_of_control", []),
                    )

                    self._insert_raw(
                        conn,
                        series_id=psc_sid,
                        obs_date=today,
                        value=ownership_pct,
                        raw_payload={
                            "company_number": company_number,
                            "company_name": profile.get("company_name", ""),
                            "psc": psc,
                        },
                    )
                    raw_count += 1

                    # Emit signal if PSC was recently notified (new filing)
                    notified = psc.get("notified_on", "")
                    if _is_recent(notified, days=30):
                        try:
                            conn.execute(text("""
                                INSERT INTO signal_sources
                                    (source_type, source_id, ticker,
                                     signal_type, signal_date,
                                     signal_value, metadata)
                                VALUES
                                    (:stype, :sid, :ticker, :signal_type,
                                     :sdate, :sval, :meta)
                            """), {
                                "stype": "beneficial_ownership",
                                "sid": psc_sid,
                                "ticker": company_number,
                                "signal_type": "PSC_CHANGE",
                                "sdate": now,
                                "sval": ownership_pct,
                                "meta": json.dumps({
                                    "company_name": profile.get(
                                        "company_name", "",
                                    ),
                                    "psc_name": psc.get("name", ""),
                                    "natures_of_control": psc.get(
                                        "natures_of_control", [],
                                    ),
                                    "notified_on": notified,
                                    "kind": psc.get("kind", ""),
                                    "confidence": "confirmed",
                                }, default=str),
                            })
                            signal_count += 1
                        except Exception as exc:
                            log.debug(
                                "Failed to emit PSC signal: {e}",
                                e=str(exc),
                            )

            # ── Store officers ──
            for i, officer in enumerate(officers):
                officer_name = _sanitize_name(
                    officer.get("name", f"officer_{i}"),
                )
                officer_sid = f"UKCH:{company_number}:officer:{officer_name}"

                if not self._row_exists(
                    officer_sid, today, conn, dedup_hours=24,
                ):
                    self._insert_raw(
                        conn,
                        series_id=officer_sid,
                        obs_date=today,
                        value=1.0,
                        raw_payload={
                            "company_number": company_number,
                            "company_name": profile.get("company_name", ""),
                            "officer": officer,
                        },
                    )
                    raw_count += 1

        log.info(
            "Companies House {cn}: {r} raw_series, {s} signals",
            cn=company_number,
            r=raw_count,
            s=signal_count,
        )
        return {
            "status": "SUCCESS",
            "company_number": company_number,
            "company_name": profile.get("company_name", ""),
            "raw_series_inserted": raw_count,
            "signals_emitted": signal_count,
            "psc_count": len(psc_list),
            "officers_count": len(officers),
        }

    # ------------------------------------------------------------------
    # Search and pull
    # ------------------------------------------------------------------
    def search_and_pull(
        self,
        name: str,
        max_results: int = 5,
    ) -> list[dict[str, Any]]:
        """Search for a company by name and pull top results.

        Parameters:
            name: Company name to search for.
            max_results: Maximum number of companies to pull (default 5).

        Returns:
            List of pull result dicts, one per company.
        """
        log.info("Searching Companies House for '{n}'", n=name)
        results = self.search_companies(name, items_per_page=max_results)

        if not results:
            log.info("No Companies House results for '{n}'", n=name)
            return []

        pull_results: list[dict[str, Any]] = []
        for item in results[:max_results]:
            company_number = item.get("company_number", "")
            if not company_number:
                continue

            result = self.pull_company(company_number)
            result["search_title"] = item.get("title", "")
            result["search_snippet"] = item.get("snippet", "")
            pull_results.append(result)

        log.info(
            "Companies House search '{n}': pulled {c} companies",
            n=name,
            c=len(pull_results),
        )
        return pull_results

    # ------------------------------------------------------------------
    # Pull watchlist
    # ------------------------------------------------------------------
    def pull_watchlist(
        self,
        ticker_map: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Pull Companies House data for all watchlist tickers.

        Maps tickers to company names, searches Companies House, and
        pulls the top result for each.

        Parameters:
            ticker_map: Optional custom ticker->name mapping. Falls back
                        to _TICKER_TO_NAME default.

        Returns:
            Summary dict with per-ticker results.
        """
        mapping = ticker_map or _TICKER_TO_NAME
        log.info(
            "Companies House watchlist pull — {n} tickers",
            n=len(mapping),
        )

        all_results: dict[str, Any] = {}
        total_raw = 0
        total_signals = 0

        for ticker, name in mapping.items():
            try:
                results = self.search_and_pull(name, max_results=1)
                if results:
                    best = results[0]
                    all_results[ticker] = best
                    total_raw += best.get("raw_series_inserted", 0)
                    total_signals += best.get("signals_emitted", 0)
                else:
                    all_results[ticker] = {"status": "NO_RESULTS"}
            except Exception as exc:
                log.warning(
                    "Companies House pull failed for {t} ({n}): {e}",
                    t=ticker,
                    n=name,
                    e=str(exc),
                )
                all_results[ticker] = {
                    "status": "FAILED",
                    "error": str(exc),
                }

        log.info(
            "Companies House watchlist complete: {r} raw_series, "
            "{s} signals across {t} tickers",
            r=total_raw,
            s=total_signals,
            t=len(mapping),
        )
        return {
            "status": "SUCCESS",
            "tickers_processed": len(mapping),
            "total_raw_series": total_raw,
            "total_signals": total_signals,
            "results": all_results,
        }

    # ------------------------------------------------------------------
    # Pull all (entry point for scheduler)
    # ------------------------------------------------------------------
    def pull_all(self) -> dict[str, Any]:
        """Run a full watchlist pull.

        This is the standard entry point called by the ingestion
        scheduler.

        Returns:
            Summary dict from pull_watchlist().
        """
        return self.pull_watchlist()


# ══════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════

def _sanitize_name(name: str) -> str:
    """Sanitize a name for use in a series_id.

    Parameters:
        name: Raw name string.

    Returns:
        Cleaned string safe for series_id usage.
    """
    if not name:
        return "unknown"
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", name.strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned[:80] or "unknown"


def _extract_ownership_pct(natures_of_control: list[str]) -> float:
    """Extract approximate ownership percentage from PSC control natures.

    Companies House reports ownership in bands, not exact percentages.
    This returns the midpoint of the highest applicable band.

    Parameters:
        natures_of_control: List of control nature strings from PSC data.

    Returns:
        Estimated ownership percentage (0-100), or 0.0 if unknown.
    """
    if not natures_of_control:
        return 0.0

    joined = " ".join(natures_of_control).lower()

    # Ownership bands defined by PSC regulations
    if "75-to-100" in joined:
        return 87.5
    if "50-to-75" in joined:
        return 62.5
    if "25-to-50" in joined:
        return 37.5
    if "ownership-of-shares" in joined or "voting-rights" in joined:
        # Has some control but band not specified — assume lower band
        return 37.5
    if "significant-influence-or-control" in joined:
        return 25.0  # Significant influence without majority
    return 0.0


def _is_recent(date_str: str, days: int = 30) -> bool:
    """Check if a date string (YYYY-MM-DD) is within the last N days.

    Parameters:
        date_str: Date in YYYY-MM-DD format.
        days: Lookback window in days.

    Returns:
        True if the date is recent, False otherwise or on parse failure.
    """
    if not date_str:
        return False
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d").date()
        return (date.today() - dt).days <= days
    except (ValueError, TypeError):
        return False
