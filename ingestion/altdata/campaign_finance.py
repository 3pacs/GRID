"""
GRID campaign finance tracker — FEC API ingestion module.

Pulls PAC contributions and individual contributions from the FEC API
(api.open.fec.gov). Free, no key needed but rate-limited.

Focus areas:
  - Semiconductor industry PACs (SIA PAC, Intel PAC, Qualcomm PAC, etc.)
  - Defense industry PACs
  - Tech industry PACs
  - Individual contributions from executives at tracked companies

Maps recipients to congressional member names for cross-reference with
trades (the hypocrisy detector: who donates to whom, and does that
lawmaker then trade in the donor's sector?).

Series pattern: FEC:{pac_name}:{recipient}:{amount}
Emits signal_sources entries for trust scoring integration.

Scheduled: monthly (FEC data updates slowly).

Data source documentation:
  https://api.open.fec.gov/
  Rate limit: 1000 requests/hour without API key
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ── API Configuration ────────────────────────────────────────────────────

_FEC_API_KEY_ENV: str = "FEC_API_KEY"
_FEC_BASE_URL: str = "https://api.open.fec.gov/v1"

_REQUEST_TIMEOUT: int = 30
_RATE_LIMIT_DELAY: float = 1.5  # Conservative — FEC rate limits are strict
_MAX_PAGES: int = 10  # Safety limit per PAC

# ── Tracked PACs (semiconductor, defense, tech) ──────────────────────────

TRACKED_PACS: dict[str, dict[str, str]] = {
    # Semiconductor industry
    "C00520809": {"name": "SIA PAC", "sector": "semiconductor", "ticker": "SMH"},
    "C00103697": {"name": "Intel PAC", "sector": "semiconductor", "ticker": "INTC"},
    "C00309823": {"name": "Qualcomm PAC", "sector": "semiconductor", "ticker": "QCOM"},
    "C00379636": {"name": "Texas Instruments PAC", "sector": "semiconductor", "ticker": "TXN"},
    "C00381624": {"name": "Broadcom PAC", "sector": "semiconductor", "ticker": "AVGO"},
    "C00390005": {"name": "Applied Materials PAC", "sector": "semiconductor", "ticker": "AMAT"},
    "C00348656": {"name": "Micron Technology PAC", "sector": "semiconductor", "ticker": "MU"},
    "C00384503": {"name": "NVIDIA PAC", "sector": "semiconductor", "ticker": "NVDA"},

    # Defense industry
    "C00303024": {"name": "Lockheed Martin PAC", "sector": "defense", "ticker": "LMT"},
    "C00104299": {"name": "Raytheon PAC", "sector": "defense", "ticker": "RTX"},
    "C00105080": {"name": "Northrop Grumman PAC", "sector": "defense", "ticker": "NOC"},
    "C00105163": {"name": "General Dynamics PAC", "sector": "defense", "ticker": "GD"},
    "C00110338": {"name": "Boeing PAC", "sector": "defense", "ticker": "BA"},

    # Big tech
    "C00428623": {"name": "Microsoft PAC", "sector": "tech", "ticker": "MSFT"},
    "C00473777": {"name": "Google PAC", "sector": "tech", "ticker": "GOOGL"},
    "C00502906": {"name": "Amazon PAC", "sector": "tech", "ticker": "AMZN"},
    "C00350793": {"name": "Apple PAC", "sector": "tech", "ticker": "AAPL"},
    "C00492785": {"name": "Meta PAC", "sector": "tech", "ticker": "META"},
    "C00474189": {"name": "Palantir PAC", "sector": "tech", "ticker": "PLTR"},
}

# ── Helper functions ─────────────────────────────────────────────────────


def _slugify(name: str, max_len: int = 40) -> str:
    """Create a slug from a name for use in series_id."""
    slug = re.sub(r"[^A-Z0-9 ]", "", name.strip().upper())
    slug = re.sub(r"\s+", "_", slug.strip())
    return slug[:max_len]


def _format_amount(amount: float) -> str:
    """Format dollar amount as compact tag."""
    if abs(amount) >= 1_000_000:
        return f"{amount / 1_000_000:.1f}M"
    if abs(amount) >= 1_000:
        return f"{amount / 1_000:.0f}K"
    return f"{amount:.0f}"


def _parse_fec_date(date_str: str | None) -> date:
    """Parse FEC date string into date object.

    Parameters:
        date_str: Date string in various FEC formats.

    Returns:
        Parsed date, or today if unparseable.
    """
    if not date_str:
        return date.today()
    try:
        # FEC uses multiple formats
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y"):
            try:
                return datetime.strptime(date_str[:19], fmt).date()
            except ValueError:
                continue
        return date.fromisoformat(date_str[:10])
    except (ValueError, TypeError):
        return date.today()


# ── Puller Class ─────────────────────────────────────────────────────────


class CampaignFinancePuller(BasePuller):
    """Pulls campaign finance data from the FEC API.

    Fetches PAC contributions to candidates and individual contributions,
    maps recipients to congressional member names, and stores each
    contribution as a raw_series row.

    Series pattern: FEC:{pac_name_slug}:{recipient_slug}:{amount_tag}
    Value: contribution amount in USD.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for FEC_CAMPAIGN_FINANCE.
        api_key: Optional FEC API key (works without, but rate limited).
    """

    SOURCE_NAME: str = "FEC_CAMPAIGN_FINANCE"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://api.open.fec.gov/v1",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 33,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the campaign finance puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        # FEC API key is optional — works without but with stricter rate limits
        self.api_key: str = os.environ.get(_FEC_API_KEY_ENV, "DEMO_KEY")
        super().__init__(db_engine)
        log.info(
            "CampaignFinancePuller initialised — source_id={sid}",
            sid=self.source_id,
        )

    # ── API helpers ──────────────────────────────────────────────────────

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _fec_get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Make a GET request to the FEC API.

        Parameters:
            endpoint: API path relative to base URL.
            params: Query parameters.

        Returns:
            Parsed JSON response dict.
        """
        url = f"{_FEC_BASE_URL}{endpoint}"
        req_params = {"api_key": self.api_key}
        if params:
            req_params.update(params)

        resp = requests.get(
            url,
            params=req_params,
            timeout=_REQUEST_TIMEOUT,
            headers={
                "User-Agent": "GRID-DataPuller/1.0",
                "Accept": "application/json",
            },
        )

        # Handle rate limiting
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            log.warning("FEC rate limited — waiting {s}s", s=retry_after)
            time.sleep(min(retry_after, 120))
            raise requests.RequestException("Rate limited — will retry")

        resp.raise_for_status()
        time.sleep(_RATE_LIMIT_DELAY)
        return resp.json()

    # ── PAC contribution fetching ────────────────────────────────────────

    def _fetch_pac_contributions(
        self,
        committee_id: str,
        cycle: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch disbursements (contributions to candidates) from a PAC.

        Parameters:
            committee_id: FEC committee ID (e.g. 'C00520809').
            cycle: Election cycle year (e.g. 2024). Defaults to current.

        Returns:
            List of contribution dicts.
        """
        if cycle is None:
            # Current or most recent election cycle
            today = date.today()
            cycle = today.year if today.year % 2 == 0 else today.year + 1

        all_contributions: list[dict[str, Any]] = []
        last_index = None
        page = 0

        while page < _MAX_PAGES:
            params: dict[str, Any] = {
                "committee_id": committee_id,
                "cycle": cycle,
                "per_page": 100,
                "sort": "-contribution_receipt_date",
            }
            if last_index:
                params["last_index"] = last_index

            try:
                data = self._fec_get(
                    "/schedules/schedule_b/",
                    params=params,
                )
            except Exception as exc:
                log.warning(
                    "FEC PAC contribution fetch failed for {id}: {e}",
                    id=committee_id, e=str(exc),
                )
                break

            results = data.get("results", [])
            if not results:
                break

            for result in results:
                parsed = self._parse_contribution(result, committee_id)
                if parsed:
                    all_contributions.append(parsed)

            # FEC uses cursor-based pagination
            pagination = data.get("pagination", {})
            last_index = pagination.get("last_indexes", {}).get("last_index")
            if not last_index or len(results) < 100:
                break

            page += 1

        return all_contributions

    def _parse_contribution(
        self,
        raw: dict[str, Any],
        committee_id: str,
    ) -> dict[str, Any] | None:
        """Parse a FEC disbursement record into a normalised dict.

        Parameters:
            raw: Raw result dict from FEC API.
            committee_id: Source committee ID.

        Returns:
            Normalised contribution dict, or None if unusable.
        """
        try:
            amount = float(raw.get("disbursement_amount", 0) or 0)
            if amount <= 0:
                return None

            recipient_name = (raw.get("recipient_name") or "").strip()
            if not recipient_name:
                return None

            pac_info = TRACKED_PACS.get(committee_id, {})

            obs_date = _parse_fec_date(
                raw.get("disbursement_date") or raw.get("contribution_receipt_date")
            )

            # Candidate info if this is a contribution to a candidate
            candidate_id = raw.get("candidate_id", "")
            candidate_name = (
                raw.get("candidate_name")
                or raw.get("recipient_name")
                or ""
            ).strip()

            return {
                "committee_id": committee_id,
                "pac_name": pac_info.get("name", committee_id),
                "pac_sector": pac_info.get("sector", "unknown"),
                "pac_ticker": pac_info.get("ticker", ""),
                "recipient_name": recipient_name,
                "candidate_id": candidate_id,
                "candidate_name": candidate_name,
                "amount": amount,
                "obs_date": obs_date,
                "disbursement_type": (raw.get("disbursement_type") or "").strip(),
                "disbursement_description": (raw.get("disbursement_description") or "").strip()[:200],
                "recipient_state": (raw.get("recipient_state") or "").strip(),
                "memo_text": (raw.get("memo_text") or "").strip()[:200],
            }

        except Exception as exc:
            log.debug("Failed to parse FEC contribution: {e}", e=str(exc))
            return None

    # ── Individual contribution fetching (executives) ────────────────────

    def _fetch_individual_contributions(
        self,
        employer_name: str,
        cycle: int | None = None,
        min_amount: float = 5000.0,
    ) -> list[dict[str, Any]]:
        """Fetch individual contributions from employees of a company.

        Parameters:
            employer_name: Company name to search for.
            cycle: Election cycle year.
            min_amount: Minimum contribution amount to include.

        Returns:
            List of contribution dicts.
        """
        if cycle is None:
            today = date.today()
            cycle = today.year if today.year % 2 == 0 else today.year + 1

        all_contributions: list[dict[str, Any]] = []
        last_index = None
        page = 0

        while page < _MAX_PAGES:
            params: dict[str, Any] = {
                "contributor_employer": employer_name,
                "cycle": cycle,
                "min_amount": min_amount,
                "per_page": 100,
                "sort": "-contribution_receipt_date",
            }
            if last_index:
                params["last_index"] = last_index

            try:
                data = self._fec_get(
                    "/schedules/schedule_a/",
                    params=params,
                )
            except Exception as exc:
                log.debug(
                    "FEC individual contribution fetch for {emp} failed: {e}",
                    emp=employer_name, e=str(exc),
                )
                break

            results = data.get("results", [])
            if not results:
                break

            for result in results:
                try:
                    amount = float(result.get("contribution_receipt_amount", 0) or 0)
                    if amount < min_amount:
                        continue

                    contributor_name = (result.get("contributor_name") or "").strip()
                    recipient_committee = (result.get("committee", {}) or {}).get("name", "")

                    obs_date = _parse_fec_date(result.get("contribution_receipt_date"))

                    all_contributions.append({
                        "contributor_name": contributor_name,
                        "contributor_employer": employer_name,
                        "contributor_occupation": (result.get("contributor_occupation") or "").strip(),
                        "recipient_committee": recipient_committee,
                        "recipient_id": result.get("committee_id", ""),
                        "amount": amount,
                        "obs_date": obs_date,
                        "contributor_state": (result.get("contributor_state") or "").strip(),
                    })
                except Exception:
                    continue

            pagination = data.get("pagination", {})
            last_index = pagination.get("last_indexes", {}).get("last_index")
            if not last_index or len(results) < 100:
                break

            page += 1

        return all_contributions

    # ── Storage ──────────────────────────────────────────────────────────

    def _store_contribution(
        self,
        conn: Any,
        contribution: dict[str, Any],
    ) -> bool:
        """Store a campaign contribution as a raw_series row.

        Parameters:
            conn: Active database connection (within a transaction).
            contribution: Normalised contribution dict.

        Returns:
            True if stored, False if duplicate.
        """
        pac_slug = _slugify(contribution.get("pac_name", contribution.get("contributor_name", "UNKNOWN")))
        recipient_slug = _slugify(
            contribution.get("candidate_name", contribution.get("recipient_name", "UNKNOWN"))
        )
        amount_tag = _format_amount(contribution["amount"])

        series_id = f"FEC:{pac_slug}:{recipient_slug}:{amount_tag}"
        obs_date = contribution["obs_date"]

        if self._row_exists(series_id, obs_date, conn, dedup_hours=168):
            return False

        self._insert_raw(
            conn=conn,
            series_id=series_id,
            obs_date=obs_date,
            value=contribution["amount"],
            raw_payload=contribution,
        )
        return True

    def _emit_signal(
        self,
        conn: Any,
        contribution: dict[str, Any],
    ) -> None:
        """Emit a signal_sources row for downstream trust scoring.

        Parameters:
            conn: Active database connection (within a transaction).
            contribution: Normalised contribution dict.
        """
        ticker = contribution.get("pac_ticker", "")
        if not ticker:
            return

        conn.execute(
            text(
                "INSERT INTO signal_sources "
                "(source_type, source_id, ticker, signal_date, signal_type, signal_value) "
                "VALUES (:stype, :sid, :ticker, :sdate, :stype2, :sval) "
                "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type) "
                "DO NOTHING"
            ),
            {
                "stype": "campaign_finance",
                "sid": contribution.get("committee_id", ""),
                "ticker": ticker,
                "sdate": contribution["obs_date"],
                "stype2": "PAC_CONTRIBUTION",
                "sval": json.dumps({
                    "pac_name": contribution.get("pac_name", ""),
                    "recipient_name": contribution.get("candidate_name", contribution.get("recipient_name", "")),
                    "amount": contribution["amount"],
                    "sector": contribution.get("pac_sector", ""),
                    "recipient_state": contribution.get("recipient_state", ""),
                }),
            },
        )

    # ── Public API ───────────────────────────────────────────────────────

    def pull_pac_contributions(self, cycle: int | None = None) -> dict[str, Any]:
        """Pull PAC contributions to candidates for all tracked PACs.

        Parameters:
            cycle: Election cycle year. Defaults to current.

        Returns:
            Summary dict with counts.
        """
        log.info("Pulling FEC PAC contributions for tracked PACs")

        total_fetched = 0
        stored = 0
        skipped_dup = 0

        with self.engine.begin() as conn:
            for committee_id, pac_info in TRACKED_PACS.items():
                log.info(
                    "Fetching contributions for {name} ({id})",
                    name=pac_info["name"], id=committee_id,
                )

                try:
                    contributions = self._fetch_pac_contributions(
                        committee_id=committee_id,
                        cycle=cycle,
                    )
                except Exception as exc:
                    log.warning(
                        "PAC fetch failed for {name}: {e}",
                        name=pac_info["name"], e=str(exc),
                    )
                    continue

                total_fetched += len(contributions)

                for contrib in contributions:
                    try:
                        was_stored = self._store_contribution(conn, contrib)
                        if was_stored:
                            stored += 1
                            self._emit_signal(conn, contrib)
                        else:
                            skipped_dup += 1
                    except Exception as exc:
                        log.warning(
                            "Failed to store FEC contribution: {e}",
                            e=str(exc),
                        )

        summary = {
            "status": "SUCCESS",
            "total_fetched": total_fetched,
            "stored": stored,
            "skipped_duplicate": skipped_dup,
            "pacs_queried": len(TRACKED_PACS),
        }
        log.info("FEC PAC contributions pull complete: {s}", s=summary)
        return summary

    def pull_individual_contributions(
        self,
        employers: list[str] | None = None,
        cycle: int | None = None,
    ) -> dict[str, Any]:
        """Pull individual contributions from executives at tracked companies.

        Parameters:
            employers: List of employer names to search. Defaults to major semi companies.
            cycle: Election cycle year.

        Returns:
            Summary dict with counts.
        """
        if employers is None:
            employers = [
                "NVIDIA", "Intel", "Qualcomm", "Broadcom",
                "Texas Instruments", "Micron Technology",
                "Applied Materials", "Lam Research",
                "Lockheed Martin", "Raytheon", "Boeing",
                "Palantir", "Microsoft", "Google", "Amazon",
            ]

        log.info("Pulling FEC individual contributions for {n} employers", n=len(employers))

        total_fetched = 0
        stored = 0

        with self.engine.begin() as conn:
            for employer in employers:
                try:
                    contributions = self._fetch_individual_contributions(
                        employer_name=employer,
                        cycle=cycle,
                    )
                except Exception as exc:
                    log.debug(
                        "Individual contribution fetch for {emp} failed: {e}",
                        emp=employer, e=str(exc),
                    )
                    continue

                total_fetched += len(contributions)

                for contrib in contributions:
                    try:
                        contrib_for_store = {
                            **contrib,
                            "pac_name": f"Individual-{employer}",
                        }
                        was_stored = self._store_contribution(conn, contrib_for_store)
                        if was_stored:
                            stored += 1
                    except Exception as exc:
                        log.debug("Store failed for individual contribution: {e}", e=str(exc))

        summary = {
            "status": "SUCCESS",
            "total_fetched": total_fetched,
            "stored": stored,
            "employers_queried": len(employers),
        }
        log.info("FEC individual contributions pull complete: {s}", s=summary)
        return summary

    def pull_all(self) -> dict[str, Any]:
        """Pull both PAC and individual contributions.

        Returns:
            Combined summary dict.
        """
        results: dict[str, Any] = {
            "status": "SUCCESS",
            "pac_contributions": {},
            "individual_contributions": {},
        }

        results["pac_contributions"] = self.pull_pac_contributions()
        results["individual_contributions"] = self.pull_individual_contributions()

        # Overall status
        statuses = [
            results["pac_contributions"].get("status", "FAILED"),
            results["individual_contributions"].get("status", "FAILED"),
        ]
        if any(s == "FAILED" for s in statuses):
            results["status"] = "PARTIAL"

        return results

    def pull_recent(self) -> dict[str, Any]:
        """Alias for pull_all — always incremental.

        Returns:
            Combined summary dict.
        """
        return self.pull_all()
