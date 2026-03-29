"""
GRID government contract tracker — USASpending.gov ingestion module.

Pulls significant federal contract awards (>$10M) from the USASpending.gov
public API (no key required), maps contractors to stock tickers, and stores
them as raw_series rows with full metadata for downstream intelligence.

Series pattern: GOV_CONTRACT:{agency}:{ticker}:{amount}
Emits signal_sources entries for trust scoring integration.

Scheduled: weekly pull (contracts update with some lag on USASpending).

Data source documentation:
  https://api.usaspending.gov/
  Endpoint: POST /api/v2/search/spending_by_award/
  Detail:   GET  /api/v2/awards/{award_id}/
"""

from __future__ import annotations

import json
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

_SEARCH_URL: str = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
_AWARD_DETAIL_URL: str = "https://api.usaspending.gov/api/v2/awards/{}/"

_REQUEST_TIMEOUT: int = 45
_RATE_LIMIT_DELAY: float = 0.5  # USASpending is generous but be polite
_MIN_CONTRACT_AMOUNT: float = 10_000_000.0  # $10M floor
_DEFAULT_LOOKBACK_DAYS: int = 7
_MAX_PAGES: int = 20  # Safety limit

# ── Contractor-to-Ticker Mapping ─────────────────────────────────────────

CONTRACTOR_TICKER_MAP: dict[str, str] = {
    # Defense primes
    "raytheon": "RTX", "rtx": "RTX", "raytheon technologies": "RTX",
    "lockheed martin": "LMT", "lockheed": "LMT",
    "boeing": "BA", "the boeing company": "BA",
    "northrop grumman": "NOC", "northrop": "NOC",
    "general dynamics": "GD",
    "l3harris": "LHX", "l3 harris": "LHX", "harris corporation": "LHX",
    "bae systems": "BAESY",
    "huntington ingalls": "HII",
    "textron": "TXT",

    # Defense IT / services
    "leidos": "LDOS", "leidos holdings": "LDOS",
    "booz allen": "BAH", "booz allen hamilton": "BAH",
    "saic": "SAIC", "science applications": "SAIC",
    "caci international": "CACI", "caci": "CACI",
    "mantech": "MANT", "mantech international": "MANT",
    "parsons": "PSN", "parsons corporation": "PSN",
    "kratos": "KTOS", "kratos defense": "KTOS",
    "maxar": "MAXR", "maxar technologies": "MAXR",
    "vectrus": "VEC",
    "perspecta": "PRSP",

    # Big tech with gov contracts
    "palantir": "PLTR", "palantir technologies": "PLTR",
    "microsoft": "MSFT", "microsoft corporation": "MSFT",
    "amazon": "AMZN", "aws": "AMZN", "amazon web services": "AMZN",
    "google": "GOOGL", "alphabet": "GOOGL", "google cloud": "GOOGL",
    "oracle": "ORCL", "oracle corporation": "ORCL",
    "ibm": "IBM", "international business machines": "IBM",
    "dell": "DELL", "dell technologies": "DELL",
    "cisco": "CSCO", "cisco systems": "CSCO",
    "salesforce": "CRM",
    "servicenow": "NOW",
    "snowflake": "SNOW",

    # Health / pharma (HHS, VA contracts)
    "pfizer": "PFE",
    "moderna": "MRNA",
    "johnson & johnson": "JNJ", "j&j": "JNJ", "janssen": "JNJ",
    "merck": "MRK", "merck & co": "MRK",
    "abbvie": "ABBV",
    "unitedhealth": "UNH", "unitedhealth group": "UNH", "optum": "UNH",
    "humana": "HUM",
    "centene": "CNC",
    "mckesson": "MCK",
    "cardinal health": "CAH",

    # Engineering / infrastructure
    "fluor": "FLR", "fluor corporation": "FLR",
    "jacobs": "J", "jacobs engineering": "J",
    "aecom": "ACM",
    "kbr": "KBR",

    # Vehicles / aerospace
    "general motors": "GM",
    "ford": "F", "ford motor": "F",
    "oshkosh": "OSK", "oshkosh defense": "OSK",
    "general electric": "GE", "ge aerospace": "GE",
    "honeywell": "HON",
    "rolls-royce": "RYCEY",

    # Semiconductors / chips
    "nvidia": "NVDA", "nvidia corporation": "NVDA",
    "tsmc": "TSM", "taiwan semiconductor": "TSM",
    "micron": "MU", "micron technology": "MU",
    "samsung semiconductor": "005930.KS",
    "globalfoundries": "GFS",
    "texas instruments": "TXN",
    "applied materials": "AMAT",
    "lam research": "LRCX",
    "kla": "KLAC", "kla corporation": "KLAC",
    "asml": "ASML",
    "qualcomm": "QCOM",
    "broadcom": "AVGO",
    "marvell": "MRVL", "marvell technology": "MRVL",
    "on semiconductor": "ON", "onsemi": "ON",
    "analog devices": "ADI",
    "synopsys": "SNPS",
    "cadence": "CDNS",
}


def _match_contractor_to_ticker(recipient_name: str) -> str | None:
    """Fuzzy-match a contract recipient name to a stock ticker.

    Tries exact substring matching against the CONTRACTOR_TICKER_MAP keys.
    Returns None if no match found.

    Parameters:
        recipient_name: Raw recipient name from USASpending.

    Returns:
        Ticker symbol or None.
    """
    if not recipient_name:
        return None
    name_lower = recipient_name.strip().lower()
    # Try longest keys first for specificity (e.g. "raytheon technologies" before "raytheon")
    for key in sorted(CONTRACTOR_TICKER_MAP, key=len, reverse=True):
        if key in name_lower:
            return CONTRACTOR_TICKER_MAP[key]
    return None


def _normalize_agency(agency_name: str) -> str:
    """Normalise agency name for use in series_id.

    Parameters:
        agency_name: Raw awarding agency name.

    Returns:
        Slug-style agency identifier.
    """
    if not agency_name:
        return "UNKNOWN"
    slug = agency_name.strip().upper()
    slug = re.sub(r"[^A-Z0-9 ]", "", slug)
    slug = re.sub(r"\s+", "_", slug.strip())
    # Truncate to keep series_id reasonable
    return slug[:40]


def _format_amount_tag(amount: float) -> str:
    """Format dollar amount as a compact tag for series_id.

    Parameters:
        amount: Dollar amount.

    Returns:
        e.g. '15M', '1.2B'
    """
    if amount >= 1_000_000_000:
        return f"{amount / 1_000_000_000:.1f}B"
    return f"{amount / 1_000_000:.0f}M"


# ── Puller Class ─────────────────────────────────────────────────────────


class GovContractsPuller(BasePuller):
    """Pulls federal contract awards from USASpending.gov.

    Fetches recent contract awards above the $10M threshold, maps
    recipients to stock tickers, and stores each award as a raw_series
    row with full metadata. Also emits signal_sources entries for
    downstream trust scoring.

    Series pattern: GOV_CONTRACT:{agency_slug}:{ticker}:{amount_tag}
    Value: contract obligated amount in USD.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for USASPENDING_GOV.
    """

    SOURCE_NAME: str = "USASPENDING_GOV"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://api.usaspending.gov/api/v2/",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 30,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the government contracts puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info(
            "GovContractsPuller initialised — source_id={sid}",
            sid=self.source_id,
        )

    # ── Public API ───────────────────────────────────────────────────────

    def pull_all(self, days_back: int = _DEFAULT_LOOKBACK_DAYS) -> dict[str, Any]:
        """Pull recent significant contract awards.

        Parameters:
            days_back: Number of days to look back (default: 7).

        Returns:
            Summary dict with counts of contracts found, stored, and mapped.
        """
        log.info("Pulling gov contracts — last {d} days, min ${m:,.0f}",
                 d=days_back, m=_MIN_CONTRACT_AMOUNT)

        awards = self._fetch_awards(days_back)
        log.info("Fetched {n} awards above ${m:,.0f}", n=len(awards), m=_MIN_CONTRACT_AMOUNT)

        stored = 0
        mapped = 0
        skipped_dup = 0

        with self.engine.begin() as conn:
            for award in awards:
                ticker = _match_contractor_to_ticker(award.get("recipient_name", ""))
                if ticker:
                    mapped += 1

                try:
                    was_stored = self._store_award(conn, award, ticker)
                    if was_stored:
                        stored += 1
                        if ticker:
                            self._emit_signal(conn, award, ticker)
                    else:
                        skipped_dup += 1
                except Exception as exc:
                    log.warning(
                        "Failed to store award {id}: {err}",
                        id=award.get("award_id", "?"),
                        err=str(exc),
                    )

        summary = {
            "total_fetched": len(awards),
            "stored": stored,
            "mapped_to_ticker": mapped,
            "skipped_duplicate": skipped_dup,
            "days_back": days_back,
        }
        log.info("Gov contracts pull complete: {s}", s=summary)
        return summary

    # ── Data Fetching ────────────────────────────────────────────────────

    @retry_on_failure(max_attempts=3, backoff=2.0)
    def _fetch_awards(self, days_back: int) -> list[dict[str, Any]]:
        """Fetch contract awards from USASpending.gov search API.

        Uses POST /api/v2/search/spending_by_award/ with date and amount
        filters. Paginates through all results.

        Parameters:
            days_back: Number of days to look back.

        Returns:
            List of normalised award dicts.
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=days_back)

        all_awards: list[dict[str, Any]] = []
        page = 1

        while page <= _MAX_PAGES:
            payload = {
                "filters": {
                    "time_period": [
                        {
                            "start_date": start_date.isoformat(),
                            "end_date": end_date.isoformat(),
                        }
                    ],
                    "award_type_codes": ["A", "B", "C", "D"],  # Contracts only
                    "award_amounts": [
                        {
                            "lower_bound": _MIN_CONTRACT_AMOUNT,
                        }
                    ],
                },
                "fields": [
                    "Award ID",
                    "Recipient Name",
                    "Award Amount",
                    "Total Outlays",
                    "Description",
                    "Start Date",
                    "End Date",
                    "Awarding Agency",
                    "Awarding Sub Agency",
                    "Contract Award Type",
                    "NAICS Code",
                    "NAICS Description",
                ],
                "type": "contracts",
                "page": page,
                "limit": 100,
                "sort": "Award Amount",
                "order": "desc",
                "subawards": False,
            }

            resp = requests.post(
                _SEARCH_URL,
                json=payload,
                timeout=_REQUEST_TIMEOUT,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            data = resp.json()

            results = data.get("results", [])
            if not results:
                break

            for r in results:
                award = self._normalize_award(r)
                if award:
                    all_awards.append(award)

            # Check if more pages exist
            has_next = data.get("page_metadata", {}).get("hasNext", False)
            if not has_next:
                break

            page += 1
            time.sleep(_RATE_LIMIT_DELAY)

        return all_awards

    def _normalize_award(self, raw: dict[str, Any]) -> dict[str, Any] | None:
        """Normalise a raw USASpending award result into a clean dict.

        Parameters:
            raw: Raw result dict from the API.

        Returns:
            Normalised award dict, or None if data is unusable.
        """
        try:
            amount = float(raw.get("Award Amount", 0) or 0)
        except (ValueError, TypeError):
            return None

        if amount < _MIN_CONTRACT_AMOUNT:
            return None

        award_id = raw.get("Award ID", "")
        if not award_id:
            return None

        # Parse start date
        start_str = raw.get("Start Date", "")
        try:
            award_date = date.fromisoformat(start_str) if start_str else date.today()
        except ValueError:
            award_date = date.today()

        return {
            "award_id": str(award_id).strip(),
            "recipient_name": (raw.get("Recipient Name") or "").strip(),
            "recipient_id": None,  # Not available in search results
            "amount": amount,
            "total_outlays": float(raw.get("Total Outlays", 0) or 0),
            "description": (raw.get("Description") or "").strip()[:500],
            "award_date": award_date,
            "end_date": raw.get("End Date", ""),
            "awarding_agency": (raw.get("Awarding Agency") or "").strip(),
            "awarding_sub_agency": (raw.get("Awarding Sub Agency") or "").strip(),
            "contract_type": (raw.get("Contract Award Type") or "").strip(),
            "naics_code": (raw.get("NAICS Code") or "").strip(),
            "naics_description": (raw.get("NAICS Description") or "").strip(),
            "internal_id": None,  # Not available in search results; use detail endpoint
        }

    # ── Detail Fetch (optional enrichment) ───────────────────────────────

    @retry_on_failure(max_attempts=2, backoff=1.5)
    def fetch_award_detail(self, internal_id: str) -> dict[str, Any] | None:
        """Fetch detailed info for a single award by internal ID.

        Parameters:
            internal_id: USASpending generated_internal_id.

        Returns:
            Detail dict or None on failure.
        """
        if not internal_id:
            return None
        url = _AWARD_DETAIL_URL.format(internal_id)
        resp = requests.get(url, timeout=_REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp.json()
        log.debug("Award detail fetch failed for {id}: HTTP {s}",
                  id=internal_id, s=resp.status_code)
        return None

    # ── Storage ──────────────────────────────────────────────────────────

    def _store_award(
        self,
        conn: Any,
        award: dict[str, Any],
        ticker: str | None,
    ) -> bool:
        """Store a contract award as a raw_series row.

        Series ID pattern: GOV_CONTRACT:{agency}:{ticker_or_UNMAPPED}:{amount_tag}

        Parameters:
            conn: Active database connection (within a transaction).
            award: Normalised award dict.
            ticker: Mapped ticker or None.

        Returns:
            True if stored, False if duplicate.
        """
        agency_slug = _normalize_agency(award["awarding_agency"])
        ticker_tag = ticker or "UNMAPPED"
        amount_tag = _format_amount_tag(award["amount"])

        series_id = f"GOV_CONTRACT:{agency_slug}:{ticker_tag}:{amount_tag}"

        obs_date = award["award_date"]

        # Dedup by award_id in payload — check if this exact award exists
        if self._row_exists(series_id, obs_date, conn, dedup_hours=168):  # 7 days
            return False

        payload = {
            "award_id": award["award_id"],
            "recipient_name": award["recipient_name"],
            "recipient_id": award.get("recipient_id"),
            "amount": award["amount"],
            "total_outlays": award.get("total_outlays", 0),
            "description": award["description"],
            "award_date": obs_date.isoformat(),
            "end_date": award.get("end_date", ""),
            "awarding_agency": award["awarding_agency"],
            "awarding_sub_agency": award.get("awarding_sub_agency", ""),
            "contract_type": award.get("contract_type", ""),
            "naics_code": award.get("naics_code", ""),
            "naics_description": award.get("naics_description", ""),
            "ticker": ticker,
        }

        self._insert_raw(
            conn=conn,
            series_id=series_id,
            obs_date=obs_date,
            value=award["amount"],
            raw_payload=payload,
            pull_status="SUCCESS",
        )
        return True

    def _emit_signal(
        self,
        conn: Any,
        award: dict[str, Any],
        ticker: str,
    ) -> None:
        """Emit a signal_sources row for downstream trust scoring.

        Only called for awards that mapped to a ticker.

        Parameters:
            conn: Active database connection (within a transaction).
            award: Normalised award dict.
            ticker: Resolved stock ticker.
        """
        conn.execute(
            text(
                "INSERT INTO signal_sources "
                "(source_type, source_id, ticker, signal_date, signal_type, signal_value) "
                "VALUES (:stype, :sid, :ticker, :sdate, :stype2, :sval) "
                "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type) "
                "DO NOTHING"
            ),
            {
                "stype": "gov_contract",
                "sid": award["awarding_agency"],
                "ticker": ticker,
                "sdate": award["award_date"],
                "stype2": "CONTRACT_AWARD",
                "sval": json.dumps({
                    "award_id": award["award_id"],
                    "amount": award["amount"],
                    "recipient_name": award["recipient_name"],
                    "description": award["description"][:200],
                    "naics_code": award.get("naics_code", ""),
                    "contract_type": award.get("contract_type", ""),
                }),
            },
        )
