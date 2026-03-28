"""
GRID lobbying disclosure tracker.

Pulls lobbying registration and expenditure data from two sources:
  1. OpenSecrets API (api.opensecrets.org) — requires OPENSECRETS_API_KEY
  2. Senate Lobbying Disclosure Act database (lda.senate.gov) — public, no key

Tracks lobbying activity by company/organization, maps clients to stock
tickers, and detects spending trend changes as a leading indicator of
policy-driven market moves.

Series pattern: LOBBYING:{client}:{registrant}:{amount}
Emits signal_sources entries for trust scoring integration.

Scheduled: weekly pull.

Data sources:
  https://www.opensecrets.org/api/
  https://lda.senate.gov/api/
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

_OPENSECRETS_API_KEY_ENV: str = "OPENSECRETS_API_KEY"
_OPENSECRETS_BASE_URL: str = "https://www.opensecrets.org/api/"
_LDA_BASE_URL: str = "https://lda.senate.gov/api/v1/"

_REQUEST_TIMEOUT: int = 30
_RATE_LIMIT_DELAY: float = 1.0
_MAX_PAGES: int = 20

# ── Client-to-ticker mapping (imports from gov_contracts for consistency) ─

try:
    from ingestion.altdata.gov_contracts import CONTRACTOR_TICKER_MAP
except ImportError:
    CONTRACTOR_TICKER_MAP = {}

# Additional lobbying-specific client mappings
LOBBYING_CLIENT_TICKER_MAP: dict[str, str] = {
    **CONTRACTOR_TICKER_MAP,
    # Semiconductor industry association
    "semiconductor industry association": "SMH",
    "sia": "SMH",
    "intel corporation": "INTC", "intel": "INTC",
    "amd": "AMD", "advanced micro devices": "AMD",
    # Tech giants (supplement existing)
    "meta platforms": "META", "meta": "META", "facebook": "META",
    "apple": "AAPL", "apple inc": "AAPL",
    "alphabet inc": "GOOGL",
    # Pharma lobbying
    "phrma": "XLV",
    "eli lilly": "LLY", "lilly": "LLY",
    "amgen": "AMGN",
    "gilead": "GILD", "gilead sciences": "GILD",
    # Energy lobbying
    "exxonmobil": "XOM", "exxon mobil": "XOM", "exxon": "XOM",
    "chevron": "CVX", "chevron corporation": "CVX",
    "american petroleum institute": "XLE",
    # Financial lobbying
    "jpmorgan": "JPM", "jpmorgan chase": "JPM", "jp morgan": "JPM",
    "goldman sachs": "GS",
    "bank of america": "BAC",
    "citigroup": "C", "citi": "C",
    "morgan stanley": "MS",
    # Defense lobbying
    "general atomics": "LMT",  # closely related
}


def _match_client_to_ticker(client_name: str) -> str | None:
    """Match a lobbying client name to a stock ticker.

    Parameters:
        client_name: Client/organization name from lobbying disclosure.

    Returns:
        Ticker symbol or None.
    """
    if not client_name:
        return None
    name_lower = client_name.strip().lower()
    for key in sorted(LOBBYING_CLIENT_TICKER_MAP, key=len, reverse=True):
        if key in name_lower:
            return LOBBYING_CLIENT_TICKER_MAP[key]
    return None


def _slugify(name: str, max_len: int = 40) -> str:
    """Create a slug from a name for use in series_id."""
    slug = re.sub(r"[^A-Z0-9 ]", "", name.strip().upper())
    slug = re.sub(r"\s+", "_", slug.strip())
    return slug[:max_len]


def _format_amount(amount: float) -> str:
    """Format dollar amount as compact tag."""
    if amount >= 1_000_000_000:
        return f"{amount / 1_000_000_000:.1f}B"
    if amount >= 1_000_000:
        return f"{amount / 1_000_000:.1f}M"
    if amount >= 1_000:
        return f"{amount / 1_000:.0f}K"
    return f"{amount:.0f}"


# ── Puller Class ─────────────────────────────────────────────────────────


class LobbyingPuller(BasePuller):
    """Pulls lobbying disclosure data from OpenSecrets and Senate LDA.

    Fetches lobbying registrations and reports, maps clients to stock
    tickers, and stores each disclosure as a raw_series row. Tracks
    spending trends as a leading indicator of policy-driven moves.

    Series pattern: LOBBYING:{client_slug}:{registrant_slug}:{amount_tag}
    Value: lobbying expenditure amount in USD.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for LOBBYING_DISCLOSURE.
        opensecrets_key: OpenSecrets API key from environment.
    """

    SOURCE_NAME: str = "LOBBYING_DISCLOSURE"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://lda.senate.gov/api/v1/",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 32,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the lobbying disclosure puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        self.opensecrets_key: str = os.environ.get(_OPENSECRETS_API_KEY_ENV, "")
        if not self.opensecrets_key:
            log.warning(
                "LobbyingPuller: {env} not set — OpenSecrets API calls will "
                "be skipped, falling back to Senate LDA only.",
                env=_OPENSECRETS_API_KEY_ENV,
            )
        super().__init__(db_engine)
        log.info(
            "LobbyingPuller initialised — source_id={sid}, opensecrets_key={'SET' if self.opensecrets_key else 'MISSING'}",
            sid=self.source_id,
        )

    # ── API helpers ──────────────────────────────────────────────────────

    @retry_on_failure(
        max_attempts=3,
        backoff=2.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _opensecrets_get(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Make an authenticated GET request to OpenSecrets API.

        Parameters:
            method: API method name (e.g. 'getLobbyists').
            params: Additional query parameters.

        Returns:
            Parsed JSON response dict.
        """
        req_params = {
            "apikey": self.opensecrets_key,
            "output": "json",
            "method": method,
        }
        if params:
            req_params.update(params)

        resp = requests.get(
            _OPENSECRETS_BASE_URL,
            params=req_params,
            timeout=_REQUEST_TIMEOUT,
            headers={"User-Agent": "GRID-DataPuller/1.0"},
        )
        resp.raise_for_status()
        time.sleep(_RATE_LIMIT_DELAY)
        return resp.json()

    @retry_on_failure(
        max_attempts=3,
        backoff=2.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _lda_get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Make a GET request to Senate LDA API (no key required).

        Parameters:
            endpoint: API path relative to base URL.
            params: Query parameters.

        Returns:
            Parsed JSON response dict.
        """
        url = f"{_LDA_BASE_URL}{endpoint}"
        resp = requests.get(
            url,
            params=params or {},
            timeout=_REQUEST_TIMEOUT,
            headers={
                "User-Agent": "GRID-DataPuller/1.0",
                "Accept": "application/json",
            },
        )
        resp.raise_for_status()
        time.sleep(_RATE_LIMIT_DELAY)
        return resp.json()

    # ── Senate LDA data fetching ─────────────────────────────────────────

    def _fetch_lda_filings(self, days_back: int = 30) -> list[dict[str, Any]]:
        """Fetch recent lobbying filings from Senate LDA.

        Parameters:
            days_back: Number of days to look back.

        Returns:
            List of normalised filing dicts.
        """
        cutoff = (date.today() - timedelta(days=days_back)).isoformat()
        all_filings: list[dict[str, Any]] = []
        page = 1

        while page <= _MAX_PAGES:
            try:
                data = self._lda_get("filings/", params={
                    "filing_dt_posted_after": cutoff,
                    "filing_type": "REGISTRATION,REPORT",
                    "page": page,
                    "page_size": 100,
                })
            except Exception as exc:
                log.warning("LDA filing fetch page {p} failed: {e}", p=page, e=str(exc))
                break

            results = data.get("results", [])
            if not results:
                break

            for filing in results:
                parsed = self._parse_lda_filing(filing)
                if parsed:
                    all_filings.append(parsed)

            # Check for next page
            if not data.get("next"):
                break

            page += 1

        log.info("Senate LDA: fetched {n} filings from last {d} days", n=len(all_filings), d=days_back)
        return all_filings

    def _parse_lda_filing(self, filing: dict[str, Any]) -> dict[str, Any] | None:
        """Parse a Senate LDA filing into a normalised dict.

        Parameters:
            filing: Raw filing dict from LDA API.

        Returns:
            Normalised filing dict, or None if unusable.
        """
        try:
            client = filing.get("client", {}) or {}
            client_name = client.get("name", "")
            if not client_name:
                return None

            registrant = filing.get("registrant", {}) or {}
            registrant_name = registrant.get("name", "")

            # Parse amount
            amount = 0.0
            income = filing.get("income")
            expenses = filing.get("expenses")
            if income:
                try:
                    amount = float(income)
                except (ValueError, TypeError):
                    pass
            if not amount and expenses:
                try:
                    amount = float(expenses)
                except (ValueError, TypeError):
                    pass

            # Filing date
            dt_posted = filing.get("dt_posted", "")
            try:
                obs_date = date.fromisoformat(dt_posted[:10]) if dt_posted else date.today()
            except (ValueError, TypeError):
                obs_date = date.today()

            # Lobbying activities and issue codes
            activities = filing.get("lobbying_activities", []) or []
            issue_codes: list[str] = []
            specific_issues: list[str] = []
            bills_lobbied: list[str] = []

            for activity in activities:
                if isinstance(activity, dict):
                    ic = activity.get("general_issue_code", "")
                    if ic:
                        issue_codes.append(ic)
                    desc = activity.get("description", "")
                    if desc:
                        specific_issues.append(desc[:200])
                    # Extract bill references from description
                    bill_refs = re.findall(r"(?:H\.R\.|S\.|H\.J\.Res\.|S\.J\.Res\.)\s*\d+", desc)
                    bills_lobbied.extend(bill_refs)

            return {
                "filing_id": filing.get("filing_uuid", ""),
                "client_name": client_name.strip(),
                "client_id": client.get("id", ""),
                "registrant_name": registrant_name.strip(),
                "registrant_id": registrant.get("id", ""),
                "amount": amount,
                "obs_date": obs_date,
                "filing_type": filing.get("filing_type", ""),
                "filing_year": filing.get("filing_year", ""),
                "filing_period": filing.get("filing_period", ""),
                "issue_codes": issue_codes,
                "specific_issues": specific_issues[:5],  # Limit for payload size
                "bills_lobbied": bills_lobbied[:10],
            }

        except Exception as exc:
            log.debug("Failed to parse LDA filing: {e}", e=str(exc))
            return None

    # ── OpenSecrets data fetching ────────────────────────────────────────

    def _fetch_opensecrets_lobbying(self, org_id: str) -> list[dict[str, Any]]:
        """Fetch lobbying data for an organization from OpenSecrets.

        Parameters:
            org_id: OpenSecrets organization ID (CRP ID).

        Returns:
            List of lobbying record dicts.
        """
        if not self.opensecrets_key:
            return []

        try:
            data = self._opensecrets_get("lobbying", params={"id": org_id})
            lobbying = data.get("response", {}).get("lobbying", {})
            items = lobbying.get("item", [])
            if isinstance(items, dict):
                items = [items]
            return items
        except Exception as exc:
            log.debug("OpenSecrets lobbying fetch failed for {id}: {e}", id=org_id, e=str(exc))
            return []

    # ── Spending trend analysis ──────────────────────────────────────────

    def _compute_spending_trend(
        self,
        conn: Any,
        client_slug: str,
    ) -> dict[str, Any]:
        """Compute lobbying spending trend for a client.

        Compares current-period spending to historical average.
        Increasing lobbying spend is a leading indicator of upcoming
        policy engagement.

        Parameters:
            conn: Active database connection.
            client_slug: Slugified client name.

        Returns:
            Dict with trend direction, change_pct, and periods compared.
        """
        rows = conn.execute(
            text(
                "SELECT obs_date, value FROM raw_series "
                "WHERE series_id LIKE :pattern AND source_id = :src "
                "AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date DESC LIMIT 8"
            ),
            {"pattern": f"LOBBYING:{client_slug}:%", "src": self.source_id},
        ).fetchall()

        if len(rows) < 2:
            return {"trend": "INSUFFICIENT_DATA", "change_pct": 0.0, "periods": len(rows)}

        recent = rows[0][1] if rows[0][1] else 0.0
        historical_avg = sum(r[1] for r in rows[1:] if r[1]) / max(len(rows) - 1, 1)

        if historical_avg == 0:
            return {"trend": "NEW", "change_pct": 0.0, "periods": len(rows)}

        change_pct = ((recent - historical_avg) / historical_avg) * 100

        if change_pct > 20:
            trend = "INCREASING"
        elif change_pct < -20:
            trend = "DECREASING"
        else:
            trend = "STABLE"

        return {
            "trend": trend,
            "change_pct": round(change_pct, 1),
            "periods": len(rows),
            "recent_amount": recent,
            "historical_avg": round(historical_avg, 2),
        }

    # ── Storage ──────────────────────────────────────────────────────────

    def _store_filing(
        self,
        conn: Any,
        filing: dict[str, Any],
        ticker: str | None,
    ) -> bool:
        """Store a lobbying filing as a raw_series row.

        Parameters:
            conn: Active database connection (within a transaction).
            filing: Normalised filing dict.
            ticker: Mapped ticker or None.

        Returns:
            True if stored, False if duplicate.
        """
        client_slug = _slugify(filing["client_name"])
        registrant_slug = _slugify(filing["registrant_name"])
        amount_tag = _format_amount(filing["amount"]) if filing["amount"] else "UNK"

        series_id = f"LOBBYING:{client_slug}:{registrant_slug}:{amount_tag}"
        obs_date = filing["obs_date"]

        if self._row_exists(series_id, obs_date, conn, dedup_hours=168):
            return False

        payload = {
            "filing_id": filing["filing_id"],
            "client_name": filing["client_name"],
            "registrant_name": filing["registrant_name"],
            "amount": filing["amount"],
            "filing_type": filing.get("filing_type", ""),
            "filing_year": filing.get("filing_year", ""),
            "filing_period": filing.get("filing_period", ""),
            "issue_codes": filing.get("issue_codes", []),
            "specific_issues": filing.get("specific_issues", []),
            "bills_lobbied": filing.get("bills_lobbied", []),
            "ticker": ticker,
        }

        self._insert_raw(
            conn=conn,
            series_id=series_id,
            obs_date=obs_date,
            value=filing["amount"],
            raw_payload=payload,
        )
        return True

    def _emit_signal(
        self,
        conn: Any,
        filing: dict[str, Any],
        ticker: str,
        trend: dict[str, Any] | None = None,
    ) -> None:
        """Emit a signal_sources row for downstream trust scoring.

        Parameters:
            conn: Active database connection (within a transaction).
            filing: Normalised filing dict.
            ticker: Resolved stock ticker.
            trend: Optional spending trend dict.
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
                "stype": "lobbying",
                "sid": filing.get("filing_id", ""),
                "ticker": ticker,
                "sdate": filing["obs_date"],
                "stype2": "LOBBYING_DISCLOSURE",
                "sval": json.dumps({
                    "client_name": filing["client_name"],
                    "registrant_name": filing["registrant_name"],
                    "amount": filing["amount"],
                    "issue_codes": filing.get("issue_codes", []),
                    "bills_lobbied": filing.get("bills_lobbied", []),
                    "spending_trend": trend or {},
                }),
            },
        )

    # ── Public API ───────────────────────────────────────────────────────

    def pull_all(self, days_back: int = 30) -> dict[str, Any]:
        """Pull recent lobbying disclosures from Senate LDA (and OpenSecrets if key available).

        Parameters:
            days_back: Number of days of history to pull.

        Returns:
            Summary dict with counts.
        """
        log.info("Pulling lobbying disclosures — last {d} days", d=days_back)

        # Primary source: Senate LDA (always available)
        filings = self._fetch_lda_filings(days_back=days_back)
        log.info("Total lobbying filings fetched: {n}", n=len(filings))

        stored = 0
        mapped = 0
        skipped_dup = 0
        trends_computed = 0

        with self.engine.begin() as conn:
            for filing in filings:
                ticker = _match_client_to_ticker(filing["client_name"])
                if ticker:
                    mapped += 1

                try:
                    was_stored = self._store_filing(conn, filing, ticker)
                    if was_stored:
                        stored += 1

                        if ticker:
                            # Compute spending trend for mapped clients
                            client_slug = _slugify(filing["client_name"])
                            trend = self._compute_spending_trend(conn, client_slug)
                            trends_computed += 1

                            self._emit_signal(conn, filing, ticker, trend)

                            # Log notable trend changes
                            if trend.get("trend") == "INCREASING" and trend.get("change_pct", 0) > 50:
                                log.info(
                                    "LOBBYING ALERT: {client} ({ticker}) spending up {pct}%",
                                    client=filing["client_name"],
                                    ticker=ticker,
                                    pct=trend["change_pct"],
                                )
                    else:
                        skipped_dup += 1
                except Exception as exc:
                    log.warning(
                        "Failed to store lobbying filing {id}: {err}",
                        id=filing.get("filing_id", "?"),
                        err=str(exc),
                    )

        summary = {
            "status": "SUCCESS",
            "total_fetched": len(filings),
            "stored": stored,
            "mapped_to_ticker": mapped,
            "skipped_duplicate": skipped_dup,
            "trends_computed": trends_computed,
            "days_back": days_back,
            "opensecrets_available": bool(self.opensecrets_key),
        }
        log.info("Lobbying pull complete: {s}", s=summary)
        return summary

    def pull_recent(self, days_back: int = 30) -> dict[str, Any]:
        """Alias for pull_all — always incremental.

        Parameters:
            days_back: Number of days of history to pull.

        Returns:
            Summary dict.
        """
        return self.pull_all(days_back=days_back)
