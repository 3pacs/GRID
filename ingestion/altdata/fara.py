"""
GRID FARA (Foreign Agent Registration Act) ingestion module.

Pulls foreign lobbying registrations and activity reports from the
U.S. Department of Justice FARA database.

FARA requires agents of foreign principals (governments, political
parties, foreign companies) to register with DOJ and disclose:
  - Who the foreign principal is (e.g., Saudi Arabia, UAE, China)
  - How much they're being paid
  - What activities they're performing (lobbying Congress, PR, media)
  - Which specific policy issues they're working on

This is criminally underused signal — it literally documents who foreign
governments are paying to influence US financial and trade policy, with
dollar amounts and specific targets.

Data sources:
  1. DOJ FARA eFile API (efile.fara.gov/api/v1) — ``Registrants/json/Active``
     (every active registrant, one call) and
     ``ForeignPrincipals/json/Active/{registration_number}`` (per registrant)
  2. FARA quick search (fara.gov/quick-search) — supplementary

Series pattern: FARA:{principal_country}:{registrant}:{activity_type}
Emits signal_sources entries for trust scoring integration.

Scheduled: weekly pull.

Honest outcomes (2026-09-29 write contract, fixed 2026-11-05): the
``Registrants/search``, ``Activities/search`` and ``Documents/search``
endpoints this module was written against return HTTP 404 (grid-hermes
journal 2026-11-05 10:56Z; anonymous probe 2026-11-05 11:3xZ), and the
fetchers swallowed that as "0 rows, SUCCESS" while the registrant summary
never reported a committed write count, so every run was recorded FAILED
"puller did not report committed write count". ``pull_all`` now reads the
live ``json/Active`` registrant list, looks up foreign principals for the
registrants registered inside ``days_back``, and returns ``status`` plus the
committed ``rows_inserted``. A fetch failure is FAILED with its reason, not
an empty success. The public API has no activity-report endpoint, so
``pull_all`` reports activities as unavailable instead of pretending to
check them.
"""

from __future__ import annotations

import json
import re
import time
from datetime import date, datetime, timedelta
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ── API Configuration ────────────────────────────────────────────────────

_FARA_EFILE_BASE: str = "https://efile.fara.gov/api/v1"
# Live (probed 2026-11-05: 200 application/json, 538 active registrants).
_FARA_ACTIVE_REGISTRANTS_URL: str = f"{_FARA_EFILE_BASE}/Registrants/json/Active"
_FARA_PRINCIPALS_URL: str = f"{_FARA_EFILE_BASE}/ForeignPrincipals/json/Active/"
# Dead: all three return HTTP 404 (probed 2026-11-05). Kept for the
# activity/document helpers, which now report that failure honestly.
_FARA_SEARCH_URL: str = "https://efile.fara.gov/api/v1/Registrants/search"
_FARA_ACTIVITIES_URL: str = "https://efile.fara.gov/api/v1/Activities/search"
_FARA_DOCUMENTS_URL: str = "https://efile.fara.gov/api/v1/Documents/search"

_ACTIVITIES_UNAVAILABLE: str = (
    "FARA eFile API has no activity-report endpoint (Activities/search "
    "returns HTTP 404); activities are not pulled"
)

_REQUEST_TIMEOUT: int = 30
# Stay under the eFile API's 5-requests-per-10-seconds limit.
_RATE_LIMIT_DELAY: float = 2.0
_MAX_PAGES: int = 20
# Foreign-principal lookups are one call each; bound a run (30 days held 14
# new registrants on 2026-11-05). Registrants past the cap are reported as
# unattempted, so the run is PARTIAL rather than silently truncated.
_MAX_PRINCIPAL_LOOKUPS: int = 40


class FaraFetchError(RuntimeError):
    """A FARA eFile request failed or returned an unexpected shape."""


def _rowset(payload: Any, outer: str) -> list[dict[str, Any]]:
    """Rows of an eFile ``{outer: {"ROW": row | [rows]}}`` envelope.

    A missing ``outer`` key is a schema change, not an empty list.
    """
    if not isinstance(payload, dict) or outer not in payload:
        raise FaraFetchError(f"unexpected response shape (no {outer})")
    node = payload[outer]
    rows = node.get("ROW") if isinstance(node, dict) else None
    if rows is None:
        return []
    return [r for r in (rows if isinstance(rows, list) else [rows]) if isinstance(r, dict)]

# ── Country-to-sector/ticker mapping ────────────────────────────────────
# Foreign principals from these countries often lobby on sectors
# that directly affect specific market segments.

COUNTRY_SECTOR_MAP: dict[str, dict[str, str]] = {
    # Oil-producing states → energy policy
    "SAUDI ARABIA": {"sector": "energy", "ticker": "XLE", "theme": "oil_policy"},
    "UNITED ARAB EMIRATES": {"sector": "energy", "ticker": "XLE", "theme": "oil_policy"},
    "QATAR": {"sector": "energy", "ticker": "XLE", "theme": "lng_policy"},
    "KUWAIT": {"sector": "energy", "ticker": "XLE", "theme": "oil_policy"},
    "RUSSIA": {"sector": "energy", "ticker": "XLE", "theme": "sanctions"},
    # Tech/trade rivals → semiconductor, tech, trade policy
    "CHINA": {"sector": "tech", "ticker": "SMH", "theme": "trade_war"},
    "TAIWAN": {"sector": "semiconductor", "ticker": "SMH", "theme": "chips_act"},
    "SOUTH KOREA": {"sector": "semiconductor", "ticker": "SMH", "theme": "trade"},
    "JAPAN": {"sector": "auto", "ticker": "XLI", "theme": "trade"},
    # Defense/geopolitical
    "ISRAEL": {"sector": "defense", "ticker": "XLI", "theme": "defense_aid"},
    "TURKEY": {"sector": "defense", "ticker": "LMT", "theme": "defense_sales"},
    "UKRAINE": {"sector": "defense", "ticker": "LMT", "theme": "military_aid"},
    # Financial centers
    "SWITZERLAND": {"sector": "finance", "ticker": "XLF", "theme": "banking_regulation"},
    "UNITED KINGDOM": {"sector": "finance", "ticker": "XLF", "theme": "financial_regulation"},
    "CAYMAN ISLANDS": {"sector": "finance", "ticker": "XLF", "theme": "offshore_regulation"},
    # Pharma/health
    "INDIA": {"sector": "pharma", "ticker": "XLV", "theme": "generic_drugs"},
    "GERMANY": {"sector": "pharma", "ticker": "XLV", "theme": "pharma_regulation"},
}

# Activity types that signal market-relevant lobbying
MARKET_RELEVANT_ACTIVITIES: set[str] = {
    "LOBBYING",
    "POLITICAL ACTIVITIES",
    "PUBLIC RELATIONS",
    "GOVERNMENT AFFAIRS",
    "POLITICAL CONSULTING",
    "LEGISLATIVE CONSULTING",
}

# Issue keywords that map to sectors (for activity description parsing)
ISSUE_SECTOR_KEYWORDS: dict[str, str] = {
    "tariff": "XLI",
    "trade": "XLI",
    "semiconductor": "SMH",
    "chip": "SMH",
    "defense": "ITA",
    "military": "ITA",
    "arms": "ITA",
    "oil": "XLE",
    "energy": "XLE",
    "natural gas": "XLE",
    "lng": "XLE",
    "sanctions": "SPY",
    "banking": "XLF",
    "financial": "XLF",
    "pharma": "XLV",
    "drug": "XLV",
    "health": "XLV",
    "agriculture": "DBA",
    "farm": "DBA",
    "technology": "XLK",
    "cyber": "XLK",
    "telecom": "XLC",
    "aviation": "XLI",
    "shipping": "XLI",
    "nuclear": "XLE",
    "rare earth": "SMH",
    "mineral": "XLB",
    "steel": "XLB",
    "aluminum": "XLB",
}


def _slugify(name: str, max_len: int = 50) -> str:
    """Create a slug from a name for use in series_id."""
    slug = re.sub(r"[^A-Z0-9 ]", "", name.strip().upper())
    slug = re.sub(r"\s+", "_", slug.strip())
    return slug[:max_len]


def _extract_sector_from_description(description: str) -> str | None:
    """Extract likely sector ticker from activity description text.

    Parameters:
        description: Activity or issue description text.

    Returns:
        Ticker symbol or None.
    """
    if not description:
        return None
    desc_lower = description.lower()
    for keyword, ticker in ISSUE_SECTOR_KEYWORDS.items():
        if keyword in desc_lower:
            return ticker
    return None


def _parse_fara_date(date_str: str | None) -> date | None:
    """Parse various FARA date formats.

    Parameters:
        date_str: Date string from FARA API.

    Returns:
        Parsed date or None.
    """
    if not date_str:
        return None
    try:
        # FARA uses ISO format or MM/DD/YYYY
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y"):
            try:
                return datetime.strptime(date_str[:19], fmt).date()
            except ValueError:
                continue
        return date.fromisoformat(date_str[:10])
    except (ValueError, TypeError):
        return None


class FARAPuller(BasePuller):
    """Pulls Foreign Agent Registration Act data from DOJ.

    Fetches registrant profiles, foreign principal details, and
    lobbying activity reports. Maps foreign principals to countries
    and sectors to identify policy-driven market signals.

    Series pattern: FARA:{country_slug}:{registrant_slug}:{activity_type}
    Value: compensation amount in USD (or 0 if undisclosed).

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for FARA_DOJ.
    """

    SOURCE_NAME: str = "FARA_DOJ"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://efile.fara.gov/api/v1",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 30,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the FARA puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info(
            "FARAPuller initialised — source_id={sid}",
            sid=self.source_id,
        )

    # ── API helpers ──────────────────────────────────────────────────────

    @retry_on_failure(
        max_attempts=3,
        backoff=2.0,
        retryable_exceptions=(
            ConnectionError, TimeoutError, OSError, requests.RequestException,
        ),
    )
    def _fara_get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Make a GET request to the FARA eFile API.

        Parameters:
            url: Full URL or API endpoint.
            params: Query parameters.

        Returns:
            Parsed JSON response dict.
        """
        resp = requests.get(
            url,
            params=params or {},
            timeout=_REQUEST_TIMEOUT,
            headers={
                "User-Agent": "GRID-DataPuller/1.0 (research)",
                "Accept": "application/json",
            },
        )
        resp.raise_for_status()
        time.sleep(_RATE_LIMIT_DELAY)
        return resp.json()

    # ── Registrant fetching ─────────────────────────────────────────────

    def _fetch_active_registrants(self, days_back: int = 30) -> dict[str, Any]:
        """Fetch active registrants and the principals of recent registrants.

        One call returns every active registrant; foreign principals need one
        call per registrant, so they are looked up only for registrants
        registered within ``days_back`` (bounded by _MAX_PRINCIPAL_LOOKUPS).

        Parameters:
            days_back: Registration-date window for principal lookups.

        Returns:
            ``registrants`` (recent registrants with principals attached),
            ``active_total``, ``lookups_failed`` and ``lookups_unattempted``.

        Raises:
            FaraFetchError: the registrant list could not be fetched or parsed.
        """
        try:
            data = self._fara_get(_FARA_ACTIVE_REGISTRANTS_URL)
        except Exception as exc:
            raise FaraFetchError(f"active registrants: {exc}") from exc
        try:
            rows = _rowset(data, "REGISTRANTS_ACTIVE")
        except FaraFetchError as exc:
            raise FaraFetchError(f"active registrants: {exc}") from exc

        parsed = [r for r in (self._parse_registrant(row) for row in rows) if r]
        cutoff = date.today() - timedelta(days=days_back)
        recent = [r for r in parsed if r["registration_date"] >= cutoff]

        registrants: list[dict[str, Any]] = []
        failed = 0
        for reg in recent[:_MAX_PRINCIPAL_LOOKUPS]:
            reg_number = reg["registrant_id"]
            if not reg_number.isdigit():
                failed += 1
                continue
            try:
                principal_rows = _rowset(
                    self._fara_get(_FARA_PRINCIPALS_URL + reg_number), "ROWSET",
                )
            except Exception as exc:
                failed += 1
                log.warning(
                    "FARA principal lookup failed for registration {n}: {e}",
                    n=reg_number, e=str(exc),
                )
                continue
            reg["principals"] = [
                {
                    "name": str(p.get("FP_NAME") or "").strip(),
                    "country": str(p.get("COUNTRY_NAME") or "").strip().upper(),
                    "type": "",
                }
                for p in principal_rows
            ]
            registrants.append(reg)

        unattempted = max(len(recent) - _MAX_PRINCIPAL_LOOKUPS, 0)
        log.info(
            "FARA: {t} active registrants, {r} registered in last {d} days, "
            "{f} principal lookups failed, {u} unattempted",
            t=len(parsed), r=len(recent), d=days_back, f=failed, u=unattempted,
        )
        return {
            "registrants": registrants,
            "active_total": len(parsed),
            "recent_total": len(recent),
            "lookups_failed": failed,
            "lookups_unattempted": unattempted,
        }

    def _parse_registrant(self, reg: dict[str, Any]) -> dict[str, Any] | None:
        """Parse a FARA registrant record into a normalised dict.

        Parameters:
            reg: Raw registrant dict from FARA API.

        Returns:
            Normalised registrant dict, or None if unusable.
        """
        try:
            registrant_name = str(
                reg.get("registrantName")
                or reg.get("name")
                or reg.get("Registrant_Name")
                or reg.get("Name")  # Registrants/json/Active
                or ""
            ).strip()
            if not registrant_name:
                return None

            # Foreign principal details
            principals = reg.get("foreignPrincipals", reg.get("principals", []))
            if not isinstance(principals, list):
                principals = [principals] if principals else []

            parsed_principals: list[dict[str, Any]] = []
            for p in principals:
                if not isinstance(p, dict):
                    continue
                country = (
                    p.get("country")
                    or p.get("principalCountry")
                    or p.get("Country")
                    or ""
                ).strip().upper()
                principal_name = (
                    p.get("name")
                    or p.get("principalName")
                    or p.get("Foreign_Principal")
                    or ""
                ).strip()
                parsed_principals.append({
                    "name": principal_name,
                    "country": country,
                    "type": (p.get("type") or p.get("principalType") or "").strip(),
                })

            # Registration date
            reg_date = _parse_fara_date(
                reg.get("registrationDate")
                or reg.get("Registration_Date")
            )

            return {
                "registrant_id": str(
                    reg.get("registrantId")
                    or reg.get("id")
                    or reg.get("Registration_Number")
                    or ""
                ),
                "registrant_name": registrant_name,
                "address": str(
                    reg.get("address") or reg.get("Address") or reg.get("Address_1") or ""
                ).strip(),
                "registration_date": reg_date or date.today(),
                "status": (reg.get("status") or "ACTIVE").strip(),
                "principals": parsed_principals,
            }

        except Exception as exc:
            log.debug("Failed to parse FARA registrant: {e}", e=str(exc))
            return None

    # ── Activity report fetching ────────────────────────────────────────

    def _fetch_recent_activities(
        self,
        days_back: int = 30,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Fetch recent FARA activity reports.

        Parameters:
            days_back: Number of days to look back.

        Returns:
            ``(activities, error)``: normalised activity dicts, and the fetch
            failure that stopped paging (None when paging completed). A
            failed fetch is not "no activities".
        """
        cutoff = (date.today() - timedelta(days=days_back)).isoformat()
        all_activities: list[dict[str, Any]] = []
        error: str | None = None
        page = 1

        while page <= _MAX_PAGES:
            try:
                data = self._fara_get(
                    _FARA_ACTIVITIES_URL,
                    params={
                        "dateFrom": cutoff,
                        "page": page,
                        "pageSize": 100,
                    },
                )
            except Exception as exc:
                log.warning(
                    "FARA activity fetch page {p} failed: {e}",
                    p=page, e=str(exc),
                )
                error = f"activities page {page}: {exc}"
                break

            results = data.get("results", data.get("data", []))
            if not results:
                break

            for activity in results:
                parsed = self._parse_activity(activity)
                if parsed:
                    all_activities.append(parsed)

            total_pages = data.get("totalPages", data.get("pages", page))
            if page >= total_pages:
                break
            page += 1

        log.info(
            "FARA: fetched {n} activities from last {d} days",
            n=len(all_activities), d=days_back,
        )
        return all_activities, error

    def _parse_activity(self, activity: dict[str, Any]) -> dict[str, Any] | None:
        """Parse a FARA activity record.

        Parameters:
            activity: Raw activity dict from FARA API.

        Returns:
            Normalised activity dict, or None if unusable.
        """
        try:
            registrant_name = (
                activity.get("registrantName")
                or activity.get("Registrant_Name")
                or ""
            ).strip()
            principal_name = (
                activity.get("foreignPrincipalName")
                or activity.get("Foreign_Principal")
                or ""
            ).strip()
            country = (
                activity.get("principalCountry")
                or activity.get("Country")
                or ""
            ).strip().upper()

            if not registrant_name and not principal_name:
                return None

            activity_type = (
                activity.get("activityType")
                or activity.get("Activity_Type")
                or "UNKNOWN"
            ).strip().upper()

            description = (
                activity.get("description")
                or activity.get("activityDescription")
                or activity.get("Description")
                or ""
            ).strip()

            # Parse compensation
            compensation = 0.0
            comp_raw = activity.get("compensation") or activity.get("Amount") or 0
            try:
                compensation = float(str(comp_raw).replace(",", "").replace("$", ""))
            except (ValueError, TypeError):
                pass

            activity_date = _parse_fara_date(
                activity.get("activityDate")
                or activity.get("date")
                or activity.get("Date")
            )

            # Contacts (who was lobbied)
            contacts: list[str] = []
            contact_data = activity.get("contacts", activity.get("governmentContacts", []))
            if isinstance(contact_data, list):
                for c in contact_data:
                    if isinstance(c, dict):
                        name = c.get("name", c.get("contactName", ""))
                        agency = c.get("agency", c.get("contactAgency", ""))
                        if name:
                            contacts.append(f"{name} ({agency})" if agency else name)
                    elif isinstance(c, str):
                        contacts.append(c)

            return {
                "registrant_name": registrant_name,
                "principal_name": principal_name,
                "country": country,
                "activity_type": activity_type,
                "description": description[:500],
                "compensation": compensation,
                "activity_date": activity_date or date.today(),
                "contacts": contacts[:10],
                "filing_url": (activity.get("url") or activity.get("documentUrl") or ""),
            }

        except Exception as exc:
            log.debug("Failed to parse FARA activity: {e}", e=str(exc))
            return None

    # ── Document/supplemental filing fetching ───────────────────────────

    def _fetch_recent_documents(
        self,
        days_back: int = 30,
    ) -> list[dict[str, Any]]:
        """Fetch recent FARA supplemental filing documents.

        These are the semi-annual reports that contain detailed
        compensation and disbursement breakdowns.

        Parameters:
            days_back: Number of days to look back.

        Returns:
            List of document metadata dicts.
        """
        cutoff = (date.today() - timedelta(days=days_back)).isoformat()

        try:
            data = self._fara_get(
                _FARA_DOCUMENTS_URL,
                params={
                    "dateFrom": cutoff,
                    "documentType": "Supplemental Statement",
                    "page": 1,
                    "pageSize": 100,
                },
            )
        except Exception as exc:
            log.warning("FARA document fetch failed: {e}", e=str(exc))
            return []

        documents: list[dict[str, Any]] = []
        for doc in data.get("results", data.get("data", [])):
            documents.append({
                "registrant_name": (doc.get("registrantName") or "").strip(),
                "document_type": (doc.get("documentType") or "").strip(),
                "filing_date": _parse_fara_date(doc.get("filingDate")) or date.today(),
                "url": doc.get("url", ""),
            })

        log.info("FARA: {n} recent documents found", n=len(documents))
        return documents

    # ── Spending trend analysis ─────────────────────────────────────────

    def _compute_country_trend(
        self,
        conn: Any,
        country_slug: str,
    ) -> dict[str, Any]:
        """Compute FARA spending trend for a country.

        Compares current-period foreign lobbying spend to historical
        average. Increasing spend from a country is a leading indicator
        of upcoming policy engagement or geopolitical pressure.

        Parameters:
            conn: Active database connection.
            country_slug: Slugified country name.

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
            {"pattern": f"FARA:{country_slug}:%", "src": self.source_id},
        ).fetchall()

        if len(rows) < 2:
            return {"trend": "INSUFFICIENT_DATA", "change_pct": 0.0, "periods": len(rows)}

        recent = rows[0][1] if rows[0][1] else 0.0
        historical_avg = sum(r[1] for r in rows[1:] if r[1]) / max(len(rows) - 1, 1)

        if historical_avg == 0:
            return {"trend": "NEW", "change_pct": 0.0, "periods": len(rows)}

        change_pct = ((recent - historical_avg) / historical_avg) * 100

        if change_pct > 25:
            trend = "INCREASING"
        elif change_pct < -25:
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

    # ── Storage ─────────────────────────────────────────────────────────

    def _store_registrant(
        self,
        conn: Any,
        registrant: dict[str, Any],
        dedup_hours: int = 168,
    ) -> int:
        """Store a FARA registrant and all its principals as raw_series rows.

        Parameters:
            conn: Active database connection (within a transaction).
            registrant: Normalised registrant dict.
            dedup_hours: Skip a principal row already pulled in this window.

        Returns:
            Number of rows inserted.
        """
        inserted = 0
        registrant_slug = _slugify(registrant["registrant_name"])

        for principal in registrant.get("principals", []):
            country = principal.get("country", "UNKNOWN")
            country_slug = _slugify(country)
            principal_name = principal.get("name", "")

            series_id = f"FARA:{country_slug}:{registrant_slug}:REGISTRATION"
            obs_date = registrant["registration_date"]

            if self._row_exists(series_id, obs_date, conn, dedup_hours=dedup_hours):
                # Already stored by an earlier run inside the window. Counted
                # so a zero-row run says why (grid-hermes 2026-11-05..06: 13
                # runs logged rows_inserted=0 for 14 recent registrants with
                # no failures, which read as "FARA not producing").
                self._already_stored = getattr(self, "_already_stored", 0) + 1
                continue

            payload = {
                "registrant_id": registrant["registrant_id"],
                "registrant_name": registrant["registrant_name"],
                "registrant_address": registrant.get("address", ""),
                "principal_name": principal_name,
                "principal_country": country,
                "principal_type": principal.get("type", ""),
                "status": registrant["status"],
                "country_sector": COUNTRY_SECTOR_MAP.get(country, {}),
            }

            self._insert_raw(
                conn=conn,
                series_id=series_id,
                obs_date=obs_date,
                value=0.0,  # Registration itself has no dollar value
                raw_payload=payload,
            )
            inserted += 1

        return inserted

    def _store_activity(
        self,
        conn: Any,
        activity: dict[str, Any],
    ) -> bool:
        """Store a FARA activity as a raw_series row.

        Parameters:
            conn: Active database connection (within a transaction).
            activity: Normalised activity dict.

        Returns:
            True if stored, False if duplicate.
        """
        country_slug = _slugify(activity["country"]) if activity["country"] else "UNKNOWN"
        registrant_slug = _slugify(activity["registrant_name"])
        activity_type = _slugify(activity["activity_type"], max_len=20)

        series_id = f"FARA:{country_slug}:{registrant_slug}:{activity_type}"
        obs_date = activity["activity_date"]

        if self._row_exists(series_id, obs_date, conn, dedup_hours=168):
            return False

        payload = {
            "registrant_name": activity["registrant_name"],
            "principal_name": activity["principal_name"],
            "country": activity["country"],
            "activity_type": activity["activity_type"],
            "description": activity["description"],
            "compensation": activity["compensation"],
            "contacts": activity["contacts"],
            "filing_url": activity["filing_url"],
            "country_sector": COUNTRY_SECTOR_MAP.get(activity["country"], {}),
        }

        self._insert_raw(
            conn=conn,
            series_id=series_id,
            obs_date=obs_date,
            value=activity["compensation"],
            raw_payload=payload,
        )
        return True

    def _emit_signal(
        self,
        conn: Any,
        activity: dict[str, Any],
        ticker: str,
        trend: dict[str, Any] | None = None,
    ) -> None:
        """Emit a signal_sources row for downstream trust scoring.

        Parameters:
            conn: Active database connection (within a transaction).
            activity: Normalised activity dict.
            ticker: Resolved sector ticker.
            trend: Optional country spending trend dict.
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
                "stype": "foreign_lobbying",
                "sid": activity.get("registrant_name", ""),
                "ticker": ticker,
                "sdate": activity["activity_date"],
                "stype2": "FARA_ACTIVITY",
                "sval": json.dumps({
                    "principal_name": activity["principal_name"],
                    "country": activity["country"],
                    "activity_type": activity["activity_type"],
                    "compensation": activity["compensation"],
                    "description": activity["description"][:200],
                    "contacts": activity["contacts"][:5],
                    "spending_trend": trend or {},
                }),
            },
        )

    # ── Public API ──────────────────────────────────────────────────────

    def pull_registrants(self, days_back: int = 30) -> dict[str, Any]:
        """Pull active FARA registrants and the principals of recent ones.

        Parameters:
            days_back: Registration-date window for principal lookups.

        Returns:
            Write-contract summary: ``status`` and ``rows_inserted`` (rows
            the transaction committed), plus coverage detail.
        """
        log.info("Pulling FARA active registrants")

        try:
            fetched = self._fetch_active_registrants(days_back=days_back)
        except FaraFetchError as exc:
            log.warning("FARA registrant pull failed: {e}", e=str(exc))
            return {"status": "FAILED", "rows_inserted": 0, "error": str(exc)}

        registrants = fetched["registrants"]
        stored = 0
        store_failed = 0
        self._already_stored = 0
        # A row from an earlier run inside the lookback window is the same
        # registration, not a new observation.
        dedup_hours = (days_back + 7) * 24

        with self.engine.begin() as conn:
            for reg in registrants:
                try:
                    # SAVEPOINT: a failed statement must not abort the whole
                    # transaction and silently roll back rows already counted.
                    with conn.begin_nested():
                        count = self._store_registrant(conn, reg, dedup_hours=dedup_hours)
                    stored += count
                except Exception as exc:
                    store_failed += 1
                    log.warning(
                        "Failed to store FARA registrant {name}: {e}",
                        name=reg.get("registrant_name", "?"),
                        e=str(exc),
                    )
        # Reached only after the transaction committed.
        rows_inserted = stored

        failed = fetched["lookups_failed"] + store_failed
        unattempted = fetched["lookups_unattempted"]
        checked = len(registrants) - store_failed
        if failed or unattempted:
            status = "PARTIAL" if (checked or rows_inserted) else "FAILED"
        else:
            status = "SUCCESS"
        summary: dict[str, Any] = {
            "status": status,
            "rows_inserted": rows_inserted,
            "registrants_fetched": fetched["active_total"],
            "registrants_recent": fetched["recent_total"],
            "principal_lookups_failed": fetched["lookups_failed"],
            "principal_lookups_unattempted": unattempted,
            "store_failed": store_failed,
            # Principal rows skipped because this registration is already in
            # raw_series: the "checked, nothing new" half of a zero-row run.
            "already_stored": self._already_stored,
        }
        if failed or unattempted:
            summary["error"] = (
                f"{fetched['lookups_failed']} principal lookups failed, "
                f"{unattempted} unattempted, {store_failed} stores failed "
                f"of {fetched['recent_total']} recent registrants"
            )
        log.info("FARA registrant pull complete: {s}", s=summary)
        return summary

    def pull_activities(self, days_back: int = 30) -> dict[str, Any]:
        """Pull recent FARA lobbying activity reports.

        The public eFile API has no activity endpoint (see
        _ACTIVITIES_UNAVAILABLE); a direct call reports that fetch failure.

        Parameters:
            days_back: Number of days of history to pull.

        Returns:
            Write-contract summary with ``status`` and ``rows_inserted``.
        """
        log.info("Pulling FARA activities — last {d} days", d=days_back)

        activities, fetch_error = self._fetch_recent_activities(days_back=days_back)
        stored = 0
        signals = 0
        skipped_dup = 0
        store_failed = 0
        alerts: list[str] = []

        with self.engine.begin() as conn:
            for activity in activities:
                try:
                    with conn.begin_nested():
                        was_stored = self._store_activity(conn, activity)
                        if not was_stored:
                            skipped_dup += 1
                            continue

                        # Resolve ticker for signal emission
                        country = activity["country"]
                        country_info = COUNTRY_SECTOR_MAP.get(country, {})
                        ticker = country_info.get("ticker")

                        # Also check activity description for sector keywords
                        if not ticker:
                            ticker = _extract_sector_from_description(
                                activity["description"]
                            )

                        trend: dict[str, Any] = {}
                        if ticker:
                            # Compute spending trend for this country
                            country_slug = _slugify(country) if country else "UNKNOWN"
                            trend = self._compute_country_trend(conn, country_slug)
                            self._emit_signal(conn, activity, ticker, trend)
                    stored += 1
                    if not ticker:
                        continue
                    signals += 1

                    # Alert on high-value or surging activity
                    if activity["compensation"] > 500_000:
                        alert_msg = (
                            f"HIGH-VALUE FARA: {activity['principal_name']} "
                            f"({country}) paying {activity['registrant_name']} "
                            f"${activity['compensation']:,.0f} for "
                            f"{activity['activity_type']}"
                        )
                        alerts.append(alert_msg)
                        log.info(alert_msg)

                    if trend.get("trend") == "INCREASING" and trend.get("change_pct", 0) > 50:
                        alert_msg = (
                            f"FARA SURGE: {country} lobbying spend up "
                            f"{trend['change_pct']}% — policy pressure signal"
                        )
                        alerts.append(alert_msg)
                        log.info(alert_msg)

                except Exception as exc:
                    store_failed += 1
                    log.warning(
                        "Failed to store FARA activity: {e}",
                        e=str(exc),
                    )

        problems = [p for p in (
            fetch_error,
            f"{store_failed} activity stores failed" if store_failed else None,
        ) if p]
        if problems:
            status = "PARTIAL" if (stored or skipped_dup) else "FAILED"
        else:
            status = "SUCCESS"
        summary: dict[str, Any] = {
            "status": status,
            "rows_inserted": stored,
            "activities_fetched": len(activities),
            "stored": stored,
            "signals_emitted": signals,
            "skipped_duplicate": skipped_dup,
            "alerts": alerts,
            "days_back": days_back,
        }
        if problems:
            summary["error"] = "; ".join(problems)
        log.info("FARA activity pull complete: {s}", s=summary)
        return summary

    def pull_all(self, days_back: int = 30) -> dict[str, Any]:
        """Pull FARA registrants (activities have no live endpoint).

        Parameters:
            days_back: Registration-date window for principal lookups.

        Returns:
            Write-contract summary: the registrant leg's ``status`` and
            committed ``rows_inserted``, with ``activities_unavailable``
            naming the coverage the public API cannot provide.
        """
        registrants = self.pull_registrants(days_back=days_back)
        results: dict[str, Any] = {
            "status": registrants["status"],
            "rows_inserted": registrants["rows_inserted"],
            "registrants": registrants,
            "activities_unavailable": _ACTIVITIES_UNAVAILABLE,
        }
        if registrants.get("error"):
            results["error"] = registrants["error"]
        return results

    def pull_recent(self, days_back: int = 30) -> dict[str, Any]:
        """Alias for pull_all — always incremental.

        Parameters:
            days_back: Number of days of history to pull.

        Returns:
            Combined summary dict.
        """
        return self.pull_all(days_back=days_back)
