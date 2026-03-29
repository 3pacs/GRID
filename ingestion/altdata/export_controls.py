"""
GRID export controls tracker — BIS Entity List & Federal Register ingestion.

Tracks US government export control actions that affect publicly traded
semiconductor and technology companies. For companies like NVIDIA, export
controls to China have been more material than the CHIPS Act (flagged in
NVDA audit).

Sources:
  1. BIS Entity List — Bureau of Industry and Security restricted entities
     https://www.bis.doc.gov/index.php/policy-guidance/lists-of-parties-of-concern/entity-list
  2. Federal Register — BIS rule-making notices affecting companies/technologies
     https://www.federalregister.gov/api/v1/documents.json?conditions[agencies][]=industry-and-security-bureau
  3. Commerce Department press releases (via Federal Register supplemental)

Series pattern: EXPORT_CONTROL:{company}:{action_type}
Emits signal_sources entries for trust scoring integration.

Scheduled: weekly pull (export control actions are infrequent but high-impact).
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

_FED_REG_API_URL: str = (
    "https://www.federalregister.gov/api/v1/documents.json"
)
_BIS_ENTITY_LIST_URL: str = (
    "https://www.bis.doc.gov/index.php/policy-guidance/"
    "lists-of-parties-of-concern/entity-list"
)
_REQUEST_TIMEOUT: int = 45
_RATE_LIMIT_DELAY: float = 1.0
_MAX_PAGES: int = 10
_DEFAULT_LOOKBACK_DAYS: int = 90

# ── Affected Company/Technology Mapping ──────────────────────────────────

# Companies directly affected by US export controls on semiconductors/tech
EXPORT_CONTROL_TICKERS: dict[str, dict[str, Any]] = {
    "NVDA": {
        "name": "NVIDIA",
        "keywords": ["nvidia", "a100", "a800", "h100", "h800", "h20",
                      "b100", "b200", "blackwell", "hopper", "advanced gpu",
                      "ai chip", "ai accelerator", "data center gpu"],
        "sector": "AI chips",
        "china_revenue_pct": 25.0,  # approx % of revenue from China/HK
    },
    "AMD": {
        "name": "AMD",
        "keywords": ["amd", "advanced micro devices", "mi250", "mi300",
                      "instinct", "epyc"],
        "sector": "AI chips",
        "china_revenue_pct": 15.0,
    },
    "INTC": {
        "name": "Intel",
        "keywords": ["intel", "gaudi", "habana", "xeon", "ponte vecchio"],
        "sector": "semiconductors",
        "china_revenue_pct": 27.0,
    },
    "ASML": {
        "name": "ASML",
        "keywords": ["asml", "euv", "duv", "lithography", "twinscan",
                      "extreme ultraviolet", "deep ultraviolet"],
        "sector": "lithography",
        "china_revenue_pct": 29.0,
    },
    "LRCX": {
        "name": "Lam Research",
        "keywords": ["lam research", "lam", "etch equipment",
                      "deposition equipment"],
        "sector": "semiconductor equipment",
        "china_revenue_pct": 30.0,
    },
    "AMAT": {
        "name": "Applied Materials",
        "keywords": ["applied materials", "amat", "semiconductor equipment"],
        "sector": "semiconductor equipment",
        "china_revenue_pct": 28.0,
    },
    "KLAC": {
        "name": "KLA Corporation",
        "keywords": ["kla", "kla corporation", "process control",
                      "inspection equipment"],
        "sector": "semiconductor equipment",
        "china_revenue_pct": 25.0,
    },
    "TSM": {
        "name": "TSMC",
        "keywords": ["tsmc", "taiwan semiconductor", "foundry",
                      "advanced node", "7nm", "5nm", "3nm"],
        "sector": "foundry",
        "china_revenue_pct": 10.0,
    },
    "QCOM": {
        "name": "Qualcomm",
        "keywords": ["qualcomm", "snapdragon"],
        "sector": "mobile chips",
        "china_revenue_pct": 62.0,
    },
    "AVGO": {
        "name": "Broadcom",
        "keywords": ["broadcom", "networking chip"],
        "sector": "networking chips",
        "china_revenue_pct": 35.0,
    },
    "MRVL": {
        "name": "Marvell Technology",
        "keywords": ["marvell", "marvell technology"],
        "sector": "infrastructure chips",
        "china_revenue_pct": 10.0,
    },
    "MU": {
        "name": "Micron",
        "keywords": ["micron", "dram", "nand", "memory chip", "hbm"],
        "sector": "memory",
        "china_revenue_pct": 25.0,
    },
}

# Keywords that indicate export control actions (vs. general BIS notices)
_ACTION_KEYWORDS: list[str] = [
    "entity list", "export control", "export restriction",
    "denied persons", "unverified list", "military end use",
    "advanced computing", "semiconductor", "supercomputer",
    "chips", "integrated circuit", "foundry", "lithography",
    "artificial intelligence", "machine learning",
    "china", "prc", "people's republic", "hong kong", "macau",
    "huawei", "smic", "biren", "cambricon", "inspur", "sugon",
    "bytedance", "hikvision", "dahua",
    "interim final rule", "final rule", "proposed rule",
    "commerce control list", "ear", "export administration regulations",
    "bis", "bureau of industry and security",
    "technology transfer", "deemed export",
    "foreign direct product rule", "fdpr",
]

# Restriction types we classify
RESTRICTION_TYPES: dict[str, str] = {
    "ENTITY_LIST_ADD": "Entity added to BIS Entity List",
    "ENTITY_LIST_MODIFY": "Entity List entry modified",
    "ENTITY_LIST_REMOVE": "Entity removed from Entity List",
    "NEW_RULE": "New export control rule issued",
    "RULE_AMENDMENT": "Existing rule amended/tightened",
    "COUNTRY_RESTRICTION": "Country-level export restriction",
    "TECHNOLOGY_BAN": "Specific technology/product ban",
    "LICENSE_REQUIREMENT": "New license requirement imposed",
    "FDPR_EXPANSION": "Foreign Direct Product Rule expansion",
    "PRESS_RELEASE": "Commerce Department announcement",
}

# Countries typically subject to semiconductor export controls
_RESTRICTED_COUNTRIES: list[str] = [
    "china", "prc", "people's republic of china", "hong kong",
    "macau", "russia", "iran", "north korea", "dprk",
    "belarus", "myanmar", "cuba", "syria", "venezuela",
]


def _extract_affected_tickers(text_content: str) -> list[str]:
    """Match document text against known company keywords.

    Parameters:
        text_content: Full text or abstract of the document.

    Returns:
        List of affected ticker symbols (deduplicated).
    """
    if not text_content:
        return []
    text_lower = text_content.lower()
    tickers: list[str] = []
    for ticker, info in EXPORT_CONTROL_TICKERS.items():
        for kw in info["keywords"]:
            if kw in text_lower:
                tickers.append(ticker)
                break
    return tickers


def _extract_countries(text_content: str) -> list[str]:
    """Extract mentioned restricted countries from document text.

    Parameters:
        text_content: Document text.

    Returns:
        Deduplicated list of country names (normalised).
    """
    if not text_content:
        return []
    text_lower = text_content.lower()
    found: list[str] = []
    # Normalise country references
    country_map = {
        "china": "China", "prc": "China",
        "people's republic of china": "China",
        "hong kong": "Hong Kong", "macau": "Macau",
        "russia": "Russia", "iran": "Iran",
        "north korea": "North Korea", "dprk": "North Korea",
        "belarus": "Belarus", "myanmar": "Myanmar",
        "cuba": "Cuba", "syria": "Syria", "venezuela": "Venezuela",
    }
    for keyword, normalised in country_map.items():
        if keyword in text_lower and normalised not in found:
            found.append(normalised)
    return found


def _classify_action_type(title: str, abstract: str) -> str:
    """Classify the type of export control action from title/abstract.

    Parameters:
        title: Document title.
        abstract: Document abstract or description.

    Returns:
        One of the RESTRICTION_TYPES keys.
    """
    combined = f"{title} {abstract}".lower()

    if "entity list" in combined:
        if any(w in combined for w in ["addition", "added", "add"]):
            return "ENTITY_LIST_ADD"
        if any(w in combined for w in ["modif", "amend", "revis"]):
            return "ENTITY_LIST_MODIFY"
        if any(w in combined for w in ["remov", "delet"]):
            return "ENTITY_LIST_REMOVE"
        return "ENTITY_LIST_MODIFY"

    if "foreign direct product" in combined or "fdpr" in combined:
        return "FDPR_EXPANSION"

    if "license" in combined and "require" in combined:
        return "LICENSE_REQUIREMENT"

    if any(w in combined for w in ["ban", "prohibit", "restrict"]):
        if any(c in combined for c in _RESTRICTED_COUNTRIES):
            return "COUNTRY_RESTRICTION"
        return "TECHNOLOGY_BAN"

    if "interim final rule" in combined or "final rule" in combined:
        return "NEW_RULE"

    if "amendment" in combined or "amend" in combined:
        return "RULE_AMENDMENT"

    if "press release" in combined or "announcement" in combined:
        return "PRESS_RELEASE"

    return "NEW_RULE"


def _is_export_control_relevant(title: str, abstract: str) -> bool:
    """Determine if a Federal Register document is export-control relevant.

    Parameters:
        title: Document title.
        abstract: Document abstract.

    Returns:
        True if the document appears related to export controls.
    """
    combined = f"{title} {abstract}".lower()
    # Must match at least 2 action keywords to reduce false positives
    matches = sum(1 for kw in _ACTION_KEYWORDS if kw in combined)
    return matches >= 2


# ── Puller Class ─────────────────────────────────────────────────────────


class ExportControlsPuller(BasePuller):
    """Pulls export control actions from Federal Register BIS filings.

    Fetches BIS rule-making notices, entity list changes, and technology
    restriction announcements. Maps each action to affected tickers and
    stores as raw_series rows with full metadata.

    Series pattern: EXPORT_CONTROL:{ticker}:{action_type}
    Value: severity score 1-10 (based on action type and scope).

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for BIS_EXPORT_CONTROLS.
    """

    SOURCE_NAME: str = "BIS_EXPORT_CONTROLS"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://www.federalregister.gov/api/v1/",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 20,  # High priority — material for semis
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the export controls puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info(
            "ExportControlsPuller initialised — source_id={sid}",
            sid=self.source_id,
        )

    # ── Public API ───────────────────────────────────────────────────────

    def pull_all(self, days_back: int = _DEFAULT_LOOKBACK_DAYS) -> dict[str, Any]:
        """Pull recent export control actions from Federal Register.

        Parameters:
            days_back: Number of days to look back (default: 90).

        Returns:
            Summary dict with counts of actions found, stored, and mapped.
        """
        log.info("Pulling export control actions — last {d} days", d=days_back)

        documents = self._fetch_federal_register(days_back)
        log.info(
            "Fetched {n} BIS-related Federal Register documents",
            n=len(documents),
        )

        stored = 0
        mapped = 0
        skipped_irrelevant = 0
        skipped_dup = 0

        with self.engine.begin() as conn:
            for doc in documents:
                title = doc.get("title", "")
                abstract = doc.get("abstract", "")

                if not _is_export_control_relevant(title, abstract):
                    skipped_irrelevant += 1
                    continue

                action_type = _classify_action_type(title, abstract)
                affected_tickers = _extract_affected_tickers(
                    f"{title} {abstract}"
                )
                countries = _extract_countries(f"{title} {abstract}")

                # If no specific tickers matched but it is a broad
                # semiconductor/AI rule, apply to all tracked tickers
                if not affected_tickers and any(
                    kw in f"{title} {abstract}".lower()
                    for kw in [
                        "advanced computing", "semiconductor",
                        "ai chip", "artificial intelligence",
                        "integrated circuit",
                    ]
                ):
                    affected_tickers = list(EXPORT_CONTROL_TICKERS.keys())

                if not affected_tickers:
                    # Store as unmapped but still relevant
                    affected_tickers = ["UNMAPPED"]

                for ticker in affected_tickers:
                    if ticker != "UNMAPPED":
                        mapped += 1

                    try:
                        was_stored = self._store_action(
                            conn, doc, ticker, action_type, countries,
                        )
                        if was_stored:
                            stored += 1
                            if ticker != "UNMAPPED":
                                self._emit_signal(
                                    conn, doc, ticker, action_type, countries,
                                )
                        else:
                            skipped_dup += 1
                    except Exception as exc:
                        log.warning(
                            "Failed to store export control action {id}: {err}",
                            id=doc.get("document_number", "?"),
                            err=str(exc),
                        )

        summary = {
            "total_fetched": len(documents),
            "stored": stored,
            "mapped_to_ticker": mapped,
            "skipped_irrelevant": skipped_irrelevant,
            "skipped_duplicate": skipped_dup,
            "days_back": days_back,
        }
        log.info("Export controls pull complete: {s}", s=summary)
        return summary

    # ── Data Fetching ────────────────────────────────────────────────────

    @retry_on_failure(max_attempts=3, backoff=2.0)
    def _fetch_federal_register(
        self, days_back: int,
    ) -> list[dict[str, Any]]:
        """Fetch BIS documents from the Federal Register API.

        Uses the free Federal Register API (no key required) to pull
        Bureau of Industry and Security documents.

        Parameters:
            days_back: Number of days to look back.

        Returns:
            List of document dicts.
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=days_back)

        all_docs: list[dict[str, Any]] = []
        page = 1

        while page <= _MAX_PAGES:
            params = [
                ("conditions[agencies][]", "industry-and-security-bureau"),
                ("conditions[publication_date][gte]", start_date.isoformat()),
                ("conditions[publication_date][lte]", end_date.isoformat()),
                ("per_page", 100),
                ("page", page),
                ("order", "newest"),
                ("fields[]", "title"),
                ("fields[]", "abstract"),
                ("fields[]", "document_number"),
                ("fields[]", "publication_date"),
                ("fields[]", "type"),
                ("fields[]", "html_url"),
                ("fields[]", "agencies"),
                ("fields[]", "action"),
                ("fields[]", "dates"),
                ("fields[]", "full_text_xml_url"),
                ("fields[]", "body_html_url"),
            ]

            resp = requests.get(
                _FED_REG_API_URL,
                params=params,
                timeout=_REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()

            results = data.get("results", [])
            if not results:
                break

            for r in results:
                doc = self._normalize_document(r)
                if doc:
                    all_docs.append(doc)

            # Check pagination
            total_pages = data.get("total_pages", 1)
            if page >= total_pages:
                break

            page += 1
            time.sleep(_RATE_LIMIT_DELAY)

        return all_docs

    def _normalize_document(
        self, raw: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Normalise a raw Federal Register document result.

        Parameters:
            raw: Raw result dict from the API.

        Returns:
            Normalised document dict, or None if unusable.
        """
        doc_number = raw.get("document_number", "")
        if not doc_number:
            return None

        pub_date_str = raw.get("publication_date", "")
        try:
            pub_date = date.fromisoformat(pub_date_str) if pub_date_str else date.today()
        except ValueError:
            pub_date = date.today()

        return {
            "document_number": doc_number,
            "title": (raw.get("title") or "").strip(),
            "abstract": (raw.get("abstract") or "").strip(),
            "publication_date": pub_date,
            "document_type": (raw.get("type") or "").strip(),
            "html_url": raw.get("html_url", ""),
            "action": (raw.get("action") or "").strip(),
            "agencies": raw.get("agencies", []),
        }

    # ── Storage ──────────────────────────────────────────────────────────

    def _store_action(
        self,
        conn: Any,
        doc: dict[str, Any],
        ticker: str,
        action_type: str,
        countries: list[str],
    ) -> bool:
        """Store an export control action as a raw_series row.

        Series ID pattern: EXPORT_CONTROL:{ticker}:{action_type}

        Parameters:
            conn: Active database connection (within a transaction).
            doc: Normalised document dict.
            ticker: Affected ticker or 'UNMAPPED'.
            action_type: Classification from RESTRICTION_TYPES.
            countries: List of restricted countries mentioned.

        Returns:
            True if stored, False if duplicate.
        """
        series_id = f"EXPORT_CONTROL:{ticker}:{action_type}"
        obs_date = doc["publication_date"]

        # Dedup by document_number — check 30-day window
        if self._row_exists(series_id, obs_date, conn, dedup_hours=720):
            return False

        severity = self._compute_severity(action_type, countries, ticker)

        payload = {
            "document_number": doc["document_number"],
            "title": doc["title"][:500],
            "abstract": doc["abstract"][:1000],
            "publication_date": obs_date.isoformat(),
            "document_type": doc["document_type"],
            "html_url": doc["html_url"],
            "action": doc["action"][:200],
            "action_type": action_type,
            "action_description": RESTRICTION_TYPES.get(action_type, ""),
            "affected_ticker": ticker,
            "affected_company": EXPORT_CONTROL_TICKERS.get(ticker, {}).get("name", ticker),
            "affected_sector": EXPORT_CONTROL_TICKERS.get(ticker, {}).get("sector", ""),
            "restricted_countries": countries,
            "severity": severity,
        }

        self._insert_raw(
            conn=conn,
            series_id=series_id,
            obs_date=obs_date,
            value=float(severity),
            raw_payload=payload,
            pull_status="SUCCESS",
        )
        return True

    def _emit_signal(
        self,
        conn: Any,
        doc: dict[str, Any],
        ticker: str,
        action_type: str,
        countries: list[str],
    ) -> None:
        """Emit a signal_sources row for downstream trust scoring.

        Only called for actions mapped to a real ticker.

        Parameters:
            conn: Active database connection (within a transaction).
            doc: Normalised document dict.
            ticker: Resolved stock ticker.
            action_type: Classification from RESTRICTION_TYPES.
            countries: List of restricted countries mentioned.
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
                "stype": "export_control",
                "sid": doc["document_number"],
                "ticker": ticker,
                "sdate": doc["publication_date"],
                "stype2": action_type,
                "sval": json.dumps({
                    "document_number": doc["document_number"],
                    "title": doc["title"][:200],
                    "countries": countries,
                    "severity": self._compute_severity(
                        action_type, countries, ticker,
                    ),
                }),
            },
        )

    @staticmethod
    def _compute_severity(
        action_type: str,
        countries: list[str],
        ticker: str,
    ) -> int:
        """Compute a 1-10 severity score for the export control action.

        Factors: action type (entity list > rule), scope (China = higher),
        and affected company (higher China revenue = higher severity).

        Parameters:
            action_type: Classification key.
            countries: List of restricted countries.
            ticker: Affected ticker.

        Returns:
            Severity score 1-10.
        """
        # Base score by action type
        type_scores = {
            "ENTITY_LIST_ADD": 8,
            "ENTITY_LIST_MODIFY": 5,
            "ENTITY_LIST_REMOVE": 3,  # positive for affected co
            "NEW_RULE": 7,
            "RULE_AMENDMENT": 6,
            "COUNTRY_RESTRICTION": 8,
            "TECHNOLOGY_BAN": 9,
            "LICENSE_REQUIREMENT": 6,
            "FDPR_EXPANSION": 9,
            "PRESS_RELEASE": 4,
        }
        base = type_scores.get(action_type, 5)

        # Country factor: China/HK = +1, multiple countries = +1
        if "China" in countries or "Hong Kong" in countries:
            base = min(10, base + 1)
        if len(countries) >= 3:
            base = min(10, base + 1)

        # Company exposure: high China revenue = higher severity
        china_pct = EXPORT_CONTROL_TICKERS.get(ticker, {}).get(
            "china_revenue_pct", 0,
        )
        if china_pct >= 30:
            base = min(10, base + 1)

        return min(10, max(1, base))
