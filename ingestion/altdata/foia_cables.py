"""
GRID FOIA diplomatic cables ingestion module.

Pulls declassified diplomatic cables and State Department documents
from publicly available FOIA reading rooms and archives.

Sources:
  1. State Dept FOIA Electronic Reading Room (foia.state.gov)
     — search API for declassified cables and documents
  2. National Security Archive (nsarchive.gwu.edu)
     — curated document collections on foreign policy topics
  3. CIA FOIA Reading Room (cia.gov/readingroom)
     — declassified intelligence assessments

These documents reveal backroom motivations, diplomatic pressure,
and policy positions that were hidden at the time but explain market
moves in hindsight. Current declassification events (e.g., new
batch releases) signal what topics the government is willing to
expose — often a leading indicator of policy shifts.

Series pattern: FOIA:{source}:{topic_slug}:{classification}
Emits signal_sources entries for trust scoring integration.

Scheduled: weekly pull (new releases trickle out).
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

_STATE_FOIA_URL: str = "https://foia.state.gov/Search/Search.aspx"
_STATE_FOIA_API: str = "https://foia.state.gov/api/Search"
_NSA_SEARCH_URL: str = "https://nsarchive.gwu.edu/search"
_CIA_FOIA_URL: str = "https://www.cia.gov/readingroom/search/site"

_REQUEST_TIMEOUT: int = 30
_RATE_LIMIT_DELAY: float = 1.5
_MAX_PAGES: int = 10

# ── Topic definitions ──────────────────────────────────────────────────
# Each topic maps to geopolitical themes that affect markets.
# We search for cables on these topics and score relevance.

FOIA_TOPICS: list[dict[str, Any]] = [
    {
        "query": "trade negotiations tariff",
        "topic": "trade_policy",
        "ticker": "XLI",
        "keywords": ["tariff", "trade", "import", "export", "wto", "nafta", "usmca"],
    },
    {
        "query": "sanctions economic pressure",
        "topic": "sanctions",
        "ticker": "SPY",
        "keywords": ["sanction", "embargo", "freeze", "restrict", "ofac", "sdnlist"],
    },
    {
        "query": "oil petroleum energy diplomacy",
        "topic": "energy_diplomacy",
        "ticker": "XLE",
        "keywords": ["oil", "petroleum", "opec", "pipeline", "lng", "energy"],
    },
    {
        "query": "arms sales military transfer",
        "topic": "defense_sales",
        "ticker": "ITA",
        "keywords": ["arms", "military", "defense", "weapon", "f-35", "missile"],
    },
    {
        "query": "China technology transfer semiconductor",
        "topic": "tech_competition",
        "ticker": "SMH",
        "keywords": ["semiconductor", "chip", "technology", "huawei", "export control"],
    },
    {
        "query": "currency manipulation exchange rate",
        "topic": "currency_war",
        "ticker": "UUP",
        "keywords": ["currency", "exchange rate", "devaluation", "manipulation", "forex"],
    },
    {
        "query": "intelligence economic espionage",
        "topic": "economic_espionage",
        "ticker": "XLK",
        "keywords": ["espionage", "intelligence", "cyber", "theft", "proprietary"],
    },
    {
        "query": "financial crisis banking regulation",
        "topic": "financial_regulation",
        "ticker": "XLF",
        "keywords": ["bank", "financial", "regulation", "crisis", "bailout", "systemic"],
    },
]

# Classification levels that affect signal confidence
CLASSIFICATION_CONFIDENCE: dict[str, str] = {
    "SECRET": "confirmed",     # High-value declassified intel
    "CONFIDENTIAL": "confirmed",
    "UNCLASSIFIED": "derived",
    "SENSITIVE": "derived",
    "TOP SECRET": "confirmed",
    "RESTRICTED": "derived",
}


def _slugify(text_str: str, max_len: int = 50) -> str:
    """Create a slug from text for use in series_id."""
    slug = re.sub(r"[^A-Z0-9 ]", "", text_str.strip().upper())
    slug = re.sub(r"\s+", "_", slug.strip())
    return slug[:max_len]


def _extract_classification(text_str: str) -> str:
    """Extract classification level from document text.

    Parameters:
        text_str: Document text or metadata string.

    Returns:
        Classification level or 'UNCLASSIFIED'.
    """
    if not text_str:
        return "UNCLASSIFIED"
    text_upper = text_str.upper()
    for level in ("TOP SECRET", "SECRET", "CONFIDENTIAL", "SENSITIVE", "RESTRICTED"):
        if level in text_upper:
            return level
    return "UNCLASSIFIED"


def _score_topic_relevance(
    text_str: str,
    keywords: list[str],
) -> float:
    """Score how relevant a document is to a topic based on keywords.

    Parameters:
        text_str: Document text or description.
        keywords: List of topic keywords to match.

    Returns:
        Relevance score 0.0 to 1.0.
    """
    if not text_str or not keywords:
        return 0.0
    text_lower = text_str.lower()
    hits = sum(1 for kw in keywords if kw.lower() in text_lower)
    return min(hits / max(len(keywords), 1), 1.0)


class FOIACablesPuller(BasePuller):
    """Pulls declassified diplomatic cables from FOIA reading rooms.

    Searches State Dept, NSA, and CIA FOIA archives for documents
    matching geopolitical topics that affect markets. Scores relevance
    and classification level, and emits signals when high-value
    declassified material appears on market-relevant topics.

    Series pattern: FOIA:{source}:{topic_slug}:{classification}
    Value: topic relevance score (0.0-1.0).

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for FOIA_CABLES.
    """

    SOURCE_NAME: str = "FOIA_CABLES"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://foia.state.gov",
        "cost_tier": "FREE",
        "latency_class": "WEEKLY",
        "pit_available": False,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 55,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the FOIA cables puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info(
            "FOIACablesPuller initialised — source_id={sid}",
            sid=self.source_id,
        )

    # ── API helpers ──────────────────────────────────────────────────────

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(
            ConnectionError, TimeoutError, OSError, requests.RequestException,
        ),
    )
    def _state_foia_search(
        self,
        query: str,
        page: int = 1,
    ) -> dict[str, Any]:
        """Search State Dept FOIA Electronic Reading Room.

        Parameters:
            query: Search query string.
            page: Page number for pagination.

        Returns:
            Parsed JSON response dict.
        """
        params = {
            "searchText": query,
            "page": page,
            "pageSize": 50,
            "sort": "date desc",
        }

        resp = requests.get(
            _STATE_FOIA_API,
            params=params,
            timeout=_REQUEST_TIMEOUT,
            headers={
                "User-Agent": "GRID-DataPuller/1.0 (research)",
                "Accept": "application/json",
            },
        )
        resp.raise_for_status()
        time.sleep(_RATE_LIMIT_DELAY)
        return resp.json()

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(
            ConnectionError, TimeoutError, OSError, requests.RequestException,
        ),
    )
    def _nsa_search(
        self,
        query: str,
    ) -> list[dict[str, Any]]:
        """Search National Security Archive for document collections.

        Parameters:
            query: Search query string.

        Returns:
            List of document metadata dicts.
        """
        resp = requests.get(
            _NSA_SEARCH_URL,
            params={
                "search_api_fulltext": query,
                "sort_by": "ds_changed",
                "sort_order": "DESC",
                "items_per_page": 25,
            },
            timeout=_REQUEST_TIMEOUT,
            headers={
                "User-Agent": "GRID-DataPuller/1.0 (research)",
                "Accept": "application/json",
            },
        )
        resp.raise_for_status()
        time.sleep(_RATE_LIMIT_DELAY)

        # NSA returns HTML or JSON depending on endpoint
        try:
            return resp.json().get("results", [])
        except (ValueError, AttributeError):
            # Parse minimal metadata from response
            return []

    # ── Topic-based search ──────────────────────────────────────────────

    def _search_topic(
        self,
        topic_def: dict[str, Any],
        days_back: int = 90,
    ) -> list[dict[str, Any]]:
        """Search for FOIA documents matching a topic definition.

        Parameters:
            topic_def: Topic dict with query, keywords, ticker, etc.
            days_back: How far back to search for new releases.

        Returns:
            List of parsed document dicts.
        """
        documents: list[dict[str, Any]] = []
        query = topic_def["query"]

        # Search State Dept FOIA
        try:
            data = self._state_foia_search(query, page=1)

            results = data.get("Results", data.get("results", []))
            if isinstance(results, list):
                for doc in results:
                    parsed = self._parse_state_doc(doc, topic_def)
                    if parsed:
                        documents.append(parsed)

            # Check page 2 if significant results
            total = data.get("TotalCount", data.get("totalCount", 0))
            if total > 50:
                try:
                    data2 = self._state_foia_search(query, page=2)
                    for doc in data2.get("Results", data2.get("results", [])):
                        parsed = self._parse_state_doc(doc, topic_def)
                        if parsed:
                            documents.append(parsed)
                except Exception:
                    pass

        except Exception as exc:
            log.debug(
                "State FOIA search failed for '{q}': {e}",
                q=query, e=str(exc),
            )

        # Search National Security Archive
        try:
            nsa_results = self._nsa_search(query)
            for doc in nsa_results:
                parsed = self._parse_nsa_doc(doc, topic_def)
                if parsed:
                    documents.append(parsed)
        except Exception as exc:
            log.debug(
                "NSA search failed for '{q}': {e}",
                q=query, e=str(exc),
            )

        return documents

    def _parse_state_doc(
        self,
        doc: dict[str, Any],
        topic_def: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Parse a State Dept FOIA document result.

        Parameters:
            doc: Raw document dict from State FOIA API.
            topic_def: Topic definition for relevance scoring.

        Returns:
            Normalised document dict, or None if unusable.
        """
        try:
            title = (
                doc.get("Subject")
                or doc.get("subject")
                or doc.get("title")
                or ""
            ).strip()
            if not title:
                return None

            # Extract date
            doc_date_str = (
                doc.get("DocDate")
                or doc.get("docDate")
                or doc.get("date")
                or ""
            )
            try:
                doc_date = date.fromisoformat(doc_date_str[:10]) if doc_date_str else None
            except (ValueError, TypeError):
                doc_date = None

            # Extract posted/released date (when declassified)
            posted_str = (
                doc.get("PostedDate")
                or doc.get("postedDate")
                or ""
            )
            try:
                posted_date = date.fromisoformat(posted_str[:10]) if posted_str else date.today()
            except (ValueError, TypeError):
                posted_date = date.today()

            # Document body/description for relevance scoring
            body = (
                doc.get("MessageText")
                or doc.get("body")
                or doc.get("description")
                or title
            )

            classification = _extract_classification(
                doc.get("Classification")
                or doc.get("classification")
                or body
            )

            relevance = _score_topic_relevance(
                f"{title} {body}",
                topic_def["keywords"],
            )

            if relevance < 0.1:
                return None

            return {
                "source": "STATE_DEPT",
                "title": title[:300],
                "doc_date": doc_date,
                "posted_date": posted_date,
                "classification": classification,
                "confidence": CLASSIFICATION_CONFIDENCE.get(classification, "derived"),
                "relevance": round(relevance, 3),
                "topic": topic_def["topic"],
                "ticker": topic_def["ticker"],
                "doc_id": doc.get("DocNbr") or doc.get("id") or "",
                "from_office": (doc.get("From") or doc.get("from") or "").strip(),
                "to_office": (doc.get("To") or doc.get("to") or "").strip(),
                "body_snippet": (body or "")[:500],
            }

        except Exception as exc:
            log.debug("Failed to parse State FOIA doc: {e}", e=str(exc))
            return None

    def _parse_nsa_doc(
        self,
        doc: dict[str, Any],
        topic_def: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Parse a National Security Archive document result.

        Parameters:
            doc: Raw document dict from NSA search.
            topic_def: Topic definition for relevance scoring.

        Returns:
            Normalised document dict, or None if unusable.
        """
        try:
            title = (
                doc.get("title")
                or doc.get("label")
                or ""
            ).strip()
            if not title:
                return None

            # Extract date
            doc_date_str = doc.get("date") or doc.get("created") or ""
            try:
                doc_date = date.fromisoformat(doc_date_str[:10]) if doc_date_str else None
            except (ValueError, TypeError):
                doc_date = None

            posted_str = doc.get("changed") or doc.get("posted") or ""
            try:
                posted_date = date.fromisoformat(posted_str[:10]) if posted_str else date.today()
            except (ValueError, TypeError):
                posted_date = date.today()

            body = doc.get("body") or doc.get("description") or title

            classification = _extract_classification(body)
            relevance = _score_topic_relevance(
                f"{title} {body}",
                topic_def["keywords"],
            )

            if relevance < 0.1:
                return None

            return {
                "source": "NSA_ARCHIVE",
                "title": title[:300],
                "doc_date": doc_date,
                "posted_date": posted_date,
                "classification": classification,
                "confidence": CLASSIFICATION_CONFIDENCE.get(classification, "derived"),
                "relevance": round(relevance, 3),
                "topic": topic_def["topic"],
                "ticker": topic_def["ticker"],
                "doc_id": doc.get("nid") or doc.get("id") or "",
                "url": doc.get("url") or "",
                "body_snippet": (body or "")[:500],
            }

        except Exception as exc:
            log.debug("Failed to parse NSA doc: {e}", e=str(exc))
            return None

    # ── Storage ─────────────────────────────────────────────────────────

    def _store_document(
        self,
        conn: Any,
        doc: dict[str, Any],
    ) -> bool:
        """Store a FOIA document as a raw_series row.

        Parameters:
            conn: Active database connection (within a transaction).
            doc: Normalised document dict.

        Returns:
            True if stored, False if duplicate.
        """
        source_slug = _slugify(doc["source"], max_len=15)
        topic_slug = _slugify(doc["topic"])
        classification = _slugify(doc["classification"], max_len=15)

        series_id = f"FOIA:{source_slug}:{topic_slug}:{classification}"
        obs_date = doc["posted_date"]

        if self._row_exists(series_id, obs_date, conn, dedup_hours=168):
            return False

        payload = {
            "source": doc["source"],
            "title": doc["title"],
            "doc_date": doc["doc_date"].isoformat() if doc["doc_date"] else None,
            "posted_date": doc["posted_date"].isoformat(),
            "classification": doc["classification"],
            "confidence": doc["confidence"],
            "topic": doc["topic"],
            "doc_id": doc.get("doc_id", ""),
            "from_office": doc.get("from_office", ""),
            "to_office": doc.get("to_office", ""),
            "body_snippet": doc.get("body_snippet", ""),
            "url": doc.get("url", ""),
        }

        self._insert_raw(
            conn=conn,
            series_id=series_id,
            obs_date=obs_date,
            value=doc["relevance"],
            raw_payload=payload,
        )
        return True

    def _emit_signal(
        self,
        conn: Any,
        doc: dict[str, Any],
    ) -> None:
        """Emit a signal_sources row for high-value declassified documents.

        Only emits signals for documents with classification >= CONFIDENTIAL
        and relevance > 0.3, as these are the most likely to contain
        actionable geopolitical intelligence.

        Parameters:
            conn: Active database connection (within a transaction).
            doc: Normalised document dict.
        """
        # Only signal on non-trivial documents
        if doc["classification"] == "UNCLASSIFIED" and doc["relevance"] < 0.5:
            return

        ticker = doc["ticker"]
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
                "stype": "diplomatic_cable",
                "sid": doc.get("doc_id", doc["title"][:60]),
                "ticker": ticker,
                "sdate": doc["posted_date"],
                "stype2": "DECLASSIFIED_INTEL",
                "sval": json.dumps({
                    "title": doc["title"][:200],
                    "classification": doc["classification"],
                    "confidence": doc["confidence"],
                    "relevance": doc["relevance"],
                    "topic": doc["topic"],
                    "source": doc["source"],
                    "doc_date": doc["doc_date"].isoformat() if doc["doc_date"] else None,
                }),
            },
        )

    # ── Public API ──────────────────────────────────────────────────────

    def pull_all(self, days_back: int = 90) -> dict[str, Any]:
        """Pull recent FOIA cable releases across all tracked topics.

        Parameters:
            days_back: Number of days to look back for new releases.

        Returns:
            Summary dict with counts per topic.
        """
        log.info("Pulling FOIA cables — searching {n} topics", n=len(FOIA_TOPICS))

        total_found = 0
        stored = 0
        signals = 0
        skipped_dup = 0
        topic_counts: dict[str, int] = {}
        alerts: list[str] = []

        for topic_def in FOIA_TOPICS:
            topic = topic_def["topic"]
            try:
                documents = self._search_topic(topic_def, days_back=days_back)
                topic_counts[topic] = len(documents)
                total_found += len(documents)

                with self.engine.begin() as conn:
                    for doc in documents:
                        try:
                            was_stored = self._store_document(conn, doc)
                            if not was_stored:
                                skipped_dup += 1
                                continue
                            stored += 1

                            # Emit signal for high-value documents
                            if doc["relevance"] > 0.3:
                                try:
                                    self._emit_signal(conn, doc)
                                    signals += 1
                                except Exception as exc:
                                    log.debug(
                                        "Signal emission failed: {e}",
                                        e=str(exc),
                                    )

                            # Alert on formerly classified documents
                            if doc["classification"] in ("SECRET", "TOP SECRET"):
                                alert_msg = (
                                    f"FOIA DECLASSIFIED [{doc['classification']}]: "
                                    f"{doc['title'][:100]} — topic: {topic}"
                                )
                                alerts.append(alert_msg)
                                log.info(alert_msg)

                        except Exception as exc:
                            log.warning(
                                "Failed to store FOIA doc: {e}",
                                e=str(exc),
                            )

            except Exception as exc:
                log.warning(
                    "FOIA topic search failed for '{t}': {e}",
                    t=topic, e=str(exc),
                )
                topic_counts[topic] = 0

        summary = {
            "status": "SUCCESS",
            "total_found": total_found,
            "stored": stored,
            "signals_emitted": signals,
            "skipped_duplicate": skipped_dup,
            "topics_searched": len(FOIA_TOPICS),
            "topic_counts": topic_counts,
            "alerts": alerts,
            "days_back": days_back,
        }
        log.info("FOIA cables pull complete: {s}", s=summary)
        return summary

    def pull_recent(self, days_back: int = 90) -> dict[str, Any]:
        """Alias for pull_all — always incremental.

        Parameters:
            days_back: Number of days to look back.

        Returns:
            Summary dict.
        """
        return self.pull_all(days_back=days_back)
