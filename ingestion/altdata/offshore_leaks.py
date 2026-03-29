"""
GRID ICIJ Offshore Leaks Database ingestion module.

Pulls and parses the ICIJ Offshore Leaks database (Panama Papers, Pandora
Papers, Paradise Papers, Bahamas Leaks, and Offshore Leaks).

Source: https://offshoreleaks.icij.org/pages/database
The ICIJ provides free downloadable CSV files (~500MB total):
    - nodes-officers.csv   (people / beneficial owners)
    - nodes-entities.csv   (shell companies / offshore entities)
    - nodes-addresses.csv  (registered addresses)
    - nodes-intermediaries.csv (law firms, banks, agents)
    - relationships.csv    (edges connecting nodes)

Series stored with pattern: OFFSHORE:{actor_name}:{entity_name}:{jurisdiction}
Signal source_type: 'offshore_leak'

This module:
    1. Downloads CSVs from the ICIJ Offshore Leaks bulk download page
    2. Parses officers (people) and entities (shell companies)
    3. Matches officer names against actor_network._KNOWN_ACTORS
    4. Stores matches in raw_series + signal_sources
    5. Flags tracked power brokers who appear in offshore structures
"""

from __future__ import annotations

import csv
import hashlib
import io
import os
import re
import tempfile
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ── URLs & Paths ──────────────────────────────────────────────────────────

# ICIJ bulk CSV download (ZIP archive)
_ICIJ_DOWNLOAD_URL: str = "https://offshoreleaks.icij.org/pages/database"

# Expected CSV filenames inside the extracted archive
_CSV_OFFICERS: str = "nodes-officers.csv"
_CSV_ENTITIES: str = "nodes-entities.csv"
_CSV_ADDRESSES: str = "nodes-addresses.csv"
_CSV_INTERMEDIARIES: str = "nodes-intermediaries.csv"
_CSV_RELATIONSHIPS: str = "relationships.csv"

# Default local data directory
# Canonical bulk location: /data/grid/bulk/icij (shared with actor_discovery)
# Falls back to user-local path for development.
_DEFAULT_DATA_DIR: str = (
    "/data/grid/bulk/icij"
    if os.path.isdir("/data/grid/bulk/icij")
    else os.path.expanduser("~/data/icij_offshore_leaks")
)

# HTTP config
_REQUEST_TIMEOUT: int = 120
_DOWNLOAD_CHUNK_SIZE: int = 65536

# Name matching config
_MIN_NAME_LENGTH_PARTIAL: int = 6  # minimum chars for partial name matching
_FUZZY_THRESHOLD: float = 0.85     # reserved for future fuzzy matching


# ══════════════════════════════════════════════════════════════════════════
# NAME MATCHING UTILITIES
# ══════════════════════════════════════════════════════════════════════════

def _normalize_name(name: str) -> str:
    """Normalize a name for matching: lowercase, strip titles, punctuation.

    Parameters:
        name: Raw name string.

    Returns:
        Cleaned lowercase name.
    """
    name = name.strip().lower()
    # Remove common titles / suffixes
    for title in ("mr.", "mrs.", "ms.", "dr.", "jr.", "sr.", "ii", "iii", "iv"):
        name = name.replace(title, "")
    # Remove punctuation except spaces
    name = re.sub(r"[^a-z\s]", "", name)
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    return name


def _build_known_names_index() -> dict[str, str]:
    """Build a lookup index from _KNOWN_ACTORS for name matching.

    Returns:
        Dict mapping normalized name variants -> actor_id.
    """
    try:
        from intelligence.actor_network import _KNOWN_ACTORS
    except ImportError:
        log.warning("Cannot import _KNOWN_ACTORS — offshore matching disabled")
        return {}

    index: dict[str, str] = {}
    for actor_id, data in _KNOWN_ACTORS.items():
        name = _normalize_name(data.get("name", ""))
        if not name:
            continue

        # Full name match
        index[name] = actor_id

        # Last name match (for names with >= 2 parts)
        parts = name.split()
        if len(parts) >= 2:
            last_name = parts[-1]
            if len(last_name) >= _MIN_NAME_LENGTH_PARTIAL:
                # Only use last name if it's distinctive enough
                index[last_name] = actor_id

            # First + Last (skip middle names)
            first_last = f"{parts[0]} {parts[-1]}"
            if first_last != name:
                index[first_last] = actor_id

    return index


def _match_officer_to_actor(
    officer_name: str,
    known_index: dict[str, str],
) -> tuple[str | None, str]:
    """Check if an officer name matches any known actor.

    Parameters:
        officer_name: Name from ICIJ officer record.
        known_index: Dict from _build_known_names_index().

    Returns:
        Tuple of (actor_id or None, match_type).
        match_type is 'exact', 'partial', or 'none'.
    """
    normalized = _normalize_name(officer_name)
    if not normalized:
        return None, "none"

    # Exact full-name match
    if normalized in known_index:
        return known_index[normalized], "exact"

    # Check if any known name is a substring of the officer name
    for known_name, actor_id in known_index.items():
        if len(known_name) >= _MIN_NAME_LENGTH_PARTIAL and known_name in normalized:
            return actor_id, "partial"

    return None, "none"


# ══════════════════════════════════════════════════════════════════════════
# PULLER CLASS
# ══════════════════════════════════════════════════════════════════════════

class OffshoreLeaksPuller(BasePuller):
    """Pulls and parses the ICIJ Offshore Leaks database.

    Downloads bulk CSV files from ICIJ, cross-references officers against
    the GRID actor network, and stores matches as raw_series rows and
    signal_sources entries.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for ICIJ_OFFSHORE.
        data_dir: Local directory for cached CSV files.
    """

    SOURCE_NAME: str = "ICIJ_OFFSHORE"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://offshoreleaks.icij.org",
        "cost_tier": "FREE",
        "latency_class": "STATIC",
        "pit_available": False,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 60,
    }

    def __init__(
        self,
        db_engine: Engine,
        data_dir: str | None = None,
    ) -> None:
        """Initialize the offshore leaks puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
            data_dir: Directory containing (or to download) ICIJ CSV files.
                      Defaults to ~/data/icij_offshore_leaks/.
        """
        super().__init__(db_engine)
        self.data_dir = Path(data_dir or _DEFAULT_DATA_DIR)
        self._known_index: dict[str, str] = {}
        log.info(
            "OffshoreLeaksPuller initialised — source_id={sid}, data_dir={d}",
            sid=self.source_id,
            d=str(self.data_dir),
        )

    # ------------------------------------------------------------------
    # CSV availability check
    # ------------------------------------------------------------------
    def _csvs_available(self) -> bool:
        """Check if the required CSV files exist locally."""
        officers = self.data_dir / _CSV_OFFICERS
        entities = self.data_dir / _CSV_ENTITIES
        return officers.exists() and entities.exists()

    # ------------------------------------------------------------------
    # Download (manual guidance — ICIJ requires browser download)
    # ------------------------------------------------------------------
    def ensure_data(self) -> bool:
        """Ensure ICIJ CSV files are available locally.

        The ICIJ Offshore Leaks database requires manual download from:
        https://offshoreleaks.icij.org/pages/database

        Download the CSV bulk data package and extract into self.data_dir.

        Returns:
            True if CSVs are available, False otherwise.
        """
        if self._csvs_available():
            log.info("ICIJ data available at {d}", d=str(self.data_dir))
            return True

        # Create directory if needed
        self.data_dir.mkdir(parents=True, exist_ok=True)

        log.warning(
            "ICIJ Offshore Leaks CSVs not found at {d}. "
            "Manual download required:\n"
            "  1. Visit https://offshoreleaks.icij.org/pages/database\n"
            "  2. Download the CSV bulk data package (~500MB)\n"
            "  3. Extract into {d}/\n"
            "  Expected files: {files}",
            d=str(self.data_dir),
            files=", ".join([
                _CSV_OFFICERS, _CSV_ENTITIES, _CSV_ADDRESSES,
                _CSV_INTERMEDIARIES, _CSV_RELATIONSHIPS,
            ]),
        )
        return False

    # ------------------------------------------------------------------
    # Parse officers
    # ------------------------------------------------------------------
    def _parse_officers(self) -> list[dict]:
        """Parse the officers CSV and return raw records.

        Returns:
            List of officer dicts with keys: node_id, name, jurisdiction,
            source_id, countries, valid_until.
        """
        officers_path = self.data_dir / _CSV_OFFICERS
        if not officers_path.exists():
            log.warning("Officers CSV not found: {p}", p=str(officers_path))
            return []

        records: list[dict] = []
        log.info("Parsing ICIJ officers from {f}", f=str(officers_path))

        try:
            with open(officers_path, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    name = (row.get("name") or "").strip()
                    if not name:
                        continue
                    records.append({
                        "node_id": row.get("node_id", ""),
                        "name": name,
                        "jurisdiction": row.get("jurisdiction", ""),
                        "source_id": row.get("sourceID", ""),
                        "countries": row.get("country_codes", ""),
                        "valid_until": row.get("valid_until", ""),
                    })
        except Exception as exc:
            log.error("Failed to parse ICIJ officers: {e}", e=str(exc))

        log.info("Parsed {n} officers from ICIJ data", n=len(records))
        return records

    # ------------------------------------------------------------------
    # Parse entities
    # ------------------------------------------------------------------
    def _parse_entities(self) -> dict[str, dict]:
        """Parse the entities CSV into a node_id -> entity dict.

        Returns:
            Dict mapping node_id -> entity metadata.
        """
        entities_path = self.data_dir / _CSV_ENTITIES
        if not entities_path.exists():
            log.warning("Entities CSV not found: {p}", p=str(entities_path))
            return {}

        entity_map: dict[str, dict] = {}
        log.info("Parsing ICIJ entities from {f}", f=str(entities_path))

        try:
            with open(entities_path, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    nid = row.get("node_id", "")
                    if not nid:
                        continue
                    entity_map[nid] = {
                        "name": (row.get("name") or "").strip(),
                        "jurisdiction": row.get("jurisdiction", ""),
                        "jurisdiction_description": row.get(
                            "jurisdiction_description", "",
                        ),
                        "incorporation_date": row.get("incorporation_date", ""),
                        "inactivation_date": row.get("inactivation_date", ""),
                        "status": row.get("status", ""),
                        "countries": row.get("country_codes", ""),
                        "source_id": row.get("sourceID", ""),
                        "address": row.get("address", ""),
                    }
        except Exception as exc:
            log.error("Failed to parse ICIJ entities: {e}", e=str(exc))

        log.info("Parsed {n} entities from ICIJ data", n=len(entity_map))
        return entity_map

    # ------------------------------------------------------------------
    # Parse relationships
    # ------------------------------------------------------------------
    def _parse_relationships(self) -> dict[str, list[dict]]:
        """Parse the relationships CSV.

        Returns:
            Dict mapping officer_node_id -> list of related entity records.
        """
        rel_path = self.data_dir / _CSV_RELATIONSHIPS
        if not rel_path.exists():
            log.debug("Relationships CSV not found: {p}", p=str(rel_path))
            return {}

        officer_to_entities: dict[str, list[dict]] = {}

        try:
            with open(rel_path, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    node_1 = row.get("node_id_start", "") or row.get("START_ID", "")
                    node_2 = row.get("node_id_end", "") or row.get("END_ID", "")
                    rel_type = row.get("rel_type", "") or row.get("TYPE", "")

                    if node_1 and node_2:
                        officer_to_entities.setdefault(node_1, []).append({
                            "entity_node_id": node_2,
                            "rel_type": rel_type,
                        })
        except Exception as exc:
            log.debug("Failed to parse ICIJ relationships: {e}", e=str(exc))

        return officer_to_entities

    # ------------------------------------------------------------------
    # Core matching logic
    # ------------------------------------------------------------------
    def match_actors(self) -> list[dict]:
        """Cross-reference ICIJ officers against GRID's known actors.

        Returns:
            List of match dicts with keys: actor_id, actor_name,
            officer_name, officer_node_id, match_type, jurisdiction,
            source_id, connected_entities.
        """
        if not self._csvs_available():
            log.warning("ICIJ CSVs not available — cannot match actors")
            return []

        # Build name index
        self._known_index = _build_known_names_index()
        if not self._known_index:
            log.warning("No known actors loaded — skipping offshore matching")
            return []

        # Parse data
        officers = self._parse_officers()
        entity_map = self._parse_entities()
        relationships = self._parse_relationships()

        # Match officers to known actors
        matches: list[dict] = []
        seen: set[str] = set()  # deduplicate actor_id+officer_node_id pairs

        try:
            from intelligence.actor_network import _KNOWN_ACTORS
        except ImportError:
            _KNOWN_ACTORS = {}

        for officer in officers:
            actor_id, match_type = _match_officer_to_actor(
                officer["name"], self._known_index,
            )
            if actor_id is None:
                continue

            dedup_key = f"{actor_id}:{officer['node_id']}"
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            # Find connected entities via relationships
            connected_entities: list[dict] = []
            officer_rels = relationships.get(officer["node_id"], [])
            for rel in officer_rels:
                entity = entity_map.get(rel["entity_node_id"])
                if entity:
                    connected_entities.append({
                        "entity_name": entity["name"],
                        "entity_jurisdiction": entity["jurisdiction"],
                        "entity_status": entity["status"],
                        "incorporation_date": entity["incorporation_date"],
                        "rel_type": rel["rel_type"],
                        "entity_source": entity.get("source_id", ""),
                    })

            actor_data = _KNOWN_ACTORS.get(actor_id, {})
            matches.append({
                "actor_id": actor_id,
                "actor_name": actor_data.get("name", actor_id),
                "actor_tier": actor_data.get("tier", "unknown"),
                "officer_name": officer["name"],
                "officer_node_id": officer["node_id"],
                "officer_jurisdiction": officer["jurisdiction"],
                "officer_source_id": officer["source_id"],
                "match_type": match_type,
                "connected_entities": connected_entities,
            })

        if matches:
            log.warning(
                "ICIJ Offshore Leaks: {n} matches found against {a} known actors!",
                n=len(matches),
                a=len({m["actor_id"] for m in matches}),
            )
        else:
            log.info("ICIJ Offshore Leaks: no matches against known actors.")

        return matches

    # ------------------------------------------------------------------
    # Build series_id
    # ------------------------------------------------------------------
    @staticmethod
    def _build_series_id(
        actor_name: str,
        entity_name: str,
        jurisdiction: str,
    ) -> str:
        """Build the OFFSHORE series_id.

        Parameters:
            actor_name: Matched actor name.
            entity_name: Offshore entity name.
            jurisdiction: Entity jurisdiction code.

        Returns:
            Series ID string.
        """
        # Sanitize components for series_id
        actor_clean = re.sub(r"[^a-zA-Z0-9_]", "_", actor_name)[:60]
        entity_clean = re.sub(r"[^a-zA-Z0-9_]", "_", entity_name)[:60]
        jur_clean = re.sub(r"[^a-zA-Z0-9_]", "_", jurisdiction)[:20]
        return f"OFFSHORE:{actor_clean}:{entity_clean}:{jur_clean}"

    # ------------------------------------------------------------------
    # Store matches in raw_series + signal_sources
    # ------------------------------------------------------------------
    def store_matches(
        self,
        matches: list[dict],
    ) -> dict[str, int]:
        """Persist offshore leak matches to raw_series and signal_sources.

        Parameters:
            matches: List of match dicts from match_actors().

        Returns:
            Dict with counts: raw_series_inserted, signals_emitted.
        """
        raw_count = 0
        signal_count = 0
        today = date.today()
        now = datetime.now(timezone.utc)

        with self.engine.begin() as conn:
            for match in matches:
                actor_name = match["actor_name"]
                actor_id = match["actor_id"]

                # Store each connected entity as a separate series row
                entities = match.get("connected_entities", [])
                if not entities:
                    # No entity link — store the officer match itself
                    entities = [{
                        "entity_name": match["officer_name"],
                        "entity_jurisdiction": match["officer_jurisdiction"],
                        "entity_status": "unknown",
                        "incorporation_date": "",
                        "rel_type": "officer",
                        "entity_source": match.get("officer_source_id", ""),
                    }]

                for entity in entities:
                    entity_name = entity.get("entity_name", "unknown")
                    jurisdiction = entity.get("entity_jurisdiction", "unknown")

                    series_id = self._build_series_id(
                        actor_name, entity_name, jurisdiction,
                    )

                    # Check dedup
                    if self._row_exists(series_id, today, conn, dedup_hours=720):
                        continue

                    # raw_series insert
                    try:
                        conn.execute(text("""
                            INSERT INTO raw_series
                                (series_id, source_id, obs_date, release_date,
                                 value, raw_payload)
                            VALUES
                                (:sid, :src, :obs, :rel, :val, :payload)
                        """), {
                            "sid": series_id,
                            "src": self.source_id,
                            "obs": today,
                            "rel": today,
                            "val": 1.0,  # binary flag: match exists
                            "payload": _json_dumps({
                                "actor_id": actor_id,
                                "actor_name": actor_name,
                                "actor_tier": match.get("actor_tier", ""),
                                "officer_name": match["officer_name"],
                                "officer_node_id": match["officer_node_id"],
                                "entity_name": entity_name,
                                "jurisdiction": jurisdiction,
                                "entity_status": entity.get("entity_status", ""),
                                "incorporation_date": entity.get(
                                    "incorporation_date", "",
                                ),
                                "rel_type": entity.get("rel_type", ""),
                                "match_type": match["match_type"],
                                "leak_source": entity.get("entity_source", ""),
                            }),
                        })
                        raw_count += 1
                    except Exception as exc:
                        log.debug(
                            "Failed to insert offshore raw_series: {e}",
                            e=str(exc),
                        )
                        continue

                    # signal_sources insert — emit as offshore_leak
                    try:
                        conn.execute(text("""
                            INSERT INTO signal_sources
                                (source_type, source_id, ticker, signal_type,
                                 signal_date, signal_value, metadata)
                            VALUES
                                (:stype, :sid, :ticker, :signal_type,
                                 :sdate, :sval, :meta)
                        """), {
                            "stype": "offshore_leak",
                            "sid": series_id,
                            "ticker": actor_id,  # use actor_id as ticker proxy
                            "signal_type": "SELL",  # offshore = bearish signal
                            "sdate": now,
                            "sval": 1.0,
                            "meta": _json_dumps({
                                "actor_name": actor_name,
                                "entity_name": entity_name,
                                "jurisdiction": jurisdiction,
                                "match_type": match["match_type"],
                                "officer_name": match["officer_name"],
                                "entity_status": entity.get("entity_status", ""),
                                "leak_source": entity.get("entity_source", ""),
                            }),
                        })
                        signal_count += 1
                    except Exception as exc:
                        log.debug(
                            "Failed to emit offshore signal: {e}",
                            e=str(exc),
                        )

        log.info(
            "Offshore leaks stored: {r} raw_series, {s} signals emitted",
            r=raw_count,
            s=signal_count,
        )
        return {
            "raw_series_inserted": raw_count,
            "signals_emitted": signal_count,
        }

    # ------------------------------------------------------------------
    # Full pull: parse, match, store
    # ------------------------------------------------------------------
    def pull(self) -> dict[str, Any]:
        """Full offshore leaks ingestion pipeline.

        1. Ensure data is downloaded
        2. Parse CSVs
        3. Match against known actors
        4. Store matches in DB
        5. Return summary

        Returns:
            Dict with match counts and details.
        """
        if not self.ensure_data():
            return {
                "status": "NO_DATA",
                "message": "ICIJ CSVs not available. Manual download required.",
            }

        matches = self.match_actors()
        if not matches:
            return {
                "status": "NO_MATCHES",
                "total_matches": 0,
                "actors_matched": 0,
            }

        store_result = self.store_matches(matches)

        # Build summary of which actors matched
        actor_summary: list[dict] = []
        for m in matches:
            entity_names = [
                e["entity_name"]
                for e in m.get("connected_entities", [])
            ]
            actor_summary.append({
                "actor_id": m["actor_id"],
                "actor_name": m["actor_name"],
                "match_type": m["match_type"],
                "officer_name": m["officer_name"],
                "jurisdiction": m["officer_jurisdiction"],
                "entities_count": len(m.get("connected_entities", [])),
                "entity_names": entity_names[:5],  # cap for logging
            })

        result = {
            "status": "SUCCESS",
            "total_matches": len(matches),
            "actors_matched": len({m["actor_id"] for m in matches}),
            "raw_series_inserted": store_result["raw_series_inserted"],
            "signals_emitted": store_result["signals_emitted"],
            "actor_summary": actor_summary,
        }

        log.warning(
            "OFFSHORE LEAKS INGESTION COMPLETE: {n} matches across {a} actors. "
            "Review required for potential conflicts of interest.",
            n=len(matches),
            a=result["actors_matched"],
        )
        return result


# ══════════════════════════════════════════════════════════════════════════
# CROSS-REFERENCE: check a single actor name against offshore data
# ══════════════════════════════════════════════════════════════════════════

def check_actor_in_offshore_leaks(
    engine: Engine,
    actor_name: str,
    actor_id: str | None = None,
) -> list[dict]:
    """Check if a specific actor name appears in stored offshore leak data.

    Used by actor_discovery.py when new actors are found — checks if
    the newly discovered actor has any offshore connections.

    Parameters:
        engine: SQLAlchemy engine.
        actor_name: Name to search for.
        actor_id: Optional actor_id for tagging.

    Returns:
        List of matching offshore records.
    """
    matches: list[dict] = []
    name_normalized = _normalize_name(actor_name)
    if not name_normalized or len(name_normalized) < 4:
        return matches

    # Search raw_series for OFFSHORE entries matching this actor
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT series_id, raw_payload, obs_date
                FROM raw_series
                WHERE series_id LIKE :pattern
                  AND source_id = (
                      SELECT id FROM source_catalog
                      WHERE name = 'ICIJ_OFFSHORE' LIMIT 1
                  )
                ORDER BY obs_date DESC
                LIMIT 50
            """), {
                "pattern": f"OFFSHORE:%{name_normalized[:30]}%",
            }).fetchall()

            for row in rows:
                matches.append({
                    "series_id": row[0],
                    "payload": row[1] if isinstance(row[1], dict) else {},
                    "obs_date": str(row[2]),
                    "actor_name": actor_name,
                    "actor_id": actor_id,
                })
    except Exception as exc:
        log.debug(
            "Offshore leak check failed for {name}: {e}",
            name=actor_name,
            e=str(exc),
        )

    # Also do a direct CSV search if data is available
    data_dir = Path(_DEFAULT_DATA_DIR)
    officers_path = data_dir / _CSV_OFFICERS
    if officers_path.exists():
        try:
            with open(officers_path, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    officer_name = (row.get("name") or "").strip()
                    if not officer_name:
                        continue
                    officer_normalized = _normalize_name(officer_name)
                    # Check exact or substring match
                    if (
                        name_normalized == officer_normalized
                        or (
                            len(name_normalized) >= _MIN_NAME_LENGTH_PARTIAL
                            and name_normalized in officer_normalized
                        )
                    ):
                        matches.append({
                            "series_id": f"OFFSHORE_LIVE:{actor_name}",
                            "officer_name": officer_name,
                            "node_id": row.get("node_id", ""),
                            "jurisdiction": row.get("jurisdiction", ""),
                            "source_id": row.get("sourceID", ""),
                            "match_type": (
                                "exact"
                                if name_normalized == officer_normalized
                                else "partial"
                            ),
                            "actor_name": actor_name,
                            "actor_id": actor_id,
                        })
        except Exception as exc:
            log.debug("Direct CSV search failed: {e}", e=str(exc))

    if matches:
        log.warning(
            "OFFSHORE ALERT: {name} found in {n} offshore leak records!",
            name=actor_name,
            n=len(matches),
        )

    return matches


def queue_offshore_investigation(
    engine: Engine,
    actor_name: str,
    actor_id: str,
    offshore_matches: list[dict],
) -> str | None:
    """Queue an LLM investigation task for an actor with offshore connections.

    Called when actor_discovery finds a new actor with offshore links.

    Parameters:
        engine: SQLAlchemy engine.
        actor_name: Name of the actor.
        actor_id: Actor ID.
        offshore_matches: Matches from check_actor_in_offshore_leaks().

    Returns:
        Task ID if enqueued, None otherwise.
    """
    if not offshore_matches:
        return None

    try:
        from orchestration.llm_taskqueue import get_task_queue

        tq = get_task_queue(engine)

        # Summarize the matches for the prompt
        entity_lines: list[str] = []
        for m in offshore_matches[:10]:
            payload = m.get("payload", {})
            entity_name = (
                payload.get("entity_name")
                or m.get("officer_name", "unknown")
            )
            jurisdiction = (
                payload.get("jurisdiction")
                or m.get("jurisdiction", "unknown")
            )
            entity_lines.append(
                f"  - Entity: {entity_name} | Jurisdiction: {jurisdiction}"
            )

        entities_text = "\n".join(entity_lines)

        prompt = (
            f"OFFSHORE LEAK INVESTIGATION: {actor_name} (ID: {actor_id})\n\n"
            f"This actor appears in the ICIJ Offshore Leaks database "
            f"(Panama Papers / Pandora Papers / Paradise Papers) with "
            f"{len(offshore_matches)} connections:\n\n"
            f"{entities_text}\n\n"
            f"Investigate:\n"
            f"1. What were these offshore entities used for? "
            f"Are they legitimate tax structures or suspicious?\n"
            f"2. What other known actors (from GRID's actor network) "
            f"are connected to the same entities?\n"
            f"3. Are there implications for their public trading "
            f"positions or political roles?\n"
            f"4. Does this change the trust/credibility assessment "
            f"for this actor?\n"
            f"5. Should any active GRID theses be re-evaluated in light "
            f"of these offshore connections?\n"
        )

        task_id = tq.enqueue(
            task_type="offshore_leak_investigation",
            prompt=prompt,
            context={
                "actor_id": actor_id,
                "actor_name": actor_name,
                "match_count": len(offshore_matches),
                "source": "icij_offshore_leaks",
            },
            priority=3,
        )

        log.info(
            "Queued offshore investigation for {name} — task_id={tid}",
            name=actor_name,
            tid=task_id,
        )
        return task_id

    except Exception as exc:
        log.warning(
            "Failed to queue offshore investigation for {name}: {e}",
            name=actor_name,
            e=str(exc),
        )
        return None


# ══════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════

def _json_dumps(obj: Any) -> str:
    """JSON-serialize for Postgres JSONB, handling edge cases."""
    import json
    return json.dumps(obj, default=str, ensure_ascii=False)
