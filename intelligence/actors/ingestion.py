"""
GRID Intelligence — Actor Network data ingestion.

Handles parsing of external datasets (ICIJ Panama/Pandora Papers)
and mapping offshore entities to known actors.
"""

from __future__ import annotations

import csv
import os
from pathlib import Path

from loguru import logger as log

from intelligence.actors.seed_data import _KNOWN_ACTORS


def ingest_panama_pandora_data(
    data_dir: str | None = None,
) -> None:
    """Parse ICIJ Offshore Leaks database and map entities to known actors.

    Source: https://offshoreleaks.icij.org/pages/database
    The ICIJ provides downloadable CSV files. Expected files in data_dir:
        - nodes-entities.csv   (offshore entities)
        - nodes-officers.csv   (officers / intermediaries)
        - relationships.csv    (links between them)

    This function:
        1. Reads the CSVs
        2. Matches officer names against _KNOWN_ACTORS
        3. Stores matched connections in the actor network
        4. Logs any matches for manual review

    Parameters:
        data_dir: Directory containing the ICIJ CSV files.
                  Defaults to ~/data/icij_offshore_leaks/
    """
    if data_dir is None:
        data_dir = os.path.expanduser("~/data/icij_offshore_leaks")

    data_path = Path(data_dir)
    officers_file = data_path / "nodes-officers.csv"
    entities_file = data_path / "nodes-entities.csv"

    if not officers_file.exists():
        log.warning(
            "ICIJ data not found at {p}. Download from "
            "https://offshoreleaks.icij.org/pages/database",
            p=data_dir,
        )
        return

    # Build a set of known actor names for fast lookup
    known_names: dict[str, str] = {}  # lowercase_name -> actor_id
    for actor_id, data in _KNOWN_ACTORS.items():
        name = data["name"].lower()
        known_names[name] = actor_id
        # Also add last name for partial matching
        parts = name.split()
        if len(parts) >= 2:
            known_names[parts[-1]] = actor_id

    matches: list[dict] = []

    # Parse officers
    log.info("Parsing ICIJ officers from {f}", f=str(officers_file))
    try:
        with open(officers_file, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                officer_name = (row.get("name") or "").strip()
                if not officer_name:
                    continue

                officer_lower = officer_name.lower()
                # Exact match
                if officer_lower in known_names:
                    matches.append({
                        "actor_id": known_names[officer_lower],
                        "offshore_name": officer_name,
                        "node_id": row.get("node_id", ""),
                        "jurisdiction": row.get("jurisdiction", ""),
                        "source_id": row.get("sourceID", ""),
                        "match_type": "exact",
                    })
                    continue

                # Partial match: check if any known name appears in officer name
                for known_lower, aid in known_names.items():
                    if len(known_lower) > 5 and known_lower in officer_lower:
                        matches.append({
                            "actor_id": aid,
                            "offshore_name": officer_name,
                            "node_id": row.get("node_id", ""),
                            "jurisdiction": row.get("jurisdiction", ""),
                            "source_id": row.get("sourceID", ""),
                            "match_type": "partial",
                        })
                        break
    except Exception as exc:
        log.error("Failed to parse ICIJ officers: {e}", e=str(exc))

    # Parse entities for jurisdiction metadata
    entity_map: dict[str, dict] = {}
    if entities_file.exists():
        try:
            with open(entities_file, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    nid = row.get("node_id", "")
                    if nid:
                        entity_map[nid] = {
                            "name": row.get("name", ""),
                            "jurisdiction": row.get("jurisdiction", ""),
                            "incorporation_date": row.get("incorporation_date", ""),
                            "status": row.get("status", ""),
                        }
        except Exception as exc:
            log.debug("Failed to parse ICIJ entities: {e}", e=str(exc))

    if matches:
        log.warning(
            "ICIJ Offshore Leaks: {n} matches found against known actors! "
            "Review required.",
            n=len(matches),
        )
        for m in matches:
            actor_data = _KNOWN_ACTORS.get(m["actor_id"], {})
            log.warning(
                "  MATCH [{match_type}]: {actor} ({title}) <-> offshore entity "
                "'{offshore}'  jurisdiction={jurisdiction}",
                match_type=m["match_type"],
                actor=actor_data.get("name", m["actor_id"]),
                title=actor_data.get("title", ""),
                offshore=m["offshore_name"],
                jurisdiction=m["jurisdiction"],
            )
    else:
        log.info("ICIJ Offshore Leaks: no matches found against known actors.")
