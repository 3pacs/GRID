"""
ICIJ Offshore Leaks puller — Panama Papers, Paradise Papers, Pandora Papers, etc.

Downloads CSV datasets from the ICIJ Offshore Leaks database (814K+ entities)
and stores entities, officers, intermediaries, addresses, and relationships.

Data source: https://offshoreleaks.icij.org/pages/database
"""

from __future__ import annotations

import csv
import zipfile
from pathlib import Path
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ICIJ data download URLs (CSV format)
ICIJ_DOWNLOAD_BASE = "https://offshoreleaks.icij.org/pages/database"
ICIJ_CSV_URL = "https://offshoreleaks-data.icij.org/offshoreleaks/csv/full-oldb.LATEST.zip"

# Local cache
DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "icij"


class ICIJPuller(BasePuller):
    """Ingest ICIJ Offshore Leaks datasets."""

    SOURCE_NAME = "icij_offshore_leaks"
    SOURCE_CONFIG = {
        "base_url": "https://offshoreleaks.icij.org",
        "cost_tier": "FREE",
        "latency_class": "BATCH",
        "pit_available": False,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 30,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        DATA_DIR.mkdir(parents=True, exist_ok=True)

    @retry_on_failure(max_attempts=3)
    def download_data(self) -> Path:
        """Download the ICIJ CSV zip if not cached.

        Returns:
            Path to the extracted CSV directory.
        """
        zip_path = DATA_DIR / "full-oldb.zip"
        extracted = DATA_DIR / "csv"

        if extracted.exists() and any(extracted.glob("*.csv")):
            log.info("ICIJ data already cached at {p}", p=extracted)
            return extracted

        log.info("Downloading ICIJ Offshore Leaks dataset...")
        resp = requests.get(ICIJ_CSV_URL, timeout=300, stream=True)
        resp.raise_for_status()

        with open(zip_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        log.info("Extracting ICIJ ZIP...")
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extracted)

        return extracted

    def pull(self) -> dict[str, int]:
        """Download and ingest all ICIJ datasets.

        Returns:
            Dict with counts per table.
        """
        csv_dir = self.download_data()
        counts: dict[str, int] = {}

        # Map CSV filenames to table names and columns
        file_map = {
            "nodes-entities": ("icij_entities", self._insert_entities),
            "nodes-officers": ("icij_officers", self._insert_officers),
            "nodes-intermediaries": ("icij_intermediaries", self._insert_intermediaries),
            "nodes-addresses": ("icij_addresses", self._insert_addresses),
            "relationships": ("icij_relationships", self._insert_relationships),
        }

        for pattern, (table, inserter) in file_map.items():
            csv_files = list(csv_dir.rglob(f"*{pattern}*"))
            if not csv_files:
                log.warning("No CSV found for pattern {p}", p=pattern)
                continue

            for csv_file in csv_files:
                n = inserter(csv_file)
                counts[table] = counts.get(table, 0) + n
                log.info("Loaded {n} rows into {t} from {f}", n=n, t=table, f=csv_file.name)

        total = sum(counts.values())
        log.info("ICIJ ingestion complete: {t} total rows across {n} tables",
                 t=total, n=len(counts))

        # Auto-discover actors from all loaded entities and officers
        self._discover_actors()

        return counts

    def _discover_actors(self) -> None:
        """Scan loaded ICIJ data and auto-add actors to the network."""
        try:
            from intelligence.actor_ingest import ingest_actor

            # Officers are the most interesting — real people behind shell companies
            with self.engine.connect() as conn:
                officers = conn.execute(
                    text("SELECT name, country_codes FROM icij_officers WHERE name != '' LIMIT 100000")
                ).fetchall()
            added = 0
            for row in officers:
                if ingest_actor(self.engine, row[0], "person", source="icij",
                               country=row[1] or None,
                               confidence="derived"):
                    added += 1

            # High-profile entities (shell companies with common names are noise,
            # but named entities are worth tracking)
            with self.engine.connect() as conn:
                entities = conn.execute(
                    text(
                        "SELECT e.name, e.jurisdiction, COUNT(r.id) AS rel_count "
                        "FROM icij_entities e "
                        "JOIN icij_relationships r ON e.node_id = r.from_node OR e.node_id = r.to_node "
                        "GROUP BY e.name, e.jurisdiction "
                        "HAVING COUNT(r.id) >= 3 "
                        "ORDER BY rel_count DESC LIMIT 50000"
                    )
                ).fetchall()
            for row in entities:
                if ingest_actor(self.engine, row[0], "entity", source="icij",
                               country=row[1] or None,
                               confidence="derived",
                               metadata={"connections": row[2]}):
                    added += 1

            # Intermediaries — the law firms and agents
            with self.engine.connect() as conn:
                intermediaries = conn.execute(
                    text("SELECT name, country_codes FROM icij_intermediaries WHERE name != '' LIMIT 50000")
                ).fetchall()
            for row in intermediaries:
                if ingest_actor(self.engine, row[0], "company", source="icij",
                               country=row[1] or None,
                               confidence="derived",
                               metadata={"role": "intermediary"}):
                    added += 1

            log.info("ICIJ actor discovery: {n} new actors auto-added", n=added)
        except Exception as exc:
            log.debug("ICIJ actor discovery failed: {e}", e=str(exc))

    def _insert_entities(self, csv_path: Path) -> int:
        """Bulk insert entities from CSV."""
        count = 0
        with self.engine.begin() as conn, open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            batch: list[dict[str, Any]] = []
            for row in reader:
                batch.append({
                    "node_id": int(row.get("node_id", row.get("id", 0))),
                    "name": row.get("name", "").strip(),
                    "jurisdiction": row.get("jurisdiction", ""),
                    "country_codes": row.get("country_codes", row.get("countries", "")),
                    "incorporation_date": row.get("incorporation_date", ""),
                    "inactivation_date": row.get("inactivation_date", ""),
                    "status": row.get("status", ""),
                    "source_dataset": row.get("sourceID", row.get("source_id", "unknown")),
                    "service_provider": row.get("service_provider", ""),
                    "address": row.get("address", ""),
                    "note": row.get("note", ""),
                })
                if len(batch) >= 5000:
                    self._batch_insert_entities(conn, batch)
                    count += len(batch)
                    batch.clear()

            if batch:
                self._batch_insert_entities(conn, batch)
                count += len(batch)

        return count

    def _batch_insert_entities(self, conn: Any, batch: list[dict[str, Any]]) -> None:
        for row in batch:
            conn.execute(
                text(
                    "INSERT INTO icij_entities "
                    "(node_id, name, jurisdiction, country_codes, incorporation_date, "
                    "inactivation_date, status, source_dataset, service_provider, address, note) "
                    "VALUES (:node_id, :name, :jurisdiction, :country_codes, :incorporation_date, "
                    ":inactivation_date, :status, :source_dataset, :service_provider, :address, :note) "
                    "ON CONFLICT (node_id) DO NOTHING"
                ),
                row,
            )

    def _insert_officers(self, csv_path: Path) -> int:
        count = 0
        with self.engine.begin() as conn, open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                conn.execute(
                    text(
                        "INSERT INTO icij_officers "
                        "(node_id, name, country_codes, source_dataset, valid_until, note) "
                        "VALUES (:nid, :name, :cc, :src, :vu, :note) "
                        "ON CONFLICT (node_id) DO NOTHING"
                    ),
                    {
                        "nid": int(row.get("node_id", row.get("id", 0))),
                        "name": row.get("name", "").strip(),
                        "cc": row.get("country_codes", row.get("countries", "")),
                        "src": row.get("sourceID", row.get("source_id", "unknown")),
                        "vu": row.get("valid_until", ""),
                        "note": row.get("note", ""),
                    },
                )
                count += 1
        return count

    def _insert_intermediaries(self, csv_path: Path) -> int:
        count = 0
        with self.engine.begin() as conn, open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                conn.execute(
                    text(
                        "INSERT INTO icij_intermediaries "
                        "(node_id, name, country_codes, source_dataset, status, address) "
                        "VALUES (:nid, :name, :cc, :src, :status, :addr) "
                        "ON CONFLICT (node_id) DO NOTHING"
                    ),
                    {
                        "nid": int(row.get("node_id", row.get("id", 0))),
                        "name": row.get("name", "").strip(),
                        "cc": row.get("country_codes", row.get("countries", "")),
                        "src": row.get("sourceID", row.get("source_id", "unknown")),
                        "status": row.get("status", ""),
                        "addr": row.get("address", ""),
                    },
                )
                count += 1
        return count

    def _insert_addresses(self, csv_path: Path) -> int:
        count = 0
        with self.engine.begin() as conn, open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                conn.execute(
                    text(
                        "INSERT INTO icij_addresses "
                        "(node_id, address, country_codes, source_dataset) "
                        "VALUES (:nid, :addr, :cc, :src) "
                        "ON CONFLICT (node_id) DO NOTHING"
                    ),
                    {
                        "nid": int(row.get("node_id", row.get("id", 0))),
                        "addr": row.get("address", row.get("name", "")).strip(),
                        "cc": row.get("country_codes", row.get("countries", "")),
                        "src": row.get("sourceID", row.get("source_id", "unknown")),
                    },
                )
                count += 1
        return count

    def _insert_relationships(self, csv_path: Path) -> int:
        count = 0
        with self.engine.begin() as conn, open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                conn.execute(
                    text(
                        "INSERT INTO icij_relationships "
                        "(from_node, to_node, rel_type, source_dataset, start_date, end_date) "
                        "VALUES (:from_n, :to_n, :rel, :src, :start, :end_d)"
                    ),
                    {
                        "from_n": int(row.get("node_id_start", row.get("START_ID", 0))),
                        "to_n": int(row.get("node_id_end", row.get("END_ID", 0))),
                        "rel": row.get("rel_type", row.get("TYPE", "unknown")),
                        "src": row.get("sourceID", row.get("source_id", "unknown")),
                        "start": row.get("start_date", ""),
                        "end_d": row.get("end_date", ""),
                    },
                )
                count += 1
        return count
