#!/usr/bin/env python3
"""
GRID Dataset Parser — Unified loader for bulk intelligence datasets.

Parses ICIJ leaks, OpenSanctions, congressional trades, FEC contributions,
OFAC sanctions, and Fed speeches into the GRID PostgreSQL database.

Usage:
    python scripts/parse_datasets.py [all|icij|sanctions|trades|fec|ofac|speeches]
    python scripts/parse_datasets.py sanctions --limit 50000
    python scripts/parse_datasets.py fec --min-amount 25000

Tables created if missing:
    - actors                  (persons, entities, donors, officers)
    - entity_relationships    (ICIJ edges, actor connections)
    - analytical_snapshots    (trades, speeches, intel records)
    - signal_data             (large-trade signals, anomalies)
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
import time
import zipfile
from collections import defaultdict
from datetime import datetime
from typing import Optional

# Allow imports from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_engine
from sqlalchemy import text
from loguru import logger as log


# ---------------------------------------------------------------------------
# Schema DDL — idempotent table creation
# ---------------------------------------------------------------------------
SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS actors (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    category        TEXT NOT NULL,
    tier            TEXT NOT NULL DEFAULT 'unknown',
    country         TEXT,
    jurisdiction    TEXT,
    source_id       TEXT,
    metadata        JSONB DEFAULT '{}',
    confidence      TEXT NOT NULL DEFAULT 'confirmed',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_actors_category ON actors(category);
CREATE INDEX IF NOT EXISTS idx_actors_name ON actors(name);
CREATE INDEX IF NOT EXISTS idx_actors_tier ON actors(tier);

CREATE TABLE IF NOT EXISTS entity_relationships (
    id              BIGSERIAL PRIMARY KEY,
    actor_a         TEXT NOT NULL,
    actor_b         TEXT NOT NULL,
    relationship    TEXT NOT NULL,
    strength        DOUBLE PRECISION DEFAULT 0.5,
    source_id       TEXT,
    metadata        JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_entity_rel_a ON entity_relationships(actor_a);
CREATE INDEX IF NOT EXISTS idx_entity_rel_b ON entity_relationships(actor_b);
CREATE INDEX IF NOT EXISTS idx_entity_rel_type ON entity_relationships(relationship);
CREATE UNIQUE INDEX IF NOT EXISTS uq_entity_rel
    ON entity_relationships(actor_a, actor_b, relationship, source_id);

CREATE TABLE IF NOT EXISTS analytical_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    category        TEXT NOT NULL,
    snapshot_date   DATE,
    actor           TEXT,
    ticker          TEXT,
    title           TEXT,
    summary         TEXT,
    data            JSONB DEFAULT '{}',
    confidence      TEXT NOT NULL DEFAULT 'confirmed',
    source_id       TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_snap_category ON analytical_snapshots(category);
CREATE INDEX IF NOT EXISTS idx_snap_date ON analytical_snapshots(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_snap_actor ON analytical_snapshots(actor);
CREATE INDEX IF NOT EXISTS idx_snap_ticker ON analytical_snapshots(ticker);

CREATE TABLE IF NOT EXISTS signal_data (
    id              BIGSERIAL PRIMARY KEY,
    signal_type     TEXT NOT NULL,
    signal_date     DATE NOT NULL,
    ticker          TEXT,
    actor           TEXT,
    direction       TEXT,
    magnitude       DOUBLE PRECISION,
    description     TEXT,
    data            JSONB DEFAULT '{}',
    confidence      TEXT NOT NULL DEFAULT 'derived',
    source_id       TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_signal_type ON signal_data(signal_type);
CREATE INDEX IF NOT EXISTS idx_signal_date ON signal_data(signal_date);
CREATE INDEX IF NOT EXISTS idx_signal_ticker ON signal_data(ticker);
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _parse_date(s: str, formats: list[str] | None = None) -> Optional[datetime]:
    """Try multiple date formats, return None on failure."""
    if not s or s.strip() in ("", "--", "N/A", "n/a"):
        return None
    formats = formats or [
        "%m/%d/%Y",
        "%Y-%m-%d",
        "%d-%b-%Y",
        "%m/%d/%Y %I:%M:%S %p",
        "%Y-%m-%dT%H:%M:%S",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            continue
    return None


def _parse_amount_range(s: str) -> tuple[float, float]:
    """Parse congressional trade amount ranges like '$1,001 - $15,000'."""
    if not s or s.strip() in ("", "--"):
        return (0.0, 0.0)
    s = s.replace("$", "").replace(",", "").replace("Over ", "")
    parts = s.split(" - ")
    try:
        lo = float(parts[0].strip())
        hi = float(parts[1].strip()) if len(parts) > 1 else lo
        return (lo, hi)
    except (ValueError, IndexError):
        return (0.0, 0.0)


def _tier_from_count(n: int) -> str:
    """Assign tier based on relationship count."""
    if n >= 50:
        return "tier1"
    elif n >= 10:
        return "tier2"
    elif n >= 3:
        return "tier3"
    return "tier4"


# ---------------------------------------------------------------------------
# DatasetParser
# ---------------------------------------------------------------------------
class DatasetParser:
    """Unified parser for all GRID intelligence datasets."""

    def __init__(self, engine, datasets_dir: str = "/data/datasets"):
        self.engine = engine
        self.datasets_dir = datasets_dir
        self._ensure_tables()

    def _ensure_tables(self):
        """Create tables if they do not exist."""
        log.info("Ensuring intelligence tables exist")
        for statement in SCHEMA_DDL.split(";"):
            stmt = statement.strip()
            if not stmt:
                continue
            try:
                with self.engine.begin() as conn:
                    conn.execute(text(stmt))
            except Exception as exc:
                log.debug("DDL skip (likely exists): {e}", e=str(exc)[:80])
        log.info("Tables ready")

    # ------------------------------------------------------------------
    # 1. ICIJ Offshore Leaks (full OLDB dump)
    # ------------------------------------------------------------------
    def parse_icij(self) -> dict:
        """Parse ICIJ full offshore leaks database (nodes + relationships).

        Sources: icij_full_oldb.zip  (nodes-entities.csv, nodes-officers.csv,
                 nodes-intermediaries.csv, nodes-others.csv, relationships.csv)

        Returns dict with counts of entities, officers, relationships loaded.
        """
        zip_path = os.path.join(self.datasets_dir, "icij_full_oldb.zip")
        if not os.path.exists(zip_path):
            log.warning("ICIJ ZIP not found at {p}", p=zip_path)
            return {"error": "file not found"}

        stats = {"entities": 0, "officers": 0, "intermediaries": 0,
                 "others": 0, "relationships": 0, "skipped": 0}

        log.info("=== Parsing ICIJ Offshore Leaks ===")
        t0 = time.time()

        # First pass: count relationships per node to assign tiers
        log.info("Pass 1: counting relationships per node for tier assignment")
        rel_counts: dict[str, int] = defaultdict(int)
        with zipfile.ZipFile(zip_path) as zf:
            with zf.open("relationships.csv") as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
                for row in reader:
                    n1 = row.get("node_id_start", "")
                    n2 = row.get("node_id_end", "")
                    if n1:
                        rel_counts[n1] += 1
                    if n2:
                        rel_counts[n2] += 1
        log.info("Counted relationships for {n} unique nodes", n=len(rel_counts))

        # Second pass: load nodes
        node_files = [
            ("nodes-entities.csv", "icij_entity", "entities"),
            ("nodes-officers.csv", "icij_officer", "officers"),
            ("nodes-intermediaries.csv", "icij_intermediary", "intermediaries"),
            ("nodes-others.csv", "icij_other", "others"),
        ]

        with zipfile.ZipFile(zip_path) as zf:
            for csv_name, category, stat_key in node_files:
                if csv_name not in zf.namelist():
                    log.warning("File {f} not in ZIP, skipping", f=csv_name)
                    continue
                log.info("Loading {f}...", f=csv_name)
                batch = []
                with zf.open(csv_name) as f:
                    reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
                    for row in reader:
                        node_id = row.get("node_id", "")
                        name = row.get("name", "").strip()
                        if not node_id or not name:
                            stats["skipped"] += 1
                            continue

                        tier = _tier_from_count(rel_counts.get(node_id, 0))
                        country = row.get("countries", row.get("country_codes", ""))
                        jurisdiction = row.get("jurisdiction_description",
                                               row.get("jurisdiction", ""))
                        source = row.get("sourceID", "ICIJ")

                        meta = {}
                        for k in ("company_type", "status", "incorporation_date",
                                  "inactivation_date", "address", "internal_id",
                                  "original_name", "former_name", "note",
                                  "valid_until", "service_provider"):
                            v = row.get(k, "")
                            if v:
                                meta[k] = v

                        batch.append({
                            "id": f"{category}_{node_id}",
                            "name": name,
                            "category": category,
                            "tier": tier,
                            "country": country[:255] if country else None,
                            "jurisdiction": jurisdiction[:255] if jurisdiction else None,
                            "source_id": source,
                            "metadata": json.dumps(meta),
                        })

                        if len(batch) >= 1000:
                            self._insert_actors_batch(batch)
                            stats[stat_key] += len(batch)
                            batch = []

                    if batch:
                        self._insert_actors_batch(batch)
                        stats[stat_key] += len(batch)

                log.info("  {k}: {n} loaded", k=stat_key, n=stats[stat_key])

        # Third pass: load relationships
        log.info("Loading relationships...")
        batch = []
        with zipfile.ZipFile(zip_path) as zf:
            with zf.open("relationships.csv") as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
                for row in reader:
                    n1 = row.get("node_id_start", "")
                    n2 = row.get("node_id_end", "")
                    rel = row.get("rel_type", row.get("link", "unknown"))
                    if not n1 or not n2:
                        stats["skipped"] += 1
                        continue

                    # Determine actor_a/b prefix from node_id ranges
                    try:
                        n1_int = int(n1)
                    except ValueError:
                        n1_int = 0
                    try:
                        n2_int = int(n2)
                    except ValueError:
                        n2_int = 0

                    def _prefix(nid: int) -> str:
                        if nid < 12000000:
                            return "icij_entity"
                        elif nid < 13000000:
                            return "icij_officer"
                        elif nid < 14000000:
                            return "icij_intermediary"
                        else:
                            return "icij_other"

                    actor_a = f"{_prefix(n1_int)}_{n1}"
                    actor_b = f"{_prefix(n2_int)}_{n2}"
                    source = row.get("sourceID", "ICIJ")

                    batch.append({
                        "actor_a": actor_a,
                        "actor_b": actor_b,
                        "relationship": rel,
                        "strength": 0.5,
                        "source_id": source,
                        "metadata": json.dumps({
                            k: row.get(k, "") for k in ("status", "start_date", "end_date")
                            if row.get(k, "")
                        }),
                    })

                    if len(batch) >= 1000:
                        self._insert_relationships_batch(batch)
                        stats["relationships"] += len(batch)
                        batch = []

                if batch:
                    self._insert_relationships_batch(batch)
                    stats["relationships"] += len(batch)

        elapsed = time.time() - t0
        log.info("ICIJ done in {t:.1f}s — {s}", t=elapsed, s=stats)
        return stats

    # ------------------------------------------------------------------
    # 2. OpenSanctions (JSONL, 2.5 GB — streamed)
    # ------------------------------------------------------------------
    def parse_opensanctions(self, limit: Optional[int] = None) -> dict:
        """Stream-parse OpenSanctions JSONL.

        Only loads Person and LegalEntity schemas. Skips Occupancy, Address, etc.
        Batch-inserts 1000 at a time. Never loads full file into memory.

        Args:
            limit: Max records to process (None = all).
        """
        path = os.path.join(self.datasets_dir, "opensanctions_default.json")
        if not os.path.exists(path):
            log.warning("OpenSanctions file not found at {p}", p=path)
            return {"error": "file not found"}

        log.info("=== Parsing OpenSanctions (streaming) ===")
        t0 = time.time()
        stats = {"persons": 0, "entities": 0, "skipped": 0, "errors": 0}
        batch = []
        processed = 0

        PERSON_SCHEMAS = {"Person", "CryptoWallet", "Company", "Organization",
                          "LegalEntity", "PublicBody"}

        with open(path, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                if limit and processed >= limit:
                    break

                line = line.strip()
                if not line:
                    continue

                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    stats["errors"] += 1
                    continue

                schema = obj.get("schema", "")
                if schema not in PERSON_SCHEMAS:
                    stats["skipped"] += 1
                    continue

                props = obj.get("properties", {})
                name = (props.get("name") or props.get("caption") or [obj.get("caption", "")])[0] \
                    if isinstance(props.get("name"), list) else obj.get("caption", "")

                if not name or len(name) < 2:
                    stats["skipped"] += 1
                    continue

                datasets = obj.get("datasets", [])
                is_sanctioned = any("sanction" in d.lower() for d in datasets)
                is_pep = any("pep" in d.lower() for d in datasets)
                if is_sanctioned:
                    category = "sanctioned"
                elif is_pep:
                    category = "pep"
                elif schema == "Person":
                    category = "person_of_interest"
                else:
                    category = "entity_of_interest"

                countries = props.get("country", props.get("nationality", []))
                country = countries[0] if isinstance(countries, list) and countries else ""

                entity_id = obj.get("id", f"os_{line_num}")

                meta = {
                    "datasets": datasets,
                    "schema": schema,
                    "first_seen": obj.get("first_seen"),
                    "last_seen": obj.get("last_seen"),
                }
                # Add select properties
                for prop_key in ("birthDate", "idNumber", "topics", "position",
                                 "program", "authority"):
                    val = props.get(prop_key)
                    if val:
                        meta[prop_key] = val

                tier = "tier2" if is_sanctioned else ("tier3" if is_pep else "tier4")

                batch.append({
                    "id": f"os_{entity_id}",
                    "name": name[:500],
                    "category": category,
                    "tier": tier,
                    "country": country[:255] if country else None,
                    "jurisdiction": None,
                    "source_id": "OpenSanctions",
                    "metadata": json.dumps(meta),
                })
                processed += 1

                if schema == "Person":
                    stats["persons"] += 1
                else:
                    stats["entities"] += 1

                if len(batch) >= 1000:
                    self._insert_actors_batch(batch)
                    batch = []
                    if processed % 50000 == 0:
                        log.info("  OpenSanctions: {n} processed...", n=processed)

        if batch:
            self._insert_actors_batch(batch)

        elapsed = time.time() - t0
        log.info("OpenSanctions done in {t:.1f}s — {s}", t=elapsed, s=stats)
        return stats

    # ------------------------------------------------------------------
    # 3. Congressional Trades (Senate + House)
    # ------------------------------------------------------------------
    def parse_congressional_trades(self) -> dict:
        """Parse Senate and House trading disclosures.

        Senate: /data/datasets/senate_trades/aggregate/all_transactions.json
        House:  /data/datasets/house_trades.json (XML error = skip)

        Large trades (>$250K midpoint) also create signal_data entries.
        """
        log.info("=== Parsing Congressional Trades ===")
        t0 = time.time()
        stats = {"senate": 0, "house": 0, "signals": 0, "errors": 0}

        # --- Senate ---
        senate_path = os.path.join(
            self.datasets_dir, "senate_trades", "aggregate", "all_transactions.json"
        )
        if os.path.exists(senate_path):
            try:
                with open(senate_path, "r", encoding="utf-8") as f:
                    trades = json.load(f)

                snap_batch = []
                sig_batch = []

                for trade in trades:
                    tx_date = _parse_date(trade.get("transaction_date", ""))
                    senator = trade.get("senator", "unknown")
                    ticker = trade.get("ticker", "")
                    tx_type = trade.get("type", "")
                    amount_str = trade.get("amount", "")
                    lo, hi = _parse_amount_range(amount_str)
                    midpoint = (lo + hi) / 2.0

                    snap_batch.append({
                        "category": "congressional_trade",
                        "snapshot_date": tx_date.date() if tx_date else None,
                        "actor": senator,
                        "ticker": ticker if ticker else None,
                        "title": f"{senator} — {tx_type}",
                        "summary": (f"{tx_type} {ticker or trade.get('asset_description','')} "
                                    f"({amount_str}) by {trade.get('owner','self')}"),
                        "data": json.dumps({
                            "type": tx_type,
                            "amount_range": amount_str,
                            "amount_lo": lo,
                            "amount_hi": hi,
                            "owner": trade.get("owner", ""),
                            "asset_type": trade.get("asset_type", ""),
                            "asset_description": trade.get("asset_description", ""),
                            "comment": trade.get("comment", ""),
                            "ptr_link": trade.get("ptr_link", ""),
                            "chamber": "senate",
                        }),
                        "confidence": "confirmed",
                        "source_id": "senate_efds",
                    })

                    # Signal for large trades
                    if midpoint >= 250000 and ticker:
                        direction = "buy" if "purchase" in tx_type.lower() else "sell"
                        sig_batch.append({
                            "signal_type": "congressional_large_trade",
                            "signal_date": tx_date.date() if tx_date else None,
                            "ticker": ticker,
                            "actor": senator,
                            "direction": direction,
                            "magnitude": midpoint,
                            "description": (f"Senator {senator} {direction} {ticker} "
                                            f"({amount_str})"),
                            "data": json.dumps({
                                "chamber": "senate",
                                "type": tx_type,
                                "amount_range": amount_str,
                            }),
                            "confidence": "confirmed",
                            "source_id": "senate_efds",
                        })

                    if len(snap_batch) >= 1000:
                        self._insert_snapshots_batch(snap_batch)
                        stats["senate"] += len(snap_batch)
                        snap_batch = []

                    if len(sig_batch) >= 500:
                        self._insert_signals_batch(sig_batch)
                        stats["signals"] += len(sig_batch)
                        sig_batch = []

                if snap_batch:
                    self._insert_snapshots_batch(snap_batch)
                    stats["senate"] += len(snap_batch)
                if sig_batch:
                    self._insert_signals_batch(sig_batch)
                    stats["signals"] += len(sig_batch)

                log.info("  Senate trades: {n}", n=stats["senate"])

            except Exception as e:
                log.error("Senate parse error: {e}", e=str(e))
                stats["errors"] += 1

        # --- House ---
        house_path = os.path.join(self.datasets_dir, "house_trades.json")
        if os.path.exists(house_path):
            try:
                # Check if file is actually JSON (it was XML/error on server)
                with open(house_path, "r", encoding="utf-8") as f:
                    first_bytes = f.read(100)
                if first_bytes.strip().startswith("<?xml") or first_bytes.strip().startswith("<"):
                    log.warning("house_trades.json is XML/error response, skipping")
                else:
                    with open(house_path, "r", encoding="utf-8") as f2:
                        trades = json.load(f2)
                    snap_batch = []
                    sig_batch = []

                    for trade in trades:
                        tx_date = _parse_date(
                            trade.get("transaction_date",
                                       trade.get("disclosure_date", ""))
                        )
                        rep = trade.get("representative", trade.get("name", "unknown"))
                        ticker = trade.get("ticker", "")
                        tx_type = trade.get("type", trade.get("transaction_type", ""))
                        amount_str = trade.get("amount", "")
                        lo, hi = _parse_amount_range(amount_str)
                        midpoint = (lo + hi) / 2.0

                        snap_batch.append({
                            "category": "congressional_trade",
                            "snapshot_date": tx_date.date() if tx_date else None,
                            "actor": rep,
                            "ticker": ticker if ticker else None,
                            "title": f"{rep} — {tx_type}",
                            "summary": (f"{tx_type} {ticker or trade.get('asset_description','')} "
                                        f"({amount_str})"),
                            "data": json.dumps({
                                "type": tx_type,
                                "amount_range": amount_str,
                                "amount_lo": lo,
                                "amount_hi": hi,
                                "asset_description": trade.get("asset_description", ""),
                                "chamber": "house",
                            }),
                            "confidence": "confirmed",
                            "source_id": "house_disclosures",
                        })

                        if midpoint >= 250000 and ticker:
                            direction = "buy" if "purchase" in tx_type.lower() else "sell"
                            sig_batch.append({
                                "signal_type": "congressional_large_trade",
                                "signal_date": tx_date.date() if tx_date else None,
                                "ticker": ticker,
                                "actor": rep,
                                "direction": direction,
                                "magnitude": midpoint,
                                "description": f"Rep {rep} {direction} {ticker} ({amount_str})",
                                "data": json.dumps({
                                    "chamber": "house",
                                    "type": tx_type,
                                    "amount_range": amount_str,
                                }),
                                "confidence": "confirmed",
                                "source_id": "house_disclosures",
                            })

                        if len(snap_batch) >= 1000:
                            self._insert_snapshots_batch(snap_batch)
                            stats["house"] += len(snap_batch)
                            snap_batch = []

                    if snap_batch:
                        self._insert_snapshots_batch(snap_batch)
                        stats["house"] += len(snap_batch)
                    if sig_batch:
                        self._insert_signals_batch(sig_batch)
                        stats["signals"] += len(sig_batch)

                    log.info("  House trades: {n}", n=stats["house"])

            except Exception as e:
                log.error("House parse error: {e}", e=str(e))
                stats["errors"] += 1
        else:
            log.warning("house_trades.json not found")

        elapsed = time.time() - t0
        log.info("Congressional trades done in {t:.1f}s — {s}", t=elapsed, s=stats)
        return stats

    # ------------------------------------------------------------------
    # 4. FEC Contributions (pipe-delimited, 4 GB ZIP — streamed)
    # ------------------------------------------------------------------
    def parse_fec(self, min_amount: int = 10000) -> dict:
        """Parse FEC individual contributions from ZIP.

        Streams the pipe-delimited itcont.txt line by line.
        Aggregates by donor, then stores donors exceeding min_amount
        as actors with category='donor'.

        FEC pipe-delimited fields (itcont.txt):
        0: CMTE_ID, 1: AMNDT_IND, 2: RPT_TP, 3: TRANSACTION_PGI,
        4: IMAGE_NUM, 5: TRANSACTION_TP, 6: ENTITY_TP, 7: NAME,
        8: CITY, 9: STATE, 10: ZIP_CODE, 11: EMPLOYER, 12: OCCUPATION,
        13: TRANSACTION_DT, 14: TRANSACTION_AMT, 15: OTHER_ID,
        16: TRAN_ID, 17: FILE_NUM, 18: MEMO_CD, 19: MEMO_TEXT,
        20: SUB_ID
        """
        zip_path = os.path.join(self.datasets_dir, "fec_contributions_2024.zip")
        if not os.path.exists(zip_path):
            log.warning("FEC ZIP not found at {p}", p=zip_path)
            return {"error": "file not found"}

        log.info("=== Parsing FEC Contributions (min_amount=${m:,}) ===", m=min_amount)
        t0 = time.time()

        # Aggregate by donor name+state
        donors: dict[str, dict] = {}
        line_count = 0
        error_count = 0

        with zipfile.ZipFile(zip_path) as zf:
            with zf.open("itcont.txt") as f:
                for raw_line in f:
                    line_count += 1
                    try:
                        line = raw_line.decode("utf-8", errors="replace").strip()
                        if not line:
                            continue
                        fields = line.split("|")
                        if len(fields) < 15:
                            continue

                        name = fields[7].strip()
                        if not name or name in ("", "N/A"):
                            continue

                        cmte_id = fields[0].strip()
                        state = fields[9].strip()
                        city = fields[8].strip()
                        employer = fields[11].strip()
                        occupation = fields[12].strip()

                        try:
                            amount = float(fields[14].strip())
                        except (ValueError, IndexError):
                            continue

                        # Skip negative/refund amounts
                        if amount <= 0:
                            continue

                        key = f"{name}|{state}"
                        if key not in donors:
                            donors[key] = {
                                "name": name,
                                "state": state,
                                "city": city,
                                "employer": employer,
                                "occupation": occupation,
                                "total": 0.0,
                                "count": 0,
                                "committees": set(),
                                "max_single": 0.0,
                            }

                        d = donors[key]
                        d["total"] += amount
                        d["count"] += 1
                        d["committees"].add(cmte_id)
                        if amount > d["max_single"]:
                            d["max_single"] = amount

                    except Exception:
                        error_count += 1
                        continue

                    if line_count % 1000000 == 0:
                        log.info("  FEC: {n:,} lines processed, {d:,} unique donors so far",
                                 n=line_count, d=len(donors))

        log.info("FEC aggregation complete: {n:,} lines, {d:,} unique donors, {e} errors",
                 n=line_count, d=len(donors), e=error_count)

        # Filter and insert qualifying donors
        batch = []
        qualifying = 0
        for key, d in donors.items():
            if d["total"] < min_amount:
                continue

            qualifying += 1
            # Tier by total contributions
            if d["total"] >= 1_000_000:
                tier = "tier1"
            elif d["total"] >= 100_000:
                tier = "tier2"
            elif d["total"] >= 50_000:
                tier = "tier3"
            else:
                tier = "tier4"

            actor_id = f"fec_donor_{key.replace(' ', '_').replace(',', '').replace('|', '_').lower()}"
            # Truncate long IDs
            if len(actor_id) > 200:
                actor_id = actor_id[:200]

            batch.append({
                "id": actor_id,
                "name": d["name"],
                "category": "donor",
                "tier": tier,
                "country": "US",
                "jurisdiction": d["state"],
                "source_id": "FEC_2024",
                "metadata": json.dumps({
                    "total_contributions": d["total"],
                    "contribution_count": d["count"],
                    "committees": list(d["committees"])[:50],  # cap for sanity
                    "max_single": d["max_single"],
                    "city": d["city"],
                    "employer": d["employer"],
                    "occupation": d["occupation"],
                }),
            })

            if len(batch) >= 1000:
                self._insert_actors_batch(batch)
                batch = []

        if batch:
            self._insert_actors_batch(batch)

        elapsed = time.time() - t0
        stats = {
            "lines_processed": line_count,
            "unique_donors": len(donors),
            "qualifying_donors": qualifying,
            "min_amount": min_amount,
            "errors": error_count,
        }
        log.info("FEC done in {t:.1f}s — {s}", t=elapsed, s=stats)
        return stats

    # ------------------------------------------------------------------
    # 5. OFAC SDN (simple CSV)
    # ------------------------------------------------------------------
    def parse_ofac(self) -> dict:
        """Parse OFAC Specially Designated Nationals list.

        CSV format (no header): ent_num, name, type, program, ...
        Fields separated by commas with -0- for empty values.
        """
        csv_path = os.path.join(self.datasets_dir, "ofac_sdn.csv")
        if not os.path.exists(csv_path):
            log.warning("OFAC SDN CSV not found at {p}", p=csv_path)
            return {"error": "file not found"}

        log.info("=== Parsing OFAC SDN ===")
        t0 = time.time()
        batch = []
        count = 0
        errors = 0

        with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            for row in reader:
                try:
                    if len(row) < 4:
                        continue

                    ent_num = row[0].strip()
                    name = row[1].strip()
                    sdn_type = row[2].strip().replace("-0-", "").strip()
                    country = row[3].strip().replace("-0-", "").strip()

                    if not name or name == "-0-":
                        continue

                    # Remaining fields may contain programs and remarks
                    programs = []
                    remarks = ""
                    for field in row[4:]:
                        val = field.strip()
                        if val and val != "-0-":
                            if "a.k.a." in val.lower() or "d.o.b." in val.lower():
                                remarks = val
                            else:
                                programs.append(val)

                    meta = {}
                    if sdn_type:
                        meta["sdn_type"] = sdn_type
                    if programs:
                        meta["programs"] = programs
                    if remarks:
                        meta["remarks"] = remarks

                    batch.append({
                        "id": f"ofac_sdn_{ent_num}",
                        "name": name,
                        "category": "ofac_sanctioned",
                        "tier": "tier2",
                        "country": country if country else None,
                        "jurisdiction": None,
                        "source_id": "OFAC_SDN",
                        "metadata": json.dumps(meta),
                    })
                    count += 1

                    if len(batch) >= 1000:
                        self._insert_actors_batch(batch)
                        batch = []

                except Exception as e:
                    errors += 1
                    continue

        if batch:
            self._insert_actors_batch(batch)

        elapsed = time.time() - t0
        stats = {"records": count, "errors": errors}
        log.info("OFAC done in {t:.1f}s — {s}", t=elapsed, s=stats)
        return stats

    # ------------------------------------------------------------------
    # 6. Fed Speeches (JSON array)
    # ------------------------------------------------------------------
    def parse_fed_speeches(self) -> dict:
        """Parse Federal Reserve speeches.

        JSON array with fields: d (date), t (title), s (speaker),
        lo (location), l (link), v (video URL).
        """
        json_path = os.path.join(self.datasets_dir, "fed_speeches.json")
        if not os.path.exists(json_path):
            log.warning("Fed speeches JSON not found at {p}", p=json_path)
            return {"error": "file not found"}

        log.info("=== Parsing Fed Speeches ===")
        t0 = time.time()

        with open(json_path, "r", encoding="utf-8-sig") as f:
            speeches = json.load(f)

        batch = []
        count = 0

        for speech in speeches:
            date_str = speech.get("d", "")
            title = speech.get("t", "")
            speaker = speech.get("s", "")
            location = speech.get("lo", "")
            link = speech.get("l", "")
            video = speech.get("v", "")

            if not title:
                continue

            speech_date = _parse_date(date_str)

            batch.append({
                "category": "fed_speech",
                "snapshot_date": speech_date.date() if speech_date else None,
                "actor": speaker,
                "ticker": None,
                "title": title[:500],
                "summary": f"{speaker} at {location}" if location else speaker,
                "data": json.dumps({
                    "location": location,
                    "link": f"https://www.federalreserve.gov{link}" if link else "",
                    "video": video if video else "",
                    "has_audio": speech.get("a", "") != "",
                }),
                "confidence": "confirmed",
                "source_id": "fed_speeches",
            })
            count += 1

            if len(batch) >= 500:
                self._insert_snapshots_batch(batch)
                batch = []

        if batch:
            self._insert_snapshots_batch(batch)

        elapsed = time.time() - t0
        stats = {"speeches": count}
        log.info("Fed speeches done in {t:.1f}s — {s}", t=elapsed, s=stats)
        return stats

    # ------------------------------------------------------------------
    # Batch insert helpers
    # ------------------------------------------------------------------
    def _insert_actors_batch(self, batch: list[dict]):
        """Upsert a batch of actors."""
        if not batch:
            return
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO actors (id, name, category, tier, country,
                                           jurisdiction, source_id, metadata)
                        VALUES (:id, :name, :category, :tier, :country,
                                :jurisdiction, :source_id, :metadata::jsonb)
                        ON CONFLICT (id) DO UPDATE SET
                            name = EXCLUDED.name,
                            category = EXCLUDED.category,
                            tier = EXCLUDED.tier,
                            country = COALESCE(EXCLUDED.country, actors.country),
                            jurisdiction = COALESCE(EXCLUDED.jurisdiction, actors.jurisdiction),
                            metadata = actors.metadata || EXCLUDED.metadata::jsonb,
                            updated_at = now()
                    """),
                    batch,
                )
        except Exception as e:
            log.error("Actors batch insert error: {e}", e=str(e))

    def _insert_relationships_batch(self, batch: list[dict]):
        """Insert a batch of entity relationships."""
        if not batch:
            return
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO entity_relationships
                            (actor_a, actor_b, relationship, strength, source_id, metadata)
                        VALUES (:actor_a, :actor_b, :relationship, :strength,
                                :source_id, :metadata::jsonb)
                        ON CONFLICT (actor_a, actor_b, relationship, source_id)
                        DO NOTHING
                    """),
                    batch,
                )
        except Exception as e:
            log.error("Relationships batch insert error: {e}", e=str(e))

    def _insert_snapshots_batch(self, batch: list[dict]):
        """Insert a batch of analytical snapshots."""
        if not batch:
            return
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO analytical_snapshots
                            (category, snapshot_date, actor, ticker, title,
                             summary, data, confidence, source_id)
                        VALUES (:category, :snapshot_date, :actor, :ticker, :title,
                                :summary, :data::jsonb, :confidence, :source_id)
                    """),
                    batch,
                )
        except Exception as e:
            log.error("Snapshots batch insert error: {e}", e=str(e))

    def _insert_signals_batch(self, batch: list[dict]):
        """Insert a batch of signal data entries."""
        if not batch:
            return
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO signal_data
                            (signal_type, signal_date, ticker, actor, direction,
                             magnitude, description, data, confidence, source_id)
                        VALUES (:signal_type, :signal_date, :ticker, :actor, :direction,
                                :magnitude, :description, :data::jsonb, :confidence,
                                :source_id)
                    """),
                    batch,
                )
        except Exception as e:
            log.error("Signals batch insert error: {e}", e=str(e))

    # ------------------------------------------------------------------
    # Run all parsers
    # ------------------------------------------------------------------
    def parse_all(self) -> dict:
        """Run every parser in sequence, collecting results."""
        results = {}
        for name, method, kwargs in [
            ("icij", self.parse_icij, {}),
            ("opensanctions", self.parse_opensanctions, {}),
            ("congressional_trades", self.parse_congressional_trades, {}),
            ("fec", self.parse_fec, {}),
            ("ofac", self.parse_ofac, {}),
            ("fed_speeches", self.parse_fed_speeches, {}),
        ]:
            log.info("--- Running parser: {n} ---", n=name)
            try:
                results[name] = method(**kwargs)
            except Exception as e:
                log.error("Parser {n} failed: {e}", n=name, e=str(e))
                results[name] = {"error": str(e)}
        return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="GRID Dataset Parser — load intelligence datasets into PostgreSQL"
    )
    parser.add_argument(
        "dataset",
        nargs="?",
        default="all",
        choices=["all", "icij", "sanctions", "trades", "fec", "ofac", "speeches"],
        help="Which dataset to parse (default: all)",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Limit records for OpenSanctions streaming",
    )
    parser.add_argument(
        "--min-amount", type=int, default=10000,
        help="Minimum FEC contribution total to qualify as actor (default: $10,000)",
    )
    parser.add_argument(
        "--datasets-dir", type=str, default="/data/datasets",
        help="Path to datasets directory",
    )

    args = parser.parse_args()

    engine = get_engine()
    dp = DatasetParser(engine, datasets_dir=args.datasets_dir)

    dataset = args.dataset
    t0 = time.time()

    if dataset == "all":
        results = dp.parse_all()
    elif dataset == "icij":
        results = dp.parse_icij()
    elif dataset == "sanctions":
        results = dp.parse_opensanctions(limit=args.limit)
    elif dataset == "trades":
        results = dp.parse_congressional_trades()
    elif dataset == "fec":
        results = dp.parse_fec(min_amount=args.min_amount)
    elif dataset == "ofac":
        results = dp.parse_ofac()
    elif dataset == "speeches":
        results = dp.parse_fed_speeches()
    else:
        log.error("Unknown dataset: {d}", d=dataset)
        sys.exit(1)

    elapsed = time.time() - t0
    log.info("=== COMPLETE in {t:.1f}s ===", t=elapsed)
    log.info("Results: {r}", r=json.dumps(results, indent=2, default=str))


if __name__ == "__main__":
    main()
