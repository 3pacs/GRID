"""
GRID Intelligence — Automated Actor Discovery & Enrichment (250K+ Scale).

Instead of hardcoding ~200 actors, this module continuously discovers new
actors from GRID's ingested data sources and enriches them with cross-
referenced metadata.  Every Form 4 filer, congressional trader, 13F
institution, lobbyist, and government official becomes an actor — and
connections between them are inferred automatically.

Scale targets (v2 — 3-degree graph expansion):
    Phase 1  —   5,000 actors  (all insiders + 13F + congressional)
    Phase 2  —  50,000 actors  (board interlocks + ICIJ officers)
    Phase 3  — 250,000+ actors (full 3-degree graph + all ICIJ entities)

Degree 0 (seed):  489 named actors from actor_network._KNOWN_ACTORS
Degree 1:         ~5,000 directly connected (existing data sources)
Degree 2:         ~50,000 second-hop connections
Degree 3:         ~250,000 third-hop connections

Data sources for scaling:
    - SEC EDGAR Form 4:    ~50,000 unique filers/year (batch historical)
    - ICIJ Offshore Leaks: 785,000+ entities, 540,000+ officers
    - 13F institutional:   ~5,000 filers managing >$100M each
    - USASpending:         contract officers, recipients, sub-contractors
    - Congressional:       535 members + committees + donors + lobbyists
    - Board interlocks:    S&P 500 = ~5,000 board seats, many shared

Key entry points:
    auto_discover_actors        — scan all data sources, create new actors
    auto_discover_connections   — find links between actors
    enrich_actor                — pull all available data for one actor
    enrich_all_actors           — batch enrichment
    run_discovery_cycle         — daily orchestrator for hermes scheduling
    run_scale_discovery         — phased scale-up to 250K+ actors
    batch_discover_insiders     — pull ALL Form 4 filers (full year)
    discover_all_13f_filers     — all ~5,000 institutional investors
    discover_all_congress       — all 535 members of Congress
    import_icij_offshore        — bulk import Panama/Pandora Papers
    discover_board_interlocks   — cross-company board connections
    run_3_degree_expansion      — BFS from seed actors, 3 hops out
    get_actor_stats             — dashboard statistics

Wired into:
    - LLM task queue as P3 background: continuously discover and enrich
    - hermes: run_discovery_cycle() daily
"""

from __future__ import annotations

import csv
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ══════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════════════════

# Phase 1 threshold: only create actors for insiders who traded >$1M total
_INSIDER_MIN_VALUE_PHASE1: float = 1_000_000.0

# Connection window: actors who traded the same ticker within N days
_CO_TRADE_WINDOW_DAYS: int = 14

# Batch sizes
_ENRICHMENT_BATCH: int = 50
_BATCH_INSERT_SIZE: int = 1000  # batch inserts for 250K scale

# ICIJ data directory
_ICIJ_DATA_DIR: str = "/data/grid/bulk/icij"

# ICIJ expected CSV filenames
_ICIJ_CSV_OFFICERS: str = "nodes-officers.csv"
_ICIJ_CSV_ENTITIES: str = "nodes-entities.csv"
_ICIJ_CSV_INTERMEDIARIES: str = "nodes-intermediaries.csv"
_ICIJ_CSV_RELATIONSHIPS: str = "relationships.csv"
_ICIJ_CSV_ADDRESSES: str = "nodes-addresses.csv"

# Scale targets — new phased approach
SCALE_TARGETS: dict[str, int] = {
    "phase_1": 5_000,      # All insiders + 13F + congressional
    "phase_2": 50_000,     # Board interlocks + ICIJ officers
    "phase_3": 250_000,    # Full 3-degree graph + all ICIJ entities
}

# SEC EDGAR full-text search endpoint for discovering ALL filers
_EDGAR_FULL_INDEX_URL: str = "https://efts.sec.gov/LATEST/search-index"
_EDGAR_FORM4_SEARCH: str = "https://efts.sec.gov/LATEST/search-index?q=%224%22&dateRange=custom&startdt={start}&enddt={end}&forms=4"
_EDGAR_13F_SEARCH: str = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=13F-HR&dateb=&owner=include&count=100&search_text=&action=getcompany"

# Influence defaults by tier
_DEFAULT_INFLUENCE: dict[str, float] = {
    "sovereign": 0.90,
    "regional": 0.70,
    "institutional": 0.55,
    "individual": 0.35,
}

# Top 13F filer CIKs (largest by AUM, publicly available from SEC EDGAR)
# This list bootstraps institutional discovery; the system expands it from
# actual 13F filings found in the database.
_SEED_13F_FILERS: dict[str, dict[str, Any]] = {
    "0001067983": {"name": "Berkshire Hathaway", "aum_est": 350_000_000_000},
    "0001350694": {"name": "BlackRock", "aum_est": 10_000_000_000_000},
    "0001061768": {"name": "Vanguard Group", "aum_est": 8_000_000_000_000},
    "0001166559": {"name": "State Street Corp", "aum_est": 4_000_000_000_000},
    "0001364742": {"name": "Bridgewater Associates", "aum_est": 150_000_000_000},
    "0001037389": {"name": "Renaissance Technologies", "aum_est": 130_000_000_000},
    "0001056728": {"name": "Citadel Advisors", "aum_est": 60_000_000_000},
    "0001649339": {"name": "Millennium Management", "aum_est": 60_000_000_000},
    "0001336528": {"name": "D.E. Shaw & Co", "aum_est": 55_000_000_000},
    "0001159159": {"name": "Two Sigma Investments", "aum_est": 50_000_000_000},
    "0001345471": {"name": "AQR Capital Management", "aum_est": 100_000_000_000},
    "0001061165": {"name": "Man Group", "aum_est": 70_000_000_000},
    "0000921669": {"name": "Elliott Management", "aum_est": 55_000_000_000},
    "0001003935": {"name": "Soros Fund Management", "aum_est": 25_000_000_000},
    "0001079114": {"name": "Tiger Global Management", "aum_est": 30_000_000_000},
    "0001135730": {"name": "Point72 Asset Management", "aum_est": 30_000_000_000},
    "0001045810": {"name": "Baupost Group", "aum_est": 25_000_000_000},
    "0001543160": {"name": "Viking Global Investors", "aum_est": 30_000_000_000},
    "0000315066": {"name": "Capital Research Global", "aum_est": 2_500_000_000_000},
    "0000093751": {"name": "Fidelity (FMR LLC)", "aum_est": 4_500_000_000_000},
    "0001037644": {"name": "T. Rowe Price Associates", "aum_est": 1_400_000_000_000},
    "0000764180": {"name": "Wellington Management", "aum_est": 1_000_000_000_000},
    "0001006438": {"name": "Appaloosa Management", "aum_est": 13_000_000_000},
    "0001279708": {"name": "Lone Pine Capital", "aum_est": 20_000_000_000},
    "0001535472": {"name": "Coatue Management", "aum_est": 20_000_000_000},
}


# ══════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════

def _normalize_name(name: str) -> str:
    """Normalise a person/entity name to a stable actor_id slug."""
    name = name.strip().lower()
    name = re.sub(r"\b(jr|sr|ii|iii|iv)\b\.?", "", name)
    name = re.sub(r"[^a-z0-9 ]", "", name)
    name = re.sub(r"\s+", "_", name.strip())
    return name


def _actor_id_insider(name: str) -> str:
    return f"insider_{_normalize_name(name)}"


def _actor_id_congress(name: str) -> str:
    return f"congress_{_normalize_name(name)}"


def _actor_id_13f(cik: str) -> str:
    return f"inst_13f_{cik.lstrip('0') or '0'}"


def _actor_id_lobbyist(name: str) -> str:
    return f"lobbyist_{_normalize_name(name)}"


def _actor_id_gov_official(name: str) -> str:
    return f"gov_{_normalize_name(name)}"


def _ensure_actors_table(engine: Engine) -> None:
    """Ensure the actors and actor_connections tables exist.

    The actors table is created by actor_network._ensure_tables();
    we additionally create actor_connections for relationship tracking.
    """
    with engine.begin() as conn:
        # actors table (created by actor_network, but be safe)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS actors (
                id              TEXT PRIMARY KEY,
                name            TEXT NOT NULL,
                tier            TEXT NOT NULL,
                category        TEXT NOT NULL,
                title           TEXT,
                net_worth_estimate NUMERIC,
                aum             NUMERIC,
                influence_score NUMERIC DEFAULT 0.5,
                trust_score     NUMERIC DEFAULT 0.5,
                motivation_model TEXT DEFAULT 'unknown',
                connections     JSONB DEFAULT '[]',
                known_positions JSONB DEFAULT '[]',
                board_seats     JSONB DEFAULT '[]',
                political_affiliations JSONB DEFAULT '[]',
                data_sources    JSONB DEFAULT '[]',
                credibility     TEXT DEFAULT 'inferred',
                metadata        JSONB DEFAULT '{}',
                updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))

        # Dedicated connections table for scalable relationship tracking
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS actor_connections (
                id              SERIAL PRIMARY KEY,
                actor_a         TEXT NOT NULL,
                actor_b         TEXT NOT NULL,
                relationship    TEXT NOT NULL,
                strength        NUMERIC DEFAULT 0.5,
                evidence        JSONB DEFAULT '[]',
                discovered_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (actor_a, actor_b, relationship)
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_actor_connections_a
                ON actor_connections (actor_a)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_actor_connections_b
                ON actor_connections (actor_b)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_actors_category
                ON actors (category)
        """))

        # ── 250K-scale additions ──────────────────────────────────
        # Add 'source' column to actors for tracking discovery origin
        conn.execute(text("""
            ALTER TABLE actors
                ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'unknown'
        """))
        # Add 'degree' column: 0 = seed, 1/2/3 = expansion hops
        conn.execute(text("""
            ALTER TABLE actors
                ADD COLUMN IF NOT EXISTS degree INT DEFAULT 0
        """))
        # Add 'icij_node_id' for ICIJ cross-reference
        conn.execute(text("""
            ALTER TABLE actors
                ADD COLUMN IF NOT EXISTS icij_node_id TEXT
        """))

        # High-scale indexes for 250K+ rows
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_actors_source
                ON actors (source)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_actors_degree
                ON actors (degree)
        """))
        # Trigram index for fuzzy name matching (requires pg_trgm extension)
        try:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_actors_name_trgm
                    ON actors USING gin (name gin_trgm_ops)
            """))
        except Exception:
            log.debug(
                "pg_trgm extension not available — "
                "skipping trigram index on actors.name",
            )
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_actors_icij_node
                ON actors (icij_node_id) WHERE icij_node_id IS NOT NULL
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_actors_influence
                ON actors (influence_score DESC)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_actors_updated
                ON actors (updated_at DESC)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_actor_connections_rel
                ON actor_connections (relationship)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_actor_connections_strength
                ON actor_connections (strength DESC)
        """))


def _upsert_actor(
    conn: Any,
    actor_id: str,
    name: str,
    tier: str,
    category: str,
    title: str = "",
    influence_score: float = 0.5,
    aum: float | None = None,
    data_sources: list[str] | None = None,
    credibility: str = "public_record",
    motivation_model: str = "unknown",
    metadata: dict | None = None,
) -> bool:
    """Insert or update an actor.  Returns True if newly inserted."""
    result = conn.execute(text("""
        INSERT INTO actors (
            id, name, tier, category, title,
            influence_score, aum,
            data_sources, credibility, motivation_model,
            metadata, updated_at
        ) VALUES (
            :id, :name, :tier, :category, :title,
            :inf, :aum,
            :sources, :cred, :motivation,
            :meta, NOW()
        )
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            title = COALESCE(NULLIF(EXCLUDED.title, ''), actors.title),
            aum = COALESCE(EXCLUDED.aum, actors.aum),
            data_sources = EXCLUDED.data_sources,
            metadata = actors.metadata || EXCLUDED.metadata,
            updated_at = NOW()
        RETURNING (xmax = 0) AS is_insert
    """), {
        "id": actor_id,
        "name": name,
        "tier": tier,
        "category": category,
        "title": title,
        "inf": influence_score,
        "aum": aum,
        "sources": json.dumps(data_sources or []),
        "cred": credibility,
        "motivation": motivation_model,
        "meta": json.dumps(metadata or {}),
    })
    row = result.fetchone()
    return bool(row and row[0])


def _upsert_connection(
    conn: Any,
    actor_a: str,
    actor_b: str,
    relationship: str,
    strength: float = 0.5,
    evidence: list[str] | None = None,
) -> bool:
    """Insert or update a connection.  Returns True if newly inserted."""
    # Canonical ordering to avoid duplicates
    a, b = sorted([actor_a, actor_b])
    result = conn.execute(text("""
        INSERT INTO actor_connections (actor_a, actor_b, relationship, strength, evidence)
        VALUES (:a, :b, :rel, :strength, :evidence)
        ON CONFLICT (actor_a, actor_b, relationship) DO UPDATE SET
            strength = GREATEST(actor_connections.strength, EXCLUDED.strength),
            evidence = actor_connections.evidence || EXCLUDED.evidence
        RETURNING (xmax = 0) AS is_insert
    """), {
        "a": a,
        "b": b,
        "rel": relationship,
        "strength": strength,
        "evidence": json.dumps(evidence or []),
    })
    row = result.fetchone()
    return bool(row and row[0])


# ══════════════════════════════════════════════════════════════════════════
# SOURCE 1: SEC Form 4 Insider Filers
# ══════════════════════════════════════════════════════════════════════════

def _discover_insiders(engine: Engine, min_value: float = 0.0) -> dict:
    """Discover actors from SEC Form 4 filings in raw_series.

    Parameters:
        engine: SQLAlchemy engine.
        min_value: Minimum total dollar value traded to qualify.

    Returns:
        dict with discovered, skipped, errors counts.
    """
    discovered = 0
    skipped = 0

    try:
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT
                    raw_payload->>'insider_name'  AS insider_name,
                    raw_payload->>'insider_title' AS insider_title,
                    raw_payload->>'ticker'        AS ticker,
                    SUM(value)                    AS total_value,
                    COUNT(*)                      AS trade_count
                FROM raw_series
                WHERE series_id LIKE 'INSIDER:%'
                  AND raw_payload->>'insider_name' IS NOT NULL
                GROUP BY
                    raw_payload->>'insider_name',
                    raw_payload->>'insider_title',
                    raw_payload->>'ticker'
                HAVING SUM(ABS(value)) >= :min_val
                ORDER BY SUM(ABS(value)) DESC
            """), {"min_val": min_value}).fetchall()

            for r in rows:
                name = r[0]
                title = r[1] or ""
                ticker = r[2] or ""
                total_val = float(r[3]) if r[3] else 0
                trade_count = int(r[4]) if r[4] else 0

                if not name or not name.strip():
                    skipped += 1
                    continue

                actor_id = _actor_id_insider(name)
                is_new = _upsert_actor(
                    conn=conn,
                    actor_id=actor_id,
                    name=name.strip(),
                    tier="individual",
                    category="insider",
                    title=title,
                    influence_score=min(0.60, 0.30 + (total_val / 50_000_000) * 0.30),
                    data_sources=["form4", "sec_edgar"],
                    credibility="hard_data",
                    motivation_model="informed",
                    metadata={
                        "primary_ticker": ticker,
                        "total_value_traded": total_val,
                        "trade_count": trade_count,
                        "discovery_source": "auto_form4",
                    },
                )
                if is_new:
                    discovered += 1
                else:
                    skipped += 1

    except Exception as exc:
        log.error("Insider discovery failed: {e}", e=str(exc))
        return {"discovered": discovered, "skipped": skipped, "error": str(exc)}

    log.info(
        "Insider discovery: {d} new, {s} existing",
        d=discovered, s=skipped,
    )
    return {"discovered": discovered, "skipped": skipped}


# ══════════════════════════════════════════════════════════════════════════
# SOURCE 2: Congressional Traders
# ══════════════════════════════════════════════════════════════════════════

def _discover_congressional(engine: Engine) -> dict:
    """Discover actors from congressional trading disclosures.

    Returns:
        dict with discovered, skipped counts.
    """
    discovered = 0
    skipped = 0

    try:
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT
                    raw_payload->>'member_name'  AS member_name,
                    raw_payload->>'party'        AS party,
                    raw_payload->>'state'        AS state,
                    raw_payload->>'committee'    AS committee,
                    raw_payload->>'chamber'      AS chamber,
                    SUM(value)                   AS total_value,
                    COUNT(*)                     AS trade_count
                FROM raw_series
                WHERE series_id LIKE 'CONGRESS:%'
                  AND raw_payload->>'member_name' IS NOT NULL
                GROUP BY
                    raw_payload->>'member_name',
                    raw_payload->>'party',
                    raw_payload->>'state',
                    raw_payload->>'committee',
                    raw_payload->>'chamber'
                ORDER BY SUM(ABS(value)) DESC
            """)).fetchall()

            for r in rows:
                name = r[0]
                party = r[1] or ""
                state = r[2] or ""
                committee = r[3] or ""
                chamber = r[4] or ""
                total_val = float(r[5]) if r[5] else 0
                trade_count = int(r[6]) if r[6] else 0

                if not name or not name.strip():
                    skipped += 1
                    continue

                actor_id = _actor_id_congress(name)
                # Committee chairs get regional tier, others individual
                tier = "regional" if "chair" in (committee or "").lower() else "individual"

                is_new = _upsert_actor(
                    conn=conn,
                    actor_id=actor_id,
                    name=name.strip(),
                    tier=tier,
                    category="politician",
                    title=f"{chamber} ({party}-{state})" if party and state else chamber,
                    influence_score=_DEFAULT_INFLUENCE.get(tier, 0.35),
                    data_sources=["congressional_disclosures"],
                    credibility="public_record",
                    motivation_model="political",
                    metadata={
                        "party": party,
                        "state": state,
                        "committee": committee,
                        "chamber": chamber,
                        "total_value_traded": total_val,
                        "trade_count": trade_count,
                        "discovery_source": "auto_congressional",
                    },
                )
                if is_new:
                    discovered += 1
                else:
                    skipped += 1

    except Exception as exc:
        log.error("Congressional discovery failed: {e}", e=str(exc))
        return {"discovered": discovered, "skipped": skipped, "error": str(exc)}

    log.info(
        "Congressional discovery: {d} new, {s} existing",
        d=discovered, s=skipped,
    )
    return {"discovered": discovered, "skipped": skipped}


# ══════════════════════════════════════════════════════════════════════════
# SOURCE 3: 13F Institutional Filers
# ══════════════════════════════════════════════════════════════════════════

def _discover_13f_filers(engine: Engine) -> dict:
    """Discover actors from 13F filings in raw_series + seed list.

    Returns:
        dict with discovered, skipped counts.
    """
    discovered = 0
    skipped = 0

    try:
        with engine.begin() as conn:
            # Seed the known large 13F filers
            for cik, info in _SEED_13F_FILERS.items():
                actor_id = _actor_id_13f(cik)
                is_new = _upsert_actor(
                    conn=conn,
                    actor_id=actor_id,
                    name=info["name"],
                    tier="institutional",
                    category="fund",
                    title=f"13F Filer (CIK {cik})",
                    influence_score=min(
                        0.85,
                        0.50 + (info.get("aum_est", 0) / 15_000_000_000_000) * 0.35,
                    ),
                    aum=info.get("aum_est"),
                    data_sources=["sec_13f", "sec_edgar"],
                    credibility="hard_data",
                    motivation_model="alpha_seeking",
                    metadata={
                        "cik": cik,
                        "aum_estimate": info.get("aum_est"),
                        "discovery_source": "seed_13f",
                    },
                )
                if is_new:
                    discovered += 1
                else:
                    skipped += 1

            # Discover additional 13F filers from raw_series
            rows = conn.execute(text("""
                SELECT
                    SPLIT_PART(series_id, ':', 2) AS manager_cik,
                    COUNT(DISTINCT SPLIT_PART(series_id, ':', 3)) AS tickers_held,
                    COUNT(*) AS filings
                FROM raw_series
                WHERE series_id LIKE '13F:%'
                GROUP BY SPLIT_PART(series_id, ':', 2)
                ORDER BY COUNT(*) DESC
                LIMIT 500
            """)).fetchall()

            for r in rows:
                cik = str(r[0]).strip()
                if not cik:
                    continue
                tickers = int(r[1]) if r[1] else 0
                filings = int(r[2]) if r[2] else 0

                actor_id = _actor_id_13f(cik)

                # Try to get name from existing filings
                name_row = conn.execute(text("""
                    SELECT raw_payload->>'manager_name'
                    FROM raw_series
                    WHERE series_id LIKE :prefix
                      AND raw_payload->>'manager_name' IS NOT NULL
                    LIMIT 1
                """), {"prefix": f"13F:{cik}:%"}).fetchone()

                name = name_row[0] if name_row and name_row[0] else f"13F Filer {cik}"

                is_new = _upsert_actor(
                    conn=conn,
                    actor_id=actor_id,
                    name=name,
                    tier="institutional",
                    category="fund",
                    title=f"13F Institutional Filer (CIK {cik})",
                    influence_score=min(0.70, 0.40 + (tickers / 500) * 0.30),
                    data_sources=["sec_13f"],
                    credibility="hard_data",
                    motivation_model="alpha_seeking",
                    metadata={
                        "cik": cik,
                        "tickers_held": tickers,
                        "filing_count": filings,
                        "discovery_source": "auto_13f",
                    },
                )
                if is_new:
                    discovered += 1
                else:
                    skipped += 1

    except Exception as exc:
        log.error("13F discovery failed: {e}", e=str(exc))
        return {"discovered": discovered, "skipped": skipped, "error": str(exc)}

    log.info(
        "13F discovery: {d} new, {s} existing",
        d=discovered, s=skipped,
    )
    return {"discovered": discovered, "skipped": skipped}


# ══════════════════════════════════════════════════════════════════════════
# SOURCE 4: Board Cross-References (interlocking directorates)
# ══════════════════════════════════════════════════════════════════════════

def _discover_board_crossrefs(engine: Engine) -> dict:
    """Find insiders who filed Form 4 for multiple companies.

    When an insider appears in filings for Company A and Company B,
    they likely sit on both boards — this builds the interlocking
    directorate map.

    Returns:
        dict with connections_found, actors_linked counts.
    """
    connections_found = 0
    actors_linked = set()

    try:
        with engine.begin() as conn:
            # Find insiders who appear in filings for multiple tickers
            rows = conn.execute(text("""
                SELECT
                    raw_payload->>'insider_name' AS insider_name,
                    ARRAY_AGG(DISTINCT raw_payload->>'ticker') AS tickers,
                    COUNT(DISTINCT raw_payload->>'ticker') AS ticker_count
                FROM raw_series
                WHERE series_id LIKE 'INSIDER:%'
                  AND raw_payload->>'insider_name' IS NOT NULL
                GROUP BY raw_payload->>'insider_name'
                HAVING COUNT(DISTINCT raw_payload->>'ticker') >= 2
                ORDER BY COUNT(DISTINCT raw_payload->>'ticker') DESC
                LIMIT 500
            """)).fetchall()

            for r in rows:
                name = r[0]
                tickers = r[1] if r[1] else []
                if not name or len(tickers) < 2:
                    continue

                actor_id = _actor_id_insider(name)

                # Update board_seats metadata
                conn.execute(text("""
                    UPDATE actors
                    SET board_seats = :boards,
                        metadata = metadata || :meta,
                        updated_at = NOW()
                    WHERE id = :aid
                """), {
                    "aid": actor_id,
                    "boards": json.dumps(tickers),
                    "meta": json.dumps({
                        "interlocking_directorate": True,
                        "board_count": len(tickers),
                    }),
                })

                # Create connections between the companies this person links
                for i in range(len(tickers)):
                    for j in range(i + 1, len(tickers)):
                        # Connection between the insider and each company
                        # is implicit; we track the cross-company link
                        is_new = _upsert_connection(
                            conn=conn,
                            actor_a=actor_id,
                            actor_b=f"company_{tickers[j].lower()}",
                            relationship="board_member",
                            strength=0.7,
                            evidence=[
                                f"Form 4 filings for both {tickers[i]} and {tickers[j]}",
                            ],
                        )
                        if is_new:
                            connections_found += 1
                        actors_linked.add(actor_id)

    except Exception as exc:
        log.error("Board cross-reference discovery failed: {e}", e=str(exc))
        return {
            "connections_found": connections_found,
            "actors_linked": len(actors_linked),
            "error": str(exc),
        }

    log.info(
        "Board cross-ref: {c} connections linking {a} actors",
        c=connections_found, a=len(actors_linked),
    )
    return {
        "connections_found": connections_found,
        "actors_linked": len(actors_linked),
    }


# ══════════════════════════════════════════════════════════════════════════
# SOURCE 5: Lobbyist Registrations
# ══════════════════════════════════════════════════════════════════════════

def _discover_lobbyists(engine: Engine) -> dict:
    """Discover actors from lobbying disclosure data.

    Returns:
        dict with discovered, skipped, connections counts.
    """
    discovered = 0
    skipped = 0
    connections = 0

    try:
        with engine.begin() as conn:
            # Lobbyists appear as registrant in LOBBYING: series
            rows = conn.execute(text("""
                SELECT
                    raw_payload->>'registrant_name' AS registrant,
                    raw_payload->>'client_name'     AS client,
                    raw_payload->>'lobbyist_name'   AS lobbyist,
                    SUM(value)                      AS total_spend,
                    COUNT(*)                        AS filing_count
                FROM raw_series
                WHERE series_id LIKE 'LOBBYING:%'
                  AND raw_payload IS NOT NULL
                GROUP BY
                    raw_payload->>'registrant_name',
                    raw_payload->>'client_name',
                    raw_payload->>'lobbyist_name'
                ORDER BY SUM(value) DESC
                LIMIT 1000
            """)).fetchall()

            seen_lobbyists: set[str] = set()
            seen_clients: set[str] = set()

            for r in rows:
                registrant = r[0] or ""
                client = r[1] or ""
                lobbyist = r[2] or ""
                total_spend = float(r[3]) if r[3] else 0
                filing_count = int(r[4]) if r[4] else 0

                # Create actor for the lobbying firm (registrant)
                if registrant and registrant not in seen_lobbyists:
                    seen_lobbyists.add(registrant)
                    actor_id = _actor_id_lobbyist(registrant)
                    is_new = _upsert_actor(
                        conn=conn,
                        actor_id=actor_id,
                        name=registrant.strip(),
                        tier="individual",
                        category="lobbyist",
                        title=f"Lobbying Firm ({filing_count} filings)",
                        influence_score=min(0.55, 0.25 + (total_spend / 50_000_000) * 0.30),
                        data_sources=["lobbying_disclosure", "senate_lda"],
                        credibility="public_record",
                        motivation_model="influence_peddling",
                        metadata={
                            "total_lobbying_spend": total_spend,
                            "filing_count": filing_count,
                            "discovery_source": "auto_lobbying",
                        },
                    )
                    if is_new:
                        discovered += 1

                # Create actor for named lobbyists (individuals)
                if lobbyist and lobbyist not in seen_lobbyists:
                    seen_lobbyists.add(lobbyist)
                    lobbyist_aid = _actor_id_lobbyist(lobbyist)
                    is_new = _upsert_actor(
                        conn=conn,
                        actor_id=lobbyist_aid,
                        name=lobbyist.strip(),
                        tier="individual",
                        category="lobbyist",
                        title=f"Registered Lobbyist at {registrant}",
                        influence_score=0.30,
                        data_sources=["lobbying_disclosure"],
                        credibility="public_record",
                        motivation_model="influence_peddling",
                        metadata={
                            "employer": registrant,
                            "discovery_source": "auto_lobbying",
                        },
                    )
                    if is_new:
                        discovered += 1

                    # Link lobbyist to their firm
                    if registrant:
                        is_new_conn = _upsert_connection(
                            conn=conn,
                            actor_a=lobbyist_aid,
                            actor_b=_actor_id_lobbyist(registrant),
                            relationship="employed_by",
                            strength=0.8,
                            evidence=[f"Lobbying disclosure: {lobbyist} at {registrant}"],
                        )
                        if is_new_conn:
                            connections += 1

                # Link lobbying firm to client company
                if registrant and client and client not in seen_clients:
                    seen_clients.add(f"{registrant}:{client}")
                    is_new_conn = _upsert_connection(
                        conn=conn,
                        actor_a=_actor_id_lobbyist(registrant),
                        actor_b=f"company_{_normalize_name(client)}",
                        relationship="lobbies_for",
                        strength=min(0.7, 0.3 + (total_spend / 20_000_000) * 0.4),
                        evidence=[f"Lobbying filing: {registrant} -> {client}"],
                    )
                    if is_new_conn:
                        connections += 1

    except Exception as exc:
        log.error("Lobbyist discovery failed: {e}", e=str(exc))
        return {
            "discovered": discovered,
            "skipped": skipped,
            "connections": connections,
            "error": str(exc),
        }

    log.info(
        "Lobbyist discovery: {d} new actors, {c} connections",
        d=discovered, c=connections,
    )
    return {"discovered": discovered, "skipped": skipped, "connections": connections}


# ══════════════════════════════════════════════════════════════════════════
# SOURCE 6: Government Officials (from gov_contracts data)
# ══════════════════════════════════════════════════════════════════════════

def _discover_gov_officials(engine: Engine) -> dict:
    """Discover government actors from contract awards data.

    Returns:
        dict with discovered, skipped, connections counts.
    """
    discovered = 0
    skipped = 0
    connections = 0

    try:
        with engine.begin() as conn:
            # Government contracts series store awarding agency info
            rows = conn.execute(text("""
                SELECT
                    raw_payload->>'awarding_agency'     AS agency,
                    raw_payload->>'contracting_officer' AS officer,
                    raw_payload->>'recipient_name'      AS recipient,
                    raw_payload->>'ticker'              AS ticker,
                    SUM(value)                          AS total_value,
                    COUNT(*)                            AS award_count
                FROM raw_series
                WHERE series_id LIKE 'GOV_CONTRACT:%'
                  AND raw_payload IS NOT NULL
                GROUP BY
                    raw_payload->>'awarding_agency',
                    raw_payload->>'contracting_officer',
                    raw_payload->>'recipient_name',
                    raw_payload->>'ticker'
                ORDER BY SUM(value) DESC
                LIMIT 500
            """)).fetchall()

            seen_agencies: set[str] = set()
            seen_officers: set[str] = set()

            for r in rows:
                agency = r[0] or ""
                officer = r[1] or ""
                recipient = r[2] or ""
                ticker = r[3] or ""
                total_val = float(r[4]) if r[4] else 0
                award_count = int(r[5]) if r[5] else 0

                # Create actor for the agency (as an institution)
                if agency and agency not in seen_agencies:
                    seen_agencies.add(agency)
                    agency_aid = _actor_id_gov_official(agency)
                    is_new = _upsert_actor(
                        conn=conn,
                        actor_id=agency_aid,
                        name=agency.strip(),
                        tier="regional",
                        category="government",
                        title=f"Federal Agency ({award_count} awards)",
                        influence_score=min(0.75, 0.50 + (total_val / 10_000_000_000) * 0.25),
                        data_sources=["usaspending", "gov_contracts"],
                        credibility="hard_data",
                        motivation_model="institutional_mandate",
                        metadata={
                            "total_contract_value": total_val,
                            "award_count": award_count,
                            "discovery_source": "auto_gov_contracts",
                        },
                    )
                    if is_new:
                        discovered += 1

                # Create actor for named contracting officer
                if officer and officer not in seen_officers:
                    seen_officers.add(officer)
                    officer_aid = _actor_id_gov_official(officer)
                    is_new = _upsert_actor(
                        conn=conn,
                        actor_id=officer_aid,
                        name=officer.strip(),
                        tier="individual",
                        category="government",
                        title=f"Contracting Officer, {agency}",
                        influence_score=0.40,
                        data_sources=["usaspending"],
                        credibility="public_record",
                        motivation_model="institutional_mandate",
                        metadata={
                            "agency": agency,
                            "total_awards_value": total_val,
                            "discovery_source": "auto_gov_contracts",
                        },
                    )
                    if is_new:
                        discovered += 1

                    # Link officer to agency
                    if agency:
                        is_new_conn = _upsert_connection(
                            conn=conn,
                            actor_a=officer_aid,
                            actor_b=_actor_id_gov_official(agency),
                            relationship="works_at",
                            strength=0.9,
                            evidence=[f"Contract awards from {agency}"],
                        )
                        if is_new_conn:
                            connections += 1

                # Link agency/officer to contractor company
                if agency and ticker:
                    is_new_conn = _upsert_connection(
                        conn=conn,
                        actor_a=_actor_id_gov_official(agency),
                        actor_b=f"company_{ticker.lower()}",
                        relationship="awards_contracts_to",
                        strength=min(0.8, 0.3 + (total_val / 5_000_000_000) * 0.5),
                        evidence=[f"${total_val:,.0f} in contracts to {recipient or ticker}"],
                    )
                    if is_new_conn:
                        connections += 1

    except Exception as exc:
        log.error("Gov official discovery failed: {e}", e=str(exc))
        return {
            "discovered": discovered,
            "skipped": skipped,
            "connections": connections,
            "error": str(exc),
        }

    log.info(
        "Gov official discovery: {d} new actors, {c} connections",
        d=discovered, c=connections,
    )
    return {"discovered": discovered, "skipped": skipped, "connections": connections}


# ══════════════════════════════════════════════════════════════════════════
# AUTO-ENRICHMENT
# ══════════════════════════════════════════════════════════════════════════

def enrich_actor(engine: Engine, actor_id: str) -> dict:
    """Pull all available data for an actor and update their record.

    Aggregates:
      - Trades they have made (insider or congressional)
      - Companies they are affiliated with
      - Government connections
      - Other actors they are connected to
      - Trust score from historical accuracy
      - Dollar flow through them

    Parameters:
        engine: SQLAlchemy engine.
        actor_id: The actor's ID in the actors table.

    Returns:
        dict with enrichment details.
    """
    enrichment: dict[str, Any] = {"actor_id": actor_id}

    try:
        with engine.begin() as conn:
            # Load current actor
            actor_row = conn.execute(text(
                "SELECT name, tier, category, metadata FROM actors WHERE id = :aid"
            ), {"aid": actor_id}).fetchone()

            if not actor_row:
                return {"actor_id": actor_id, "error": "not_found"}

            name = actor_row[0]
            category = actor_row[2]
            existing_meta = json.loads(actor_row[3]) if actor_row[3] else {}

            # ── Trades made ─────────────────────────────────
            trades = []
            if category == "insider":
                trade_rows = conn.execute(text("""
                    SELECT series_id, obs_date, value,
                           raw_payload->>'ticker' AS ticker,
                           raw_payload->>'transaction_type' AS txn_type
                    FROM raw_series
                    WHERE series_id LIKE 'INSIDER:%'
                      AND raw_payload->>'insider_name' ILIKE :name
                    ORDER BY obs_date DESC
                    LIMIT 50
                """), {"name": f"%{name}%"}).fetchall()
                for tr in trade_rows:
                    trades.append({
                        "date": str(tr[1]),
                        "value": float(tr[2]) if tr[2] else 0,
                        "ticker": tr[3] or "",
                        "type": tr[4] or "",
                    })

            elif category == "politician":
                trade_rows = conn.execute(text("""
                    SELECT series_id, obs_date, value,
                           raw_payload->>'ticker' AS ticker,
                           raw_payload->>'transaction_type' AS txn_type
                    FROM raw_series
                    WHERE series_id LIKE 'CONGRESS:%'
                      AND raw_payload->>'member_name' ILIKE :name
                    ORDER BY obs_date DESC
                    LIMIT 50
                """), {"name": f"%{name}%"}).fetchall()
                for tr in trade_rows:
                    trades.append({
                        "date": str(tr[1]),
                        "value": float(tr[2]) if tr[2] else 0,
                        "ticker": tr[3] or "",
                        "type": tr[4] or "",
                    })

            enrichment["recent_trades"] = trades
            enrichment["total_trade_value"] = sum(t["value"] for t in trades)
            enrichment["tickers_traded"] = list({t["ticker"] for t in trades if t["ticker"]})

            # ── Trust score from signal_sources ─────────────
            trust_row = conn.execute(text("""
                SELECT AVG(trust_score), COUNT(*)
                FROM signal_sources
                WHERE source_id ILIKE :name
                  AND trust_score IS NOT NULL
            """), {"name": f"%{name}%"}).fetchone()

            if trust_row and trust_row[0] is not None:
                trust_score = float(trust_row[0])
                signal_count = int(trust_row[1])
                enrichment["trust_score"] = round(trust_score, 4)
                enrichment["signal_count"] = signal_count

                # Update the actor's trust score
                conn.execute(text("""
                    UPDATE actors
                    SET trust_score = :trust,
                        updated_at = NOW()
                    WHERE id = :aid
                """), {"trust": trust_score, "aid": actor_id})

            # ── Connections ─────────────────────────────────
            conn_rows = conn.execute(text("""
                SELECT actor_a, actor_b, relationship, strength
                FROM actor_connections
                WHERE actor_a = :aid OR actor_b = :aid
                ORDER BY strength DESC
                LIMIT 30
            """), {"aid": actor_id}).fetchall()

            connections = []
            for cr in conn_rows:
                other = cr[1] if cr[0] == actor_id else cr[0]
                connections.append({
                    "actor_id": other,
                    "relationship": cr[2],
                    "strength": float(cr[3]) if cr[3] else 0.5,
                })
            enrichment["connections"] = connections
            enrichment["connection_count"] = len(connections)

            # ── Dollar flow estimate ────────────────────────
            flow_row = conn.execute(text("""
                SELECT SUM(amount_estimate), COUNT(*)
                FROM wealth_flows
                WHERE from_actor = :aid
            """), {"aid": actor_id}).fetchone()

            if flow_row and flow_row[0] is not None:
                enrichment["total_dollar_flow"] = float(flow_row[0])
                enrichment["flow_count"] = int(flow_row[1])

            # ── Update metadata ─────────────────────────────
            updated_meta = {
                **existing_meta,
                "last_enriched": datetime.now(timezone.utc).isoformat(),
                "trade_count": len(trades),
                "connection_count": len(connections),
                "tickers_traded": enrichment.get("tickers_traded", []),
            }
            conn.execute(text("""
                UPDATE actors
                SET metadata = :meta,
                    known_positions = :positions,
                    updated_at = NOW()
                WHERE id = :aid
            """), {
                "meta": json.dumps(updated_meta),
                "positions": json.dumps(trades[:20]),  # store last 20
                "aid": actor_id,
            })

            enrichment["status"] = "enriched"

    except Exception as exc:
        log.error("Enrichment failed for {a}: {e}", a=actor_id, e=str(exc))
        enrichment["status"] = "error"
        enrichment["error"] = str(exc)

    return enrichment


# ══════════════════════════════════════════════════════════════════════════
# CONNECTION DISCOVERY
# ══════════════════════════════════════════════════════════════════════════

def discover_connections(engine: Engine) -> list[dict]:
    """Find connections between actors automatically.

    Discovery methods:
      - Same company (insider + board member)
      - Same fund family (13F filers with overlapping holdings)
      - Same committee (congressional members)
      - Same lobbying client
      - Traded same ticker within 14 days (co-trading signal)

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        List of connection dicts created/updated.
    """
    _ensure_actors_table(engine)
    new_connections: list[dict] = []

    # ── Same-ticker co-trading within window ────────────────────────
    try:
        with engine.begin() as conn:
            # Insiders who traded the same ticker within the window
            rows = conn.execute(text("""
                SELECT
                    a.raw_payload->>'insider_name' AS name_a,
                    b.raw_payload->>'insider_name' AS name_b,
                    a.raw_payload->>'ticker'       AS ticker,
                    MIN(ABS(a.obs_date - b.obs_date)) AS min_gap_days
                FROM raw_series a
                JOIN raw_series b
                    ON  a.raw_payload->>'ticker' = b.raw_payload->>'ticker'
                    AND a.raw_payload->>'insider_name' < b.raw_payload->>'insider_name'
                    AND ABS(a.obs_date - b.obs_date) <= :window
                WHERE a.series_id LIKE 'INSIDER:%'
                  AND b.series_id LIKE 'INSIDER:%'
                  AND a.obs_date >= CURRENT_DATE - INTERVAL '90 days'
                GROUP BY
                    a.raw_payload->>'insider_name',
                    b.raw_payload->>'insider_name',
                    a.raw_payload->>'ticker'
                HAVING COUNT(*) >= 1
                ORDER BY MIN(ABS(a.obs_date - b.obs_date))
                LIMIT 500
            """), {"window": _CO_TRADE_WINDOW_DAYS}).fetchall()

            for r in rows:
                name_a = r[0]
                name_b = r[1]
                ticker = r[2]
                gap = int(r[3]) if r[3] else _CO_TRADE_WINDOW_DAYS

                if not name_a or not name_b:
                    continue

                aid_a = _actor_id_insider(name_a)
                aid_b = _actor_id_insider(name_b)
                strength = max(0.3, 0.7 - (gap / _CO_TRADE_WINDOW_DAYS) * 0.4)

                is_new = _upsert_connection(
                    conn=conn,
                    actor_a=aid_a,
                    actor_b=aid_b,
                    relationship="co_traded",
                    strength=strength,
                    evidence=[f"Both traded {ticker} within {gap} days"],
                )
                if is_new:
                    new_connections.append({
                        "a": aid_a, "b": aid_b,
                        "rel": "co_traded",
                        "ticker": ticker,
                    })

    except Exception as exc:
        log.warning("Co-trading connection discovery failed: {e}", e=str(exc))

    # ── Congressional same-committee links ──────────────────────────
    try:
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT
                    a.raw_payload->>'member_name' AS name_a,
                    b.raw_payload->>'member_name' AS name_b,
                    a.raw_payload->>'committee'   AS committee
                FROM raw_series a
                JOIN raw_series b
                    ON  a.raw_payload->>'committee' = b.raw_payload->>'committee'
                    AND a.raw_payload->>'member_name' < b.raw_payload->>'member_name'
                WHERE a.series_id LIKE 'CONGRESS:%'
                  AND b.series_id LIKE 'CONGRESS:%'
                  AND a.raw_payload->>'committee' IS NOT NULL
                  AND a.raw_payload->>'committee' != ''
                GROUP BY
                    a.raw_payload->>'member_name',
                    b.raw_payload->>'member_name',
                    a.raw_payload->>'committee'
                LIMIT 300
            """)).fetchall()

            for r in rows:
                name_a = r[0]
                name_b = r[1]
                committee = r[2]
                if not name_a or not name_b:
                    continue

                is_new = _upsert_connection(
                    conn=conn,
                    actor_a=_actor_id_congress(name_a),
                    actor_b=_actor_id_congress(name_b),
                    relationship="same_committee",
                    strength=0.5,
                    evidence=[f"Both on {committee}"],
                )
                if is_new:
                    new_connections.append({
                        "a": _actor_id_congress(name_a),
                        "b": _actor_id_congress(name_b),
                        "rel": "same_committee",
                    })

    except Exception as exc:
        log.warning("Committee connection discovery failed: {e}", e=str(exc))

    # ── Insider-congressional overlap (same ticker) ─────────────────
    try:
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT
                    ins.raw_payload->>'insider_name' AS insider_name,
                    cong.raw_payload->>'member_name' AS member_name,
                    ins.raw_payload->>'ticker'       AS ticker,
                    MIN(ABS(ins.obs_date - cong.obs_date)) AS min_gap
                FROM raw_series ins
                JOIN raw_series cong
                    ON  ins.raw_payload->>'ticker' = cong.raw_payload->>'ticker'
                    AND ABS(ins.obs_date - cong.obs_date) <= :window
                WHERE ins.series_id LIKE 'INSIDER:%'
                  AND cong.series_id LIKE 'CONGRESS:%'
                  AND ins.obs_date >= CURRENT_DATE - INTERVAL '180 days'
                GROUP BY
                    ins.raw_payload->>'insider_name',
                    cong.raw_payload->>'member_name',
                    ins.raw_payload->>'ticker'
                ORDER BY MIN(ABS(ins.obs_date - cong.obs_date))
                LIMIT 200
            """), {"window": _CO_TRADE_WINDOW_DAYS}).fetchall()

            for r in rows:
                insider = r[0]
                member = r[1]
                ticker = r[2]
                gap = int(r[3]) if r[3] else _CO_TRADE_WINDOW_DAYS

                if not insider or not member:
                    continue

                is_new = _upsert_connection(
                    conn=conn,
                    actor_a=_actor_id_insider(insider),
                    actor_b=_actor_id_congress(member),
                    relationship="co_traded_cross_type",
                    strength=max(0.4, 0.8 - (gap / _CO_TRADE_WINDOW_DAYS) * 0.4),
                    evidence=[
                        f"Insider {insider} and Rep {member} "
                        f"both traded {ticker} within {gap} days",
                    ],
                )
                if is_new:
                    new_connections.append({
                        "a": _actor_id_insider(insider),
                        "b": _actor_id_congress(member),
                        "rel": "co_traded_cross_type",
                        "ticker": ticker,
                    })

    except Exception as exc:
        log.warning("Insider-congressional connection discovery failed: {e}", e=str(exc))

    log.info(
        "Connection discovery: {n} new connections found",
        n=len(new_connections),
    )
    return new_connections


# ══════════════════════════════════════════════════════════════════════════
# BATCH ENRICHMENT
# ══════════════════════════════════════════════════════════════════════════

def enrich_all_actors(engine: Engine, batch_size: int = _ENRICHMENT_BATCH) -> dict:
    """Batch-enrich actors, prioritising those not recently enriched.

    Parameters:
        engine: SQLAlchemy engine.
        batch_size: Maximum actors to enrich in one call.

    Returns:
        dict with enriched, errors, skipped counts.
    """
    enriched = 0
    errors = 0

    try:
        with engine.connect() as conn:
            # Fetch actors sorted by least-recently enriched
            rows = conn.execute(text("""
                SELECT id
                FROM actors
                ORDER BY
                    COALESCE(
                        (metadata->>'last_enriched')::timestamptz,
                        '1970-01-01'::timestamptz
                    ) ASC,
                    influence_score DESC
                LIMIT :batch
            """), {"batch": batch_size}).fetchall()

        actor_ids = [r[0] for r in rows]

    except Exception as exc:
        log.error("Failed to fetch actors for enrichment: {e}", e=str(exc))
        return {"enriched": 0, "errors": 1, "error": str(exc)}

    for aid in actor_ids:
        result = enrich_actor(engine, aid)
        if result.get("status") == "enriched":
            enriched += 1
        else:
            errors += 1

    log.info(
        "Batch enrichment: {e} enriched, {err} errors out of {t} total",
        e=enriched, err=errors, t=len(actor_ids),
    )
    return {"enriched": enriched, "errors": errors, "total_attempted": len(actor_ids)}


# ══════════════════════════════════════════════════════════════════════════
# PUBLIC API — ORCHESTRATORS
# ══════════════════════════════════════════════════════════════════════════

def auto_discover_actors(engine: Engine) -> dict:
    """Scan all data sources and create new actor entries.

    Runs all six discovery sources in sequence:
      1. SEC Form 4 insiders
      2. Congressional traders
      3. 13F institutional filers
      4. Board cross-references
      5. Lobbyist registrations
      6. Government officials

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        dict with per-source results and aggregate totals.
    """
    _ensure_actors_table(engine)

    results: dict[str, Any] = {}
    total_discovered = 0

    # Phase 1: insiders who traded >$1M
    log.info("Actor discovery: scanning Form 4 insiders...")
    r = _discover_insiders(engine, min_value=_INSIDER_MIN_VALUE_PHASE1)
    results["insiders_phase1"] = r
    total_discovered += r.get("discovered", 0)

    # Phase 1 also: all insiders (no minimum) for Phase 2+ completeness
    log.info("Actor discovery: scanning all Form 4 insiders...")
    r = _discover_insiders(engine, min_value=0)
    results["insiders_all"] = r
    total_discovered += r.get("discovered", 0)

    # Congressional traders
    log.info("Actor discovery: scanning congressional traders...")
    r = _discover_congressional(engine)
    results["congressional"] = r
    total_discovered += r.get("discovered", 0)

    # 13F filers
    log.info("Actor discovery: scanning 13F filers...")
    r = _discover_13f_filers(engine)
    results["13f_filers"] = r
    total_discovered += r.get("discovered", 0)

    # Board cross-references
    log.info("Actor discovery: scanning board cross-references...")
    r = _discover_board_crossrefs(engine)
    results["board_crossrefs"] = r

    # Lobbyists
    log.info("Actor discovery: scanning lobbyist registrations...")
    r = _discover_lobbyists(engine)
    results["lobbyists"] = r
    total_discovered += r.get("discovered", 0)

    # Government officials
    log.info("Actor discovery: scanning government officials...")
    r = _discover_gov_officials(engine)
    results["gov_officials"] = r
    total_discovered += r.get("discovered", 0)

    results["total_discovered"] = total_discovered
    results["timestamp"] = datetime.now(timezone.utc).isoformat()

    log.info(
        "Actor discovery complete: {n} new actors discovered across all sources",
        n=total_discovered,
    )
    return results


def auto_discover_connections(engine: Engine) -> dict:
    """Find links between actors automatically.

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        dict with connections list and counts.
    """
    _ensure_actors_table(engine)
    new_conns = discover_connections(engine)
    board_result = _discover_board_crossrefs(engine)

    return {
        "new_connections": len(new_conns),
        "board_crossrefs": board_result.get("connections_found", 0),
        "connections_detail": new_conns[:50],  # cap detail for logging
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def run_discovery_cycle(engine: Engine, scale_phase: int | None = None) -> dict:
    """Full daily discovery + enrichment cycle.

    Orchestrator for hermes scheduling.  Runs:
      1. auto_discover_actors  — find new actors
      2. auto_discover_connections — find links
      3. enrich_all_actors — update data for stale actors

    If scale_phase is set (1, 2, or 3), also runs scaled discovery
    targeting 5K / 50K / 250K+ actors respectively.

    Parameters:
        engine: SQLAlchemy engine.
        scale_phase: Optional phase for scale discovery (1-3).

    Returns:
        dict with full cycle results.
    """
    log.info("=== Actor Discovery Cycle START ===")
    cycle_start = datetime.now(timezone.utc)

    # Step 1: discover
    discovery_result = auto_discover_actors(engine)

    # Step 2: connections
    connection_result = auto_discover_connections(engine)

    # Step 3: enrich (batch)
    enrichment_result = enrich_all_actors(engine, batch_size=_ENRICHMENT_BATCH)

    # Step 3b: cross-reference newly discovered actors against ICIJ offshore leaks
    offshore_result = _cross_reference_offshore_leaks(
        engine, discovery_result,
    )

    # Step 3c: if scale_phase requested, run scaled discovery
    scale_result = None
    if scale_phase and scale_phase in (1, 2, 3):
        log.info(
            "Running scale discovery phase {p}...",
            p=scale_phase,
        )
        scale_result = run_scale_discovery(engine, target_phase=scale_phase)

    # Step 4: get stats
    stats = get_actor_stats(engine)

    cycle_end = datetime.now(timezone.utc)
    elapsed = (cycle_end - cycle_start).total_seconds()

    result = {
        "status": "SUCCESS",
        "discovery": discovery_result,
        "connections": connection_result,
        "enrichment": enrichment_result,
        "offshore_crossref": offshore_result,
        "scale_discovery": scale_result,
        "stats": stats,
        "elapsed_seconds": round(elapsed, 1),
        "timestamp": cycle_end.isoformat(),
    }

    log.info(
        "=== Actor Discovery Cycle END === "
        "{n} actors total, {new} new, {conn} connections, {sec:.1f}s elapsed",
        n=stats.get("total_actors", 0),
        new=discovery_result.get("total_discovered", 0),
        conn=connection_result.get("new_connections", 0),
        sec=elapsed,
    )

    # Queue background LLM enrichment for newly discovered actors
    _queue_llm_enrichment(engine, discovery_result)

    return result


def get_actor_stats(engine: Engine) -> dict:
    """Dashboard statistics about the actor network.

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        dict with total counts, tier breakdown, category breakdown,
        connection count, and enrichment coverage.
    """
    stats: dict[str, Any] = {}

    try:
        with engine.connect() as conn:
            # Total actors
            row = conn.execute(text("SELECT COUNT(*) FROM actors")).fetchone()
            stats["total_actors"] = int(row[0]) if row else 0

            # By tier
            tier_rows = conn.execute(text(
                "SELECT tier, COUNT(*) FROM actors GROUP BY tier ORDER BY COUNT(*) DESC"
            )).fetchall()
            stats["by_tier"] = {r[0]: int(r[1]) for r in tier_rows}

            # By category
            cat_rows = conn.execute(text(
                "SELECT category, COUNT(*) FROM actors GROUP BY category ORDER BY COUNT(*) DESC"
            )).fetchall()
            stats["by_category"] = {r[0]: int(r[1]) for r in cat_rows}

            # Total connections
            conn_row = conn.execute(text(
                "SELECT COUNT(*) FROM actor_connections"
            )).fetchone()
            stats["total_connections"] = int(conn_row[0]) if conn_row else 0

            # Connection types
            rel_rows = conn.execute(text(
                "SELECT relationship, COUNT(*) FROM actor_connections "
                "GROUP BY relationship ORDER BY COUNT(*) DESC"
            )).fetchall()
            stats["connection_types"] = {r[0]: int(r[1]) for r in rel_rows}

            # Enrichment coverage
            enriched_row = conn.execute(text("""
                SELECT COUNT(*)
                FROM actors
                WHERE metadata->>'last_enriched' IS NOT NULL
            """)).fetchone()
            stats["enriched_actors"] = int(enriched_row[0]) if enriched_row else 0
            if stats["total_actors"] > 0:
                stats["enrichment_pct"] = round(
                    stats["enriched_actors"] / stats["total_actors"] * 100, 1,
                )
            else:
                stats["enrichment_pct"] = 0.0

            # Average influence score
            avg_row = conn.execute(text(
                "SELECT AVG(influence_score), AVG(trust_score) FROM actors"
            )).fetchone()
            if avg_row:
                stats["avg_influence"] = round(float(avg_row[0] or 0), 3)
                stats["avg_trust"] = round(float(avg_row[1] or 0), 3)

            # Phase assessment (updated for 250K scale targets)
            total = stats["total_actors"]
            if total >= SCALE_TARGETS["phase_3"]:
                stats["phase"] = f"Phase 3 COMPLETE ({total:,} actors)"
            elif total >= SCALE_TARGETS["phase_2"]:
                stats["phase"] = f"Phase 2+ ({total:,} / {SCALE_TARGETS['phase_3']:,})"
            elif total >= SCALE_TARGETS["phase_1"]:
                stats["phase"] = f"Phase 1+ ({total:,} / {SCALE_TARGETS['phase_2']:,})"
            elif total >= 500:
                stats["phase"] = f"Phase 0 ({total:,} / {SCALE_TARGETS['phase_1']:,})"
            else:
                stats["phase"] = f"Pre-Phase ({total:,} actors)"

            # Degree distribution
            degree_rows = conn.execute(text(
                "SELECT degree, COUNT(*) FROM actors "
                "WHERE degree IS NOT NULL "
                "GROUP BY degree ORDER BY degree"
            )).fetchall()
            stats["by_degree"] = {
                f"degree_{r[0]}": int(r[1]) for r in degree_rows
            }

            # Source distribution
            source_rows = conn.execute(text(
                "SELECT source, COUNT(*) FROM actors "
                "GROUP BY source ORDER BY COUNT(*) DESC "
                "LIMIT 20"
            )).fetchall()
            stats["by_source"] = {r[0]: int(r[1]) for r in source_rows}

    except Exception as exc:
        log.warning("Failed to compute actor stats: {e}", e=str(exc))
        stats["error"] = str(exc)

    return stats


# ══════════════════════════════════════════════════════════════════════════
# LLM TASK QUEUE INTEGRATION
# ══════════════════════════════════════════════════════════════════════════

def _cross_reference_offshore_leaks(
    engine: Engine,
    discovery_result: dict,
) -> dict:
    """Cross-reference newly discovered actors against ICIJ offshore leaks.

    For each actor discovered in this cycle, check if their name appears
    in the offshore leaks database. If so, flag them and queue an LLM
    investigation task.

    Parameters:
        engine: SQLAlchemy engine.
        discovery_result: Output from auto_discover_actors().

    Returns:
        dict with offshore cross-reference results.
    """
    result: dict = {
        "actors_screened": 0,
        "offshore_hits": 0,
        "investigations_queued": 0,
    }

    total_discovered = discovery_result.get("total_discovered", 0)
    if total_discovered == 0:
        return result

    try:
        from ingestion.altdata.offshore_leaks import (
            check_actor_in_offshore_leaks,
            queue_offshore_investigation,
        )
    except ImportError:
        log.debug("offshore_leaks module not available — skipping cross-reference")
        return result

    # Get names of recently discovered actors from the DB
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, name
                FROM actors
                WHERE created_at >= NOW() - INTERVAL '1 day'
                ORDER BY created_at DESC
                LIMIT 200
            """)).fetchall()
    except Exception as exc:
        log.debug("Failed to fetch recent actors for offshore check: {e}", e=str(exc))
        return result

    for row in rows:
        actor_id = row[0]
        actor_name = row[1]
        result["actors_screened"] += 1

        try:
            offshore_hits = check_actor_in_offshore_leaks(
                engine, actor_name, actor_id=actor_id,
            )
            if offshore_hits:
                result["offshore_hits"] += len(offshore_hits)
                task_id = queue_offshore_investigation(
                    engine, actor_name, actor_id, offshore_hits,
                )
                if task_id:
                    result["investigations_queued"] += 1
                log.warning(
                    "OFFSHORE HIT: newly discovered actor {name} ({aid}) "
                    "found in {n} offshore leak records",
                    name=actor_name,
                    aid=actor_id,
                    n=len(offshore_hits),
                )
        except Exception as exc:
            log.debug(
                "Offshore check failed for {name}: {e}",
                name=actor_name,
                e=str(exc),
            )

    if result["offshore_hits"] > 0:
        log.warning(
            "Offshore cross-reference: {screened} actors screened, "
            "{hits} offshore hits, {inv} investigations queued",
            screened=result["actors_screened"],
            hits=result["offshore_hits"],
            inv=result["investigations_queued"],
        )
    else:
        log.info(
            "Offshore cross-reference: {n} actors screened, no hits",
            n=result["actors_screened"],
        )

    return result


def _queue_llm_enrichment(engine: Engine, discovery_result: dict) -> None:
    """Queue background LLM tasks for newly discovered actors.

    Submits P3 (background priority) tasks to the LLM task queue
    for richer enrichment — generating actor summaries, inferring
    motivations, and identifying strategic implications.

    Parameters:
        engine: SQLAlchemy engine.
        discovery_result: Output from auto_discover_actors().
    """
    try:
        from orchestration.llm_taskqueue import get_task_queue
        tq = get_task_queue()
    except Exception:
        log.debug("LLM task queue not available — skipping background enrichment")
        return

    total_discovered = discovery_result.get("total_discovered", 0)
    if total_discovered == 0:
        return

    # Queue a summary task for the discovery cycle
    try:
        tq.enqueue(
            task_type="actor_discovery_summary",
            prompt=(
                f"Actor discovery cycle completed. {total_discovered} new actors "
                f"discovered across sources: "
                f"insiders={discovery_result.get('insiders_all', {}).get('discovered', 0)}, "
                f"congressional={discovery_result.get('congressional', {}).get('discovered', 0)}, "
                f"13F={discovery_result.get('13f_filers', {}).get('discovered', 0)}, "
                f"lobbyists={discovery_result.get('lobbyists', {}).get('discovered', 0)}, "
                f"gov_officials={discovery_result.get('gov_officials', {}).get('discovered', 0)}. "
                f"Analyse which newly discovered actors are most significant for "
                f"trading intelligence.  Which cross-connections are most suspicious? "
                f"Any potential insider trading rings or political-financial nexuses?"
            ),
            context={"source": "actor_discovery", "cycle_result": discovery_result},
            priority=3,
        )
        log.info("Queued P3 LLM task for actor discovery analysis")
    except Exception as exc:
        log.debug("Failed to queue LLM enrichment task: {e}", e=str(exc))


# ══════════════════════════════════════════════════════════════════════════
# SCALE ENGINE — 250K+ ACTOR DISCOVERY
# ══════════════════════════════════════════════════════════════════════════


def _batch_upsert_actors(
    conn: Any,
    actors: list[dict],
    batch_size: int = _BATCH_INSERT_SIZE,
) -> int:
    """Batch-upsert actors in chunks of batch_size for 250K-scale ingestion.

    Each dict in ``actors`` must have keys: id, name, tier, category.
    Optional keys: title, influence_score, aum, data_sources, credibility,
    motivation_model, metadata, source, degree, icij_node_id.

    Parameters:
        conn: Active SQLAlchemy connection (inside a transaction).
        actors: List of actor dicts.
        batch_size: Rows per INSERT statement.

    Returns:
        Number of newly inserted actors (excludes updates).
    """
    inserted = 0
    for start in range(0, len(actors), batch_size):
        chunk = actors[start : start + batch_size]
        for actor in chunk:
            try:
                result = conn.execute(text("""
                    INSERT INTO actors (
                        id, name, tier, category, title,
                        influence_score, aum,
                        data_sources, credibility, motivation_model,
                        metadata, source, degree, icij_node_id, updated_at
                    ) VALUES (
                        :id, :name, :tier, :category, :title,
                        :inf, :aum,
                        :sources, :cred, :motivation,
                        :meta, :source, :degree, :icij_node_id, NOW()
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        title = COALESCE(NULLIF(EXCLUDED.title, ''), actors.title),
                        aum = COALESCE(EXCLUDED.aum, actors.aum),
                        data_sources = EXCLUDED.data_sources,
                        metadata = actors.metadata || EXCLUDED.metadata,
                        source = COALESCE(NULLIF(EXCLUDED.source, 'unknown'), actors.source),
                        degree = LEAST(actors.degree, EXCLUDED.degree),
                        icij_node_id = COALESCE(EXCLUDED.icij_node_id, actors.icij_node_id),
                        updated_at = NOW()
                    RETURNING (xmax = 0) AS is_insert
                """), {
                    "id": actor["id"],
                    "name": actor["name"],
                    "tier": actor.get("tier", "individual"),
                    "category": actor.get("category", "unknown"),
                    "title": actor.get("title", ""),
                    "inf": actor.get("influence_score", 0.3),
                    "aum": actor.get("aum"),
                    "sources": json.dumps(actor.get("data_sources", [])),
                    "cred": actor.get("credibility", "inferred"),
                    "motivation": actor.get("motivation_model", "unknown"),
                    "meta": json.dumps(actor.get("metadata", {})),
                    "source": actor.get("source", "unknown"),
                    "degree": actor.get("degree", 0),
                    "icij_node_id": actor.get("icij_node_id"),
                })
                row = result.fetchone()
                if row and row[0]:
                    inserted += 1
            except Exception as exc:
                log.debug(
                    "Batch upsert failed for {aid}: {e}",
                    aid=actor.get("id", "?"),
                    e=str(exc),
                )
    return inserted


def _batch_upsert_connections(
    conn: Any,
    connections: list[dict],
    batch_size: int = _BATCH_INSERT_SIZE,
) -> int:
    """Batch-upsert actor_connections in chunks.

    Each dict in ``connections`` must have: actor_a, actor_b, relationship.
    Optional: strength, evidence.

    Parameters:
        conn: Active SQLAlchemy connection (inside a transaction).
        connections: List of connection dicts.
        batch_size: Rows per INSERT statement.

    Returns:
        Number of newly inserted connections.
    """
    inserted = 0
    for start in range(0, len(connections), batch_size):
        chunk = connections[start : start + batch_size]
        for c in chunk:
            a, b = sorted([c["actor_a"], c["actor_b"]])
            try:
                result = conn.execute(text("""
                    INSERT INTO actor_connections
                        (actor_a, actor_b, relationship, strength, evidence)
                    VALUES (:a, :b, :rel, :strength, :evidence)
                    ON CONFLICT (actor_a, actor_b, relationship) DO UPDATE SET
                        strength = GREATEST(
                            actor_connections.strength, EXCLUDED.strength
                        ),
                        evidence = actor_connections.evidence || EXCLUDED.evidence
                    RETURNING (xmax = 0) AS is_insert
                """), {
                    "a": a,
                    "b": b,
                    "rel": c["relationship"],
                    "strength": c.get("strength", 0.5),
                    "evidence": json.dumps(c.get("evidence", [])),
                })
                row = result.fetchone()
                if row and row[0]:
                    inserted += 1
            except Exception as exc:
                log.debug(
                    "Batch connection upsert failed: {e}",
                    e=str(exc),
                )
    return inserted


# ──────────────────────────────────────────────────────────────────────────
# 1. BATCH HISTORICAL INSIDER DISCOVERY (Form 4)
# ──────────────────────────────────────────────────────────────────────────

def batch_discover_insiders(engine: Engine, days_back: int = 365) -> dict:
    """Pull a full year of Form 4 filers — targeting ~50K unique actors.

    Unlike _discover_insiders() which only looks at data already in raw_series,
    this function:
    1. Queries raw_series for ALL INSIDER: prefixed rows going back days_back
    2. Removes the minimum value threshold to capture every filer
    3. Uses batch inserts for performance at scale
    4. Tags each actor with source='form4_batch' and degree=1

    Parameters:
        engine: SQLAlchemy engine.
        days_back: How many days of history to scan (default 365).

    Returns:
        dict with discovered, skipped, total_filers counts.
    """
    _ensure_actors_table(engine)
    discovered = 0
    skipped = 0
    total_filers = 0
    batch_actors: list[dict] = []

    try:
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT
                    raw_payload->>'insider_name'  AS insider_name,
                    raw_payload->>'insider_title' AS insider_title,
                    raw_payload->>'ticker'        AS ticker,
                    raw_payload->>'cik'           AS cik,
                    SUM(value)                    AS total_value,
                    COUNT(*)                      AS trade_count,
                    MIN(obs_date)                 AS first_trade,
                    MAX(obs_date)                 AS last_trade
                FROM raw_series
                WHERE series_id LIKE 'INSIDER:%'
                  AND raw_payload->>'insider_name' IS NOT NULL
                  AND obs_date >= CURRENT_DATE - MAKE_INTERVAL(days => :days)
                GROUP BY
                    raw_payload->>'insider_name',
                    raw_payload->>'insider_title',
                    raw_payload->>'ticker',
                    raw_payload->>'cik'
                ORDER BY COUNT(*) DESC
            """), {"days": days_back}).fetchall()

            total_filers = len(rows)
            log.info(
                "Batch insider discovery: found {n} filer-ticker combos "
                "over {d} days",
                n=total_filers,
                d=days_back,
            )

            for r in rows:
                name = r[0]
                title = r[1] or ""
                ticker = r[2] or ""
                cik = r[3] or ""
                total_val = float(r[4]) if r[4] else 0
                trade_count = int(r[5]) if r[5] else 0
                first_trade = str(r[6]) if r[6] else ""
                last_trade = str(r[7]) if r[7] else ""

                if not name or not name.strip():
                    skipped += 1
                    continue

                actor_id = _actor_id_insider(name)
                batch_actors.append({
                    "id": actor_id,
                    "name": name.strip(),
                    "tier": "individual",
                    "category": "insider",
                    "title": title,
                    "influence_score": min(
                        0.60, 0.20 + (total_val / 100_000_000) * 0.40,
                    ),
                    "data_sources": ["form4", "sec_edgar"],
                    "credibility": "hard_data",
                    "motivation_model": "informed",
                    "source": "form4_batch",
                    "degree": 1,
                    "metadata": {
                        "primary_ticker": ticker,
                        "cik": cik,
                        "total_value_traded": total_val,
                        "trade_count": trade_count,
                        "first_trade": first_trade,
                        "last_trade": last_trade,
                        "discovery_source": "batch_form4",
                    },
                })

            discovered = _batch_upsert_actors(conn, batch_actors)

    except Exception as exc:
        log.error("Batch insider discovery failed: {e}", e=str(exc))
        return {
            "discovered": discovered,
            "skipped": skipped,
            "total_filers": total_filers,
            "error": str(exc),
        }

    log.info(
        "Batch insider discovery complete: {d} new actors from {t} filer rows",
        d=discovered,
        t=total_filers,
    )
    return {
        "discovered": discovered,
        "skipped": skipped,
        "total_filers": total_filers,
    }


# ──────────────────────────────────────────────────────────────────────────
# 2. DISCOVER ALL 13F FILERS (~5,000 institutions)
# ──────────────────────────────────────────────────────────────────────────

def discover_all_13f_filers(engine: Engine) -> dict:
    """Query EDGAR data for all 13F-HR filers — targeting ~5,000 institutions.

    Expands beyond the 25 seed filers to capture every institutional investor
    managing >$100M. Pulls from:
    1. The seed list (_SEED_13F_FILERS)
    2. All 13F: prefixed series in raw_series (no LIMIT cap)
    3. Any 13F-HR filing metadata found in signal_sources

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        dict with discovered, total_filers counts.
    """
    _ensure_actors_table(engine)
    discovered = 0
    batch_actors: list[dict] = []

    # Seed filers first
    for cik, info in _SEED_13F_FILERS.items():
        actor_id = _actor_id_13f(cik)
        batch_actors.append({
            "id": actor_id,
            "name": info["name"],
            "tier": "institutional",
            "category": "fund",
            "title": f"13F Filer (CIK {cik})",
            "influence_score": min(
                0.85,
                0.50 + (info.get("aum_est", 0) / 15_000_000_000_000) * 0.35,
            ),
            "aum": info.get("aum_est"),
            "data_sources": ["sec_13f", "sec_edgar"],
            "credibility": "hard_data",
            "motivation_model": "alpha_seeking",
            "source": "seed_13f",
            "degree": 0,
            "metadata": {
                "cik": cik,
                "aum_estimate": info.get("aum_est"),
                "discovery_source": "seed_13f",
            },
        })

    try:
        with engine.begin() as conn:
            # Pull ALL 13F filers from raw_series without the old LIMIT 500
            rows = conn.execute(text("""
                SELECT
                    SPLIT_PART(series_id, ':', 2)                AS manager_cik,
                    COUNT(DISTINCT SPLIT_PART(series_id, ':', 3)) AS tickers_held,
                    COUNT(*)                                     AS filings,
                    SUM(value)                                   AS total_value
                FROM raw_series
                WHERE series_id LIKE '13F:%'
                GROUP BY SPLIT_PART(series_id, ':', 2)
                ORDER BY COUNT(*) DESC
            """)).fetchall()

            for r in rows:
                cik = str(r[0]).strip()
                if not cik:
                    continue
                tickers = int(r[1]) if r[1] else 0
                filings = int(r[2]) if r[2] else 0
                total_val = float(r[3]) if r[3] else 0
                actor_id = _actor_id_13f(cik)

                # Resolve name from filing payload
                name_row = conn.execute(text("""
                    SELECT raw_payload->>'manager_name'
                    FROM raw_series
                    WHERE series_id LIKE :prefix
                      AND raw_payload->>'manager_name' IS NOT NULL
                    LIMIT 1
                """), {"prefix": f"13F:{cik}:%"}).fetchone()

                name = (
                    name_row[0]
                    if name_row and name_row[0]
                    else f"13F Filer {cik}"
                )

                batch_actors.append({
                    "id": actor_id,
                    "name": name,
                    "tier": "institutional",
                    "category": "fund",
                    "title": f"13F Institutional Filer (CIK {cik})",
                    "influence_score": min(
                        0.70, 0.35 + (tickers / 1000) * 0.35,
                    ),
                    "data_sources": ["sec_13f"],
                    "credibility": "hard_data",
                    "motivation_model": "alpha_seeking",
                    "source": "13f_all",
                    "degree": 1,
                    "metadata": {
                        "cik": cik,
                        "tickers_held": tickers,
                        "filing_count": filings,
                        "total_value": total_val,
                        "discovery_source": "discover_all_13f",
                    },
                })

            # Also check signal_sources for 13F filers we may have missed
            sig_rows = conn.execute(text("""
                SELECT DISTINCT
                    source_id,
                    metadata->>'manager_name'  AS mgr_name,
                    metadata->>'manager_cik'   AS mgr_cik
                FROM signal_sources
                WHERE source_type = '13f'
                  AND metadata->>'manager_cik' IS NOT NULL
            """)).fetchall()

            existing_ciks = {a.get("metadata", {}).get("cik") for a in batch_actors}
            for sr in sig_rows:
                mgr_cik = sr[2]
                if mgr_cik and mgr_cik not in existing_ciks:
                    existing_ciks.add(mgr_cik)
                    actor_id = _actor_id_13f(mgr_cik)
                    batch_actors.append({
                        "id": actor_id,
                        "name": sr[1] or f"13F Filer {mgr_cik}",
                        "tier": "institutional",
                        "category": "fund",
                        "title": f"13F Filer (CIK {mgr_cik})",
                        "influence_score": 0.45,
                        "data_sources": ["sec_13f"],
                        "credibility": "hard_data",
                        "motivation_model": "alpha_seeking",
                        "source": "13f_signal",
                        "degree": 1,
                        "metadata": {
                            "cik": mgr_cik,
                            "discovery_source": "signal_sources_13f",
                        },
                    })

            discovered = _batch_upsert_actors(conn, batch_actors)

    except Exception as exc:
        log.error("Full 13F discovery failed: {e}", e=str(exc))
        return {
            "discovered": discovered,
            "total_filers": len(batch_actors),
            "error": str(exc),
        }

    log.info(
        "Full 13F discovery: {d} new from {t} total filers",
        d=discovered,
        t=len(batch_actors),
    )
    return {"discovered": discovered, "total_filers": len(batch_actors)}


# ──────────────────────────────────────────────────────────────────────────
# 3. DISCOVER ALL CONGRESS MEMBERS (535 + staff)
# ──────────────────────────────────────────────────────────────────────────

# Comprehensive list of all 535 members is too large to embed; instead
# we discover from data already in raw_series AND supplement from known
# public lists via the CONGRESS: series prefix.

def discover_all_congress(engine: Engine) -> dict:
    """Discover all 535+ members of Congress and their connections.

    Sources:
    1. All CONGRESS: prefixed rows in raw_series (trading disclosures)
    2. LOBBYING: series for lobbyist-politician links
    3. GOV_CONTRACT: series for committee-contractor links

    Builds connections for:
    - Committee assignments (same_committee)
    - Campaign donor links (lobbies_for connections)
    - Stock trade correlations (co_traded_cross_type)

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        dict with discovered members, connections, committees.
    """
    _ensure_actors_table(engine)
    discovered = 0
    connections_created = 0
    batch_actors: list[dict] = []
    batch_connections: list[dict] = []
    committees_found: set[str] = set()

    try:
        with engine.begin() as conn:
            # All unique congressional members from raw_series (no LIMIT)
            rows = conn.execute(text("""
                SELECT
                    raw_payload->>'member_name'  AS member_name,
                    raw_payload->>'party'        AS party,
                    raw_payload->>'state'        AS state,
                    raw_payload->>'committee'    AS committee,
                    raw_payload->>'chamber'      AS chamber,
                    raw_payload->>'district'     AS district,
                    SUM(value)                   AS total_value,
                    COUNT(*)                     AS trade_count,
                    MIN(obs_date)                AS first_trade,
                    MAX(obs_date)                AS last_trade,
                    ARRAY_AGG(DISTINCT raw_payload->>'ticker')
                        FILTER (WHERE raw_payload->>'ticker' IS NOT NULL)
                        AS tickers
                FROM raw_series
                WHERE series_id LIKE 'CONGRESS:%'
                  AND raw_payload->>'member_name' IS NOT NULL
                GROUP BY
                    raw_payload->>'member_name',
                    raw_payload->>'party',
                    raw_payload->>'state',
                    raw_payload->>'committee',
                    raw_payload->>'chamber',
                    raw_payload->>'district'
                ORDER BY COUNT(*) DESC
            """)).fetchall()

            seen_members: dict[str, dict] = {}  # name -> best record
            for r in rows:
                name = r[0]
                if not name or not name.strip():
                    continue
                party = r[1] or ""
                state = r[2] or ""
                committee = r[3] or ""
                chamber = r[4] or ""
                district = r[5] or ""
                total_val = float(r[6]) if r[6] else 0
                trade_count = int(r[7]) if r[7] else 0
                first_trade = str(r[8]) if r[8] else ""
                last_trade = str(r[9]) if r[9] else ""
                tickers = r[10] if r[10] else []

                if committee:
                    committees_found.add(committee)

                norm = _normalize_name(name)
                if norm in seen_members:
                    # Merge: keep the one with more trades
                    existing = seen_members[norm]
                    existing["metadata"]["trade_count"] = max(
                        existing["metadata"].get("trade_count", 0),
                        trade_count,
                    )
                    if committee and committee not in existing["metadata"].get(
                        "committees", [],
                    ):
                        existing["metadata"].setdefault("committees", []).append(
                            committee,
                        )
                    continue

                actor_id = _actor_id_congress(name)
                tier = (
                    "regional"
                    if "chair" in (committee or "").lower()
                    else "individual"
                )

                actor_dict = {
                    "id": actor_id,
                    "name": name.strip(),
                    "tier": tier,
                    "category": "politician",
                    "title": (
                        f"{chamber} ({party}-{state})"
                        if party and state
                        else chamber or "Member of Congress"
                    ),
                    "influence_score": _DEFAULT_INFLUENCE.get(tier, 0.35),
                    "data_sources": ["congressional_disclosures"],
                    "credibility": "public_record",
                    "motivation_model": "political",
                    "source": "congress_all",
                    "degree": 1,
                    "metadata": {
                        "party": party,
                        "state": state,
                        "committees": [committee] if committee else [],
                        "chamber": chamber,
                        "district": district,
                        "total_value_traded": total_val,
                        "trade_count": trade_count,
                        "first_trade": first_trade,
                        "last_trade": last_trade,
                        "tickers_traded": (
                            tickers[:20] if isinstance(tickers, list) else []
                        ),
                        "discovery_source": "discover_all_congress",
                    },
                }
                seen_members[norm] = actor_dict
                batch_actors.append(actor_dict)

            # Build committee co-membership connections
            committee_members: dict[str, list[str]] = {}
            for actor in batch_actors:
                comms = actor.get("metadata", {}).get("committees", [])
                for c in comms:
                    committee_members.setdefault(c, []).append(actor["id"])

            for committee, members in committee_members.items():
                if len(members) < 2:
                    continue
                # Limit pairwise connections to avoid combinatorial explosion
                capped = members[:50]
                for i in range(len(capped)):
                    for j in range(i + 1, min(len(capped), i + 20)):
                        batch_connections.append({
                            "actor_a": capped[i],
                            "actor_b": capped[j],
                            "relationship": "same_committee",
                            "strength": 0.5,
                            "evidence": [f"Both on {committee}"],
                        })

            discovered = _batch_upsert_actors(conn, batch_actors)
            connections_created = _batch_upsert_connections(
                conn, batch_connections,
            )

    except Exception as exc:
        log.error("Full congress discovery failed: {e}", e=str(exc))
        return {
            "discovered": discovered,
            "connections_created": connections_created,
            "committees": len(committees_found),
            "error": str(exc),
        }

    log.info(
        "Congress discovery: {d} members, {c} connections, {cm} committees",
        d=discovered,
        c=connections_created,
        cm=len(committees_found),
    )
    return {
        "discovered": discovered,
        "connections_created": connections_created,
        "total_members_found": len(batch_actors),
        "committees": len(committees_found),
    }


# ──────────────────────────────────────────────────────────────────────────
# 4. ICIJ OFFSHORE LEAKS BULK IMPORT
# ──────────────────────────────────────────────────────────────────────────

def _icij_csvs_available(data_dir: str = _ICIJ_DATA_DIR) -> dict[str, bool]:
    """Check which ICIJ CSV files exist locally.

    Parameters:
        data_dir: Directory to check for CSVs.

    Returns:
        Dict mapping CSV filename -> exists boolean.
    """
    base = Path(data_dir)
    return {
        "officers": (base / _ICIJ_CSV_OFFICERS).exists(),
        "entities": (base / _ICIJ_CSV_ENTITIES).exists(),
        "intermediaries": (base / _ICIJ_CSV_INTERMEDIARIES).exists(),
        "relationships": (base / _ICIJ_CSV_RELATIONSHIPS).exists(),
        "addresses": (base / _ICIJ_CSV_ADDRESSES).exists(),
    }


def _icij_print_download_instructions(data_dir: str = _ICIJ_DATA_DIR) -> str:
    """Return human-readable instructions for downloading ICIJ data.

    Parameters:
        data_dir: Target directory for the files.

    Returns:
        Instruction string.
    """
    return (
        f"ICIJ Offshore Leaks bulk CSVs not found at {data_dir}/.\n"
        f"To download (~500MB total):\n"
        f"  1. Visit https://offshoreleaks.icij.org/pages/database\n"
        f"  2. Click 'Download the data' to get the CSV bulk archive\n"
        f"  3. Extract into {data_dir}/\n"
        f"  Expected files:\n"
        f"    - {_ICIJ_CSV_OFFICERS}   (540,000+ officers / beneficial owners)\n"
        f"    - {_ICIJ_CSV_ENTITIES}   (785,000+ shell companies)\n"
        f"    - {_ICIJ_CSV_INTERMEDIARIES} (law firms, banks, agents)\n"
        f"    - {_ICIJ_CSV_RELATIONSHIPS}  (edges connecting all nodes)\n"
        f"    - {_ICIJ_CSV_ADDRESSES}  (registered addresses)\n"
        f"\n"
        f"  Or via command line:\n"
        f"    mkdir -p {data_dir}\n"
        f"    cd {data_dir}\n"
        f"    # Download from ICIJ website (no direct URL — requires browser)\n"
    )


def import_icij_offshore(
    engine: Engine,
    data_dir: str = _ICIJ_DATA_DIR,
    cross_reference: bool = True,
) -> dict:
    """Bulk import ICIJ Offshore Leaks (Panama/Pandora Papers) into actors.

    This is the heavy-lift function for Phase 2/3 scaling.
    785,000+ entities and 540,000+ officers from the ICIJ database.

    Process:
    1. Check if CSVs exist in data_dir; if not, print download instructions
    2. Parse officers CSV -> actors (category='icij_officer')
    3. Parse entities CSV -> actors (category='icij_entity')
    4. Parse intermediaries CSV -> actors (category='icij_intermediary')
    5. Parse relationships CSV -> actor_connections
    6. Cross-reference officers against existing GRID actors

    Parameters:
        engine: SQLAlchemy engine.
        data_dir: Directory containing ICIJ CSV files.
        cross_reference: If True, match ICIJ officers against known actors.

    Returns:
        dict with import counts and cross-reference results.
    """
    _ensure_actors_table(engine)
    result: dict[str, Any] = {
        "officers_imported": 0,
        "entities_imported": 0,
        "intermediaries_imported": 0,
        "relationships_imported": 0,
        "cross_references": 0,
        "status": "PENDING",
    }

    # Step 1: Check CSV availability
    csv_status = _icij_csvs_available(data_dir)
    if not csv_status.get("officers") and not csv_status.get("entities"):
        instructions = _icij_print_download_instructions(data_dir)
        log.warning(instructions)
        result["status"] = "NO_DATA"
        result["instructions"] = instructions
        return result

    base = Path(data_dir)

    # Step 2: Import officers (540K+ people / beneficial owners)
    officers_path = base / _ICIJ_CSV_OFFICERS
    if officers_path.exists():
        log.info("Importing ICIJ officers from {f}...", f=str(officers_path))
        batch_actors: list[dict] = []
        officer_node_map: dict[str, str] = {}  # node_id -> actor_id

        try:
            with open(
                officers_path, "r", encoding="utf-8", errors="replace",
            ) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    name = (row.get("name") or "").strip()
                    if not name:
                        continue
                    node_id = row.get("node_id", "")
                    jurisdiction = row.get("jurisdiction", "")
                    countries = row.get("country_codes", "")
                    source_id = row.get("sourceID", "")

                    actor_id = f"icij_officer_{node_id}" if node_id else (
                        f"icij_officer_{_normalize_name(name)}"
                    )
                    officer_node_map[node_id] = actor_id

                    batch_actors.append({
                        "id": actor_id,
                        "name": name,
                        "tier": "individual",
                        "category": "icij_officer",
                        "title": f"ICIJ Officer ({jurisdiction})",
                        "influence_score": 0.25,
                        "data_sources": ["icij_offshore_leaks"],
                        "credibility": "leaked_data",
                        "motivation_model": "unknown",
                        "source": f"icij_{source_id}" if source_id else "icij",
                        "degree": 2,
                        "icij_node_id": node_id,
                        "metadata": {
                            "jurisdiction": jurisdiction,
                            "countries": countries,
                            "icij_source": source_id,
                            "valid_until": row.get("valid_until", ""),
                            "discovery_source": "icij_bulk_import",
                        },
                    })

            with engine.begin() as conn:
                result["officers_imported"] = _batch_upsert_actors(
                    conn, batch_actors,
                )
            log.info(
                "ICIJ officers: {n} imported from {t} rows",
                n=result["officers_imported"],
                t=len(batch_actors),
            )
        except Exception as exc:
            log.error("ICIJ officer import failed: {e}", e=str(exc))
            result["officer_error"] = str(exc)
    else:
        officer_node_map = {}

    # Step 3: Import entities (785K+ shell companies)
    entities_path = base / _ICIJ_CSV_ENTITIES
    entity_node_map: dict[str, str] = {}  # node_id -> actor_id
    if entities_path.exists():
        log.info("Importing ICIJ entities from {f}...", f=str(entities_path))
        batch_actors = []

        try:
            with open(
                entities_path, "r", encoding="utf-8", errors="replace",
            ) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    name = (row.get("name") or "").strip()
                    if not name:
                        continue
                    node_id = row.get("node_id", "")
                    jurisdiction = row.get("jurisdiction", "")
                    status = row.get("status", "")
                    source_id = row.get("sourceID", "")
                    inc_date = row.get("incorporation_date", "")

                    actor_id = f"icij_entity_{node_id}" if node_id else (
                        f"icij_entity_{_normalize_name(name)}"
                    )
                    entity_node_map[node_id] = actor_id

                    batch_actors.append({
                        "id": actor_id,
                        "name": name,
                        "tier": "individual",
                        "category": "icij_entity",
                        "title": f"Offshore Entity ({jurisdiction})",
                        "influence_score": 0.15,
                        "data_sources": ["icij_offshore_leaks"],
                        "credibility": "leaked_data",
                        "motivation_model": "offshore_structure",
                        "source": f"icij_{source_id}" if source_id else "icij",
                        "degree": 3,
                        "icij_node_id": node_id,
                        "metadata": {
                            "jurisdiction": jurisdiction,
                            "entity_status": status,
                            "incorporation_date": inc_date,
                            "countries": row.get("country_codes", ""),
                            "icij_source": source_id,
                            "discovery_source": "icij_bulk_import",
                        },
                    })

            with engine.begin() as conn:
                result["entities_imported"] = _batch_upsert_actors(
                    conn, batch_actors,
                )
            log.info(
                "ICIJ entities: {n} imported from {t} rows",
                n=result["entities_imported"],
                t=len(batch_actors),
            )
        except Exception as exc:
            log.error("ICIJ entity import failed: {e}", e=str(exc))
            result["entity_error"] = str(exc)

    # Step 4: Import intermediaries (law firms, banks, agents)
    intermediaries_path = base / _ICIJ_CSV_INTERMEDIARIES
    intermediary_node_map: dict[str, str] = {}
    if intermediaries_path.exists():
        log.info(
            "Importing ICIJ intermediaries from {f}...",
            f=str(intermediaries_path),
        )
        batch_actors = []

        try:
            with open(
                intermediaries_path, "r", encoding="utf-8", errors="replace",
            ) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    name = (row.get("name") or "").strip()
                    if not name:
                        continue
                    node_id = row.get("node_id", "")
                    jurisdiction = row.get("jurisdiction", "")
                    source_id = row.get("sourceID", "")

                    actor_id = f"icij_intermed_{node_id}" if node_id else (
                        f"icij_intermed_{_normalize_name(name)}"
                    )
                    intermediary_node_map[node_id] = actor_id

                    batch_actors.append({
                        "id": actor_id,
                        "name": name,
                        "tier": "institutional",
                        "category": "icij_intermediary",
                        "title": f"Offshore Intermediary ({jurisdiction})",
                        "influence_score": 0.30,
                        "data_sources": ["icij_offshore_leaks"],
                        "credibility": "leaked_data",
                        "motivation_model": "facilitator",
                        "source": f"icij_{source_id}" if source_id else "icij",
                        "degree": 2,
                        "icij_node_id": node_id,
                        "metadata": {
                            "jurisdiction": jurisdiction,
                            "countries": row.get("country_codes", ""),
                            "icij_source": source_id,
                            "discovery_source": "icij_bulk_import",
                        },
                    })

            with engine.begin() as conn:
                result["intermediaries_imported"] = _batch_upsert_actors(
                    conn, batch_actors,
                )
            log.info(
                "ICIJ intermediaries: {n} imported from {t} rows",
                n=result["intermediaries_imported"],
                t=len(batch_actors),
            )
        except Exception as exc:
            log.error("ICIJ intermediary import failed: {e}", e=str(exc))
            result["intermediary_error"] = str(exc)

    # Step 5: Import relationships -> actor_connections
    # Merge all node maps for lookup
    all_node_map: dict[str, str] = {
        **officer_node_map,
        **entity_node_map,
        **intermediary_node_map,
    }

    relationships_path = base / _ICIJ_CSV_RELATIONSHIPS
    if relationships_path.exists() and all_node_map:
        log.info(
            "Importing ICIJ relationships from {f}...",
            f=str(relationships_path),
        )
        batch_connections: list[dict] = []

        try:
            with open(
                relationships_path, "r", encoding="utf-8", errors="replace",
            ) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    node_1 = (
                        row.get("node_id_start", "")
                        or row.get("START_ID", "")
                    )
                    node_2 = (
                        row.get("node_id_end", "")
                        or row.get("END_ID", "")
                    )
                    rel_type = (
                        row.get("rel_type", "")
                        or row.get("TYPE", "")
                        or "related_to"
                    )

                    actor_a = all_node_map.get(node_1)
                    actor_b = all_node_map.get(node_2)

                    if actor_a and actor_b and actor_a != actor_b:
                        batch_connections.append({
                            "actor_a": actor_a,
                            "actor_b": actor_b,
                            "relationship": f"icij_{rel_type.lower().replace(' ', '_')}",
                            "strength": 0.6,
                            "evidence": [
                                f"ICIJ relationship: {rel_type}",
                            ],
                        })

            with engine.begin() as conn:
                result["relationships_imported"] = _batch_upsert_connections(
                    conn, batch_connections,
                )
            log.info(
                "ICIJ relationships: {n} imported from {t} edges",
                n=result["relationships_imported"],
                t=len(batch_connections),
            )
        except Exception as exc:
            log.error("ICIJ relationship import failed: {e}", e=str(exc))
            result["relationship_error"] = str(exc)

    # Step 6: Cross-reference ICIJ officers against existing GRID actors
    if cross_reference and officer_node_map:
        xref_count = _cross_reference_icij_with_known_actors(
            engine, officer_node_map,
        )
        result["cross_references"] = xref_count

    result["status"] = "SUCCESS"
    total = (
        result.get("officers_imported", 0)
        + result.get("entities_imported", 0)
        + result.get("intermediaries_imported", 0)
    )
    log.info(
        "ICIJ bulk import complete: {t} actors, {r} relationships, "
        "{x} cross-references",
        t=total,
        r=result.get("relationships_imported", 0),
        x=result.get("cross_references", 0),
    )
    return result


def _cross_reference_icij_with_known_actors(
    engine: Engine,
    officer_node_map: dict[str, str],
) -> int:
    """Cross-reference ICIJ officers against known GRID actors by name.

    Uses the name matching utilities from offshore_leaks module.

    Parameters:
        engine: SQLAlchemy engine.
        officer_node_map: Dict of ICIJ node_id -> actor_id.

    Returns:
        Number of cross-reference connections created.
    """
    try:
        from ingestion.altdata.offshore_leaks import (
            _normalize_name as _icij_normalize,
            _build_known_names_index,
            _match_officer_to_actor,
        )
    except ImportError:
        log.debug("offshore_leaks module not available for cross-reference")
        return 0

    known_index = _build_known_names_index()
    if not known_index:
        return 0

    xref_connections: list[dict] = []

    try:
        with engine.connect() as conn:
            # Get ICIJ officer names from actors table
            rows = conn.execute(text("""
                SELECT id, name, icij_node_id
                FROM actors
                WHERE category = 'icij_officer'
                  AND icij_node_id IS NOT NULL
                LIMIT 100000
            """)).fetchall()

        for row in rows:
            icij_actor_id = row[0]
            officer_name = row[1]
            matched_id, match_type = _match_officer_to_actor(
                officer_name, known_index,
            )
            if matched_id:
                xref_connections.append({
                    "actor_a": icij_actor_id,
                    "actor_b": matched_id,
                    "relationship": f"icij_match_{match_type}",
                    "strength": 0.8 if match_type == "exact" else 0.5,
                    "evidence": [
                        f"ICIJ officer '{officer_name}' matched to "
                        f"known actor '{matched_id}' ({match_type})",
                    ],
                })

        if xref_connections:
            with engine.begin() as conn:
                return _batch_upsert_connections(conn, xref_connections)

    except Exception as exc:
        log.warning(
            "ICIJ cross-reference failed: {e}",
            e=str(exc),
        )

    return 0


# ──────────────────────────────────────────────────────────────────────────
# 5. BOARD INTERLOCKS (cross-company board connections)
# ──────────────────────────────────────────────────────────────────────────

def discover_board_interlocks(engine: Engine) -> dict:
    """Discover cross-company board connections at scale.

    Every public company has ~10 board members. S&P 500 = ~5,000 board seats.
    Many directors sit on multiple boards, creating interlocking directorates.

    This expands on _discover_board_crossrefs() by:
    1. Removing the LIMIT 500 cap on insider queries
    2. Also checking for board_seats stored in actor metadata
    3. Creating company-to-company connections through shared directors
    4. Using batch inserts for performance

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        dict with actors, connections, interlock stats.
    """
    _ensure_actors_table(engine)
    new_actors = 0
    new_connections = 0
    interlocked_directors: set[str] = set()
    company_pairs: set[tuple[str, str]] = set()
    batch_connections: list[dict] = []

    try:
        with engine.begin() as conn:
            # Find ALL insiders who filed Form 4 for multiple companies
            rows = conn.execute(text("""
                SELECT
                    raw_payload->>'insider_name'  AS insider_name,
                    raw_payload->>'insider_title' AS insider_title,
                    ARRAY_AGG(DISTINCT raw_payload->>'ticker')
                        FILTER (WHERE raw_payload->>'ticker' IS NOT NULL)
                        AS tickers,
                    COUNT(DISTINCT raw_payload->>'ticker') AS ticker_count
                FROM raw_series
                WHERE series_id LIKE 'INSIDER:%'
                  AND raw_payload->>'insider_name' IS NOT NULL
                GROUP BY
                    raw_payload->>'insider_name',
                    raw_payload->>'insider_title'
                HAVING COUNT(DISTINCT raw_payload->>'ticker') >= 2
                ORDER BY COUNT(DISTINCT raw_payload->>'ticker') DESC
            """)).fetchall()

            for r in rows:
                name = r[0]
                title = r[1] or ""
                tickers = r[2] if r[2] else []
                if not name or len(tickers) < 2:
                    continue

                # Only count as interlock if title suggests board/executive
                title_lower = title.lower()
                is_board = any(
                    kw in title_lower
                    for kw in (
                        "director", "board", "chairman", "trustee",
                        "ceo", "cfo", "president", "chief", "officer",
                    )
                )
                if not is_board:
                    continue

                actor_id = _actor_id_insider(name)
                interlocked_directors.add(actor_id)

                # Update board_seats in metadata
                conn.execute(text("""
                    UPDATE actors
                    SET board_seats = :boards,
                        metadata = metadata || :meta,
                        updated_at = NOW()
                    WHERE id = :aid
                """), {
                    "aid": actor_id,
                    "boards": json.dumps(tickers),
                    "meta": json.dumps({
                        "interlocking_directorate": True,
                        "board_count": len(tickers),
                        "title_at_filing": title,
                    }),
                })

                # Create connections: director <-> each company
                for ticker in tickers:
                    batch_connections.append({
                        "actor_a": actor_id,
                        "actor_b": f"company_{ticker.lower()}",
                        "relationship": "board_member",
                        "strength": 0.7,
                        "evidence": [
                            f"Form 4 filing: {name} ({title}) at {ticker}",
                        ],
                    })

                # Create company <-> company connections through shared director
                for i in range(len(tickers)):
                    for j in range(i + 1, len(tickers)):
                        pair = tuple(sorted([tickers[i], tickers[j]]))
                        if pair not in company_pairs:
                            company_pairs.add(pair)
                            batch_connections.append({
                                "actor_a": f"company_{pair[0].lower()}",
                                "actor_b": f"company_{pair[1].lower()}",
                                "relationship": "shared_director",
                                "strength": 0.6,
                                "evidence": [
                                    f"Shared director: {name} ({title}) "
                                    f"sits on boards of {pair[0]} and {pair[1]}",
                                ],
                            })

            new_connections = _batch_upsert_connections(conn, batch_connections)

    except Exception as exc:
        log.error("Board interlock discovery failed: {e}", e=str(exc))
        return {
            "interlocked_directors": len(interlocked_directors),
            "connections_created": new_connections,
            "company_pairs": len(company_pairs),
            "error": str(exc),
        }

    log.info(
        "Board interlocks: {d} directors, {c} connections, "
        "{p} company pairs linked",
        d=len(interlocked_directors),
        c=new_connections,
        p=len(company_pairs),
    )
    return {
        "interlocked_directors": len(interlocked_directors),
        "connections_created": new_connections,
        "company_pairs": len(company_pairs),
    }


# ──────────────────────────────────────────────────────────────────────────
# 6. 3-DEGREE BFS GRAPH EXPANSION
# ──────────────────────────────────────────────────────────────────────────

def run_3_degree_expansion(engine: Engine, max_per_degree: int = 100_000) -> dict:
    """BFS expansion from seed actors, 3 hops out through connections.

    Starting from degree-0 seed actors (from actor_network._KNOWN_ACTORS),
    expands outward:
      Degree 1: Everyone directly connected to seeds (~5K)
      Degree 2: Everyone THOSE people connect to (~50K)
      Degree 3: One more hop (~250K)

    Uses the actor_connections table as the edge list for BFS.
    Only assigns degree to actors that don't already have a lower degree.

    Parameters:
        engine: SQLAlchemy engine.
        max_per_degree: Maximum actors to expand per degree (safety cap).

    Returns:
        dict with counts per degree level.
    """
    _ensure_actors_table(engine)
    result: dict[str, Any] = {
        "degree_0": 0,
        "degree_1": 0,
        "degree_2": 0,
        "degree_3": 0,
        "total": 0,
    }

    try:
        with engine.begin() as conn:
            # Seed: mark all existing known actors as degree 0
            # (actors from actor_network._KNOWN_ACTORS imported into actors table)
            conn.execute(text("""
                UPDATE actors SET degree = 0
                WHERE degree IS NULL
                  AND (
                      source IN ('seed_13f', 'actor_network')
                      OR id IN (
                          SELECT DISTINCT actor_a FROM actor_connections
                          UNION
                          SELECT DISTINCT actor_b FROM actor_connections
                      )
                  )
                  AND (metadata->>'discovery_source') IS NULL
            """))

            # Count degree 0
            row = conn.execute(text(
                "SELECT COUNT(*) FROM actors WHERE degree = 0",
            )).fetchone()
            result["degree_0"] = int(row[0]) if row else 0

            # BFS: degree 1, 2, 3
            for target_degree in (1, 2, 3):
                prev_degree = target_degree - 1

                # Find all actors at previous degree
                seed_rows = conn.execute(text("""
                    SELECT id FROM actors WHERE degree = :d
                    LIMIT :lim
                """), {"d": prev_degree, "lim": max_per_degree}).fetchall()

                seed_ids = [r[0] for r in seed_rows]
                if not seed_ids:
                    log.info(
                        "BFS degree {d}: no seeds at degree {p}, stopping",
                        d=target_degree,
                        p=prev_degree,
                    )
                    break

                # Find all actors connected to seeds that don't have
                # a degree assignment yet (or have a higher degree)
                # Process in chunks to avoid huge IN clauses
                expanded = 0
                chunk_size = 500
                for i in range(0, len(seed_ids), chunk_size):
                    chunk = seed_ids[i : i + chunk_size]
                    # Use a temp approach: find neighbors via connections
                    neighbors = conn.execute(text("""
                        SELECT DISTINCT
                            CASE
                                WHEN actor_a = ANY(:seeds) THEN actor_b
                                ELSE actor_a
                            END AS neighbor_id
                        FROM actor_connections
                        WHERE actor_a = ANY(:seeds) OR actor_b = ANY(:seeds)
                    """), {"seeds": chunk}).fetchall()

                    neighbor_ids = [n[0] for n in neighbors]
                    if not neighbor_ids:
                        continue

                    # Update degree for neighbors that don't have one
                    # or have a higher degree
                    for nid_chunk_start in range(
                        0, len(neighbor_ids), chunk_size,
                    ):
                        nid_chunk = neighbor_ids[
                            nid_chunk_start : nid_chunk_start + chunk_size
                        ]
                        upd = conn.execute(text("""
                            UPDATE actors
                            SET degree = :deg
                            WHERE id = ANY(:nids)
                              AND (degree IS NULL OR degree > :deg)
                        """), {
                            "deg": target_degree,
                            "nids": nid_chunk,
                        })
                        expanded += upd.rowcount

                result[f"degree_{target_degree}"] = expanded
                log.info(
                    "BFS degree {d}: {n} actors expanded from {s} seeds",
                    d=target_degree,
                    n=expanded,
                    s=len(seed_ids),
                )

            # Total
            total_row = conn.execute(text(
                "SELECT COUNT(*) FROM actors WHERE degree IS NOT NULL",
            )).fetchone()
            result["total"] = int(total_row[0]) if total_row else 0

    except Exception as exc:
        log.error("3-degree BFS expansion failed: {e}", e=str(exc))
        result["error"] = str(exc)

    log.info(
        "BFS expansion complete: D0={d0}, D1={d1}, D2={d2}, D3={d3}, "
        "total={t}",
        d0=result["degree_0"],
        d1=result["degree_1"],
        d2=result["degree_2"],
        d3=result["degree_3"],
        t=result["total"],
    )
    return result


# ──────────────────────────────────────────────────────────────────────────
# 7. SCALE ORCHESTRATOR
# ──────────────────────────────────────────────────────────────────────────

def run_scale_discovery(engine: Engine, target_phase: int = 1) -> dict:
    """Run a scaled discovery cycle targeting the specified phase count.

    Phase 1 (5,000):   All insiders + all 13F + all congressional
    Phase 2 (50,000):  Phase 1 + board interlocks + ICIJ officers
    Phase 3 (250,000): Phase 2 + full ICIJ entities + 3-degree BFS

    Parameters:
        engine: SQLAlchemy engine.
        target_phase: 1, 2, or 3.

    Returns:
        dict with per-source results and aggregate statistics.
    """
    _ensure_actors_table(engine)
    target = SCALE_TARGETS.get(f"phase_{target_phase}", 5_000)
    log.info(
        "=== SCALE DISCOVERY Phase {p} START (target: {t:,} actors) ===",
        p=target_phase,
        t=target,
    )
    cycle_start = datetime.now(timezone.utc)
    results: dict[str, Any] = {
        "target_phase": target_phase,
        "target_count": target,
    }

    # ── Phase 1: core data sources ────────────────────────────────
    log.info("Scale Phase 1: batch insider discovery...")
    results["batch_insiders"] = batch_discover_insiders(engine, days_back=365)

    log.info("Scale Phase 1: full 13F filer discovery...")
    results["all_13f"] = discover_all_13f_filers(engine)

    log.info("Scale Phase 1: full congressional discovery...")
    results["all_congress"] = discover_all_congress(engine)

    # Also run the existing discovery sources
    log.info("Scale Phase 1: lobbyists + gov officials...")
    results["lobbyists"] = _discover_lobbyists(engine)
    results["gov_officials"] = _discover_gov_officials(engine)

    # Check Phase 1 count
    stats = get_actor_stats(engine)
    results["after_phase1_count"] = stats.get("total_actors", 0)
    log.info(
        "After Phase 1: {n:,} actors",
        n=results["after_phase1_count"],
    )

    if target_phase < 2:
        results["status"] = "PHASE_1_COMPLETE"
        results["stats"] = stats
        results["elapsed_seconds"] = (
            datetime.now(timezone.utc) - cycle_start
        ).total_seconds()
        return results

    # ── Phase 2: board interlocks + ICIJ officers ─────────────────
    log.info("Scale Phase 2: board interlocks...")
    results["board_interlocks"] = discover_board_interlocks(engine)

    log.info("Scale Phase 2: ICIJ offshore import...")
    results["icij_import"] = import_icij_offshore(engine)

    stats = get_actor_stats(engine)
    results["after_phase2_count"] = stats.get("total_actors", 0)
    log.info(
        "After Phase 2: {n:,} actors",
        n=results["after_phase2_count"],
    )

    if target_phase < 3:
        results["status"] = "PHASE_2_COMPLETE"
        results["stats"] = stats
        results["elapsed_seconds"] = (
            datetime.now(timezone.utc) - cycle_start
        ).total_seconds()
        return results

    # ── Phase 3: 3-degree BFS expansion ───────────────────────────
    log.info("Scale Phase 3: 3-degree graph expansion...")
    results["bfs_expansion"] = run_3_degree_expansion(engine)

    # Run connection discovery across the expanded graph
    log.info("Scale Phase 3: discovering connections in expanded graph...")
    results["expanded_connections"] = auto_discover_connections(engine)

    # Final stats
    stats = get_actor_stats(engine)
    results["stats"] = stats
    results["status"] = "PHASE_3_COMPLETE"

    elapsed = (datetime.now(timezone.utc) - cycle_start).total_seconds()
    results["elapsed_seconds"] = round(elapsed, 1)

    log.info(
        "=== SCALE DISCOVERY Phase {p} END === "
        "{n:,} total actors, {c:,} connections, {s:.1f}s elapsed",
        p=target_phase,
        n=stats.get("total_actors", 0),
        c=stats.get("total_connections", 0),
        s=elapsed,
    )
    return results


# ══════════════════════════════════════════════════════════════════════════
# HERMES SCHEDULING HOOK
# ══════════════════════════════════════════════════════════════════════════

def hermes_daily_actor_discovery(engine: Engine) -> dict:
    """Entry point for hermes daily scheduling.

    This function is the hook that hermes calls once per day.
    It runs the full discovery cycle and returns a status dict.

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        dict with cycle results.
    """
    return run_discovery_cycle(engine)
