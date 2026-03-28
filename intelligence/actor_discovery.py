"""
GRID Intelligence — Automated Actor Discovery & Enrichment.

Instead of hardcoding ~200 actors, this module continuously discovers new
actors from GRID's ingested data sources and enriches them with cross-
referenced metadata.  Every Form 4 filer, congressional trader, 13F
institution, lobbyist, and government official becomes an actor — and
connections between them are inferred automatically.

Scale targets:
    Phase 1  —   500 actors  (insiders who traded >$1M)
    Phase 2  — 2,000 actors  (all 13F filers + congressional + insiders)
    Phase 3  — 5,000+ actors (lobbyists, government officials, board members)

Key entry points:
    auto_discover_actors      — scan all data sources, create new actors
    auto_discover_connections — find links between actors
    enrich_actor              — pull all available data for one actor
    enrich_all_actors         — batch enrichment
    run_discovery_cycle       — daily orchestrator for hermes scheduling
    get_actor_stats           — dashboard statistics

Wired into:
    - LLM task queue as P3 background: continuously discover and enrich
    - hermes: run_discovery_cycle() daily
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
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


def run_discovery_cycle(engine: Engine) -> dict:
    """Full daily discovery + enrichment cycle.

    Orchestrator for hermes scheduling.  Runs:
      1. auto_discover_actors  — find new actors
      2. auto_discover_connections — find links
      3. enrich_all_actors — update data for stale actors

    Parameters:
        engine: SQLAlchemy engine.

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

    # Step 4: get stats
    stats = get_actor_stats(engine)

    cycle_end = datetime.now(timezone.utc)
    elapsed = (cycle_end - cycle_start).total_seconds()

    result = {
        "status": "SUCCESS",
        "discovery": discovery_result,
        "connections": connection_result,
        "enrichment": enrichment_result,
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

            # Phase assessment
            total = stats["total_actors"]
            if total >= 5000:
                stats["phase"] = "Phase 3 (5,000+ actors)"
            elif total >= 2000:
                stats["phase"] = "Phase 2 (2,000+ actors)"
            elif total >= 500:
                stats["phase"] = "Phase 1 (500+ actors)"
            else:
                stats["phase"] = f"Pre-Phase 1 ({total} actors)"

    except Exception as exc:
        log.warning("Failed to compute actor stats: {e}", e=str(exc))
        stats["error"] = str(exc)

    return stats


# ══════════════════════════════════════════════════════════════════════════
# LLM TASK QUEUE INTEGRATION
# ══════════════════════════════════════════════════════════════════════════

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
