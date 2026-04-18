"""
GRID Intelligence — Actor Network database layer.

Handles table creation, actor seeding, and loading actors from the DB.
All functions are pure side-effect-free reads or transactional writes.
"""

from __future__ import annotations

import json
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.actors.models import Actor
from intelligence.actors.seed_data import _KNOWN_ACTORS


def _ensure_tables(engine: Engine) -> None:
    """Create the actors and wealth_flows tables if they do not exist.

    Parameters:
        engine: SQLAlchemy engine connected to the GRID database.
    """
    with engine.begin() as conn:
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
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_actors_tier
                ON actors (tier)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_actors_influence
                ON actors (influence_score DESC)
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS wealth_flows (
                id              SERIAL PRIMARY KEY,
                from_actor      TEXT REFERENCES actors(id),
                to_entity       TEXT NOT NULL,
                amount_estimate NUMERIC,
                confidence      TEXT DEFAULT 'inferred',
                evidence        JSONB DEFAULT '[]',
                flow_date       DATE,
                implication     TEXT,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_wealth_flows_date
                ON wealth_flows (flow_date DESC)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_wealth_flows_actor
                ON wealth_flows (from_actor)
        """))
    log.debug("actors / wealth_flows tables ensured")


def _seed_known_actors(engine: Engine) -> int:
    """Insert or update all _KNOWN_ACTORS into the actors table.

    Returns:
        Number of actors upserted.
    """
    _ensure_tables(engine)
    count = 0
    with engine.begin() as conn:
        for actor_id, data in _KNOWN_ACTORS.items():
            conn.execute(text("""
                INSERT INTO actors (
                    id, name, tier, category, title,
                    net_worth_estimate, aum, influence_score,
                    trust_score, motivation_model,
                    data_sources, credibility, updated_at
                ) VALUES (
                    :id, :name, :tier, :category, :title,
                    :nw, :aum, :inf,
                    :trust, :motivation,
                    :sources, :cred, NOW()
                )
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    tier = EXCLUDED.tier,
                    category = EXCLUDED.category,
                    title = EXCLUDED.title,
                    net_worth_estimate = COALESCE(EXCLUDED.net_worth_estimate, actors.net_worth_estimate),
                    aum = COALESCE(EXCLUDED.aum, actors.aum),
                    influence_score = EXCLUDED.influence_score,
                    motivation_model = EXCLUDED.motivation_model,
                    data_sources = EXCLUDED.data_sources,
                    credibility = EXCLUDED.credibility,
                    updated_at = NOW()
            """), {
                "id": actor_id,
                "name": data["name"],
                "tier": data["tier"],
                "category": data["category"],
                "title": data["title"],
                "nw": data.get("net_worth_estimate"),
                "aum": data.get("aum"),
                "inf": data.get("influence_score", 0.5),
                "trust": data.get("trust_score", 0.5),
                "motivation": data.get("motivation_model", "unknown"),
                "sources": json.dumps(data.get("data_sources", [])),
                "cred": data.get("credibility", "inferred"),
            })
            count += 1
    log.info("Seeded {n} actors into the database", n=count)
    return count


_ICIJ_CATEGORIES = frozenset({"icij_entity", "icij_officer", "icij_intermediary"})


def _load_actors_from_db(
    engine: Engine,
    *,
    exclude_categories: frozenset[str] | None = _ICIJ_CATEGORIES,
) -> dict[str, Actor]:
    """Load actors from the DB into Actor dataclass instances.

    Parameters:
        engine: SQLAlchemy engine.
        exclude_categories: Categories to skip (default: ICIJ bulk data).
            Pass ``None`` or ``frozenset()`` to load everything.

    Returns:
        Dict mapping actor_id -> Actor.
    """
    actors: dict[str, Actor] = {}
    try:
        with engine.connect() as conn:
            if exclude_categories:
                query = text("""
                    SELECT id, name, tier, category, title,
                           net_worth_estimate, aum, influence_score,
                           trust_score, motivation_model,
                           connections, known_positions, board_seats,
                           political_affiliations, data_sources, credibility
                    FROM actors
                    WHERE category != ALL(:excluded)
                    ORDER BY influence_score DESC
                """)
                rows = conn.execute(
                    query, {"excluded": list(exclude_categories)}
                ).fetchall()
            else:
                rows = conn.execute(text("""
                    SELECT id, name, tier, category, title,
                           net_worth_estimate, aum, influence_score,
                           trust_score, motivation_model,
                           connections, known_positions, board_seats,
                           political_affiliations, data_sources, credibility
                    FROM actors
                    ORDER BY influence_score DESC
                """)).fetchall()
            for r in rows:
                actors[r[0]] = Actor(
                    id=r[0],
                    name=r[1],
                    tier=r[2],
                    category=r[3],
                    title=r[4] or "",
                    net_worth_estimate=float(r[5]) if r[5] is not None else None,
                    aum=float(r[6]) if r[6] is not None else None,
                    influence_score=float(r[7]) if r[7] is not None else 0.5,
                    trust_score=float(r[8]) if r[8] is not None else 0.5,
                    motivation_model=r[9] or "unknown",
                    connections=_parse_jsonb(r[10]),
                    known_positions=_parse_jsonb(r[11]),
                    board_seats=_parse_jsonb(r[12]),
                    political_affiliations=_parse_jsonb(r[13]),
                    data_sources=_parse_jsonb(r[14]),
                    credibility=r[15] or "inferred",
                )
    except Exception as exc:
        log.warning("Failed to load actors from DB: {e}", e=str(exc))
    return actors


def _parse_jsonb(val: Any) -> list:
    """Safely parse a JSONB field that may arrive as str, list, or None."""
    if val is None:
        return []
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            return parsed if isinstance(parsed, list) else []
        except (json.JSONDecodeError, TypeError):
            return []
    return []


# ══════════════════════════════════════════════════════════════════════════
# Spider writers (merged from intelligence/spider/db.py — SYNTH-15)
# Canonical writer for actors / actor_connections / spider_queue / spider_runs
# ══════════════════════════════════════════════════════════════════════════


def ensure_spider_tables(engine: Engine) -> None:
    """Create spider_queue and spider_runs tables if they don't exist.

    Moved from intelligence/spider/db.py during SYNTH-15 dedupe.
    """
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS spider_queue (
                actor_id        TEXT PRIMARY KEY,
                priority        NUMERIC NOT NULL DEFAULT 0,
                degree          INT NOT NULL DEFAULT 0,
                status          TEXT NOT NULL DEFAULT 'pending',
                queued_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                started_at      TIMESTAMPTZ,
                completed_at    TIMESTAMPTZ,
                sources_checked JSONB DEFAULT '[]',
                connections_found INT DEFAULT 0,
                actors_created  INT DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_spider_queue_priority
                ON spider_queue (priority DESC) WHERE status = 'pending';

            CREATE TABLE IF NOT EXISTS spider_runs (
                id               SERIAL PRIMARY KEY,
                started_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                completed_at     TIMESTAMPTZ,
                actors_processed INT DEFAULT 0,
                connections_found INT DEFAULT 0,
                new_actors       INT DEFAULT 0,
                max_degree_reached INT DEFAULT 0,
                errors           JSONB DEFAULT '[]'
            );
        """))
        conn.commit()
    log.info("Spider tables ensured")


def save_actor(engine: Engine, actor_id: str, data: dict[str, Any]) -> None:
    """Upsert an actor into the actors table (spider writer).

    Moved from intelligence/spider/db.py during SYNTH-15 dedupe. Note: this
    spider-oriented upsert uses a slim column set (influence/trust/degree/source)
    and is distinct from _seed_known_actors which uses the fuller seed schema.
    """
    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO actors (id, name, tier, category, title, influence_score,
                    trust_score, degree, source, credibility, data_sources, updated_at)
                VALUES (:id, :name, :tier, :category, :title, :influence,
                    :trust, :degree, :source, :credibility, :data_sources, NOW())
                ON CONFLICT (id) DO UPDATE SET
                    influence_score = GREATEST(actors.influence_score, EXCLUDED.influence_score),
                    data_sources = EXCLUDED.data_sources,
                    updated_at = NOW()
            """),
            {
                "id": actor_id,
                "name": data.get("name", ""),
                "tier": data.get("tier", "institutional"),
                "category": data.get("category", "corporation"),
                "title": data.get("title", ""),
                "influence": data.get("influence_score", 0.3),
                "trust": data.get("trust_score", 0.5),
                "degree": data.get("degree", 0),
                "source": data.get("source", "spider"),
                "credibility": data.get("credibility", "inferred"),
                "data_sources": json.dumps(data.get("data_sources", [])),
            },
        )
        conn.commit()


def save_connection(engine: Engine, actor_a: str, actor_b: str, meta: Any) -> None:
    """Upsert a connection into actor_connections.

    Moved from intelligence/spider/db.py during SYNTH-15 dedupe. ``meta`` is a
    ``ConnectionMeta`` (imported lazily to avoid a hard spider dependency).
    """
    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO actor_connections (actor_a, actor_b, relationship, strength, evidence)
                VALUES (:a, :b, :rel, :strength, :evidence)
                ON CONFLICT (actor_a, actor_b, relationship)
                DO UPDATE SET
                    strength = GREATEST(actor_connections.strength, EXCLUDED.strength),
                    evidence = EXCLUDED.evidence
            """),
            {
                "a": actor_a,
                "b": actor_b,
                "rel": meta.relationship,
                "strength": meta.strength,
                "evidence": json.dumps([{"source": s} for s in meta.sources]),
            },
        )
        conn.commit()
