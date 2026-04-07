"""Database operations for the spider — queue persistence and connection storage."""

from __future__ import annotations

import json
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.spider.models import ConnectionMeta


def ensure_spider_tables(engine: Engine) -> None:
    """Create spider_queue and spider_runs tables if they don't exist."""
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
    """Upsert an actor into the actors table."""
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


def save_connection(engine: Engine, actor_a: str, actor_b: str, meta: ConnectionMeta) -> None:
    """Upsert a connection into actor_connections."""
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
