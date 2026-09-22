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

    Every row written here is hand-curated content about a named real person
    or organization, so each one is stamped ``provenance = 'seed'`` and
    ``updated_at = SEED_VINTAGE_TS`` -- the date those figures were last
    hand-edited, *not* ``NOW()``. Stamping wall-clock time made a net-worth
    literal typed in months ago look like a reading taken this second
    (audit A-H13).

    ``provenance = 'seed'`` is a claim about current state, not permanent
    origin (see ``migrations/versions/actors_provenance_columns_0922.py`` and
    ``intelligence/actors/provenance.py`` for the precise definition,
    including why a moved ``updated_at`` alone is not proof of a real
    observation). A row already stamped anything other than
    ``PROVENANCE_SEED`` or ``PROVENANCE_UNKNOWN`` -- ``'observed'`` (confirmed
    by ``save_actor``'s writer contract) or ``'unconfirmed'`` (touched by
    something else, not confirmed) -- must not be reset by a later call here:
    that would either relabel genuinely-observed data as a hand-typed guess,
    or paper over an unconfirmed modification by reasserting "still pristine
    seed data" over it.

    ``PROVENANCE_UNKNOWN`` alone is NOT enough to make a row fair game,
    though. ``'unknown'`` means *unverified* -- nothing has formally
    classified this row -- not *disposable*. Several writers besides
    ``save_actor`` insert or update ``actors`` rows directly and never touch
    ``provenance`` at all (``intelligence/actor_discovery.py``,
    ``intelligence/actors/trial_bridge.py``, ``intelligence/actor_ingest.py``,
    among others) -- a row one of them created or enriched with real
    ``data_sources`` content can sit at the column default ``'unknown'``
    indefinitely despite carrying genuine, non-trivial information. Treating
    every ``'unknown'`` row as equivalent to "doesn't exist yet" would let
    this reseed silently clobber that enrichment the moment its id happens to
    also be on the curated seed list. The guard below therefore only treats
    an ``'unknown'`` row as fair game when it is ALSO still pristine, checked
    across every column a writer can populate independently of
    ``data_sources``: ``data_sources`` itself empty or absent (the same
    evidence signal ``save_actor`` gates on -- see that function), AND
    ``title`` empty or absent, AND ``net_worth_estimate`` absent, AND ``aum``
    absent. This closes a real gap the ``data_sources``-only check left open:
    ``intelligence/actor_discovery.py``'s own upsert function unconditionally
    overwrites ``name``/``title`` on every conflict regardless of whether its
    caller passed ``data_sources`` (a ``None`` default on that function), and
    ``scripts/seed_vip_network.py`` writes ``title``/``net_worth_estimate``
    directly without ever touching ``data_sources`` at all -- both can leave
    a row with real, enriched content sitting behind an empty
    ``data_sources`` list. ``influence_score``, ``trust_score``,
    ``motivation_model``, and ``credibility`` are deliberately NOT part of
    this check: unlike ``title``/``net_worth_estimate``/``aum`` (nullable,
    no schema default -- non-null is unambiguous evidence a writer set them),
    these four columns carry non-null defaults in ``_ensure_tables``
    (``0.5``, ``0.5``, ``'unknown'``, ``'inferred'``) that a genuine writer
    could also plausibly assign for real, so a value equal to the default
    cannot be told apart from "never touched" -- this is a known, bounded
    limitation of the pristine check, not a silent gap. A row still
    ``'unknown'`` but merely touched (``updated_at`` moved, none of the
    checked columns carrying real content) remains eligible: a bare
    timestamp move carries no evidentiary content in this design, on either
    side of a promotion OR a protection decision, so it is not itself
    grounds to withhold seeding. What must be protected is recorded data,
    not clock movement.

    Because ``influence_score`` and the other seed-authored fields would
    otherwise keep refreshing from ``_KNOWN_ACTORS`` on every call regardless
    of the row's provenance -- silently overwriting genuinely observed,
    unconfirmed-but-real, or enriched-but-unclassified values while the label
    claims something else -- the ``ON CONFLICT ... WHERE`` clause below
    suppresses the *entire* update, not just the provenance columns, unless
    the existing row is still exactly ``'seed'``, or still ``'unknown'`` AND
    still pristine across ``data_sources``/``title``/``net_worth_estimate``/
    ``aum`` as described above.

    Returns:
        Number of actors upserted.
    """
    from intelligence.actors.provenance import (
        PROVENANCE_SEED,
        PROVENANCE_UNKNOWN,
        SEED_VINTAGE,
        SEED_VINTAGE_TS,
    )

    _ensure_tables(engine)
    count = 0
    with engine.begin() as conn:
        for actor_id, data in _KNOWN_ACTORS.items():
            conn.execute(text("""
                INSERT INTO actors (
                    id, name, tier, category, title,
                    net_worth_estimate, aum, influence_score,
                    trust_score, motivation_model,
                    data_sources, credibility,
                    provenance, provenance_as_of, updated_at
                ) VALUES (
                    :id, :name, :tier, :category, :title,
                    :nw, :aum, :inf,
                    :trust, :motivation,
                    :sources, :cred,
                    :provenance, :vintage_date, :vintage_ts
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
                    provenance = EXCLUDED.provenance,
                    provenance_as_of = EXCLUDED.provenance_as_of,
                    updated_at = EXCLUDED.updated_at
                WHERE actors.provenance = :seed
                   OR (
                       actors.provenance = :unknown
                       AND (actors.data_sources IS NULL OR actors.data_sources = '[]'::jsonb)
                       AND (actors.title IS NULL OR actors.title = '')
                       AND actors.net_worth_estimate IS NULL
                       AND actors.aum IS NULL
                   )
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
                "provenance": PROVENANCE_SEED,
                "vintage_date": SEED_VINTAGE,
                "vintage_ts": SEED_VINTAGE_TS,
                "seed": PROVENANCE_SEED,
                "unknown": PROVENANCE_UNKNOWN,
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

    The smallest writer transition to ``PROVENANCE_OBSERVED``: this is the one
    writer contract intelligence/actors/provenance.py trusts as evidence of a
    real observation (see that module's docstring) -- but the trust is EARNED
    per call, not assumed from the mere fact that this function was the
    caller. ``provenance = 'observed'`` is stamped only when ``data`` carries
    qualifying evidence -- a non-empty ``data_sources`` list, the same
    "where did this come from" signal every other real writer in this
    codebase uses (``intelligence/actor_discovery.py``,
    ``intelligence/actors/trial_bridge.py``, and ``_seed_known_actors``'s own
    reseed guard). A call with no ``data_sources`` (missing, ``None``, or an
    empty list) is a maintenance-only touch: it still writes the row's
    identity fields and bumps ``updated_at`` (the same liveness signal every
    other maintenance writer produces), but it MUST NOT create or overwrite
    ``provenance`` -- a brand-new row reads the column's own honest
    ``'unknown'`` default, and an existing row's classification, whatever it
    is (``'unknown'``, ``'seed'``, a confirmed ``'observed'``, or an
    ``'unconfirmed'``), is left completely untouched. This is what makes
    "timestamp changes alone never qualify" (see the module docstring) true
    of THIS writer too, not just the other maintenance-only ones -- an empty
    or missing evidence payload must never manufacture a confirmed
    observation.

    ``influence_score`` is deliberately NOT part of the evidence test: a
    legitimate observation can carry a real score of exactly ``0.0``, and
    every caller in this codebase (``intelligence/spider/discovery.py``)
    always supplies SOME numeric default regardless of whether real evidence
    exists, so gating on it would both reject a genuine zero-score
    observation and let a placeholder-only call through. ``data_sources`` is
    the one field that is reliably empty when nothing was actually found
    (see ``intelligence/spider/discovery.py``'s
    ``[dc.evidence[0]...] if dc.evidence else []`` -- a real, existing
    no-evidence path this gate closes).

    When evidence IS present, the write stamps ``provenance = 'observed'``
    regardless of what the row's provenance was before (``'unknown'``,
    ``'seed'``, or a stale ``'unconfirmed'``), because a genuine observation
    right now supersedes any of those -- alongside the real evidence itself
    (a merged ``data_sources`` list, a ``GREATEST``-combined
    ``influence_score``).
    """
    from intelligence.actors.provenance import PROVENANCE_OBSERVED

    data_sources = data.get("data_sources") or []
    has_evidence = bool(data_sources)

    params = {
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
        "data_sources": json.dumps(data_sources),
    }

    with engine.connect() as conn:
        if has_evidence:
            conn.execute(
                text("""
                    INSERT INTO actors (id, name, tier, category, title, influence_score,
                        trust_score, degree, source, credibility, data_sources,
                        provenance, provenance_as_of, updated_at)
                    VALUES (:id, :name, :tier, :category, :title, :influence,
                        :trust, :degree, :source, :credibility, :data_sources,
                        :provenance, NULL, NOW())
                    ON CONFLICT (id) DO UPDATE SET
                        influence_score = GREATEST(actors.influence_score, EXCLUDED.influence_score),
                        data_sources = EXCLUDED.data_sources,
                        provenance = :provenance,
                        provenance_as_of = NULL,
                        updated_at = NOW()
                """),
                {**params, "provenance": PROVENANCE_OBSERVED},
            )
        else:
            # No qualifying evidence -- a maintenance-only call. provenance /
            # provenance_as_of are never referenced here at all: a brand-new
            # row reads the column's own honest default, and an existing
            # row's classification -- confirmed or not -- is left exactly as
            # it was.
            conn.execute(
                text("""
                    INSERT INTO actors (id, name, tier, category, title, influence_score,
                        trust_score, degree, source, credibility, data_sources, updated_at)
                    VALUES (:id, :name, :tier, :category, :title, :influence,
                        :trust, :degree, :source, :credibility, :data_sources, NOW())
                    ON CONFLICT (id) DO UPDATE SET
                        updated_at = NOW()
                """),
                params,
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
