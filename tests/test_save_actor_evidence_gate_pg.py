"""Real-PostgreSQL proof that intelligence/actors/db.py::save_actor gates the
``PROVENANCE_OBSERVED`` transition on qualifying evidence -- part of the #596
remediation reconciliation (00-Agent-Reports/2026-09-22/claude__ANIK__grid-596-
remediation-plan.md).

Before this file's changes, ``save_actor`` stamped ``provenance = 'observed'``
unconditionally on every call, regardless of whether the call actually carried any
evidence. That made the writer's own contract -- described in
``intelligence/actors/provenance.py`` as "every call writes real evidence... never
just a timestamp" -- an assumption about the caller, not something the function
itself verified. A call with no evidence at all (a real, existing path:
``intelligence/spider/discovery.py`` produces exactly this shape,
``"data_sources": [] if not dc.evidence else [...]``, when a discovered connection
carries no evidence) would still have earned the strongest provenance claim.

Proves, against the real function (not a re-implementation):
  1. Missing ``data_sources`` (key absent) on a brand-new row does not create
     ``PROVENANCE_OBSERVED`` -- the row reads the column's own honest default.
  2. An explicit empty ``data_sources`` list does the same.
  3. An explicit ``data_sources = None`` does the same (a defensive input shape,
     not just "key omitted").
  4. A maintenance-only call (no evidence) through the REAL save_actor preserves an
     existing CONFIRMED ('observed') row's provenance untouched, while still moving
     updated_at (the call is not a no-op -- it is a liveness touch with zero
     evidentiary weight, exactly like every other maintenance writer in this
     codebase).
  5. The same maintenance-only call does not promote a 'seed' row to 'observed'
     either -- provenance is preserved regardless of which classified state it
     started in, not just 'observed' specifically.
  6. A legitimate observation whose score is exactly 0.0 is both classified
     'observed' (qualifying evidence is data_sources, not influence_score, so a
     falsy-but-real score is never mistaken for "no evidence") and the 0.0 value
     itself is stored verbatim, not silently replaced by the 0.3 placeholder default.

Uses the shared ``pg_engine`` fixture (tests/conftest.py) -- skips cleanly if no
PostgreSQL is reachable.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.actors.db import save_actor
from intelligence.actors.provenance import (
    PROVENANCE_OBSERVED,
    PROVENANCE_SEED,
    PROVENANCE_UNKNOWN,
)

_MINIMAL_ACTORS_DDL = """
CREATE TABLE IF NOT EXISTS actors (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    tier TEXT NOT NULL,
    category TEXT NOT NULL,
    title TEXT,
    influence_score DOUBLE PRECISION,
    trust_score DOUBLE PRECISION,
    degree INT,
    source TEXT,
    credibility TEXT,
    data_sources JSONB,
    provenance TEXT NOT NULL DEFAULT 'unknown',
    provenance_as_of DATE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""


@pytest.fixture(autouse=True)
def _actors_table(pg_engine: Engine):
    with pg_engine.begin() as conn:
        conn.execute(text(_MINIMAL_ACTORS_DDL))
        for stmt in (
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS provenance TEXT NOT NULL DEFAULT 'unknown'",
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS provenance_as_of DATE",
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS title TEXT",
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS influence_score DOUBLE PRECISION",
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS trust_score DOUBLE PRECISION",
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS degree INT",
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS source TEXT",
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS credibility TEXT",
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS data_sources JSONB",
        ):
            conn.execute(text(stmt))
    yield


@pytest.fixture
def test_ids() -> list[str]:
    return []


@pytest.fixture(autouse=True)
def cleanup_test_rows(pg_engine: Engine, test_ids: list[str]):
    yield
    if not test_ids:
        return
    with pg_engine.begin() as conn:
        conn.execute(text("DELETE FROM actors WHERE id = ANY(:ids)").bindparams(ids=test_ids))


def _row(pg_engine: Engine, actor_id: str):
    with pg_engine.connect() as conn:
        return conn.execute(text(
            "SELECT provenance, influence_score, data_sources::text AS data_sources, updated_at "
            "FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()


@pytest.mark.parametrize("evidence_free_data", [
    {},
    {"data_sources": None},
    {"data_sources": []},
])
def test_missing_or_empty_evidence_does_not_create_observed_provenance(
    pg_engine: Engine, test_ids: list[str], evidence_free_data: dict,
):
    actor_id = f"save_actor_gate_pg_{uuid.uuid4().hex[:16]}"
    test_ids.append(actor_id)

    save_actor(pg_engine, actor_id, {"name": "No Evidence Yet", "tier": "test", "category": "test", **evidence_free_data})

    row = _row(pg_engine, actor_id)
    assert row is not None, "a maintenance-only call must still create the row"
    assert row.provenance == PROVENANCE_UNKNOWN, (
        "invalid/absent evidence must never earn PROVENANCE_OBSERVED -- the row reads "
        "the column's own honest default"
    )


def test_maintenance_only_call_preserves_an_existing_confirmed_observed_row(
    pg_engine: Engine, test_ids: list[str],
):
    actor_id = f"save_actor_gate_pg_{uuid.uuid4().hex[:16]}"
    test_ids.append(actor_id)

    save_actor(pg_engine, actor_id, {
        "name": "Confirmed Actor", "tier": "test", "category": "test",
        "influence_score": 0.61, "data_sources": ["sec_form4"],
    })
    before = _row(pg_engine, actor_id)
    assert before.provenance == PROVENANCE_OBSERVED, "fixture check: first call must have earned 'observed'"

    # A real re-visit with nothing new to report -- the exact shape
    # intelligence/spider/discovery.py produces when a discovered connection
    # carries no evidence.
    save_actor(pg_engine, actor_id, {
        "name": "Confirmed Actor", "tier": "test", "category": "test",
        "data_sources": [],
    })

    after = _row(pg_engine, actor_id)
    assert after.provenance == PROVENANCE_OBSERVED, "a maintenance-only call must never downgrade confirmed provenance"
    assert after.influence_score == before.influence_score, "no evidence in means no evidence-bearing field changes"
    assert after.data_sources == before.data_sources
    assert after.updated_at > before.updated_at, (
        "the call is not a no-op -- it is a liveness touch with zero evidentiary weight, "
        "the same shape every other maintenance writer in this codebase produces"
    )


def test_maintenance_only_call_does_not_promote_a_seed_row_to_observed(
    pg_engine: Engine, test_ids: list[str],
):
    """Generalizes the previous case beyond 'observed' specifically: whatever
    classified state a row is already in, a no-evidence save_actor call must
    preserve it, not just avoid downgrading a confirmed observation."""
    actor_id = f"save_actor_gate_pg_{uuid.uuid4().hex[:16]}"
    test_ids.append(actor_id)

    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO actors (id, name, tier, category, provenance, influence_score) "
            "VALUES (:id, 'Seed Actor', 'test', 'test', :seed, 0.5)"
        ).bindparams(id=actor_id, seed=PROVENANCE_SEED))

    save_actor(pg_engine, actor_id, {
        "name": "Seed Actor", "tier": "test", "category": "test",
    })

    row = _row(pg_engine, actor_id)
    assert row.provenance == PROVENANCE_SEED, "a no-evidence touch must not promote a seed row to 'observed'"


def test_legitimate_observation_with_zero_score_is_classified_observed_and_stored_verbatim(
    pg_engine: Engine, test_ids: list[str],
):
    actor_id = f"save_actor_gate_pg_{uuid.uuid4().hex[:16]}"
    test_ids.append(actor_id)

    save_actor(pg_engine, actor_id, {
        "name": "Zero Score Actor", "tier": "test", "category": "test",
        "influence_score": 0.0, "data_sources": ["sec_form4"],
    })

    row = _row(pg_engine, actor_id)
    assert row.provenance == PROVENANCE_OBSERVED, (
        "the evidence gate is data_sources, not influence_score -- a real but falsy "
        "score must not be mistaken for 'no evidence'"
    )
    assert row.influence_score == 0.0, "a legitimate zero must be stored verbatim, not replaced by the 0.3 default"
