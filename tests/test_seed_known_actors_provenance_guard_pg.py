"""Real-PostgreSQL proof for intelligence/actors/db.py::_seed_known_actors's
provenance-aware ON CONFLICT guard -- part of the #596 remediation reconciliation
(00-Agent-Reports/2026-09-22/claude__ANIK__grid-596-remediation-plan.md).

Proves, against the real function (not a re-implementation):
  1. A brand-new row is inserted with provenance='seed' and updated_at=SEED_VINTAGE_TS
     (not NOW() -- the original audit A-H13 bug this design fixes).
  2. A row still exactly 'seed' DOES get refreshed by a reseed call.
  3. A row that has become 'observed' is left COMPLETELY untouched by a reseed --
     not just its provenance columns, but every other seed-authored field too
     (influence_score specifically, since that is the field the module docstring
     names as the concrete harm of overwriting a genuinely-observed value).
  4. Same for 'unconfirmed'.

Uses the shared ``pg_engine`` fixture (tests/conftest.py) -- skips cleanly if no
PostgreSQL is reachable. Requires the actors_provenance_columns_0922 columns, which
this file adds directly (schema-only, matching that migration) rather than depending on
alembic having been run against the test database.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.actors.db import _seed_known_actors
from intelligence.actors.provenance import PROVENANCE_SEED, SEED_VINTAGE_TS

_MINIMAL_ACTORS_DDL = """
CREATE TABLE IF NOT EXISTS actors (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    tier TEXT NOT NULL,
    category TEXT NOT NULL,
    title TEXT,
    net_worth_estimate DOUBLE PRECISION,
    aum DOUBLE PRECISION,
    influence_score DOUBLE PRECISION,
    trust_score DOUBLE PRECISION,
    motivation_model TEXT,
    data_sources JSONB,
    credibility TEXT,
    provenance TEXT NOT NULL DEFAULT 'observed',
    provenance_as_of DATE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""


@pytest.fixture(autouse=True)
def _actors_table(pg_engine: Engine):
    with pg_engine.begin() as conn:
        conn.execute(text(_MINIMAL_ACTORS_DDL))
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


def _seed_one(monkeypatch, actor_id: str, influence_score: float = 0.5) -> None:
    import intelligence.actors.seed_data as seed_data
    monkeypatch.setattr(seed_data, "_KNOWN_ACTORS", {
        actor_id: {
            "name": "Test Actor", "tier": "test", "category": "test", "title": "Tester",
            "influence_score": influence_score, "data_sources": ["seed"],
        },
    })
    import intelligence.actors.db as db_module
    monkeypatch.setattr(db_module, "_KNOWN_ACTORS", seed_data._KNOWN_ACTORS)


def test_fresh_row_is_seeded_as_seed_with_vintage_timestamp(pg_engine: Engine, test_ids, monkeypatch):
    actor_id = f"seed_guard_pg_{uuid.uuid4().hex[:16]}"
    test_ids.append(actor_id)
    _seed_one(monkeypatch, actor_id)

    _seed_known_actors(pg_engine)

    with pg_engine.connect() as conn:
        row = conn.execute(text(
            "SELECT provenance, updated_at, influence_score FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert row.provenance == PROVENANCE_SEED
    assert row.updated_at == SEED_VINTAGE_TS, "must stamp the fixed seed vintage, never NOW() (audit A-H13)"
    assert row.influence_score == 0.5


def test_reseed_refreshes_a_row_still_exactly_seed(pg_engine: Engine, test_ids, monkeypatch):
    actor_id = f"seed_guard_pg_{uuid.uuid4().hex[:16]}"
    test_ids.append(actor_id)
    _seed_one(monkeypatch, actor_id, influence_score=0.5)
    _seed_known_actors(pg_engine)

    _seed_one(monkeypatch, actor_id, influence_score=0.9)  # seed data "updated"
    _seed_known_actors(pg_engine)

    with pg_engine.connect() as conn:
        row = conn.execute(text(
            "SELECT provenance, influence_score FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert row.provenance == PROVENANCE_SEED
    assert row.influence_score == 0.9, "a row still exactly 'seed' must refresh from the seed table"


def test_reseed_does_not_touch_an_observed_row_at_all(pg_engine: Engine, test_ids, monkeypatch):
    actor_id = f"seed_guard_pg_{uuid.uuid4().hex[:16]}"
    test_ids.append(actor_id)
    _seed_one(monkeypatch, actor_id, influence_score=0.5)
    _seed_known_actors(pg_engine)

    # Simulate save_actor's contract confirming a real observation.
    with pg_engine.begin() as conn:
        conn.execute(text(
            "UPDATE actors SET provenance = 'observed', influence_score = 0.77, "
            "updated_at = NOW() WHERE id = :id"
        ).bindparams(id=actor_id))

    _seed_one(monkeypatch, actor_id, influence_score=0.5)  # reseed tries the OLD hand-typed value
    _seed_known_actors(pg_engine)

    with pg_engine.connect() as conn:
        row = conn.execute(text(
            "SELECT provenance, influence_score FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert row.provenance == "observed", "reseed must not relabel an observed row"
    assert row.influence_score == 0.77, (
        "reseed must not overwrite influence_score (or any other seed-authored field) "
        "on an observed row -- the ON CONFLICT ... WHERE guard must suppress the WHOLE "
        "update, not just the provenance columns"
    )


def test_reseed_does_not_touch_an_unconfirmed_row_at_all(pg_engine: Engine, test_ids, monkeypatch):
    actor_id = f"seed_guard_pg_{uuid.uuid4().hex[:16]}"
    test_ids.append(actor_id)
    _seed_one(monkeypatch, actor_id, influence_score=0.5)
    _seed_known_actors(pg_engine)

    # Simulate a maintenance touch (e.g. an alias-fold) that stamps 'unconfirmed'.
    with pg_engine.begin() as conn:
        conn.execute(text(
            "UPDATE actors SET provenance = 'unconfirmed', influence_score = 0.61, "
            "updated_at = NOW() WHERE id = :id"
        ).bindparams(id=actor_id))

    _seed_one(monkeypatch, actor_id, influence_score=0.5)
    _seed_known_actors(pg_engine)

    with pg_engine.connect() as conn:
        row = conn.execute(text(
            "SELECT provenance, influence_score FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert row.provenance == "unconfirmed", "reseed must not relabel an unconfirmed row back to 'seed'"
    assert row.influence_score == 0.61, "reseed must not overwrite an unconfirmed row's fields either"
