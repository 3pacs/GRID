"""Real-PostgreSQL proof: _seed_known_actors must not clobber an actor row
that has been enriched by a real observed writer since it was seeded.

intelligence/actors/db.py::_seed_known_actors's ON CONFLICT clause used to
unconditionally reset provenance/provenance_as_of/updated_at to the seed
values on every rerun. That branch is not reachable through
build_actor_graph's own gate today (it only calls the seeder when the actors
query comes back empty, and any existing row -- seeded or observed -- makes
that query non-empty), but the function is public and callable directly, and
nothing should rely on the caller's gate to keep it honest. This test calls
the real function against a real database: seed an actor, simulate a real
writer confirming/updating it (provenance -> 'observed'), rerun the seeder
for the same id, and confirm the stronger provenance survives.

Uses the shared pg_engine fixture (tests/conftest.py) -- skips cleanly if no
PostgreSQL is reachable.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.actors.db import _seed_known_actors
from intelligence.actors.provenance import PROVENANCE_OBSERVED, PROVENANCE_SEED


@pytest.fixture
def test_id() -> str:
    return f"pkt2seedconflict_pg_{uuid.uuid4().hex[:16]}"


# _seed_known_actors calls the real _ensure_tables(), whose CREATE TABLE IF
# NOT EXISTS is a no-op if another test file already created `actors` with a
# stripped-down shape (e.g. test_actors_provenance_migration_pg.py's minimal
# DDL, for testing the migration file in isolation from application code).
# Guarantee the full production shape defensively, regardless of which test
# file's fixtures happened to run first against this shared disposable
# database -- every statement here is idempotent.
@pytest.fixture(autouse=True)
def _full_actors_shape(pg_engine: Engine):
    from intelligence.actors.db import _ensure_tables

    _ensure_tables(pg_engine)  # CREATE TABLE IF NOT EXISTS -- no-op if present

    with pg_engine.begin() as conn:
        for column_ddl in (
            "title TEXT",
            "net_worth_estimate NUMERIC",
            "aum NUMERIC",
            "influence_score NUMERIC DEFAULT 0.5",
            "trust_score NUMERIC DEFAULT 0.5",
            "motivation_model TEXT DEFAULT 'unknown'",
            "connections JSONB DEFAULT '[]'",
            "known_positions JSONB DEFAULT '[]'",
            "board_seats JSONB DEFAULT '[]'",
            "political_affiliations JSONB DEFAULT '[]'",
            "data_sources JSONB DEFAULT '[]'",
            "credibility TEXT DEFAULT 'inferred'",
            "metadata JSONB DEFAULT '{}'",
            "provenance TEXT NOT NULL DEFAULT 'observed'",
            "provenance_as_of DATE",
            "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
        ):
            conn.execute(text(f"ALTER TABLE actors ADD COLUMN IF NOT EXISTS {column_ddl}"))
    yield


@pytest.fixture(autouse=True)
def cleanup(pg_engine: Engine, test_id: str):
    yield
    with pg_engine.begin() as conn:
        conn.execute(
            text("DELETE FROM actors WHERE id = :id").bindparams(id=test_id),
        )


def _seed_data_for(test_id: str) -> dict:
    return {
        test_id: {
            "name": "Enriched Then Reseeded Test Actor",
            "tier": "individual",
            "category": "insider",
            "title": "Test Title",
            "net_worth_estimate": 1_000_000,
            "aum": None,
            "influence_score": 0.5,
            "trust_score": 0.5,
            "motivation_model": "unknown",
            "data_sources": ["seed_data"],
            "credibility": "curated_estimate",
        },
    }


def test_seed_rerun_preserves_an_already_observed_row(
    pg_engine: Engine, test_id: str, monkeypatch,
):
    # _seed_known_actors iterates the name bound in db.py's own namespace
    # (a `from ... import _KNOWN_ACTORS`), not seed_data's -- patch that one.
    import intelligence.actors.db as actors_db

    monkeypatch.setattr(actors_db, "_KNOWN_ACTORS", _seed_data_for(test_id))

    # First seed: the row starts out 'seed', as expected.
    _seed_known_actors(pg_engine)
    with pg_engine.connect() as conn:
        row = dict(conn.execute(
            text(
                "SELECT provenance, provenance_as_of, updated_at, influence_score "
                "FROM actors WHERE id = :id",
            ).bindparams(id=test_id),
        ).fetchone()._mapping)
    assert row["provenance"] == PROVENANCE_SEED

    # Simulate a real writer (save_actor / a Form4-13F puller) confirming
    # this row since it was seeded: provenance flips to 'observed', with a
    # fresh updated_at and a real influence_score bump -- exactly the kind
    # of enrichment that must not be discarded by a later seed rerun.
    enriched_updated_at = datetime.now(timezone.utc)
    with pg_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE actors SET provenance = :observed, provenance_as_of = NULL, "
                "updated_at = :updated_at, influence_score = 0.87 "
                "WHERE id = :id",
            ).bindparams(
                observed=PROVENANCE_OBSERVED, updated_at=enriched_updated_at, id=test_id,
            ),
        )

    # Seed rerun for the same id -- the ON CONFLICT branch.
    _seed_known_actors(pg_engine)

    with pg_engine.connect() as conn:
        after = dict(conn.execute(
            text(
                "SELECT provenance, provenance_as_of, updated_at, influence_score, name "
                "FROM actors WHERE id = :id",
            ).bindparams(id=test_id),
        ).fetchone()._mapping)

    assert after["provenance"] == PROVENANCE_OBSERVED, (
        "a seed rerun must not relabel an already-observed row 'seed'"
    )
    assert after["provenance_as_of"] is None
    assert after["updated_at"] == enriched_updated_at, (
        "updated_at must not be reset to the seed vintage once a row is observed"
    )
    assert after["influence_score"] == pytest.approx(0.87), (
        "the observed influence_score must survive the reseed, not be "
        "overwritten by the hand-typed seed value"
    )
    # Non-provenance identity fields still refresh from the seed table, as
    # before this fix -- only provenance/provenance_as_of/updated_at changed.
    assert after["name"] == "Enriched Then Reseeded Test Actor"


def test_seed_rerun_still_promotes_a_pristine_row(
    pg_engine: Engine, test_id: str, monkeypatch,
):
    """Control case: a row nothing has touched since seeding must still get
    (re)promoted to 'seed' on a rerun -- the fix must not make the seeder
    inert for the ordinary, expected case."""
    import intelligence.actors.db as actors_db

    monkeypatch.setattr(actors_db, "_KNOWN_ACTORS", _seed_data_for(test_id))

    _seed_known_actors(pg_engine)
    _seed_known_actors(pg_engine)  # rerun, nothing touched it in between

    with pg_engine.connect() as conn:
        row = dict(conn.execute(
            text(
                "SELECT provenance, provenance_as_of FROM actors WHERE id = :id",
            ).bindparams(id=test_id),
        ).fetchone()._mapping)

    assert row["provenance"] == PROVENANCE_SEED
    assert row["provenance_as_of"] is not None
