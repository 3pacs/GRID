"""Real-PostgreSQL proof: _seed_known_actors must not clobber an actor row
that has been enriched by a real observed writer since it was seeded.

intelligence/actors/db.py::_seed_known_actors's ON CONFLICT clause used to
unconditionally reset every seed-authored field, including
provenance/provenance_as_of/updated_at, to the seed values on every rerun.
That branch is not reachable through build_actor_graph's own gate today (it
only calls the seeder when the actors query comes back empty, and any
existing row -- seeded or observed -- makes that query non-empty), but the
function is public and callable directly, and nothing should rely on the
caller's gate to keep it honest. Fixed with a single ``WHERE
actors.provenance = 'seed'`` guard on the whole ``ON CONFLICT DO UPDATE`` --
not per-column CASE guards -- so an already-observed row is left completely
untouched, not just relabeled: overwriting influence_score with the
hand-typed seed figure while the row's provenance still read 'observed'
would have made that label a lie for the overwritten field.

This file proves two things against a real database: (1) an enriched row's
entire state, not just its provenance columns, survives a reseed, with a
pristine row still correctly promoted as a control case; (2) the same
enriched-then-reseeded row is read correctly through the real router
(api.routers.intelligence_actors.get_ego_graph), proving the seeder fix and
the router's switch from seed-list membership to the stored provenance
column work together end to end, not just in isolation.

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
    with pg_engine.begin() as conn:
        # Base shape, matching what a sibling test file's minimal DDL
        # already establishes -- a no-op if `actors` already exists in any
        # shape, minimal or full. Columns must land *before* the real
        # _seed_known_actors() call (below, in each test) runs its own
        # internal _ensure_tables(), whose CREATE INDEX statements need
        # influence_score/tier to already exist -- they fail outright
        # against a table missing those columns, so this fixture cannot
        # rely on that call to add them itself.
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS actors ("
            "id TEXT PRIMARY KEY, name TEXT NOT NULL, "
            "tier TEXT NOT NULL, category TEXT NOT NULL)",
        ))
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
    # fresh updated_at and a real influence_score reading distinct from the
    # seed's hand-typed figure -- exactly the state a later seed rerun must
    # not discard, in whole or in part.
    enriched_updated_at = datetime.now(timezone.utc)
    enriched_influence = 0.87
    seed_influence = _seed_data_for(test_id)[test_id]["influence_score"]
    assert enriched_influence != seed_influence, "test premise: values must differ"
    enriched_name = "Enriched Canonical Name"
    with pg_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE actors SET provenance = :observed, provenance_as_of = NULL, "
                "updated_at = :updated_at, influence_score = :inf, name = :name "
                "WHERE id = :id",
            ).bindparams(
                observed=PROVENANCE_OBSERVED, updated_at=enriched_updated_at,
                inf=enriched_influence, name=enriched_name, id=test_id,
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
    # The fix guards the WHOLE update, not just the provenance columns: an
    # already-observed row must not have ANY seed-authored field silently
    # overwritten while its label still claims 'observed' -- that would make
    # the label a lie for the overwritten field. influence_score (a real
    # measurement-shaped field, distinct from the identity fields below)
    # must survive exactly like provenance did.
    assert float(after["influence_score"]) == pytest.approx(enriched_influence), (
        "influence_score must survive the reseed once the row is 'observed' -- "
        "overwriting it with the seed's hand-typed figure would make the "
        "'observed' label false for this field"
    )
    # Identity fields the seed table would otherwise refresh are also left
    # alone once the row is 'observed' -- a real overwrite-vs-preserve test,
    # not tautological: the enriched name differs from the seed's own name,
    # so a reseed clobbering it would be caught here.
    assert after["name"] == enriched_name, (
        "name must also survive the reseed -- the WHERE guard suppresses "
        "the whole update, not just the provenance columns"
    )


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


def test_enriched_row_survives_reseeding_and_reports_correctly_through_the_router(
    pg_engine: Engine, test_id: str, monkeypatch,
):
    """End to end: seed -> enrich -> reseed -> read through the real router.

    Ties the seeder's ON CONFLICT ... WHERE fix together with the router's
    switch from seed-list membership to the stored provenance column.
    ``test_id`` is deliberately added to the membership set too (on top of
    being patched into ``_KNOWN_ACTORS``), so a regression to
    membership-only fallback would be caught here rather than masked by the
    id never appearing on the real seed list.
    """
    import intelligence.actors.db as actors_db
    import intelligence.actors.provenance as provenance_module

    monkeypatch.setattr(actors_db, "_KNOWN_ACTORS", _seed_data_for(test_id))
    monkeypatch.setattr(
        provenance_module, "SEED_ACTOR_IDS",
        provenance_module.SEED_ACTOR_IDS | {test_id},
    )

    _seed_known_actors(pg_engine)

    enriched_influence = 0.91
    enriched_updated_at = datetime.now(timezone.utc)
    with pg_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE actors SET provenance = :observed, provenance_as_of = NULL, "
                "updated_at = :updated_at, influence_score = :inf "
                "WHERE id = :id",
            ).bindparams(
                observed=PROVENANCE_OBSERVED, updated_at=enriched_updated_at,
                inf=enriched_influence, id=test_id,
            ),
        )

    # Reseed -- exercises the ON CONFLICT ... WHERE suppression again.
    _seed_known_actors(pg_engine)

    with pg_engine.connect() as conn:
        row = dict(conn.execute(
            text(
                "SELECT influence_score, provenance FROM actors WHERE id = :id",
            ).bindparams(id=test_id),
        ).fetchone()._mapping)
    assert row["provenance"] == PROVENANCE_OBSERVED
    assert float(row["influence_score"]) == pytest.approx(enriched_influence), (
        "enrichment must survive the reseed before it ever reaches the router"
    )

    # Now read it through the REAL router (not a fake connection) -- proves
    # the stored column, not seed-list membership, drives the wire label.
    from api.routers import intelligence_actors as ia

    monkeypatch.setattr(ia, "get_db_engine", lambda: pg_engine)
    result = ia.get_ego_graph(test_id, depth=0, max_nodes=5, _token="t")
    node = next(n for n in result["nodes"] if n["id"] == test_id)

    assert test_id in provenance_module.SEED_ACTOR_IDS, (
        "test premise: this id is (deliberately) on the seed-membership set"
    )
    assert node["source"] == "observed", (
        "the router must report the stored provenance, not fall back to "
        "seed-list membership just because this id is on that list"
    )
    assert node["source_as_of"] is None
