"""Real-PostgreSQL proof for intelligence/actors/db.py::_seed_known_actors's
provenance-aware ON CONFLICT guard -- part of the #596 remediation reconciliation
(00-Agent-Reports/2026-09-22/claude__ANIK__grid-596-remediation-plan.md).

Proves, against the real function (not a re-implementation):
  1. A brand-new row is inserted with provenance='seed' and updated_at=SEED_VINTAGE_TS
     (not NOW() -- the original audit A-H13 bug this design fixes).
  2. A row still exactly 'seed' DOES get refreshed by a reseed call.
  3. A row still at the column default 'unknown', with no recorded data_sources
     (never classified OR touched by anything) DOES get seeded/refreshed --
     legitimate seeding under the revised honest default must keep working exactly
     as if the row didn't exist yet.
  4. A row that has become 'observed' is left COMPLETELY untouched by a reseed --
     not just its provenance columns, but every other seed-authored field too
     (influence_score specifically, since that is the field the module docstring
     names as the concrete harm of overwriting a genuinely-observed value).
  5. Same for 'unconfirmed'.
  6. 'unknown' means unverified, not disposable: a row still 'unknown' but ENRICHED
     with real data_sources by some other writer (actor_discovery.py's own shape) is
     left completely untouched by a reseed too -- the guard must check more than the
     provenance label alone, or it would silently clobber real, unclassified
     information the moment its id happens to collide with the curated seed list.
  7. By contrast, a row still 'unknown' that was merely TOUCHED (updated_at moved,
     no data_sources recorded) remains eligible for reseeding -- a bare timestamp
     move carries no evidentiary content, so it is not itself grounds to withhold
     seeding either. This draws the precise boundary case 6 sits on the other side
     of.
  8. The data_sources check alone is not enough: a row with EMPTY data_sources but
     real identity/enrichment content in title/net_worth_estimate/aum -- the exact
     shape actor_discovery.py's own upsert (unconditionally overwrites name/title
     regardless of whether data_sources was passed) and scripts/seed_vip_network.py
     (writes title/net_worth_estimate directly, never touches data_sources) both
     produce -- must also survive a reseed untouched.

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
from intelligence.actors.provenance import PROVENANCE_SEED, PROVENANCE_UNKNOWN, SEED_VINTAGE_TS

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
    provenance TEXT NOT NULL DEFAULT 'unknown',
    provenance_as_of DATE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""


@pytest.fixture(autouse=True)
def _actors_table(pg_engine: Engine):
    with pg_engine.begin() as conn:
        conn.execute(text(_MINIMAL_ACTORS_DDL))
        # A table left behind by another test file's narrower DDL (this suite
        # shares one disposable database across files within a run) still
        # needs every column this file depends on added -- CREATE TABLE IF
        # NOT EXISTS alone is a no-op once any file has created the table
        # first, regardless of which columns that first DDL included.
        for stmt in (
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS title TEXT",
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS net_worth_estimate DOUBLE PRECISION",
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS aum DOUBLE PRECISION",
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS influence_score DOUBLE PRECISION",
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS trust_score DOUBLE PRECISION",
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS motivation_model TEXT",
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS data_sources JSONB",
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS credibility TEXT",
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS provenance TEXT NOT NULL DEFAULT 'unknown'",
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS provenance_as_of DATE",
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


def test_seeding_a_row_still_at_the_unknown_default_succeeds(pg_engine: Engine, test_ids, monkeypatch):
    """Legitimate seeding under the revised honest default. A row can be at 'unknown'
    without ever having gone through _seed_known_actors before -- e.g. created by the
    schema migration's own default, or by some other minimal writer -- and the seeder
    must still be able to claim it, exactly as if the row did not exist yet."""
    actor_id = f"seed_guard_pg_{uuid.uuid4().hex[:16]}"
    test_ids.append(actor_id)

    # Row pre-exists at the column default 'unknown' -- NOT via _seed_known_actors.
    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO actors (id, name, tier, category) VALUES (:id, 'Pre-existing', 'test', 'test')"
        ).bindparams(id=actor_id))
        row = conn.execute(text(
            "SELECT provenance FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert row.provenance == PROVENANCE_UNKNOWN, "fixture check: row must start at the real column default"

    _seed_one(monkeypatch, actor_id, influence_score=0.42)
    _seed_known_actors(pg_engine)

    with pg_engine.connect() as conn:
        after = conn.execute(text(
            "SELECT provenance, updated_at, influence_score FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert after.provenance == PROVENANCE_SEED
    assert after.updated_at == SEED_VINTAGE_TS
    assert after.influence_score == 0.42


def test_reseed_does_not_overwrite_an_unknown_row_enriched_with_real_data_sources(
    pg_engine: Engine, test_ids, monkeypatch,
):
    """'unknown' means unverified, not disposable. A row can sit at the column
    default 'unknown' forever while still carrying real, non-trivial information --
    several writers besides save_actor/_seed_known_actors insert or update actors
    rows directly and never touch provenance at all (actor_discovery.py's own
    INSERT shape is reproduced here: real name/influence_score/data_sources, no
    provenance stamp). Reseeding must not treat that row as equivalent to one that
    doesn't exist yet merely because its label still reads 'unknown'."""
    actor_id = f"seed_guard_pg_{uuid.uuid4().hex[:16]}"
    test_ids.append(actor_id)

    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO actors (id, name, tier, category, influence_score, data_sources) "
            "VALUES (:id, 'Independently Discovered', 'institutional', 'corporation', "
            "0.58, :sources)"
        ).bindparams(id=actor_id, sources='["actor_discovery"]'))
        before = conn.execute(text(
            "SELECT provenance, data_sources::text AS data_sources FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert before.provenance == PROVENANCE_UNKNOWN, "fixture check: still the column default"
    assert before.data_sources == '["actor_discovery"]', "fixture check: real evidence is recorded"

    _seed_one(monkeypatch, actor_id, influence_score=0.5)
    _seed_known_actors(pg_engine)

    with pg_engine.connect() as conn:
        after = conn.execute(text(
            "SELECT provenance, name, influence_score, data_sources::text AS data_sources "
            "FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert after.provenance == PROVENANCE_UNKNOWN, "an enriched unknown row must not be promoted to 'seed'"
    assert after.name == "Independently Discovered", "reseed must not overwrite the enriched name"
    assert after.influence_score == 0.58, "reseed must not overwrite the enriched influence_score"
    assert after.data_sources == '["actor_discovery"]', "reseed must not overwrite the enriched data_sources"


def test_reseed_still_claims_an_unknown_row_that_was_merely_touched_with_no_real_data(
    pg_engine: Engine, test_ids, monkeypatch,
):
    """The other side of the boundary the previous test draws: a row still
    'unknown' with NO recorded data_sources remains fair game to seed even after a
    bare updated_at touch (e.g. a maintenance-only save_actor call, or any of the
    other writers that only ever move updated_at). A timestamp move alone carries
    no evidentiary content in this design -- neither earning a promotion nor
    earning protection from one."""
    actor_id = f"seed_guard_pg_{uuid.uuid4().hex[:16]}"
    test_ids.append(actor_id)

    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO actors (id, name, tier, category) VALUES (:id, 'Barely Touched', 'test', 'test')"
        ).bindparams(id=actor_id))
    with pg_engine.begin() as conn:
        # The maintenance writer's exact shape: updated_at only, nothing else.
        conn.execute(text("UPDATE actors SET updated_at = NOW() WHERE id = :id").bindparams(id=actor_id))
        before = conn.execute(text(
            "SELECT provenance, data_sources FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert before.provenance == PROVENANCE_UNKNOWN
    assert before.data_sources is None, "fixture check: no real content was ever recorded"

    _seed_one(monkeypatch, actor_id, influence_score=0.33)
    _seed_known_actors(pg_engine)

    with pg_engine.connect() as conn:
        after = conn.execute(text(
            "SELECT provenance, influence_score FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert after.provenance == PROVENANCE_SEED, "a merely-touched unknown row (no real data) must still be claimable"
    assert after.influence_score == 0.33


def test_reseed_does_not_overwrite_an_unknown_row_enriched_via_title_or_financials_with_empty_data_sources(
    pg_engine: Engine, test_ids, monkeypatch,
):
    """The data_sources check alone is not sufficient. A row can carry real,
    non-trivial content in title/net_worth_estimate/aum while data_sources stays
    empty -- the exact shape two real writers produce: actor_discovery.py's own
    upsert function unconditionally overwrites name/title on every conflict
    regardless of whether its caller passed data_sources (a None default on that
    function), and scripts/seed_vip_network.py writes title/net_worth_estimate
    directly and never touches data_sources at all. Reseeding must preserve this
    enrichment, not just data_sources-flavored enrichment."""
    actor_id = f"seed_guard_pg_{uuid.uuid4().hex[:16]}"
    test_ids.append(actor_id)

    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO actors (id, name, tier, category, title, net_worth_estimate, aum) "
            "VALUES (:id, 'Enriched Via Title', 'institutional', 'corporation', "
            "'Chairman', 2500000000, 900000000)"
        ).bindparams(id=actor_id))
        before = conn.execute(text(
            "SELECT provenance, data_sources FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert before.provenance == PROVENANCE_UNKNOWN, "fixture check: still the column default"
    assert before.data_sources is None, "fixture check: data_sources alone gives no signal here"

    _seed_one(monkeypatch, actor_id, influence_score=0.5)
    _seed_known_actors(pg_engine)

    with pg_engine.connect() as conn:
        after = conn.execute(text(
            "SELECT provenance, name, title, net_worth_estimate, aum FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert after.provenance == PROVENANCE_UNKNOWN, "must not be promoted to 'seed' merely because data_sources was empty"
    assert after.name == "Enriched Via Title", "reseed must not overwrite the real name"
    assert after.title == "Chairman", "reseed must not overwrite the real title"
    assert after.net_worth_estimate == 2500000000, "reseed must not overwrite the real net_worth_estimate"
    assert after.aum == 900000000, "reseed must not overwrite the real aum"


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
