"""Real-PostgreSQL proof for intelligence/actors/db.py::_seed_known_actors's
provenance-aware ON CONFLICT guard -- part of the #596 remediation reconciliation
(00-Agent-Reports/2026-09-22/claude__ANIK__grid-596-remediation-plan.md).

Proves, against the real function (not a re-implementation):
  1. A brand-new row is inserted with provenance='seed' and updated_at=SEED_VINTAGE_TS
     (not NOW() -- the original audit A-H13 bug this design fixes). This is the only
     path by which this function ever produces 'seed' from a NEW id -- there is no
     legacy history to protect for a row that didn't exist a moment ago.
  2. A row still exactly 'seed' DOES get refreshed by a reseed call.
  3. An EXISTING row still at the column default 'unknown' is left COMPLETELY
     untouched by a reseed call, unconditionally -- not promoted to 'seed', no field
     changed, even when the row looks pristine (no data_sources, no title, no
     financials) and would have qualified as "fair game" under an earlier design of
     this guard. Classifying a legacy 'unknown' row is the separately authorized
     backfill script's job alone, using its own updated_at-vs-SEED_VINTAGE_TS
     evidence -- this function no longer makes that call by any means, including a
     "looks untouched" guess.
  4. A row that has become 'observed' is left COMPLETELY untouched by a reseed --
     not just its provenance columns, but every other seed-authored field too
     (influence_score specifically, since that is the field the module docstring
     names as the concrete harm of overwriting a genuinely-observed value).
  5. Same for 'unconfirmed'.
  6. An 'unknown' row ENRICHED with real data_sources by some other writer
     (actor_discovery.py's own shape) is untouched too -- a specific case of rule 3,
     kept as a concrete example.
  7. An 'unknown' row that was merely TOUCHED (updated_at moved, no data_sources
     recorded) is untouched as well -- rule 3 applies uniformly regardless of
     whether the row looks pristine or touched; there is no longer a boundary
     between the two to draw.
  8. An 'unknown' row enriched via title/net_worth_estimate/aum with EMPTY
     data_sources is untouched -- another concrete case of rule 3.
  9. The decisive proof that rule 3 is now a genuinely uniform, single-column check
     and not another column-enumerating guard: an 'unknown' row with EMPTY
     data_sources/title/net_worth_estimate/aum (i.e. "pristine" under every earlier
     draft's checklist) but a real, changed name AND influence_score is still left
     completely untouched -- name/score are not, and were never meant to be, part of
     any pristine checklist; the guard no longer has a checklist at all.

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


def test_reseed_does_not_touch_an_existing_unknown_row_even_when_pristine(
    pg_engine: Engine, test_ids, monkeypatch,
):
    """A row can be at 'unknown' without ever having gone through
    _seed_known_actors before -- e.g. created by the schema migration's own
    default, or by some other minimal writer. Even when that row looks
    completely pristine (no data_sources, no title, no financials -- exactly the
    shape an earlier design of this guard treated as "fair game"), an EXISTING
    row is never this function's to reclassify. Only a genuinely NEW id (no row
    at all) gets inserted as 'seed' -- see
    test_fresh_row_is_seeded_as_seed_with_vintage_timestamp for that path.
    Classifying this row is the separately authorized backfill script's job."""
    actor_id = f"seed_guard_pg_{uuid.uuid4().hex[:16]}"
    test_ids.append(actor_id)

    # Row pre-exists at the column default 'unknown' -- NOT via _seed_known_actors.
    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO actors (id, name, tier, category) VALUES (:id, 'Pre-existing', 'test', 'test')"
        ).bindparams(id=actor_id))
        before = conn.execute(text(
            "SELECT provenance, updated_at, influence_score FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert before.provenance == PROVENANCE_UNKNOWN, "fixture check: row must start at the real column default"

    _seed_one(monkeypatch, actor_id, influence_score=0.42)
    _seed_known_actors(pg_engine)

    with pg_engine.connect() as conn:
        after = conn.execute(text(
            "SELECT provenance, name, updated_at, influence_score FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert after.provenance == PROVENANCE_UNKNOWN, "an existing unknown row must never be promoted to 'seed' by this function"
    assert after.name == "Pre-existing", "reseed must not overwrite the existing name"
    assert after.updated_at == before.updated_at, "reseed must not touch updated_at either -- nothing about this row changes"
    assert after.influence_score is None, "reseed must not write the seed table's influence_score onto this row"


def test_reseed_does_not_overwrite_an_unknown_row_enriched_with_real_data_sources(
    pg_engine: Engine, test_ids, monkeypatch,
):
    """One concrete case of the uniform rule: an EXISTING 'unknown' row is never
    this function's to reclassify, regardless of what it carries. Reproduces
    actor_discovery.py's own INSERT shape (real name/influence_score/data_sources,
    no provenance stamp) to prove reseeding doesn't touch it, same as it wouldn't
    touch a pristine 'unknown' row either."""
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


def test_reseed_does_not_touch_an_unknown_row_that_was_merely_touched_with_no_real_data(
    pg_engine: Engine, test_ids, monkeypatch,
):
    """A row still 'unknown' with NO recorded data_sources, touched only by a bare
    updated_at move (e.g. a maintenance-only save_actor call, or any of the other
    writers that only ever move updated_at), is left alone by a reseed -- same as
    every other existing 'unknown' row, touched or not. There is no boundary left
    to draw here: rule 3 in the module docstring above applies uniformly."""
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
            "SELECT provenance, updated_at, data_sources FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert before.provenance == PROVENANCE_UNKNOWN
    assert before.data_sources is None, "fixture check: no real content was ever recorded"

    _seed_one(monkeypatch, actor_id, influence_score=0.33)
    _seed_known_actors(pg_engine)

    with pg_engine.connect() as conn:
        after = conn.execute(text(
            "SELECT provenance, updated_at, influence_score FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert after.provenance == PROVENANCE_UNKNOWN, "a merely-touched unknown row must not be claimed by a reseed either"
    assert after.updated_at == before.updated_at, "reseed must not touch updated_at"
    assert after.influence_score is None, "reseed must not write the seed table's influence_score onto this row"


def test_reseed_does_not_overwrite_an_unknown_row_enriched_via_title_or_financials_with_empty_data_sources(
    pg_engine: Engine, test_ids, monkeypatch,
):
    """Another concrete case of the uniform rule, reproducing two real writers'
    shapes: actor_discovery.py's own upsert function unconditionally overwrites
    name/title on every conflict regardless of whether its caller passed
    data_sources (a None default on that function), and
    scripts/seed_vip_network.py writes title/net_worth_estimate directly and never
    touches data_sources at all -- both leave a row with empty data_sources but
    real title/financial content. Reseeding preserves it, same as it would any
    other existing 'unknown' row."""
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


def test_reseed_does_not_touch_an_unknown_row_with_empty_sources_but_a_changed_name_or_score(
    pg_engine: Engine, test_ids, monkeypatch,
):
    """The decisive proof that the guard is now a uniform, single-column check and
    not another entry in a column checklist. Every earlier draft of this guard --
    data_sources alone, then data_sources+title+net_worth_estimate+aum -- would have
    treated THIS row as "pristine" and reseeded it, because none of those drafts
    ever checked name or influence_score. A row with empty data_sources AND empty
    title/net_worth_estimate/aum, but a real, analyst-assigned name and score that
    differ from the curated seed table's values, must still be left completely
    untouched -- proving there is no longer any checklist to defeat by touching an
    uncovered column, because EXISTING 'unknown' rows are never reclassified by
    this function at all, regardless of which fields they carry."""
    actor_id = f"seed_guard_pg_{uuid.uuid4().hex[:16]}"
    test_ids.append(actor_id)

    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO actors (id, name, tier, category, influence_score) "
            "VALUES (:id, 'Real Analyst-Assigned Name', 'test', 'test', 0.71)"
        ).bindparams(id=actor_id))
        before = conn.execute(text(
            "SELECT provenance, name, influence_score, updated_at, data_sources, title, "
            "net_worth_estimate, aum FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert before.provenance == PROVENANCE_UNKNOWN, "fixture check: still the column default"
    assert before.data_sources is None
    assert before.title is None
    assert before.net_worth_estimate is None
    assert before.aum is None
    assert before.name == "Real Analyst-Assigned Name", "fixture check: a real, changed name is recorded"
    assert before.influence_score == 0.71, "fixture check: a real, changed score is recorded"

    _seed_one(monkeypatch, actor_id, influence_score=0.5)  # deliberately a DIFFERENT value
    _seed_known_actors(pg_engine)

    with pg_engine.connect() as conn:
        after = conn.execute(text(
            "SELECT provenance, name, influence_score, updated_at FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert after.provenance == PROVENANCE_UNKNOWN, "an existing unknown row must never be promoted to 'seed', empty sources or not"
    assert after.name == "Real Analyst-Assigned Name", "reseed must not overwrite the real name"
    assert after.influence_score == 0.71, "reseed must not overwrite the real score with the seed table's 0.5"
    assert after.updated_at == before.updated_at, "reseed must not touch updated_at either"


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
