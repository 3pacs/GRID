"""Real-PostgreSQL proof for scripts/backfill_actor_provenance.py -- the standalone,
chunked, independently-committing replacement for the backfill UPDATE that used to live
inside the same Alembic transaction as the schema change (actors_provenance_20260917.py,
reverted by #597). See 00-Agent-Reports/2026-09-22/claude__ANIK__grid-596-remediation-plan.md.

Proves, against the real script (not a re-implementation):
  1. Classification correctness -- same three-way rule as the original migration: a
     pristine seed row (updated_at <= SEED_VINTAGE_TS) is promoted to 'seed'; a seed-list
     row touched by something else since (updated_at > SEED_VINTAGE_TS, e.g. an alias-fold
     or connection-merge touching updated_at with no confirmed measurement) is explicitly
     'unconfirmed', never silently 'observed' and never relabeled 'seed'.
  2. Idempotency: a second pass over already-classified rows changes nothing (rowcount 0).
  3. THE property this whole redesign exists for: independent per-chunk commits. A later
     chunk failing does NOT roll back an earlier chunk's already-committed classification
     -- a real, structural difference from the original single-transaction design, proven
     here by forcing a real failure partway through a multi-chunk run and confirming the
     earlier chunk's row is still correctly classified while the failed chunk's row is
     untouched, then confirming a rerun resumes and completes it.
  4. --dry-run (exercised via the module's own backfill_chunk(..., dry_run=True)) writes
     nothing.
  5. Concurrency: a REAL second connection runs the ACTUAL
     intelligence.actors.db.save_actor -- not a hand-rolled re-implementation of its SQL
     -- BETWEEN this function's own snapshot read and its UPDATE. The backfill must not
     overwrite that newer write with a stale classification; it must skip the row for
     this pass and leave the concurrent writer's data (and provenance) exactly as it
     left them.
  6. Genuine observation, sequential (no race at all): a row already 'observed' by a
     real, ordinary PRIOR call to save_actor() -- not concurrent with anything -- stays
     'observed' and completely untouched. This is the exact bug the honest 'unknown'
     column default (migrations/versions/actors_provenance_columns_0922.py) exists to
     fix: under the old design, a column default of 'observed' made an already-confirmed
     row indistinguishable from an unclassified one, so ANY row observed before a
     backfill run -- not only one raced against it -- was at risk of reclassification.
  7. A maintenance-only touch -- updated_at moves, nothing else, the exact shape of
     intelligence/actors/trial_bridge.py, intelligence/actor_discovery.py,
     scripts/fold_actor_aliases.py, none of which touch the provenance column at all --
     is classified 'unconfirmed', never 'observed'. Timestamp movement alone must never
     earn the strongest claim (see intelligence/actors/provenance.py's "Timestamp
     changes alone never qualify" rule).
  8. A subsequent, ordinary (non-racing) backfill rerun, run twice more after a genuine
     observation, leaves the row completely untouched -- including its updated_at.
     Confirmed provenance survives repeated backfill passes, not just the one live race
     case 5 exercises.

Uses the shared ``pg_engine`` fixture (tests/conftest.py) -- skips cleanly if no
PostgreSQL is reachable.
"""

from __future__ import annotations

import importlib
import uuid
from datetime import timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.actors import db as actors_db
from intelligence.actors.provenance import (
    PROVENANCE_OBSERVED,
    PROVENANCE_SEED,
    PROVENANCE_UNCONFIRMED,
    PROVENANCE_UNKNOWN,
    SEED_VINTAGE_TS,
)

_SCRIPT_MODULE = "scripts.backfill_actor_provenance"

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
        # This suite shares one disposable database across files within a
        # run, so CREATE TABLE IF NOT EXISTS alone is a no-op once any other
        # file has already created the table -- ensure this file's needed
        # columns (including every column the REAL save_actor() writes) are
        # present regardless of which DDL got there first.
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


def _insert_actor(conn, *, actor_id: str, updated_at) -> None:
    conn.execute(text(
        "INSERT INTO actors (id, name, tier, category, updated_at) "
        "VALUES (:id, 'Test Actor', 'test', 'test', :updated_at)"
    ).bindparams(id=actor_id, updated_at=updated_at))


def test_backfill_chunk_classifies_seed_vs_unconfirmed(pg_engine: Engine, test_ids: list[str]):
    script = importlib.import_module(_SCRIPT_MODULE)

    pristine_id = f"bf_prov_pg_{uuid.uuid4().hex[:16]}"
    touched_id = f"bf_prov_pg_{uuid.uuid4().hex[:16]}"
    test_ids.extend([pristine_id, touched_id])

    with pg_engine.begin() as conn:
        _insert_actor(conn, actor_id=pristine_id, updated_at=SEED_VINTAGE_TS)
        _insert_actor(conn, actor_id=touched_id, updated_at=SEED_VINTAGE_TS + timedelta(days=30))

    with pg_engine.begin() as conn:
        promoted, unconfirmed, skipped = script.backfill_chunk(
            conn, [pristine_id, touched_id],
            lock_timeout="5s", statement_timeout="30s", dry_run=False,
        )
    assert promoted == 1
    assert unconfirmed == 1
    assert skipped == 0

    with pg_engine.connect() as conn:
        rows = {
            r.id: (r.provenance, r.provenance_as_of)
            for r in conn.execute(text(
                "SELECT id, provenance, provenance_as_of FROM actors WHERE id = ANY(:ids)"
            ).bindparams(ids=[pristine_id, touched_id])).fetchall()
        }
    assert rows[pristine_id][0] == PROVENANCE_SEED
    assert str(rows[pristine_id][1]) == "2026-04-07"
    assert rows[touched_id][0] == PROVENANCE_UNCONFIRMED
    assert rows[touched_id][1] is None


def test_backfill_chunk_is_idempotent(pg_engine: Engine, test_ids: list[str]):
    script = importlib.import_module(_SCRIPT_MODULE)

    actor_id = f"bf_prov_pg_{uuid.uuid4().hex[:16]}"
    test_ids.append(actor_id)
    with pg_engine.begin() as conn:
        _insert_actor(conn, actor_id=actor_id, updated_at=SEED_VINTAGE_TS)

    with pg_engine.begin() as conn:
        first_promoted, _, _ = script.backfill_chunk(
            conn, [actor_id], lock_timeout="5s", statement_timeout="30s", dry_run=False,
        )
    assert first_promoted == 1

    with pg_engine.begin() as conn:
        second_promoted, second_unconfirmed, second_skipped = script.backfill_chunk(
            conn, [actor_id], lock_timeout="5s", statement_timeout="30s", dry_run=False,
        )
    assert second_promoted == 0, "a second pass over an already-classified row must be a no-op"
    assert second_unconfirmed == 0
    assert second_skipped == 0, "an already-classified row is out of scope via the eligibility check, not the conflict guard"


def test_dry_run_writes_nothing(pg_engine: Engine, test_ids: list[str]):
    script = importlib.import_module(_SCRIPT_MODULE)

    actor_id = f"bf_prov_pg_{uuid.uuid4().hex[:16]}"
    test_ids.append(actor_id)
    with pg_engine.begin() as conn:
        _insert_actor(conn, actor_id=actor_id, updated_at=SEED_VINTAGE_TS)

    with pg_engine.begin() as conn:
        promoted, unconfirmed, skipped = script.backfill_chunk(
            conn, [actor_id], lock_timeout="5s", statement_timeout="30s", dry_run=True,
        )
    assert promoted == 1, "dry-run must still REPORT what it would do"
    assert skipped == 0

    with pg_engine.connect() as conn:
        row = conn.execute(text(
            "SELECT provenance FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert row.provenance == PROVENANCE_UNKNOWN, "dry-run must not have written anything -- still the column default"


def test_a_later_chunk_failing_does_not_roll_back_an_earlier_chunks_commit(
    pg_engine: Engine, test_ids: list[str],
):
    """The core property of this redesign. Mirrors main()'s own per-chunk loop
    structure (engine.begin() per chunk, independent of the others) rather than
    invoking main() as a subprocess, so this exercises the real backfill_chunk()
    function under the real per-chunk-commit pattern -- not a re-implementation.
    """
    script = importlib.import_module(_SCRIPT_MODULE)

    chunk1_id = f"bf_prov_pg_{uuid.uuid4().hex[:16]}"
    chunk2_id = f"bf_prov_pg_{uuid.uuid4().hex[:16]}"
    test_ids.extend([chunk1_id, chunk2_id])

    with pg_engine.begin() as conn:
        _insert_actor(conn, actor_id=chunk1_id, updated_at=SEED_VINTAGE_TS)
        _insert_actor(conn, actor_id=chunk2_id, updated_at=SEED_VINTAGE_TS)

    # Chunk 1: succeeds and commits, exactly like main()'s loop.
    with pg_engine.begin() as conn:
        script.backfill_chunk(
            conn, [chunk1_id], lock_timeout="5s", statement_timeout="30s", dry_run=False,
        )

    # Chunk 2: a REAL failure mid-transaction (not simulated with a mock) -- an
    # invalid statement_timeout value Postgres itself rejects, thrown from inside
    # backfill_chunk's own SET LOCAL call, exactly where a real transient failure
    # would surface. The `with pg_engine.begin()` context manager rolls this
    # transaction back on the exception, same as main()'s try/except does for a
    # real chunk failure.
    with pytest.raises(Exception):
        with pg_engine.begin() as conn:
            script.backfill_chunk(
                conn, [chunk2_id],
                lock_timeout="5s", statement_timeout="not-a-valid-timeout",
                dry_run=False,
            )

    # Decisive check: chunk 1's commit survived chunk 2's failure entirely --
    # this is the property the original single-transaction migration could not
    # offer (a failure anywhere rolled back everything, ADD COLUMNs included).
    with pg_engine.connect() as conn:
        row1 = conn.execute(text(
            "SELECT provenance FROM actors WHERE id = :id"
        ).bindparams(id=chunk1_id)).fetchone()
        row2 = conn.execute(text(
            "SELECT provenance FROM actors WHERE id = :id"
        ).bindparams(id=chunk2_id)).fetchone()
    assert row1.provenance == PROVENANCE_SEED, "chunk 1 must remain committed"
    assert row2.provenance == PROVENANCE_UNKNOWN, "chunk 2 must have rolled back to its pre-attempt state"

    # Resumability: rerunning chunk 2 (the only thing an operator needs to do)
    # completes it, with chunk 1 untouched by the resume.
    with pg_engine.begin() as conn:
        promoted, _, _ = script.backfill_chunk(
            conn, [chunk2_id], lock_timeout="5s", statement_timeout="30s", dry_run=False,
        )
    assert promoted == 1

    with pg_engine.connect() as conn:
        row2_after = conn.execute(text(
            "SELECT provenance FROM actors WHERE id = :id"
        ).bindparams(id=chunk2_id)).fetchone()
    assert row2_after.provenance == PROVENANCE_SEED


def test_main_dry_run_end_to_end_against_real_seed_list(
    pg_engine: Engine, test_ids: list[str], monkeypatch, capsys,
):
    """Basic CLI-wiring sanity: main() itself, in --dry-run mode, against a
    monkeypatched small seed list, runs to completion and touches nothing."""
    script = importlib.import_module(_SCRIPT_MODULE)

    a_id = f"bf_prov_pg_{uuid.uuid4().hex[:16]}"
    test_ids.append(a_id)
    with pg_engine.begin() as conn:
        _insert_actor(conn, actor_id=a_id, updated_at=SEED_VINTAGE_TS)

    monkeypatch.setattr(script, "SEED_ACTOR_IDS", frozenset({a_id}))
    monkeypatch.setattr(script, "get_engine", lambda: pg_engine)
    monkeypatch.setattr("sys.argv", ["backfill_actor_provenance.py", "--dry-run", "--chunk-size", "1"])

    rc = script.main()
    assert rc == 0

    with pg_engine.connect() as conn:
        row = conn.execute(text(
            "SELECT provenance FROM actors WHERE id = :id"
        ).bindparams(id=a_id)).fetchone()
    assert row.provenance == PROVENANCE_UNKNOWN, "--dry-run through main() must not write"


def test_genuine_observation_via_real_save_actor_is_permanently_out_of_scope(
    pg_engine: Engine, test_ids: list[str],
):
    """No race at all -- a row already classified 'observed' by a real, ordinary
    PRIOR call to save_actor() must never be reclassified by a backfill pass that
    runs afterward. This is exactly the bug the honest 'unknown' default fixes:
    under the old design (column default 'observed'), this row would have been
    indistinguishable from an unclassified one, and any row observed before a
    backfill run -- not only one raced against it -- was at risk."""
    script = importlib.import_module(_SCRIPT_MODULE)
    actor_id = f"bf_prov_pg_{uuid.uuid4().hex[:16]}"
    test_ids.append(actor_id)

    actors_db.save_actor(pg_engine, actor_id, {
        "name": "Observed Actor", "tier": "test", "category": "test",
        "influence_score": 0.64, "data_sources": ["10-K"],
    })
    with pg_engine.connect() as conn:
        before = conn.execute(text(
            "SELECT provenance FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert before.provenance == PROVENANCE_OBSERVED, "fixture check: save_actor must have stamped 'observed'"

    with pg_engine.begin() as conn:
        promoted, unconfirmed, skipped = script.backfill_chunk(
            conn, [actor_id], lock_timeout="5s", statement_timeout="30s", dry_run=False,
        )
    assert (promoted, unconfirmed, skipped) == (0, 0, 0), (
        "a row already 'observed' before backfill ever ran must not be touched at all "
        "-- not reclassified, and not even counted as a conflict, since it was never "
        "eligible in the first place"
    )

    with pg_engine.connect() as conn:
        row = conn.execute(text(
            "SELECT provenance, influence_score FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert row.provenance == PROVENANCE_OBSERVED
    assert row.influence_score == 0.64


def test_maintenance_only_touch_is_classified_unconfirmed_never_observed(
    pg_engine: Engine, test_ids: list[str],
):
    """A maintenance writer's exact shape -- intelligence/actors/trial_bridge.py,
    intelligence/actor_discovery.py, scripts/fold_actor_aliases.py, none of which
    touch the provenance column -- moves updated_at and nothing else. Backfill must
    classify the result 'unconfirmed': modification is real, but nothing confirms
    an observation, so it must never earn 'observed'."""
    script = importlib.import_module(_SCRIPT_MODULE)
    actor_id = f"bf_prov_pg_{uuid.uuid4().hex[:16]}"
    test_ids.append(actor_id)

    with pg_engine.begin() as conn:
        _insert_actor(conn, actor_id=actor_id, updated_at=SEED_VINTAGE_TS)

    # The maintenance writer's contract: updated_at only.
    with pg_engine.begin() as conn:
        conn.execute(text(
            "UPDATE actors SET updated_at = NOW() WHERE id = :id"
        ).bindparams(id=actor_id))

    with pg_engine.begin() as conn:
        promoted, unconfirmed, skipped = script.backfill_chunk(
            conn, [actor_id], lock_timeout="5s", statement_timeout="30s", dry_run=False,
        )
    assert promoted == 0
    assert unconfirmed == 1, "touched since the seed vintage by something, but not through save_actor's contract"
    assert skipped == 0

    with pg_engine.connect() as conn:
        row = conn.execute(text(
            "SELECT provenance FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert row.provenance == PROVENANCE_UNCONFIRMED, "a bare timestamp touch must never produce 'observed'"


def test_concurrent_real_writer_evidence_survives_backfill(pg_engine: Engine, test_ids: list[str]):
    """The concurrency case this design exists to handle: a REAL second connection,
    running the ACTUAL intelligence.actors.db.save_actor (not a hand-rolled
    re-implementation of its SQL), writes to a row AFTER backfill_chunk() has taken
    its snapshot but BEFORE its UPDATE runs.

    Reproduced deterministically (not as a timing-dependent race) by intercepting
    backfill_chunk()'s own snapshot SELECT and triggering the real save_actor() call
    at that exact moment -- both the interception point's target and save_actor()
    itself are real, only the trigger point is controlled so the test does not
    depend on winning an actual timing race to be meaningful.
    """
    script = importlib.import_module(_SCRIPT_MODULE)

    actor_id = f"bf_prov_pg_{uuid.uuid4().hex[:16]}"
    test_ids.append(actor_id)

    # Pristine row at the column default: eligible to be promoted to 'seed' by an
    # ordinary pass, exactly like any other never-classified seed-list id.
    with pg_engine.begin() as conn:
        _insert_actor(conn, actor_id=actor_id, updated_at=SEED_VINTAGE_TS)

    class _InterceptingConnection:
        """Wraps the real connection backfill_chunk() uses; after its snapshot
        SELECT returns, calls the REAL save_actor() -- which opens and commits its
        own separate real connection, exactly as it always does in production --
        before backfill_chunk()'s own UPDATE gets a chance to run."""

        def __init__(self, real_conn):
            self._real = real_conn
            self._select_seen = False

        def execute(self, stmt, params=None):
            result = self._real.execute(stmt, params) if params is not None else self._real.execute(stmt)
            sql_text = str(stmt)
            if not self._select_seen and "SELECT id, provenance, updated_at" in sql_text:
                self._select_seen = True
                actors_db.save_actor(pg_engine, actor_id, {
                    "name": "Concurrent Observation", "tier": "test", "category": "test",
                    "influence_score": 0.83, "data_sources": ["sec_form4"],
                })
            return result

        def __getattr__(self, name):
            return getattr(self._real, name)

    with pg_engine.begin() as real_conn:
        wrapped = _InterceptingConnection(real_conn)
        promoted, unconfirmed, skipped = script.backfill_chunk(
            wrapped, [actor_id], lock_timeout="5s", statement_timeout="30s", dry_run=False,
        )

    assert skipped == 1, "the concurrent write must be detected and the row skipped, not overwritten"
    assert promoted == 0
    assert unconfirmed == 0

    with pg_engine.connect() as conn:
        row = conn.execute(text(
            "SELECT provenance, influence_score, data_sources::text AS data_sources "
            "FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()

    # The concurrent writer's evidence is fully intact -- backfill did not touch it.
    assert row.influence_score == 0.83, "the concurrent writer's real data must survive"
    assert row.data_sources == '["sec_form4"]'
    assert row.provenance == PROVENANCE_OBSERVED, (
        "save_actor's real contract stamps 'observed' explicitly and unconditionally "
        "-- verified against its actual current source, not assumed"
    )

    # A later pass must leave the row alone entirely -- not merely refrain from
    # overwriting the concurrent write in THIS pass, but never reconsider an
    # 'observed' row again. Under the pre-'unknown'-default design, a later pass
    # would have seen an unclassified 'observed' row touched after the seed vintage
    # and wrongly marked it 'unconfirmed'; see
    # test_genuine_observation_via_real_save_actor_is_permanently_out_of_scope and
    # test_subsequent_backfill_rerun_after_genuine_observation_leaves_it_untouched
    # for the same property proven without any race scaffolding at all.
    with pg_engine.begin() as conn:
        promoted2, unconfirmed2, skipped2 = script.backfill_chunk(
            conn, [actor_id], lock_timeout="5s", statement_timeout="30s", dry_run=False,
        )
    assert (promoted2, unconfirmed2, skipped2) == (0, 0, 0)

    with pg_engine.connect() as conn:
        row2 = conn.execute(text(
            "SELECT provenance FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert row2.provenance == PROVENANCE_OBSERVED


def test_subsequent_backfill_rerun_after_genuine_observation_leaves_it_untouched(
    pg_engine: Engine, test_ids: list[str],
):
    """Confirmed provenance survives repeated, ORDINARY (non-racing) backfill
    reruns -- not just the one live race the concurrency test exercises. Two
    sequential passes, days apart in spirit, with no interleaving at all."""
    script = importlib.import_module(_SCRIPT_MODULE)
    actor_id = f"bf_prov_pg_{uuid.uuid4().hex[:16]}"
    test_ids.append(actor_id)

    actors_db.save_actor(pg_engine, actor_id, {
        "name": "Real Observation", "tier": "test", "category": "test",
        "influence_score": 0.71, "data_sources": ["sec_form4"],
    })
    with pg_engine.connect() as conn:
        before = conn.execute(text(
            "SELECT provenance, influence_score, updated_at FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert before.provenance == PROVENANCE_OBSERVED

    for _ in range(2):
        with pg_engine.begin() as conn:
            promoted, unconfirmed, skipped = script.backfill_chunk(
                conn, [actor_id], lock_timeout="5s", statement_timeout="30s", dry_run=False,
            )
        assert (promoted, unconfirmed, skipped) == (0, 0, 0), (
            "an already-observed row is permanently out of scope on every rerun -- "
            "not reclassified, not even counted as a conflict"
        )

    with pg_engine.connect() as conn:
        after = conn.execute(text(
            "SELECT provenance, influence_score, updated_at FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert after.provenance == PROVENANCE_OBSERVED
    assert after.influence_score == before.influence_score
    assert after.updated_at == before.updated_at, "backfill must not even touch updated_at on an out-of-scope row"
