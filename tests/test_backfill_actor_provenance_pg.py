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

from intelligence.actors.provenance import PROVENANCE_SEED, PROVENANCE_UNCONFIRMED, SEED_VINTAGE_TS

_SCRIPT_MODULE = "scripts.backfill_actor_provenance"

_MINIMAL_ACTORS_DDL = """
CREATE TABLE IF NOT EXISTS actors (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    tier TEXT NOT NULL,
    category TEXT NOT NULL,
    provenance TEXT NOT NULL DEFAULT 'observed',
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
        # columns are present regardless of which DDL got there first.
        conn.execute(text(
            "ALTER TABLE actors ADD COLUMN IF NOT EXISTS provenance TEXT NOT NULL DEFAULT 'observed'"
        ))
        conn.execute(text("ALTER TABLE actors ADD COLUMN IF NOT EXISTS provenance_as_of DATE"))
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
        promoted, unconfirmed = script.backfill_chunk(
            conn, [pristine_id, touched_id],
            lock_timeout="5s", statement_timeout="30s", dry_run=False,
        )
    assert promoted == 1
    assert unconfirmed == 1

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
        first_promoted, _ = script.backfill_chunk(
            conn, [actor_id], lock_timeout="5s", statement_timeout="30s", dry_run=False,
        )
    assert first_promoted == 1

    with pg_engine.begin() as conn:
        second_promoted, second_unconfirmed = script.backfill_chunk(
            conn, [actor_id], lock_timeout="5s", statement_timeout="30s", dry_run=False,
        )
    assert second_promoted == 0, "a second pass over an already-classified row must be a no-op"
    assert second_unconfirmed == 0


def test_dry_run_writes_nothing(pg_engine: Engine, test_ids: list[str]):
    script = importlib.import_module(_SCRIPT_MODULE)

    actor_id = f"bf_prov_pg_{uuid.uuid4().hex[:16]}"
    test_ids.append(actor_id)
    with pg_engine.begin() as conn:
        _insert_actor(conn, actor_id=actor_id, updated_at=SEED_VINTAGE_TS)

    with pg_engine.begin() as conn:
        promoted, unconfirmed = script.backfill_chunk(
            conn, [actor_id], lock_timeout="5s", statement_timeout="30s", dry_run=True,
        )
    assert promoted == 1, "dry-run must still REPORT what it would do"

    with pg_engine.connect() as conn:
        row = conn.execute(text(
            "SELECT provenance FROM actors WHERE id = :id"
        ).bindparams(id=actor_id)).fetchone()
    assert row.provenance == "observed", "dry-run must not have written anything"


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
    assert row2.provenance == "observed", "chunk 2 must have rolled back to its pre-attempt state"

    # Resumability: rerunning chunk 2 (the only thing an operator needs to do)
    # completes it, with chunk 1 untouched by the resume.
    with pg_engine.begin() as conn:
        promoted, _ = script.backfill_chunk(
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
    assert row.provenance == "observed", "--dry-run through main() must not write"
