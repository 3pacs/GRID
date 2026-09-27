"""Real-PostgreSQL proof that PR #683's resolved_series_retractions FK does
not break resolved_series dedup delete paths.

PR #683 (feat/resolved-series-retractions-20260927, not yet on main) adds
``resolved_series_retractions``: an append-only table with an FK to the
retracted ``resolved_series`` row, keyed on
``(feature_id, obs_date, vintage_date)``. Its review found two dedup delete
paths that would start hitting FK violations the moment a retracted row
happens to also be a dedup loser:

* ``intelligence/resolution_audit.py::auto_fix_issues`` ("duplicate" branch)
* ``scripts/hermes_fixers.py::_run_data_quality_fix`` (Hermes'
  ``FIX_DATA_QUALITY`` dedup loop), which additionally shared one
  transaction across every dupe-group with no per-row isolation: one FK
  violation aborted the whole transaction, so a bare ``try/except`` caught
  the immediate exception but every later statement on that same
  connection -- including the rest of the loop and the implicit commit --
  failed too, silently rolling back every other dedup in the batch.

Runs in a throwaway schema (random name, dropped afterwards) holding a
minimal ``feature_registry``, ``resolved_series`` (production's composite
unique key) and, where a test needs it, ``resolved_series_retractions``
built exactly to PR #683's shape (FK + append-only trigger). Proves:

* a retracted dedup-loser row survives both fix functions without ever
  raising -- the anti-join keeps it out of the delete's target set, so the
  FK is never actually hit;
* an unretracted duplicate in the same batch still gets cleaned up
  normally;
* the SAVEPOINT this fix adds around the Hermes delete truly confines a
  *real* Postgres FK-violation rollback to the one dupe-group that hit it
  (monkeypatching the table-exists check off for one call proves this with
  an actual violation, not just the fake-connection tests in
  tests/test_dedup_retraction_guard.py, which cannot reproduce Postgres'
  real transaction-abort semantics).

Uses the shared ``pg_engine`` fixture's URL (``GRID_TEST_DB_URL``); skips
when no PostgreSQL is reachable (the CI step fails on a skip).
"""

from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from intelligence import resolution_audit
from scripts import hermes_fixers

_FEATURE_REGISTRY_DDL = """
CREATE TABLE feature_registry (
    id    SERIAL PRIMARY KEY,
    name  TEXT NOT NULL UNIQUE
)
"""

# schema.sql's resolved_series, trimmed to what these fixes touch.
_RESOLVED_SERIES_DDL = """
CREATE TABLE resolved_series (
    id                    BIGSERIAL PRIMARY KEY,
    feature_id            INTEGER NOT NULL REFERENCES feature_registry(id),
    obs_date              DATE NOT NULL,
    release_date          DATE NOT NULL,
    vintage_date          DATE NOT NULL,
    value                 DOUBLE PRECISION NOT NULL,
    source_priority_used  INTEGER NOT NULL,
    conflict_flag         BOOLEAN NOT NULL DEFAULT FALSE,
    conflict_detail       JSONB,
    resolution_version    INTEGER NOT NULL DEFAULT 1
)
"""
_RESOLVED_SERIES_KEY = (
    "CREATE UNIQUE INDEX uq_resolved_series_composite "
    "ON resolved_series (feature_id, obs_date, vintage_date)"
)

# PR #683's schema.sql addition, reproduced exactly (FK + append-only guard).
_RETRACTIONS_DDL = """
CREATE TABLE resolved_series_retractions (
    id            BIGSERIAL PRIMARY KEY,
    feature_id    INTEGER NOT NULL,
    obs_date      DATE NOT NULL,
    vintage_date  DATE NOT NULL,
    retracted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reason        TEXT NOT NULL CHECK (btrim(reason) <> ''),
    run_tag       TEXT NOT NULL CHECK (btrim(run_tag) <> ''),
    CONSTRAINT uq_resolved_series_retractions_key
        UNIQUE (feature_id, obs_date, vintage_date) INCLUDE (retracted_at),
    CONSTRAINT fk_resolved_series_retractions_row
        FOREIGN KEY (feature_id, obs_date, vintage_date)
        REFERENCES resolved_series (feature_id, obs_date, vintage_date)
)
"""
_RETRACTIONS_GUARD_FN = """
CREATE OR REPLACE FUNCTION resolved_series_retractions_guard()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION
        'resolved_series_retractions is append-only (% refused)', TG_OP;
END;
$$ LANGUAGE plpgsql;
"""
_RETRACTIONS_GUARD_TRIGGER = """
CREATE TRIGGER trg_resolved_series_retractions_guard
    BEFORE INSERT OR UPDATE OR DELETE ON resolved_series_retractions
    FOR EACH ROW
    EXECUTE FUNCTION resolved_series_retractions_guard();
"""


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"dedup_retract_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(
        pg_engine.url, connect_args={"options": f"-csearch_path={schema}"},
    )
    try:
        with engine.begin() as conn:
            conn.execute(text(_FEATURE_REGISTRY_DDL))
            conn.execute(text(_RESOLVED_SERIES_DDL))
            conn.execute(text(_RESOLVED_SERIES_KEY))
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _install_retractions_table(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(_RETRACTIONS_DDL))
        conn.execute(text(_RETRACTIONS_GUARD_FN))
        conn.execute(text(_RETRACTIONS_GUARD_TRIGGER))


def _retract(engine: Engine, feature_id: int, obs_date: date, vintage_date: date) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO resolved_series_retractions "
                "(feature_id, obs_date, vintage_date, reason, run_tag) "
                "VALUES (:fid, :od, :vd, 'wrong instrument', 'test-run')"
            ),
            {"fid": feature_id, "od": obs_date, "vd": vintage_date},
        )


def _seed_duplicate(engine: Engine, feature_name: str, obs_date: date) -> tuple[int, date]:
    """Two resolved_series rows for the same (feature, obs_date): a keeper
    (inserted first, lower source_priority_used -- never a delete target
    under either fix's dedup rule) and a loser (inserted second, higher
    source_priority_used -- the row both dedup deletes target).

    Returns (feature_id, loser's vintage_date).
    """
    loser_vintage = obs_date - timedelta(days=1)
    with engine.begin() as conn:
        fid = conn.execute(
            text("INSERT INTO feature_registry (name) VALUES (:n) RETURNING id"),
            {"n": feature_name},
        ).scalar()
        conn.execute(
            text(
                "INSERT INTO resolved_series "
                "(feature_id, obs_date, release_date, vintage_date, value, "
                "source_priority_used) "
                "VALUES (:fid, :od, :od, :od, 1.0, 1)"
            ),
            {"fid": fid, "od": obs_date},
        )
        conn.execute(
            text(
                "INSERT INTO resolved_series "
                "(feature_id, obs_date, release_date, vintage_date, value, "
                "source_priority_used) "
                "VALUES (:fid, :od, :vd, :vd, 2.0, 2)"
            ),
            {"fid": fid, "od": obs_date, "vd": loser_vintage},
        )
    return fid, loser_vintage


def _row_count(engine: Engine, feature_id: int, obs_date: date) -> int:
    with engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT COUNT(*) FROM resolved_series "
                "WHERE feature_id = :fid AND obs_date = :od"
            ),
            {"fid": feature_id, "od": obs_date},
        ).scalar()


# ---------------------------------------------------------------------------
# intelligence/resolution_audit.py::auto_fix_issues
# ---------------------------------------------------------------------------


def test_resolution_audit_dedup_never_deletes_a_retracted_loser(scratch):
    engine = scratch
    obs_date = date.today() - timedelta(days=1)
    fid, loser_vintage = _seed_duplicate(engine, "dedup_pg_audit_feature", obs_date)
    _install_retractions_table(engine)
    _retract(engine, fid, obs_date, loser_vintage)

    finding = resolution_audit.AuditFinding(
        check_type="duplicate", severity="warning", feature="dedup_pg_audit_feature",
        description="dup", evidence={"obs_date": obs_date.isoformat()},
    )
    result = resolution_audit.auto_fix_issues(engine, [finding], dry_run=False)

    assert result["duplicates_fixed"] == 1  # ran without an FK violation
    assert _row_count(engine, fid, obs_date) == 2  # keeper + FK-protected loser


def test_resolution_audit_dedup_still_deletes_an_unretracted_loser(scratch):
    """The anti-join must not become a blanket skip: an ordinary duplicate
    with nothing retracted still gets cleaned up."""
    engine = scratch
    obs_date = date.today() - timedelta(days=1)
    fid, _loser_vintage = _seed_duplicate(engine, "dedup_pg_audit_clean_feature", obs_date)
    _install_retractions_table(engine)  # table exists, but nothing retracted

    finding = resolution_audit.AuditFinding(
        check_type="duplicate", severity="warning", feature="dedup_pg_audit_clean_feature",
        description="dup", evidence={"obs_date": obs_date.isoformat()},
    )
    result = resolution_audit.auto_fix_issues(engine, [finding], dry_run=False)

    assert result["duplicates_fixed"] == 1
    assert _row_count(engine, fid, obs_date) == 1  # loser was deleted as before


# ---------------------------------------------------------------------------
# scripts/hermes_fixers.py::_run_data_quality_fix
# ---------------------------------------------------------------------------


def test_hermes_dedup_respects_retraction_and_still_cleans_others(scratch, monkeypatch):
    engine = scratch
    obs_a = date.today() - timedelta(days=1)
    obs_b = date.today() - timedelta(days=2)
    fid_a, loser_vintage_a = _seed_duplicate(engine, "dedup_pg_hermes_feature_a", obs_a)
    fid_b, _loser_vintage_b = _seed_duplicate(engine, "dedup_pg_hermes_feature_b", obs_b)
    _install_retractions_table(engine)
    _retract(engine, fid_a, obs_a, loser_vintage_a)
    # feature_b's loser is deliberately left unretracted.

    monkeypatch.setattr(hermes_fixers, "log_issue", lambda engine, **kw: None)
    result = hermes_fixers._run_data_quality_fix(engine, None, SimpleNamespace(cycle_count=1))

    assert result["duplicates_fixed"] == 2  # both groups processed, no FK error
    assert _row_count(engine, fid_a, obs_a) == 2  # keeper + FK-protected loser
    assert _row_count(engine, fid_b, obs_b) == 1  # unretracted loser was deleted


def test_hermes_dedup_savepoint_isolates_a_real_fk_violation(scratch, monkeypatch):
    """Force a genuine Postgres FK violation (bypass the anti-join via
    monkeypatch, as if the guard this fix adds were absent) to prove the
    SAVEPOINT it also adds truly confines the rollback to the one
    dupe-group that hit it -- the real, Postgres-level regression #683's
    review found. A fake connection (tests/test_dedup_retraction_guard.py)
    cannot reproduce Postgres' actual transaction-abort semantics, so this
    is the test that proves the fix rather than merely describing it.
    """
    engine = scratch
    obs_a = date.today() - timedelta(days=1)
    obs_b = date.today() - timedelta(days=2)
    fid_a, loser_vintage_a = _seed_duplicate(engine, "dedup_pg_fk_violation_feature_a", obs_a)
    fid_b, _loser_vintage_b = _seed_duplicate(engine, "dedup_pg_fk_violation_feature_b", obs_b)
    _install_retractions_table(engine)
    _retract(engine, fid_a, obs_a, loser_vintage_a)

    # Simulate the anti-join being absent (pre-fix): the delete for
    # feature_a's retracted loser now genuinely violates the FK inside
    # Postgres, not just in Python.
    monkeypatch.setattr(
        hermes_fixers, "_hermes_retractions_table_exists", lambda conn: False,
    )
    monkeypatch.setattr(hermes_fixers, "log_issue", lambda engine, **kw: None)

    result = hermes_fixers._run_data_quality_fix(engine, None, SimpleNamespace(cycle_count=1))

    # feature_a's delete raised a real FK violation and was caught; only
    # feature_b's succeeded -- proof the SAVEPOINT confined feature_a's
    # abort to itself rather than poisoning the shared transaction.
    assert result["duplicates_fixed"] == 1
    assert _row_count(engine, fid_a, obs_a) == 2  # the violation blocked the delete
    assert _row_count(engine, fid_b, obs_b) == 1  # feature_b's dedup still went through
