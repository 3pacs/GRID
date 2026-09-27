"""Real-PostgreSQL proof that the resolved_series "dedup" paths never
delete a vintage row.

PR #688 review (2026-09-27) reversed an earlier fix. That fix treated two
resolved_series rows sharing (feature_id, obs_date) as a "duplicate" and
deleted the lower-priority one, guarded only against deleting a row PR
#683's ``resolved_series_retractions`` had already retracted (an FK from
that table to the retracted row, keyed on
``(feature_id, obs_date, vintage_date)``, plus an append-only trigger).

That guard was not the fix this needed. resolved_series' real unique index
is ``(feature_id, obs_date, vintage_date)`` -- so ANY two rows sharing
(feature_id, obs_date) are, by construction, distinct vintages, never true
duplicates. An exact duplicate (same feature_id, obs_date, vintage_date,
AND value) cannot exist; the index forbids it. ``store/pit.py``'s
FIRST_RELEASE and LATEST_AS_OF vintage policies read across exactly these
rows, so deleting the "loser" per (feature_id, obs_date) group silently
throws away point-in-time history -- measured against production, ~52% of
eligible groups would have lost their FIRST_RELEASE row.

Both delete paths are gone entirely:

* ``intelligence/resolution_audit.py::auto_fix_issues`` -- "duplicate" and
  NaN/Infinity findings are report-only: they count and log, never DELETE.
* ``scripts/hermes_fixers.py::_run_data_quality_fix`` -- the
  ``FIX_DATA_QUALITY`` dedup loop is report-only in the same way.

Runs in a throwaway schema (random name, dropped afterwards) holding a
minimal ``feature_registry``, ``resolved_series`` (production's composite
unique key) and, where a test needs it, ``resolved_series_retractions``
built exactly to PR #683's shape (FK + append-only trigger). Proves, against
real Postgres, that a two-vintage group -- the exact shape the old code
called a "duplicate" -- survives both functions completely intact, whether
or not resolved_series_retractions exists and whether or not either vintage
happens to be retracted, and that the report-only counting logic (which
does run real SELECT COUNT(*) queries) does not error.

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


def _seed_two_vintages(engine: Engine, feature_name: str, obs_date: date) -> tuple[int, date, date]:
    """Two resolved_series rows for the same (feature, obs_date): distinct
    vintages, exactly the shape the old code incorrectly called a
    "duplicate". Neither is a legitimate delete target under the current
    (report-only) code -- there is no delete target at all any more.

    Returns (feature_id, first vintage_date, second vintage_date).
    """
    vintage_a = obs_date
    vintage_b = obs_date - timedelta(days=1)
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
                "VALUES (:fid, :od, :vd, :vd, 1.0, 1)"
            ),
            {"fid": fid, "od": obs_date, "vd": vintage_a},
        )
        conn.execute(
            text(
                "INSERT INTO resolved_series "
                "(feature_id, obs_date, release_date, vintage_date, value, "
                "source_priority_used) "
                "VALUES (:fid, :od, :vd, :vd, 2.0, 2)"
            ),
            {"fid": fid, "od": obs_date, "vd": vintage_b},
        )
    return fid, vintage_a, vintage_b


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


def test_resolution_audit_duplicate_branch_never_deletes_any_vintage_no_retractions_table(scratch):
    engine = scratch
    obs_date = date.today() - timedelta(days=1)
    fid, _va, _vb = _seed_two_vintages(engine, "dedup_pg_audit_no_table", obs_date)
    # resolved_series_retractions is deliberately NOT installed for this case.

    finding = resolution_audit.AuditFinding(
        check_type="duplicate", severity="warning", feature="dedup_pg_audit_no_table",
        description="dup", evidence={"obs_date": obs_date.isoformat()},
    )
    result = resolution_audit.auto_fix_issues(engine, [finding], dry_run=False)

    assert result["duplicates_fixed"] == 0
    assert result["multi_vintage_groups_reported"] == 1
    assert _row_count(engine, fid, obs_date) == 2  # both vintages survive


def test_resolution_audit_duplicate_branch_never_deletes_any_vintage_with_retractions_table(scratch):
    engine = scratch
    obs_date = date.today() - timedelta(days=1)
    fid, vintage_a, _vb = _seed_two_vintages(engine, "dedup_pg_audit_with_table", obs_date)
    _install_retractions_table(engine)
    _retract(engine, fid, obs_date, vintage_a)  # one vintage is also retracted

    finding = resolution_audit.AuditFinding(
        check_type="duplicate", severity="warning", feature="dedup_pg_audit_with_table",
        description="dup", evidence={"obs_date": obs_date.isoformat()},
    )
    result = resolution_audit.auto_fix_issues(engine, [finding], dry_run=False)

    assert result["duplicates_fixed"] == 0
    assert result["multi_vintage_groups_reported"] == 1
    assert _row_count(engine, fid, obs_date) == 2  # both vintages survive


def test_resolution_audit_nan_branch_never_deletes_a_nan_value_row(scratch):
    """A NaN/Infinity value is real production noise, but the delete this
    branch used to run had no vintage_date filter -- report-only now."""
    engine = scratch
    obs_date = date.today() - timedelta(days=1)
    with engine.begin() as conn:
        fid = conn.execute(
            text("INSERT INTO feature_registry (name) VALUES (:n) RETURNING id"),
            {"n": "dedup_pg_audit_nan"},
        ).scalar()
        conn.execute(
            text(
                "INSERT INTO resolved_series "
                "(feature_id, obs_date, release_date, vintage_date, value, "
                "source_priority_used) "
                "VALUES (:fid, :od, :od, :od, 'NaN'::DOUBLE PRECISION, 1)"
            ),
            {"fid": fid, "od": obs_date},
        )

    finding = resolution_audit.AuditFinding(
        check_type="sanity", severity="warning", feature="dedup_pg_audit_nan",
        description="NaN detected", evidence={"obs_date": obs_date.isoformat()},
    )
    result = resolution_audit.auto_fix_issues(engine, [finding], dry_run=False)

    assert result["nan_removed"] == 0
    assert result["nan_values_reported"] == 1
    assert _row_count(engine, fid, obs_date) == 1  # the NaN row survives


# ---------------------------------------------------------------------------
# scripts/hermes_fixers.py::_run_data_quality_fix
# ---------------------------------------------------------------------------


def test_hermes_dedup_never_deletes_any_vintage_across_multiple_groups(scratch, monkeypatch):
    engine = scratch
    obs_a = date.today() - timedelta(days=1)
    obs_b = date.today() - timedelta(days=2)
    fid_a, vintage_a1, _va2 = _seed_two_vintages(engine, "dedup_pg_hermes_feature_a", obs_a)
    fid_b, _vb1, _vb2 = _seed_two_vintages(engine, "dedup_pg_hermes_feature_b", obs_b)
    _install_retractions_table(engine)
    _retract(engine, fid_a, obs_a, vintage_a1)
    # feature_b is left entirely unretracted.

    monkeypatch.setattr(hermes_fixers, "log_issue", lambda engine, **kw: None)
    result = hermes_fixers._run_data_quality_fix(engine, None, SimpleNamespace(cycle_count=1))

    assert result["duplicates_fixed"] == 0
    assert result["multi_vintage_groups_found"] == 2
    assert _row_count(engine, fid_a, obs_a) == 2  # both of feature_a's vintages survive
    assert _row_count(engine, fid_b, obs_b) == 2  # both of feature_b's vintages survive


def test_hermes_dedup_never_deletes_when_retractions_table_absent(scratch, monkeypatch):
    engine = scratch
    obs_date = date.today() - timedelta(days=1)
    fid, _va, _vb = _seed_two_vintages(engine, "dedup_pg_hermes_no_table", obs_date)
    # resolved_series_retractions deliberately not installed.

    monkeypatch.setattr(hermes_fixers, "log_issue", lambda engine, **kw: None)
    result = hermes_fixers._run_data_quality_fix(engine, None, SimpleNamespace(cycle_count=1))

    assert result["duplicates_fixed"] == 0
    assert result["multi_vintage_groups_found"] == 1
    assert _row_count(engine, fid, obs_date) == 2  # both vintages survive
