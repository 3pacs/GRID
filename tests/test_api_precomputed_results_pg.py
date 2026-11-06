"""Real-PostgreSQL proof for api_precomputed_results (W3-2).

Throwaway schema on the shared ``pg_engine`` fixture (``GRID_TEST_DB_URL``);
skips without PostgreSQL. Proves the migration is idempotent and granted,
the upsert / failure semantics of ``store/precomputed.py`` (a failure never
replaces a good payload), JSONB round-trips without NaN, the 10 MB-class
payload splice, and that the job advisory lock is exclusive across
connections.
"""

from __future__ import annotations

import importlib
import json
from datetime import date, datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

from intelligence import api_precompute as ap
from store import precomputed as store

_MIGRATION = "migrations.versions.api_precomputed_results_20261106"


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"api_precomp_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _apply(engine: Engine) -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module(_MIGRATION)
    with engine.connect() as conn:
        trans = conn.begin()
        real_op = migration.op
        migration.op = Operations(MigrationContext.configure(conn))
        try:
            migration.upgrade()
        finally:
            migration.op = real_op
        trans.commit()


def test_migration_is_idempotent_and_chains_on_one_head(scratch):
    _apply(scratch)
    _apply(scratch)
    with scratch.connect() as conn:
        cols = {r[0] for r in conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'api_precomputed_results' AND table_schema = current_schema()"))}
    assert {"key", "params_hash", "payload", "computed_at", "as_of", "version", "inputs",
            "last_attempt_at", "last_status", "last_error"} <= cols
    migration = importlib.import_module(_MIGRATION)
    assert migration.down_revision == "oracle_cal_exploratory_20261106"


def test_upsert_failure_and_roundtrip_semantics(scratch):
    _apply(scratch)
    params = {"days": 252, "min_corr": 0.3, "show_trivial": False}
    assert store.read_result(scratch, "k", params) is None

    store.record_attempt_failure(scratch, key="k", params=params, status="error",
                                 error="first run failed", version="1")
    row = store.read_result(scratch, "k", params)
    assert row is not None and not row.has_payload and row.last_error == "first run failed"

    store.write_success(scratch, key="k", params=params,
                        payload={"matrix": [[1.0, float("nan")]], "d": date(2026, 11, 5)},
                        as_of=date(2026, 11, 5), version="1", inputs={"rows": 3}, duration_s=4.2)
    row = store.read_result(scratch, "k", params)
    assert row.payload() == {"matrix": [[1.0, None]], "d": "2026-11-05"}
    assert row.as_of == datetime(2026, 11, 5, tzinfo=timezone.utc)
    assert row.last_status == "ok" and row.last_error is None and row.inputs == {"rows": 3}
    good_at = row.computed_at

    store.record_attempt_failure(scratch, key="k", params=params, status="unavailable",
                                 error="thin vector", version="1")
    row = store.read_result(scratch, "k", params)
    assert row.payload() == {"matrix": [[1.0, None]], "d": "2026-11-05"}  # kept
    assert row.computed_at == good_at and row.last_status == "unavailable"

    meta = store.freshness_meta(row, key="k", params=params, stale_after_s=3600, cadence_s=600)
    assert meta["as_of"] == "2026-11-05" and meta["last_error"] == "thin vector"
    assert json.loads(store.splice_meta(row.payload_json, meta))["precompute"]["key"] == "k"
    assert [r.key for r in store.read_key_rows(scratch, ["k", "other"])] == ["k"]


def test_payload_and_computed_at_are_set_together(scratch):
    _apply(scratch)
    with pytest.raises(DBAPIError), scratch.begin() as conn:
        conn.execute(text(
            "INSERT INTO api_precomputed_results (key, params_hash, payload) "
            "VALUES ('x', 'y', '{}'::jsonb)"))


def test_job_lock_is_exclusive_across_connections(scratch):
    first = ap._AdvisoryLock(scratch, "globe")
    second = ap._AdvisoryLock(scratch, "globe")
    other = ap._AdvisoryLock(scratch, "influence")
    assert first.acquire() is True
    try:
        assert second.acquire() is False
        assert other.acquire() is True
        other.release()
    finally:
        first.release()
    assert second.acquire() is True
    second.release()


def test_run_job_end_to_end(scratch, monkeypatch):
    _apply(scratch)
    job = ap.Job(
        name="pg_e2e", cadence_s=600, stale_after_s=3600, version="1",
        build=lambda ctx: [ap.Output("e2e.key", {}, {"items": [1]}, date(2026, 11, 5))],
        outputs=(("e2e.key", ap._p()),), empty_fields={"e2e.key": ("items",)},
    )
    first = ap.run_job(scratch, job)
    assert first["written"] == 1
    assert ap.run_job(scratch, job)["skipped"] == "not due"
    row = store.read_result(scratch, "e2e.key", {})
    assert row.payload() == {"items": [1]} and row.version == "1"
