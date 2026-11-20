"""PostgreSQL contract for astrogrid.backtest_run_invalidation (ag_backtest_invalid_20261110).

Real schema from the AstroGrid migration chain in a disposable database:
the migration marks rotation_variant_* runs and nothing else, the marks are
append-only, and list_backtest_results never returns a marked run's results.
"""

from __future__ import annotations

import importlib
import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.exc import DBAPIError, IntegrityError

from store.astrogrid import AstroGridStore

_BASE_CHAIN = (
    "b6bba10f0fdb_add_astrogrid_schema_boundary",
    "c91b4a2e7d33_add_astrogrid_learning_loop_scaffold",
)
_MIGRATION = "ag_backtest_invalid_20261110"


def _run_migration(conn, name: str) -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    module = importlib.import_module(f"migrations.versions.{name}")
    original = getattr(module, "op", None)
    module.op = Operations(MigrationContext.configure(conn))
    try:
        module.upgrade()
    finally:
        module.op = original


def _add_run(conn, run_key: str) -> int:
    run_id = conn.execute(text("""
        INSERT INTO astrogrid.backtest_run (run_key, strategy_variant, horizon_label, target_universe, status)
        VALUES (:k, 'grid_only', 'swing', 'asset_groups', 'completed') RETURNING id
    """), {"k": run_key}).scalar()
    conn.execute(text("""
        INSERT INTO astrogrid.backtest_result (backtest_run_id, result_key, strategy_variant, target_symbol, alpha_vs_benchmark)
        VALUES (:r, 'summary', 'grid_only', 'SPY', :a)
    """), {"r": run_id, "a": 96.7 if run_key.startswith("rotation_variant_") else 0.4})
    return int(run_id)


@pytest.fixture
def pg_engine() -> Engine:
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        pytest.skip("GRID_TEST_DB_URL is required for disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("backtest invalidation tests require a local disposable test database")
    engine = create_engine(url)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS astrogrid CASCADE"))
        for name in _BASE_CHAIN:
            _run_migration(conn, name)
    try:
        yield engine
    finally:
        with engine.begin() as conn:
            conn.execute(text("DROP SCHEMA IF EXISTS astrogrid CASCADE"))
        engine.dispose()


def test_migration_marks_only_rotation_runs_and_listing_skips_them(pg_engine: Engine) -> None:
    with pg_engine.begin() as conn:
        rot = [_add_run(conn, f"rotation_variant_{i:02d}_2024-01-01_2026-03-25") for i in range(3)]
        lookalike = _add_run(conn, "rotationXvariant_not_a_match")  # the LIKE escapes '_'
        normal = _add_run(conn, "grid_only:abc")
        _run_migration(conn, _MIGRATION)
        _run_migration(conn, _MIGRATION)  # idempotent
        marked = {r[0] for r in conn.execute(text("SELECT backtest_run_id FROM astrogrid.backtest_run_invalidation"))}
        reasons = {r[0] for r in conn.execute(text("SELECT run_tag FROM astrogrid.backtest_run_invalidation"))}
    assert marked == set(rot)
    assert lookalike not in marked and normal not in marked
    assert reasons == {"mixed-basis-panel-20261110"}

    listed = AstroGridStore(pg_engine).list_backtest_results(limit=100)
    assert sorted(r["alpha_vs_benchmark"] for r in listed) == [0.4, 0.4]


def test_invalidation_marks_are_append_only(pg_engine: Engine) -> None:
    with pg_engine.begin() as conn:
        _add_run(conn, "rotation_variant_01_x")
        _run_migration(conn, _MIGRATION)
    for stmt in ("UPDATE astrogrid.backtest_run_invalidation SET reason = 'x'",
                 "DELETE FROM astrogrid.backtest_run_invalidation"):
        with pytest.raises(DBAPIError, match="append-only"), pg_engine.begin() as conn:
            conn.execute(text(stmt))
    blank_reason = """INSERT INTO astrogrid.backtest_run_invalidation (backtest_run_id, reason, run_tag)
                      SELECT id, ' ', 't' FROM astrogrid.backtest_run LIMIT 1"""
    with pytest.raises(IntegrityError), pg_engine.begin() as conn:
        conn.execute(text(blank_reason))
