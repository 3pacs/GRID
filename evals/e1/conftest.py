"""Fixtures for the E1 gates.

``pg_scratch`` is a throwaway PostgreSQL schema with a random name. It holds
:data:`evals.e1.world.PG_DDL` and is dropped after the test.

The URL comes only from ``GRID_TEST_DB_URL``, the same variable every
``*_pg.py`` contract step in CI uses. There is **no default**: a default of
``grid_user@localhost/grid`` would point at the production database on
grid-svr.

* ``GRID_TEST_DB_URL`` unset, or PostgreSQL unreachable: the test is
  skipped, unless ``E1_REQUIRE_PG=1`` (set by the CI step), in which case it
  fails. A PIT gate that silently skipped would be a false green.
* The URL names a production database (:mod:`evals.e1.pg_safety`): the test
  always fails, before any connection is opened.
"""

from __future__ import annotations

import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from evals.e1.pg_safety import ProductionDatabaseRefused, check_scratch_target
from evals.e1.world import PG_DDL, seed_sources


@pytest.fixture()
def pg_scratch():
    require = os.environ.get("E1_REQUIRE_PG") == "1"
    url = os.environ.get("GRID_TEST_DB_URL", "").strip()
    if not url:
        if require:
            pytest.fail("E1_REQUIRE_PG=1 but GRID_TEST_DB_URL is unset")
        pytest.skip("GRID_TEST_DB_URL unset (set E1_REQUIRE_PG=1 to make this a failure)")
    try:
        check_scratch_target(url)
    except ProductionDatabaseRefused as exc:
        pytest.fail(str(exc))
    try:
        admin = create_engine(url, pool_pre_ping=True)
        with admin.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:  # pragma: no cover - environment dependent
        if require:
            pytest.fail(f"E1_REQUIRE_PG=1 but PostgreSQL is unreachable: {type(exc).__name__}")
        pytest.skip("PostgreSQL not available (set E1_REQUIRE_PG=1 to make this a failure)")
    schema = f"e1_gate_{uuid4().hex[:12]}"
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    # griddb runs Etc/UTC; pin it so naive fixture timestamps mean UTC here too.
    engine = create_engine(admin.url, connect_args={"options": f"-csearch_path={schema} -ctimezone=UTC"})
    try:
        with engine.begin() as conn:
            for ddl in PG_DDL:
                conn.execute(text(ddl))
        seed_sources(engine)
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        admin.dispose()
