"""Fixtures for the E1 gates.

``pg_scratch``: a throwaway PostgreSQL schema (random name, dropped after the
test) holding :data:`evals.e1.world.PG_DDL`. The URL comes from
``GRID_TEST_DB_URL`` (the same variable every ``*_pg.py`` contract step in CI
uses). Without a reachable PostgreSQL the test is skipped -- unless
``E1_REQUIRE_PG=1`` (set by the CI step), in which case it fails: a PIT gate
that silently skipped would be a false green.
"""

from __future__ import annotations

import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from evals.e1.world import PG_DDL, seed_sources

_DEFAULT_DB_URL = "postgresql://grid_user:changeme@localhost:5432/grid"


def _pg_url() -> str:
    return os.environ.get("GRID_TEST_DB_URL", _DEFAULT_DB_URL)


@pytest.fixture()
def pg_scratch():
    require = os.environ.get("E1_REQUIRE_PG") == "1"
    try:
        admin = create_engine(_pg_url(), pool_pre_ping=True)
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
