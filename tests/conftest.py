"""
Shared pytest fixtures for the GRID test suite.

Provides mock database engine and mock PIT store fixtures so that tests
can run without a real PostgreSQL instance, plus the pytest-xdist
scheduling policy that keeps the suite correct when it runs in parallel.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from unittest.mock import MagicMock, create_autospec

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Mirrors the DB_* defaults in config.Settings (config.py:46-50), so a plain
# local `pytest` behaves exactly as it did before this was configurable.
# Point the suite at a throwaway database by exporting GRID_TEST_DB_URL.
#
# Note for CI: test.yml sets a DB_URL env var, but config.Settings.DB_URL is a
# read-only @property computed from DB_USER/DB_PASSWORD/DB_HOST/DB_PORT/DB_NAME,
# so pydantic-settings cannot populate it from the environment and that variable
# has no effect. Wiring CI's Postgres through to the suite is deliberately left
# out of this change — it would switch on ~91 integration tests that have never
# run in CI, and that deserves its own red/green.
_DEFAULT_DB_URL = "postgresql://grid_user:changeme@localhost:5432/grid"


def _db_url() -> str:
    """URL for the live test database, overridable via GRID_TEST_DB_URL."""
    return os.environ.get("GRID_TEST_DB_URL", _DEFAULT_DB_URL)


# ---------------------------------------------------------------------------
# pytest-xdist scheduling
# ---------------------------------------------------------------------------
# CI runs the suite with `-n auto --dist loadgroup`. Under `loadgroup` every
# test carrying an `xdist_group(name)` marker is routed to the worker that owns
# that group, so the grouping below is what decides which tests may run
# concurrently:
#
#   * Tests that reach a live PostgreSQL all share the single "postgres" group
#     and therefore run one after another on one worker. They share a single
#     database, write to the public schema, and do not roll back per test, so
#     two workers running them at once would clobber each other's rows.
#     Serialising them keeps their behaviour identical to the old
#     single-process run, and it costs almost nothing: ~188 of ~7,400 tests.
#
#   * Every other test is grouped by its own file, which reproduces
#     `--dist loadfile` semantics — a module's tests stay together, in order,
#     on one worker. That preserves the four module-scoped fixtures in the
#     suite and whatever incidental intra-file ordering it has grown, which
#     keeps the parallel run as close to the serial run as scheduling allows.
#
# Membership is detected by scanning the module source rather than from a
# hand-maintained list, so a new database test file is picked up on its own.
# A false positive only costs a little parallelism, which is the right
# direction for this to fail in.
#
# The hook below MUST stay `tryfirst=True`. xdist encodes the group by
# appending "@<group>" to each item's nodeid from its own
# pytest_collection_modifyitems inside the worker (xdist/remote.py), and that
# implementation runs before an undecorated conftest hook. Without tryfirst the
# markers are added too late to be seen, no suffix is written, and every group
# silently degrades to plain `--dist load` scheduling — the database tests
# scatter across workers with no error and no warning. Verified against
# pytest-xdist 3.8.0: without tryfirst six database files landed on four
# different workers; with it, all six land on one.
_DB_TOKENS = ("pg_engine", "live_engine", "get_engine(", "get_connection(")


@lru_cache(maxsize=None)
def _module_touches_db(path: str) -> bool:
    """True if the test module at ``path`` can reach a live database."""
    if not path:
        return False
    try:
        source = Path(path).read_text(encoding="utf-8")
    except OSError:
        # Unreadable module: assume it might touch the database and let it
        # serialise rather than risk a parallel write collision.
        return True
    return any(token in source for token in _DB_TOKENS)


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config, items):
    """Tag every test with the xdist group described above."""
    if not config.pluginmanager.hasplugin("xdist"):
        return

    for item in items:
        # nodeid prefix ("tests/test_foo.py") keeps the "@group" suffix that
        # xdist appends short and readable; item.path is the absolute path the
        # source scan needs.
        module_id = item.nodeid.split("::")[0]
        group = "postgres" if _module_touches_db(str(getattr(item, "path", "") or "")) else module_id
        item.add_marker(pytest.mark.xdist_group(group))


@pytest.fixture
def pg_engine():
    """Return a SQLAlchemy engine connected to the test PostgreSQL database.

    Skips the test if PostgreSQL is not available.
    """
    try:
        engine = create_engine(_db_url(), pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception:
        pytest.skip("PostgreSQL not available")

    yield engine

    engine.dispose()


@pytest.fixture
def mock_engine():
    """Return a mock SQLAlchemy Engine.

    The engine supports ``.connect()`` and ``.begin()`` context managers
    that yield a mock connection with an ``.execute()`` method.
    """
    engine = create_autospec(Engine, instance=True)

    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.fetchone.return_value = None
    mock_result.fetchall.return_value = []
    mock_conn.execute.return_value = mock_result

    # context manager for engine.connect()
    engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    # context manager for engine.begin()
    engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    return engine


@pytest.fixture
def mock_pit_store():
    """Return a mock PITStore with sensible defaults.

    ``get_pit()`` returns an empty DataFrame.
    ``get_latest_values()`` returns an empty DataFrame.
    """
    pit = MagicMock()
    pit.get_pit.return_value = pd.DataFrame(
        columns=["feature_id", "obs_date", "value"]
    )
    pit.get_latest_values.return_value = pd.DataFrame(
        columns=["feature_id", "obs_date", "value"]
    )
    return pit
