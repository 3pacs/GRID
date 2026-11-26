"""
Shared pytest fixtures for the GRID test suite.

Provides mock database engine and mock PIT store fixtures so that tests
can run without a real PostgreSQL instance, plus the pytest-xdist
scheduling policy that keeps the suite correct when it runs in parallel.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, create_autospec

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from tests import ci_scheduling as _ci

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
# pytest-xdist scheduling and CI shards
# ---------------------------------------------------------------------------
# CI runs the suite as GRID_TEST_SHARD=i/n jobs, each with
# `-n auto --dist loadgroup`. The policy lives in tests/ci_scheduling.py:
#
#   * a shard keeps only the test files it owns (every file goes to exactly
#     one shard, so the shards together run the whole suite once);
#   * tests that reach a live PostgreSQL share one "postgres" unit and run one
#     after another on one worker -- they share a single database, write to
#     the public schema, and do not roll back per test, so two workers running
#     them at once would clobber each other's rows;
#   * every other file is one unit (`--dist loadfile` semantics: its tests stay
#     together, in order, on one worker, which keeps its module fixtures),
#     except the slow files in SPREAD_FILES, whose tests are spread one by one.
#
# The grouping is applied by a scheduler on the xdist controller, not by
# `xdist_group` markers. Markers stopped working with pytest 9.2.0: xdist
# 3.8.0 writes the group to `item._nodeid`, which pytest 9.2 no longer reads,
# so from 2026-11-05 every `loadgroup` run silently scheduled as plain `load`
# and the database tests ran on several workers at once. Explicit
# `xdist_group("postgres")` markers in test files still work: the source scan
# looks for them. See tests/ci_scheduling.py for the details.


def _is_xdist_loadgroup_worker(config) -> bool:
    # On a worker xdist resets `dist` to "no" and keeps the mode in
    # `option.loadgroup` (xdist/remote.py, setup_config).
    return hasattr(config, "workerinput") and bool(getattr(config.option, "loadgroup", False))


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config, items):
    """Keep this shard's files (GRID_TEST_SHARD) and put heavy units first under loadgroup."""
    shard = os.environ.get(_ci.SHARD_ENV, "").strip()
    if shard:
        try:
            index, total = _ci.parse_shard(shard)
        except ValueError as exc:
            raise pytest.UsageError(str(exc)) from exc
        counts: dict[str, int] = {}
        for item in items:
            module = _ci.module_of(item.nodeid)
            counts[module] = counts.get(module, 0) + 1
        db_files = {m for m in counts if _ci.module_touches_db(str(config.rootpath / m))}
        owner = _ci.assign_shards(_ci.file_weights(counts), db_files, total)
        keep = [item for item in items if owner[_ci.module_of(item.nodeid)] == index]
        dropped = [item for item in items if owner[_ci.module_of(item.nodeid)] != index]
        if dropped:
            config.hook.pytest_deselected(items=dropped)
            items[:] = keep

    if _is_xdist_loadgroup_worker(config):
        # Every worker sorts the same collection the same way, as xdist
        # requires. The controller dispatches units in this order.
        items.sort(key=lambda item: _ci.dispatch_rank(
            _ci.module_of(item.nodeid),
            _ci.module_touches_db(str(config.rootpath / _ci.module_of(item.nodeid))),
        ))


@pytest.hookimpl(optionalhook=True, tryfirst=True)
def pytest_xdist_make_scheduler(config, log):
    """`--dist loadgroup` uses GRID's units (tests/ci_scheduling.py); other modes are xdist's own."""
    if config.getvalue("dist") != "loadgroup":
        return None
    return _ci.make_scheduler(config, log)


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
    # api/routers/system.py's daily-audit reader (_read_daily_freshness_audit)
    # uses SELECT ... .scalar_one_or_none() to detect a missing/empty
    # data_freshness_audit table. Default to "no audit row" so callers that
    # don't care about that path (most users of this fixture) get the
    # honest "never configured" branch instead of a truthy MagicMock
    # blowing up arithmetic on a fake timestamp.
    mock_result.scalar_one_or_none.return_value = None
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


@pytest.fixture(autouse=True)
def _isolated_scheduler_run_dates(tmp_path, monkeypatch):
    """Keep ingestion.scheduler's persisted run dates out of the real home dir."""
    monkeypatch.setenv("GRID_SCHEDULER_LAST_RUN_PATH", str(tmp_path / "scheduler_last_run.json"))


@pytest.fixture(autouse=True)
def _isolated_gdelt_doc_gate(tmp_path, monkeypatch):
    """Give each test its own GDELT DOC gate file (ingestion/altdata/gdelt_doc.py).

    The real gate lives in /tmp and carries spacing and 429 cooldowns across
    processes; a shared file would make one test wait out another's state.
    """
    monkeypatch.setenv("GRID_GDELT_DOC_LOCK", str(tmp_path / "gdelt-doc.lock"))


@pytest.fixture(autouse=True)
def _options_provider_offline_default(monkeypatch):
    """Pin OptionsPuller.pull_all to its Yahoo client, which tests replace.

    Production defaults to Cboe delayed chains (ingestion/options.py); tests
    that exercise the Cboe path opt in by deleting this variable.
    """
    monkeypatch.setenv("GRID_OPTIONS_PRIMARY_PROVIDER", "yahoo")
    # The dedicated options timer hand-off (ingestion/options.py) is opt-in per test.
    monkeypatch.delenv("GRID_OPTIONS_DEDICATED_TIMER", raising=False)


@pytest.fixture(autouse=True)
def _isolated_options_tail_cursor(tmp_path, monkeypatch):
    """Keep schwab-mode tail cursors (ingestion/options_schwab.py) out of the operator's state directory."""
    monkeypatch.setenv("GRID_OPTIONS_TAIL_CURSOR_DIR", str(tmp_path / "options-tail-cursor"))
