"""PostgreSQL-backed proof of the real autoresearch data-load path.

Every other test guarding scripts/autoresearch.py's subfamily -> signal_subtype
fix (tests/test_autoresearch_schema_contract.py,
tests/test_autoresearch_failure_visibility.py) is a static check or uses a
fake cursor that raises on ``execute`` — neither one ever runs the real SQL
scripts.autoresearch.py sends to a real PostgreSQL planner. This file does:
it opens a real psycopg2 connection (the same driver
``run_autoresearch()`` uses in production — see scripts/autoresearch.py's
``pg = psycopg2.connect(...)``), creates a disposable per-test schema, and
calls the real ``scripts.autoresearch._load_research_context(cur)`` (which
calls the real ``get_feature_list`` / ``get_feature_name_map`` /
``get_market_snapshot`` / ``_select_orthogonal_features``) against it.

Three scenarios:

1. Production-shaped table (no ``subfamily``; has ``signal_domain`` /
   ``signal_subtype``, confirmed live on griddb by a 2026-09-19 catalog
   query) — the real data-load path must succeed end to end.
2. Regression: the *old*, pre-fix query text (copied verbatim below) must
   raise ``psycopg2.errors.UndefinedColumn`` against that same
   production-shaped table — this is the literal production failure
   (data-load phase ``feature_list``, observed 2026-09-19 05:39Z and in
   May-2026 logs) that the fix in ``get_feature_list()`` resolves.
3. Fresh-install shape: a table built the way ``schema.sql`` had it before
   this slice (``subfamily`` present, no taxonomy columns), then
   ``migrations/versions/feature_signal_taxonomy_20260919.py``'s
   ``TAXONOMY_DDL`` applied twice (proving it is idempotent), after which
   the real data-load path must succeed too.

Hard boundary for this lane: no local PostgreSQL exists and this lane must
not connect to one. This file therefore never runs standalone — it always
goes through the ``pg_engine`` fixture (tests/conftest.py), which
``pytest.skip``s the whole file's tests cleanly whenever
``GRID_TEST_DB_URL`` is unset or unreachable. Run it against a disposable
database (never the shared alien griddb_test — see this session's memory on
disposable test-DB naming) with:

    GRID_TEST_DB_URL=postgresql://user:pass@host:port/scratchdb \\
    DB_PASSWORD=testpass PYTHONUTF8=1 \\
    python -m pytest tests/test_autoresearch_feature_list_pg.py -q

``GRID_TEST_DB_URL`` is also what routes this file into the serial
"postgres" xdist group (tests/conftest.py's ``_DB_TOKENS`` scan matches
``pg_engine`` in this module's source).
"""

from __future__ import annotations

import importlib.util
import math
import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path

import psycopg2
import psycopg2.errors
import psycopg2.sql
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

# config.Settings requires a non-empty DB_PASSWORD at import time (see
# config.py's _check_db_password validator) even though this file's actual
# queries never go through config.settings / db.get_engine — only through a
# psycopg2 connection built straight from GRID_TEST_DB_URL. Mirrors the same
# workaround tests/test_autoresearch_failure_visibility.py documents.
os.environ.setdefault("DB_PASSWORD", "testpass")

import scripts.autoresearch as autoresearch  # noqa: E402

MIGRATION_PATH = (
    REPO_ROOT / "migrations" / "versions" / "feature_signal_taxonomy_20260919.py"
)

# The exact pre-fix query text from scripts/autoresearch.py's get_feature_list,
# before this slice changed f.subfamily -> f.signal_subtype. Kept verbatim so
# scenario 2 reproduces the actual production failure, not a paraphrase of it.
OLD_PRE_FIX_QUERY = """
    SELECT f.id, f.name, f.family, COALESCE(f.subfamily, ''), f.description,
           COUNT(rs.id) as obs_count
    FROM feature_registry f
    JOIN resolved_series rs ON rs.feature_id = f.id
    WHERE f.id = ANY(%s)
      AND rs.obs_date >= CURRENT_DATE - INTERVAL '1 year'
    GROUP BY f.id, f.name, f.family, f.subfamily, f.description
    HAVING COUNT(rs.id) >= 30
    ORDER BY f.family, f.id
"""


def _load_taxonomy_ddl() -> tuple[str, ...]:
    """Load TAXONOMY_DDL from the new migration by file path.

    migrations/versions has no __init__.py (alembic loads revisions by path,
    not via normal import), so this mirrors
    tests/test_alembic_single_head.py::test_migration_warnings_are_not_swallowed's
    approach of loading a migrations/ file via importlib rather than assuming
    it's an importable package.
    """
    spec = importlib.util.spec_from_file_location(
        "feature_signal_taxonomy_20260919", MIGRATION_PATH
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.TAXONOMY_DDL


@pytest.fixture
def pg_conn_params(pg_engine) -> dict:
    """psycopg2 connect() kwargs for the database pg_engine already proved
    reachable (and pytest.skip'd cleanly if it wasn't). Every actual query
    in this file goes through a raw psycopg2 connection/cursor — the same
    driver scripts/autoresearch.py's run_autoresearch() uses in production
    — never through pg_engine's SQLAlchemy connection.
    """
    url = pg_engine.url
    return {
        "host": url.host,
        "port": url.port,
        "dbname": url.database,
        "user": url.username,
        "password": url.password,
    }


_schema_counter = 0


@contextmanager
def _scratch_schema(pg_conn_params: dict) -> Iterator[psycopg2.extensions.cursor]:
    """Open a psycopg2 connection, create a uniquely-named scratch schema,
    point search_path at it so the unqualified table names in the real
    autoresearch queries resolve there, yield a cursor, and always drop the
    schema (CASCADE) in a finally — regardless of whether the test body
    raised.

    Per this session's disposable-test-DB-naming rule: this creates its own
    throwaway schema rather than assuming exclusive use of whatever database
    GRID_TEST_DB_URL points at.
    """
    global _schema_counter
    _schema_counter += 1
    schema_name = f"ar_subtype_{os.getpid()}_{_schema_counter}"

    conn = psycopg2.connect(**pg_conn_params)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        ident = psycopg2.sql.Identifier(schema_name)
        cur.execute(psycopg2.sql.SQL("CREATE SCHEMA {}").format(ident))
        cur.execute(psycopg2.sql.SQL("SET search_path TO {}, public").format(ident))
        yield cur
    finally:
        try:
            cur.execute(
                psycopg2.sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    psycopg2.sql.Identifier(schema_name)
                )
            )
        finally:
            cur.close()
            conn.close()


def _create_production_shaped_tables(cur) -> None:
    """feature_registry as production actually has it (2026-09-19 catalog
    query): no subfamily; has signal_domain/signal_subtype. Only the
    columns the real queries in scripts/autoresearch.py touch are included
    (get_feature_list, get_feature_name_map, get_market_snapshot,
    _select_orthogonal_features)."""
    cur.execute(
        """
        CREATE TABLE feature_registry (
            id             SERIAL PRIMARY KEY,
            name           TEXT NOT NULL UNIQUE,
            family         TEXT NOT NULL,
            description    TEXT NOT NULL,
            model_eligible BOOLEAN NOT NULL DEFAULT FALSE,
            signal_domain  TEXT,
            signal_subtype TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE resolved_series (
            id         SERIAL PRIMARY KEY,
            feature_id INTEGER NOT NULL REFERENCES feature_registry(id),
            obs_date   DATE NOT NULL,
            value      DOUBLE PRECISION
        )
        """
    )


def _create_fresh_install_shaped_tables(cur) -> None:
    """feature_registry as schema.sql had it before this slice: subfamily
    present, no taxonomy columns."""
    cur.execute(
        """
        CREATE TABLE feature_registry (
            id             SERIAL PRIMARY KEY,
            name           TEXT NOT NULL UNIQUE,
            family         TEXT NOT NULL,
            subfamily      TEXT,
            description    TEXT NOT NULL,
            model_eligible BOOLEAN NOT NULL DEFAULT FALSE
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE resolved_series (
            id         SERIAL PRIMARY KEY,
            feature_id INTEGER NOT NULL REFERENCES feature_registry(id),
            obs_date   DATE NOT NULL,
            value      DOUBLE PRECISION
        )
        """
    )


# Three deterministic, pairwise-uncorrelated value patterns (verified offline:
# max |pearson| ~0.26 across all three pairs over 45 points) so
# _select_orthogonal_features's greedy elimination (corr_threshold=0.7) keeps
# all three rather than dropping one for looking too much like another.
def _series_value(pattern: str, day_offset: int) -> float:
    if pattern == "linear":
        return float(day_offset)
    if pattern == "sine":
        return 50.0 * math.sin(day_offset * 0.9)
    if pattern == "quad":
        return float((day_offset * day_offset) % 37)
    raise ValueError(pattern)  # pragma: no cover


_FEATURES = (
    ("ar_test_alpha", "rates", "linear"),
    ("ar_test_beta", "equity", "sine"),
    ("ar_test_gamma", "commodity", "quad"),
)
OBS_DAYS = 45  # >= the 30-obs HAVING threshold in get_feature_list/_select_orthogonal_features


def _seed_features_and_series(cur, *, use_taxonomy_columns: bool) -> list[int]:
    """Insert 3 model_eligible features (>=40 daily observations each,
    within the last year, non-perfectly-correlated) into whichever
    feature_registry shape the caller already created."""
    feature_ids: list[int] = []
    for name, family, _pattern in _FEATURES:
        if use_taxonomy_columns:
            cur.execute(
                "INSERT INTO feature_registry "
                "(name, family, description, model_eligible, signal_domain, signal_subtype) "
                "VALUES (%s, %s, %s, TRUE, %s, %s) RETURNING id",
                (name, family, f"{name} test feature", family.upper(), f"{name}_subtype"),
            )
        else:
            cur.execute(
                "INSERT INTO feature_registry "
                "(name, family, subfamily, description, model_eligible) "
                "VALUES (%s, %s, %s, %s, TRUE) RETURNING id",
                (name, family, None, f"{name} test feature"),
            )
        feature_ids.append(cur.fetchone()[0])

    today = date.today()
    for (name, _family, pattern), fid in zip(_FEATURES, feature_ids):
        for day_offset in range(OBS_DAYS):
            obs_date = today - timedelta(days=day_offset)
            value = _series_value(pattern, day_offset)
            cur.execute(
                "INSERT INTO resolved_series (feature_id, obs_date, value) VALUES (%s, %s, %s)",
                (fid, obs_date, value),
            )
    return feature_ids


@pytest.fixture(autouse=True)
def _reset_ortho_cache():
    """_select_orthogonal_features caches its result in the module-level
    autoresearch._ortho_cache global. Each scenario in this file uses a
    fresh schema with different data, so the cache from one test must never
    leak into the next.
    """
    autoresearch._ortho_cache = None
    yield
    autoresearch._ortho_cache = None


# ---------------------------------------------------------------------------
# Scenario 1: production-shaped table, real data-load path succeeds.
# ---------------------------------------------------------------------------


def test_real_data_load_path_succeeds_on_production_shaped_table(pg_conn_params):
    with _scratch_schema(pg_conn_params) as cur:
        _create_production_shaped_tables(cur)
        feature_ids = _seed_features_and_series(cur, use_taxonomy_columns=True)

        ctx = autoresearch._load_research_context(cur)

    # get_feature_name_map selects every model_eligible row regardless of
    # orthogonal selection, so all 3 seeded features must appear.
    assert set(ctx["feature_names"].keys()) == set(feature_ids)
    for (name, _family, _pattern), fid in zip(_FEATURES, feature_ids):
        assert ctx["feature_names"][fid] == name

    # get_feature_list depends on _select_orthogonal_features's greedy pick;
    # the three seeded series were chosen to be pairwise uncorrelated enough
    # that all three survive, but the assertions below only require "at
    # least one" to avoid coupling this test to the exact greedy order.
    assert ctx["feature_list"] != "(no features)"
    assert "obs]" in ctx["feature_list"]
    matched_family_subtype_label = any(
        f"({family}/{name}_subtype)" in ctx["feature_list"] for name, family, _ in _FEATURES
    )
    assert matched_family_subtype_label, (
        f"expected a 'family/signal_subtype' label in feature_list:\n{ctx['feature_list']}"
    )

    assert ctx["market_snapshot"] != "(no data)"
    assert any(name in ctx["market_snapshot"] for name, _family, _pattern in _FEATURES)


# ---------------------------------------------------------------------------
# Scenario 2: regression — the OLD pre-fix query fails on that same table.
# ---------------------------------------------------------------------------


def test_old_pre_fix_query_reproduces_the_production_failure(pg_conn_params):
    """Reproduces the actual production failure this slice fixes: the
    pre-fix `COALESCE(f.subfamily, '')` query raises UndefinedColumn against
    a production-shaped feature_registry, which has no subfamily column.
    """
    with _scratch_schema(pg_conn_params) as cur:
        _create_production_shaped_tables(cur)
        feature_ids = _seed_features_and_series(cur, use_taxonomy_columns=True)

        with pytest.raises(psycopg2.errors.UndefinedColumn):
            cur.execute(OLD_PRE_FIX_QUERY, (feature_ids,))


# ---------------------------------------------------------------------------
# Scenario 3: fresh-install shape + idempotent TAXONOMY_DDL -> real path works.
# ---------------------------------------------------------------------------


def test_taxonomy_migration_applied_twice_then_real_data_load_succeeds(pg_conn_params):
    taxonomy_ddl = _load_taxonomy_ddl()

    with _scratch_schema(pg_conn_params) as cur:
        _create_fresh_install_shaped_tables(cur)

        # Apply twice: proves TAXONOMY_DDL is idempotent (ADD COLUMN IF NOT
        # EXISTS / CREATE INDEX IF NOT EXISTS), exactly as it will be safe to
        # run on griddb where scripts/signal_taxonomy.py already applied the
        # same DDL by hand.
        for _ in range(2):
            for statement in taxonomy_ddl:
                cur.execute(statement)

        feature_ids = _seed_features_and_series(cur, use_taxonomy_columns=True)

        ctx = autoresearch._load_research_context(cur)

    assert set(ctx["feature_names"].keys()) == set(feature_ids)
    assert ctx["feature_list"] != "(no features)"
    assert ctx["market_snapshot"] != "(no data)"
