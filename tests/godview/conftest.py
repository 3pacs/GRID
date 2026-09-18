"""Fixtures for the DB-gated godview tests.

Per the W6 assignment: a disposable PostgreSQL is available ONLY through
``GRID_TEST_DB_URL`` (a uniquely-named scratch DB on the CI host, reached
through a tunnel, supplied by the lead). Until that arrives, every test that
needs it must skip with the reason "GRID_TEST_DB_URL not set" -- this is
deliberately a DIFFERENT skip reason than the shared ``pg_engine`` fixture in
tests/conftest.py (which skips with "PostgreSQL not available" whenever
*any* Postgres, including a stray local default, isn't reachable). That
distinction is the point: a reader of `pytest -rs` output should be able to
tell "nobody has given me a scratch DB yet" apart from "a DB was configured
but is unreachable."

Bootstrap mirrors the existing DB-test convention referenced in the task:
schema.sql, then `alembic stamp` to the schema.sql baseline, then
`alembic upgrade head` -- which runs the full chain including
god_view_market_tables_20260918 (creates cftc_positioning_daily),
signal_evaluations_0918, promotion_ledger_0918, and this lane's own
godview_pit_cftc_0918.

NEVER creates any database other than the one named by GRID_TEST_DB_URL, and
never touches a table this lane did not create except the CFTC God View
ones from the tracked migration -- see the module docstring in
godview/cftc_pillar.py for what "touches" means here (INSERT only).
"""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlsplit

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text

REPO_ROOT = Path(__file__).resolve().parents[2]
BASELINE_REVISION = "7e4dfecce247"


def _require_test_db_url() -> str:
    db_url = os.environ.get("GRID_TEST_DB_URL")
    if not db_url:
        pytest.skip("GRID_TEST_DB_URL not set")
    return db_url


def _set_settings_env_from_url(monkeypatch: pytest.MonkeyPatch, db_url: str) -> None:
    """Point config.Settings().DB_URL (and therefore migrations/env.py) at db_url.

    migrations/env.py unconditionally overwrites the Alembic Config's
    sqlalchemy.url with a freshly-constructed ``Settings().DB_URL`` -- so the
    only way to steer `alembic upgrade` at a specific scratch DB is via the
    env vars pydantic-settings reads for DB_HOST/DB_PORT/DB_NAME/DB_USER/
    DB_PASSWORD, not via Config.set_main_option.
    """
    parts = urlsplit(db_url)
    monkeypatch.setenv("DB_HOST", parts.hostname or "localhost")
    monkeypatch.setenv("DB_PORT", str(parts.port or 5432))
    monkeypatch.setenv("DB_NAME", (parts.path or "/").lstrip("/"))
    monkeypatch.setenv("DB_USER", parts.username or "")
    monkeypatch.setenv("DB_PASSWORD", parts.password or "")
    monkeypatch.setenv("ENVIRONMENT", "development")


@pytest.fixture
def godview_pg_engine(monkeypatch: pytest.MonkeyPatch):
    """A scratch-DB engine, bootstrapped to alembic head, or a skip.

    Bootstraps once per call (idempotent: schema.sql uses CREATE TABLE IF
    NOT EXISTS throughout, and `alembic upgrade head` no-ops if already
    there), so it is safe to depend on this from every test in this
    directory without worrying about ordering.
    """
    db_url = _require_test_db_url()
    _set_settings_env_from_url(monkeypatch, db_url)

    engine = create_engine(db_url, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"GRID_TEST_DB_URL set but unreachable: {exc}")

    with engine.begin() as conn:
        has_alembic = conn.execute(text("SELECT to_regclass('alembic_version')")).scalar()

    if not has_alembic:
        schema_sql = (REPO_ROOT / "schema.sql").read_text(encoding="utf-8")
        with engine.begin() as conn:
            conn.execute(text(schema_sql))

        alembic_cfg = Config(str(REPO_ROOT / "alembic.ini"))
        alembic_cfg.set_main_option("script_location", str(REPO_ROOT / "migrations"))
        command.stamp(alembic_cfg, BASELINE_REVISION)

    alembic_cfg = Config(str(REPO_ROOT / "alembic.ini"))
    alembic_cfg.set_main_option("script_location", str(REPO_ROOT / "migrations"))
    command.upgrade(alembic_cfg, "head")

    yield engine
    engine.dispose()
