"""Real-PostgreSQL proof: equity_put_call_ratio on a fresh database (#1397).

Codex review 6026828927 held two writer-side findings on the Cboe equity
put/call series, and this file runs both on PostgreSQL:

* finding 2 (clean install): the first registry revision dated its row from
  ``put_call_ratio``'s first raw row through a CROSS JOIN, so on a fresh
  database it inserted nothing, Alembic never replayed it, and the series
  the puller then wrote was a ghost mapping (every row dropped by the
  resolver). Here the database is fresh: schema.sql's tables and seed rows,
  no raw_series rows. The revision runs, then the first pull, then the
  resolver, which must resolve the equity ratio with no mapped-but-
  unregistered feature.
* finding 1 (clock): raw_series.pull_timestamp is DEFAULT NOW(), the insert
  transaction's start. Every stored row's stamp must be at or after the
  database clock when its page was read.

Throwaway schema per test, dropped afterwards. Skips without
``GRID_TEST_DB_URL``; the CI step "Cboe equity put/call fresh-install
PostgreSQL contract" fails on any skip.
"""

from __future__ import annotations

import importlib
import json
import os
import re
from datetime import date
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

from ingestion.altdata import cboe_indices
from ingestion.altdata.cboe_indices import (
    EQUITY_PUT_CALL_SERIES_ID,
    PUT_CALL_SERIES_ID,
    CBOEIndicesPuller,
)

pytestmark = pytest.mark.xdist_group("postgres")

REPO = Path(__file__).resolve().parents[1]
REAL_PAGE = REPO / "tests" / "fixtures" / "cboe" / "daily_options_2026-11-20.json"
_MIGRATION = "migrations.versions.cboe_equity_pcr_20261123"
_SESSION = date(2026, 11, 20)  # the fixture page's session (a Friday)


class _Today(date):
    @classmethod
    def today(cls) -> date:
        return cls(2026, 11, 23)  # Monday: the run reads Friday's page; Monday's is not up yet


def _schema_sql() -> list[str]:
    """A fresh database as schema.sql builds it: the tables these steps touch and both seeds."""
    schema = (REPO / "schema.sql").read_text(encoding="utf-8")
    out = []
    for table in ("source_catalog", "raw_series", "feature_registry", "resolved_series"):
        m = re.search(rf"CREATE TABLE IF NOT EXISTS {table} \(.*?\n\);", schema, re.DOTALL)
        assert m, f"schema.sql has no {table} table"
        out.append(m.group(0))
    for index in ("uq_raw_series_composite", "uq_resolved_series_composite"):
        m = re.search(rf"CREATE UNIQUE INDEX IF NOT EXISTS {index}\s+ON [^;]+;", schema)
        assert m, f"schema.sql has no {index}"
        out.append(m.group(0))
    for table in ("source_catalog", "feature_registry"):
        m = re.search(rf"INSERT INTO {table} \(.*?ON CONFLICT \(name\) DO NOTHING;", schema, re.DOTALL)
        assert m, f"schema.sql has no {table} seed"
        out.append(m.group(0))
    return out


@pytest.fixture
def pg_engine() -> Engine:
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        pytest.skip("GRID_TEST_DB_URL is required for the disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("the equity put/call fresh-install proof requires a local disposable test database")

    schema = "cboe_epcr_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema} -ctimezone=UTC"})
    try:
        with engine.begin() as conn:
            for statement in _schema_sql():
                conn.execute(text(statement))
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        admin.dispose()


def _upgrade(engine: Engine) -> None:
    """Run the revision's upgrade() on ``engine``, as alembic would."""
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


def _registry(engine: Engine, name: str) -> list[Any]:
    with engine.connect() as conn:
        return conn.execute(text(
            "SELECT family, normalization, lag_days, eligible_from_date, model_eligible "
            "FROM feature_registry WHERE name = :n"
        ), {"n": name}).fetchall()


def test_fresh_upgrade_first_pull_and_resolve_leave_no_ghost_mapping(pg_engine, monkeypatch) -> None:
    from normalization.resolver import Resolver

    # A fresh database: put_call_ratio is a schema.sql seed, nothing is pulled
    # yet (the case where the first version of the revision inserted nothing).
    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM raw_series")).scalar_one() == 0
    assert _registry(pg_engine, PUT_CALL_SERIES_ID)
    assert _registry(pg_engine, EQUITY_PUT_CALL_SERIES_ID) == []

    _upgrade(pg_engine)
    _upgrade(pg_engine)  # idempotent

    assert [tuple(r) for r in _registry(pg_engine, EQUITY_PUT_CALL_SERIES_ID)] == [
        ("vol", "RAW", 0, date(2026, 11, 20), False),
    ]

    # First pull: Friday's page from the fixture (the real 2026-11-20 page),
    # no page yet for Monday. The database clock is read as each page is read.
    monkeypatch.setattr(cboe_indices, "date", _Today)
    monkeypatch.setattr(cboe_indices.time, "sleep", lambda *_a: None)
    page = json.loads(REAL_PAGE.read_text(encoding="utf-8"))
    read_at: list[Any] = []

    def fake_json(url: str) -> dict[str, Any] | None:
        with pg_engine.connect() as conn:
            read_at.append(conn.execute(text("SELECT clock_timestamp()")).scalar_one())
        return page if url.rsplit("/", 1)[1].startswith(_SESSION.isoformat()) else None

    puller = CBOEIndicesPuller(pg_engine)
    monkeypatch.setattr(puller, "_download_json", fake_json)
    out = {r["feature_name"]: r for r in puller.pull_put_call_ratio(days_back=3)}

    assert out == {
        PUT_CALL_SERIES_ID: {"status": "SUCCESS", "rows_inserted": 1, "feature_name": PUT_CALL_SERIES_ID},
        EQUITY_PUT_CALL_SERIES_ID: {"status": "SUCCESS", "rows_inserted": 1, "feature_name": EQUITY_PUT_CALL_SERIES_ID},
    }
    assert len(read_at) == 2  # Friday and Monday; the weekend is never requested
    with pg_engine.connect() as conn:
        raw = conn.execute(text(
            "SELECT r.series_id, r.obs_date, r.value, r.pull_status, r.pull_timestamp, sc.name "
            "FROM raw_series r JOIN source_catalog sc ON sc.id = r.source_id ORDER BY r.series_id"
        )).fetchall()
    assert [(r.series_id, r.obs_date, r.value, r.pull_status, r.name) for r in raw] == [
        (EQUITY_PUT_CALL_SERIES_ID, _SESSION, 0.77, "SUCCESS", "CBOE"),
        (PUT_CALL_SERIES_ID, _SESSION, 1.13, "SUCCESS", "CBOE"),
    ]
    # Finding 1: no row is stamped before the last page was read.
    assert all(r.pull_timestamp >= max(read_at) for r in raw), (raw, read_at)

    # Resolve: both ratios reach resolved_series; nothing is mapped but unregistered.
    summary = Resolver(pg_engine).resolve_pending(workers=1, lookback_days=3)
    assert summary["errors"] == 0
    assert summary["unmapped"].get("mapped_unregistered_count", 0) == 0, summary["unmapped"]
    assert EQUITY_PUT_CALL_SERIES_ID not in summary["unmapped"]["unregistered_features"]
    with pg_engine.connect() as conn:
        resolved = conn.execute(text(
            "SELECT fr.name, rs.obs_date, rs.value FROM resolved_series rs "
            "JOIN feature_registry fr ON fr.id = rs.feature_id ORDER BY fr.name"
        )).fetchall()
    assert [tuple(r) for r in resolved] == [
        (EQUITY_PUT_CALL_SERIES_ID, _SESSION, 0.77),
        (PUT_CALL_SERIES_ID, _SESSION, 1.13),
    ]
