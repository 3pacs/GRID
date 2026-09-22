"""Real PostgreSQL proof of the versioned SPY close receipt contract.

The test database must be explicitly named by GRID_TEST_DB_URL and local to
the runner. CI runs this module against its disposable PostgreSQL service.
"""

from __future__ import annotations

import importlib
import json
import os
from datetime import date, datetime, timezone
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import IntegrityError

from normalization.resolver import _resolve_spy_close_receipt
from price_close_contract import capture_payload
from store.astrogrid import AstroGridStore


@pytest.fixture
def receipt_pg_engine() -> Engine:
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        pytest.skip("GRID_TEST_DB_URL is required for disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("SPY receipt tests require a local disposable test database")

    schema = "spy_receipt_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS astrogrid"))
        # Only the named disposable test database may be used here.
        conn.execute(text("DROP TABLE IF EXISTS astrogrid.price_close_receipt"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema},public"})
    migration = importlib.import_module("migrations.versions.spy_close_receipt_20260922")
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE source_catalog (id INTEGER PRIMARY KEY, name TEXT NOT NULL, priority_rank INTEGER NOT NULL)"))
            conn.execute(text("CREATE TABLE feature_registry (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)"))
            conn.execute(text("""
                CREATE TABLE raw_series (
                    id BIGSERIAL PRIMARY KEY, series_id TEXT NOT NULL,
                    source_id INTEGER NOT NULL REFERENCES source_catalog(id),
                    obs_date DATE NOT NULL, pull_timestamp TIMESTAMPTZ NOT NULL,
                    value DOUBLE PRECISION NOT NULL, raw_payload JSONB,
                    pull_status TEXT NOT NULL)
            """))
            conn.execute(text("""
                CREATE TABLE resolved_series (
                    id BIGSERIAL PRIMARY KEY,
                    feature_id INTEGER NOT NULL REFERENCES feature_registry(id),
                    obs_date DATE NOT NULL, release_date DATE NOT NULL,
                    vintage_date DATE NOT NULL, value DOUBLE PRECISION NOT NULL,
                    source_priority_used INTEGER NOT NULL REFERENCES source_catalog(id),
                    conflict_flag BOOLEAN NOT NULL DEFAULT FALSE,
                    UNIQUE (feature_id, obs_date, vintage_date))
            """))
            conn.execute(text("INSERT INTO source_catalog VALUES (1, 'yfinance', 1)"))
            conn.execute(text("INSERT INTO feature_registry VALUES (2791, 'spy_full')"))
            ops = Operations(MigrationContext.configure(conn))
            original_op = migration.op
            migration.op = ops
            try:
                migration.upgrade()
            finally:
                migration.op = original_op
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS astrogrid.price_close_receipt"))
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        admin.dispose()


def _raw(engine: Engine, obs_date: date, pulled_at: datetime, price: float,
         payload: dict | None) -> int:
    with engine.begin() as conn:
        return conn.execute(text("""
            INSERT INTO raw_series
                (series_id, source_id, obs_date, pull_timestamp, value,
                 raw_payload, pull_status)
            VALUES ('YF:SPY:close', 1, :od, :ts, :val,
                    CAST(:payload AS jsonb), 'SUCCESS')
            RETURNING id
        """), {"od": obs_date, "ts": pulled_at, "val": price,
                "payload": json.dumps(payload) if payload else None}).scalar_one()


def _source(raw_id: int, pulled_at: datetime, price: float, payload: dict | None) -> dict:
    return {"source_name": "yfinance", "source_id": 1, "raw_id": raw_id,
            "pull_timestamp": pulled_at, "value": price, "raw_payload": payload}


def test_provisional_transition_exact_lineage_atomicity_and_pit(receipt_pg_engine: Engine) -> None:
    engine = receipt_pg_engine
    obs = date(2026, 9, 22)
    provisional_ts = datetime(2026, 9, 22, 13, 33, tzinfo=timezone.utc)
    completed_ts = datetime(2026, 9, 23, 0, 30, tzinfo=timezone.utc)
    raw_provisional = _raw(engine, obs, provisional_ts, 680.0, None)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO resolved_series
                (feature_id, obs_date, release_date, vintage_date, value,
                 source_priority_used)
            VALUES (2791, :od, :od, :od, 680.0, 1)
        """), {"od": obs})
    assert _resolve_spy_close_receipt(
        engine, 2791, obs, [_source(raw_provisional, provisional_ts, 680.0, None)]
    ) == 0

    marker = capture_payload(obs, completed_ts)
    assert marker is not None and marker["provider_certified_final"] is False
    raw_completed = _raw(engine, obs, completed_ts, 685.0, marker)
    assert _resolve_spy_close_receipt(
        engine, 2791, obs, [_source(raw_completed, completed_ts, 685.0, marker)]
    ) == 1
    with engine.connect() as conn:
        receipts = conn.execute(text("""
            SELECT pc.raw_series_id, pc.resolved_series_id, rs.value,
                   pc.available_at, rs.vintage_date
            FROM astrogrid.price_close_receipt pc
            JOIN resolved_series rs ON rs.id = pc.resolved_series_id
        """)).fetchall()
        assert len(receipts) == 1
        assert receipts[0][0] == raw_completed
        assert receipts[0][2] == 685.0
        assert receipts[0][3] == completed_ts
        assert receipts[0][4] == date(2026, 9, 23)
        store = AstroGridStore(engine)
        assert store._verified_spy_receipt(
            conn, cutoff=datetime(2026, 9, 23, 0, 10, tzinfo=timezone.utc), mode="entry"
        ) is None
        verified = store._verified_spy_receipt(
            conn, cutoff=datetime(2026, 9, 24, tzinfo=timezone.utc), mode="entry"
        )
        assert verified and verified["raw_series_id"] == raw_completed
        assert verified["resolved_series_id"] == receipts[0][1]

    # A later marked raw revision cannot mutate the pinned receipt. Its
    # resolved insert must roll back when the receipt's date uniqueness hits.
    later_ts = datetime(2026, 9, 24, 0, 30, tzinfo=timezone.utc)
    later_marker = capture_payload(obs, later_ts)
    raw_later = _raw(engine, obs, later_ts, 686.0, later_marker)
    with pytest.raises(IntegrityError):
        _resolve_spy_close_receipt(
            engine, 2791, obs, [_source(raw_later, later_ts, 686.0, later_marker)]
        )
    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM resolved_series")).scalar_one() == 2
        assert conn.execute(text("SELECT count(*) FROM astrogrid.price_close_receipt")).scalar_one() == 1


def test_existing_vintage_collision_stays_unverified(receipt_pg_engine: Engine) -> None:
    engine = receipt_pg_engine
    obs = date(2026, 9, 22)
    completed_ts = datetime(2026, 9, 23, 0, 30, tzinfo=timezone.utc)
    marker = capture_payload(obs, completed_ts)
    raw_id = _raw(engine, obs, completed_ts, 685.0, marker)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO resolved_series
                (feature_id, obs_date, release_date, vintage_date, value,
                 source_priority_used)
            VALUES (2791, :od, :vd, :vd, 685.0, 1)
        """), {"od": obs, "vd": completed_ts.date()})
    assert _resolve_spy_close_receipt(
        engine, 2791, obs, [_source(raw_id, completed_ts, 685.0, marker)]
    ) == 0
    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM astrogrid.price_close_receipt")).scalar_one() == 0
