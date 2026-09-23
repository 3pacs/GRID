"""Real-PostgreSQL proof that #606's SPY close-receipt policy and Binance
canonical-close policy resolve correctly TOGETHER, in one
``Resolver.resolve_pending()`` call.

Every existing test for these two policies (``tests/test_spy_price_receipt_pg.py``,
``tests/test_binance_canonical_close_pg.py``) exercises them separately, each
against its own isolated database populated with only that policy's own
source/feature rows. That leaves the actual COMPOSITION question -- the one
#606 exists to answer -- unverified end-to-end: ``Resolver._resolve_partition``
computes ``spy_feature_id`` once per partition and then, inside a single
shared per-``(series_id, obs_date)`` loop, branches into either the SPY
receipt path or the Binance canonical-close filter depending on which
``series_id`` is current. With ``workers=1`` every distinct series_id lands
in the same partition and the same loop, so a live resolve cycle spanning
both a due SPY close and a due Binance close runs both branches back to
back on one connection, inside one transaction per flush. This file proves
that actually works: both resolve correctly, neither one's branch leaks
into or suppresses the other's.

Development-only, dev-only fixture. No production changes. DDL below
combines exactly the same table shapes each policy's own dedicated test
file already uses (not guessed) -- and, like the SPY file's own fixture,
runs the real ``spy_close_receipt_20260922`` migration module for
``astrogrid.price_close_receipt`` rather than hand-copying its DDL.
"""

from __future__ import annotations

import importlib
import json
import os
from datetime import date, datetime, time, timedelta, timezone
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

from binance_close_contract import CANONICAL_CLOSE_SERIES
from normalization.entity_map import EntityMap
from normalization.resolver import Resolver
from price_close_contract import SPY_CLOSE_SERIES, capture_payload


@pytest.fixture
def composed_pg_engine() -> Engine:
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        pytest.skip("GRID_TEST_DB_URL is required for disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("Composition proof requires a local disposable test database")

    schema = "spy_binance_composed_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
        conn.execute(text("DROP SCHEMA IF EXISTS astrogrid CASCADE"))
        conn.execute(text("CREATE SCHEMA astrogrid"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema},public"})
    migration = importlib.import_module("migrations.versions.spy_close_receipt_20260922")
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE source_catalog (
                    id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE,
                    priority_rank INTEGER NOT NULL)
            """))
            conn.execute(text("""
                CREATE TABLE feature_registry (
                    id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, family TEXT NOT NULL)
            """))
            conn.execute(text("""
                CREATE TABLE raw_series (
                    id BIGSERIAL PRIMARY KEY, series_id TEXT NOT NULL,
                    source_id INTEGER NOT NULL REFERENCES source_catalog(id),
                    obs_date DATE NOT NULL, pull_timestamp TIMESTAMPTZ NOT NULL,
                    value DOUBLE PRECISION NOT NULL, raw_payload JSONB,
                    pull_status TEXT NOT NULL)
            """))
            # conflict_detail is required: the generic _flush_batch path
            # (used for Binance's resolution here) inserts it; the SPY-only
            # test fixture this was copied from never exercises that path
            # (it calls _resolve_spy_close_receipt directly), which is why
            # its own resolved_series shape gets away without this column.
            conn.execute(text("""
                CREATE TABLE resolved_series (
                    id BIGSERIAL PRIMARY KEY,
                    feature_id INTEGER NOT NULL REFERENCES feature_registry(id),
                    obs_date DATE NOT NULL, release_date DATE NOT NULL,
                    vintage_date DATE NOT NULL, value DOUBLE PRECISION NOT NULL,
                    source_priority_used INTEGER NOT NULL REFERENCES source_catalog(id),
                    conflict_flag BOOLEAN NOT NULL DEFAULT FALSE,
                    conflict_detail TEXT,
                    UNIQUE (feature_id, obs_date, vintage_date))
            """))
            # yfinance outranks binance here only because each already does
            # in the real source_catalog -- irrelevant to this test, since
            # spy_full and btc_full never compete for the same feature_id.
            conn.execute(text("""
                INSERT INTO source_catalog (id, name, priority_rank) VALUES
                    (1, 'yfinance', 1), (184, 'binance', 25)
            """))
            conn.execute(text("""
                INSERT INTO feature_registry (id, name, family) VALUES
                    (2791, 'spy_full', 'equity'), (101, 'btc_full', 'crypto')
            """))
            conn.execute(text("""
                CREATE TABLE astrogrid.prediction_run (
                    id BIGSERIAL PRIMARY KEY, prediction_id TEXT NOT NULL UNIQUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    as_of_ts TIMESTAMPTZ NOT NULL, horizon_label TEXT NOT NULL,
                    target_universe TEXT NOT NULL, scoring_class TEXT NOT NULL,
                    target_symbols JSONB NOT NULL, question TEXT NOT NULL,
                    call TEXT NOT NULL, timing TEXT NOT NULL, setup TEXT NOT NULL,
                    invalidation TEXT NOT NULL, note TEXT, seer_summary TEXT,
                    market_overlay_snapshot JSONB NOT NULL,
                    mystical_feature_payload JSONB NOT NULL,
                    grid_feature_payload JSONB NOT NULL,
                    weight_version TEXT NOT NULL, model_version TEXT NOT NULL,
                    live_or_local TEXT NOT NULL, status TEXT NOT NULL,
                    comparable_publish_status TEXT NOT NULL,
                    comparable_prediction_ref TEXT,
                    comparable_publish_payload JSONB NOT NULL,
                    lens_set_id BIGINT, sky_snapshot_id BIGINT,
                    seer_run_id BIGINT, persona_run_id BIGINT)
            """))
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
            conn.execute(text("DROP SCHEMA IF EXISTS astrogrid CASCADE"))
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        admin.dispose()


def _insert_spy_raw(conn, obs_date: date, pulled_at: datetime, price: float) -> None:
    marker = capture_payload(obs_date, pulled_at)
    assert marker is not None, "test setup: pulled_at must be past the SPY observation-day end"
    conn.execute(text("""
        INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, raw_payload, pull_status)
        VALUES (:sid, 1, :od, :ts, :val, CAST(:payload AS jsonb), 'SUCCESS')
    """), {"sid": SPY_CLOSE_SERIES, "od": obs_date, "ts": pulled_at, "val": price,
           "payload": json.dumps(marker)})


def _insert_binance_raw(conn, obs_date: date, pulled_at: datetime, price: float) -> None:
    open_ms = int(datetime.combine(obs_date, time.min, timezone.utc).timestamp() * 1000)
    close_ms = open_ms + 86_400_000 - 1
    evidence = {
        "close_contract": "binance_utc_close_v1", "endpoint_host": "data-api.binance.vision",
        "interval": "1d", "time_zone": "UTC", "open_time_ms": open_ms, "close_time_ms": close_ms,
        "captured_at_utc": pulled_at.isoformat(),
    }
    conn.execute(text("""
        INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, raw_payload, pull_status)
        VALUES ('binance.BTCUSDT.close', 184, :od, :ts, :val, CAST(:payload AS jsonb), 'SUCCESS')
    """), {"od": obs_date, "ts": pulled_at, "val": price, "payload": json.dumps(evidence)})


def _insert_unmarked_spy_raw(
    conn, series_id: str, obs_date: date, pulled_at: datetime, price: float,
) -> None:
    conn.execute(text("""
        INSERT INTO raw_series
            (series_id, source_id, obs_date, pull_timestamp, value, raw_payload, pull_status)
        VALUES (:sid, 1, :od, :ts, :val, NULL, 'SUCCESS')
    """), {"sid": series_id, "od": obs_date, "ts": pulled_at, "val": price})


def test_spy_and_binance_resolve_together_in_one_partition_without_interference(
    composed_pg_engine: Engine,
) -> None:
    engine = composed_pg_engine
    now = datetime.now(timezone.utc)
    # Both raw rows' pull_timestamp must fall within resolve_pending's scan
    # window (bounded below by `since`) or the resolver's own series-id scan
    # never finds them at all -- confirmed directly: an earlier version of
    # this test picked timestamps outside that window and the scan found
    # only 1 of the 2 series (a test-setup bug, not a resolver bug).
    spy_obs = (now - timedelta(days=2)).date()
    spy_pulled_at = datetime.combine(spy_obs + timedelta(days=1), time(1, 0), tzinfo=timezone.utc)
    btc_obs = (now - timedelta(days=1)).date()
    btc_pulled_at = datetime.combine(btc_obs + timedelta(days=1), time(1, 0), tzinfo=timezone.utc)
    assert spy_pulled_at < now and btc_pulled_at < now, "test setup: both pulls must be in the past"

    with engine.begin() as conn:
        _insert_spy_raw(conn, spy_obs, spy_pulled_at, 680.0)
        _insert_binance_raw(conn, btc_obs, btc_pulled_at, 65000.0)

    # workers=1 forces every distinct series_id -- SPY's and Binance's alike
    # -- into the SAME partition, exercising both branches of
    # Resolver._resolve_partition's shared per-(series_id, obs_date) loop in
    # one call, on one connection. This is the exact scenario #606's own PR
    # description names as its purpose and no existing test constructs.
    resolver = Resolver(engine)
    result = resolver.resolve_pending(
        workers=1, since=min(spy_pulled_at, btc_pulled_at) - timedelta(hours=1),
    )
    assert result["errors"] == 0

    with engine.connect() as conn:
        resolved = conn.execute(text("""
            SELECT fr.name, rs.obs_date, rs.value, sc.name
            FROM resolved_series rs
            JOIN feature_registry fr ON fr.id = rs.feature_id
            JOIN source_catalog sc ON sc.id = rs.source_priority_used
            ORDER BY fr.name
        """)).fetchall()
        assert [(row[0], row[1], row[2], row[3]) for row in resolved] == [
            ("btc_full", btc_obs, 65000.0, "binance"),
            ("spy_full", spy_obs, 680.0, "yfinance"),
        ], "both series must resolve correctly in the same call -- neither branch suppressed the other"

        receipts = conn.execute(text("""
            SELECT price_basis, obs_date, value FROM astrogrid.price_close_receipt
        """)).fetchall()
        assert [(row[0], row[1], row[2]) for row in receipts] == [
            (SPY_CLOSE_SERIES, spy_obs, 680.0)
        ], "exactly one receipt, for SPY only -- the Binance row must never earn a price_close_receipt row"

    print(
        "SPY_BINANCE_COMPOSITION_E2E "
        f"spy_resolved=1 btc_resolved=1 receipts={len(receipts)} "
        f"canonical_series_member={'binance.BTCUSDT.close' in CANONICAL_CLOSE_SERIES}"
    )


def test_spy_adjusted_close_cannot_displace_marked_unadjusted_close(
    composed_pg_engine: Engine,
) -> None:
    engine = composed_pg_engine
    now = datetime.now(timezone.utc)
    obs = (now - timedelta(days=2)).date()
    pulled_at = datetime.combine(obs + timedelta(days=1), time(1), timezone.utc)
    assert pulled_at < now
    entity_map = EntityMap(engine)
    assert entity_map.get_feature_id("YF:SPY:adj_close") == 2791
    assert entity_map.get_feature_id(SPY_CLOSE_SERIES) == 2791

    with engine.begin() as conn:
        _insert_unmarked_spy_raw(conn, "YF:SPY:adj_close", obs, pulled_at, 678.0)
        _insert_spy_raw(conn, obs, pulled_at, 680.0)
    result = Resolver(engine).resolve_pending(
        workers=1, since=pulled_at - timedelta(hours=1),
    )
    assert result["errors"] == 0
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT rs.value, raw.series_id
            FROM resolved_series rs
            JOIN astrogrid.price_close_receipt pc ON pc.resolved_series_id = rs.id
            JOIN raw_series raw ON raw.id = pc.raw_series_id
            WHERE rs.feature_id = 2791
        """)).fetchall()
        assert [(row[0], row[1]) for row in rows] == [(680.0, SPY_CLOSE_SERIES)]
        assert conn.execute(text("SELECT count(*) FROM resolved_series")).scalar_one() == 1
        assert conn.execute(text("SELECT count(*) FROM astrogrid.price_close_receipt")).scalar_one() == 1
    print("SPY_ADJ_CLOSE_BOUNDARY marked_close_resolved=1 adjusted_displaced=0 receipts=1")


def test_spy_adjusted_close_without_marked_close_leaves_shared_feature_unresolved(
    composed_pg_engine: Engine,
) -> None:
    engine = composed_pg_engine
    now = datetime.now(timezone.utc)
    obs = (now - timedelta(days=2)).date()
    pulled_at = datetime.combine(obs + timedelta(days=1), time(1), timezone.utc)
    assert pulled_at < now
    with engine.begin() as conn:
        _insert_unmarked_spy_raw(conn, "YF:SPY:adj_close", obs, pulled_at, 678.0)
        _insert_unmarked_spy_raw(conn, SPY_CLOSE_SERIES, obs, pulled_at, 680.0)
    result = Resolver(engine).resolve_pending(
        workers=1, since=pulled_at - timedelta(hours=1),
    )
    assert result["errors"] == 0
    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM resolved_series")).scalar_one() == 0
        assert conn.execute(text("SELECT count(*) FROM astrogrid.price_close_receipt")).scalar_one() == 0
    print("SPY_ADJ_CLOSE_BOUNDARY no_marked_close=1 adjusted_suppressed=1 resolved=0 receipts=0")
