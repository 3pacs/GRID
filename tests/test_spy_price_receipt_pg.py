"""Real PostgreSQL proof of the versioned SPY close receipt contract.

The test database must be explicitly named by GRID_TEST_DB_URL and local to
the runner. CI runs this module against its disposable PostgreSQL service.
"""

from __future__ import annotations

import importlib
import json
import os
from datetime import date, datetime, timezone
from datetime import timedelta
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from fastapi import FastAPI
from fastapi.testclient import TestClient
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
        conn.execute(text("DROP TABLE IF EXISTS astrogrid.prediction_score"))
        conn.execute(text("DROP TABLE IF EXISTS astrogrid.prediction_run"))
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
            conn.execute(text("""
                CREATE TABLE astrogrid.prediction_score (
                    id BIGSERIAL PRIMARY KEY,
                    prediction_run_id BIGINT NOT NULL UNIQUE REFERENCES astrogrid.prediction_run(id),
                    scored_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    benchmark_symbol TEXT, realized_return DOUBLE PRECISION,
                    benchmark_return DOUBLE PRECISION, alpha_vs_benchmark DOUBLE PRECISION,
                    verdict TEXT, invalidation_status TEXT,
                    max_favorable_excursion DOUBLE PRECISION,
                    max_adverse_excursion DOUBLE PRECISION,
                    regime_context JSONB NOT NULL, attribution_grid JSONB NOT NULL,
                    attribution_mystical JSONB NOT NULL,
                    attribution_noise JSONB NOT NULL, raw_payload JSONB NOT NULL)
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
            conn.execute(text("DROP TABLE IF EXISTS astrogrid.prediction_score"))
            conn.execute(text("DROP TABLE IF EXISTS astrogrid.prediction_run"))
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


def test_receipt_to_astrogrid_api_anchor_to_score_without_hindsight(
    receipt_pg_engine: Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the real API and store SQL against one disposable PostgreSQL tree."""
    import astrogrid_api.astrogrid_predictions as api
    import store.astrogrid as store_module

    engine = receipt_pg_engine
    store = AstroGridStore(engine)
    store.ensure_lens_set = lambda _mode, _lenses: None
    store.save_prediction_stub_postmortem = lambda *_args, **_kwargs: None
    store.get_prediction = lambda prediction_id: {"prediction_id": prediction_id}
    monkeypatch.setattr(api, "get_astrogrid_store", lambda: store)
    app = FastAPI()
    app.include_router(api.router)
    app.dependency_overrides[api.require_auth] = lambda: "disposable-test-token"
    client = TestClient(app)

    with engine.connect() as conn:
        real_now = conn.execute(text("SELECT NOW()")).scalar_one()
    entry_day = real_now.astimezone(timezone.utc).date() - timedelta(days=2)
    entry_available = datetime.combine(entry_day + timedelta(days=1), datetime.min.time(), timezone.utc) + timedelta(minutes=5)
    entry_marker = capture_payload(entry_day, entry_available)
    entry_raw_id = _raw(engine, entry_day, entry_available, 680.0, entry_marker)
    assert _resolve_spy_close_receipt(
        engine, 2791, entry_day,
        [_source(entry_raw_id, entry_available, 680.0, entry_marker)],
    ) == 1

    target_day = real_now.astimezone(timezone.utc).date() + timedelta(days=7)
    outcome_available = datetime.combine(target_day + timedelta(days=1), datetime.min.time(), timezone.utc) + timedelta(minutes=5)
    outcome_marker = capture_payload(target_day, outcome_available)
    outcome_raw_id = _raw(engine, target_day, outcome_available, 700.0, outcome_marker)
    assert _resolve_spy_close_receipt(
        engine, 2791, target_day,
        [_source(outcome_raw_id, outcome_available, 700.0, outcome_marker)],
    ) == 1

    created = client.post("/predictions", json={
        "question": "What will SPY do this week?", "call": "buy SPY",
        "timing": "this week", "setup": "defined SPY forecast",
        "invalidation": "close below baseline", "target_symbols": ["SPY"],
        "horizon_label": "swing", "live_or_local": "live",
        "publish_oracle": False,
    })
    assert created.status_code == 200, created.text
    prediction_id = created.json()["prediction_id"]
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT market_overlay_snapshot, created_at
            FROM astrogrid.prediction_run WHERE prediction_id = :pid
        """), {"pid": prediction_id}).one()
        anchor = row[0]["price_close_contract"]
        assert anchor["entry_raw_series_id"] == entry_raw_id
        assert anchor["entry_obs_date"] == entry_day.isoformat()
        assert anchor["entry_available_at"] <= row[1].isoformat()
        # A future outcome exists in the disposable DB but is invisible at
        # creation time. The frozen entry cannot point to it.
        assert anchor["entry_raw_series_id"] != outcome_raw_id

    # The outcome is already inserted for the test, yet a score evaluated
    # before its recorded availability must remain explicitly unscored.
    before_outcome = outcome_available - timedelta(minutes=1)
    monkeypatch.setattr(store_module, "_utc_now", lambda: before_outcome)
    early = client.post("/predictions/score", json={
        "as_of_date": before_outcome.date().isoformat(),
        "prediction_ids": [prediction_id],
    })
    assert early.status_code == 200, early.text
    assert early.json()["scored"] == 0
    assert early.json()["unscored"][0]["reason"] == "missing_verified_outcome"

    monkeypatch.setattr(store_module, "_utc_now", lambda: outcome_available + timedelta(days=1))
    scored = client.post("/predictions/score", json={
        "as_of_date": (target_day + timedelta(days=2)).isoformat(),
        "prediction_ids": [prediction_id],
    })
    assert scored.status_code == 200, scored.text
    assert scored.json()["scored"] == 1
    with engine.connect() as conn:
        evidence = conn.execute(text("""
            SELECT ps.raw_payload, ps.realized_return
            FROM astrogrid.prediction_score ps
            JOIN astrogrid.prediction_run pr ON pr.id = ps.prediction_run_id
            WHERE pr.prediction_id = :pid
        """), {"pid": prediction_id}).one()
        assert evidence[0]["price_close_evidence"]["entry_receipt_id"] == anchor["entry_receipt_id"]
        assert evidence[0]["price_close_evidence"]["outcome_obs_date"] == target_day.isoformat()
        assert evidence[1] == pytest.approx((700.0 - 680.0) / 680.0, abs=1e-6)
    # CI keeps this sanitized receipt in its log before the fixture removes
    # the disposable schema and test rows.
    print(
        "SPY_RECEIPT_E2E version=spy_close_v1 "
        f"entry_obs={entry_day.isoformat()} outcome_obs={target_day.isoformat()} "
        "preavailability_unscored=1 entry_linked=1 outcome_linked=1 scored=1"
    )
