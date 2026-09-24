"""Real PostgreSQL proof of the versioned SPY close receipt contract.

The test database must be explicitly named by GRID_TEST_DB_URL and local to
the runner. CI runs this module against its disposable PostgreSQL service.
"""

from __future__ import annotations

import importlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
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
from price_close_contract import (
    SPY_CLOSE_CONTRACT, SPY_CLOSE_SERIES, SPY_ENTRY_RULE, SPY_OUTCOME_RULE,
    capture_payload,
)
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
        # This separately invoked CI proof owns the local disposable test DB.
        # The general suite may have left AstroGrid tables with dependent FKs;
        # reset its schema instead of dropping individual tables in FK order.
        conn.execute(text("DROP SCHEMA IF EXISTS astrogrid CASCADE"))
        conn.execute(text("CREATE SCHEMA astrogrid"))
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
            conn.execute(text("INSERT INTO feature_registry VALUES (2792, 'btc_full')"))
            conn.execute(text("CREATE TABLE regime_history (obs_date DATE, regime TEXT, confidence DOUBLE PRECISION)"))
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
            conn.execute(text("DROP SCHEMA IF EXISTS astrogrid CASCADE"))
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


def _receipt(engine: Engine, obs_date: date, price: float,
             *, delay_days: int = 1) -> dict:
    pulled_at = datetime.combine(
        obs_date + timedelta(days=delay_days), datetime.min.time(), timezone.utc,
    ) + timedelta(minutes=5)
    marker = capture_payload(obs_date, pulled_at)
    raw_id = _raw(engine, obs_date, pulled_at, price, marker)
    assert _resolve_spy_close_receipt(
        engine, 2791, obs_date, [_source(raw_id, pulled_at, price, marker)],
    ) == 1
    with engine.connect() as conn:
        receipt = AstroGridStore(engine)._verified_spy_receipt(
            conn, cutoff=pulled_at + timedelta(days=1), mode="id",
            receipt_id=conn.execute(text("""
                SELECT id FROM astrogrid.price_close_receipt WHERE raw_series_id = :raw_id
            """), {"raw_id": raw_id}).scalar_one(),
        )
    assert receipt is not None
    return receipt


def _prediction(engine: Engine, created_day: date, entry: dict) -> str:
    created_at = datetime.combine(created_day, datetime.min.time(), timezone.utc) + timedelta(hours=12)
    prediction_id = "spy-test-" + uuid4().hex
    anchor = {
        "version": SPY_CLOSE_CONTRACT, "entry_rule": SPY_ENTRY_RULE,
        "outcome_rule": SPY_OUTCOME_RULE, "basis": SPY_CLOSE_SERIES,
        "symbol": "SPY", "entry_receipt_id": entry["receipt_id"],
        "entry_raw_series_id": entry["raw_series_id"],
        "entry_resolved_series_id": entry["resolved_series_id"],
        "entry_obs_date": entry["obs_date"].isoformat(),
        "entry_price": entry["price"],
        "entry_available_at": entry["available_at"].isoformat(),
    }
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO astrogrid.prediction_run (
                prediction_id, created_at, as_of_ts, horizon_label, target_universe,
                scoring_class, target_symbols, question, call, timing, setup,
                invalidation, market_overlay_snapshot, mystical_feature_payload,
                grid_feature_payload, weight_version, model_version, live_or_local,
                status, comparable_publish_status, comparable_publish_payload
            ) VALUES (
                :pid, :created, :created, 'swing', 'equity', 'liquid_market',
                '["SPY"]'::jsonb, 'SPY test', 'buy SPY', 'one week', 'test',
                'invalidate', CAST(:overlay AS jsonb), '{}'::jsonb, '{}'::jsonb,
                'test', 'test', 'live', 'created', 'not_attempted', '{}'::jsonb
            )
        """), {"pid": prediction_id, "created": created_at,
                "overlay": json.dumps({"price_close_contract": anchor})})
    return prediction_id


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
    # Use a completed trading weekday. A Sunday synthetic SPY close would
    # prove links but could never be produced by the intended daily feed.
    entry_day = real_now.astimezone(timezone.utc).date() - timedelta(days=1)
    while True:
        entry_available = datetime.combine(
            entry_day + timedelta(days=1), datetime.min.time(), timezone.utc,
        ) + timedelta(minutes=5)
        if entry_day.weekday() < 5 and entry_available < real_now:
            break
        entry_day -= timedelta(days=1)
    entry_marker = capture_payload(entry_day, entry_available)
    entry_raw_id = _raw(engine, entry_day, entry_available, 680.0, entry_marker)
    assert _resolve_spy_close_receipt(
        engine, 2791, entry_day,
        [_source(entry_raw_id, entry_available, 680.0, entry_marker)],
    ) == 1

    target_day = real_now.astimezone(timezone.utc).date() + timedelta(days=7)
    outcome_day = target_day
    while outcome_day.weekday() >= 5:
        outcome_day += timedelta(days=1)
    outcome_available = datetime.combine(outcome_day + timedelta(days=1), datetime.min.time(), timezone.utc) + timedelta(minutes=5)
    outcome_marker = capture_payload(outcome_day, outcome_available)
    outcome_raw_id = _raw(engine, outcome_day, outcome_available, 700.0, outcome_marker)
    assert _resolve_spy_close_receipt(
        engine, 2791, outcome_day,
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
        "as_of_date": (outcome_day + timedelta(days=1)).isoformat(),
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
        assert evidence[0]["price_close_evidence"]["outcome_obs_date"] == outcome_day.isoformat()
        assert evidence[1] == pytest.approx((700.0 - 680.0) / 680.0, abs=1e-6)
    # CI keeps this sanitized receipt in its log before the fixture removes
    # the disposable schema and test rows.
    print(
        "SPY_RECEIPT_E2E version=spy_close_v1 "
        f"entry_obs={entry_day.isoformat()} outcome_obs={outcome_day.isoformat()} "
        "preavailability_unscored=1 entry_linked=1 outcome_linked=1 scored=1"
    )


@pytest.mark.parametrize("scoring_zone", ["UTC", "America/New_York"])
def test_missing_outcomes_do_not_fill_bounded_batch(
    receipt_pg_engine: Engine, scoring_zone: str,
) -> None:
    engine = receipt_pg_engine
    today = datetime.now(timezone.utc).date()
    old_ids = []
    for days_ago in (70, 45):
        created_day = today - timedelta(days=days_ago)
        entry = _receipt(engine, created_day - timedelta(days=1), 680.0)
        old_ids.append(_prediction(engine, created_day, entry))
    created_day = today - timedelta(days=20)
    entry = _receipt(engine, created_day - timedelta(days=1), 680.0)
    wanted_id = _prediction(engine, created_day, entry)
    _receipt(engine, created_day + timedelta(days=7), 700.0)

    from sqlalchemy import event

    @event.listens_for(engine, "checkout")
    def set_scoring_zone(dbapi_conn, _record, _proxy) -> None:
        with dbapi_conn.cursor() as cursor:
            cursor.execute(f"SET TIME ZONE '{scoring_zone}'")

    try:
        engine.dispose()
        summary = AstroGridStore(engine).score_predictions(as_of_date=today, limit=2)
    finally:
        event.remove(engine, "checkout", set_scoring_zone)
    assert summary["scored"] == 1
    assert summary["prediction_ids"] == [wanted_id]
    with engine.connect() as conn:
        assert conn.execute(text("""
            SELECT count(*) FROM astrogrid.prediction_score ps
            JOIN astrogrid.prediction_run pr ON pr.id = ps.prediction_run_id
            WHERE pr.prediction_id = ANY(:ids)
        """), {"ids": old_ids}).scalar_one() == 0


def test_malformed_outcomes_do_not_fill_bounded_batch(receipt_pg_engine: Engine) -> None:
    engine = receipt_pg_engine
    today = datetime.now(timezone.utc).date()
    old_ids = []
    for days_ago in (70, 45):
        created_day = today - timedelta(days=days_ago)
        entry = _receipt(engine, created_day - timedelta(days=1), 680.0)
        old_ids.append(_prediction(engine, created_day, entry))
        outcome = _receipt(engine, created_day + timedelta(days=7), 700.0)
        # Simulate a broken source lineage after the receipt was created.
        # The receipt still exists, but _verified_spy_receipt rejects it.
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE raw_series SET raw_payload = '{}'::jsonb WHERE id = :raw_id
            """), {"raw_id": outcome["raw_series_id"]})
    created_day = today - timedelta(days=20)
    entry = _receipt(engine, created_day - timedelta(days=1), 680.0)
    wanted_id = _prediction(engine, created_day, entry)
    _receipt(engine, created_day + timedelta(days=7), 705.0)

    summary = AstroGridStore(engine).score_predictions(as_of_date=today, limit=2)
    assert summary["scored"] == 1
    assert summary["prediction_ids"] == [wanted_id]
    with engine.connect() as conn:
        assert conn.execute(text("""
            SELECT count(*) FROM astrogrid.prediction_score ps
            JOIN astrogrid.prediction_run pr ON pr.id = ps.prediction_run_id
            WHERE pr.prediction_id = ANY(:ids)
        """), {"ids": old_ids}).scalar_one() == 0


def test_earlier_malformed_receipt_does_not_hide_later_verified_close(
    receipt_pg_engine: Engine,
) -> None:
    engine = receipt_pg_engine
    created_day = datetime.now(timezone.utc).date() - timedelta(days=20)
    entry = _receipt(engine, created_day - timedelta(days=1), 680.0)
    prediction_id = _prediction(engine, created_day, entry)
    malformed = _receipt(engine, created_day + timedelta(days=7), 690.0)
    with engine.begin() as conn:
        conn.execute(text("UPDATE raw_series SET raw_payload = '{}'::jsonb WHERE id = :id"),
                     {"id": malformed["raw_series_id"]})
    valid = _receipt(engine, created_day + timedelta(days=8), 700.0)

    summary = AstroGridStore(engine).score_predictions(as_of_date=datetime.now(timezone.utc).date(), limit=1)
    assert summary["scored"] == 1
    assert summary["prediction_ids"] == [prediction_id]
    with engine.connect() as conn:
        chosen = conn.execute(text("""
            SELECT ps.raw_payload->'price_close_evidence'->>'outcome_receipt_id'
            FROM astrogrid.prediction_score ps
            JOIN astrogrid.prediction_run pr ON pr.id = ps.prediction_run_id
            WHERE pr.prediction_id = :pid
        """), {"pid": prediction_id}).scalar_one()
    assert int(chosen) == valid["receipt_id"]


def test_missing_spy_backlog_does_not_starve_non_spy_candidate(
    receipt_pg_engine: Engine,
) -> None:
    engine = receipt_pg_engine
    today = datetime.now(timezone.utc).date()
    for days_ago in (70, 45):
        created_day = today - timedelta(days=days_ago)
        entry = _receipt(engine, created_day - timedelta(days=1), 680.0)
        _prediction(engine, created_day, entry)
    non_spy_id = "non-spy-test-" + uuid4().hex
    created_at = datetime.combine(today - timedelta(days=20), datetime.min.time(), timezone.utc)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO resolved_series
                (feature_id, obs_date, release_date, vintage_date, value, source_priority_used)
            VALUES (2792, :entry_day, :entry_day, :entry_day, 70000, 1),
                   (2792, :outcome_day, :outcome_day, :outcome_day, 71000, 1)
        """), {"entry_day": created_at.date(), "outcome_day": today - timedelta(days=1)})
        conn.execute(text("""
            INSERT INTO astrogrid.prediction_run (
                prediction_id, created_at, as_of_ts, horizon_label, target_universe,
                scoring_class, target_symbols, question, call, timing, setup,
                invalidation, market_overlay_snapshot, mystical_feature_payload,
                grid_feature_payload, weight_version, model_version, live_or_local,
                status, comparable_publish_status, comparable_publish_payload
            ) VALUES (
                :pid, :created, :created, 'swing', 'crypto', 'liquid_market',
                '["BTC"]'::jsonb, 'BTC test', 'buy BTC', 'one week', 'test',
                'invalidate', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
                'test', 'test', 'live', 'created', 'not_attempted', '{}'::jsonb
            )
        """), {"pid": non_spy_id, "created": created_at})
    summary = AstroGridStore(engine).score_predictions(as_of_date=today - timedelta(days=1), limit=1)
    assert summary["candidates"] == 1
    assert summary["scored"] == 1
    assert summary["prediction_ids"] == [non_spy_id]


@pytest.mark.parametrize("field,value", [
    ("entry_receipt_id", "0"),
    ("entry_available_at", '"invalid timestamp"'),
])
def test_invalid_entry_anchor_cannot_occupy_ready_slot(
    receipt_pg_engine: Engine, field: str, value: str,
) -> None:
    engine = receipt_pg_engine
    today = datetime.now(timezone.utc).date()
    bad_day = today - timedelta(days=40)
    bad_entry = _receipt(engine, bad_day - timedelta(days=1), 680.0)
    bad_id = _prediction(engine, bad_day, bad_entry)
    _receipt(engine, bad_day + timedelta(days=7), 700.0)
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE astrogrid.prediction_run
            SET market_overlay_snapshot = jsonb_set(
                market_overlay_snapshot, CAST(:path AS text[]), CAST(:value AS jsonb))
            WHERE prediction_id = :pid
        """), {"pid": bad_id, "path": ["price_close_contract", field], "value": value})
    good_day = today - timedelta(days=20)
    good_entry = _receipt(engine, good_day - timedelta(days=1), 680.0)
    good_id = _prediction(engine, good_day, good_entry)
    _receipt(engine, good_day + timedelta(days=7), 710.0)
    summary = AstroGridStore(engine).score_predictions(as_of_date=today, limit=1)
    assert summary["prediction_ids"] == [good_id]


def test_case_normalized_spy_uses_created_utc_day_for_maturity(
    receipt_pg_engine: Engine,
) -> None:
    engine = receipt_pg_engine
    today = datetime.now(timezone.utc).date()
    created_day = today - timedelta(days=8)
    entry = _receipt(engine, created_day - timedelta(days=1), 680.0)
    prediction_id = _prediction(engine, created_day, entry)
    _receipt(engine, created_day + timedelta(days=7), 700.0)
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE astrogrid.prediction_run
            SET target_symbols = '["spy"]'::jsonb,
                as_of_ts = created_at + INTERVAL '1 day'
            WHERE prediction_id = :pid
        """), {"pid": prediction_id})
    # The scorer recognizes lowercase SPY, but its anchor rejects a forged
    # as_of timestamp. Candidate maturity must still match created_at UTC.
    summary = AstroGridStore(engine).score_predictions(
        as_of_date=created_day + timedelta(days=7), prediction_ids=[prediction_id],
    )
    assert summary["candidates"] == 1
    assert summary["unscored"][0]["reason"] == "invalid_entry_anchor"


def test_outcome_window_edges_and_earliest_date(receipt_pg_engine: Engine) -> None:
    engine = receipt_pg_engine
    created_day = datetime.now(timezone.utc).date() - timedelta(days=20)
    target_day = created_day + timedelta(days=7)
    entry = _receipt(engine, created_day - timedelta(days=1), 680.0)
    prediction_id = _prediction(engine, created_day, entry)
    _receipt(engine, target_day - timedelta(days=1), 690.0)
    _receipt(engine, target_day + timedelta(days=5), 710.0)
    store = AstroGridStore(engine)
    before = store.score_predictions(prediction_ids=[prediction_id])
    assert before["scored"] == 0
    assert before["unscored"][0]["reason"] == "missing_verified_outcome"

    late_edge = _receipt(engine, target_day + timedelta(days=4), 704.0)
    with engine.connect() as conn:
        selected = store._verified_spy_receipt(
            conn, cutoff=datetime.now(timezone.utc), mode="outcome",
            min_date=target_day, max_date=target_day + timedelta(days=4),
        )
    assert selected and selected["receipt_id"] == late_edge["receipt_id"]
    early_edge = _receipt(engine, target_day, 700.0)
    scored = store.score_predictions(prediction_ids=[prediction_id])
    assert scored["scored"] == 1
    with engine.connect() as conn:
        evidence = conn.execute(text("""
            SELECT ps.raw_payload->'price_close_evidence'->>'outcome_receipt_id'
            FROM astrogrid.prediction_score ps
            JOIN astrogrid.prediction_run pr ON pr.id = ps.prediction_run_id
            WHERE pr.prediction_id = :pid
        """), {"pid": prediction_id}).scalar_one()
    assert int(evidence) == early_edge["receipt_id"]
    assert early_edge["receipt_id"] != late_edge["receipt_id"]


def test_scoring_is_idempotent_with_concurrent_callers(receipt_pg_engine: Engine) -> None:
    engine = receipt_pg_engine
    created_day = datetime.now(timezone.utc).date() - timedelta(days=20)
    entry = _receipt(engine, created_day - timedelta(days=1), 680.0)
    prediction_id = _prediction(engine, created_day, entry)
    _receipt(engine, created_day + timedelta(days=7), 700.0)
    store = AstroGridStore(engine)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(
            lambda _: store.score_predictions(prediction_ids=[prediction_id]), range(2),
        ))
    assert sum(result["scored"] for result in results) == 1
    assert store.score_predictions(prediction_ids=[prediction_id])["scored"] == 0
    with engine.connect() as conn:
        assert conn.execute(text("""
            SELECT count(*) FROM astrogrid.prediction_score ps
            JOIN astrogrid.prediction_run pr ON pr.id = ps.prediction_run_id
            WHERE pr.prediction_id = :pid
        """), {"pid": prediction_id}).scalar_one() == 1


def test_entry_timestamp_matches_across_session_time_zones(receipt_pg_engine: Engine) -> None:
    engine = receipt_pg_engine
    created_day = datetime.now(timezone.utc).date() - timedelta(days=20)
    entry = _receipt(engine, created_day - timedelta(days=1), 680.0)
    prediction_id = _prediction(engine, created_day, entry)
    _receipt(engine, created_day + timedelta(days=7), 700.0)
    # Force all pooled sessions used by the scorer to represent timestamptz
    # values in New York time while the frozen anchor remains in UTC text.
    from sqlalchemy import event

    @event.listens_for(engine, "checkout")
    def set_scoring_zone(dbapi_conn, _record, _proxy) -> None:
        with dbapi_conn.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'America/New_York'")

    try:
        engine.dispose()
        assert AstroGridStore(engine).score_predictions(
            prediction_ids=[prediction_id],
        )["scored"] == 1
    finally:
        event.remove(engine, "checkout", set_scoring_zone)


def test_late_earlier_close_exposes_outcome_finality_gap(receipt_pg_engine: Engine) -> None:
    """Document existing v1 behavior for a separate controller policy decision."""
    engine = receipt_pg_engine
    created_day = datetime.now(timezone.utc).date() - timedelta(days=20)
    target_day = created_day + timedelta(days=7)
    entry = _receipt(engine, created_day - timedelta(days=1), 680.0)
    prediction_id = _prediction(engine, created_day, entry)
    later_day = _receipt(engine, target_day + timedelta(days=1), 705.0)
    store = AstroGridStore(engine)
    assert store.score_predictions(prediction_ids=[prediction_id])["scored"] == 1
    earlier_day = _receipt(engine, target_day, 700.0, delay_days=3)

    with engine.connect() as conn:
        now_selected = store._verified_spy_receipt(
            conn, cutoff=datetime.now(timezone.utc), mode="outcome",
            min_date=target_day, max_date=target_day + timedelta(days=4),
        )
        stored_id = int(conn.execute(text("""
            SELECT ps.raw_payload->'price_close_evidence'->>'outcome_receipt_id'
            FROM astrogrid.prediction_score ps
            JOIN astrogrid.prediction_run pr ON pr.id = ps.prediction_run_id
            WHERE pr.prediction_id = :pid
        """), {"pid": prediction_id}).scalar_one())
    assert now_selected and now_selected["receipt_id"] == earlier_day["receipt_id"]
    assert stored_id == later_day["receipt_id"]
    assert store.score_predictions(prediction_ids=[prediction_id])["scored"] == 0
