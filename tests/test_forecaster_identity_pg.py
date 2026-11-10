"""TimesFM identity and decision-clock contracts through the actual PG writer.

Uses the shared pg_engine only when the database name ends in ``_test``.
Each test owns a temporary schema; no shared tables, live models or providers
are used. Calibration and regime reads are fixed synthetic inputs.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.schema import CreateSchema, DropSchema

from intelligence import confidence_calibration
from oracle import forecaster_adapter, prediction_context
from oracle.engine import OracleEngine, OraclePrediction
from oracle.dedup_index import _DEDUP_INDEX_DDL, forecast_identity


_TABLE_DDL = """
CREATE TABLE oracle_predictions (
    id TEXT PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ticker TEXT, prediction_type TEXT, direction TEXT, target_price DOUBLE PRECISION,
    entry_price DOUBLE PRECISION, expiry DATE, confidence DOUBLE PRECISION,
    expected_move_pct DOUBLE PRECISION, signal_strength DOUBLE PRECISION,
    coherence DOUBLE PRECISION, model_name TEXT, model_version TEXT,
    signals JSONB, anti_signals JSONB, flow_context JSONB, model_weights JSONB,
    horizon_days INTEGER, null_write_policy TEXT, recording_policy TEXT, verdict TEXT DEFAULT 'pending',
    actual_price DOUBLE PRECISION, scored_at TIMESTAMPTZ, score_notes TEXT,
    dedup_keep BOOLEAN DEFAULT TRUE
)
"""
_INDEX_DDL = _DEDUP_INDEX_DDL


@pytest.fixture
def identity_engine(pg_engine: Engine):
    """Reject non-test databases before DDL, then isolate all fixture writes."""
    if not (pg_engine.url.database or "").endswith("_test"):
        pytest.skip("Identity PG proof requires a database name ending in _test")
    with pg_engine.connect() as conn:
        database = conn.execute(text("SELECT current_database()")).scalar_one()
        assert database.endswith("_test"), "Refusing DDL outside a test database"

    schema = "forecaster_identity_" + uuid4().hex
    with pg_engine.begin() as conn:
        conn.execute(CreateSchema(schema))
    engine = create_engine(
        pg_engine.url,
        connect_args={"options": "-c search_path=" + schema + " -c statement_timeout=5000"},
    )
    try:
        with engine.begin() as conn:
            conn.execute(text(_TABLE_DDL))
            conn.execute(text(_INDEX_DDL))
            conn.execute(text("CREATE TABLE oracle_models (name TEXT PRIMARY KEY, active BOOLEAN)"))
            conn.execute(text("INSERT INTO oracle_models VALUES ('timeseries_enhanced', TRUE), ('other', TRUE)"))
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(DropSchema(schema, cascade=True))


@pytest.fixture
def writer(identity_engine: Engine, monkeypatch):
    monkeypatch.setattr(
        confidence_calibration, "calibrate_confidence_default", lambda value, model: value,
    )
    monkeypatch.setattr(prediction_context, "fetch_liquidity_regime", lambda *a: "NEUTRAL")
    monkeypatch.setattr(prediction_context, "fetch_fci_regime", lambda *a: "NEUTRAL")
    monkeypatch.setattr(prediction_context, "fetch_vix_level", lambda *a: None)
    oracle = OracleEngine.__new__(OracleEngine)
    oracle.engine = identity_engine
    return oracle


def _prediction(target: float, version: str = "fixture-v1") -> OraclePrediction:
    forecast = SimpleNamespace(
        predictions=[100.0, target], forecast_std=[1.0], horizon=7,
        model_version=version, lower_bound=[], upper_bound=[],
    )
    prediction = forecaster_adapter.forecast_to_prediction(forecast, "SYNTH", 100.0)
    assert prediction is not None
    issued = datetime(2026, 11, 10, 15, 10, tzinfo=timezone.utc)
    prediction = replace(prediction, timestamp=issued, expiry=issued.date() + timedelta(days=7),
                         flow_context={"entry_price_basis": _receipt()})
    return _with_identity(prediction)


def _receipt(ticker="SYNTH", price=100.0):
    return {
        "status": "measured", "policy": "oracle_observed_reference_v1", "price": price,
        "ticker": ticker, "source": "options_capture_batches", "provider": "cboe",
        "basis": "raw_spot", "capture_batch_id": "fixture", "capture_source": "smart_scheduler",
        "market_at": "2026-11-10T15:05:00+00:00", "capture_started_at": "2026-11-10T15:08:00+00:00",
        "capture_completed_at": "2026-11-10T15:09:00+00:00", "available_at": "2026-11-10T15:09:01+00:00",
        "observed_on": "2026-11-10", "max_age_seconds": 1200,
        "read_observed_at": "2026-11-10T15:09:02+00:00",
        "registered_at": "2026-11-10T15:09:01+00:00",
        "available_at_basis": "registration_lower_bound_not_commit",
        "evidence_class": "prospective_observed_reference", "executable": False, "backfilled": False,
    }


def _with_identity(prediction):
    return replace(prediction, id=forecast_identity(
        ticker=prediction.ticker, direction=prediction.direction, expiry=prediction.expiry,
        prediction_type=prediction.prediction_type.value, model_name=prediction.model_name,
        model_version=prediction.model_version, timestamp=prediction.timestamp,
    ))


def _row(engine: Engine, prediction_id: str):
    with engine.connect() as conn:
        return conn.execute(text("""
            SELECT *
            FROM oracle_predictions WHERE id = :id
        """), {"id": prediction_id}).one()


def _count(engine: Engine) -> int:
    with engine.connect() as conn:
        return conn.execute(text("SELECT count(*) FROM oracle_predictions")).scalar_one()


def test_reversal_and_version_change_do_not_abort_unrelated_batch_rows(writer):
    call = _prediction(110.0)
    put = _prediction(90.0)
    new_version = _prediction(110.0, "fixture-v2")
    assert len({call.id, put.id, new_version.id}) == 3
    sentinel = _with_identity(replace(call, ticker="SENTINEL",
        flow_context={"entry_price_basis": _receipt("SENTINEL")}))

    writer._store_predictions([call])
    before = _row(writer.engine, call.id)
    writer._store_predictions([sentinel, put, new_version])

    assert _count(writer.engine) == 4
    assert _row(writer.engine, call.id) == before
    assert _row(writer.engine, put.id).direction == "PUT"
    assert _row(writer.engine, new_version.id).model_version == "fixture-v2"
    assert _row(writer.engine, sentinel.id).ticker == "SENTINEL"


def test_retry_preserves_original_forecast_and_final_outcome(writer):
    original = _prediction(110.0)
    writer._store_predictions([original])
    settled_at = original.timestamp + timedelta(days=8)
    with writer.engine.begin() as conn:
        conn.execute(text("""
            UPDATE oracle_predictions SET verdict = 'hit', actual_price = 115,
                scored_at = :scored_at WHERE id = :id
        """), {"id": original.id, "scored_at": settled_at})
    before = _row(writer.engine, original.id)

    same_day_later = original.timestamp.replace(
        microsecond=(original.timestamp.microsecond + 1) % 1_000_000,
    )
    retry = replace(
        original, timestamp=same_day_later,
        current_price=101.0, target_price=120.0, confidence=0.94,
        flow_context={"entry_price_basis": _receipt(price=101.0), "changed": True},
    )
    writer._store_predictions([retry])

    assert _count(writer.engine) == 1
    assert _row(writer.engine, original.id) == before


def test_delayed_persistence_keeps_utc_decision_day_and_identity(writer):
    prediction = _prediction(110.0)
    issued = prediction.timestamp
    prediction = replace(prediction, timestamp=issued.astimezone(timezone(timedelta(hours=12))))
    assert prediction.timestamp.date() != issued.date()
    first = writer._store_predictions([prediction])
    assert len(first) == 1
    assert writer._store_predictions([prediction]) == []
    assert writer._last_store_counts["duplicates"] == 1
    assert _row(writer.engine, first[0].id).created_at == issued



def test_naive_timestamp_refuses_batch_before_any_database_statement(writer):
    valid = _prediction(110.0)
    naive = replace(_prediction(90.0), timestamp=datetime(2020, 1, 2, 0, 0, 1))
    statements = []

    def record_statement(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(writer.engine, "before_cursor_execute", record_statement)
    try:
        with pytest.raises(ValueError, match="timezone-aware"):
            writer._store_predictions([valid, naive])
    finally:
        event.remove(writer.engine, "before_cursor_execute", record_statement)

    assert statements == []
    assert _count(writer.engine) == 0


def test_two_models_same_version_same_day_preserve_separate_original_payloads(writer):
    first = _prediction(110.0)
    second = _with_identity(replace(first, model_name="other", confidence=0.2))
    accepted = writer._store_predictions([first, second])
    assert {p.model_name for p in accepted} == {"timeseries_enhanced", "other"}
    with writer.engine.connect() as conn:
        before = conn.execute(text("SELECT row_to_json(p) FROM oracle_predictions p ORDER BY id")).all()
    assert writer._store_predictions([replace(first, confidence=0.99), replace(second, confidence=0.9)]) == []
    with writer.engine.connect() as conn:
        after = conn.execute(text("SELECT row_to_json(p) FROM oracle_predictions p ORDER BY id")).all()
    assert after == before
    assert before[0][0]["recording_policy"] == "oracle_forward_v2"


def test_retired_unknown_and_unobserved_entries_are_rejected(writer):
    first = _prediction(110.0)
    with writer.engine.begin() as conn:
        conn.execute(text("UPDATE oracle_models SET active=FALSE WHERE name='timeseries_enhanced'"))
    assert writer._store_predictions([first, replace(first, model_name="unknown")]) == []
    with writer.engine.begin() as conn:
        conn.execute(text("UPDATE oracle_models SET active=TRUE WHERE name='timeseries_enhanced'"))
    rows = writer._store_predictions([replace(first, flow_context={})])
    assert len(rows) == 1
    assert rows[0].verdict.value == "no_data"
    assert rows[0].current_price is None
    assert rows[0].score_notes
    assert writer._last_store_counts["abstentions"] == 1
    assert writer._last_store_counts["qualified"] == 0
    assert _count(writer.engine) == 1


def test_migration_preserves_legacy_and_changes_arbiter_atomically(identity_engine):
    import importlib
    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    from sqlalchemy.exc import ProgrammingError
    from oracle.dedup_index import ensure_dedup_index
    migration = importlib.import_module("migrations.versions.oracle_forward_v2_20261110")
    legacy = _prediction(110.0)
    with identity_engine.begin() as conn:
        conn.execute(text("DROP INDEX oracle_predictions_dedup_unique"))
        conn.execute(text("ALTER TABLE oracle_predictions DROP COLUMN recording_policy"))
        conn.execute(text("""CREATE UNIQUE INDEX oracle_predictions_dedup_unique ON oracle_predictions (
            ticker,direction,expiry,prediction_type,(COALESCE(model_version,'')),
            ((created_at AT TIME ZONE 'UTC')::date)) WHERE dedup_keep=TRUE"""))
        conn.execute(text("""INSERT INTO oracle_predictions(id,ticker,direction,expiry,prediction_type,
            model_name,model_version,created_at,confidence,signals) VALUES
            ('legacy','SYNTH','CALL',:expiry,'direction','timeseries_enhanced',:version,:at,.7,CAST(:signals AS JSONB))"""),
            {"expiry": legacy.expiry, "version": legacy.model_version, "at": legacy.timestamp, "signals": '{"original":true}'})
        before = conn.execute(text("SELECT to_jsonb(p) FROM oracle_predictions p")).scalar_one()
    with pytest.raises(RuntimeError, match="migration required"):
        ensure_dedup_index(identity_engine)
    with identity_engine.begin() as conn:
        with Operations.context(MigrationContext.configure(conn)):
            migration.upgrade()
        after = conn.execute(text("SELECT to_jsonb(p) - 'recording_policy' FROM oracle_predictions p")).scalar_one()
        assert after == before
        assert conn.execute(text("SELECT recording_policy FROM oracle_predictions")).scalar_one() is None
        assert conn.execute(text("SELECT indisvalid FROM pg_index WHERE indexrelid='oracle_predictions_dedup_unique'::regclass")).scalar_one()
    ensure_dedup_index(identity_engine)
    # A still-running old writer cannot infer its arbiter after cutover.
    with pytest.raises(ProgrammingError, match="no unique or exclusion constraint"):
        with identity_engine.begin() as conn:
            conn.execute(text("""INSERT INTO oracle_predictions(id,ticker,direction,expiry,prediction_type,
                model_name,model_version,created_at) VALUES
                ('old-writer','SYNTH','CALL',:expiry,'direction','timeseries_enhanced',:version,:at)
                ON CONFLICT (ticker,direction,expiry,prediction_type,(COALESCE(model_version,'')),
                ((created_at AT TIME ZONE 'UTC')::date)) WHERE dedup_keep=TRUE
                DO UPDATE SET confidence=.99"""),
                {"expiry": legacy.expiry,"version": legacy.model_version,"at": legacy.timestamp})
    oracle = OracleEngine.__new__(OracleEngine)
    oracle.engine = identity_engine
    assert len(oracle._store_predictions([legacy])) == 1
    assert _count(identity_engine) == 2


def test_runtime_schema_guard_is_read_only_after_migration(identity_engine):
    from oracle.dedup_index import ensure_dedup_index
    statements = []
    def record(conn,cursor,statement,parameters,context,executemany):
        statements.append(statement)
    event.listen(identity_engine, "before_cursor_execute", record)
    try:
        ensure_dedup_index(identity_engine)
        ensure_dedup_index(identity_engine)
    finally:
        event.remove(identity_engine, "before_cursor_execute", record)
    assert len(statements) == 2
    assert all(stmt.strip().startswith("SELECT") for stmt in statements)


def test_admission_lock_serializes_retirement_with_commit(writer):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    at_insert, allow_insert, retired = Event(), Event(), Event()
    def pause_insert(conn, cursor, statement, params, context, many):
        if "INSERT INTO oracle_predictions" in statement:
            at_insert.set()
            assert allow_insert.wait(5)
    def retire():
        with writer.engine.begin() as conn:
            conn.execute(text("UPDATE oracle_models SET active=FALSE WHERE name='timeseries_enhanced'"))
        retired.set()
    event.listen(writer.engine, "before_cursor_execute", pause_insert)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            storing = pool.submit(writer._store_predictions, [_prediction(110.0)])
            assert at_insert.wait(5)
            retirement = pool.submit(retire)
            assert not retired.wait(.1), "retirement crossed a locked admission"
            allow_insert.set()
            assert len(storing.result(timeout=5)) == 1
            retirement.result(timeout=5)
    finally:
        allow_insert.set()
        event.remove(writer.engine, "before_cursor_execute", pause_insert)
    assert writer._store_predictions([_prediction(90.0)]) == []
    assert _count(writer.engine) == 1


def test_external_publisher_retry_returns_original_id_and_entry(writer, monkeypatch):
    from oracle import publish
    monkeypatch.setattr(publish, "build_prediction_context", lambda *a, **k: {})
    monkeypatch.setattr(publish, "_measured_entry_price", lambda *a: (100.0, {"status": "measured", "fixture": "original"}))
    payload = {"prediction_id": "first", "target_symbols": ["SYNTH"], "call": "buy",
               "as_of_ts": "2026-11-10T15:10:00+00:00", "confidence": .7}
    original = publish.publish_astrogrid_prediction(writer.engine, payload)
    with writer.engine.connect() as conn:
        before = conn.execute(text("SELECT to_jsonb(p) FROM oracle_predictions p")).scalar_one()
    monkeypatch.setattr(publish, "_measured_entry_price", lambda *a: (130.0, {"fixture": "changed"}))
    again = publish.publish_astrogrid_prediction(writer.engine, payload | {"prediction_id": "new", "confidence": .99})
    assert again["oracle_prediction_id"] == original["oracle_prediction_id"]
    assert again["status"] == "already_published"
    assert again["entry_price"] == 100.0
    assert again["entry_price_basis"] == original["entry_price_basis"]
    with writer.engine.connect() as conn:
        assert conn.execute(text("SELECT to_jsonb(p) FROM oracle_predictions p")).scalar_one() == before
    assert before["recording_policy"] == "oracle_external_v2"


def test_vault_stub_cannot_reopen_finalized_record(writer):
    from intelligence.obsidian_agent import act_on_approval
    note = {"domain": "alpha", "title": "Synthetic note", "body": "Watching $SPY."}
    with writer.engine.begin() as conn:
        act_on_approval(conn, note)
        conn.execute(text("UPDATE oracle_predictions SET verdict='no_data', score_notes='research stub'"))
        before = conn.execute(text("SELECT to_jsonb(p) FROM oracle_predictions p")).scalar_one()
    with writer.engine.begin() as conn:
        act_on_approval(conn, note | {"title": "Altered note"})
        assert conn.execute(text("SELECT to_jsonb(p) FROM oracle_predictions p")).scalar_one() == before
    assert before["recording_policy"] == "oracle_external_v2"


def test_abstention_does_not_consume_later_qualified_forecast_identity(writer):
    prediction = _prediction(110.0)
    stale = replace(prediction, timestamp=prediction.timestamp + timedelta(minutes=30))
    abstention = writer._store_predictions([stale])[0]
    assert abstention.direction == "NONE"
    assert abstention.flow_context["candidate_direction"] == "CALL"
    assert abstention.confidence is None
    assert abstention.recording_policy == "oracle_forward_v2"
    receipt = dict(_receipt())
    for key in ("market_at", "capture_started_at", "capture_completed_at", "available_at", "read_observed_at", "registered_at"):
        receipt[key] = (datetime.fromisoformat(receipt[key]) + timedelta(minutes=30)).isoformat()
    fresh = replace(stale, flow_context={"entry_price_basis": receipt})
    qualified = writer._store_predictions([fresh])[0]
    assert qualified.verdict.value == "pending"
    assert qualified.direction == "CALL"
    assert qualified.id != abstention.id
    assert _count(writer.engine) == 2
    assert qualified.to_dict()["recording_policy"] == "oracle_forward_v2"


def test_quote_clock_migration_preserves_immutable_legacy_header(identity_engine):
    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    from migrations.versions import options_spot_clock_20261110 as migration
    with identity_engine.begin() as conn:
        conn.execute(text("""CREATE TABLE options_capture_batches (
            capture_batch_id TEXT PRIMARY KEY, spot_price DOUBLE PRECISION,
            capture_started_at TIMESTAMPTZ, capture_completed_at TIMESTAMPTZ
        )"""))
        conn.execute(text("""INSERT INTO options_capture_batches VALUES
            ('legacy',100,'2026-11-10T15:08:00Z','2026-11-10T15:09:00Z')"""))
        conn.execute(text("""CREATE FUNCTION fixture_no_mutation() RETURNS TRIGGER
            LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'immutable fixture'; END $$"""))
        conn.execute(text("""CREATE TRIGGER fixture_no_mutation BEFORE UPDATE OR DELETE
            ON options_capture_batches FOR EACH ROW EXECUTE FUNCTION fixture_no_mutation()"""))
        before = conn.execute(text("SELECT to_jsonb(b) FROM options_capture_batches b")).scalar_one()
        with Operations.context(MigrationContext.configure(conn)):
            migration.upgrade()
        row = conn.execute(text("SELECT to_jsonb(b) FROM options_capture_batches b")).scalar_one()
        assert row.pop("provider_regular_market_at") is None
        assert row.pop("spot_provider") is None
        assert row == before
        conn.execute(text("""INSERT INTO options_capture_batches
            (capture_batch_id,spot_price,capture_started_at,capture_completed_at,
             provider_regular_market_at,spot_provider)
            VALUES ('forward',101,'2026-11-10T15:08:00Z','2026-11-10T15:09:00Z',
                    '2026-11-10T15:05:00Z','cboe')"""))
        assert conn.execute(text("""SELECT spot_provider FROM options_capture_batches
            WHERE capture_batch_id='forward'""")).scalar_one() == "cboe"


def test_engine_bootstrap_after_migration_does_not_alter_existing_columns(identity_engine):
    oracle = OracleEngine.__new__(OracleEngine)
    oracle.engine = identity_engine
    oracle._ensure_tables()
    statements = []
    def record(conn, cursor, statement, params, context, many):
        statements.append(statement)
    event.listen(identity_engine, "before_cursor_execute", record)
    try:
        oracle._ensure_tables()
    finally:
        event.remove(identity_engine, "before_cursor_execute", record)
    assert not any("ALTER TABLE" in stmt or "DROP INDEX" in stmt for stmt in statements)
    assert not any("CREATE UNIQUE INDEX" in stmt for stmt in statements)


@pytest.mark.parametrize("expires_after", ["admission", "first_insert", "last_insert"])
def test_deadline_expiry_rolls_back_entire_batch_and_counts(writer, expires_after):
    still_valid = True
    inserts_seen = 0
    statements = []
    def expire(conn, cursor, statement, params, context, many):
        nonlocal still_valid, inserts_seen
        statements.append(statement)
        if "FROM oracle_models" in statement and expires_after == "admission":
            still_valid = False
        if "INSERT INTO oracle_predictions" in statement:
            inserts_seen += 1
            if (expires_after == "first_insert" and inserts_seen == 1
                    or expires_after == "last_insert" and inserts_seen == 2):
                still_valid = False
    writer._last_store_counts = {"inserted": 123, "qualified": 123}
    event.listen(writer.engine, "after_cursor_execute", expire)
    try:
        with pytest.raises(TimeoutError, match="batch rolled back"):
            writer._store_predictions([_prediction(110.0), _prediction(90.0)],
                                      should_continue=lambda: still_valid)
    finally:
        event.remove(writer.engine, "after_cursor_execute", expire)
    assert _count(writer.engine) == 0
    assert all(value == 0 for value in writer._last_store_counts.values())
    assert "SET LOCAL lock_timeout = '3s'" in statements
    assert "SET LOCAL statement_timeout = '10s'" in statements
    assert not any("UPDATE oracle_models" in stmt for stmt in statements)


def test_deadline_check_after_connection_checkout_prevents_admission(writer):
    connected = False
    statements = []
    def checkout(conn):
        nonlocal connected
        connected = True
    def record(conn, cursor, statement, params, context, many):
        statements.append(statement)
    event.listen(writer.engine, "engine_connect", checkout)
    event.listen(writer.engine, "before_cursor_execute", record)
    try:
        with pytest.raises(TimeoutError, match="batch rolled back"):
            writer._store_predictions([_prediction(110.0)], should_continue=lambda: not connected)
    finally:
        event.remove(writer.engine, "engine_connect", checkout)
        event.remove(writer.engine, "before_cursor_execute", record)
    assert statements == []
    assert _count(writer.engine) == 0


def test_deadline_settings_are_local_to_guarded_transaction(writer):
    with writer.engine.connect() as conn:
        before = (conn.execute(text("SHOW lock_timeout")).scalar_one(),
                  conn.execute(text("SHOW statement_timeout")).scalar_one())
    assert len(writer._store_predictions([_prediction(110.0)], should_continue=lambda: True)) == 1
    with writer.engine.connect() as conn:
        after = (conn.execute(text("SHOW lock_timeout")).scalar_one(),
                 conn.execute(text("SHOW statement_timeout")).scalar_one())
    assert after == before
