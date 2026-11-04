"""Native publication compatibility: real imports, SQLAlchemy/PG14 and FastAPI.

No AST extraction, substitute production modules, provider calls or inference.
DB cases require explicit GRID_OCT25_TEST_DB_URL and GRID_OCT25_REQUIRE_PG=1
from the hosted lane. Ordinary no-opt-in DB cases skip. Focused missing or
broken DB configuration FAILS; focused qualification requires zero skips.
Use only an isolated disposable CI database whose
role can CREATE/DROP its own schema. No production schema or credentials.
"""
from __future__ import annotations

from copy import deepcopy
import json
import os
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import make_url

# Verified config.py flag: prevents its GitSink background worker/file sink.
os.environ["GRID_GIT_SINK_DISABLED"] = "1"

@pytest.fixture(autouse=True)
def publication_external_socket_fence(monkeypatch):
    import socket
    original_connect = socket.socket.connect
    original_connect_ex = socket.socket.connect_ex
    def check(address):
        assert isinstance(address, tuple) and address[:2] == ("127.0.0.1", 15432), "EXTERNAL_SOCKET_REFUSED"
    def connect(sock, address):
        check(address)
        return original_connect(sock, address)
    def connect_ex(sock, address):
        check(address)
        return original_connect_ex(sock, address)
    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket.socket, "connect_ex", connect_ex)


@pytest.fixture(autouse=True)
def actual_publication_modules(publication_external_socket_fence):
    # Ordinary full-module imports occur after the external-I/O fence.
    global publication, check_numbers_grounded
    from ollama import market_briefing as publication
    from ollama.number_grounding import check_numbers_grounded

# Literal independent contract: accepted 20 legacy fields plus six declarations.
# It is deliberately a whole-record oracle, not copied from runtime output.
EXPECTED_RECORD = {'feature_key': 'alpha',
 'path': 'features.alpha.value',
 'raw_value': 0.125,
 'observation_date': '2026-10-23',
 'baseline_date': 'unknown',
 'selection_policy': 'unknown',
 'policy_status': 'unknown',
 'source_id': 'unknown',
 'receipt_id': 'unknown',
 'unit': 'unknown',
 'available_at': 'unknown',
 'collected_at': 'unknown',
 'vintage_date': 'unknown',
 'basis': 'unknown',
 'measure': 'unknown',
 'denominator': 'unknown',
 'cohort_id': 'unknown',
 'metric_id': 'unknown',
 'definition_version': 'unknown',
 'metric_binding': {'status': 'ACCEPT_WITH_LIMITATION',
                    'reason': 'metric_definition_unknown',
                    'expected_definition': None,
                    'source_truth': 'unknown',
                    'scope': 'supplied_metric_contract_only'},
 'lineage': {'status': 'ACCEPT_WITH_LIMITATION',
             'clocks': {'observed_at': None,
                        'release_at': None,
                        'available_at': None,
                        'ingested_at': None,
                        'stored_row_clock': None,
                        'captured_at': None,
                        'artifact_captured_at': None,
                        'original_publication_time': None,
                        'pit_possession': None},
             'clock_status': 'ACCEPT_WITH_LIMITATION',
             'clock_reason': 'publication_or_pit_unknown',
             'versions': [],
             'version_status': 'ACCEPT_WITH_LIMITATION',
             'version_reason': 'version_or_comparison_unknown',
             'source_truth': 'unknown',
             'receipts': 'declared_references_not_verified',
             'scope': 'renderer_declarations_not_storage_or_historical_possession'},
 'verification_state': 'unknown',
 'unverified_flag': None,
 'flag_scope': 'upstream_declaration_only',
 'source_truth': 'unknown',
 'freshness': 'unknown'}

FLAG_CASES = [
    pytest.param({}, None, id="missing"),
    pytest.param({"unverified": False}, False, id="false"),
    pytest.param({"unverified": "false"}, None, id="malformed"),
    pytest.param({"unverified": True}, True, id="true"),
]


def footer_for(snapshot, narrative="Observation 0.125."):
    diagnostics = publication._publication_stage_diagnostics(
        narrative, narrative, narrative, "supplied 0.125.", snapshot, None
    )
    return publication._publication_evidence_footer(
        snapshot, diagnostics, narrative_span=(0, len(narrative))
    )


def decode_footer(content):
    block = content.split("```json\n", 1)[1].split("\n```", 1)[0]
    return json.loads(block)


def expected_record(flag):
    record = deepcopy(EXPECTED_RECORD)
    record["unverified_flag"] = flag
    return record


@pytest.mark.parametrize("extra,flag", FLAG_CASES)
def test_native_full_record_and_rendered_json_preserve_unknowns(extra, flag):
    snapshot = {"features": {"alpha": {"value": 0.125, "date": "2026-10-23", **extra}}}
    original = deepcopy(snapshot)
    footer = footer_for(snapshot)
    # Real renderer, real numeric diagnostics and JSON consumer. No AST loader.
    assert footer["records"] == [expected_record(flag)]
    assert decode_footer(footer["rendered_text"])["records"] == [expected_record(flag)]
    assert snapshot == original
    assert footer["snapshot_presence_is_grounding_permission"] is False
    assert footer["matching_is_source_verification"] is False


def test_native_equal_value_keys_and_historical_baseline_survive_serialization():
    snapshot = {"features": {
        "alpha": {"value": 0.125, "date": "2026-10-23", "baseline_date": "2020-01-02",
                  "selection_policy": "fixed_reference_date"},
        "beta": {"value": 0.125, "date": "2026-10-23"},
    }}
    original = deepcopy(snapshot)
    records = decode_footer(footer_for(snapshot)["rendered_text"])["records"]
    assert [r["feature_key"] for r in records] == ["alpha", "beta"]
    assert records[0]["baseline_date"] == "2020-01-02"
    assert records[0]["observation_date"] == "2026-10-23"
    assert records[0]["freshness"] == "unknown"
    assert records[1]["selection_policy"] == "unknown"
    assert snapshot == original


@pytest.mark.parametrize("changes", [
    pytest.param({}, id="absent-legacy-metric"),
    pytest.param({"metric_contract": None, "metric_assertion": None}, id="null-legacy-metric"),
])
def test_native_optional_legacy_definitions_do_not_imply_verified(changes):
    info = {"value": 0.125, "date": "2026-10-23", **changes}
    original = deepcopy(info)
    record = footer_for({"features": {"alpha": info}})["records"][0]
    assert record == expected_record(None)
    engine = publication.MarketBriefingEngine.__new__(publication.MarketBriefingEngine)
    rendered = engine._format_evidence_value("features", "alpha", info)
    assert "unit=unknown" in rendered
    assert "metric_contract_status=PASS" not in rendered
    assert info == original


def test_native_storage_capture_clock_does_not_create_publication_or_pit():
    snapshot = {"features": {"alpha": {
        "value": 0.125, "date": "2026-10-23",
        "lineage": {"clocks": {"stored_row_clock": "2030-01-02T01:00:00Z",
                               "captured_at": "2030-01-02T02:00:00Z"}},
    }}}
    original = deepcopy(snapshot)
    record = decode_footer(footer_for(snapshot)["rendered_text"])["records"][0]
    clocks = record["lineage"]["clocks"]
    assert clocks["stored_row_clock"] == "2030-01-02T01:00:00Z"
    assert clocks["captured_at"] == "2030-01-02T02:00:00Z"
    assert clocks["original_publication_time"] is None
    assert clocks["pit_possession"] is None
    assert record["lineage"]["clock_status"] == "ACCEPT_WITH_LIMITATION"
    assert record["lineage"]["versions"] == []
    assert record["lineage"]["version_status"] == "ACCEPT_WITH_LIMITATION"
    assert record["verification_state"] == record["source_truth"] == "unknown"
    assert snapshot == original


@pytest.mark.parametrize("narrative,selected", [
    pytest.param("Observation 999.25.", False, id="warning-only-collision-refused"),
    pytest.param("Observation 100 and 999.25.", True, id="retained-value-under-warning"),
])
def test_native_guard_warning_cannot_supply_footer_eligibility(narrative, selected):
    snapshot = {"features": {"alpha": {"value": 100}}}
    original = deepcopy(snapshot)
    guarded = check_numbers_grounded(narrative, "supplied 100.")
    # The retained narrative range excludes generated warning/footer text.
    retained = narrative.replace("999.25", "999.25 [unverified]")
    emitted = guarded.annotated_text
    assert retained in emitted
    start = emitted.index(retained)
    diagnostics = publication._publication_stage_diagnostics(
        narrative, narrative, emitted, "supplied 100.", snapshot, None
    )
    footer = publication._publication_evidence_footer(
        snapshot, diagnostics, narrative_span=(start, start + len(retained))
    )
    assert bool(footer["records"]) is selected
    if selected:
        assert [r["feature_key"] for r in footer["records"]] == ["alpha"]
        assert any(emitted[m["start"]:m["end"]] == "100" for m in footer["mentions"])
    assert snapshot == original


@pytest.fixture
def native_publication_engine():
    # No config fallback: never connect to the application's default DB.
    if os.environ.get("GRID_OCT25_REQUIRE_PG") != "1":
        pytest.skip("PostgreSQL compatibility requires focused GRID_OCT25_REQUIRE_PG=1")
    pg_major = os.environ.get("GRID_OCT25_PG_MAJOR")
    assert pg_major in ("14", "15"), "EXPLICIT_PG14_OR_PG15_SELECTOR_REQUIRED"
    url = os.environ.get("GRID_OCT25_TEST_DB_URL")
    assert url, "EXPLICIT_HOSTED_PG_URL_REQUIRED"
    target = make_url(url)
    assert target.get_backend_name() == "postgresql", "POSTGRES_REQUIRED"
    assert (target.host, target.port, target.database, target.username) == (
        "127.0.0.1", 15432, "oct25_native_test", "oct25_ci_writer"
    ), "DISPOSABLE_HOSTED_IDENTITY_REQUIRED"
    assert not target.query, "UNREVIEWED_CONNECTION_OPTIONS_REFUSED"
    if os.environ.get("GRID_TEST_DB_URL") != url:
        pytest.fail("SHARED_TEST_URL_MUST_MATCH", pytrace=False)
    for name, value in {"DB_HOST": "127.0.0.1", "DB_PORT": "15432",
                        "DB_NAME": "oct25_native_test", "DB_USER": "oct25_ci_writer"}.items():
        assert os.environ.get(name) == value, "SETTINGS_IDENTITY_MUST_MATCH"
    if not target.password or os.environ.get("DB_PASSWORD") != target.password:
        pytest.fail("SETTINGS_PASSWORD_MUST_MATCH", pytrace=False)
    schema = "pub_native_" + uuid.uuid4().hex
    owner = create_engine(url)
    isolated = None
    try:
        with owner.begin() as conn:
            identity = conn.execute(text(
                "SELECT current_database(), current_user, current_setting('server_version_num')::int, "
                "rolsuper, rolcreaterole, rolcreatedb, rolreplication "
                "FROM pg_roles WHERE rolname=current_user"
            )).one()
            assert tuple(identity[:2]) == ("oct25_native_test", "oct25_ci_writer")
            assert identity[2] // 10000 == int(pg_major), "OBSERVED_PG_MAJOR_MUST_MATCH_SELECTOR"
            assert tuple(identity[3:]) == (False, False, False, False), "LIMITED_WRITER_REQUIRED"
            conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        isolated = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
        # Explicit isolated synthetic test preparation, outside the writer.
        # Root composes the shared preparation owner implementation separately.
        with isolated.begin() as conn:
            conn.execute(text("""
                CREATE TABLE market_briefings (
                    id SERIAL PRIMARY KEY,
                    briefing_type TEXT NOT NULL,
                    briefing_date DATE NOT NULL,
                    content TEXT NOT NULL,
                    sentiment_score REAL,
                    sentiment_label TEXT,
                    sentiment_data JSONB,
                    snapshot_data JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX idx_market_briefings_date ON market_briefings (briefing_date DESC);
                CREATE INDEX idx_market_briefings_type ON market_briefings (briefing_type, briefing_date DESC);
            """))
        yield isolated
    finally:
        if isolated is not None:
            isolated.dispose()
        with owner.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        owner.dispose()


@pytest.mark.parametrize("extra,flag", FLAG_CASES)
def test_native_pg_writer_to_fastapi_latest_keeps_record_and_null_pit(
    native_publication_engine, monkeypatch, extra, flag
):
    from config import settings
    assert (settings.DB_HOST, int(settings.DB_PORT), settings.DB_NAME, settings.DB_USER) == (
        "127.0.0.1", 15432, "oct25_native_test", "oct25_ci_writer"
    ), "ACTUAL_SETTINGS_MUST_BIND_SYNTHETIC_DB"
    from api.auth import require_auth
    from api.routers import briefing

    db_engine = native_publication_engine
    snapshot = {"features": {"alpha": {"value": 0.125, "date": "2026-10-23", **extra}}}
    original = deepcopy(snapshot)
    narrative = "Observation 0.125."
    content = narrative + footer_for(snapshot)["rendered_text"]
    result = {"type": "daily", "content": content, "snapshot": snapshot, "sentiment": None}
    result_before = deepcopy(result)
    engine = publication.MarketBriefingEngine.__new__(publication.MarketBriefingEngine)
    engine.engine = db_engine
    record_id = engine._persist_to_db(result)
    assert type(record_id) is int, "REAL_WRITER_MUST_PERSIST_IN_EPHEMERAL_POSTGRES"
    with db_engine.connect() as conn:
        row = conn.execute(text(
            "SELECT content, snapshot_data, sentiment_data, created_at FROM market_briefings WHERE id=:id"
        ), {"id": record_id}).one()
    assert row[0] == content
    assert row[1] == original
    assert row[2] is None
    assert row[3] is not None  # Storage clock exists, not publication/PIT proof.

    monkeypatch.setattr(briefing, "get_engine", lambda: db_engine)  # DB read input only.
    app = FastAPI()
    app.include_router(briefing.router)
    app.dependency_overrides[require_auth] = lambda: "synthetic-authorized-reader"
    statements = []
    def capture(_conn, _cursor, statement, _params, _context, _many):
        statements.append(statement)
    event.listen(db_engine, "before_cursor_execute", capture)
    try:
        with TestClient(app) as client:
            response = client.get("/api/v1/briefing/latest", params={"briefing_type": "daily"})
            assert response.status_code == 200
            served = response.json()
    finally:
        event.remove(db_engine, "before_cursor_execute", capture)
    assert served["id"] == record_id
    assert served["content"] == content
    assert served["sentiment"] is None
    assert served["created_at"]
    assert decode_footer(served["content"])["records"] == [expected_record(flag)]
    assert len(statements) == 1
    assert statements[0].lstrip().upper().startswith("SELECT")
    assert snapshot == original
    assert result == result_before


@pytest.mark.parametrize("measure", ["open_interest", "traded_volume"])
@pytest.mark.parametrize("change,expected", [("none", "PASS"),
    ("asserted_cohort", "ACCEPT_WITH_LIMITATION"), ("display_value", "ACCEPT_WITH_LIMITATION")])
def test_native_typed_metric_bindings_reach_renderer_and_json(measure, change, expected):
    contract = {"metric_id": "synthetic", "definition_version": "v1", "measure": measure,
                "unit": "ratio", "denominator": "calls", "cohort_id": "population-A",
                "puts": 6, "calls": 3}
    assertion = {k: contract[k] for k in
                 ("metric_id", "definition_version", "measure", "unit", "denominator", "cohort_id")}
    info = {"value": 2, "measure": measure, "unit": "ratio", "denominator": "calls",
            "cohort_id": "population-A", "metric_contract": contract,
            "metric_assertion": {**assertion, "value": 2}}
    if change == "asserted_cohort":
        info["metric_assertion"]["cohort_id"] = "population-B"
    elif change == "display_value":
        info["value"] = 99
    original = deepcopy(info)
    narrative = f"Observation {info['value']:.1f}."
    record = footer_for({"features": {"alpha": info}}, narrative)["records"][0]
    assert record["metric_binding"]["status"] == expected
    engine = publication.MarketBriefingEngine.__new__(publication.MarketBriefingEngine)
    rendered = engine._format_evidence_value("features", "alpha", info)
    assert ("metric_contract_status=PASS" in rendered) is (expected == "PASS")
    assert json.loads(json.dumps(record))["raw_value"] == info["value"]
    assert info == original


@pytest.mark.parametrize("rewrite,expected", [(False, "PASS"), (True, "REFUSE_MISMATCH")])
def test_native_versions_keep_old_identity_without_inventing_pit(rewrite, expected):
    first = {"source_id": "synthetic", "series_id": "alpha", "observation_date": "2026-10-23",
             "version_id": "v1", "content_sha256": "a" * 64,
             "revision_clock": "2030-01-01T00:00:00Z", "contract_receipt_id": "synthetic-v1"}
    second = {**first, "version_id": "v2", "content_sha256": "b" * 64,
              "revision_clock": "2030-01-02T00:00:00Z", "contract_receipt_id": "synthetic-v2"}
    versions = [{**first, "content_sha256": "c" * 64}, second] if rewrite else [first, second]
    info = {"value": 0.125, "date": "2026-10-23", "lineage": {
        "versions": versions, "previous_versions": [deepcopy(first)], "assert_immutable_history": True}}
    original = deepcopy(info)
    record = decode_footer(footer_for({"features": {"alpha": info}})["rendered_text"])["records"][0]
    assert record["lineage"]["versions"] == versions
    assert record["lineage"]["version_status"] == expected
    assert record["lineage"]["clocks"]["original_publication_time"] is None
    assert record["lineage"]["clocks"]["pit_possession"] is None
    assert record["source_truth"] == "unknown"
    assert info == original


@pytest.mark.parametrize("presentation,expected", [
    ({"timezone": "UTC", "date": "2026-10-27", "weekday": "Tuesday"}, "PASS"),
    ({"timezone": "America/Los_Angeles", "date": "2026-10-26", "weekday": "Monday"}, "PASS"),
    ({"date": "2026-10-27", "weekday": "Tuesday"}, "ACCEPT_WITH_LIMITATION"),
    ({"timezone": "UTC", "date": "2026-10-27", "weekday": "Monday"}, "REFUSE_MISMATCH"),
])
def test_native_calendar_json_keeps_narrow_explicit_basis(presentation, expected):
    declaration = {"basis": "gregorian_civil_date", "intent": "instant",
                   "reference_instant": "2026-10-27T00:30:00Z", **presentation}
    original = deepcopy(declaration)
    result = json.loads(json.dumps(publication._publication_calendar_assessment(declaration)))
    assert result["status"] == expected
    assert result["scope"] == "declared_action_calendar_only"
    assert declaration == original


# New persistence-hardening cases: native execution is delegated to existing CI.
def persistence_payload():
    snapshot = {"features": {"alpha": {"value": 0.125, "date": "2026-10-23"}}}
    return {"type": "daily", "content": "Observation 0.125." + footer_for(snapshot)["rendered_text"],
            "snapshot": snapshot, "sentiment": None}


def persistence_writer(db_engine):
    writer = publication.MarketBriefingEngine.__new__(publication.MarketBriefingEngine)
    writer.engine = db_engine
    return writer


def test_prepared_writer_has_no_ddl_and_restores_session_timeouts(native_publication_engine):
    db = native_publication_engine
    payload = persistence_payload()
    original = deepcopy(payload)
    with db.connect() as conn:
        before = tuple(conn.execute(text(
            "SELECT current_setting('lock_timeout'), current_setting('statement_timeout')"
        )).one())
    statements = []
    def capture(_conn, _cursor, statement, _params, _context, _many):
        statements.append(statement)
    event.listen(db, "before_cursor_execute", capture)
    try:
        writer = persistence_writer(db)
        first, second = writer._persist_to_db(payload), writer._persist_to_db(payload)
    finally:
        event.remove(db, "before_cursor_execute", capture)
    assert type(first) is int and type(second) is int and first != second
    assert statements
    assert all(s.lstrip().upper().startswith(("SELECT", "WITH", "INSERT")) for s in statements), "ORDINARY_WRITER_MUST_NOT_DDL"
    assert sum("INSERT INTO market_briefings" in s for s in statements) == 2
    with db.connect() as conn:
        after = tuple(conn.execute(text(
            "SELECT current_setting('lock_timeout'), current_setting('statement_timeout')"
        )).one())
        assert conn.execute(text("SELECT count(*) FROM market_briefings")).scalar_one() == 2
    assert after == before, "TRANSACTION_LOCAL_TIMEOUTS_MUST_NOT_LEAK"
    assert payload == original


@pytest.mark.parametrize("broken", ["missing_relation", "missing_column", "wrong_content_type",
                                    "nullable_content", "missing_id_default"])
def test_invalid_schema_refuses_without_lazy_repair(native_publication_engine, broken):
    db = native_publication_engine
    sql = {"missing_relation": "DROP TABLE market_briefings",
           "missing_column": "ALTER TABLE market_briefings DROP COLUMN snapshot_data",
           "wrong_content_type": "ALTER TABLE market_briefings ALTER COLUMN content TYPE integer USING NULL",
           "nullable_content": "ALTER TABLE market_briefings ALTER COLUMN content DROP NOT NULL",
           "missing_id_default": "ALTER TABLE market_briefings ALTER COLUMN id DROP DEFAULT"}[broken]
    with db.begin() as conn:
        conn.execute(text(sql))
    statements = []
    def capture(_conn, _cursor, statement, _params, _context, _many):
        statements.append(statement)
    event.listen(db, "before_cursor_execute", capture)
    payload = persistence_payload()
    original = deepcopy(payload)
    try:
        assert persistence_writer(db)._persist_to_db(payload) is None, "UNREADY_SCHEMA_MUST_REFUSE"
    finally:
        event.remove(db, "before_cursor_execute", capture)
    assert all(s.lstrip().upper().startswith(("SELECT", "WITH", "INSERT")) for s in statements)
    assert not any("INSERT INTO market_briefings" in s for s in statements), "READINESS_REFUSAL_MUST_PRECEDE_INSERT"
    with db.connect() as conn:
        if broken == "missing_relation":
            assert conn.execute(text("SELECT to_regclass('market_briefings')")).scalar_one() is None
        else:
            assert conn.execute(text("SELECT count(*) FROM market_briefings")).scalar_one() == 0
    assert payload == original


def test_lock_contention_refuses_and_next_transaction_recovers(native_publication_engine):
    import time
    db = native_publication_engine
    writer = persistence_writer(db)
    payload = persistence_payload()
    original = deepcopy(payload)
    with db.connect() as blocker:
        tx = blocker.begin()
        try:
            blocker.execute(text("LOCK TABLE market_briefings IN ACCESS EXCLUSIVE MODE"))
            started = time.monotonic()
            assert writer._persist_to_db(payload) is None, "LOCK_CONTENTION_MUST_NOT_PUBLISH"
            elapsed = time.monotonic() - started
            assert elapsed < 8, "SOURCE_TRANSACTION_BOUND_MUST_BE_EFFECTIVE"
        finally:
            tx.rollback()
    with db.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM market_briefings")).scalar_one() == 0
    assert type(writer._persist_to_db(payload)) is int, "FAILED_TRANSACTION_MUST_RELEASE_AND_RECOVER"
    assert payload == original


def test_statement_timeout_cancels_synthetic_slow_trigger(native_publication_engine):
    import time
    db = native_publication_engine
    with db.begin() as conn:
        conn.execute(text("""
            CREATE FUNCTION synthetic_slow_publication() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN PERFORM pg_sleep(10); RETURN NEW; END $$;
            CREATE TRIGGER synthetic_slow_publication BEFORE INSERT ON market_briefings
                FOR EACH ROW EXECUTE FUNCTION synthetic_slow_publication();
        """))
    payload = persistence_payload()
    original = deepcopy(payload)
    started = time.monotonic()
    assert persistence_writer(db)._persist_to_db(payload) is None, "SLOW_WRITE_MUST_BE_CANCELLED"
    assert time.monotonic() - started < 8
    with db.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM market_briefings")).scalar_one() == 0
    assert payload == original


def test_failed_insert_rolls_back_trigger_effects_and_preserves_original(native_publication_engine):
    db = native_publication_engine
    writer = persistence_writer(db)
    payload = persistence_payload()
    row_id = writer._persist_to_db(payload)
    assert type(row_id) is int
    with db.begin() as conn:
        old = conn.execute(text(
            "SELECT content,snapshot_data,created_at FROM market_briefings WHERE id=:id"
        ), {"id": row_id}).one()
        conn.execute(text("""
            CREATE TABLE synthetic_publication_audit (note TEXT);
            CREATE FUNCTION synthetic_reject_publication() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN INSERT INTO synthetic_publication_audit VALUES ('must rollback');
                RAISE EXCEPTION 'synthetic publication refusal'; END $$;
            CREATE TRIGGER synthetic_reject_publication BEFORE INSERT ON market_briefings
                FOR EACH ROW EXECUTE FUNCTION synthetic_reject_publication();
        """))
    original = deepcopy(payload)
    assert writer._persist_to_db(payload) is None
    with db.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM synthetic_publication_audit")).scalar_one() == 0
        assert conn.execute(text("SELECT count(*) FROM market_briefings")).scalar_one() == 1
        current = conn.execute(text(
            "SELECT content,snapshot_data,created_at FROM market_briefings WHERE id=:id"
        ), {"id": row_id}).one()
    assert tuple(current) == tuple(old), "HISTORICAL_CONTENT_AND_STORAGE_CLOCK_MUST_REMAIN_IMMUTABLE"
    assert decode_footer(current[0])["records"] == [expected_record(None)]
    assert payload == original


def test_deferred_commit_failure_cannot_acknowledge_publication(native_publication_engine):
    db = native_publication_engine
    with db.begin() as conn:
        conn.execute(text("""
            CREATE FUNCTION synthetic_reject_publication_commit() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'synthetic commit refusal'; END $$;
            CREATE CONSTRAINT TRIGGER synthetic_reject_publication_commit
                AFTER INSERT ON market_briefings DEFERRABLE INITIALLY DEFERRED
                FOR EACH ROW EXECUTE FUNCTION synthetic_reject_publication_commit();
        """))
    payload = persistence_payload()
    original = deepcopy(payload)
    assert persistence_writer(db)._persist_to_db(payload) is None, "COMMIT_FAILURE_MUST_NOT_ACKNOWLEDGE_ROW"
    with db.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM market_briefings")).scalar_one() == 0
    assert payload == original


@pytest.mark.parametrize("broken", ["constant_id", "missing_timestamp_default", "constant_timestamp",
                                    "missing_primary_key", "composite_primary_key"])
def test_functional_generation_and_primary_key_refuse_before_insert(native_publication_engine, broken):
    db = native_publication_engine
    sql = {
        "constant_id": "ALTER TABLE market_briefings ALTER COLUMN id SET DEFAULT 7",
        "missing_timestamp_default": "ALTER TABLE market_briefings ALTER COLUMN created_at DROP DEFAULT",
        "constant_timestamp": "ALTER TABLE market_briefings ALTER COLUMN created_at SET DEFAULT TIMESTAMPTZ '2000-01-01T00:00:00Z'",
        "missing_primary_key": "ALTER TABLE market_briefings DROP CONSTRAINT market_briefings_pkey",
        "composite_primary_key": "ALTER TABLE market_briefings DROP CONSTRAINT market_briefings_pkey; ALTER TABLE market_briefings ADD PRIMARY KEY (id,briefing_type)",
    }[broken]
    with db.begin() as conn:
        conn.execute(text(sql))
    statements = []
    def capture(_conn, _cursor, statement, _params, _context, _many):
        statements.append(statement)
    event.listen(db, "before_cursor_execute", capture)
    payload = persistence_payload()
    original = deepcopy(payload)
    try:
        assert persistence_writer(db)._persist_to_db(payload) is None
    finally:
        event.remove(db, "before_cursor_execute", capture)
    assert not any("INSERT INTO market_briefings" in s for s in statements), "FUNCTIONAL_READINESS_MUST_REFUSE_BEFORE_INSERT"
    with db.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM market_briefings")).scalar_one() == 0
    assert payload == original


def test_optional_performance_indexes_are_not_write_authority(native_publication_engine):
    db = native_publication_engine
    with db.begin() as conn:
        conn.execute(text("DROP INDEX idx_market_briefings_date; DROP INDEX idx_market_briefings_type"))
    payload = persistence_payload()
    assert type(persistence_writer(db)._persist_to_db(payload)) is int
    with db.connect() as conn:
        assert conn.execute(text("SELECT to_regclass('idx_market_briefings_date')")).scalar_one() is None
        assert conn.execute(text("SELECT to_regclass('idx_market_briefings_type')")).scalar_one() is None
    assert decode_footer(payload["content"])["records"] == [expected_record(None)]


def test_prior_schema_readiness_does_not_authorize_actual_other_connection(native_publication_engine):
    from intelligence.schema_readiness import require_relation, ColumnRequirement
    db = native_publication_engine
    # Prior A observation is explicitly NOT passed as authorization to writer B.
    with db.connect() as conn:
        schema_a = require_relation(conn, "market_briefings", {"id": ColumnRequirement("int4")})
    schema_b = "pub_other_" + uuid.uuid4().hex
    actual = None
    try:
        with db.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA "{schema_b}"'))
            conn.execute(text(f'CREATE TABLE "{schema_b}".market_briefings (LIKE market_briefings INCLUDING ALL)'))
            conn.execute(text(f'ALTER TABLE "{schema_b}".market_briefings ALTER COLUMN created_at DROP DEFAULT'))
        actual = create_engine(os.environ["GRID_OCT25_TEST_DB_URL"],
                               connect_args={"options": f"-csearch_path={schema_b}"})
        with actual.connect() as conn:
            assert conn.execute(text("SELECT current_schema()")).scalar_one() == schema_b
        assert schema_a != schema_b
        payload = persistence_payload()
        original = deepcopy(payload)
        assert persistence_writer(actual)._persist_to_db(payload) is None, "PRIOR_A_READY_MUST_NOT_AUTHORIZE_B"
        for engine in (db, actual):
            with engine.connect() as conn:
                assert conn.execute(text("SELECT count(*) FROM market_briefings")).scalar_one() == 0
        assert payload == original
    finally:
        if actual is not None:
            actual.dispose()
        with db.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_b}" CASCADE'))
