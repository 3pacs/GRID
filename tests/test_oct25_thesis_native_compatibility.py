"""Native full-module/PostgreSQL compatibility; invented rows only.

Ordinary collection skips without opt-in. With opt-in, missing setup fails.
No AST extraction, application-module stubs, provider traffic, or outcome runs.
"""
from __future__ import annotations

import json
import os
import socket
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool

pytestmark = [
    pytest.mark.xdist_group("postgres"),
    pytest.mark.timeout(60),
    pytest.mark.skipif(
        os.environ.get("GRID_OCT25_REQUIRE_PG") is None,
        reason="Oct25 native PG14 compatibility is opt-in; set GRID_OCT25_REQUIRE_PG=1 in the focused validation job",
    ),
]


def _test_url():
    assert os.environ.get("GRID_OCT25_REQUIRE_PG") == "1"
    raw = os.environ.get("GRID_OCT25_TEST_DB_URL")
    assert raw, "GRID_OCT25_TEST_DB_URL required; no default DB and no skip"
    url = make_url(raw)
    assert url.drivername in {"postgresql", "postgresql+psycopg2"}
    assert (url.host, url.port, url.database, url.username) == (
        "127.0.0.1", 15432, "oct25_native_test", "oct25_ci_writer")
    assert not url.query, "URL connection overrides are outside the test envelope"
    assert make_url(os.environ["GRID_TEST_DB_URL"]) == url
    assert (os.environ["DB_HOST"], os.environ["DB_PORT"], os.environ["DB_NAME"], os.environ["DB_USER"]) == (
        "127.0.0.1", "15432", "oct25_native_test", "oct25_ci_writer")
    assert os.environ["DB_PASSWORD"] == url.password
    return url


@pytest.fixture
def native_modules(monkeypatch):
    # Existing config.py gate, verified at a22a1ef. Must be set by the CI
    # process before collection too, since other test modules may import config.
    assert os.environ.get("GRID_GIT_SINK_DISABLED") == "1"
    assert os.environ.get("GRID_OCT25_REQUIRE_PG") == "1"
    assert os.environ.get("GIT_SINK_PUSH_ENABLED", "").lower() not in {"1", "true", "yes"}
    _test_url()  # Validate all routing before any application import.

    def no_network(*args, **kwargs):
        raise AssertionError("provider/network traffic forbidden in synthetic thesis fixture")

    # psycopg2's libpq uses the separately guarded PG connection. Python
    # provider sockets cannot escape; HTTP TestClient is an in-process transport.
    monkeypatch.setattr(socket.socket, "connect", no_network)
    monkeypatch.setattr(socket.socket, "connect_ex", no_network)
    monkeypatch.setattr(socket, "create_connection", no_network)
    from analysis import flow_thesis_scoring, thesis_scorer
    from intelligence import thesis_tracker
    assert thesis_tracker.THESIS_SCORING_HELD is True
    return flow_thesis_scoring, thesis_scorer, thesis_tracker


@pytest.fixture
def native_pg_engine(native_modules):
    """Per-test UUID schema on the explicitly opted-in nonprivileged CI DB."""
    url = _test_url()
    from config import settings
    assert make_url(settings.DB_URL) == url.set(drivername="postgresql")
    schema = "oct25_thesis_" + uuid4().hex
    owner = create_engine(url, poolclass=NullPool, connect_args={"connect_timeout": 5})
    scoped = None
    created = False
    try:
        with owner.begin() as conn:
            identity = conn.execute(text("""
                SELECT current_database(), current_user,
                       current_setting('server_version_num')::int,
                       r.rolsuper, r.rolcreaterole, r.rolcreatedb, r.rolreplication,
                       pg_get_userbyid(d.datdba) = current_user
                FROM pg_roles r, pg_database d
                WHERE r.rolname = current_user AND d.datname = current_database()
            """)).one()
            assert identity[:2] == ("oct25_native_test", "oct25_ci_writer")
            major = int(os.environ['GRID_OCT25_PG_MAJOR'])
            assert major in (14, 15)
            assert major * 10000 <= identity[2] < (major + 1) * 10000
            assert tuple(identity[3:]) == (False, False, False, False, True)
            conn.execute(text(f'CREATE SCHEMA "{schema}"'))
            created = True
        scoped = create_engine(url, poolclass=NullPool, connect_args={
            "connect_timeout": 5,
            "options": f"-csearch_path={schema} -cstatement_timeout=10000 -clock_timeout=5000",
        })
        with scoped.connect() as conn:
            assert conn.execute(text("SELECT current_schema()")).scalar_one() == schema
        from intelligence.thesis_schema import prepare_thesis_schema
        prepare_thesis_schema(scoped, schema)
        yield scoped
    finally:
        if scoped is not None:
            scoped.dispose()
        if created:
            with owner.begin() as conn:
                conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        owner.dispose()


def test_generator_storage_history_http(native_modules, native_pg_engine, monkeypatch):
    flow, _, tracker = native_modules
    engine = native_pg_engine
    tracker._ensure_tables(engine)
    # Only external current-state provider inputs are replaced. Generator,
    # learned-weight read, tracker DDL/SQL, history and serializer are real.
    directions = {"large_bull": ("bullish", "high"), "bear_a": ("bearish", "low"),
                  "bear_b": ("bearish", "low"), "neutral": ("neutral", "moderate")}
    monkeypatch.setattr(flow, "FLOW_KNOWLEDGE", {
        key: {"confidence": confidence, "thesis": "invented", "source": "synthetic"}
        for key, (_, confidence) in directions.items()})
    monkeypatch.setattr(flow, "_STATE_UPDATERS", {
        key: (lambda _engine, d=direction: {"direction": d, "value": 1, "detail": "invented"})
        for key, (direction, _) in directions.items()})
    assert flow._load_learned_weights(engine) == {}
    thesis = flow.generate_unified_thesis(engine)
    assert (thesis["overall_direction"], thesis["conviction"], thesis["conviction_unit"]) == ("BULLISH", 14, "percent")
    assert "Weighted scores: 3 bull vs 2 bear" in thesis["narrative"]
    assert "not vote counts" in thesis["narrative"]
    assert "not calibrated probability" in thesis["narrative"]
    snapshot_id = tracker.snapshot_thesis(engine, thesis)
    with engine.connect() as conn:
        stored = conn.execute(text("SELECT conviction, outcome, scored_at FROM thesis_snapshots WHERE id=:id"), {"id": snapshot_id}).one()
    assert stored == (Decimal("0.14"), None, None)
    record = tracker.get_thesis_history(engine)[0]
    assert record.id == snapshot_id and record.conviction == 0.14
    assert record.narrative == thesis["narrative"] and record.conviction_unit == "fraction"
    payload = _history_http(engine, monkeypatch)
    assert payload["count"] == 1 and "error" not in payload
    assert payload["snapshots"][0] == record.to_dict()
    signal = _directional_signal(engine)
    assert signal.confidence == 0.14 and signal.value == pytest.approx(14)
    assert signal.metadata["stored_conviction_unit"] == "fraction"


def _history_http(engine, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api.routers import intelligence_thesis as router
    app = FastAPI()
    app.include_router(router.router)
    # Test auth identity and test DB routing; endpoint/read/serialization real.
    app.dependency_overrides[router.require_auth] = lambda: "synthetic-test-identity"
    monkeypatch.setattr(router, "get_db_engine", lambda: engine)
    with TestClient(app) as client:
        response = client.get("/thesis/history?days=90")
    assert response.status_code == 200
    return response.json()


@pytest.mark.parametrize("payload,expected", [
    ({"conviction": 1, "conviction_unit": "percent"}, 0.01),
    ({"conviction": 1, "conviction_unit": "fraction"}, 1.0),
    ({"conviction": 1}, 1.0),
    ({"conviction": "0.99999999999999999", "conviction_unit": "fraction"}, 1.0),
    ({"conviction": Decimal("99.99999999999999999"), "conviction_unit": "percent"}, 1.0),
    ({"conviction": 0}, 0.0), ({}, None),
    ({"conviction": None, "conviction_unit": None}, None),
])
def test_units_null_default_real_persistence(native_modules, native_pg_engine, payload, expected, monkeypatch):
    tracker = native_modules[2]
    identifier = tracker.snapshot_thesis(native_pg_engine, dict(payload))
    records = tracker.get_thesis_history(native_pg_engine)
    assert len(records) == 1 and records[0].id == identifier
    assert records[0].conviction == expected
    unit = "fraction" if expected is not None else None
    assert records[0].to_dict()["conviction_unit"] == unit
    assert json.loads(json.dumps(records[0].to_dict()))["conviction"] == expected
    response = _history_http(native_pg_engine, monkeypatch)["snapshots"][0]
    assert (response["conviction"], response["conviction_unit"]) == (expected, unit)
    signal = _directional_signal(native_pg_engine)
    if expected is None:
        assert signal is None
    else:
        assert signal.confidence == expected and signal.value == expected * 100


@pytest.mark.parametrize("value,unit", [
    ("1.00000000000000001", "fraction"),
    (Decimal("100.00000000000000001"), "percent"),
    ("-1e-999", "fraction"), (float("nan"), "fraction"),
    (Decimal("Infinity"), "percent"), (True, "fraction"),
    (1, None), (1, "unknown"), (75, "fraction"),
])
def test_invalid_before_any_db_statement(native_modules, native_pg_engine, value, unit):
    tracker = native_modules[2]
    statements = []
    def observed(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)
    event.listen(native_pg_engine, "before_cursor_execute", observed)
    try:
        with pytest.raises(ValueError):
            tracker.snapshot_thesis(native_pg_engine, {"conviction": value, "conviction_unit": unit})
        assert statements == [], "invalid input reached table/SQL boundary"
    finally:
        event.remove(native_pg_engine, "before_cursor_execute", observed)


def test_historical_unknown_null_http(native_modules, native_pg_engine, monkeypatch):
    tracker = native_modules[2]
    tracker._ensure_tables(native_pg_engine)
    with native_pg_engine.begin() as conn:
        for value in [None, 0, 1, Decimal("0.75"), 75]:
            conn.execute(text("INSERT INTO thesis_snapshots (overall_direction,conviction,narrative) VALUES ('neutral',:v,'original synthetic record')"), {"v": value})
    payload = _history_http(native_pg_engine, monkeypatch)
    assert payload["count"] == 5 and "error" not in payload
    by_id = sorted(payload["snapshots"], key=lambda row: row["id"])
    assert [row["conviction"] for row in by_id] == [None, 0, 1, 0.75, 75]
    assert all(row["conviction_unit"] is None for row in by_id)
    assert all(row["narrative"] == "original synthetic record" and row["outcome"] is None for row in by_id)
    assert _directional_signal(native_pg_engine) is None


def _directional_signal(engine):
    from intelligence.adapters.thesis_tracker_adapter import ThesisTrackerAdapter
    from intelligence.signal_registry import RegisteredSignal, SignalType
    signals = ThesisTrackerAdapter().extract_signals(engine)
    directional = [s for s in signals if s.signal_type == SignalType.DIRECTIONAL]
    assert len(directional) <= 1
    assert all(isinstance(s, RegisteredSignal) for s in signals)
    return directional[0] if directional else None


@pytest.mark.parametrize("value,unit,expected", [
    (Decimal("0.75"), "fraction", 0.75), (75, "percent", 0.75),
    (1, "percent", 0.01), (1, "fraction", 1.0), (0, "fraction", 0.0),
    (75, None, None), (0, None, None), (None, None, None),
    (Decimal("0.75"), "unknown", None),
    (Decimal("1.00000000000000001"), "fraction", None),
    (Decimal("NaN"), "fraction", None), (101, "percent", None),
    (Decimal("-0.00000000000000001"), "fraction", None),
])
def test_adapter_explicit_receipts_and_refusal(native_modules, native_pg_engine, value, unit, expected):
    tracker = native_modules[2]
    tracker._ensure_tables(native_pg_engine)
    with native_pg_engine.begin() as conn:
        conn.execute(text("INSERT INTO thesis_snapshots (overall_direction,conviction,conviction_unit,narrative) VALUES ('bullish',:v,:u,'invented')"), {"v":value,"u":unit})
    signal = _directional_signal(native_pg_engine)
    if expected is None:
        assert signal is None
    else:
        assert signal.confidence == expected and signal.value == expected * 100
        assert signal.metadata["conviction_unit"] == "percent"
        assert signal.metadata["confidence_unit"] == "fraction"


def test_additive_receipt_preserves_legacy_and_accuracy_denominator(native_modules, native_pg_engine):
    from intelligence.adapters.thesis_tracker_adapter import ThesisTrackerAdapter
    from intelligence.signal_registry import SignalType
    tracker = native_modules[2]
    tracker._ensure_tables(native_pg_engine)
    with native_pg_engine.begin() as conn:
        conn.execute(text("ALTER TABLE thesis_snapshots DROP COLUMN conviction_unit"))
        conn.execute(text("INSERT INTO thesis_snapshots (overall_direction,conviction) VALUES ('bullish',75)"))
        for actual in ['bullish','bullish','bearish',None,None]:
            conn.execute(text("INSERT INTO thesis_postmortems (thesis_direction,actual_direction) VALUES ('bullish',:a)"), {"a":actual})
    # Actual old schema produces no fabricated directional signal; accuracy's
    # original total includes the two unknown actual directions.
    signals = ThesisTrackerAdapter().extract_signals(native_pg_engine)
    assert len(signals) == 1 and signals[0].signal_type == SignalType.MAGNITUDE
    assert signals[0].value == 0.4 and signals[0].metadata["total"] == 5
    from intelligence.schema_readiness import SchemaNotReady
    from intelligence.thesis_schema import prepare_thesis_schema
    with pytest.raises(SchemaNotReady):
        tracker.snapshot_thesis(native_pg_engine, {"conviction":75,"conviction_unit":"percent"})
    with native_pg_engine.connect() as conn:
        schema = conn.execute(text("SELECT current_schema()")).scalar_one()
    prepare_thesis_schema(native_pg_engine, schema)
    tracker.snapshot_thesis(native_pg_engine, {"conviction":75,"conviction_unit":"percent"})
    with native_pg_engine.connect() as conn:
        rows = conn.execute(text("SELECT conviction,conviction_unit FROM thesis_snapshots ORDER BY id")).all()
    assert rows == [(Decimal(75),None),(Decimal("0.75"),"fraction")]


def test_scorer_writer_receipt_to_adapter(native_modules, native_pg_engine, monkeypatch):
    scorer, tracker = native_modules[1:]
    thesis = {"direction":"BULLISH","conviction":75,"score":75,"bull_pct":87.5,
              "bear_pct":12.5,"evaluation_window":"5d","active_models":0,"total_models":0,"models":[]}
    identifier = scorer.snapshot_thesis(native_pg_engine, thesis)
    assert identifier is not None
    record = tracker.get_thesis_history(native_pg_engine)[0]
    assert record.conviction == 0.75 and record.conviction_unit == "fraction"
    response = _history_http(native_pg_engine, monkeypatch)["snapshots"][0]
    assert response["conviction"] == 0.75 and response["conviction_unit"] == "fraction"
    assert _directional_signal(native_pg_engine).confidence == 0.75


@pytest.mark.parametrize("payload", [
    {"conviction":101}, {"conviction":None}, {"conviction":float("nan")},
    {"conviction":1,"conviction_unit":"fraction"}, {"conviction":1,"conviction_unit":None},
])
def test_scorer_writer_invalid_unknown_before_effects(native_modules, native_pg_engine, payload):
    statements = []
    def observed(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)
    event.listen(native_pg_engine, "before_cursor_execute", observed)
    try:
        assert native_modules[1].snapshot_thesis(native_pg_engine, payload) is None
        assert statements == []
    finally:
        event.remove(native_pg_engine, "before_cursor_execute", observed)


@pytest.mark.parametrize("directions,expected_counts,expected_score", [
    (["bullish", "bullish", "bearish", "neutral", "other"], (2, 1, 1, 1), 7.2),
    (["bullish"]*4+["bearish"]*4+["neutral"]*2, (4, 4, 2, 0), 0),
    (["other", None], (0, 0, 0, 2), 0),
])
def test_category_denominator_native_sql(native_modules, native_pg_engine, directions, expected_counts, expected_score):
    scorer = native_modules[1]
    with native_pg_engine.begin() as conn:
        conn.execute(text("CREATE TABLE signal_registry (direction TEXT, confidence NUMERIC, value NUMERIC, valid_from TIMESTAMPTZ, source_module TEXT, signal_type TEXT)"))
        for direction in directions:
            conn.execute(text("INSERT INTO signal_registry VALUES (:d,:c,1,NOW(),'discovery.changepoint_detector','regime_change')"), {"d": direction, "c": 0 if direction in ("other", None) else 0.75})
    verdict = scorer._score_regime_changepoints(native_pg_engine, 0.5)
    counts = verdict["category_counts"]
    assert tuple(counts[k] for k in ("bullish", "bearish", "neutral", "other_unknown")) == expected_counts
    assert counts["total"] == len(directions)
    assert counts["known_categories_exhaustive"] == (expected_counts[-1] == 0)
    assert verdict["score"] == expected_score
    if expected_score == 7.2:
        assert verdict["confidence"] == 72.5
    assert "other/unknown" in verdict["data_point"]


@pytest.mark.parametrize('caller', ['snapshot_thesis', 'generate_thesis_postmortem',
    'get_thesis_history', 'get_thesis_accuracy', 'run_thesis_cycle', 'load_thesis_postmortems'])
def test_missing_schema_refuses_before_caller_work(native_modules, native_pg_engine, caller):
    from intelligence.schema_readiness import SchemaNotReady
    with native_pg_engine.begin() as conn:
        conn.execute(text('ALTER TABLE thesis_snapshots DROP COLUMN conviction_unit'))
    statements=[]
    def record(conn,cursor,statement,parameters,context,executemany): statements.append(statement)
    event.listen(native_pg_engine,'before_cursor_execute',record)
    args = ({'conviction':0},) if caller=='snapshot_thesis' else ((1,) if caller=='generate_thesis_postmortem' else ())
    try:
        with pytest.raises(SchemaNotReady):
            getattr(native_modules[2],caller)(native_pg_engine,*args)
        assert statements and all(s.lstrip().startswith('SELECT') for s in statements)
    finally:
        event.remove(native_pg_engine,'before_cursor_execute',record)


def test_missing_schema_second_writer_refuses(native_modules,native_pg_engine):
    with native_pg_engine.begin() as conn:
        conn.execute(text('ALTER TABLE thesis_snapshots DROP COLUMN conviction_unit'))
    assert native_modules[1].snapshot_thesis(native_pg_engine,{'conviction':75}) is None
    with native_pg_engine.connect() as conn:
        assert conn.execute(text('SELECT count(*) FROM thesis_snapshots')).scalar_one()==0


@pytest.mark.parametrize('change', [
    'ALTER COLUMN conviction_unit TYPE INTEGER USING NULL',
    'ALTER COLUMN conviction_unit SET NOT NULL',
    "ALTER COLUMN conviction_unit SET DEFAULT 'fraction'",
])
def test_incompatible_receipt_refuses_readiness_and_preparation(native_modules,native_pg_engine,change):
    from intelligence.schema_readiness import SchemaNotReady
    from intelligence.thesis_schema import prepare_thesis_schema
    with native_pg_engine.begin() as conn:
        schema=conn.execute(text('SELECT current_schema()')).scalar_one()
        conn.execute(text('ALTER TABLE thesis_snapshots '+change))
    with pytest.raises(SchemaNotReady): native_modules[2]._ensure_tables(native_pg_engine)
    with pytest.raises(SchemaNotReady): prepare_thesis_schema(native_pg_engine,schema)


def test_explicit_preparation_rolls_back_partial_ddl(native_modules,native_pg_engine):
    from intelligence.thesis_schema import prepare_thesis_schema
    with native_pg_engine.begin() as conn:
        schema=conn.execute(text('SELECT current_schema()')).scalar_one()
        conn.execute(text('ALTER TABLE thesis_snapshots DROP COLUMN conviction_unit'))
    def fail_after_addition(conn,cursor,statement,parameters,context,executemany):
        if 'CREATE INDEX' in statement: raise RuntimeError('invented post-addition failure')
    event.listen(native_pg_engine,'before_cursor_execute',fail_after_addition)
    try:
        with pytest.raises(RuntimeError,match='invented'): prepare_thesis_schema(native_pg_engine,schema)
    finally: event.remove(native_pg_engine,'before_cursor_execute',fail_after_addition)
    with native_pg_engine.connect() as conn:
        assert not conn.execute(text("SELECT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='thesis_snapshots'::regclass AND attname='conviction_unit' AND NOT attisdropped)")).scalar_one()


def test_explicit_preparation_contention_fails_without_retry(native_modules,native_pg_engine):
    from intelligence.thesis_schema import prepare_thesis_schema
    from sqlalchemy.exc import DBAPIError
    attempted=[]
    def record(conn,cursor,statement,parameters,context,executemany):
        if statement.lstrip().startswith(('ALTER TABLE','CREATE TABLE','CREATE INDEX')): attempted.append(statement)
    with native_pg_engine.begin() as blocker:
        schema=blocker.execute(text('SELECT current_schema()')).scalar_one()
        blocker.execute(text('LOCK TABLE thesis_snapshots IN ACCESS EXCLUSIVE MODE'))
        event.listen(native_pg_engine,'before_cursor_execute',record)
        try:
            with pytest.raises(DBAPIError) as exc: prepare_thesis_schema(native_pg_engine,schema)
            assert exc.value.orig.pgcode=='55P03'
            assert attempted.count(exc.value.statement)==1
            assert len(attempted)<=2
        finally: event.remove(native_pg_engine,'before_cursor_execute',record)
    native_modules[2]._ensure_tables(native_pg_engine)


def test_explicit_statement_timeout_rolls_back(native_modules,native_pg_engine):
    from intelligence.schema_readiness import bounded_schema_transaction
    from sqlalchemy.exc import DBAPIError
    with pytest.raises(DBAPIError) as exc:
        with bounded_schema_transaction(native_pg_engine) as (conn,execute):
            execute("INSERT INTO thesis_snapshots(overall_direction) VALUES('neutral')")
            execute('SELECT pg_catalog.pg_sleep(3)')
    assert exc.value.orig.pgcode=='57014'
    with native_pg_engine.connect() as conn:
        assert conn.execute(text('SELECT count(*) FROM thesis_snapshots')).scalar_one()==0


def test_nonowner_runtime_can_write_read_without_ddl(native_modules,native_pg_engine,monkeypatch):
    from sqlalchemy.exc import DBAPIError
    from intelligence.thesis_schema import prepare_thesis_schema
    raw=os.environ.get('GRID_OCT25_RUNTIME_DB_URL')
    assert raw, 'explicit synthetic nonowner runtime URL required'
    url=make_url(raw)
    assert url.set(username='oct25_ci_writer',password=_test_url().password)==_test_url()
    assert url.username=='oct25_ci_runtime' and url.password and url.password!=_test_url().password
    with native_pg_engine.begin() as conn:
        schema=conn.execute(text('SELECT current_schema()')).scalar_one()
        conn.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO oct25_ci_runtime'))
        conn.execute(text(f'GRANT SELECT,INSERT ON ALL TABLES IN SCHEMA "{schema}" TO oct25_ci_runtime'))
        conn.execute(text(f'GRANT USAGE ON ALL SEQUENCES IN SCHEMA "{schema}" TO oct25_ci_runtime'))
    runtime=create_engine(url,poolclass=NullPool,connect_args={'connect_timeout':5,'options':f'-csearch_path={schema} -cstatement_timeout=10000 -clock_timeout=5000'})
    try:
        with runtime.connect() as conn:
            identity=conn.execute(text("SELECT current_user,rolsuper,rolcreatedb,rolcreaterole,rolreplication FROM pg_roles WHERE rolname=current_user")).one()
            assert tuple(identity)==('oct25_ci_runtime',False,False,False,False)
            assert conn.execute(text("SELECT NOT has_schema_privilege(current_user,current_schema(),'CREATE')")).scalar_one()
        native_modules[2].snapshot_thesis(runtime,{'conviction':75,'conviction_unit':'percent'})
        assert native_modules[2].get_thesis_history(runtime)[0].conviction==0.75
        assert _history_http(runtime,monkeypatch)['snapshots'][0]['conviction_unit']=='fraction'
        assert _directional_signal(runtime).confidence==0.75
        with pytest.raises(DBAPIError) as exc: prepare_thesis_schema(runtime,schema)
        assert exc.value.orig.pgcode=='42501'
    finally: runtime.dispose()


def test_held_scoring_stays_before_readiness(native_modules,native_pg_engine):
    with native_pg_engine.begin() as conn:
        conn.execute(text('ALTER TABLE thesis_snapshots DROP COLUMN conviction_unit'))
    assert native_modules[2].score_old_theses(native_pg_engine)['held'] is True


def test_ordinary_ready_paths_never_issue_ddl(native_modules,native_pg_engine):
    statements=[]
    def record(conn,cursor,statement,parameters,context,executemany): statements.append(statement)
    event.listen(native_pg_engine,'before_cursor_execute',record)
    try:
        native_modules[2].snapshot_thesis(native_pg_engine,{'conviction':0})
        native_modules[2].get_thesis_history(native_pg_engine)
        assert statements and not any(s.lstrip().upper().startswith(('CREATE','ALTER','DROP','SET')) for s in statements)
    finally: event.remove(native_pg_engine,'before_cursor_execute',record)


@pytest.mark.parametrize('change',[
    'ALTER TABLE thesis_snapshots ALTER COLUMN id DROP DEFAULT',
    'ALTER TABLE thesis_snapshots ALTER COLUMN timestamp DROP DEFAULT',
    'ALTER TABLE thesis_postmortems ALTER COLUMN id DROP DEFAULT',
    'ALTER TABLE thesis_postmortems ALTER COLUMN generated_at DROP DEFAULT',
    'ALTER TABLE thesis_snapshots DROP CONSTRAINT thesis_snapshots_pkey CASCADE',
    'ALTER TABLE thesis_postmortems DROP CONSTRAINT thesis_postmortems_pkey',
    'ALTER TABLE thesis_postmortems DROP CONSTRAINT thesis_postmortems_snapshot_id_fkey',
])
def test_functional_schema_contract_refusal(native_modules,native_pg_engine,change):
    from intelligence.schema_readiness import SchemaNotReady
    from intelligence.thesis_schema import prepare_thesis_schema
    with native_pg_engine.begin() as conn:
        schema=conn.execute(text('SELECT current_schema()')).scalar_one()
        conn.execute(text(change))
    with pytest.raises(SchemaNotReady):native_modules[2]._ensure_tables(native_pg_engine)
    with pytest.raises(SchemaNotReady):prepare_thesis_schema(native_pg_engine,schema)


@pytest.mark.parametrize('caller',['snapshot','history','second_writer','adapter'])
def test_actual_operation_connection_namespace_is_bound(native_modules,native_pg_engine,caller):
    from intelligence.schema_readiness import SchemaNotReady
    from intelligence.thesis_schema import prepare_thesis_schema
    other='oct25_thesis_'+uuid4().hex
    with native_pg_engine.begin() as conn:
        primary=conn.execute(text('SELECT current_schema()')).scalar_one()
        conn.execute(text(f'CREATE SCHEMA "{other}"'))
    try:
        prepare_thesis_schema(native_pg_engine,other)
        connections=[]; statements=[]
        def select_namespace(dbapi,record):
            chosen=primary if not connections else other
            connections.append(chosen)
            cursor=dbapi.cursor()
            try:cursor.execute(f'SET search_path TO "{chosen}"')
            finally:cursor.close()
        def observe(conn,cursor,statement,parameters,context,executemany):statements.append(statement)
        event.listen(native_pg_engine,'connect',select_namespace)
        event.listen(native_pg_engine,'before_cursor_execute',observe)
        try:
            if caller=='second_writer':
                assert native_modules[1].snapshot_thesis(native_pg_engine,{'conviction':75,'direction':'BULLISH','models':[]}) is None
            else:
                with pytest.raises(SchemaNotReady,match='outside'):
                    if caller=='snapshot':native_modules[2].snapshot_thesis(native_pg_engine,{'conviction':0})
                    elif caller=='history':native_modules[2].get_thesis_history(native_pg_engine)
                    else:
                        from datetime import datetime,timezone
                        from intelligence.adapters.thesis_tracker_adapter import ThesisTrackerAdapter
                        ThesisTrackerAdapter()._build_signals(native_pg_engine,datetime.now(timezone.utc))
            assert connections==[primary,other]
            assert not any(s.lstrip().startswith('INSERT') for s in statements)
        finally:
            event.remove(native_pg_engine,'connect',select_namespace)
            event.remove(native_pg_engine,'before_cursor_execute',observe)
        with native_pg_engine.connect() as conn:
            assert conn.execute(text(f'SELECT count(*) FROM "{other}".thesis_snapshots')).scalar_one()==0
    finally:
        with native_pg_engine.begin() as conn:conn.execute(text(f'DROP SCHEMA "{other}" CASCADE'))


def test_combined_preparation_supports_actual_thesis_and_publication_writers(native_modules,native_pg_engine):
    from intelligence.runtime_schema import prepare_runtime_schema
    with native_pg_engine.connect() as conn:
        schema=conn.execute(text('SELECT current_schema()')).scalar_one()
    result=prepare_runtime_schema(native_pg_engine,thesis_schema=schema,publication_schema=schema)
    assert result['state']=='ready'
    native_modules[2].snapshot_thesis(native_pg_engine,{'conviction':75,'conviction_unit':'percent'})
    from ollama.market_briefing import MarketBriefingEngine
    publication=MarketBriefingEngine.__new__(MarketBriefingEngine)
    publication.engine=native_pg_engine
    payload={'type':'synthetic','content':'invented combined preparation',
             'sentiment':None,'snapshot':{'publication_time':None,'pit':'UNKNOWN'}}
    assert type(publication._persist_to_db(payload)) is int
    with native_pg_engine.connect() as conn:
        content,snapshot=conn.execute(text('SELECT content,snapshot_data FROM market_briefings')).one()
        assert content==payload['content'] and snapshot==payload['snapshot']
        assert conn.execute(text('SELECT conviction_unit FROM thesis_snapshots')).scalar_one()=='fraction'


def test_combined_preparation_late_failure_is_atomic(native_modules,native_pg_engine):
    from intelligence.runtime_schema import prepare_runtime_schema
    with native_pg_engine.begin() as conn:
        schema=conn.execute(text('SELECT current_schema()')).scalar_one()
        conn.execute(text('ALTER TABLE thesis_snapshots DROP COLUMN conviction_unit'))
    def fail_publication_index(conn,cursor,statement,parameters,context,executemany):
        if 'CREATE INDEX' in statement and 'idx_market_briefings_date' in statement:
            raise RuntimeError('invented publication DDL failure')
    event.listen(native_pg_engine,'before_cursor_execute',fail_publication_index)
    try:
        with pytest.raises(RuntimeError,match='invented'):
            prepare_runtime_schema(native_pg_engine,thesis_schema=schema,publication_schema=schema)
    finally:event.remove(native_pg_engine,'before_cursor_execute',fail_publication_index)
    with native_pg_engine.connect() as conn:
        assert conn.execute(text("SELECT to_regclass('market_briefings')")).scalar_one() is None
        assert not conn.execute(text("SELECT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='thesis_snapshots'::regclass AND attname='conviction_unit' AND NOT attisdropped)")).scalar_one()


def test_incompatible_publication_refuses_before_thesis_upgrade(native_modules,native_pg_engine):
    from intelligence.runtime_schema import prepare_runtime_schema
    from intelligence.schema_readiness import SchemaNotReady
    with native_pg_engine.connect() as conn:
        schema=conn.execute(text('SELECT current_schema()')).scalar_one()
    prepare_runtime_schema(native_pg_engine,thesis_schema=schema,publication_schema=schema)
    with native_pg_engine.begin() as conn:
        conn.execute(text('ALTER TABLE thesis_snapshots DROP COLUMN conviction_unit'))
        conn.execute(text('ALTER TABLE market_briefings ALTER COLUMN created_at DROP DEFAULT'))
    statements=[]
    def capture(conn,cursor,statement,parameters,context,executemany):statements.append(statement)
    event.listen(native_pg_engine,'before_cursor_execute',capture)
    try:
        with pytest.raises(SchemaNotReady):
            prepare_runtime_schema(native_pg_engine,thesis_schema=schema,publication_schema=schema)
        assert not any(s.lstrip().startswith(('CREATE','ALTER')) for s in statements)
    finally:event.remove(native_pg_engine,'before_cursor_execute',capture)
