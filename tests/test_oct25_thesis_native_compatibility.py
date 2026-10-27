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
            assert 140000 <= identity[2] < 150000
            assert tuple(identity[3:]) == (False, False, False, False, True)
            conn.execute(text(f'CREATE SCHEMA "{schema}"'))
            created = True
        scoped = create_engine(url, poolclass=NullPool, connect_args={
            "connect_timeout": 5,
            "options": f"-csearch_path={schema} -cstatement_timeout=10000 -clock_timeout=5000",
        })
        with scoped.connect() as conn:
            assert conn.execute(text("SELECT current_schema()")).scalar_one() == schema
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
    assert record.narrative == thesis["narrative"] and record.conviction_unit is None
    payload = _history_http(engine, monkeypatch)
    assert payload["count"] == 1 and "error" not in payload
    assert payload["snapshots"][0] == record.to_dict()


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
def test_units_null_default_real_persistence(native_modules, native_pg_engine, payload, expected):
    tracker = native_modules[2]
    identifier = tracker.snapshot_thesis(native_pg_engine, dict(payload))
    records = tracker.get_thesis_history(native_pg_engine)
    assert len(records) == 1 and records[0].id == identifier
    assert records[0].conviction == expected
    assert records[0].to_dict()["conviction_unit"] is None
    assert json.loads(json.dumps(records[0].to_dict()))["conviction"] == expected


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
