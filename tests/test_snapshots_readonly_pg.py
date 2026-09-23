"""Four snapshot GET routes against isolated disposable PostgreSQL schemas."""

from __future__ import annotations

import os
import uuid
from datetime import date

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, text

from api.auth import require_auth
from api.routers import snapshots as snapshots_router
from store.snapshots import AnalyticalSnapshotStore, clear_category_cache


@pytest.fixture
def isolated_engine():
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        pytest.skip("GRID_TEST_DB_URL is required for disposable PostgreSQL proof")
    admin = create_engine(url)
    schema = "snap_ro_" + uuid.uuid4().hex
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    clear_category_cache()
    try:
        yield engine
    finally:
        clear_category_cache()
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


def _client(monkeypatch, engine):
    monkeypatch.setattr(snapshots_router, "get_db_engine", lambda: engine)
    app = FastAPI()
    app.include_router(snapshots_router.router)
    app.dependency_overrides[require_auth] = lambda: {"sub": "test-user"}
    return TestClient(app)


def _capture_sql(engine):
    statements = []

    def capture(_conn, _cursor, statement, _parameters, _context, _executemany):
        statements.append(statement.strip())

    event.listen(engine, "before_cursor_execute", capture)
    return statements, capture


def _paths():
    d = date.today().isoformat()
    return (
        "/api/v1/snapshots/latest/test",
        "/api/v1/snapshots/history/test",
        f"/api/v1/snapshots/compare/test?date_a={d}&date_b={d}",
        "/api/v1/snapshots/categories",
    )


def test_missing_schema_all_four_gets_are_unavailable_and_select_only(isolated_engine, monkeypatch):
    client = _client(monkeypatch, isolated_engine)
    clear_category_cache()
    statements, capture = _capture_sql(isolated_engine)
    try:
        responses = [client.get(path) for path in _paths()]
        assert [r.status_code for r in responses] == [503] * 4
        assert all(r.json() == {"detail": "snapshot_store_unavailable"} for r in responses)
        assert len(statements) == 4
        assert all(s.upper().startswith("SELECT") for s in statements)
    finally:
        event.remove(isolated_engine, "before_cursor_execute", capture)


def test_populated_four_gets_preserve_payload_and_writer_default(isolated_engine, monkeypatch):
    # Default writer constructor must still bootstrap the absent table.
    writer = AnalyticalSnapshotStore(isolated_engine)
    assert writer.save_snapshot("test", {"ok": True}, metrics={"score": 2}) is not None
    client = _client(monkeypatch, isolated_engine)
    clear_category_cache()
    statements, capture = _capture_sql(isolated_engine)
    try:
        latest, history, compare, categories = [client.get(path) for path in _paths()]
        assert [r.status_code for r in (latest, history, compare, categories)] == [200] * 4
        assert latest.json()[0]["payload"] == {"ok": True}
        assert latest.json()[0]["metrics"] == {"score": 2}
        assert history.json()[0]["score"] == 2
        assert compare.json()["metrics_a"] == {"score": 2}
        assert categories.json()["entries"][0]["category"] == "test"
        assert categories.json()["entries"][0]["snapshot_count"] == 1
        assert any("GROUP BY category" in s for s in statements)
        assert all(s.upper().startswith("SELECT") for s in statements)
    finally:
        event.remove(isolated_engine, "before_cursor_execute", capture)


def test_existing_empty_table_is_empty_and_compare_is_404(isolated_engine, monkeypatch):
    AnalyticalSnapshotStore(isolated_engine)
    client = _client(monkeypatch, isolated_engine)
    clear_category_cache()
    statements, capture = _capture_sql(isolated_engine)
    try:
        latest, history, compare, categories = [client.get(path) for path in _paths()]
        assert latest.status_code == 200 and latest.json() == []
        assert history.status_code == 200 and history.json() == []
        assert compare.status_code == 404
        assert "Missing snapshots" in compare.json()["detail"]
        assert categories.status_code == 200
        assert categories.json()["entries"] == []
        assert categories.json()["total"] == 0
        assert all(s.upper().startswith("SELECT") for s in statements)
    finally:
        event.remove(isolated_engine, "before_cursor_execute", capture)
