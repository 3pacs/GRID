"""Route-level tests for api/routers/godview.py (G8) without a database.

The PostgreSQL contract (real reads, PIT, SELECT-only) is
``tests/godview/test_godview_api_readonly_pg.py``.
"""

from __future__ import annotations

import os
from unittest.mock import patch

os.environ.setdefault("DB_PASSWORD", "testpass")

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

from api.auth import require_auth
from api.routers import godview as godview_router


def _client(*, authed: bool = True) -> TestClient:
    app = FastAPI()
    app.include_router(godview_router.router)
    if authed:
        app.dependency_overrides[require_auth] = lambda: "test-token"
    return TestClient(app)


class _BrokenEngine:
    def connect(self):
        raise OperationalError("SELECT 1", {}, Exception("connection refused"))


def test_routes_require_auth():
    client = _client(authed=False)
    assert client.get("/api/v1/godview/latest").status_code in (401, 403)
    assert client.get("/api/v1/godview/history?pillar=cftc").status_code in (401, 403)


def test_router_is_mounted_in_the_app_registry():
    with open(os.path.join(os.path.dirname(__file__), "..", "api", "main.py"), encoding="utf-8") as fh:
        source = fh.read()
    assert '("godview", "api.routers.godview", False)' in source
    assert "api.routers.god_view" not in source


@pytest.mark.parametrize(
    "query",
    [
        "pillar=nope",
        "",  # pillar is required
        "pillar=fed_liquidity&market=ES",
        "pillar=cftc&market=XX",
        "pillar=cftc&market=es",
        "pillar=cftc&from=2026-09-10&to=2026-09-01",
        "pillar=cftc&limit=0",
        "pillar=cftc&limit=5000",
        "pillar=cftc&offset=-1",
        "pillar=cftc&as_of=2999-01-01",
        "pillar=cftc&as_of=2026-09-25T20:00:00",
    ],
)
def test_history_rejects_bad_parameters(query):
    with patch.object(godview_router, "get_db_engine", side_effect=AssertionError("no DB for a 422")):
        resp = _client().get(f"/api/v1/godview/history?{query}")
    assert resp.status_code == 422, resp.text


@pytest.mark.parametrize("as_of", ["2999-01-01", "not-a-date", "2026-09-25T20:00:00"])
def test_latest_rejects_bad_as_of(as_of):
    with patch.object(godview_router, "get_db_engine", side_effect=AssertionError("no DB for a 422")):
        resp = _client().get(f"/api/v1/godview/latest?as_of={as_of}")
    assert resp.status_code == 422


def test_database_failure_is_503_not_a_fabricated_payload():
    with patch.object(godview_router, "get_db_engine", return_value=_BrokenEngine()):
        latest = _client().get("/api/v1/godview/latest")
        history = _client().get("/api/v1/godview/history?pillar=fed_liquidity")
    assert latest.status_code == history.status_code == 503
    assert latest.json()["detail"] == "godview_store_unavailable"
    assert "connection refused" not in latest.text


def test_history_defaults_to_the_trailing_year_at_as_of():
    captured = {}

    def fake_history(conn, **kwargs):
        captured.update(kwargs)
        return {"entries": []}

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class _Engine:
        def connect(self):
            return _Conn()

    with patch.object(godview_router, "get_db_engine", return_value=_Engine()), patch.object(
        godview_router.read_model, "read_history", side_effect=fake_history
    ):
        resp = _client().get("/api/v1/godview/history?pillar=cftc&market=ES&as_of=2026-09-25")
    assert resp.status_code == 200
    assert str(captured["date_to"]) == "2026-09-25"
    assert str(captured["date_from"]) == "2025-09-25"
    assert captured["market"] == "ES" and captured["limit"] == 500 and captured["offset"] == 0
