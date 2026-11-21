"""In-process HTTP routing proof; no server, real auth store or provider calls.

AsyncIO needs its local self-pipe wakeup; run outside a sandbox that denies
socket.send even for local IPC. All socket connections remain blocked here.
"""
import asyncio
import socket
from datetime import datetime, timezone

import httpx
import pytest
from fastapi import FastAPI

from api.auth import require_auth
from api.routers import derivatives
from tests.test_wing_iv_semantics import api_engine


@pytest.fixture(autouse=True)
def forbid_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError("network forbidden in HTTP route proof")
    monkeypatch.setattr(socket.socket, "connect", denied)
    monkeypatch.setattr(socket.socket, "connect_ex", denied)


def request(app):
    async def send():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://offline.test",
        ) as client:
            return await client.get("/api/v1/derivatives/term-structure/spy")
    return asyncio.run(send())


def app_with_auth_override():
    app = FastAPI()
    app.include_router(derivatives.router)
    app.dependency_overrides[require_auth] = lambda: "synthetic-auth"
    return app


def test_http_term_structure_routes_dependencies_and_serializes_lineage(monkeypatch):
    batch = ("batch-1", 100, "cboe", datetime(2026, 11, 16, 15, tzinfo=timezone.utc), False)
    db, calls = api_engine(batch)
    monkeypatch.setattr(derivatives, "get_db_engine", lambda: db)
    response = request(app_with_auth_override())
    assert response.status_code == 200
    data = response.json()
    assert data["ticker"] == "SPY" and data["capture_batch_id"] == "batch-1"
    near, far = data["term_structure"]
    assert near["iv_m90_put_v1"] == .4 and far["iv_m90_put_v1"] == .8
    assert near["iv_25d_put"] is None and near["iv_25d_call"] is None
    assert near["wing_iv_metadata"]["source"] == "cboe"
    assert near["wing_iv_metadata"]["dte"] == 30 and far["wing_iv_metadata"]["dte"] == 60
    assert len(calls) == 3 and all(params["t"] == "SPY" for _, params in calls)
    assert all(sql.lstrip().startswith("SELECT") for sql, _ in calls)


def test_http_without_auth_never_reaches_database(monkeypatch):
    def forbidden_db():
        raise AssertionError("unauthenticated HTTP request reached database")
    monkeypatch.setattr(derivatives, "get_db_engine", forbidden_db)
    app = FastAPI()
    app.include_router(derivatives.router)
    assert request(app).status_code == 401


@pytest.mark.parametrize("batch", [None, ("b", 0, "cboe", None, False),
                                   ("b", 100, "cboe", None, True)])
def test_http_missing_observed_capture_stays_explicitly_unavailable(monkeypatch, batch):
    db, calls = api_engine(batch)
    monkeypatch.setattr(derivatives, "get_db_engine", lambda: db)
    response = request(app_with_auth_override())
    assert response.status_code == 200
    assert response.json()["term_structure"] == []
    assert response.json()["error"] == "Observed capture spot unavailable"
    assert len(calls) == 2


def test_http_unknown_provider_withholds_wings_without_inventing_empty_bands(monkeypatch):
    db, _ = api_engine(("batch-1", 100, None, None, False))
    monkeypatch.setattr(derivatives, "get_db_engine", lambda: db)
    response = request(app_with_auth_override())
    assert response.status_code == 200
    row = response.json()["term_structure"][0]
    assert row["iv_m90_put_v1"] is None and row["iv_m110_call_v1"] is None
    assert row["wing_iv_metadata"]["source"] is None
    assert row["wing_iv_metadata"]["put"]["count"] == 1
