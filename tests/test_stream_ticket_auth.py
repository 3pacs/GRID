"""SSE auth: single-use stream tickets replace the session JWT in the URL (D7).

EventSource cannot send an Authorization header, so the PWA used to put the
long-lived session JWT in ``?token=``, where it lands in tunnel/proxy/access
logs. These tests pin the replacement contract:

* ``?token=`` is no longer accepted by ``require_auth``/``require_role``;
* ``POST /api/v1/auth/stream-ticket`` mints a 60 s, path-bound, single-use
  ticket that is useless as a session token;
* both SSE routes authenticate with ``require_stream_auth``.
"""

from __future__ import annotations

import os
import time

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from jose import jwt

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("DB_PASSWORD", "testpass")

import api.auth as auth  # noqa: E402
from api.auth import (  # noqa: E402
    create_token,
    require_auth,
    require_role,
    require_stream_auth,
)

GOLD_PATH = "/api/v1/dad/ticker/AAPL/gold/stream"
EVENTS_PATH = "/api/v1/events/stream"


@pytest.fixture(autouse=True)
def _jwt_secret(monkeypatch):
    monkeypatch.setenv("GRID_JWT_SECRET", "stream-ticket-test-secret")
    monkeypatch.setenv("GRID_JWT_EXPIRE_HOURS", "1")
    auth._redeemed_stream_tickets.clear()
    yield
    auth._redeemed_stream_tickets.clear()


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(auth.router)

    @app.get("/probe/auth")
    async def probe_auth(_t: str = Depends(require_auth)) -> dict:
        return {"ok": True}

    @app.get("/probe/admin")
    async def probe_admin(_t: str = Depends(require_role("admin"))) -> dict:
        return {"ok": True}

    @app.get("/api/v1/dad/ticker/{ticker}/gold/stream")
    async def gold_stream(ticker: str, _t: str = Depends(require_stream_auth)) -> dict:
        return {"ticker": ticker}

    @app.get("/api/v1/events/stream")
    async def events_stream(_t: str = Depends(require_stream_auth)) -> dict:
        return {"ok": True}

    return TestClient(app)


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _ticket(client: TestClient, path: str, token: str | None = None) -> str:
    token = token or create_token(role="admin", username="op", expires_hours=1)
    response = client.post(
        "/api/v1/auth/stream-ticket", json={"path": path}, headers=_bearer(token),
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["path"] == path
    assert 0 < body["expires_in"] <= auth.STREAM_TICKET_TTL_SECONDS
    return body["ticket"]


def test_query_param_session_token_is_refused_everywhere(client):
    token = create_token(role="admin", expires_hours=1)
    assert client.get("/probe/auth", headers=_bearer(token)).status_code == 200
    assert client.get(f"/probe/auth?token={token}").status_code == 401
    assert client.get(f"/probe/admin?token={token}").status_code == 401
    # The SSE routes do not read ?token= either — only ?ticket=.
    assert client.get(f"{GOLD_PATH}?token={token}").status_code == 401
    assert client.get(f"{EVENTS_PATH}?token={token}").status_code == 401


def test_ticket_requires_bearer_session_and_a_stream_path(client):
    assert client.post("/api/v1/auth/stream-ticket", json={"path": GOLD_PATH}).status_code == 401
    token = create_token(expires_hours=1)
    for bad in ("/api/v1/auth/verify", "/api/v1/dad/ticker/AAPL/gold", "https://evil.test" + GOLD_PATH):
        response = client.post(
            "/api/v1/auth/stream-ticket", json={"path": bad}, headers=_bearer(token),
        )
        assert response.status_code == 400, bad


def test_ticket_is_single_use_and_path_bound(client):
    ticket = _ticket(client, GOLD_PATH)
    assert client.get(f"{GOLD_PATH}?ticket={ticket}").json() == {"ticker": "AAPL"}
    # Replay of the same ticket is refused.
    assert client.get(f"{GOLD_PATH}?ticket={ticket}").status_code == 401

    other = _ticket(client, GOLD_PATH)
    assert client.get(f"/api/v1/dad/ticker/MSFT/gold/stream?ticket={other}").status_code == 401
    assert client.get(f"{EVENTS_PATH}?ticket={other}").status_code == 401

    events = _ticket(client, EVENTS_PATH)
    assert client.get(f"{EVENTS_PATH}?ticket={events}").status_code == 200


def test_ticket_is_not_a_session_token(client):
    ticket = _ticket(client, EVENTS_PATH)
    assert client.get("/probe/auth", headers=_bearer(ticket)).status_code == 401
    assert client.get("/probe/admin", headers=_bearer(ticket)).status_code == 401
    assert client.get(EVENTS_PATH, headers=_bearer(ticket)).status_code == 401
    assert auth.verify_token(ticket) is False
    # And a session JWT cannot be smuggled in as a ticket.
    session = create_token(expires_hours=1)
    assert client.get(f"{EVENTS_PATH}?ticket={session}").status_code == 401


def test_expired_ticket_is_refused(client):
    now = int(time.time())
    expired = jwt.encode(
        {"typ": "stream_ticket", "sub": "op", "role": "admin", "path": EVENTS_PATH,
         "jti": "expired-jti", "iat": now - 120, "exp": now - 60},
        auth._stream_ticket_key(), algorithm="HS256",
    )
    assert client.get(f"{EVENTS_PATH}?ticket={expired}").status_code == 401


def test_ticket_never_outlives_parent_session(client):
    parent = jwt.encode(
        {"sub": "op", "role": "admin", "exp": int(time.time()) + 5, "iat": int(time.time())},
        "stream-ticket-test-secret", algorithm="HS256",
    )
    minted = auth.issue_stream_ticket(parent, EVENTS_PATH)
    assert minted["expires_in"] <= 5


def test_bearer_header_still_works_on_stream_routes(client):
    token = create_token(expires_hours=1)
    assert client.get(GOLD_PATH, headers=_bearer(token)).status_code == 200
    assert client.get(EVENTS_PATH, headers=_bearer(token)).status_code == 200
    assert client.get(GOLD_PATH).status_code == 401


def test_replay_cache_fails_closed_when_full(monkeypatch):
    monkeypatch.setattr(auth, "_STREAM_TICKET_MAX_OUTSTANDING", 2)
    future = time.time() + 60
    assert auth._burn_stream_ticket("a", future)
    assert auth._burn_stream_ticket("b", future)
    assert not auth._burn_stream_ticket("c", future)
    # Expired entries are pruned, freeing room.
    auth._redeemed_stream_tickets.clear()
    assert auth._burn_stream_ticket("old", time.time() - 1)
    assert auth._burn_stream_ticket("d", future)
    assert auth._burn_stream_ticket("e", future)


def test_real_sse_routes_use_stream_auth():
    from api.routers import dad, sse

    def deps(router, path):
        for route in router.routes:
            if getattr(route, "path", None) == path:
                return {d.call for d in route.dependant.dependencies}
        raise AssertionError(f"route {path} not found")

    assert require_stream_auth in deps(dad.router, "/api/v1/dad/ticker/{ticker}/gold/stream")
    assert require_auth not in deps(dad.router, "/api/v1/dad/ticker/{ticker}/gold/stream")
    assert require_stream_auth in deps(sse.router, "/api/v1/events/stream")
    assert require_auth not in deps(sse.router, "/api/v1/events/stream")
