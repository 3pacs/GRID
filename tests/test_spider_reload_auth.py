"""Spider → API reload ping authenticates (2026-11-08, plan item 0.6).

grid-spider's reload ping got 401 on every run: the daemon only sent a
bearer when GRID_SPIDER_API_TOKEN / GRID_API_TOKEN was set, neither is in
the shared .env, and /spider/graph/reload sits behind require_auth (a JWT
signed with GRID_JWT_SECRET). The daemon now mints a short-lived JWT from
that secret when no explicit token is set, and never logs it.
"""

from __future__ import annotations

import io

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from loguru import logger

from api.dependencies import get_db_engine
from api.routers import intelligence_spider as spider_router
from intelligence.spider import daemon

SECRET = "spider-reload-test-secret"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("GRID_JWT_SECRET", SECRET)
    monkeypatch.delenv("GRID_SPIDER_API_TOKEN", raising=False)
    monkeypatch.delenv("GRID_API_TOKEN", raising=False)


@pytest.fixture
def client(monkeypatch) -> TestClient:
    """The real reload route + require_auth, with the graph load stubbed."""
    monkeypatch.setattr(spider_router, "refresh_graph", lambda engine: {"status": "reloaded"})
    app = FastAPI()
    app.include_router(spider_router.router)
    app.dependency_overrides[get_db_engine] = lambda: None
    return TestClient(app)


@pytest.fixture
def sent(monkeypatch, client) -> list[dict]:
    """Route the daemon's urlopen through the TestClient; record each request."""
    import urllib.request

    calls: list[dict] = []

    class _Resp:
        def __init__(self, r):
            self.status = r.status_code
            self._body = r.content

        def read(self, n=-1):
            return self._body[:n] if n >= 0 else self._body

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def fake_urlopen(req, timeout=None):
        headers = dict(req.header_items())
        path = req.full_url.split("127.0.0.1:8000", 1)[1]
        r = client.post(path, headers=headers)
        calls.append({"url": req.full_url, "headers": headers, "status": r.status_code})
        if r.status_code >= 400:
            raise urllib.error.HTTPError(req.full_url, r.status_code, r.reason_phrase, {}, None)
        return _Resp(r)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    return calls


@pytest.fixture
def logs() -> io.StringIO:
    buf = io.StringIO()
    sink = logger.add(buf, level="DEBUG", format="{message}")
    yield buf
    logger.remove(sink)


def test_route_rejects_an_unauthenticated_ping(client) -> None:
    assert client.post("/api/v1/intelligence/spider/graph/reload").status_code == 401


def test_ping_without_explicit_token_mints_one_the_api_accepts(sent, logs) -> None:
    daemon._ping_api_reload()

    assert len(sent) == 1
    assert sent[0]["url"] == "http://127.0.0.1:8000/api/v1/intelligence/spider/graph/reload"
    assert sent[0]["headers"]["Authorization"].startswith("Bearer ")
    assert sent[0]["status"] == 200
    assert "API reload ping → 200" in logs.getvalue()


def test_minted_token_is_short_lived_least_privilege(sent) -> None:
    from api.auth import decode_token

    daemon._ping_api_reload()
    payload = decode_token(sent[0]["headers"]["Authorization"].removeprefix("Bearer "))

    assert payload["role"] == "contributor"
    assert payload["sub"] == "grid-spider"
    assert payload["exp"] - payload["iat"] <= 15 * 60


def test_token_never_reaches_the_log(sent, logs) -> None:
    daemon._ping_api_reload()
    token = sent[0]["headers"]["Authorization"].removeprefix("Bearer ")

    assert token not in logs.getvalue()
    assert SECRET not in logs.getvalue()


def test_explicit_env_token_takes_precedence(monkeypatch, sent) -> None:
    from api.auth import create_token

    explicit = create_token(role="admin", username="ops", expires_hours=1)
    monkeypatch.setenv("GRID_SPIDER_API_TOKEN", explicit)

    daemon._ping_api_reload()

    assert sent[0]["headers"]["Authorization"] == f"Bearer {explicit}"
    assert sent[0]["status"] == 200


def test_missing_secret_degrades_to_a_logged_warning(monkeypatch, sent, logs) -> None:
    monkeypatch.delenv("GRID_JWT_SECRET")

    daemon._ping_api_reload()  # must not raise: the ping is advisory

    assert "Authorization" not in sent[0]["headers"]
    out = logs.getvalue()
    assert "could not sign the reload JWT (RuntimeError)" in out
    assert "API reload ping failed" in out
