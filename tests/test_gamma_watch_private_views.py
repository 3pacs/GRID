"""Private projection parity and access controls; synthetic inputs, no live I/O."""
import copy
import io
import json
import socket
import urllib.error
from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.auth import create_token
from api.routers import gamma_watch as gw


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setenv("GRID_JWT_SECRET", "gex-private-bridge-test-secret")
    def deny(*args, **kwargs):
        raise AssertionError("No outbound connections in bridge tests")
    monkeypatch.setattr(socket.socket, "connect", deny)
    monkeypatch.setattr(socket.socket, "connect_ex", deny)


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(gw.router)
    return TestClient(app)


def bearer(role="admin"):
    return {"Authorization": "Bearer " + create_token(role=role, expires_hours=1)}


def payload(view, status="degraded"):
    data = {"status": status, "as_of": "2000-01-01T00:00:00Z",
            "volume_quality": {"status": "unavailable", "current_interpretation": False},
            "quote": {"source": "schwab", "clock": "api-receipt"},
            "current": {"call": "WAIT", "stop": None}, "signed_flow": None,
            "private_source": True, "assumptions": "OI is not observed dealer inventory"}
    if gw.PRIVATE_VIEWS[view] is not None:
        data["schema"] = gw.PRIVATE_VIEWS[view]
    return data


@pytest.mark.parametrize("view", gw.PRIVATE_VIEWS)
def test_admin_gets_exact_guarded_projection(client, monkeypatch, view):
    data = payload(view, "observed" if view == "confirmations" else "degraded")
    before = copy.deepcopy(data)
    calls = []
    monkeypatch.setattr(gw, "_private_views_enabled", lambda: True)
    monkeypatch.setattr(gw, "_read_upstream", lambda path: calls.append(path) or data)
    response = client.get(f"/api/v1/gamma-watch/private/{view}?path=/api/private/state", headers=bearer())
    assert response.status_code == 200
    assert calls == ["/api/" + view]
    body = response.json()
    assert body["data"] == data == before
    assert body["status"] == data["status"]
    assert body["transport_status"] == "available"
    assert body["data"]["as_of"] == "2000-01-01T00:00:00Z"
    assert body["audience"] == "private" and body["redistribution"] == "prohibited"
    assert response.headers["cache-control"] == "no-store, private"
    assert response.headers["x-grid-audience"] == "private"
    assert datetime.fromisoformat(body["bridge_received_at"]).tzinfo == timezone.utc


@pytest.mark.parametrize("view", gw.PRIVATE_VIEWS)
@pytest.mark.parametrize("role", [None, "contributor"])
def test_unauthorized_never_reads(client, monkeypatch, view, role):
    monkeypatch.setattr(gw, "_private_views_enabled", lambda: True)
    monkeypatch.setattr(gw, "_read_upstream", lambda path: pytest.fail("Must not read upstream"))
    r = client.get(f"/api/v1/gamma-watch/private/{view}", headers={} if role is None else bearer(role))
    assert r.status_code == (401 if role is None else 403)


def test_query_token_does_not_authorize(client, monkeypatch):
    monkeypatch.setattr(gw, "_read_upstream", lambda path: pytest.fail("Must not read upstream"))
    r = client.get("/api/v1/gamma-watch/private/levels?token=" + create_token())
    assert r.status_code == 401


def test_disabled_without_read(client, monkeypatch):
    monkeypatch.setattr(gw, "_private_views_enabled", lambda: False)
    monkeypatch.setattr(gw, "_read_upstream", lambda path: pytest.fail("Must not read upstream"))
    r = client.get("/api/v1/gamma-watch/private/levels", headers=bearer())
    assert r.status_code == 503 and r.json()["data"] is None
    assert "disabled" in r.json()["reason"]
    assert r.json()["bridge_received_at"] is None
    assert r.headers["cache-control"] == "no-store, private"


def test_flag_default_and_blank_handling():
    from config import Settings
    assert Settings.model_fields["GAMMA_WATCH_PRIVATE_VIEWS_ENABLED"].default is False
    assert Settings._coerce_blank_bool_flag("") is False


@pytest.mark.parametrize("view", ["state", "contracts", "scenario", "feed", "private", "not-a-view"])
def test_private_state_and_arbitrary_views_not_exposed(client, monkeypatch, view):
    monkeypatch.setattr(gw, "_private_views_enabled", lambda: True)
    monkeypatch.setattr(gw, "_read_upstream", lambda path: pytest.fail("Must not read upstream"))
    assert client.get(f"/api/v1/gamma-watch/private/{view}", headers=bearer()).status_code == 422


@pytest.mark.parametrize("status", ["ok", "degraded", "unavailable"])
def test_status_not_promoted(client, monkeypatch, status):
    data = payload("panel", status)
    monkeypatch.setattr(gw, "_private_views_enabled", lambda: True)
    monkeypatch.setattr(gw, "_read_upstream", lambda path: data)
    r = client.get("/api/v1/gamma-watch/private/panel", headers=bearer())
    assert r.status_code == 200
    assert r.json()["status"] == status
    assert r.json()["data"] == data


@pytest.mark.parametrize("data", [{}, {"schema": "wrong", "status": "ok"}, {"schema": "gex-panel/1", "status": "live"}, {"schema": "gex-panel/1"}])
def test_unknown_contract_fails_closed(client, monkeypatch, data):
    monkeypatch.setattr(gw, "_private_views_enabled", lambda: True)
    monkeypatch.setattr(gw, "_read_upstream", lambda path: data)
    r = client.get("/api/v1/gamma-watch/private/panel", headers=bearer())
    assert r.status_code == 503 and r.json()["data"] is None
    assert r.headers["cache-control"] == "no-store, private"


@pytest.mark.parametrize("code", [302, 401, 404, 500, 503])
def test_http_failure_has_no_fallback_or_body_leak(client, monkeypatch, code):
    monkeypatch.setattr(gw, "_private_views_enabled", lambda: True)
    calls = []
    def fail(path):
        calls.append(path)
        raise urllib.error.HTTPError(gw.UPSTREAM + path, code, "/private/file", {}, io.BytesIO(b'{"secret":"private"}'))
    monkeypatch.setattr(gw, "_read_upstream", fail)
    r = client.get("/api/v1/gamma-watch/private/levels", headers=bearer())
    assert r.status_code == 503 and r.json()["data"] is None
    assert calls == ["/api/levels"]
    assert r.json()["bridge_received_at"] is None
    assert "/private/file" not in r.text and '"secret"' not in r.text
    assert r.json()["reason"] == (gw.ROUTE_NOT_SERVED_REASON if code == 404 else gw.UNAVAILABLE_REASON)
    assert r.headers["cache-control"] == "no-store, private"


@pytest.mark.parametrize("body", [b'{"x":NaN}', b'[]', b'not json', b'{"x":Infinity}', b'{"x":1e400}', b'{"x":-1e400}'])
def test_reader_rejects_invalid_json(monkeypatch, body):
    class Opener:
        def open(self, req, timeout):
            assert req.full_url == "http://127.0.0.1:8769/api/panel"
            assert timeout == 3
            return io.BytesIO(body)
    monkeypatch.setattr(gw.urllib.request, "build_opener", lambda *handlers: Opener())
    with pytest.raises(ValueError):
        gw._read_upstream("/api/panel")


def test_reader_caps_response(monkeypatch):
    class Opener:
        def open(self, req, timeout):
            return io.BytesIO(json.dumps({"padding": "x" * 100}).encode())
    monkeypatch.setattr(gw, "MAX_BYTES", 64)
    monkeypatch.setattr(gw.urllib.request, "build_opener", lambda *handlers: Opener())
    with pytest.raises(ValueError, match="Snapshot too large"):
        gw._read_upstream("/api/panel")


@pytest.mark.parametrize("value", [False, None, "false", "true", "0", "1", 0, 1, [], {}])
def test_raw_settings_assignment_does_not_enable_private_transport(monkeypatch, value):
    from config import settings
    monkeypatch.setattr(settings, "GAMMA_WATCH_PRIVATE_VIEWS_ENABLED", value)
    assert gw._private_views_enabled() is False


def test_only_boolean_true_enables_transport(monkeypatch):
    from config import settings
    monkeypatch.setattr(settings, "GAMMA_WATCH_PRIVATE_VIEWS_ENABLED", True)
    assert gw._private_views_enabled() is True


def test_env_example_keeps_bridge_disabled():
    from pathlib import Path
    assert "GAMMA_WATCH_PRIVATE_VIEWS_ENABLED=false" in Path(".env.example").read_text()
