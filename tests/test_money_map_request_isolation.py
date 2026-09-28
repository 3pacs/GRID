"""A blocked money-map database call must not stop unrelated HTTP requests."""
import sys
import threading
import types
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.auth import require_auth
from api.routers import flows


def test_slow_money_map_leaves_event_loop_responsive(monkeypatch):
    started = threading.Event()
    release = threading.Event()
    ping_before_release = []
    def build(_engine):
        started.set()
        release.wait(5)
        return {"layers": [], "source": "fixture"}
    monkeypatch.setitem(sys.modules, "analysis.money_flow", types.SimpleNamespace(build_flow_map=build))
    monkeypatch.setattr(flows, "get_db_engine", lambda: object())
    monkeypatch.setattr(flows, "_money_map_cache", flows.TTLCache(ttl=900, max_size=1))
    app = FastAPI()
    app.include_router(flows.router)
    app.dependency_overrides[require_auth] = lambda: "fixture"
    @app.get("/ping")
    async def ping():
        ping_before_release.append(not release.is_set())
        return {"ok": True}
    with TestClient(app) as client, ThreadPoolExecutor(max_workers=1) as pool:
        request = pool.submit(client.get, "/api/v1/flows/money-map")
        assert started.wait(2)
        fallback = threading.Timer(2, release.set)
        fallback.start()
        try:
            assert client.get("/ping").json() == {"ok": True}
            assert ping_before_release == [True]
        finally:
            release.set()
            fallback.cancel()
        assert request.result(timeout=3).json() == {"layers": [], "source": "fixture"}
        assert client.get("/api/v1/flows/money-map").json() == {"layers": [], "source": "fixture"}
