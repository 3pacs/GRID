"""/flows/layers, /layers/{id}, /flow-map-v2, /waterfall and /orthogonality must not block the event loop.

Their handlers are ``async def`` but the flow-map build (``_get_flow_map``:
TTLCache + ``build_flow_map``, synchronous DB reads) used to run directly on
the loop. On 2026-10-19 a slow build held the single uvicorn process for
minutes (db.py logged "DB connection held 83.0s ... acquired_by=MainThread")
and Cloudflare cancelled /flows/layers. The build now runs in a worker thread;
the TTL-cache semantics are unchanged.
"""

from __future__ import annotations

import asyncio
import os
import sys
import threading
import types

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("DB_PASSWORD", "testpass")

from api.auth import require_auth  # noqa: E402
from api.routers import flows  # noqa: E402


class _FlowMap:
    layers: list = []
    edges: list = []
    global_liquidity_total = 1.0
    global_liquidity_change_1m = 0.0
    global_policy_score = 0.0

    def to_dict(self):
        return {"layers": [], "edges": []}


def _call(handler, *args):
    """Run an async handler; return (result, loop_thread_id)."""
    async def run():
        loop_thread = threading.get_ident()
        return await handler(*args), loop_thread

    return asyncio.run(run())


@pytest.fixture(autouse=True)
def _engine_and_cache(monkeypatch):
    monkeypatch.setattr(flows, "get_db_engine", lambda: object())
    monkeypatch.setattr(flows, "_flow_map_cache", flows.TTLCache(ttl=60, max_size=4))


@pytest.mark.parametrize("handler,args", [
    (flows.get_flow_layers, ("t",)),
    (flows.get_flow_map_v2, ("t",)),
    (flows.get_flow_layer_detail, ("fed", "t")),
    (flows.get_flow_waterfall, ("fed_balance_sheet", "t")),
    (flows.get_flow_orthogonality, ("t",)),
])
def test_handlers_do_not_build_the_flow_map_on_the_event_loop(handler, args, monkeypatch):
    build_threads: list[int] = []

    def fake_build(_engine, as_of=None):
        build_threads.append(threading.get_ident())
        return _FlowMap()

    monkeypatch.setitem(sys.modules, "analysis.money_flow_engine",
                        types.SimpleNamespace(build_flow_map=fake_build))
    _result, loop_thread = _call(handler, *args)
    assert len(build_threads) == 1
    assert build_threads[0] != loop_thread
    assert build_threads[0] != threading.main_thread().ident


def test_ttl_cache_semantics_unchanged(monkeypatch):
    calls: list[int] = []

    def fake_build(_engine, as_of=None):
        calls.append(1)
        return _FlowMap()

    monkeypatch.setitem(sys.modules, "analysis.money_flow_engine",
                        types.SimpleNamespace(build_flow_map=fake_build))
    first, _ = _call(flows.get_flow_layers, "t")
    second, _ = _call(flows.get_flow_map_v2, "t")
    assert len(calls) == 1                      # second request is a cache hit
    assert first["layers"] == [] and second == {"layers": [], "edges": []}
    assert first["global_liquidity_total"] == 1.0


def test_slow_flow_map_build_leaves_event_loop_responsive(monkeypatch):
    started, release = threading.Event(), threading.Event()
    ping_while_blocked: list[bool] = []

    def slow_build(_engine, as_of=None):
        started.set()
        release.wait(5)
        return _FlowMap()

    monkeypatch.setitem(sys.modules, "analysis.money_flow_engine",
                        types.SimpleNamespace(build_flow_map=slow_build))
    app = FastAPI()
    app.include_router(flows.router)
    app.dependency_overrides[require_auth] = lambda: "fixture"

    @app.get("/ping")
    async def ping():
        ping_while_blocked.append(not release.is_set())
        return {"ok": True}

    from concurrent.futures import ThreadPoolExecutor

    with TestClient(app) as client, ThreadPoolExecutor(max_workers=1) as pool:
        request = pool.submit(client.get, "/api/v1/flows/layers")
        assert started.wait(2)
        timer = threading.Timer(2, release.set)
        timer.start()
        try:
            assert client.get("/ping").json() == {"ok": True}
            assert ping_while_blocked == [True]
        finally:
            release.set()
            timer.cancel()
        assert request.result(timeout=5).status_code == 200
