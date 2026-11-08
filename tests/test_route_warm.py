"""FastAPI 0.139 builds route contexts lazily on the event loop; grid-api's
watchdog traced 35 of 36 stalls (2-6 s, 48 h to 2026-11-07) to that build.
``warm_route_contexts`` pre-builds them so request matching builds none."""

from __future__ import annotations

import fastapi.routing as fastapi_routing
from fastapi import APIRouter, Depends, FastAPI, Query
from fastapi.testclient import TestClient

from api.route_warm import warm_route_contexts


def _dep(q: int = Query(1)) -> int:
    return q


def _app() -> FastAPI:
    inner = APIRouter()

    @inner.get("/leaf/{x}")
    def leaf(x: int, d: int = Depends(_dep)) -> dict:
        return {"x": x, "d": d}

    outer = APIRouter()
    outer.include_router(inner, prefix="/inner")

    @outer.get("/top")
    def top() -> dict:
        return {"ok": True}

    app = FastAPI()
    app.include_router(outer, prefix="/api")
    return app


def _count_builds(monkeypatch) -> list[int]:
    calls = [0]
    orig = fastapi_routing._IncludedRouter._build_effective_context

    def spy(self, route):
        calls[0] += 1
        return orig(self, route)

    monkeypatch.setattr(fastapi_routing._IncludedRouter, "_build_effective_context", spy)
    return calls


def test_requests_build_contexts_lazily_without_warming(monkeypatch) -> None:
    app = _app()
    calls = _count_builds(monkeypatch)
    assert TestClient(app).get("/api/inner/leaf/3?q=2").json() == {"x": 3, "d": 2}
    assert calls[0] > 0  # the behaviour the warmer exists for


def test_warm_then_requests_build_nothing(monkeypatch) -> None:
    app = _app()
    assert warm_route_contexts(app.router) == 2
    calls = _count_builds(monkeypatch)
    client = TestClient(app)
    assert client.get("/api/inner/leaf/3?q=2").json() == {"x": 3, "d": 2}
    assert client.get("/api/top").json() == {"ok": True}
    assert calls[0] == 0


def test_warm_is_a_no_op_without_fastapi_internals(monkeypatch) -> None:
    monkeypatch.delattr(fastapi_routing, "_IncludedRouter")
    assert warm_route_contexts(FastAPI().router) == 0


def test_warm_reports_but_survives_a_failing_router(monkeypatch) -> None:
    app = _app()

    def boom(self):
        raise RuntimeError("internals changed")

    monkeypatch.setattr(fastapi_routing._IncludedRouter, "effective_candidates", boom)
    assert warm_route_contexts(app.router) == 0



def test_prewarm_builds_route_contexts_and_openapi(monkeypatch) -> None:
    """The deploy fetches /openapi.json right after restart; building it there stalled the loop ~5 s."""
    from api.main import _prewarm_request_path

    app = _app()
    assert app.openapi_schema is None
    _prewarm_request_path(app)
    assert "/api/inner/leaf/{x}" in app.openapi_schema["paths"]
    calls = _count_builds(monkeypatch)
    assert TestClient(app).get("/api/top").json() == {"ok": True}
    assert calls[0] == 0
