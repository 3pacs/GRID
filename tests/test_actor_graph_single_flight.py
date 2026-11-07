"""The actor-network graph must never be built on the event loop, and only once at a time.

2026-11-06: ``get_actor_network`` was an ``async def`` that rebuilt the full
actor graph inline on every TTL miss. The build ran on grid-api's event loop
(live stack: intelligence_actors.get_actor_network -> _build_full_actor_cache
-> build_actor_graph -> _infer_sector), every request that arrived meanwhile
queued another build, and the site returned Cloudflare 524s with /health
blocked for 10+ minutes. These tests pin the fix: single-flight builds,
stale-while-revalidate, a fast "building" answer on a cold cache, sync
(threadpool) handlers, and a graph sized to the curated actors.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import os
import threading
import time
from pathlib import Path

import pytest

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("DB_PASSWORD", "testpass")

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import intelligence.actor_network as actor_network  # noqa: E402
from api.auth import require_auth  # noqa: E402
from api.routers import intelligence_actors as ia  # noqa: E402
from intelligence.actors import graph as graph_mod  # noqa: E402
from intelligence.actors.models import Actor  # noqa: E402

REPO = Path(__file__).resolve().parents[1]


class _BlockingBuilder:
    """Stands in for build_actor_graph: counts calls, blocks until released."""

    def __init__(self) -> None:
        self.calls = 0
        self.release = threading.Event()
        self.started = threading.Event()
        self._lock = threading.Lock()

    def __call__(self, engine, **kwargs):  # noqa: ARG002
        with self._lock:
            self.calls += 1
            n = self.calls
        self.started.set()
        assert self.release.wait(10), "test never released the builder"
        node = {"id": f"a{n}", "label": f"Actor {n}", "sector": "AI", "influence": 0.9}
        return {"nodes": [node], "links": [], "metadata": {"build": n}}


@pytest.fixture
def builder(monkeypatch):
    b = _BlockingBuilder()
    monkeypatch.setattr(actor_network, "build_actor_graph", b)
    monkeypatch.setattr(ia, "get_db_engine", lambda: object())
    ia.clear_actor_graph()
    yield b
    b.release.set()
    _wait_for_build_idle()
    ia.clear_actor_graph()


def _wait_for_build_idle(timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if ia._actor_graph_build_lock.acquire(blocking=False):
            ia._actor_graph_build_lock.release()
            return
        time.sleep(0.01)
    raise AssertionError("actor graph build never finished")


def _call() -> dict:
    return ia.get_actor_network(limit=500, sector=None, _token="t")


def test_handlers_run_in_threadpool_not_on_the_event_loop():
    for fn in (ia.get_actor_network, ia.get_actor_detail):
        assert not inspect.iscoroutinefunction(fn), f"{fn.__name__} must be a plain def"


def test_cold_cache_returns_building_immediately_and_builds_once(builder):
    results: list[dict] = []
    durations: list[float] = []

    def hit() -> None:
        t0 = time.monotonic()
        results.append(_call())
        durations.append(time.monotonic() - t0)

    threads = [threading.Thread(target=hit) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(5)

    assert len(results) == 8
    assert all(r["status"] == "building" and r["nodes"] == [] for r in results)
    assert max(durations) < 1.0, "a cold request must not wait for the build"
    assert builder.started.wait(5)
    assert builder.calls == 1, "concurrent cold requests must share one build"

    builder.release.set()
    _wait_for_build_idle()
    served = _call()
    assert "status" not in served
    assert [n["id"] for n in served["nodes"]] == ["a1"]
    assert builder.calls == 1


def test_expired_graph_is_served_stale_while_one_rebuild_runs(builder):
    builder.release.set()
    assert ia.get_full_actor_graph(wait=True)["metadata"]["build"] == 1

    builder.release.clear()
    ia._actor_graph_cache.clear()  # simulate the TTL expiring

    t0 = time.monotonic()
    stale = [_call() for _ in range(5)]
    assert time.monotonic() - t0 < 1.0, "an expired graph must be served without waiting"
    assert all([n["id"] for n in r["nodes"]] == ["a1"] for r in stale)
    assert builder.started.wait(5)
    assert builder.calls == 2, "exactly one background rebuild"

    builder.release.set()
    _wait_for_build_idle()
    assert [n["id"] for n in _call()["nodes"]] == ["a2"]


def test_wait_true_joins_the_in_flight_build_instead_of_starting_another(builder):
    assert ia.start_actor_graph_rebuild() is True
    assert builder.started.wait(5)
    got: list[dict | None] = []
    waiter = threading.Thread(target=lambda: got.append(ia.get_full_actor_graph(wait=True)))
    waiter.start()
    time.sleep(0.1)
    assert waiter.is_alive(), "waiter should block on the in-flight build"
    builder.release.set()
    waiter.join(5)
    assert got and got[0]["metadata"]["build"] == 1
    assert builder.calls == 1


def test_endpoint_through_fastapi_is_fast_while_build_blocks(builder):
    app = FastAPI()
    app.include_router(ia.router)
    app.dependency_overrides[require_auth] = lambda: "t"
    with TestClient(app) as client:
        t0 = time.monotonic()
        resp = client.get("/actor-network")
        assert resp.status_code == 200
        assert resp.json()["status"] == "building"
        assert time.monotonic() - t0 < 2.0
    assert builder.calls == 1


def test_failed_build_releases_the_lock(monkeypatch):
    def boom(engine, **kwargs):  # noqa: ARG001
        raise RuntimeError("db down")

    monkeypatch.setattr(actor_network, "build_actor_graph", boom)
    monkeypatch.setattr(ia, "get_db_engine", lambda: object())
    ia.clear_actor_graph()
    assert ia.get_full_actor_graph(wait=True) is None
    assert ia._actor_graph_build_lock.acquire(blocking=False)
    ia._actor_graph_build_lock.release()
    ia.clear_actor_graph()


# ── No blocking async route handlers in the hotfixed routers ────────────────

_ROUTE_METHODS = {"get", "post", "put", "delete", "patch"}


@pytest.mark.parametrize("relpath", [
    "api/routers/intelligence_actors.py",
    "api/routers/intel_cross_reference.py",
    "api/routers/intelligence_deepdive.py",
])
def test_no_async_route_without_await(relpath):
    tree = ast.parse((REPO / relpath).read_text())
    offenders = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncFunctionDef):
            continue
        is_route = any(
            isinstance(d, ast.Call) and isinstance(d.func, ast.Attribute)
            and d.func.attr in _ROUTE_METHODS
            for d in node.decorator_list
        )
        awaits = any(isinstance(n, (ast.Await, ast.AsyncFor, ast.AsyncWith)) for n in ast.walk(node))
        if is_route and not awaits:
            offenders.append(f"{relpath}:{node.lineno} {node.name}")
    assert not offenders, (
        "async route handlers with no await run all their (sync) work on the "
        f"event loop and freeze grid-api; make them plain def: {offenders}"
    )


# ── /levers single-flight ───────────────────────────────────────────────────

def test_levers_cold_requests_share_one_build(monkeypatch):
    from api.routers import intelligence_deepdive as dd
    import intelligence.global_levers as gl

    calls = {"n": 0}
    release = threading.Event()

    def hierarchy(engine):  # noqa: ARG001
        calls["n"] += 1
        release.wait(5)
        return {"hierarchy": {"monetary": {}}}

    monkeypatch.setattr(gl, "get_lever_hierarchy", hierarchy)
    monkeypatch.setattr(gl, "find_cross_domain_actors", lambda engine: [])
    monkeypatch.setattr(dd, "get_db_engine", lambda: object())
    dd._lever_cache.clear()

    out: list[dict] = []
    threads = [threading.Thread(target=lambda: out.append(asyncio.run(dd.get_levers(_token="t"))))
               for _ in range(4)]
    for t in threads:
        t.start()
    time.sleep(0.2)
    release.set()
    for t in threads:
        t.join(5)
    dd._lever_cache.clear()
    assert len(out) == 4 and all(o["hierarchy"] == {"monetary": {}} for o in out)
    assert calls["n"] == 1


# ── Graph size: curated actors only, capped ─────────────────────────────────

def test_build_actor_graph_excludes_bulk_lists_and_caps(monkeypatch):
    seen: dict = {}

    def fake_load(engine, **kwargs):  # noqa: ARG001
        seen.update(kwargs)
        return {"x": Actor(id="x", name="X Corp", tier="institution", category="fund", title="")}

    class _NoConnEngine:
        def connect(self):
            raise RuntimeError("no db in unit test")

    monkeypatch.setattr(graph_mod, "_ensure_tables", lambda engine: None)
    monkeypatch.setattr(graph_mod, "_load_actors_from_db", fake_load)
    monkeypatch.setattr(graph_mod, "_resolve_dynamic_insiders", lambda engine: [])
    monkeypatch.setenv("GRID_ACTOR_GRAPH_MAX_ACTORS", "1234")

    out = graph_mod.build_actor_graph(_NoConnEngine())

    assert [n["id"] for n in out["nodes"]] == ["x"]
    excluded = seen["exclude_categories"]
    for bulk in ("icij_entity", "icij_officer", "pep", "person_of_interest",
                 "entity_of_interest", "donor", "sanctioned", "ofac_sanctioned"):
        assert bulk in excluded
    assert seen["exclude_sources"] == frozenset({"icij"})
    assert seen["limit"] == 1234


def test_infer_sector_uses_word_boundary_tagger():
    """Same labels as before for real matches; no more substring false hits.

    The old rule scanned title + name + positions for substrings, so
    "Craig" (contains "ai") was tagged AI. The tagger now lives in
    intelligence/actors/sectors.py (tests/test_actor_sectors.py).
    """
    samples = [
        (Actor(id="1", name="Jensen Huang", tier="individual", category="insider", title="CEO, NVIDIA"), "Semiconductors"),
        (Actor(id="2", name="Fed", tier="sovereign", category="central_bank", title=""), "Macro"),
        (Actor(id="3", name="Barrick", tier="institution", category="corporation", title="Gold mining"), "Metals"),
        (Actor(id="4", name="Nobody", tier="individual", category="individual", title=""), "Other"),
        (Actor(id="5", name="X", tier="individual", category="fund", title="", board_seats=["Coinbase"]), "Crypto"),
        (Actor(id="6", name="Y", tier="individual", category="other", title="", known_positions=["Uranium Royalty"]), "Nuclear"),
        (Actor(id="7", name="Craig Wright", tier="individual", category="individual", title=""), "Other"),
        (Actor(id="8", name="Megan Smith", tier="individual", category="insider", title="Director"), "Other"),
    ]
    for actor, expected in samples:
        assert graph_mod._infer_sector(actor) == expected, actor.name
