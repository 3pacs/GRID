"""Offline ASGI regression: association reads must not stall God View's loop.

Database/PIT boundaries and auth are synthetic; no app lifespan, providers,
database, or email dispatch is started. The blocking event has a two-second
failsafe so the pre-fix async handlers fail without hanging the test runner.
"""

from __future__ import annotations

import asyncio
import threading
from unittest.mock import MagicMock

import httpx
import numpy as np
import pandas as pd
import pytest
from fastapi import FastAPI


@pytest.fixture
def isolated_app(monkeypatch):
    # Synthetic import-time settings only; never load a runtime environment.
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.setenv("DB_PASSWORD", "offline-test-only")
    monkeypatch.setenv("GRID_GIT_SINK_DISABLED", "1")
    from api.auth import require_auth
    from api.routers import associations, godview

    app = FastAPI()
    app.include_router(associations.router)
    app.include_router(godview.router)

    async def synthetic_auth() -> str:
        return "offline-fixture"

    app.dependency_overrides[require_auth] = synthetic_auth
    monkeypatch.setattr(associations, "get_db_engine", lambda: object())
    monkeypatch.setattr(associations, "get_pit_store", lambda: object())
    monkeypatch.setattr(godview, "get_db_engine", lambda: MagicMock())
    monkeypatch.setattr(
        godview.read_model, "read_latest",
        lambda *_args, **_kwargs: {"source": "offline-fixture", "pillars": {}},
    )
    return app, associations, require_auth


@pytest.mark.unit
@pytest.mark.parametrize("path", ["correlation-matrix", "anomalies"])
@pytest.mark.parametrize("blocked_boundary", ["registry", "matrix"])
def test_slow_association_read_allows_concurrent_godview(
    isolated_app, monkeypatch, path, blocked_boundary,
):
    app, associations, _auth = isolated_app
    started = threading.Event()
    release = threading.Event()
    expired = threading.Event()
    owner_threads: list[int] = []
    registry = pd.DataFrame({
        "id": [10, 20], "name": ["alpha", "beta"],
        "family": ["rates_fixture", "credit_fixture"],
    })
    values = np.arange(90, dtype=float)
    matrix = pd.DataFrame(
        {10: values, 20: values * 2 + 1},
        index=pd.date_range("2026-01-01", periods=90),
    )

    def block() -> None:
        owner_threads.append(threading.get_ident())
        started.set()
        if not release.wait(timeout=2):
            expired.set()

    def registry_read(_engine):
        if blocked_boundary == "registry":
            block()
        return registry.copy()

    def matrix_read(_pit, feature_ids, days):
        assert feature_ids == [10, 20]
        assert days == 252
        if blocked_boundary == "matrix":
            block()
        return matrix.copy()

    monkeypatch.setattr(associations, "_get_feature_registry", registry_read)
    monkeypatch.setattr(associations, "_build_feature_matrix", matrix_read)

    async def exercise() -> None:
        loop_thread = threading.get_ident()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://fixture") as client:
            slow = asyncio.create_task(client.get(f"/api/v1/associations/{path}"))
            try:
                # Poll cooperatively; the original async handler blocks even
                # this observer until its synchronous boundary returns.
                async def wait_for_start() -> None:
                    while not started.is_set():
                        await asyncio.sleep(0)

                await asyncio.wait_for(wait_for_start(), timeout=3)
                foreground = await asyncio.wait_for(
                    client.get("/api/v1/godview/latest"), timeout=1,
                )
                assert foreground.status_code == 200
                assert foreground.json()["source"] == "offline-fixture"
                assert not expired.is_set(), "the event loop was blocked until the failsafe released"
                assert not slow.done(), "God View must finish while the background read is still blocked"
                assert len(owner_threads) == 1
                assert owner_threads[0] != loop_thread
            finally:
                release.set()
                background = await asyncio.wait_for(slow, timeout=3)

            assert background.status_code == 200
            payload = background.json()
            if path == "correlation-matrix":
                assert payload["features"] == ["alpha", "beta"]
                assert payload["matrix"] == [[1.0, 1.0], [1.0, 1.0]]
                assert payload["pair_counts"] == {"interesting": 1, "expected": 0, "trivial": 0}
            else:
                assert payload == {"anomalies": [], "threshold": 2.5}

    asyncio.run(exercise())


@pytest.mark.unit
def test_saturated_worker_pool_still_queues_godview(isolated_app, monkeypatch):
    """Offloading frees the loop, but does not promise reserved foreground capacity."""
    from anyio.to_thread import current_default_thread_limiter

    app, associations, _auth = isolated_app
    started = threading.Event()
    release = threading.Event()

    def blocked_registry(_engine):
        started.set()
        release.wait(timeout=2)
        return pd.DataFrame()

    monkeypatch.setattr(associations, "_get_feature_registry", blocked_registry)

    async def exercise() -> None:
        limiter = current_default_thread_limiter()
        original_tokens = limiter.total_tokens
        limiter.total_tokens = 1
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture") as client:
            slow = asyncio.create_task(client.get("/api/v1/associations/correlation-matrix"))
            foreground = None
            try:
                async def observe(predicate) -> None:
                    while not predicate():
                        await asyncio.sleep(0)

                await asyncio.wait_for(observe(started.is_set), timeout=3)
                foreground = asyncio.create_task(client.get("/api/v1/godview/latest"))
                await asyncio.wait_for(
                    observe(lambda: limiter.statistics().tasks_waiting == 1), timeout=1,
                )
                assert not foreground.done()
            finally:
                release.set()
                try:
                    background = await asyncio.wait_for(slow, timeout=3)
                    if foreground is not None:
                        selected = await asyncio.wait_for(foreground, timeout=3)
                finally:
                    limiter.total_tokens = original_tokens

            assert background.status_code == selected.status_code == 200

    asyncio.run(exercise())


@pytest.mark.unit
@pytest.mark.parametrize(
    "path,expected",
    [("correlation-matrix", {"features": [], "matrix": [], "strong_pairs": []}),
     ("anomalies", {"anomalies": []})],
)
def test_empty_registry_retains_response(isolated_app, monkeypatch, path, expected):
    app, associations, _auth = isolated_app
    monkeypatch.setattr(associations, "_get_feature_registry", lambda _engine: pd.DataFrame())
    monkeypatch.setattr(
        associations, "_build_feature_matrix",
        lambda *_args: pytest.fail("empty registry must not request a PIT matrix"),
    )

    async def exercise() -> None:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture") as client:
            response = await client.get(f"/api/v1/associations/{path}")
            assert response.status_code == 200
            assert response.json() == expected

    asyncio.run(exercise())


@pytest.mark.unit
@pytest.mark.parametrize("path", ["correlation-matrix", "anomalies"])
@pytest.mark.parametrize("gate", ["auth", "validation"])
def test_rejected_requests_never_enter_database(isolated_app, monkeypatch, path, gate):
    app, associations, auth = isolated_app
    monkeypatch.setattr(
        associations, "get_db_engine",
        lambda: pytest.fail("request rejected before handler must not acquire a database"),
    )
    if gate == "auth":
        app.dependency_overrides.pop(auth)

    async def exercise() -> None:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture") as client:
            response = await client.get(
                f"/api/v1/associations/{path}",
                params={"days": 1} if gate == "validation" else {},
            )
            assert response.status_code == (401 if gate == "auth" else 422)

    asyncio.run(exercise())
