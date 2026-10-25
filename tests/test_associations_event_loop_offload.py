"""Blocking DB/PIT work in async handlers must not run on the event loop.

2026-10-19: the single uvicorn process logged "DB connection held 29.7s --
acquired_by=MainThread" while GET /api/v1/associations/correlation-matrix (an
``async def`` that built a 757x2599 PIT matrix inline) ran, stalling every other
route. The handler (and /intelligence/levers, same shape) now offload the sync
body to a worker thread. Offline: every DB / PIT / builder call is a fake.
"""

from __future__ import annotations

import sys
import threading
import types
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.auth import require_auth
from api.routers import associations, intelligence_deepdive


def _app(router) -> tuple[FastAPI, list[bool], threading.Event]:
    release = threading.Event()
    ping_before_release: list[bool] = []
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[require_auth] = lambda: "fixture"

    @app.get("/ping")
    async def ping():
        ping_before_release.append(not release.is_set())
        return {"ok": True}

    return app, ping_before_release, release


def _fixture_matrix() -> pd.DataFrame:
    idx = pd.date_range("2026-09-01", periods=40, freq="D")
    matrix = pd.DataFrame({1: range(40), 2: [i * 2.0 + (i % 3) for i in range(40)]}, index=idx)
    matrix.attrs["excluded_stale"] = {}
    return matrix


@pytest.fixture
def assoc(monkeypatch):
    threads: dict[str, int] = {}
    started = threading.Event()

    registry = pd.DataFrame(
        [(1, "feat_a", "rates", ""), (2, "feat_b", "credit", "")],
        columns=["id", "name", "family", "description"],
    )

    def get_engine():
        threads["engine"] = threading.get_ident()
        return object()

    def get_pit():
        threads["pit"] = threading.get_ident()
        return object()

    monkeypatch.setattr(associations, "get_db_engine", get_engine)
    monkeypatch.setattr(associations, "get_pit_store", get_pit)
    monkeypatch.setattr(associations, "_get_feature_registry", lambda _e: registry)
    return threads, started, monkeypatch


def test_correlation_matrix_build_runs_off_the_event_loop_and_loop_stays_responsive(assoc) -> None:
    threads, started, monkeypatch = assoc
    app, ping_before_release, release = _app(associations.router)

    def build(_pit, _ids, _days):
        threads["build"] = threading.get_ident()
        started.set()
        release.wait(5)
        return _fixture_matrix()

    monkeypatch.setattr(associations, "_build_feature_matrix", build)

    with TestClient(app) as client, ThreadPoolExecutor(max_workers=1) as pool:
        loop_thread = client.portal.call(threading.get_ident)
        request = pool.submit(client.get, "/api/v1/associations/correlation-matrix?days=252")
        assert started.wait(2)
        fallback = threading.Timer(3, release.set)
        fallback.start()
        try:
            assert client.get("/ping").json() == {"ok": True}
            assert ping_before_release == [True]  # served while the build was still blocked
        finally:
            release.set()
            fallback.cancel()
        response = request.result(timeout=5)

    assert response.status_code == 200
    assert {threads["engine"], threads["pit"], threads["build"]}.isdisjoint({loop_thread})
    body = response.json()
    # Response shape is unchanged.
    assert set(body) == {
        "features", "matrix", "strong_pairs", "pair_counts", "as_of_date",
        "window_start", "window_end", "excluded_stale_features",
    }
    assert body["features"] == ["feat_a", "feat_b"]
    assert body["matrix"][0][0] == 1.0


def test_correlation_matrix_empty_registry_shape_is_unchanged(assoc) -> None:
    _threads, _started, monkeypatch = assoc
    monkeypatch.setattr(
        associations, "_get_feature_registry",
        lambda _e: pd.DataFrame(columns=["id", "name", "family", "description"]),
    )
    app, _ping, _release = _app(associations.router)

    with TestClient(app) as client:
        body = client.get("/api/v1/associations/correlation-matrix").json()

    assert body == {"features": [], "matrix": [], "strong_pairs": []}


def _anomaly_matrix() -> pd.DataFrame:
    """100 days, two features; feat_a ends on a 6-sigma spike."""
    idx = pd.date_range("2026-06-01", periods=100, freq="D")
    a = [float(i % 5) for i in range(99)] + [40.0]
    b = [float((i * 3) % 7) for i in range(100)]
    matrix = pd.DataFrame({1: a, 2: b}, index=idx)
    matrix.attrs["excluded_stale"] = {}
    return matrix


def test_anomalies_build_runs_off_the_event_loop_and_loop_stays_responsive(assoc) -> None:
    threads, started, monkeypatch = assoc
    app, ping_before_release, release = _app(associations.router)
    alerts: list[tuple[str, int]] = []

    def build(_pit, _ids, _days):
        threads["build"] = threading.get_ident()
        started.set()
        release.wait(5)
        return _anomaly_matrix()

    def alert(title, _body):
        alerts.append((title, threading.get_ident()))

    monkeypatch.setattr(associations, "_build_feature_matrix", build)
    monkeypatch.setitem(sys.modules, "alerts.email", types.SimpleNamespace(
        alert_on_discovery_insight=alert))

    with TestClient(app) as client, ThreadPoolExecutor(max_workers=1) as pool:
        loop_thread = client.portal.call(threading.get_ident)
        request = pool.submit(client.get, "/api/v1/associations/anomalies?sigma_threshold=2.5")
        assert started.wait(2)
        fallback = threading.Timer(3, release.set)
        fallback.start()
        try:
            assert client.get("/ping").json() == {"ok": True}
            assert ping_before_release == [True]  # served while the build was still blocked
        finally:
            release.set()
            fallback.cancel()
        response = request.result(timeout=5)

    assert response.status_code == 200
    assert {threads["engine"], threads["pit"], threads["build"]}.isdisjoint({loop_thread})
    body = response.json()
    # Response shape is unchanged.
    assert set(body) == {"anomalies", "threshold"} and body["threshold"] == 2.5
    [anomaly] = body["anomalies"]
    assert anomaly["feature"] == "feat_a" and anomaly["severity"] == "extreme"
    assert set(anomaly) == {
        "feature", "current_value", "zscore", "historical_mean", "historical_std",
        "severity", "broken_correlations",
    }
    # The alert e-mail (blocking SMTP) also left the event loop.
    assert [t for t, _ in alerts] == ["Extreme Anomalies Detected (1)"]
    assert loop_thread not in {ident for _, ident in alerts}


def test_anomalies_short_circuit_shapes_are_unchanged(assoc) -> None:
    _threads, _started, monkeypatch = assoc
    app, _ping, _release = _app(associations.router)

    with TestClient(app) as client:
        monkeypatch.setattr(
            associations, "_build_feature_matrix",
            lambda *_a: _fixture_matrix().iloc[:10],  # < 30 rows
        )
        assert client.get("/api/v1/associations/anomalies").json() == {
            "anomalies": [], "message": "Insufficient data"}
        monkeypatch.setattr(
            associations, "_get_feature_registry",
            lambda _e: pd.DataFrame(columns=["id", "name", "family", "description"]),
        )
        assert client.get("/api/v1/associations/anomalies").json() == {"anomalies": []}


def test_levers_build_runs_off_the_event_loop_and_loop_stays_responsive(monkeypatch) -> None:
    started = threading.Event()
    threads: dict[str, int] = {}
    app, ping_before_release, release = _app(intelligence_deepdive.router)

    def hierarchy(_engine):
        threads["build"] = threading.get_ident()
        started.set()
        release.wait(5)
        return {"hierarchy": {"monetary": []}}

    monkeypatch.setitem(sys.modules, "intelligence.global_levers", types.SimpleNamespace(
        get_lever_hierarchy=hierarchy,
        find_cross_domain_actors=lambda _engine: [{"actor": "a"}] * 30,
    ))
    monkeypatch.setattr(intelligence_deepdive, "get_db_engine", lambda: object())
    monkeypatch.setattr(intelligence_deepdive, "_lever_cache", intelligence_deepdive.TTLCache(ttl=600, max_size=1))

    with TestClient(app) as client, ThreadPoolExecutor(max_workers=1) as pool:
        loop_thread = client.portal.call(threading.get_ident)
        request = pool.submit(client.get, "/levers")
        assert started.wait(2)
        fallback = threading.Timer(3, release.set)
        fallback.start()
        try:
            assert client.get("/ping").json() == {"ok": True}
            assert ping_before_release == [True]
        finally:
            release.set()
            fallback.cancel()
        response = request.result(timeout=5)
        cached = client.get("/levers").json()

    assert threads["build"] != loop_thread
    body = response.json()
    assert body["hierarchy"] == {"monetary": []}
    assert len(body["cross_domain_actors"]) == 20  # unchanged [:20] truncation
    assert cached == body  # second call is served from the TTL cache


def test_levers_failure_shape_is_unchanged(monkeypatch) -> None:
    app, _ping, _release = _app(intelligence_deepdive.router)

    def boom(_engine):
        raise RuntimeError("db down")

    monkeypatch.setitem(sys.modules, "intelligence.global_levers", types.SimpleNamespace(
        get_lever_hierarchy=boom, find_cross_domain_actors=lambda _e: []))
    monkeypatch.setattr(intelligence_deepdive, "get_db_engine", lambda: object())
    monkeypatch.setattr(intelligence_deepdive, "_lever_cache", intelligence_deepdive.TTLCache(ttl=600, max_size=1))

    with TestClient(app) as client:
        body = client.get("/levers").json()

    assert body == {"error": "db down", "hierarchy": {}}
