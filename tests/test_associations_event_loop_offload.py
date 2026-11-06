"""Blocking DB/PIT work in async handlers must not run on the event loop.

2026-10-19: the single uvicorn process logged "DB connection held 29.7s --
acquired_by=MainThread" while GET /api/v1/associations/correlation-matrix (an
``async def`` that built a 757x2599 PIT matrix inline) ran, stalling every other
route. /intelligence/levers offloads its sync body to a worker thread. Since
W3-2 (2026-11-06) the association views do not compute in the request at all:
the precompute job builds them and the GETs read the stored result; the tests
below pin the unchanged body shapes and that the routes never compute.
Offline: every DB / PIT / builder call is a fake.
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


def test_correlation_matrix_body_shape_is_unchanged(assoc) -> None:
    """The build moved to the precompute job (W3-2); its body is unchanged."""
    _threads, _started, monkeypatch = assoc
    monkeypatch.setattr(associations, "_build_feature_matrix",
                        lambda *_a, **_k: _fixture_matrix())

    body = associations._compute_correlation_matrix(252, 0.3, False)

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
    body = associations._compute_correlation_matrix(252, 0.3, False)
    assert body == {"features": [], "matrix": [], "strong_pairs": []}


def _anomaly_matrix() -> pd.DataFrame:
    """100 days, two features; feat_a ends on a 6-sigma spike."""
    idx = pd.date_range("2026-06-01", periods=100, freq="D")
    a = [float(i % 5) for i in range(99)] + [40.0]
    b = [float((i * 3) % 7) for i in range(100)]
    matrix = pd.DataFrame({1: a, 2: b}, index=idx)
    matrix.attrs["excluded_stale"] = {}
    return matrix


def test_anomalies_body_shape_and_alert_switch(assoc) -> None:
    _threads, _started, monkeypatch = assoc
    alerts: list[str] = []
    monkeypatch.setattr(associations, "_build_feature_matrix",
                        lambda *_a, **_k: _anomaly_matrix())
    monkeypatch.setitem(sys.modules, "alerts.email", types.SimpleNamespace(
        alert_on_discovery_insight=lambda title, _body: alerts.append(title)))

    body = associations._compute_anomalies(2.5, 252, alert=False)

    assert set(body) == {"anomalies", "threshold"} and body["threshold"] == 2.5
    [anomaly] = body["anomalies"]
    assert anomaly["feature"] == "feat_a" and anomaly["severity"] == "extreme"
    assert set(anomaly) == {
        "feature", "current_value", "zscore", "historical_mean", "historical_std",
        "severity", "broken_correlations",
    }
    assert alerts == []  # the job alerts itself, only on a changed extreme set
    associations._compute_anomalies(2.5, 252, alert=True)
    assert alerts == ["Extreme Anomalies Detected (1)"]


def test_association_routes_never_compute(assoc, monkeypatch) -> None:
    """GETs read api_precomputed_results; the builders must not run."""
    _threads, _started, _mp = assoc

    def boom(*_a, **_k):
        raise AssertionError("route computed inline")

    for name in ("_build_feature_matrix", "_compute_correlation_matrix",
                 "_compute_anomalies", "_compute_regime_features"):
        monkeypatch.setattr(associations, name, boom)
    from api import precompute_serving

    monkeypatch.setattr(precompute_serving.store, "read_result", lambda *_a, **_k: None)
    app, _ping, _release = _app(associations.router)
    with TestClient(app) as client:
        for url in ("/api/v1/associations/correlation-matrix?days=252",
                    "/api/v1/associations/anomalies?sigma_threshold=2.5",
                    "/api/v1/associations/regime-features"):
            r = client.get(url)
            assert r.status_code == 503, url
            assert r.json()["available"] is False


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
