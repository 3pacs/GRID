"""``GET /api/v1/conviction/long-plays`` and ``POST .../long-plays/refresh``.

Read-back of ``long_plays_board`` plus the admin-only rebuild. The domain
helpers are monkeypatched so no DB is touched; the route contract (404
when nothing has run, ``top_k`` bounds, admin gating, persist-after-build)
is what these tests pin.
"""
from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from api.routers import conviction as conviction_module
from tests.test_api_conviction import _build_app

_BOARD = {
    "id": 3,
    "as_of": "2026-09-06",
    "generated_at": "2026-09-06T05:30:00+00:00",
    "universe_size": 31,
    "candidates": [
        {"ticker": "CCJ", "stance": "entry_candidate", "asymmetry_score": 0.71},
        {"ticker": "UEC", "stance": "avoid", "asymmetry_score": 0.02},
    ],
    "entry_candidates": 1,
    "stand_down_reason": None,
    "method_notes": ["projection: proxy"],
}


@pytest.fixture
def client() -> TestClient:
    return TestClient(_build_app())


@pytest.fixture
def admin_client() -> TestClient:
    app = _build_app()
    app.dependency_overrides[conviction_module._require_admin] = lambda: "admin-token"
    return TestClient(app)


def test_get_long_plays_returns_latest_board(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    seen: list[Any] = []

    def fake_latest(engine: Any) -> dict[str, Any]:
        seen.append(engine)
        return _BOARD

    monkeypatch.setattr(conviction_module, "load_latest_board", fake_latest)
    resp = client.get("/api/v1/conviction/long-plays")
    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == 3
    assert body["candidates"][0]["ticker"] == "CCJ"
    assert body["stand_down_reason"] is None
    assert len(seen) == 1


def test_get_long_plays_404_when_nothing_persisted(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    monkeypatch.setattr(conviction_module, "load_latest_board", lambda engine: None)
    resp = client.get("/api/v1/conviction/long-plays")
    assert resp.status_code == 404
    assert resp.json()["detail"]["stage"] == "load_latest_board"


def test_refresh_requires_admin(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    called: list[Any] = []
    monkeypatch.setattr(conviction_module, "build_long_plays_board", lambda *a, **k: called.append(1) or _BOARD)
    # ``require_auth`` is overridden in ``_build_app`` but ``_require_admin`` is not:
    # with no bearer token the admin dependency must reject before the build runs.
    resp = client.post("/api/v1/conviction/long-plays/refresh")
    assert resp.status_code == 401
    assert called == []


def test_refresh_builds_persists_and_returns_board(monkeypatch: pytest.MonkeyPatch, admin_client: TestClient) -> None:
    seen: dict[str, Any] = {}

    def fake_build(engine: Any, **kwargs: Any) -> dict[str, Any]:
        seen["build"] = kwargs
        return dict(_BOARD)

    def fake_persist(engine: Any, board: dict[str, Any]) -> int:
        seen["persisted"] = board["as_of"]
        return 44

    monkeypatch.setattr(conviction_module, "build_long_plays_board", fake_build)
    monkeypatch.setattr(conviction_module, "persist_board", fake_persist)
    resp = admin_client.post("/api/v1/conviction/long-plays/refresh?top_k=7")
    assert resp.status_code == 200
    assert seen == {"build": {"top_k": 7}, "persisted": "2026-09-06"}
    body = resp.json()
    assert body["persisted_id"] == 44
    assert body["universe_size"] == 31
    assert body["candidates"][0]["stance"] == "entry_candidate"


def test_refresh_bounds_top_k(monkeypatch: pytest.MonkeyPatch, admin_client: TestClient) -> None:
    monkeypatch.setattr(conviction_module, "build_long_plays_board", lambda *a, **k: _BOARD)
    monkeypatch.setattr(conviction_module, "persist_board", lambda *a, **k: 1)
    assert admin_client.post("/api/v1/conviction/long-plays/refresh?top_k=0").status_code == 422
    assert admin_client.post("/api/v1/conviction/long-plays/refresh?top_k=101").status_code == 422
    assert admin_client.post("/api/v1/conviction/long-plays/refresh").status_code == 200


def test_refresh_surfaces_build_failure_as_500(monkeypatch: pytest.MonkeyPatch, admin_client: TestClient) -> None:
    def boom(*a: Any, **k: Any) -> Any:
        raise RuntimeError("universe exploded")

    monkeypatch.setattr(conviction_module, "build_long_plays_board", boom)
    resp = admin_client.post("/api/v1/conviction/long-plays/refresh")
    assert resp.status_code == 500
    detail = resp.json()["detail"]
    assert detail["stage"] == "build_long_plays_board"
    assert detail["error_type"] == "RuntimeError"
