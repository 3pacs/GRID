"""``GET /api/v1/conviction/sweeps`` and ``/sweeps/latest``.

Read-back of ``universe_ranking_history`` for the canvas verdict layer. The
domain helpers are monkeypatched so no DB is touched; the route contract
(filters forwarded, 404 when nothing has run, list pagination fields) is
what these tests pin.
"""
from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from api.routers import conviction as conviction_module
from tests.test_api_conviction import _build_app

_SWEEP = {
    "id": 7,
    "generated_at": "2026-09-06T05:00:00+00:00",
    "universe_name": "custom",
    "horizon_days": 90,
    "tickers_attempted": 33,
    "tickers_succeeded": 31,
    "regime_signature": "trending",
    "top_k": [{"ticker": "NVDA", "verdict": "high", "composite_score": 1.3}],
    "sector_distributions": [],
    "concentration_alerts": [],
    "narrative": "One name actionable.",
}


@pytest.fixture
def client() -> TestClient:
    return TestClient(_build_app())


def test_latest_sweep_forwards_filters(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    seen: dict[str, Any] = {}

    def fake_latest(engine: Any, **kwargs: Any) -> dict[str, Any]:
        seen.update(kwargs)
        return _SWEEP

    monkeypatch.setattr(conviction_module, "load_latest_ranking", fake_latest)
    resp = client.get("/api/v1/conviction/sweeps/latest?horizon_days=90&universe=custom")
    assert resp.status_code == 200
    assert seen == {"horizon_days": 90, "universe_name": "custom"}
    body = resp.json()
    assert body["horizon_days"] == 90
    assert body["top_k"][0]["ticker"] == "NVDA"


def test_latest_sweep_404_when_nothing_persisted(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    monkeypatch.setattr(conviction_module, "load_latest_ranking", lambda *a, **k: None)
    resp = client.get("/api/v1/conviction/sweeps/latest?horizon_days=180")
    assert resp.status_code == 404
    detail = resp.json()["detail"]
    assert detail["stage"] == "load_latest_ranking"
    assert detail["horizon_days"] == 180


def test_latest_sweep_validates_horizon(client: TestClient) -> None:
    assert client.get("/api/v1/conviction/sweeps/latest?horizon_days=0").status_code == 422
    assert client.get("/api/v1/conviction/sweeps/latest?horizon_days=999").status_code == 422


def test_list_sweeps_pagination_contract(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    seen: dict[str, Any] = {}

    def fake_list(engine: Any, **kwargs: Any) -> dict[str, Any]:
        seen.update(kwargs)
        return {"entries": [_SWEEP], "total": 3, "limit": 1, "offset": 2, "has_more": False}

    monkeypatch.setattr(conviction_module, "list_rankings", fake_list)
    resp = client.get("/api/v1/conviction/sweeps?limit=1&offset=2&horizon_days=90")
    assert resp.status_code == 200
    assert seen == {"limit": 1, "offset": 2, "horizon_days": 90}
    body = resp.json()
    assert set(body) == {"entries", "total", "limit", "offset", "has_more"}
    assert body["entries"][0]["id"] == 7


def test_list_sweeps_bounds(client: TestClient) -> None:
    assert client.get("/api/v1/conviction/sweeps?limit=0").status_code == 422
    assert client.get("/api/v1/conviction/sweeps?limit=101").status_code == 422
    assert client.get("/api/v1/conviction/sweeps?offset=-1").status_code == 422
