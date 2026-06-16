"""Smoke tests for the unauthenticated visualization router.

The 9 routes mounted under `/api/v1/viz/*` in `api/routers/viz.py` are the
only public, no-auth surface that returns canned VizSpec payloads — no DB
writes, no PII. The punch-list-2026-05-13 (api/ section, P2) calls for a
regression check so a future edit cannot accidentally widen exposure
without tripping a test.

This file pins the contract:
- Each route returns 200 without an Authorization header.
- The response shape is the documented envelope (`spec`/`weights`/list).

It does NOT assert that the routes MUST remain unauthenticated forever; if
the security model intentionally tightens, this file is the right place to
update and re-state intent.
"""

from __future__ import annotations

import os

# Match the env setup in tests/test_api.py so importing api.main succeeds
# regardless of which test module loads first.
os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")
os.environ.setdefault("GRID_JWT_EXPIRE_HOURS", "1")
os.environ.setdefault(
    "GRID_MASTER_PASSWORD_HASH",
    "$2b$12$abcdefghijklmnopqrstuuFb1mY3p5oXq0rN8sxqf6vV2QcVx1zSi",
)

import pytest
from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)


# The full set of routes mounted by api/routers/viz.py. Keep in sync with
# the @router.get decorators in that file.
SPEC_ROUTES = [
    "/api/v1/viz/spec/capital-flows",
    "/api/v1/viz/spec/regime-phase",
    "/api/v1/viz/spec/feature-network",
    "/api/v1/viz/spec/energy-particle",
    "/api/v1/viz/spec/sector-orbital",
    "/api/v1/viz/spec/lead-lag-river",
]


class TestVizRulesNoAuth:
    def test_rules_200_without_token(self):
        resp = client.get("/api/v1/viz/rules")
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, list)
        assert len(body) > 0
        # Each rule must carry the documented shape (see VISUALIZATION_RULES).
        first = body[0]
        assert "chart_type" in first
        assert "why" in first


class TestVizWeightsNoAuth:
    def test_weights_default_families_200(self):
        resp = client.get("/api/v1/viz/weights")
        assert resp.status_code == 200
        body = resp.json()
        assert set(body.keys()) == {"weights", "schedules"}
        assert isinstance(body["weights"], dict)
        assert isinstance(body["schedules"], dict)
        # Default response covers all known WEIGHT_SCHEDULES families.
        assert len(body["schedules"]) > 0

    def test_weights_explicit_families_200(self):
        resp = client.get("/api/v1/viz/weights?families=equity,flows")
        assert resp.status_code == 200
        body = resp.json()
        assert set(body["schedules"].keys()) == {"equity", "flows"}


class TestVizRecommendNoAuth:
    def test_recommend_minimal_query_200(self):
        resp = client.get(
            "/api/v1/viz/recommend",
            params={"description": "capital flowing between sectors"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert set(body.keys()) == {"spec", "reasoning"}
        assert "chart_type" in body["spec"]
        assert isinstance(body["reasoning"], str)

    def test_recommend_requires_description(self):
        # FastAPI validates the required `description` query param.
        resp = client.get("/api/v1/viz/recommend")
        assert resp.status_code == 422


@pytest.mark.parametrize("path", SPEC_ROUTES)
class TestVizSpecRoutesNoAuth:
    def test_spec_route_200_without_token(self, path: str):
        resp = client.get(path)
        assert resp.status_code == 200
        body = resp.json()
        # to_dict() on a VizSpec always emits chart_type.
        assert "chart_type" in body
        # Title is set on every spec defined in viz.py.
        assert body.get("title")


class TestVizUnknownRoute404:
    def test_unknown_spec_route_returns_404(self):
        # Locks in that arbitrary /spec/* paths are NOT served — only the
        # explicitly mounted handlers in SPEC_ROUTES respond.
        resp = client.get("/api/v1/viz/spec/does-not-exist")
        assert resp.status_code == 404
