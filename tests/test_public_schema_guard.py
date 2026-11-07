"""The OpenAPI schema is not served through Cloudflare outside development (2026-11-07).

Production ran with ENVIRONMENT=development until 2026-11-07, so /api/docs
and /openapi.json were public. /api/docs is now off outside development; the
schema stays on localhost because deploy.yml checks routes against
localhost:8000/openapi.json, so it is refused only for tunnel traffic
(CF-Connecting-IP present).
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import api.main as main

CF = {"CF-Connecting-IP": "203.0.113.7"}


@pytest.fixture()
def client():
    return TestClient(main.app)


def test_schema_is_hidden_from_tunnel_traffic_in_production(client, monkeypatch) -> None:
    monkeypatch.setattr(main, "_environment", "production")
    for path in ("/openapi.json", "/api/docs", "/docs", "/redoc"):
        r = client.get(path, headers=CF)
        assert r.status_code == 404, path
        assert "paths" not in r.text


def test_local_deploy_check_still_reads_the_schema(client, monkeypatch) -> None:
    monkeypatch.setattr(main, "_environment", "production")
    r = client.get("/openapi.json")
    assert r.status_code == 200
    assert "/api/v1/intelligence/actor-network" in r.json()["paths"]


def test_development_serves_the_schema_through_the_tunnel(client, monkeypatch) -> None:
    monkeypatch.setattr(main, "_environment", "development")
    assert client.get("/openapi.json", headers=CF).status_code == 200


def test_other_routes_are_untouched(client, monkeypatch) -> None:
    monkeypatch.setattr(main, "_environment", "production")
    assert client.get("/api/v1/system/health", headers=CF).status_code != 404
