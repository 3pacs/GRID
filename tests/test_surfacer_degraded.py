"""Surfacer must report an outage as an outage, never as a market call.

Covers audit findings B-M14 (a swallowed DB exception rendered as a confident
"Stand down / Nothing cleared the front page" brief) and the honesty rules that
go with it: a degraded response carries a status and an error, carries no brief,
and is never cached.
"""

from __future__ import annotations

import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.auth import require_auth
from api.dependencies import get_db_engine
from api.routers.surfacer import router


class _ExplodingEngine:
    """Stands in for an engine whose connection attempt fails."""

    def begin(self):
        raise RuntimeError("connection to server at 'localhost', port 5432 failed")


def _client(engine) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[require_auth] = lambda: None
    app.dependency_overrides[get_db_engine] = lambda: engine
    return TestClient(app)


@pytest.fixture()
def degraded_response():
    with _client(_ExplodingEngine()) as client:
        yield client.get("/api/v1/surfacer/candidates")


def test_degraded_response_declares_its_status_and_error(degraded_response) -> None:
    payload = degraded_response.json()

    assert degraded_response.status_code in (200, 503)
    assert payload["status"] == "degraded"
    assert isinstance(payload["error"], str) and payload["error"]


def test_degraded_response_never_says_stand_down(degraded_response) -> None:
    """The exact regression from B-M14: an outage rendered as a market call."""
    body = json.dumps(degraded_response.json())

    assert "Stand down" not in body
    assert "Nothing cleared the front page" not in body
    assert degraded_response.json()["brief"] is None


def test_degraded_response_has_no_fabricated_candidate_read(degraded_response) -> None:
    payload = degraded_response.json()

    # candidates: [] would read as "we looked and found nothing".
    assert payload["candidates"] is None
    assert payload["thesis"] is None
    assert payload["meta"]["count"] is None
    assert payload["meta"]["status"] == "degraded"


def test_degraded_response_is_not_cached(degraded_response) -> None:
    """Two independent caches must both refuse this payload.

    - HTTP/offline mirror: the no-store header (scripts/offline_bake/bake.py
      additionally refuses to write any body whose status is "degraded").
    - PWA client: pwa/src/api.js only caches a GET when `!data?.error`.
    """
    assert "no-store" in degraded_response.headers.get("cache-control", "")
    assert degraded_response.json()["error"]


def test_healthy_response_is_marked_ok() -> None:
    """A real read is still a real read — status ok, error null, brief present."""

    class _Engine:
        def begin(self):
            return _NoopConn()

    class _NoopConn:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, statement, params=None):
            class _Result:
                def scalar(self):
                    return None

                def fetchall(self):
                    return []

                def fetchone(self):
                    return None

            return _Result()

    with _client(_Engine()) as client:
        response = client.get("/api/v1/surfacer/candidates")

    payload = response.json()
    assert response.status_code == 200
    assert payload["status"] == "ok"
    assert payload["error"] is None
    assert payload["candidates"] == []
    # With the DB reachable and genuinely empty, a stand-down brief is honest.
    assert payload["brief"]["stance"] == "Stand down"
    assert "no-store" not in response.headers.get("cache-control", "")
