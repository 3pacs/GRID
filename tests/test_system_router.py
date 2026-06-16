"""Smoke tests for the system router's operator-facing health endpoints.

Covers the happy path of `/freshness`, `/pipeline-health`, and `/hermes-status`
— three endpoints in the 1,686-LOC `api/routers/system.py` that previously had
no direct coverage (only `/health` and `/status` were exercised in
`tests/test_api.py`). Each handler wraps its DB access in try/except and
degrades to empty/neutral payloads, so a mocked empty engine drives the
graceful-degradation path and asserts the response envelope shape.
"""

from __future__ import annotations

import os
from unittest.mock import patch

# Match the test environment set up in tests/test_api.py so the app imports
# with a valid JWT secret regardless of which test module loads first.
os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")
os.environ.setdefault("GRID_JWT_EXPIRE_HOURS", "1")
# A pre-computed bcrypt hash so the app imports with a configured master
# password. These endpoints use bearer-token auth (create_token), so the
# hash itself is never verified here.
os.environ.setdefault(
    "GRID_MASTER_PASSWORD_HASH",
    "$2b$12$abcdefghijklmnopqrstuuFb1mY3p5oXq0rN8sxqf6vV2QcVx1zSi",
)

from fastapi.testclient import TestClient

from api.auth import create_token
from api.main import app

client = TestClient(app)


def _auth_header() -> dict[str, str]:
    return {"Authorization": f"Bearer {create_token(expires_hours=1)}"}


class TestFreshness:
    def test_requires_auth(self):
        assert client.get("/api/v1/system/freshness").status_code == 401

    def test_happy_path_empty_engine(self, mock_engine):
        with patch("api.routers.system.get_db_engine", return_value=mock_engine):
            resp = client.get("/api/v1/system/freshness", headers=_auth_header())
        assert resp.status_code == 200
        data = resp.json()
        assert data["families"] == []
        # No families => worst-case overall status.
        assert data["overall_status"] == "RED"


class TestPipelineHealth:
    def test_requires_auth(self):
        assert client.get("/api/v1/system/pipeline-health").status_code == 401

    def test_happy_path_empty_engine(self, mock_engine):
        with patch("api.routers.system.get_db_engine", return_value=mock_engine):
            resp = client.get(
                "/api/v1/system/pipeline-health", headers=_auth_header()
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["sources"] == []
        assert data["summary"]["total_sources"] == 0
        assert data["summary"]["healthy"] == 0
        assert data["recent_errors"] == []


class TestHermesStatus:
    def test_requires_auth(self):
        assert client.get("/api/v1/system/hermes-status").status_code == 401

    def test_happy_path_no_running_operator(self, mock_engine):
        with patch("api.routers.system._hermes_state", None), patch(
            "api.routers.system.get_db_engine", return_value=mock_engine
        ):
            resp = client.get(
                "/api/v1/system/hermes-status", headers=_auth_header()
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["running"] is False
        assert data["task_count"] == 0
        assert data["tasks"] == []
        assert "schedule" in data
