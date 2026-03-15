"""
GRID API integration tests.

Uses pytest + httpx for async FastAPI testing.
Tests run against the FastAPI app directly without requiring a live server.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# Set test environment before importing the app
os.environ["ENVIRONMENT"] = "development"
os.environ["GRID_JWT_SECRET"] = "test-secret-key-for-testing-only"
os.environ["GRID_JWT_EXPIRE_HOURS"] = "1"

# Create a test password hash for "testpassword123"
from passlib.context import CryptContext

_pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
_TEST_PASSWORD = "testpassword123"
_TEST_HASH = _pwd_ctx.hash(_TEST_PASSWORD)
os.environ["GRID_MASTER_PASSWORD_HASH"] = _TEST_HASH

from api.auth import create_token
from api.main import app

client = TestClient(app)


def _auth_header() -> dict[str, str]:
    """Return a valid Authorization header."""
    token = create_token(expires_hours=1)
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# 1. Health endpoint (no auth required)
# ---------------------------------------------------------------------------


class TestHealthNoAuth:
    def test_health_no_auth(self):
        """GET /api/v1/system/health returns 200 without auth."""
        response = client.get("/api/v1/system/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data


# ---------------------------------------------------------------------------
# 2. Protected route requires auth
# ---------------------------------------------------------------------------


class TestProtectedRouteRequiresAuth:
    def test_protected_route_requires_auth(self):
        """GET /api/v1/system/status without token returns 401."""
        response = client.get("/api/v1/system/status")
        assert response.status_code == 401


# ---------------------------------------------------------------------------
# 3. Login with invalid password
# ---------------------------------------------------------------------------


class TestLoginInvalidPassword:
    def test_login_invalid_password(self):
        """POST /api/v1/auth/login with wrong password returns 401."""
        response = client.post(
            "/api/v1/auth/login",
            json={"password": "wrong-password-here"},
        )
        assert response.status_code == 401


# ---------------------------------------------------------------------------
# 4. Login with valid password returns token
# ---------------------------------------------------------------------------


class TestLoginValidReturnsToken:
    def test_login_valid_returns_token(self):
        """POST /api/v1/auth/login with correct password returns token."""
        response = client.post(
            "/api/v1/auth/login",
            json={"password": _TEST_PASSWORD},
        )
        assert response.status_code == 200
        data = response.json()
        assert "token" in data
        assert isinstance(data["token"], str)
        assert len(data["token"]) > 20
        assert "expires_in" in data


# ---------------------------------------------------------------------------
# 5. Protected route with valid token
# ---------------------------------------------------------------------------


class TestProtectedRouteWithToken:
    def test_protected_route_with_token(self):
        """GET /api/v1/system/status with valid token returns 200."""
        response = client.get("/api/v1/system/status", headers=_auth_header())
        # May return 200 even if DB is down — the endpoint handles errors gracefully
        assert response.status_code == 200
        data = response.json()
        assert "database" in data
        assert "hyperspace" in data
        assert "grid" in data
        assert "uptime_seconds" in data
        assert "server_time" in data


# ---------------------------------------------------------------------------
# 6. Journal immutability via API
# ---------------------------------------------------------------------------


class TestJournalImmutabilityViaApi:
    @patch("api.dependencies.get_db_engine")
    def test_journal_immutability_via_api(self, mock_engine):
        """Journal outcome cannot be recorded twice (HTTP 409)."""
        mock_eng = MagicMock()
        mock_engine.return_value = mock_eng

        # Mock the journal to track calls
        with patch("api.routers.journal.get_journal") as mock_get_journal:
            mock_journal = MagicMock()

            # First call succeeds
            mock_journal.log_decision.return_value = 42
            mock_journal.record_outcome.side_effect = [
                True,  # First outcome recording succeeds
                ValueError("Outcome already recorded for decision 42. Journal is immutable."),
            ]
            mock_get_journal.return_value = mock_journal

            headers = _auth_header()

            # Create entry
            create_resp = client.post(
                "/api/v1/journal",
                json={
                    "model_version_id": 1,
                    "inferred_state": "Expansion",
                    "state_confidence": 0.85,
                    "transition_probability": 0.15,
                    "contradiction_flags": {},
                    "grid_recommendation": "Hold equities",
                    "baseline_recommendation": "Neutral",
                    "action_taken": "Held equities",
                    "counterfactual": "Reduced equity",
                    "operator_confidence": "HIGH",
                },
                headers=headers,
            )
            assert create_resp.status_code == 201
            entry_id = create_resp.json()["id"]

            # First outcome
            outcome1 = client.put(
                f"/api/v1/journal/{entry_id}/outcome",
                json={
                    "outcome_value": 0.05,
                    "verdict": "HELPED",
                    "annotation": "Good call",
                },
                headers=headers,
            )
            assert outcome1.status_code == 200

            # Second outcome — should be 409
            outcome2 = client.put(
                f"/api/v1/journal/{entry_id}/outcome",
                json={
                    "outcome_value": -0.02,
                    "verdict": "HARMED",
                },
                headers=headers,
            )
            assert outcome2.status_code == 409


# ---------------------------------------------------------------------------
# 7. Regime current returns UNCALIBRATED when no production model
# ---------------------------------------------------------------------------


class TestRegimeCurrentUncalibrated:
    def test_regime_current_uncalibrated(self):
        """GET /api/v1/regime/current returns UNCALIBRATED without production model."""
        # Mock get_db_engine at the point where regime router uses it
        with patch("api.routers.regime.get_db_engine") as mock_engine:
            mock_eng = MagicMock()
            mock_engine.return_value = mock_eng

            # Mock the connection to return no production models
            mock_conn = MagicMock()
            mock_eng.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_eng.connect.return_value.__exit__ = MagicMock(return_value=False)
            mock_conn.execute.return_value.fetchone.return_value = None

            response = client.get("/api/v1/regime/current", headers=_auth_header())
            assert response.status_code == 200
            data = response.json()
            assert data["state"] == "UNCALIBRATED"
