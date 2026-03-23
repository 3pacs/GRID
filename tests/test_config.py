"""
Tests for the GRID configuration endpoints.

Verifies config read/write, source updates, feature updates,
and field validation / allowlisting.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# Set test environment before importing the app
os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")
os.environ.setdefault("GRID_JWT_EXPIRE_HOURS", "1")

from passlib.context import CryptContext

_pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
os.environ.setdefault("GRID_MASTER_PASSWORD_HASH", _pwd_ctx.hash("testpassword123"))

from api.auth import create_token
from api.main import app

client = TestClient(app)


def _auth_header() -> dict[str, str]:
    """Return a valid Authorization header."""
    token = create_token(expires_hours=1)
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# GET /api/v1/config — read configuration
# ---------------------------------------------------------------------------


class TestGetConfig:
    """Reading system configuration hides sensitive fields."""

    def test_get_config_requires_auth(self):
        """GET /api/v1/config without token returns 401."""
        response = client.get("/api/v1/config")
        assert response.status_code == 401

    def test_get_config_returns_config(self):
        """GET /api/v1/config with valid token returns config dict."""
        response = client.get("/api/v1/config", headers=_auth_header())
        assert response.status_code == 200
        data = response.json()
        assert "config" in data
        config = data["config"]
        # Sensitive fields must be excluded
        for field in ("DB_PASSWORD", "FRED_API_KEY", "GRID_MASTER_PASSWORD_HASH", "GRID_JWT_SECRET"):
            assert field not in config and field.lower() not in config


# ---------------------------------------------------------------------------
# PUT /api/v1/config — update configuration
# ---------------------------------------------------------------------------


class TestUpdateConfig:
    """Updating configuration respects sensitive-field restrictions."""

    def test_update_sensitive_field_rejected(self):
        """PUT /api/v1/config with a sensitive field returns 400."""
        response = client.put(
            "/api/v1/config",
            json={"DB_PASSWORD": "hacked"},
            headers=_auth_header(),
        )
        assert response.status_code == 400
        assert "sensitive" in response.json()["detail"].lower()

    def test_update_nonexistent_field_ignored(self):
        """PUT /api/v1/config with unknown field returns 200 but no updates."""
        response = client.put(
            "/api/v1/config",
            json={"totally_fake_field": 42},
            headers=_auth_header(),
        )
        assert response.status_code == 200
        assert response.json()["updated"] == {}


# ---------------------------------------------------------------------------
# PUT /api/v1/config/sources/{id} — update source
# ---------------------------------------------------------------------------


class TestUpdateSource:
    """Source update endpoint validates fields against allowlist."""

    @patch("api.routers.config.get_db_engine")
    def test_update_source_disallowed_fields_rejected(self, mock_get_engine):
        """Fields outside the allowlist are silently ignored; empty update = 400."""
        response = client.put(
            "/api/v1/config/sources/1",
            json={"name": "evil_source"},
            headers=_auth_header(),
        )
        assert response.status_code == 400
        assert "No valid fields" in response.json()["detail"]

    @patch("api.routers.config.get_db_engine")
    def test_update_source_allowed_field(self, mock_get_engine):
        """Allowed field (active) is accepted and forwarded to DB."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.execute.return_value.rowcount = 1

        response = client.put(
            "/api/v1/config/sources/1",
            json={"active": False},
            headers=_auth_header(),
        )
        assert response.status_code == 200
        assert response.json()["status"] == "updated"

    @patch("api.routers.config.get_db_engine")
    def test_update_source_not_found(self, mock_get_engine):
        """Non-existent source returns 404."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.execute.return_value.rowcount = 0

        response = client.put(
            "/api/v1/config/sources/9999",
            json={"active": True},
            headers=_auth_header(),
        )
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# PUT /api/v1/config/features/{id} — update feature
# ---------------------------------------------------------------------------


class TestUpdateFeature:
    """Feature update endpoint validates fields against allowlist."""

    @patch("api.routers.config.get_db_engine")
    def test_update_feature_disallowed_fields_rejected(self, mock_get_engine):
        """Fields outside feature allowlist are rejected."""
        response = client.put(
            "/api/v1/config/features/1",
            json={"name": "evil"},
            headers=_auth_header(),
        )
        assert response.status_code == 400

    @patch("api.routers.config.get_db_engine")
    def test_update_feature_allowed_field(self, mock_get_engine):
        """Allowed field (model_eligible) is accepted."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.execute.return_value.rowcount = 1

        response = client.put(
            "/api/v1/config/features/1",
            json={"model_eligible": True},
            headers=_auth_header(),
        )
        assert response.status_code == 200
        assert response.json()["status"] == "updated"


# ---------------------------------------------------------------------------
# Security headers — verify CSP is present
# ---------------------------------------------------------------------------


class TestSecurityHeaders:
    """Verify security headers are set on all responses."""

    def test_csp_header_present(self):
        """Responses include Content-Security-Policy header."""
        response = client.get("/api/v1/system/health")
        assert "content-security-policy" in response.headers
        csp = response.headers["content-security-policy"]
        assert "default-src 'self'" in csp
        assert "object-src 'none'" in csp

    def test_x_content_type_options(self):
        """Responses include X-Content-Type-Options: nosniff."""
        response = client.get("/api/v1/system/health")
        assert response.headers.get("x-content-type-options") == "nosniff"

    def test_x_frame_options(self):
        """Responses include X-Frame-Options: DENY."""
        response = client.get("/api/v1/system/health")
        assert response.headers.get("x-frame-options") == "DENY"
