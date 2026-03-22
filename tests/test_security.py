"""Tests for security fixes: JWT secret, DB password, security headers."""

from __future__ import annotations

import importlib
import os
import sys
from unittest.mock import MagicMock, patch

import pytest


class TestJWTSecretValidation:
    """GRID_JWT_SECRET must be set in non-development environments."""

    def test_raises_in_production_without_secret(self):
        with patch.dict(os.environ, {"ENVIRONMENT": "production", "GRID_JWT_SECRET": ""}, clear=False):
            from api.auth import _get_settings

            with pytest.raises(RuntimeError, match="GRID_JWT_SECRET"):
                _get_settings()

    def test_dev_fallback_works(self):
        with patch.dict(os.environ, {"ENVIRONMENT": "development", "GRID_JWT_SECRET": ""}, clear=False):
            from api.auth import _get_settings

            _, secret, _ = _get_settings()
            assert secret
            assert "DO-NOT-USE-IN-PRODUCTION" in secret

    def test_custom_secret_used(self):
        with patch.dict(os.environ, {"GRID_JWT_SECRET": "my-strong-secret"}, clear=False):
            from api.auth import _get_settings

            _, secret, _ = _get_settings()
            assert secret == "my-strong-secret"


class TestDBPasswordValidation:
    """DB_PASSWORD validator should reject 'changeme' in production.

    config.py has a module-level ``settings = Settings()`` singleton.
    When we force-reload with ENVIRONMENT=production and a bad password,
    that singleton construction itself raises ValidationError -- which
    IS the security behaviour we are verifying.
    """

    def test_changeme_rejected_in_production(self):
        """Module-level Settings() should raise when DB_PASSWORD='changeme'
        in a production environment."""
        from pydantic import ValidationError

        saved = sys.modules.pop("config", None)
        try:
            with patch.dict(
                os.environ,
                {"ENVIRONMENT": "production", "DB_PASSWORD": "changeme", "FRED_API_KEY": "fake"},
                clear=False,
            ):
                with pytest.raises(ValidationError):
                    importlib.import_module("config")
        finally:
            sys.modules.pop("config", None)
            if saved is not None:
                sys.modules["config"] = saved

    def test_empty_rejected_in_production(self):
        """Module-level Settings() should raise when DB_PASSWORD is empty
        in a production environment."""
        from pydantic import ValidationError

        saved = sys.modules.pop("config", None)
        try:
            with patch.dict(
                os.environ,
                {"ENVIRONMENT": "production", "DB_PASSWORD": "", "FRED_API_KEY": "fake"},
                clear=False,
            ):
                with pytest.raises(ValidationError):
                    importlib.import_module("config")
        finally:
            sys.modules.pop("config", None)
            if saved is not None:
                sys.modules["config"] = saved

    def test_changeme_allowed_in_development(self):
        with patch.dict(os.environ, {"ENVIRONMENT": "development"}, clear=False):
            from config import Settings

            s = Settings(DB_PASSWORD="changeme")
            assert s.DB_PASSWORD == "changeme"


class TestSecurityHeaders:
    """API responses should include security headers.

    Requires psycopg2 to be available (api.main imports db module).
    Tests are skipped if psycopg2 is not installed.
    """

    @pytest.fixture
    def test_client(self):
        try:
            import psycopg2  # noqa: F401
        except ImportError:
            pytest.skip("psycopg2 not installed -- cannot import api.main")

        env_vars = {
            "ENVIRONMENT": "development",
            "GRID_JWT_SECRET": "test-secret",
        }
        with patch.dict(os.environ, env_vars, clear=False):
            from fastapi.testclient import TestClient
            from api.main import app

            return TestClient(app)

    def test_x_content_type_options_present(self, test_client):
        resp = test_client.get("/api/docs")
        assert resp.headers.get("X-Content-Type-Options") == "nosniff"

    def test_x_frame_options_present(self, test_client):
        resp = test_client.get("/api/docs")
        assert resp.headers.get("X-Frame-Options") == "DENY"

    def test_x_xss_protection_present(self, test_client):
        resp = test_client.get("/api/docs")
        assert resp.headers.get("X-XSS-Protection") == "1; mode=block"

    def test_referrer_policy_present(self, test_client):
        resp = test_client.get("/api/docs")
        assert resp.headers.get("Referrer-Policy") == "strict-origin-when-cross-origin"
