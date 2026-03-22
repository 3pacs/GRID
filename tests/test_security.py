"""Tests for security fixes: JWT secret, DB password, security headers."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest


class TestJWTSecretValidation:
    def test_raises_in_production_without_secret(self):
        with patch.dict(os.environ, {"ENVIRONMENT": "production", "GRID_JWT_SECRET": ""}, clear=False):
            from api.auth import _get_settings
            with pytest.raises(RuntimeError, match="GRID_JWT_SECRET must be set"):
                _get_settings()

    def test_dev_fallback_works(self):
        with patch.dict(os.environ, {"ENVIRONMENT": "development", "GRID_JWT_SECRET": ""}, clear=False):
            from api.auth import _get_settings
            _, secret, _ = _get_settings()
            assert "DO-NOT-USE-IN-PRODUCTION" in secret

    def test_custom_secret_used(self):
        with patch.dict(os.environ, {"GRID_JWT_SECRET": "my-strong-secret"}, clear=False):
            from api.auth import _get_settings
            _, secret, _ = _get_settings()
            assert secret == "my-strong-secret"


class TestDBPasswordValidation:
    def test_changeme_rejected_in_production(self):
        with patch.dict(os.environ, {"ENVIRONMENT": "production"}, clear=False):
            from config import Settings
            with pytest.raises(Exception):  # Pydantic ValidationError
                Settings(DB_PASSWORD="changeme")

    def test_empty_rejected_in_production(self):
        with patch.dict(os.environ, {"ENVIRONMENT": "production"}, clear=False):
            from config import Settings
            with pytest.raises(Exception):
                Settings(DB_PASSWORD="")

    def test_changeme_allowed_in_development(self):
        with patch.dict(os.environ, {"ENVIRONMENT": "development"}, clear=False):
            from config import Settings
            s = Settings(DB_PASSWORD="changeme")
            assert s.DB_PASSWORD == "changeme"
