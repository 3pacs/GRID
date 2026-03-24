"""
Tests for the celestial signals endpoint.

Verifies the /api/v1/signals/celestial endpoint returns correctly
shaped responses and uses PIT-correct queries.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

# Set test environment before importing the app
os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")
os.environ.setdefault("GRID_JWT_EXPIRE_HOURS", "1")

from passlib.context import CryptContext

_pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
_TEST_PASSWORD = "testpassword123"
_TEST_HASH = _pwd_ctx.hash(_TEST_PASSWORD)
os.environ.setdefault("GRID_MASTER_PASSWORD_HASH", _TEST_HASH)

from api.auth import create_token
from api.main import app
from api.routers.celestial import _categorize_feature

client = TestClient(app)


def _auth_header() -> dict[str, str]:
    """Return a valid Authorization header."""
    token = create_token(expires_hours=1)
    return {"Authorization": f"Bearer {token}"}


class TestCategorizeFeature:
    """Test the celestial feature name categorizer."""

    def test_lunar_patterns(self) -> None:
        assert _categorize_feature("lunar_phase") == "lunar"
        assert _categorize_feature("moon_illumination") == "lunar"

    def test_solar_patterns(self) -> None:
        assert _categorize_feature("solar_kp_index") == "solar"
        assert _categorize_feature("solar_flux_f10_7") == "solar"
        assert _categorize_feature("sunspot_number") == "solar"

    def test_vedic_patterns(self) -> None:
        assert _categorize_feature("vedic_nakshatra_index") == "vedic"
        assert _categorize_feature("nakshatra_ruler") == "vedic"

    def test_planetary_patterns(self) -> None:
        assert _categorize_feature("planetary_aspect_score") == "planetary"

    def test_chinese_patterns(self) -> None:
        assert _categorize_feature("chinese_zodiac_element") == "chinese"
        assert _categorize_feature("chinese_year_animal") == "chinese"

    def test_non_celestial(self) -> None:
        assert _categorize_feature("fed_funds_rate") is None
        assert _categorize_feature("sp500_close") is None


class TestCelestialEndpoint:
    """Test the GET /api/v1/signals/celestial endpoint."""

    def test_requires_auth(self) -> None:
        """Endpoint requires authentication."""
        response = client.get("/api/v1/signals/celestial")
        assert response.status_code == 401

    @patch("api.routers.celestial.get_db_engine")
    def test_empty_response_when_no_features(self, mock_engine) -> None:
        """Returns empty but well-shaped response when no celestial features exist."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_engine.return_value.connect.return_value.__enter__ = lambda s: mock_conn
        mock_engine.return_value.connect.return_value.__exit__ = MagicMock(return_value=False)

        response = client.get(
            "/api/v1/signals/celestial",
            headers=_auth_header(),
        )
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 0
        assert data["features"] == []
        assert "categories" in data
        assert "as_of" in data
        # All expected category keys present
        for cat in ["lunar", "solar", "vedic", "planetary", "chinese"]:
            assert cat in data["categories"]

    @patch("api.routers.celestial.get_pit_store")
    @patch("api.routers.celestial.get_db_engine")
    def test_returns_features_when_present(self, mock_engine, mock_pit_store) -> None:
        """Returns categorized features when celestial data exists."""
        # Mock feature_registry query
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [
            (101, "lunar_phase", "Moon phase 0-1"),
            (102, "solar_kp_index", "Planetary K-index"),
        ]
        mock_engine.return_value.connect.return_value.__enter__ = lambda s: mock_conn
        mock_engine.return_value.connect.return_value.__exit__ = MagicMock(return_value=False)

        # Mock PIT store latest values
        mock_pit = MagicMock()
        mock_pit.get_latest_values.return_value = pd.DataFrame({
            "feature_id": [101, 102],
            "obs_date": ["2026-03-24", "2026-03-24"],
            "value": [0.75, 3.5],
            "release_date": ["2026-03-24", "2026-03-24"],
            "vintage_date": ["2026-03-24", "2026-03-24"],
        })
        mock_pit_store.return_value = mock_pit

        response = client.get(
            "/api/v1/signals/celestial",
            headers=_auth_header(),
        )
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
        assert len(data["features"]) == 2

        # Check categorization
        lunar_features = data["categories"].get("lunar", [])
        solar_features = data["categories"].get("solar", [])
        assert len(lunar_features) == 1
        assert len(solar_features) == 1
        assert lunar_features[0]["name"] == "lunar_phase"
        assert solar_features[0]["name"] == "solar_kp_index"

    @patch("api.routers.celestial.get_db_engine")
    def test_handles_db_error_gracefully(self, mock_engine) -> None:
        """Returns empty response with error field on database failure."""
        mock_engine.return_value.connect.side_effect = Exception("DB connection failed")

        response = client.get(
            "/api/v1/signals/celestial",
            headers=_auth_header(),
        )
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 0
        assert "error" in data
