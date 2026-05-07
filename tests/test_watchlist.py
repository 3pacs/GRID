"""
Watchlist API tests.

Tests the watchlist CRUD endpoints using mocked database connections.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")
os.environ.setdefault("GRID_JWT_EXPIRE_HOURS", "1")

from passlib.context import CryptContext

_pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
_TEST_PASSWORD = "testpassword123"
os.environ.setdefault("GRID_MASTER_PASSWORD_HASH", _pwd_ctx.hash(_TEST_PASSWORD))

from api.auth import create_token
from api.main import app

client = TestClient(app)


def _auth_header() -> dict[str, str]:
    """Return a valid Authorization header."""
    token = create_token(expires_hours=1)
    return {"Authorization": f"Bearer {token}"}


class TestWatchlistAuth:
    def test_list_requires_auth(self):
        """GET /api/v1/watchlist without token returns 401."""
        response = client.get("/api/v1/watchlist/")
        assert response.status_code == 401

    def test_add_requires_auth(self):
        """POST /api/v1/watchlist without token returns 401."""
        response = client.post(
            "/api/v1/watchlist/",
            json={"ticker": "AAPL"},
        )
        assert response.status_code == 401

    def test_delete_requires_auth(self):
        """DELETE /api/v1/watchlist/AAPL without token returns 401."""
        response = client.delete("/api/v1/watchlist/AAPL")
        assert response.status_code == 401

    def test_analysis_requires_auth(self):
        """GET /api/v1/watchlist/AAPL/analysis without token returns 401."""
        response = client.get("/api/v1/watchlist/AAPL/analysis")
        assert response.status_code == 401


class TestWatchlistValidation:
    def test_add_invalid_asset_type(self):
        """POST with invalid asset_type returns 422."""
        response = client.post(
            "/api/v1/watchlist/",
            json={"ticker": "AAPL", "asset_type": "invalid"},
            headers=_auth_header(),
        )
        assert response.status_code == 422

    def test_add_empty_ticker(self):
        """POST with empty ticker returns 422."""
        response = client.post(
            "/api/v1/watchlist/",
            json={"ticker": ""},
            headers=_auth_header(),
        )
        assert response.status_code == 422

    def test_add_ticker_too_long(self):
        """POST with ticker > 20 chars returns 422."""
        response = client.post(
            "/api/v1/watchlist/",
            json={"ticker": "A" * 21},
            headers=_auth_header(),
        )
        assert response.status_code == 422


class TestWatchlistCRUD:
    @patch("api.routers.watchlist_core._init_table")
    @patch("api.routers.watchlist_core.get_db_engine")
    def test_list_empty(self, mock_engine, mock_init):
        """GET /api/v1/watchlist returns empty list when no items."""
        mock_conn = MagicMock()
        mock_engine.return_value.connect.return_value.__enter__ = MagicMock(
            return_value=mock_conn
        )
        mock_engine.return_value.connect.return_value.__exit__ = MagicMock(
            return_value=False
        )

        # First call returns rows, second returns count
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_conn.execute.return_value.fetchone.return_value = (0,)

        response = client.get("/api/v1/watchlist/", headers=_auth_header())
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    @patch("api.routers.watchlist_core._init_table")
    @patch("api.routers.watchlist_core.get_db_engine")
    def test_add_ticker(self, mock_engine, mock_init):
        """POST /api/v1/watchlist adds a ticker."""
        mock_conn = MagicMock()
        mock_engine.return_value.begin.return_value.__enter__ = MagicMock(
            return_value=mock_conn
        )
        mock_engine.return_value.begin.return_value.__exit__ = MagicMock(
            return_value=False
        )

        # No existing ticker
        mock_conn.execute.return_value.fetchone.side_effect = [
            None,  # check existing
            (1, "2026-01-01T00:00:00Z"),  # insert returning
        ]

        response = client.post(
            "/api/v1/watchlist/",
            json={"ticker": "aapl", "asset_type": "stock", "notes": "Tech giant"},
            headers=_auth_header(),
        )
        assert response.status_code == 201
        data = response.json()
        assert data["ticker"] == "AAPL"  # uppercased
        assert data["status"] == "added"

    @patch("api.routers.watchlist_core._init_table")
    @patch("api.routers.watchlist_core.get_db_engine")
    def test_add_duplicate_ticker(self, mock_engine, mock_init):
        """POST /api/v1/watchlist with existing ticker returns 409."""
        mock_conn = MagicMock()
        mock_engine.return_value.begin.return_value.__enter__ = MagicMock(
            return_value=mock_conn
        )
        mock_engine.return_value.begin.return_value.__exit__ = MagicMock(
            return_value=False
        )

        # Existing ticker found
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, key: 1
        mock_conn.execute.return_value.fetchone.return_value = mock_row

        response = client.post(
            "/api/v1/watchlist/",
            json={"ticker": "AAPL"},
            headers=_auth_header(),
        )
        assert response.status_code == 409

    @patch("api.routers.watchlist_core._init_table")
    @patch("api.routers.watchlist_core.get_db_engine")
    def test_delete_ticker(self, mock_engine, mock_init):
        """DELETE /api/v1/watchlist/AAPL removes ticker."""
        mock_conn = MagicMock()
        mock_engine.return_value.begin.return_value.__enter__ = MagicMock(
            return_value=mock_conn
        )
        mock_engine.return_value.begin.return_value.__exit__ = MagicMock(
            return_value=False
        )

        mock_conn.execute.return_value.fetchone.return_value = (1,)

        response = client.delete("/api/v1/watchlist/AAPL", headers=_auth_header())
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "removed"
        assert data["ticker"] == "AAPL"

    @patch("api.routers.watchlist_core._init_table")
    @patch("api.routers.watchlist_core.get_db_engine")
    def test_delete_nonexistent_ticker(self, mock_engine, mock_init):
        """DELETE /api/v1/watchlist/XYZ returns 404 if not found."""
        mock_conn = MagicMock()
        mock_engine.return_value.begin.return_value.__enter__ = MagicMock(
            return_value=mock_conn
        )
        mock_engine.return_value.begin.return_value.__exit__ = MagicMock(
            return_value=False
        )

        mock_conn.execute.return_value.fetchone.return_value = None

        response = client.delete("/api/v1/watchlist/XYZ", headers=_auth_header())
        assert response.status_code == 404


class TestWatchlistPriceEndpoints:
    @patch("api.routers.watchlist_core._get_cached_prices")
    @patch("api.routers.watchlist_core._batch_fetch_prices")
    def test_get_prices_returns_cached_snapshot_without_refresh(self, mock_batch_fetch, mock_cached):
        """GET /api/v1/watchlist/prices serves cache only and never refreshes."""
        mock_cached.return_value = {"SPY": {"price": 500.0, "pct_1d": 0.01}}

        response = client.get("/api/v1/watchlist/prices", headers=_auth_header())

        assert response.status_code == 200
        assert response.json() == {
            "prices": {"SPY": {"price": 500.0, "pct_1d": 0.01}},
            "fresh": True,
            "cached": True,
        }
        mock_batch_fetch.assert_not_called()

    @patch("api.main.broadcast_event")
    @patch("api.routers.watchlist_core._batch_fetch_prices")
    @patch("api.routers.watchlist_core.get_db_engine")
    @patch("api.routers.watchlist_core._init_table")
    @patch("api.routers.watchlist_core._get_cached_prices")
    def test_refresh_prices_fetches_and_broadcasts_explicitly(
        self,
        mock_cached,
        mock_init,
        mock_engine,
        mock_batch_fetch,
        mock_broadcast,
    ):
        """POST /api/v1/watchlist/refresh-prices performs the expensive refresh path."""
        mock_cached.return_value = None
        mock_batch_fetch.return_value = {"SPY": {"price": 501.0, "pct_1d": 0.02}}

        mock_conn = MagicMock()
        mock_engine.return_value.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.return_value.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.execute.return_value.fetchall.return_value = [("SPY",), ("QQQ",)]

        response = client.post("/api/v1/watchlist/refresh-prices", headers=_auth_header())

        assert response.status_code == 200
        assert response.json() == {
            "prices": {"SPY": {"price": 501.0, "pct_1d": 0.02}},
            "cached": False,
        }
        mock_batch_fetch.assert_called_once_with(["SPY", "QQQ"])
        mock_broadcast.assert_called_once_with(
            "prices",
            {"SPY": {"price": 501.0, "pct_1d": 0.02}},
        )
