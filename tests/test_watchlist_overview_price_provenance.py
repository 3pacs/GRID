"""
Tests for GET /api/v1/watchlist/{ticker}/overview's price provenance fields.

#F1 D6: /overview already computed the price's own date and source
internally (`price_info["date"]` / `price_info["source"]`) but dropped both
from the response. The only timestamp the UI got was `generated_at` — the
AI narrative's clock, not the price's — which reads like price currency
without being one. `price_as_of` and `price_source` close that gap.

Also covers the live-fallback half of the same defect (#F1 D5): the
fallback branch used to build `price_info` with no date at all, so a
live-fetched price had no provenance to report even once this fix landed.
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")
os.environ.setdefault("GRID_JWT_EXPIRE_HOURS", "1")

from passlib.context import CryptContext

_pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
os.environ.setdefault("GRID_MASTER_PASSWORD_HASH", _pwd_ctx.hash("testpassword123"))

from fastapi.testclient import TestClient

from api.auth import create_token
from api.main import app

client = TestClient(app)


def _auth_header() -> dict[str, str]:
    token = create_token(expires_hours=1)
    return {"Authorization": f"Bearer {token}"}


def _mock_overview_conn(price_row=None, opt_row=None, regime_row=None, feat_rows=None):
    """Mock connection answering get_ticker_overview's four sequential
    reads in order: price, options, regime, related-features."""
    mock_conn = MagicMock()
    price_result = MagicMock()
    price_result.fetchone.return_value = price_row
    opt_result = MagicMock()
    opt_result.fetchone.return_value = opt_row
    regime_result = MagicMock()
    regime_result.fetchone.return_value = regime_row
    feat_result = MagicMock()
    feat_result.fetchall.return_value = feat_rows or []
    mock_conn.execute.side_effect = [price_result, opt_result, regime_result, feat_result]
    return mock_conn


def _wire_engine(mock_engine, mock_conn):
    mock_engine.return_value.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.return_value.connect.return_value.__exit__ = MagicMock(return_value=False)


def _no_llm():
    """Two context managers that keep the overview on its rule-based path."""
    unavailable = MagicMock(is_available=False)
    return (
        patch("llm.router.get_llm", return_value=unavailable),
        patch("ollama.client.get_client", return_value=unavailable),
    )


class TestOverviewPriceProvenance:
    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_grid_sourced_price_reports_its_own_date_and_source(self, mock_engine):
        today = date.today()
        _wire_engine(mock_engine, _mock_overview_conn(price_row=(100.0, today)))

        p1, p2 = _no_llm()
        with p1, p2:
            response = client.get("/api/v1/watchlist/AAPL/overview", headers=_auth_header())

        assert response.status_code == 200
        data = response.json()
        assert data["price_as_of"] == str(today)
        assert data["price_source"] == "grid"

    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_live_fallback_price_reports_its_real_date(self, mock_engine):
        """Previously the live-fallback branch of /overview set no date at
        all for the price, leaving `generated_at` (the LLM narrative's
        clock) as the response's only timestamp."""
        _wire_engine(mock_engine, _mock_overview_conn(price_row=None))
        real_price_date = date.today() - timedelta(days=1)

        p1, p2 = _no_llm()
        with p1, p2, patch(
            "api.routers.watchlist_overview._fetch_live_price",
            return_value={"price": 123.0, "pct_1d": 0.02, "as_of": real_price_date},
        ):
            response = client.get("/api/v1/watchlist/ZZZ/overview", headers=_auth_header())

        assert response.status_code == 200
        data = response.json()
        assert data["price_as_of"] == str(real_price_date)
        assert data["price_source"] == "live"

    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_live_fallback_with_unresolvable_date_reports_none_not_a_fabricated_date(
        self, mock_engine
    ):
        _wire_engine(mock_engine, _mock_overview_conn(price_row=None))

        p1, p2 = _no_llm()
        with p1, p2, patch(
            "api.routers.watchlist_overview._fetch_live_price",
            return_value={"price": 123.0, "pct_1d": 0.02, "as_of": None},
        ):
            response = client.get("/api/v1/watchlist/ZZZ/overview", headers=_auth_header())

        assert response.status_code == 200
        data = response.json()
        assert data["price_source"] == "live"
        assert data["price_as_of"] is None

    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_no_price_at_all_reports_no_provenance(self, mock_engine):
        _wire_engine(mock_engine, _mock_overview_conn(price_row=None))

        p1, p2 = _no_llm()
        with p1, p2, patch(
            "api.routers.watchlist_overview._fetch_live_price", return_value=None
        ):
            response = client.get("/api/v1/watchlist/ZZZ/overview", headers=_auth_header())

        assert response.status_code == 200
        data = response.json()
        assert data["price_as_of"] is None
        assert data["price_source"] is None
