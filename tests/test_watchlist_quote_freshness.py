"""
Tests for GET /api/v1/watchlist/{ticker}/quote's freshness fields.

Regression coverage for the frozen-price incident: the endpoint always
returned change_pct=null for GRID-sourced prices (only the live-fallback
path ever set it) and exposed no way for the ticker_pulse card to know a
price was stale. Both are fixed by fetching the two most recent closes so
change_pct comes from the actual prior session, and by adding a `stale`
flag (as_of older than 3 calendar days).

Also guards a review-caught bug in that same fix: a plain
`ORDER BY obs_date DESC LIMIT 2` can return two *vintages* of the same
day (uq_resolved_series_composite is (feature_id, obs_date, vintage_date),
and a puller with a lookback window re-inserts yesterday's close under a
new vintage_date on every run), turning change_pct into a same-day delta
instead of a day-over-day one. Postgres's DISTINCT ON isn't exercisable
against a mocked connection, so the guard below asserts the query shape
itself rather than re-deriving Postgres semantics in Python.
"""

from __future__ import annotations

import inspect
import os
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")
os.environ.setdefault("GRID_JWT_EXPIRE_HOURS", "1")

from passlib.context import CryptContext

_pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
os.environ.setdefault("GRID_MASTER_PASSWORD_HASH", _pwd_ctx.hash("testpassword123"))

from api.auth import create_token
from api.main import app
from api.routers.watchlist_overview import get_ticker_quote

client = TestClient(app)


def _auth_header() -> dict[str, str]:
    token = create_token(expires_hours=1)
    return {"Authorization": f"Bearer {token}"}


def _mock_quote_conn(price_rows, opt_row=None):
    """Mock connection: `price_rows` (freshest first) answers the resolved_series
    fetchall(), `opt_row` answers the options_daily_signals fetchone()."""
    mock_conn = MagicMock()
    price_result = MagicMock()
    price_result.fetchall.return_value = price_rows
    opt_result = MagicMock()
    opt_result.fetchone.return_value = opt_row
    mock_conn.execute.side_effect = [price_result, opt_result]
    return mock_conn


def _wire_engine(mock_engine, mock_conn):
    mock_engine.return_value.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.return_value.connect.return_value.__exit__ = MagicMock(return_value=False)


class TestQuoteChangePct:
    @patch("api.routers.watchlist_overview._init_table")
    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_change_pct_computed_from_prior_close(self, mock_engine, _mock_init):
        today = date.today()
        yesterday = today - timedelta(days=1)
        _wire_engine(mock_engine, _mock_quote_conn([(110.0, today), (100.0, yesterday)]))

        response = client.get("/api/v1/watchlist/AAPL/quote", headers=_auth_header())

        assert response.status_code == 200
        data = response.json()
        assert data["price"] == 110.0
        assert data["change_pct"] == 0.1
        assert data["source"] == "grid"

    @patch("api.routers.watchlist_overview._init_table")
    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_change_pct_null_with_only_one_stored_close(self, mock_engine, _mock_init):
        """No prior day on record -> change_pct stays null rather than crashing."""
        today = date.today()
        _wire_engine(mock_engine, _mock_quote_conn([(100.0, today)]))

        response = client.get("/api/v1/watchlist/AAPL/quote", headers=_auth_header())

        assert response.status_code == 200
        data = response.json()
        assert data["price"] == 100.0
        assert data["change_pct"] is None

    @patch("api.routers.watchlist_overview._init_table")
    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_change_pct_none_when_no_price_history(self, mock_engine, _mock_init):
        _wire_engine(mock_engine, _mock_quote_conn([]))

        response = client.get("/api/v1/watchlist/AAPL/quote", headers=_auth_header())

        assert response.status_code == 200
        assert response.json()["price"] is None
        assert response.json()["change_pct"] is None


class TestQuoteStaleFlag:
    @patch("api.routers.watchlist_overview._init_table")
    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_fresh_price_is_not_stale(self, mock_engine, _mock_init):
        today = date.today()
        _wire_engine(mock_engine, _mock_quote_conn([(110.0, today)]))

        response = client.get("/api/v1/watchlist/AAPL/quote", headers=_auth_header())

        assert response.json()["stale"] is False

    @patch("api.routers.watchlist_overview._init_table")
    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_price_older_than_three_days_is_stale(self, mock_engine, _mock_init):
        old_date = date.today() - timedelta(days=57)
        older_date = old_date - timedelta(days=1)
        _wire_engine(mock_engine, _mock_quote_conn([(327.50, old_date), (320.0, older_date)]))

        response = client.get("/api/v1/watchlist/AAPL/quote", headers=_auth_header())

        data = response.json()
        assert data["as_of"] == str(old_date)
        assert data["stale"] is True
        # A stale price still carries whatever last real day-over-day move
        # is on record — the card can show both facts together.
        assert data["change_pct"] is not None

    @patch("api.routers.watchlist_overview._init_table")
    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_price_exactly_three_days_old_is_not_stale(self, mock_engine, _mock_init):
        boundary_date = date.today() - timedelta(days=3)
        _wire_engine(mock_engine, _mock_quote_conn([(100.0, boundary_date)]))

        response = client.get("/api/v1/watchlist/AAPL/quote", headers=_auth_header())

        assert response.json()["stale"] is False

    @patch("api.routers.watchlist_overview._init_table")
    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_no_price_at_all_leaves_stale_none(self, mock_engine, _mock_init):
        """No as_of at all (not even a live fallback hit) -> stale is None,
        not a false claim of freshness or staleness."""
        _wire_engine(mock_engine, _mock_quote_conn([]))
        with patch(
            "api.routers.watchlist_overview._fetch_live_price", return_value=None
        ):
            response = client.get("/api/v1/watchlist/ZZZ/quote", headers=_auth_header())

        data = response.json()
        assert data["as_of"] is None
        assert data["stale"] is None


class TestQuoteLiveFallback:
    @patch("api.routers.watchlist_overview._init_table")
    @patch("api.routers.watchlist_overview.get_db_engine")
    @patch("api.routers.watchlist_overview._cache_price_to_db")
    def test_live_fallback_carries_honest_as_of_and_stale(
        self, _mock_cache, mock_engine, _mock_init
    ):
        """Previously as_of stayed null on the live-fallback path even
        though a fresh live price was returned."""
        _wire_engine(mock_engine, _mock_quote_conn([]))
        with patch(
            "api.routers.watchlist_overview._fetch_live_price",
            return_value={"price": 42.0, "pct_1d": 0.01},
        ):
            response = client.get("/api/v1/watchlist/ZZZ/quote", headers=_auth_header())

        data = response.json()
        assert data["source"] == "live"
        assert data["price"] == 42.0
        assert data["change_pct"] == 0.01
        assert data["as_of"] == str(date.today())
        assert data["stale"] is False


class TestQuoteQueryCollapsesVintages:
    """Regression guard for the review finding on this same fix: a naive
    `ORDER BY obs_date DESC LIMIT 2` can return two vintages of the same
    calendar day (uq_resolved_series_composite includes vintage_date), so
    the query must collapse to one row per obs_date and pin to a single
    feature before taking the two most recent days. DISTINCT ON is
    Postgres-only and not exercisable against a mocked connection, so this
    asserts the query text carries the fix rather than re-deriving
    Postgres semantics in Python.
    """

    def test_price_query_pins_one_feature_and_collapses_vintages(self):
        src = inspect.getsource(get_ticker_quote)

        assert "DISTINCT ON (rs.obs_date)" in src
        assert "vintage_date DESC" in src
        assert "WITH winner AS" in src
        # The naive, buggy shape must not reappear.
        assert "ORDER BY rs.obs_date DESC LIMIT 2" not in src
