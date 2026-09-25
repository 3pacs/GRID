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
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from loguru import logger as loguru_logger

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")
os.environ.setdefault("GRID_JWT_EXPIRE_HOURS", "1")

from passlib.context import CryptContext

_pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
os.environ.setdefault("GRID_MASTER_PASSWORD_HASH", _pwd_ctx.hash("testpassword123"))

from api.auth import create_token
from api.main import app
from api.routers.watchlist_overview import get_ticker_quote


class _FakeOrig(Exception):
    """Stand-in for a psycopg2 error carrying a SQLSTATE."""

    def __init__(self, pgcode: str, message: str = "") -> None:
        super().__init__(message)
        self.pgcode = pgcode


class _FakeDBAPIError(Exception):
    """Stand-in for a SQLAlchemy DBAPIError wrapping a driver error."""

    def __init__(self, message: str, orig: Exception | None = None) -> None:
        super().__init__(message)
        self.orig = orig

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
    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_change_pct_computed_from_prior_close(self, mock_engine):
        today = date.today()
        yesterday = today - timedelta(days=1)
        _wire_engine(mock_engine, _mock_quote_conn([(110.0, today), (100.0, yesterday)]))

        response = client.get("/api/v1/watchlist/AAPL/quote", headers=_auth_header())

        assert response.status_code == 200
        data = response.json()
        assert data["price"] == 110.0
        assert data["change_pct"] == 0.1
        assert data["source"] == "grid"

    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_change_pct_null_with_only_one_stored_close(self, mock_engine):
        """No prior day on record -> change_pct stays null rather than crashing."""
        today = date.today()
        _wire_engine(mock_engine, _mock_quote_conn([(100.0, today)]))

        response = client.get("/api/v1/watchlist/AAPL/quote", headers=_auth_header())

        assert response.status_code == 200
        data = response.json()
        assert data["price"] == 100.0
        assert data["change_pct"] is None

    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_change_pct_none_when_no_price_history(self, mock_engine):
        _wire_engine(mock_engine, _mock_quote_conn([]))

        # No stored price falls through to the live-fetch fallback; force it
        # to also come back empty so this test doesn't depend on whether the
        # runner has real network access to Yahoo Finance.
        with patch(
            "api.routers.watchlist_overview._fetch_live_price", return_value=None
        ):
            response = client.get("/api/v1/watchlist/AAPL/quote", headers=_auth_header())

        assert response.status_code == 200
        assert response.json()["price"] is None
        assert response.json()["change_pct"] is None


class TestQuoteStaleFlag:
    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_fresh_price_is_not_stale(self, mock_engine):
        today = date.today()
        _wire_engine(mock_engine, _mock_quote_conn([(110.0, today)]))

        response = client.get("/api/v1/watchlist/AAPL/quote", headers=_auth_header())

        assert response.json()["stale"] is False

    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_price_older_than_three_days_is_stale(self, mock_engine):
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

    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_price_exactly_three_days_old_is_not_stale(self, mock_engine):
        boundary_date = date.today() - timedelta(days=3)
        _wire_engine(mock_engine, _mock_quote_conn([(100.0, boundary_date)]))

        response = client.get("/api/v1/watchlist/AAPL/quote", headers=_auth_header())

        assert response.json()["stale"] is False

    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_no_price_at_all_leaves_stale_none(self, mock_engine):
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
    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_live_fallback_carries_honest_as_of_and_stale(
        self, mock_engine
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


class TestSpyIntradayQuote:
    @staticmethod
    def _wire_spy(mock_engine, candle):
        mock_conn = MagicMock()
        prices = MagicMock()
        # Daily fixtures must follow the bar's date, even just after midnight
        # when a recent delayed bar still belongs to the previous UTC day.
        bar_date = (candle[1] + timedelta(minutes=5)).date()
        prices.fetchall.return_value = [
            (104.0, bar_date),
            (100.0, bar_date - timedelta(days=1)),
        ]
        options = MagicMock()
        options.fetchone.return_value = None
        realtime = MagicMock()
        realtime.fetchone.return_value = candle
        mock_conn.execute.side_effect = [prices, options, realtime]
        _wire_engine(mock_engine, mock_conn)
        return mock_conn

    @pytest.mark.parametrize("now", [
        datetime(2026, 9, 24, 18, 0, tzinfo=timezone.utc),
        datetime(2026, 9, 25, 0, 10, tzinfo=timezone.utc),
    ], ids=["daytime", "utc-midnight"])
    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_recent_delayed_bar_beats_daily_price_with_its_own_timestamp(self, mock_engine, now):
        bucket = now - timedelta(minutes=20)
        conn = self._wire_spy(mock_engine, (105.0, bucket))

        with patch("api.routers.watchlist_overview.datetime", wraps=datetime) as clock:
            clock.now.return_value = now
            response = client.get("/api/v1/watchlist/SPY/quote", headers=_auth_header())

        assert response.status_code == 200
        data = response.json()
        assert data["price"] == 105.0
        assert data["change_pct"] == 0.05
        assert data["source"] == "yahoo_intraday"
        assert data["price_tier"] == "intraday_delayed"
        assert data["price_bar_end_at"] == (bucket + timedelta(minutes=5)).isoformat()
        assert "realtime_candles" in str(conn.execute.call_args_list[2].args[0])

    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_old_bar_keeps_daily_price(self, mock_engine):
        bucket = datetime.now(timezone.utc) - timedelta(minutes=50)
        self._wire_spy(mock_engine, (105.0, bucket))

        response = client.get("/api/v1/watchlist/SPY/quote", headers=_auth_header())

        assert response.status_code == 200
        data = response.json()
        assert data["price"] == 104.0
        assert data["source"] == "grid"
        assert data["price_tier"] == "daily"
        assert data["price_bar_end_at"] is None


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


class TestQuoteQueryFailureIsAudible:
    """The other half of the frozen-price incident: when the price query
    itself failed, the handler logged at `log.debug` and the endpoint quietly
    answered from the live-fetch fallback. Debug level is off in production,
    so `.server-logs/errors.jsonl` — the canonical operational health signal
    per CLAUDE.md — recorded nothing at all, and a dead query looked exactly
    like a healthy one. Route it the way `intelligence/entity_resolver.py`
    does: a schema fault is an application bug (`log.error`), anything else
    is operational (`log.warning`). Neither is `log.debug` — that is the bug
    class PRs #477 and #479 spent two rounds removing.
    """

    @staticmethod
    def _records():
        """Capture (level, message) pairs; loguru does not use stdlib logging."""
        records: list[tuple[str, str]] = []
        sink_id = loguru_logger.add(
            lambda msg: records.append(
                (msg.record["level"].name, msg.record["message"])
            ),
            level="WARNING",
        )
        return records, sink_id

    def _quote_with_price_query_raising(self, mock_engine, exc):
        """Drive /quote with a price query that raises `exc`, return log records."""
        mock_conn = MagicMock()
        opt_result = MagicMock()
        opt_result.fetchone.return_value = None
        mock_conn.execute.side_effect = [exc, opt_result]
        _wire_engine(mock_engine, mock_conn)

        records, sink_id = self._records()
        try:
            with patch(
                "api.routers.watchlist_overview._fetch_live_price", return_value=None
            ):
                response = client.get(
                    "/api/v1/watchlist/AAPL/quote", headers=_auth_header()
                )
        finally:
            loguru_logger.remove(sink_id)
        assert response.status_code == 200
        return records

    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_schema_fault_in_price_query_logs_at_error(self, mock_engine):
        """SQLSTATE 42703 can't succeed on any retry — it is a code bug."""
        records = self._quote_with_price_query_raising(
            mock_engine, _FakeDBAPIError("boom", _FakeOrig("42703"))
        )

        levels = [level for level, _ in records]
        assert "ERROR" in levels, records
        assert any("Quote price query for AAPL" in msg for _, msg in records), records

    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_undefined_column_message_logs_at_error(self, mock_engine):
        """Same fault via the message, for drivers that expose no SQLSTATE."""
        records = self._quote_with_price_query_raising(
            mock_engine, _FakeDBAPIError('column "obs_date" does not exist')
        )

        assert "ERROR" in [level for level, _ in records], records

    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_transient_failure_stays_a_warning(self, mock_engine):
        """A statement timeout is operational — errors.jsonl keeps its signal."""
        records = self._quote_with_price_query_raising(
            mock_engine,
            _FakeDBAPIError("canceling statement due to statement timeout"),
        )

        levels = [level for level, _ in records]
        assert "WARNING" in levels, records
        assert "ERROR" not in levels, records

    def test_no_query_failure_is_swallowed_at_debug(self):
        """Guard the whole endpoint, not just the one arm the test drives."""
        src = inspect.getsource(get_ticker_quote)
        assert "log.debug" not in src, (
            "a failed query in /quote must not be invisible in production"
        )
