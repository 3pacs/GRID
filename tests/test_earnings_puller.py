"""Tests for the earnings data puller (ingestion/altdata/earnings_puller.py).

Covers:
  - Helper functions: _safe_float, compute_surprise_pct, classify_beat_miss
  - EarningsPuller methods with mocked yfinance and database
  - Edge cases: empty DataFrames, NaN values, zero estimates
  - Significant surprise detection
  - Rate limiting and error handling
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, PropertyMock, patch

import pandas as pd
import pytest


# ── Helper Functions ────────────────────────────────────────────────────────


class TestSafeFloat:
    """Tests for _safe_float conversion helper."""

    def test_normal_float(self):
        from ingestion.altdata.earnings_puller import _safe_float
        assert _safe_float(3.14) == 3.14

    def test_int_to_float(self):
        from ingestion.altdata.earnings_puller import _safe_float
        assert _safe_float(42) == 42.0

    def test_string_number(self):
        from ingestion.altdata.earnings_puller import _safe_float
        assert _safe_float("2.5") == 2.5

    def test_zero(self):
        from ingestion.altdata.earnings_puller import _safe_float
        assert _safe_float(0) == 0.0

    def test_none_returns_none(self):
        from ingestion.altdata.earnings_puller import _safe_float
        assert _safe_float(None) is None

    def test_nan_returns_none(self):
        from ingestion.altdata.earnings_puller import _safe_float
        assert _safe_float(float("nan")) is None

    def test_inf_returns_none(self):
        from ingestion.altdata.earnings_puller import _safe_float
        assert _safe_float(float("inf")) is None

    def test_neg_inf_returns_none(self):
        from ingestion.altdata.earnings_puller import _safe_float
        assert _safe_float(float("-inf")) is None

    def test_non_numeric_string_returns_none(self):
        from ingestion.altdata.earnings_puller import _safe_float
        assert _safe_float("not_a_number") is None

    def test_negative_float(self):
        from ingestion.altdata.earnings_puller import _safe_float
        assert _safe_float(-1.5) == -1.5


class TestComputeSurprisePct:
    """Tests for compute_surprise_pct calculation."""

    def test_positive_surprise(self):
        from ingestion.altdata.earnings_puller import compute_surprise_pct
        result = compute_surprise_pct(1.10, 1.00)
        assert result == pytest.approx(10.0)

    def test_negative_surprise(self):
        from ingestion.altdata.earnings_puller import compute_surprise_pct
        result = compute_surprise_pct(0.90, 1.00)
        assert result == pytest.approx(-10.0)

    def test_no_surprise(self):
        from ingestion.altdata.earnings_puller import compute_surprise_pct
        result = compute_surprise_pct(1.00, 1.00)
        assert result == pytest.approx(0.0)

    def test_none_actual(self):
        from ingestion.altdata.earnings_puller import compute_surprise_pct
        assert compute_surprise_pct(None, 1.00) is None

    def test_none_estimate(self):
        from ingestion.altdata.earnings_puller import compute_surprise_pct
        assert compute_surprise_pct(1.00, None) is None

    def test_both_none(self):
        from ingestion.altdata.earnings_puller import compute_surprise_pct
        assert compute_surprise_pct(None, None) is None

    def test_zero_estimate_returns_none(self):
        from ingestion.altdata.earnings_puller import compute_surprise_pct
        assert compute_surprise_pct(1.00, 0) is None

    def test_negative_estimate(self):
        from ingestion.altdata.earnings_puller import compute_surprise_pct
        # actual=-0.50, estimate=-1.00 => (-0.50 - (-1.00)) / abs(-1.00) * 100 = 50%
        result = compute_surprise_pct(-0.50, -1.00)
        assert result == pytest.approx(50.0)

    def test_large_surprise(self):
        from ingestion.altdata.earnings_puller import compute_surprise_pct
        result = compute_surprise_pct(2.00, 1.00)
        assert result == pytest.approx(100.0)


class TestClassifyBeatMiss:
    """Tests for classify_beat_miss classification."""

    def test_significant_beat(self):
        from ingestion.altdata.earnings_puller import classify_beat_miss
        assert classify_beat_miss(15.0) == "significant_beat"

    def test_significant_miss(self):
        from ingestion.altdata.earnings_puller import classify_beat_miss
        assert classify_beat_miss(-15.0) == "significant_miss"

    def test_normal_beat(self):
        from ingestion.altdata.earnings_puller import classify_beat_miss
        assert classify_beat_miss(5.0) == "beat"

    def test_normal_miss(self):
        from ingestion.altdata.earnings_puller import classify_beat_miss
        assert classify_beat_miss(-5.0) == "miss"

    def test_inline(self):
        from ingestion.altdata.earnings_puller import classify_beat_miss
        assert classify_beat_miss(0.0) == "inline"

    def test_none_returns_unknown(self):
        from ingestion.altdata.earnings_puller import classify_beat_miss
        assert classify_beat_miss(None) == "unknown"

    def test_threshold_boundary_beat(self):
        from ingestion.altdata.earnings_puller import classify_beat_miss
        # Exactly at 10% should be beat, not significant_beat
        assert classify_beat_miss(10.0) == "beat"

    def test_threshold_boundary_miss(self):
        from ingestion.altdata.earnings_puller import classify_beat_miss
        assert classify_beat_miss(-10.0) == "miss"

    def test_just_over_threshold(self):
        from ingestion.altdata.earnings_puller import classify_beat_miss
        assert classify_beat_miss(10.01) == "significant_beat"
        assert classify_beat_miss(-10.01) == "significant_miss"


# ── EarningsPuller Integration Tests (mocked DB + yfinance) ────────────────


def _make_mock_engine():
    """Create a mock SQLAlchemy engine with basic source_catalog behaviour."""
    engine = MagicMock()
    mock_conn = MagicMock()

    # source_catalog lookup returns id=1
    mock_result = MagicMock()
    mock_result.fetchone.return_value = (1,)
    mock_conn.execute.return_value = mock_result

    engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    return engine, mock_conn


def _make_earnings_dates_df() -> pd.DataFrame:
    """Create a sample earnings_dates DataFrame matching yfinance format."""
    dates = pd.to_datetime(["2025-01-29", "2024-10-31", "2024-07-25"])
    return pd.DataFrame(
        {
            "EPS Estimate": [2.35, 1.65, 1.40],
            "Reported EPS": [2.58, 1.64, 1.53],
            "Surprise(%)": [9.79, -0.61, 9.29],
        },
        index=dates,
    )


def _make_quarterly_earnings_df() -> pd.DataFrame:
    """Create a sample quarterly_earnings DataFrame."""
    dates = pd.to_datetime(["2024-12-31", "2024-09-30", "2024-06-30"])
    return pd.DataFrame(
        {
            "Revenue": [124000000000, 94900000000, 85800000000],
            "Earnings": [36300000000, 25000000000, 21400000000],
        },
        index=dates,
    )


def _make_earnings_history_df() -> pd.DataFrame:
    """Create a sample earnings_history DataFrame."""
    dates = pd.to_datetime(["2024-12-31", "2024-09-30"])
    return pd.DataFrame(
        {
            "epsEstimate": [2.35, 1.65],
            "epsActual": [2.58, 1.64],
            "surprisePercent": [9.79, -0.61],
        },
        index=dates,
    )


class TestEarningsPullerInit:
    """Tests for EarningsPuller initialization."""

    def test_init_resolves_source_id(self):
        engine, _ = _make_mock_engine()
        from ingestion.altdata.earnings_puller import EarningsPuller
        puller = EarningsPuller(db_engine=engine)
        assert puller.source_id == 1

    def test_source_name(self):
        engine, _ = _make_mock_engine()
        from ingestion.altdata.earnings_puller import EarningsPuller
        puller = EarningsPuller(db_engine=engine)
        assert puller.SOURCE_NAME == "yfinance_earnings"


class TestProcessEarningsDates:
    """Tests for _process_earnings_dates with mocked yfinance."""

    def test_inserts_eps_data(self):
        engine, mock_conn = _make_mock_engine()
        from ingestion.altdata.earnings_puller import EarningsPuller

        puller = EarningsPuller(db_engine=engine)

        stock = MagicMock()
        stock.earnings_dates = _make_earnings_dates_df()

        # Mock _get_existing_dates to return empty sets (no dedup hits)
        puller._get_existing_dates = MagicMock(return_value=set())

        inserted = puller._process_earnings_dates(mock_conn, "AAPL", stock)
        # 3 rows x 3 fields (eps_estimate, eps_actual, surprise_pct) = 9
        # No beat_flags because none exceed 10%
        assert inserted == 9

    def test_skips_existing_dates(self):
        engine, mock_conn = _make_mock_engine()
        from ingestion.altdata.earnings_puller import EarningsPuller

        puller = EarningsPuller(db_engine=engine)

        stock = MagicMock()
        stock.earnings_dates = _make_earnings_dates_df()

        # All dates already exist
        existing = {date(2025, 1, 29), date(2024, 10, 31), date(2024, 7, 25)}
        puller._get_existing_dates = MagicMock(return_value=existing)

        inserted = puller._process_earnings_dates(mock_conn, "AAPL", stock)
        assert inserted == 0

    def test_handles_empty_dataframe(self):
        engine, mock_conn = _make_mock_engine()
        from ingestion.altdata.earnings_puller import EarningsPuller

        puller = EarningsPuller(db_engine=engine)
        stock = MagicMock()
        stock.earnings_dates = pd.DataFrame()

        inserted = puller._process_earnings_dates(mock_conn, "AAPL", stock)
        assert inserted == 0

    def test_handles_none_earnings_dates(self):
        engine, mock_conn = _make_mock_engine()
        from ingestion.altdata.earnings_puller import EarningsPuller

        puller = EarningsPuller(db_engine=engine)
        stock = MagicMock()
        stock.earnings_dates = None

        inserted = puller._process_earnings_dates(mock_conn, "AAPL", stock)
        assert inserted == 0

    def test_handles_exception_on_access(self):
        engine, mock_conn = _make_mock_engine()
        from ingestion.altdata.earnings_puller import EarningsPuller

        puller = EarningsPuller(db_engine=engine)
        stock = MagicMock()
        type(stock).earnings_dates = PropertyMock(side_effect=Exception("API error"))

        inserted = puller._process_earnings_dates(mock_conn, "AAPL", stock)
        assert inserted == 0

    def test_significant_surprise_creates_beat_flag(self):
        engine, mock_conn = _make_mock_engine()
        from ingestion.altdata.earnings_puller import EarningsPuller

        puller = EarningsPuller(db_engine=engine)

        # Create DataFrame with >10% surprise
        dates = pd.to_datetime(["2025-01-29"])
        df = pd.DataFrame(
            {
                "EPS Estimate": [1.00],
                "Reported EPS": [1.50],
                "Surprise(%)": [50.0],
            },
            index=dates,
        )
        stock = MagicMock()
        stock.earnings_dates = df

        puller._get_existing_dates = MagicMock(return_value=set())

        inserted = puller._process_earnings_dates(mock_conn, "AAPL", stock)
        # eps_estimate + eps_actual + surprise_pct + beat_flag = 4
        assert inserted == 4


class TestProcessQuarterlyEarnings:
    """Tests for _process_quarterly_earnings."""

    def test_inserts_revenue_and_earnings(self):
        engine, mock_conn = _make_mock_engine()
        from ingestion.altdata.earnings_puller import EarningsPuller

        puller = EarningsPuller(db_engine=engine)

        stock = MagicMock()
        stock.quarterly_earnings = _make_quarterly_earnings_df()

        puller._get_existing_dates = MagicMock(return_value=set())

        inserted = puller._process_quarterly_earnings(mock_conn, "AAPL", stock)
        # 3 rows x 2 fields (revenue_actual + quarterly_earnings) = 6
        assert inserted == 6

    def test_handles_empty_dataframe(self):
        engine, mock_conn = _make_mock_engine()
        from ingestion.altdata.earnings_puller import EarningsPuller

        puller = EarningsPuller(db_engine=engine)
        stock = MagicMock()
        stock.quarterly_earnings = pd.DataFrame()

        inserted = puller._process_quarterly_earnings(mock_conn, "AAPL", stock)
        assert inserted == 0

    def test_handles_none(self):
        engine, mock_conn = _make_mock_engine()
        from ingestion.altdata.earnings_puller import EarningsPuller

        puller = EarningsPuller(db_engine=engine)
        stock = MagicMock()
        stock.quarterly_earnings = None

        inserted = puller._process_quarterly_earnings(mock_conn, "AAPL", stock)
        assert inserted == 0


class TestProcessEarningsHistory:
    """Tests for _process_earnings_history."""

    def test_inserts_history_surprise(self):
        engine, mock_conn = _make_mock_engine()
        from ingestion.altdata.earnings_puller import EarningsPuller

        puller = EarningsPuller(db_engine=engine)

        stock = MagicMock()
        stock.earnings_history = _make_earnings_history_df()

        puller._get_existing_dates = MagicMock(return_value=set())

        inserted = puller._process_earnings_history(mock_conn, "AAPL", stock)
        # 2 rows with surprise data
        assert inserted == 2

    def test_handles_empty_history(self):
        engine, mock_conn = _make_mock_engine()
        from ingestion.altdata.earnings_puller import EarningsPuller

        puller = EarningsPuller(db_engine=engine)
        stock = MagicMock()
        stock.earnings_history = pd.DataFrame()

        inserted = puller._process_earnings_history(mock_conn, "AAPL", stock)
        assert inserted == 0


class TestPullTicker:
    """Tests for pull_ticker end-to-end (mocked)."""

    @patch("ingestion.altdata.earnings_puller.yf.Ticker")
    def test_successful_pull(self, mock_ticker_cls):
        engine, mock_conn = _make_mock_engine()
        from ingestion.altdata.earnings_puller import EarningsPuller

        puller = EarningsPuller(db_engine=engine)

        mock_stock = MagicMock()
        mock_stock.earnings_dates = _make_earnings_dates_df()
        mock_stock.quarterly_earnings = _make_quarterly_earnings_df()
        mock_stock.earnings_history = _make_earnings_history_df()
        mock_ticker_cls.return_value = mock_stock

        puller._get_existing_dates = MagicMock(return_value=set())

        result = puller.pull_ticker("AAPL")
        assert result["ticker"] == "AAPL"
        assert result["status"] == "SUCCESS"
        assert result["rows_inserted"] > 0

    @patch("ingestion.altdata.earnings_puller.yf.Ticker")
    def test_failed_pull(self, mock_ticker_cls):
        engine, _ = _make_mock_engine()
        from ingestion.altdata.earnings_puller import EarningsPuller

        puller = EarningsPuller(db_engine=engine)

        mock_ticker_cls.side_effect = ConnectionError("Network error")

        result = puller.pull_ticker("AAPL")
        assert result["status"] == "FAILED"
        assert len(result["errors"]) > 0

    @patch("ingestion.altdata.earnings_puller.yf.Ticker")
    def test_partial_when_no_data(self, mock_ticker_cls):
        engine, mock_conn = _make_mock_engine()
        from ingestion.altdata.earnings_puller import EarningsPuller

        puller = EarningsPuller(db_engine=engine)

        mock_stock = MagicMock()
        mock_stock.earnings_dates = pd.DataFrame()
        mock_stock.quarterly_earnings = pd.DataFrame()
        mock_stock.earnings_history = pd.DataFrame()
        mock_ticker_cls.return_value = mock_stock

        result = puller.pull_ticker("UNKNOWN")
        assert result["status"] == "PARTIAL"


class TestPullAll:
    """Tests for pull_all orchestration."""

    @patch("ingestion.altdata.earnings_puller.time.sleep")
    @patch("ingestion.altdata.earnings_puller.yf.Ticker")
    def test_pull_all_with_rate_limit(self, mock_ticker_cls, mock_sleep):
        engine, mock_conn = _make_mock_engine()
        from ingestion.altdata.earnings_puller import EarningsPuller

        puller = EarningsPuller(db_engine=engine)

        mock_stock = MagicMock()
        mock_stock.earnings_dates = pd.DataFrame()
        mock_stock.quarterly_earnings = pd.DataFrame()
        mock_stock.earnings_history = pd.DataFrame()
        mock_ticker_cls.return_value = mock_stock

        results = puller.pull_all(ticker_list=["AAPL", "MSFT", "GOOGL"], rate_limit=0.1)
        assert len(results) == 3

        # Should sleep between tickers (2 sleeps for 3 tickers)
        assert mock_sleep.call_count == 2

    @patch("ingestion.altdata.earnings_puller.time.sleep")
    @patch("ingestion.altdata.earnings_puller.yf.Ticker")
    def test_pull_all_continues_on_failure(self, mock_ticker_cls, mock_sleep):
        engine, mock_conn = _make_mock_engine()
        from ingestion.altdata.earnings_puller import EarningsPuller

        puller = EarningsPuller(db_engine=engine)

        # First ticker fails, second succeeds
        def side_effect(ticker):
            if ticker == "BAD":
                raise ConnectionError("fail")
            mock_stock = MagicMock()
            mock_stock.earnings_dates = pd.DataFrame()
            mock_stock.quarterly_earnings = pd.DataFrame()
            mock_stock.earnings_history = pd.DataFrame()
            return mock_stock

        mock_ticker_cls.side_effect = side_effect

        results = puller.pull_all(ticker_list=["BAD", "GOOD"], rate_limit=0)
        assert len(results) == 2
        assert results[0]["status"] == "FAILED"
        assert results[1]["status"] == "PARTIAL"  # no data, but no crash


class TestGetSummary:
    """Tests for get_summary reporting."""

    def test_summary_counts(self):
        engine, _ = _make_mock_engine()
        from ingestion.altdata.earnings_puller import EarningsPuller

        puller = EarningsPuller(db_engine=engine)

        results = [
            {
                "ticker": "AAPL",
                "status": "SUCCESS",
                "rows_inserted": 15,
                "errors": [],
                "significant_surprises": [
                    {"ticker": "AAPL", "date": "2025-01-29", "surprise_pct": 15.0,
                     "classification": "significant_beat", "eps_estimate": 1.0, "eps_actual": 1.15},
                ],
            },
            {
                "ticker": "BAD",
                "status": "FAILED",
                "rows_inserted": 0,
                "errors": ["Network error"],
                "significant_surprises": [],
            },
            {
                "ticker": "MSFT",
                "status": "PARTIAL",
                "rows_inserted": 0,
                "errors": ["No data"],
                "significant_surprises": [],
            },
        ]

        summary = puller.get_summary(results)
        assert summary["total_tickers"] == 3
        assert summary["succeeded"] == 1
        assert summary["failed"] == 1
        assert summary["partial"] == 1
        assert summary["total_rows_inserted"] == 15
        assert summary["failed_tickers"] == ["BAD"]
        assert len(summary["significant_beats"]) == 1
        assert len(summary["significant_misses"]) == 0


class TestDetectSignificantSurprises:
    """Tests for _detect_significant_surprises."""

    def test_detects_significant_beat(self):
        engine, _ = _make_mock_engine()
        from ingestion.altdata.earnings_puller import EarningsPuller

        puller = EarningsPuller(db_engine=engine)

        dates = pd.to_datetime(["2025-01-29"])
        df = pd.DataFrame(
            {"EPS Estimate": [1.0], "Reported EPS": [1.5], "Surprise(%)": [50.0]},
            index=dates,
        )
        stock = MagicMock()
        stock.earnings_dates = df

        surprises = puller._detect_significant_surprises("AAPL", stock)
        assert len(surprises) == 1
        assert surprises[0]["classification"] == "significant_beat"
        assert surprises[0]["surprise_pct"] == 50.0

    def test_ignores_small_surprise(self):
        engine, _ = _make_mock_engine()
        from ingestion.altdata.earnings_puller import EarningsPuller

        puller = EarningsPuller(db_engine=engine)

        dates = pd.to_datetime(["2025-01-29"])
        df = pd.DataFrame(
            {"EPS Estimate": [1.0], "Reported EPS": [1.05], "Surprise(%)": [5.0]},
            index=dates,
        )
        stock = MagicMock()
        stock.earnings_dates = df

        surprises = puller._detect_significant_surprises("AAPL", stock)
        assert len(surprises) == 0

    def test_handles_empty_earnings(self):
        engine, _ = _make_mock_engine()
        from ingestion.altdata.earnings_puller import EarningsPuller

        puller = EarningsPuller(db_engine=engine)

        stock = MagicMock()
        stock.earnings_dates = pd.DataFrame()

        surprises = puller._detect_significant_surprises("AAPL", stock)
        assert surprises == []


class TestTickerList:
    """Tests for the ticker universe constant."""

    def test_ticker_count(self):
        from ingestion.altdata.earnings_puller import EARNINGS_TICKERS
        # Should have ~96 tickers as specified
        assert len(EARNINGS_TICKERS) >= 90
        assert len(EARNINGS_TICKERS) <= 120

    def test_key_tickers_present(self):
        from ingestion.altdata.earnings_puller import EARNINGS_TICKERS
        for t in ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"]:
            assert t in EARNINGS_TICKERS

    def test_no_duplicates(self):
        from ingestion.altdata.earnings_puller import EARNINGS_TICKERS
        assert len(EARNINGS_TICKERS) == len(set(EARNINGS_TICKERS))
