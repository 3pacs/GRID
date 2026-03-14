"""
Tests for the GRID ingestion layer.

Uses unittest.mock to avoid real API calls and database writes.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


class TestFREDPuller:
    """Tests for the FRED data puller."""

    @patch("ingestion.fred.Fred")
    def test_fred_pull_inserts_rows(self, mock_fred_class):
        """Pulling a FRED series should insert one row per observation."""
        # Set up mock engine
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        # Mock source_catalog lookup
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, idx: 1
        mock_conn.execute.return_value.fetchone.return_value = mock_row

        # Mock fredapi to return 3 observations
        mock_fred_instance = mock_fred_class.return_value
        mock_series = pd.Series(
            [1.5, 2.0, 2.5],
            index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
        )
        mock_fred_instance.get_series.return_value = mock_series

        # Make _row_exists return False (no duplicates)
        # The fetchone for dedup check returns None
        mock_conn.execute.return_value.fetchone.side_effect = [
            mock_row,  # source_id lookup
            None, None, None,  # dedup checks (3 obs)
        ]

        from ingestion.fred import FREDPuller

        puller = FREDPuller(api_key="test_key", db_engine=mock_engine)
        result = puller.pull_series("T10Y2Y", "2024-01-01")

        assert result["status"] == "SUCCESS"
        assert result["rows_inserted"] == 3
        assert result["series_id"] == "T10Y2Y"

    @patch("ingestion.fred.Fred")
    def test_fred_pull_failure_logs_not_raises(self, mock_fred_class):
        """A failed FRED pull should not raise; should record FAILED status."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, idx: 1
        mock_conn.execute.return_value.fetchone.return_value = mock_row

        # Mock fredapi to raise an exception
        mock_fred_instance = mock_fred_class.return_value
        mock_fred_instance.get_series.side_effect = Exception("API Error")

        from ingestion.fred import FREDPuller

        puller = FREDPuller(api_key="test_key", db_engine=mock_engine)

        # Should NOT raise
        result = puller.pull_series("BAD_SERIES", "2024-01-01")

        assert result["status"] == "FAILED"
        assert len(result["errors"]) > 0
        assert "API Error" in result["errors"][0]


class TestYFinancePuller:
    """Tests for the yfinance data puller."""

    @patch("ingestion.yfinance_pull.yf")
    def test_yfinance_pull_inserts_ohlcv(self, mock_yf):
        """Pulling a ticker should insert rows for close and volume fields."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, idx: 2
        mock_conn.execute.return_value.fetchone.return_value = mock_row

        # Mock yfinance.download to return a minimal DataFrame
        dates = pd.to_datetime(["2024-01-02", "2024-01-03"])
        mock_df = pd.DataFrame(
            {
                "Open": [100.0, 101.0],
                "High": [102.0, 103.0],
                "Low": [99.0, 100.0],
                "Close": [101.0, 102.0],
                "Volume": [1000000, 1100000],
                "Adj Close": [101.0, 102.0],
            },
            index=dates,
        )
        mock_yf.download.return_value = mock_df

        from ingestion.yfinance_pull import YFinancePuller

        puller = YFinancePuller(db_engine=mock_engine)
        result = puller.pull_ticker("^GSPC", "2024-01-01")

        assert result["status"] == "SUCCESS"
        # 6 fields x 2 dates = 12 rows
        assert result["rows_inserted"] == 12
        assert result["ticker"] == "^GSPC"
