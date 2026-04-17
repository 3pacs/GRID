"""
Tests for the GRID ingestion layer.

Uses unittest.mock to avoid real API calls and database writes.
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


class TestFREDPuller:
    """Tests for the FRED data puller (fedfred-based)."""

    @patch("ingestion.fred.FredAPI")
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

        # Mock fedfred to return a DataFrame with 3 observations
        mock_fred_instance = mock_fred_class.return_value
        mock_df = pd.DataFrame({
            "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "value": [1.5, 2.0, 2.5],
        })
        mock_fred_instance.get_series_observations.return_value = mock_df

        # Make _row_exists return False (no duplicates)
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

    @patch("ingestion.fred.FredAPI")
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

        # Mock fedfred to raise an exception
        mock_fred_instance = mock_fred_class.return_value
        mock_fred_instance.get_series_observations.side_effect = Exception("API Error")

        from ingestion.fred import FREDPuller

        puller = FREDPuller(api_key="test_key", db_engine=mock_engine)

        # Should NOT raise
        result = puller.pull_series("BAD_SERIES", "2024-01-01")

        assert result["status"] == "FAILED"
        assert len(result["errors"]) > 0
        assert "API Error" in result["errors"][0]

    @patch("ingestion.fred.FredAPI")
    def test_fred_permanent_http_error_is_soft_skipped(self, mock_fred_class):
        """Unavailable FRED series should not create fake FAILED rows."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, idx: 1
        mock_conn.execute.return_value.fetchone.return_value = mock_row

        exc = RuntimeError("not found")
        exc.response = SimpleNamespace(status_code=404)
        mock_fred_class.return_value.get_series_observations.side_effect = exc

        from ingestion.fred import FREDPuller

        puller = FREDPuller(api_key="test_key", db_engine=mock_engine)
        result = puller.pull_series("BAD_SERIES", "2024-01-01")

        assert result["status"] == "SKIPPED"
        assert "HTTP 404" in result["errors"][0]

    @patch("ingestion.fred.FredAPI")
    def test_fred_wrapped_http_error_is_soft_skipped(self, mock_fred_class):
        """fedfred may wrap HTTPStatusError in a retry object."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, idx: 1
        mock_conn.execute.return_value.fetchone.return_value = mock_row

        inner = RuntimeError("not found")
        inner.response = SimpleNamespace(status_code=404)
        wrapped = RuntimeError("RetryError")
        wrapped.last_attempt = SimpleNamespace(exception=lambda: inner)
        mock_fred_class.return_value.get_series_observations.side_effect = wrapped

        from ingestion.fred import FREDPuller

        puller = FREDPuller(api_key="test_key", db_engine=mock_engine)
        result = puller.pull_series("BAD_SERIES", "2024-01-01")

        assert result["status"] == "SKIPPED"
        assert "HTTP 404" in result["errors"][0]

    @patch("ingestion.fred.FredAPI")
    def test_fred_wrapped_http_status_error_without_code_is_soft_skipped(self, mock_fred_class):
        """fedfred/httpx retry wrappers do not always expose a status cleanly."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, idx: 1
        mock_conn.execute.return_value.fetchone.return_value = mock_row

        HTTPStatusError = type("HTTPStatusError", (Exception,), {})
        inner = HTTPStatusError("upstream rejected series")
        wrapped = RuntimeError("RetryError")
        wrapped.last_attempt = SimpleNamespace(exception=lambda: inner)
        mock_fred_class.return_value.get_series_observations.side_effect = wrapped

        from ingestion.fred import FREDPuller

        puller = FREDPuller(api_key="test_key", db_engine=mock_engine)
        result = puller.pull_series("ADVFN", "2024-01-01")

        assert result["status"] == "SKIPPED"
        assert "HTTP rejection" in result["errors"][0]

    def test_invalid_breadth_series_are_not_in_default_fred_pull(self):
        from ingestion.fred import FRED_SERIES_LIST

        assert "ADVFN" not in FRED_SERIES_LIST
        assert "DECFN" not in FRED_SERIES_LIST


class TestAltDataPullers:
    def test_solar_kp_parser_accepts_current_dict_payload(self, monkeypatch):
        from ingestion.celestial.solar import SolarActivityPuller

        puller = SolarActivityPuller.__new__(SolarActivityPuller)
        monkeypatch.setattr(
            puller,
            "_fetch_json",
            lambda url: [
                {"time_tag": "2026-04-10T00:00:00", "Kp": 4.0},
                {"time_tag": "2026-04-10T03:00:00", "Kp": 2.0},
            ],
        )

        result = puller._pull_kp_index()

        assert result[date(2026, 4, 10)] == 3.0

    def test_fed_liquidity_missing_layout_returns_empty_frame(self, monkeypatch):
        import sys

        from ingestion.altdata.fed_liquidity import FedLiquidityPuller

        class _FakeFred:
            def __init__(self, _api_key):
                pass

            def get_series_observations(self, *_args, **_kwargs):
                return pd.DataFrame({"unexpected": ["not-a-date"]})

        monkeypatch.setitem(sys.modules, "fedfred", SimpleNamespace(FredAPI=_FakeFred))

        puller = FedLiquidityPuller.__new__(FedLiquidityPuller)
        puller._api_key = "test"

        result = puller._fetch_fred_series("H8B1023NCBCMG", date(2026, 1, 1), date(2026, 1, 2))

        assert list(result.columns) == ["date", "value"]
        assert result.empty

    def test_prediction_odds_caps_market_scan_to_scheduler_budget(self, monkeypatch):
        from ingestion.altdata.prediction_odds import PredictionOddsPuller

        puller = PredictionOddsPuller.__new__(PredictionOddsPuller)
        puller._fetch_all_active_markets = MagicMock(return_value=[{"question": f"Fed {i}"} for i in range(100)])
        puller._detect_rapid_shifts = MagicMock(return_value=None)
        monkeypatch.setattr("ingestion.altdata.prediction_odds.time.sleep", lambda *_args, **_kwargs: None)

        result = puller.pull_shifts()

        assert result["status"] == "SUCCESS"
        assert result["markets_scanned"] == 40
        assert result["markets_relevant"] == 100


class TestYFinancePuller:
    """Tests for the yfinance data puller."""

    def test_yfinance_pull_inserts_ohlcv(self):
        """Pulling a ticker should insert rows for close and volume fields."""
        import sys

        # Inject a fake yfinance module so the top-level import succeeds
        mock_yf_module = MagicMock()
        sys.modules.setdefault("yfinance", mock_yf_module)

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

        with patch("ingestion.yfinance_pull.yf") as mock_yf:
            mock_yf.download.return_value = mock_df

            from ingestion.yfinance_pull import YFinancePuller

            puller = YFinancePuller(db_engine=mock_engine)
            result = puller.pull_ticker("^GSPC", "2024-01-01")

        assert result["status"] == "SUCCESS"
        # 6 fields x 2 dates = 12 rows
        assert result["rows_inserted"] == 12
        assert result["ticker"] == "^GSPC"
