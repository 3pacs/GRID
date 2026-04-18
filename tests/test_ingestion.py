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


class _FakeRow:
    def __init__(self, value):
        self._value = value

    def __getitem__(self, idx):
        return self._value


def _make_raw_series_engine(source_id: int = 2, existing_dates: dict[str, set[date]] | None = None):
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    existing_dates = existing_dates or {}
    execute_calls: list[tuple[str, object]] = []

    def _execute(sql, params=None):
        sql_text = str(sql)
        execute_calls.append((sql_text, params))
        result = MagicMock()
        if "SELECT id FROM source_catalog" in sql_text:
            result.fetchone.return_value = _FakeRow(source_id)
        elif "SELECT DISTINCT obs_date FROM raw_series" in sql_text:
            sid = params["sid"] if isinstance(params, dict) else None
            dates = existing_dates.get(sid, set())
            result.fetchall.return_value = [(d,) for d in dates]
        elif "INSERT INTO raw_series" in sql_text:
            result.rowcount = 1
        else:
            result.fetchone.return_value = None
            result.fetchall.return_value = []
        return result

    conn.execute.side_effect = _execute
    return engine, conn, execute_calls


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

    @patch("ingestion.fred.FredAPI")
    def test_fred_unknown_dataframe_layout_is_soft_skipped(self, mock_fred_class):
        """Malformed fedfred frames should not create FAILED rows."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, idx: 1
        mock_conn.execute.return_value.fetchone.return_value = mock_row
        mock_fred_class.return_value.get_series_observations.return_value = pd.DataFrame(
            {"unexpected": ["not-a-date"]}
        )

        from ingestion.fred import FREDPuller

        puller = FREDPuller(api_key="test_key", db_engine=mock_engine)
        result = puller.pull_series("H8B1023NCBCMG", "2024-01-01")

        assert result["status"] == "SKIPPED"
        assert "Unknown column layout" in result["errors"][0]
        mock_engine.begin.assert_not_called()

    @patch("ingestion.fred.FredAPI")
    def test_fred_prefers_observation_index_over_realtime_date(self, mock_fred_class):
        """fedfred can expose realtime vintage dates in a column named date."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, idx: 1
        mock_conn.execute.return_value.fetchone.return_value = mock_row

        frame = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-04-17", "2026-04-17"]),
                "value": [10.0, 11.0],
            },
            index=pd.to_datetime(["2026-01-01", "2026-02-01"]),
        )
        frame.index.name = "date"
        mock_fred_class.return_value.get_series_observations.return_value = frame

        from ingestion.fred import FREDPuller

        puller = FREDPuller(api_key="test_key", db_engine=mock_engine)
        mock_conn.reset_mock()
        with patch.object(FREDPuller, "_get_existing_dates", return_value=set()):
            result = puller.pull_series("TOTRESNS", "2026-01-01")

        insert_params = [
            call.args[1]
            for call in mock_conn.execute.call_args_list
            if len(call.args) > 1 and isinstance(call.args[1], dict) and "od" in call.args[1]
        ]

        assert result["status"] == "SUCCESS"
        assert result["rows_inserted"] == 2
        assert [params["od"] for params in insert_params] == [
            date(2026, 1, 1),
            date(2026, 2, 1),
        ]

    @patch("ingestion.fred.FredAPI")
    def test_fred_index_only_date_series_inserts_cleanly(self, mock_fred_class):
        """Regression for the 'date' KeyError that spammed logs before Apr 8.

        When fedfred returns a frame whose observation dates live only in a
        DatetimeIndex (no column named 'date'), _normalise_observation_frame
        must synthesise a 'date' column; pull_series must then iterate without
        KeyError.
        """
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, idx: 1
        mock_conn.execute.return_value.fetchone.return_value = mock_row

        frame = pd.DataFrame(
            {"value": [1.5, 2.0, 2.5]},
            index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
        )
        # No name on the index — mimics the shape that used to KeyError
        frame.index.name = None
        mock_fred_class.return_value.get_series_observations.return_value = frame

        from ingestion.fred import FREDPuller

        puller = FREDPuller(api_key="test_key", db_engine=mock_engine)
        with patch.object(FREDPuller, "_get_existing_dates", return_value=set()):
            result = puller.pull_series("DGSN", "2024-01-01")

        assert result["status"] == "SUCCESS", result
        assert result["rows_inserted"] == 3

    @patch("ingestion.fred.FredAPI")
    def test_fred_retry_error_string_with_http_status_is_soft_skipped(self, mock_fred_class):
        """tenacity RetryError reprs can hide HTTPStatusError as plain text."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, idx: 1
        mock_conn.execute.return_value.fetchone.return_value = mock_row
        mock_fred_class.return_value.get_series_observations.side_effect = RuntimeError(
            "RetryError[<Future at 0x1 state=finished raised HTTPStatusError>]"
        )

        from ingestion.fred import FREDPuller

        puller = FREDPuller(api_key="test_key", db_engine=mock_engine)
        result = puller.pull_series("HOUST", "2024-01-01")

        assert result["status"] == "SKIPPED"
        assert "HTTP rejection" in result["errors"][0]


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

    @pytest.mark.parametrize(
        ("frame", "series_id"),
        [
            (pd.DataFrame({"date": pd.to_datetime(["2026-01-01"])}), "TOTRESNS"),
            (pd.DataFrame({"value": [1.23]}), "RRPONTSYD"),
        ],
    )
    def test_fed_liquidity_missing_date_or_value_columns_returns_empty_frame(
        self, monkeypatch, frame, series_id
    ):
        import sys

        from ingestion.altdata.fed_liquidity import FedLiquidityPuller

        class _FakeFred:
            def __init__(self, _api_key):
                pass

            def get_series_observations(self, *_args, **_kwargs):
                return frame

        monkeypatch.setitem(sys.modules, "fedfred", SimpleNamespace(FredAPI=_FakeFred))

        puller = FedLiquidityPuller.__new__(FedLiquidityPuller)
        puller._api_key = "test"

        result = puller._fetch_fred_series(series_id, date(2026, 1, 1), date(2026, 1, 2))

        assert list(result.columns) == ["date", "value"]
        assert result.empty

    def test_fed_liquidity_prefers_observation_index_over_realtime_date(self, monkeypatch):
        import sys

        from ingestion.altdata.fed_liquidity import FedLiquidityPuller

        class _FakeFred:
            def __init__(self, _api_key):
                pass

            def get_series_observations(self, *_args, **_kwargs):
                frame = pd.DataFrame(
                    {
                        "date": pd.to_datetime(["2026-04-17", "2026-04-17"]),
                        "value": [10.0, 11.0],
                    },
                    index=pd.to_datetime(["2026-01-01", "2026-02-01"]),
                )
                frame.index.name = "date"
                return frame

        monkeypatch.setitem(sys.modules, "fedfred", SimpleNamespace(FredAPI=_FakeFred))

        puller = FedLiquidityPuller.__new__(FedLiquidityPuller)
        puller._api_key = "test"

        result = puller._fetch_fred_series("TOTRESNS", date(2026, 1, 1), date(2026, 2, 1))

        assert list(result["date"].dt.date) == [date(2026, 1, 1), date(2026, 2, 1)]
        assert list(result["value"]) == [10.0, 11.0]

    def test_fed_liquidity_pull_raw_dedupes_duplicate_dates(self):
        from ingestion.altdata.fed_liquidity import FedLiquidityPuller

        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        puller = FedLiquidityPuller.__new__(FedLiquidityPuller)
        puller.engine = mock_engine
        puller.source_id = 1
        puller._fetch_fred_series = MagicMock(
            return_value=pd.DataFrame(
                {
                    "date": pd.to_datetime(["2026-01-01", "2026-01-01"]),
                    "value": [1.0, 1.0],
                }
            )
        )
        puller._get_existing_dates = MagicMock(return_value=set())
        puller._insert_raw = MagicMock()

        result = puller._pull_raw_series("RRPONTSYD", date(2026, 1, 1), date(2026, 1, 2))

        assert result["status"] == "SUCCESS"
        assert result["rows_inserted"] == 1
        puller._insert_raw.assert_called_once()

    def test_fed_liquidity_pull_raw_skips_bad_row_dates(self):
        from ingestion.altdata.fed_liquidity import FedLiquidityPuller

        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        puller = FedLiquidityPuller.__new__(FedLiquidityPuller)
        puller.engine = mock_engine
        puller.source_id = 1
        puller._fetch_fred_series = MagicMock(
            return_value=pd.DataFrame(
                {
                    "date": ["2026-01-01", "not-a-date"],
                    "value": [1.0, 2.0],
                }
            )
        )
        puller._get_existing_dates = MagicMock(return_value=set())
        puller._insert_raw = MagicMock()

        result = puller._pull_raw_series("TOTRESNS", date(2026, 1, 1), date(2026, 1, 2))

        assert result["status"] == "SUCCESS"
        assert result["rows_inserted"] == 1
        puller._insert_raw.assert_called_once()

    def test_fed_liquidity_pull_raw_skips_malformed_dataframe(self):
        from ingestion.altdata.fed_liquidity import FedLiquidityPuller

        mock_engine = MagicMock()
        puller = FedLiquidityPuller.__new__(FedLiquidityPuller)
        puller.engine = mock_engine
        puller.source_id = 1
        puller._fetch_fred_series = MagicMock(
            return_value=pd.DataFrame({"unexpected": ["not-a-date"]})
        )

        result = puller._pull_raw_series("TOTRESNS", date(2026, 1, 1), date(2026, 1, 2))

        assert result["status"] == "SKIPPED"
        assert "Malformed dataframe" in result["error"]
        mock_engine.begin.assert_not_called()

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

    def test_yfinance_pull_skips_existing_success_dates(self):
        """Existing SUCCESS rows should be skipped before insert."""
        import sys

        mock_yf_module = MagicMock()
        sys.modules.setdefault("yfinance", mock_yf_module)

        mock_engine, _, execute_calls = _make_raw_series_engine(
            source_id=2,
            existing_dates={
                "YF:^GSPC:close": {date(2024, 1, 3)},
                "YF:^GSPC:volume": {date(2024, 1, 3)},
            },
        )

        dates = pd.to_datetime(["2024-01-02", "2024-01-03"])
        mock_df = pd.DataFrame(
            {
                "Close": [101.0, 102.0],
                "Volume": [1000000, 1100000],
            },
            index=dates,
        )

        with patch("ingestion.yfinance_pull.yf") as mock_yf:
            mock_yf.download.return_value = mock_df

            from ingestion.yfinance_pull import YFinancePuller

            puller = YFinancePuller(db_engine=mock_engine)
            result = puller.pull_ticker("^GSPC", "2024-01-01")

        insert_sql = [sql for sql, _ in execute_calls if "INSERT INTO raw_series" in sql]

        assert result["status"] == "SUCCESS"
        assert result["rows_inserted"] == 2
        assert len(insert_sql) == 2
        assert all("ON CONFLICT" not in sql for sql in insert_sql)

    def test_yfinance_invalid_ticker_is_skipped_before_network(self):
        from ingestion.yfinance_pull import YFinancePuller

        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, idx: 2
        mock_conn.execute.return_value.fetchone.return_value = mock_row

        with patch("ingestion.yfinance_pull.yf") as mock_yf:
            puller = YFinancePuller(db_engine=mock_engine)
            result = puller.pull_ticker("NYSE:FLG", "2024-01-01")

        assert result["status"] == "SKIPPED"
        mock_yf.download.assert_not_called()

    def test_yfinance_class_share_ticker_is_normalized(self):
        from ingestion.yfinance_pull import _normalize_yahoo_ticker

        assert _normalize_yahoo_ticker("BRK.B") == "BRK-B"
        assert _normalize_yahoo_ticker("N/A") is None
        assert _normalize_yahoo_ticker("MOGA/MOGB") is None


class TestBLSPuller:
    """Tests for the BLS data puller."""

    @patch("ingestion.bls.requests.post")
    def test_bls_pull_skips_existing_success_dates(self, mock_post):
        mock_engine, _, execute_calls = _make_raw_series_engine(
            source_id=2,
            existing_dates={
                "LNS14000000": {date(2024, 1, 1)},
            },
        )

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {
            "status": "REQUEST_SUCCEEDED",
            "Results": {
                "series": [
                    {
                        "seriesID": "LNS14000000",
                        "data": [
                            {"year": "2024", "period": "M01", "value": "3.9"},
                            {"year": "2024", "period": "M02", "value": "3.8"},
                        ],
                    }
                ]
            },
        }
        mock_post.return_value = mock_resp

        from ingestion.bls import BLSPuller

        puller = BLSPuller(db_engine=mock_engine)
        result = puller.pull_series(["LNS14000000"], start_year=2024, end_year=2024)

        insert_sql = [sql for sql, _ in execute_calls if "INSERT INTO raw_series" in sql]

        assert result["status"] == "SUCCESS"
        assert result["rows_inserted"] == 1
        assert len(insert_sql) == 1
        assert all("ON CONFLICT" not in sql for sql in insert_sql)


class TestBulkDownloadPrices:
    """Tests for the bulk CSV loader."""

    def test_bulk_loader_skips_existing_success_dates(self, tmp_path):
        from scripts.bulk_download_prices import load_csvs_to_db

        csv_dir = tmp_path / "prices"
        csv_dir.mkdir()
        csv_path = csv_dir / "abc.csv"
        csv_path.write_text(
            "Date,Open,High,Low,Close,Volume\n"
            "2024-01-02,1,2,0.5,1.5,100\n"
            "2024-01-03,1.1,2.1,0.6,1.6,110\n"
        )

        mock_engine, _, execute_calls = _make_raw_series_engine(
            source_id=7,
            existing_dates={
                "YF:ABC:open": {date(2024, 1, 3)},
                "YF:ABC:high": {date(2024, 1, 3)},
                "YF:ABC:low": {date(2024, 1, 3)},
                "YF:ABC:close": {date(2024, 1, 3)},
                "YF:ABC:volume": {date(2024, 1, 3)},
            },
        )

        inserted = load_csvs_to_db(str(csv_dir), mock_engine, source_name="TEST_BULK")

        insert_sql = [sql for sql, _ in execute_calls if "INSERT INTO raw_series" in sql]

        assert inserted == 5
        assert len(insert_sql) == 1
        assert all("ON CONFLICT" not in sql for sql in insert_sql)
