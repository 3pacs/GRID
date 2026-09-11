"""Tests for GET /api/v1/signals/timeseries date/series alignment."""

from __future__ import annotations

import datetime
from unittest.mock import MagicMock, patch


def _make_engine(rows: list[tuple]) -> MagicMock:
    """Build a MagicMock engine whose single execute() call returns the given rows."""
    engine = MagicMock()
    conn = MagicMock()
    result = MagicMock()
    result.fetchall.return_value = rows
    conn.execute.return_value = result
    engine.connect.return_value.__enter__.return_value = conn
    return engine


def _call(rows: list[tuple], features: str = "spy_close,btc_price", days: int = 30) -> dict:
    engine = _make_engine(rows)
    with patch("api.routers.signals.get_db_engine", return_value=engine):
        from api.routers.signals import get_timeseries

        return get_timeseries(features=features, days=days, _token="test")


class TestTimeseriesDateAlignment:
    """dates[feature][i] must be the obs_date for series[feature][i]."""

    def test_dates_and_series_aligned_per_feature(self):
        rows = [
            ("spy_close", datetime.date(2026, 9, 1), 100.0),
            ("spy_close", datetime.date(2026, 9, 2), 101.0),
            ("spy_close", datetime.date(2026, 9, 3), 102.0),
            ("btc_price", datetime.date(2026, 8, 30), 50000.0),
            ("btc_price", datetime.date(2026, 8, 31), 51000.0),
            ("btc_price", datetime.date(2026, 9, 1), 52000.0),
            ("btc_price", datetime.date(2026, 9, 2), 53000.0),
        ]
        result = _call(rows)

        assert result["series"]["spy_close"] == [100.0, 101.0, 102.0]
        assert result["dates"]["spy_close"] == ["2026-09-01", "2026-09-02", "2026-09-03"]

        assert result["series"]["btc_price"] == [50000.0, 51000.0, 52000.0, 53000.0]
        assert result["dates"]["btc_price"] == [
            "2026-08-30", "2026-08-31", "2026-09-01", "2026-09-02",
        ]

        for name in result["series"]:
            assert len(result["series"][name]) == len(result["dates"][name])

    def test_series_shape_unchanged_for_existing_consumers(self):
        rows = [
            ("spy_close", datetime.date(2026, 9, 1), 100.0),
            ("spy_close", datetime.date(2026, 9, 2), 101.0),
        ]
        result = _call(rows, features="spy_close", days=30)

        assert result["series"] == {"spy_close": [100.0, 101.0]}
        assert result["days"] == 30
        assert result["count"] == 1

    def test_null_value_and_null_date_handled(self):
        rows = [("spy_close", None, None)]
        result = _call(rows, features="spy_close")

        assert result["series"]["spy_close"] == [0.0]
        assert result["dates"]["spy_close"] == [None]

    def test_empty_result_returns_empty_series_and_dates(self):
        result = _call(rows=[], features="nonexistent_feature")

        assert result["series"] == {}
        assert result["dates"] == {}
        assert result["count"] == 0
