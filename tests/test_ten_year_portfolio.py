from __future__ import annotations

import threading
from datetime import date, timedelta

from strategy.ten_year_portfolio import (
    FRONTIER_RAW_HISTORY_TICKERS,
    FRONTIER_THEME_CANDIDATES,
    build_weekly_recommendation,
    compute_chart_metrics,
    parse_yf_series_id,
)
from api.routers.ten_year_portfolio import (
    _RESOLVED_TICKER_TO_FEATURE,
    _load_price_history,
)


class _FakeCursor:
    """Records executed SQL + positional params; returns no rows.

    Shared across the two psycopg2 connections the loader opens (one per
    fetch thread), so a lock guards the recorder list.
    """

    def __init__(self, recorder: list, lock: threading.Lock):
        self._recorder = recorder
        self._lock = lock

    def execute(self, sql, params=None):
        with self._lock:
            self._recorder.append((str(sql), params))

    def fetchall(self):
        return []

    def close(self):
        pass


class _FakePgConnection:
    """Stand-in for the psycopg2 connection used by the threaded fetchers."""

    def __init__(self, recorder: list, lock: threading.Lock):
        self._recorder = recorder
        self._lock = lock

    def cursor(self):
        return _FakeCursor(self._recorder, self._lock)

    def rollback(self):
        pass

    def close(self):
        pass


class _FakeURL:
    def render_as_string(self, hide_password: bool = False) -> str:
        return "postgresql://grid:testpass@localhost:5432/griddb_test"


class _FakeEngine:
    """Minimal SQLAlchemy Engine stand-in exposing only ``.url``.

    The loader derives a DSN via ``engine.url.render_as_string`` and then
    opens its own psycopg2 connections, so the engine needs nothing else.
    """

    def __init__(self):
        self.url = _FakeURL()


def _weekly_growth(start: date, weeks: int, first: float, weekly_rate: float):
    value = first
    rows = []
    for idx in range(weeks):
        rows.append((start + timedelta(days=idx * 7), value))
        value *= 1.0 + weekly_rate
    return rows


def _choppy_growth(start: date, weeks: int, first: float):
    value = first
    rows = []
    for idx in range(weeks):
        rows.append((start + timedelta(days=idx * 7), value))
        value *= 1.04 if idx % 5 == 0 else 0.995
    return rows


def test_parse_yf_series_id_accepts_close_fields_only():
    assert parse_yf_series_id("YF:AAPL:adj_close") == ("AAPL", "adj_close")
    assert parse_yf_series_id("YF:BRK-B:close") == ("BRK-B", "close")
    assert parse_yf_series_id("FRED:DFF") is None
    assert parse_yf_series_id("YF:AAPL:volume") is None


def test_price_history_loader_uses_core_yahoo_adjusted_close_universe(monkeypatch):
    executed: list[tuple[str, object]] = []
    lock = threading.Lock()

    def _fake_connect(dsn):
        assert dsn == "postgresql://grid:testpass@localhost:5432/griddb_test"
        return _FakePgConnection(executed, lock)

    monkeypatch.setattr(
        "api.routers.ten_year_portfolio.psycopg2.connect", _fake_connect
    )

    result = _load_price_history(_FakeEngine(), years=10)

    # No rows from either query -> empty history.
    assert result == {}

    # Both analytical queries ran (the SET statement_timeout calls carry no
    # params and are ignored here).
    resolved = next((c for c in executed if "resolved_series" in c[0]), None)
    raw = next((c for c in executed if "FROM raw_series" in c[0]), None)
    assert resolved is not None, "resolved_series query was not issued"
    assert raw is not None, "raw_series query was not issued"

    resolved_features, resolved_lookback = resolved[1]
    raw_series_ids, raw_lookback = raw[1]

    # raw_series covers exactly the frontier raw-history tickers; the resolved
    # universe covers the deduped <ticker>_full features (AAPL/QQQ live there,
    # NOT in raw_series).
    assert set(raw_series_ids) == {
        f"YF:{t}:adj_close" for t in FRONTIER_RAW_HISTORY_TICKERS
    }
    assert set(_RESOLVED_TICKER_TO_FEATURE.values()).issubset(set(resolved_features))
    assert "aapl_full" in resolved_features
    assert "qqq_full" in resolved_features

    # years=10 -> lookback_days = max(365, int(10 * 365.25) + 45) = 3697,
    # passed to both queries.
    assert resolved_lookback == 3697
    assert raw_lookback == 3697


def test_chart_metrics_reward_smooth_up_right_relative_to_qqq():
    start = date(2016, 1, 1)
    qqq = compute_chart_metrics("QQQ", _weekly_growth(start, 520, 100, 0.0019), None)
    smooth = compute_chart_metrics("AAA", _weekly_growth(start, 520, 50, 0.0032), qqq)
    choppy = compute_chart_metrics("BBB", _choppy_growth(start, 520, 50), qqq)

    assert smooth is not None
    assert choppy is not None
    assert smooth["cagr"] > qqq["cagr"]
    assert smooth["trend_r2"] > choppy["trend_r2"]
    assert smooth["relative_cagr"] > 0


def test_weekly_recommendation_builds_profile_allocations_from_one_million():
    start = date(2016, 1, 1)
    history = {
        "QQQ": _weekly_growth(start, 520, 100, 0.0020),
        "AAPL": _weekly_growth(start, 520, 80, 0.0036),
        "MSFT": _weekly_growth(start, 520, 40, 0.0031),
        "NVDA": _weekly_growth(start, 520, 60, 0.0028),
        "AMZN": _choppy_growth(start, 520, 55),
        "GOOGL": _weekly_growth(start, 520, 30, 0.0025),
        "META": _weekly_growth(start, 520, 25, 0.0024),
        "AVGO": _weekly_growth(start, 520, 45, 0.0023),
        "COST": _weekly_growth(start, 520, 35, 0.0022),
    }

    result = build_weekly_recommendation(history, capital=1_000_000, years=10)

    assert result["status"] == "ok"
    assert result["capital"] == 1_000_000
    assert result["universe"]["ranked_candidates"] >= 8
    dad = next(profile for profile in result["profiles"] if profile["id"] == "dad_chartist")
    assert dad["allocations"][0]["ticker"] == "AAPL"
    assert dad["estimated_residual_cash"] >= dad["cash_target"]
    assert all(pick["target_weight"] <= dad["max_position"] + 0.0001 for pick in dad["allocations"])
    assert dad["monte_carlo"]["simulations"] == 2000
    assert dad["monte_carlo"]["p10"] <= dad["monte_carlo"]["p50"] <= dad["monte_carlo"]["p90"]
    assert 0 <= dad["monte_carlo"]["probability_above_start"] <= 1


def test_weekly_recommendation_includes_frontier_candidate_board():
    start = date(2016, 1, 1)
    history = {
        "QQQ": _weekly_growth(start, 520, 100, 0.0020),
        "NVDA": _weekly_growth(start, 520, 50, 0.0050),
        "CCJ": _weekly_growth(start, 520, 15, 0.0032),
        "FCX": _weekly_growth(start, 520, 18, 0.0026),
        "HPE": _weekly_growth(start, 520, 12, 0.0024),
        "AAPL": _weekly_growth(start, 520, 80, 0.0036),
    }

    result = build_weekly_recommendation(history, capital=1_000_000, years=10)
    board = result["candidate_boards"][0]

    assert board["id"] == "frontier_infrastructure"
    assert board["universe"]["ranked_candidates"] >= 4
    assert board["ranked"][0]["ticker"] == "NVDA"
    assert "AI compute" in FRONTIER_THEME_CANDIDATES["NVDA"]
    assert any(row["ticker"] == "CCJ" and "uranium" in row["themes"] for row in board["ranked"])
