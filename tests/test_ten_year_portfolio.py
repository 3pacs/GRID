from __future__ import annotations

from datetime import date, timedelta

from strategy.ten_year_portfolio import (
    FRONTIER_THEME_CANDIDATES,
    build_weekly_recommendation,
    compute_chart_metrics,
    parse_yf_series_id,
)
from api.routers.ten_year_portfolio import _load_price_history


class _FakeCursor:
    def __init__(self, calls: list):
        self._calls = calls

    def execute(self, sql, params=None):
        self._calls.append((str(sql), params))

    def fetchall(self):
        return []


class _FakeConnection:
    def __init__(self, calls: list):
        self._calls = calls

    def cursor(self):
        return _FakeCursor(self._calls)

    def rollback(self):
        return None

    def close(self):
        return None


class _FakeURL:
    def render_as_string(self, hide_password: bool = False) -> str:
        return "postgresql://test:test@localhost:5432/test"


class _FakeEngine:
    def __init__(self):
        self.calls: list[tuple[str, object]] = []
        self.url = _FakeURL()

    def make_connection(self):
        return _FakeConnection(self.calls)


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
    engine = _FakeEngine()

    import api.routers.ten_year_portfolio as mod

    monkeypatch.setattr(
        mod.psycopg2,
        "connect",
        lambda dsn: engine.make_connection(),
    )

    result = _load_price_history(engine, years=10)

    assert result == {}
    select_calls = [
        (sql, params)
        for sql, params in engine.calls
        if "SET statement_timeout" not in sql
    ]
    raw_call = next(
        (sql, params) for sql, params in select_calls if "FROM raw_series" in sql
    )
    resolved_call = next(
        (sql, params)
        for sql, params in select_calls
        if "JOIN resolved_series rs" in sql
    )
    raw_sql, raw_params = raw_call
    resolved_sql, resolved_params = resolved_call
    raw_series_ids = raw_params[0]
    resolved_feature_names = resolved_params[0]
    # Frontier tickers without a *_full resolved equivalent hit raw_series.
    assert "series_id = ANY" in raw_sql
    assert "YF:CCJ:adj_close" in raw_series_ids
    assert "YF:HPE:adj_close" in raw_series_ids
    # Main 20 + thematic frontier tickers route through resolved_series.
    assert "fr.name = ANY" in resolved_sql
    assert "aapl_full" in resolved_feature_names
    assert "qqq_full" in resolved_feature_names
    assert "tsm_full" in resolved_feature_names


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
