"""Focused tests for alpha_research.conviction_scorer.

The tests use connection and engine doubles only. They guard the PIT query
contract, representative scorer buckets for all seven layers, and the public
entry-point alert level mapping.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import pandas as pd
import pytest

from alpha_research import conviction_scorer as cs


@dataclass(frozen=True)
class _ExecCall:
    sql: str
    params: dict[str, Any]


class _Result:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows


class _RecordingConn:
    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self.rows = rows or []
        self.calls: list[_ExecCall] = []

    def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> _Result:
        bound = dict(params or {})
        self.calls.append(_ExecCall(sql=str(stmt), params=bound))
        return _Result(self.rows)


class _Engine:
    def __init__(self, conn: _RecordingConn) -> None:
        self.conn = conn

    def connect(self) -> "_Engine":
        return self

    def __enter__(self) -> _RecordingConn:
        return self.conn

    def __exit__(self, *_args: Any) -> None:
        return None


def _series(values: list[float], end: date = date(2026, 5, 13)) -> pd.Series:
    idx = pd.date_range(end=end, periods=len(values), freq="D")
    return pd.Series(values, index=idx)


def test_load_latest_uses_release_date_as_of_filter() -> None:
    as_of = date(2026, 5, 13)
    conn = _RecordingConn(rows=[(19.5,)])

    assert cs._load_latest(conn, "vix_spot", as_of_date=as_of) == 19.5

    call = conn.calls[0]
    assert "rs.release_date <= :as_of" in call.sql
    assert "rs.obs_date <= :as_of" in call.sql
    assert call.params == {"n": "vix_spot", "as_of": as_of}


def test_load_raw_latest_uses_pull_timestamp_as_release_proxy_with_source() -> None:
    as_of = date(2026, 5, 13)
    conn = _RecordingConn(rows=[(1.23,)])

    assert cs._load_raw_latest(conn, "CBOE:totalpc", source_id=5, as_of_date=as_of) == 1.23

    call = conn.calls[0]
    assert "pull_timestamp::date <= :as_of" in call.sql
    assert "obs_date <= :as_of" in call.sql
    assert "source_id = :s" in call.sql
    assert call.params["s"] == 5
    assert call.params["as_of"] == as_of


def test_load_raw_series_uses_as_of_window_and_pull_timestamp_release_proxy() -> None:
    as_of = date(2026, 5, 13)
    conn = _RecordingConn(
        rows=[
            (date(2026, 5, 12), 10.0),
            (date(2026, 5, 13), 11.0),
        ]
    )

    out = cs._load_raw_series(conn, "XBRL:ABC:Revenues", days=30, as_of_date=as_of)

    call = conn.calls[0]
    assert "pull_timestamp::date <= :as_of" in call.sql
    assert "obs_date <= :as_of" in call.sql
    assert call.params == {
        "s": "XBRL:ABC:Revenues",
        "d": as_of - timedelta(days=30),
        "as_of": as_of,
    }
    assert list(out) == [10.0, 11.0]


def test_load_price_uses_release_date_as_of_filter() -> None:
    as_of = date(2026, 5, 13)
    conn = _RecordingConn(
        rows=[
            (date(2026, 5, 12), 100.0),
            (date(2026, 5, 13), 101.0),
        ]
    )

    out = cs._load_price(conn, "ABC", as_of_date=as_of)

    call = conn.calls[0]
    assert "rs.release_date <= :as_of" in call.sql
    assert "rs.obs_date <= :as_of" in call.sql
    assert call.params == {"n": "abc_full", "as_of": as_of}
    assert list(out) == [100.0, 101.0]


def test_score_setup_bucket_maxes_on_extreme_macro_conditions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = {
        "vix_spot": 36.0,
        "hy_oas_spread": 5.5,
        "planetary_stress_index": 1.0,
        "yld_curve_2s10s": -0.25,
    }
    monkeypatch.setattr(cs, "_load_latest", lambda _c, name, **_kw: values.get(name))

    result = cs.score_setup(object(), "ABC", _series([100.0, 20.0]), as_of_date=date(2026, 5, 13))

    assert result.name == "SETUP"
    assert result.score == 20
    assert result.max_score == 20
    assert result.data_available is True
    assert any("[EXTREME]" in signal for signal in result.signals)


def test_score_company_buckets_growth_profit_buyback_and_cash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data = {
        "XBRL:ABC:Revenues": _series([100.0, 120.0]),
        "XBRL:ABC:EarningsPerShareDiluted": _series([-1.0, 1.0]),
        "XBRL:ABC:CommonStockSharesOutstanding": _series([100.0, 90.0]),
        "XBRL:ABC:CashAndCashEquivalentsAtCarryingValue": _series([60.0]),
        "XBRL:ABC:Liabilities": _series([100.0]),
    }

    def fake_raw_series(_conn: Any, series_id: str, *_args: Any, **_kwargs: Any) -> pd.Series:
        return data.get(series_id, pd.Series(dtype=float))

    monkeypatch.setattr(cs, "_load_raw_series", fake_raw_series)

    result = cs.score_company(object(), "ABC", as_of_date=date(2026, 5, 13))

    assert result.name == "COMPANY"
    assert result.score == 15
    assert result.trust == pytest.approx(0.90)
    assert result.data_available is True


def test_score_smart_money_buckets_insider_surge_and_inflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data = {
        "SEC_FORM4:ABC:monthly": _series([1.0, 1.0, 1.0, 3.0, 3.0, 3.0]),
        "INST_FLOW:ABC:%": _series([-1.0, 2.0]),
    }

    def fake_raw_series(_conn: Any, series_id: str, *_args: Any, **_kwargs: Any) -> pd.Series:
        return data.get(series_id, pd.Series(dtype=float))

    monkeypatch.setattr(cs, "_load_raw_series", fake_raw_series)

    result = cs.score_smart_money(object(), "ABC", as_of_date=date(2026, 5, 13))

    assert result.name == "SMART_MONEY"
    assert result.score == 10
    assert "Institutional net inflow" in result.signals
    assert result.data_available is True


def test_score_crowd_buckets_short_ftd_and_put_call_extremes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data = {
        "SHORT:ABC:ratio": _series([0.60, 0.62, 0.61]),
        "FTD:ABC:qty": _series([5_000.0] * 30 + [20_000.0] * 10),
    }

    def fake_raw_series(_conn: Any, series_id: str, *_args: Any, **_kwargs: Any) -> pd.Series:
        return data.get(series_id, pd.Series(dtype=float))

    monkeypatch.setattr(cs, "_load_raw_series", fake_raw_series)
    monkeypatch.setattr(cs, "_load_raw_latest", lambda *_args, **_kwargs: 1.30)

    result = cs.score_crowd(object(), "ABC", as_of_date=date(2026, 5, 13))

    assert result.name == "CROWD"
    assert result.score == 13
    assert any("HEAVILY SHORTED" in signal for signal in result.signals)
    assert result.data_available is True


def test_score_narrative_buckets_news_attention_and_gdelt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cs,
        "_load_raw_series",
        lambda *_args, **_kwargs: _series([1.0] * 30 + [3.0] * 7),
    )

    latest = {"wiki_abc": 10.0, "gdelt_article_count": 1500.0}
    monkeypatch.setattr(cs, "_load_latest", lambda _c, name, **_kw: latest.get(name))

    result = cs.score_narrative(object(), "ABC", as_of_date=date(2026, 5, 13))

    assert result.name == "NARRATIVE"
    assert result.score == 8
    assert any("News volume surging" in signal for signal in result.signals)
    assert result.data_available is True


def test_score_flow_buckets_volatility_options_and_equity_put_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prices = _series([100.0] * 60 + [110.0, 121.0, 133.1, 146.41, 161.051])

    def fake_raw_latest(_conn: Any, pattern: str, *_args: Any, **_kwargs: Any) -> float | None:
        if pattern.startswith("WHALE:ABC"):
            return 1.0
        if pattern == "CBOE:equitypc":
            return 0.90
        return None

    monkeypatch.setattr(cs, "_load_raw_latest", fake_raw_latest)

    result = cs.score_flow(object(), "ABC", prices, as_of_date=date(2026, 5, 13))

    assert result.name == "FLOW"
    assert result.score == 13
    assert any("Unusual options" in signal for signal in result.signals)
    assert result.data_available is True


def test_score_confirmation_buckets_turning_momentum_and_higher_low() -> None:
    down = [100.0 - (50.0 * i / 60.0) for i in range(61)]
    up = [50.0 + (40.0 * (i + 1) / 30.0) for i in range(30)]
    prices = _series(down + up)

    result = cs.score_confirmation(prices)

    assert result.name == "CONFIRM"
    assert result.score == 10
    assert any("Momentum TURNING" in signal for signal in result.signals)
    assert result.data_available is True


def test_score_ticker_passes_as_of_date_to_layer_helpers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    as_of = date(2026, 5, 13)
    seen: list[tuple[str, date | None]] = []

    def fake_price(_conn: Any, ticker: str, *, as_of_date: date | None = None) -> pd.Series:
        seen.append((f"price:{ticker}", as_of_date))
        return _series([100.0] * 91)

    def layer(name: str, score: float, max_score: float):
        return cs.LayerResult(name, score, max_score, 1.0, (), True)

    def fake_setup(_conn: Any, ticker: str, _price: pd.Series, *, as_of_date: date | None = None):
        seen.append((f"setup:{ticker}", as_of_date))
        return layer("SETUP", 10, 20)

    def fake_company(_conn: Any, ticker: str, *, as_of_date: date | None = None):
        seen.append((f"company:{ticker}", as_of_date))
        return layer("COMPANY", 7.5, 15)

    def fake_smart(_conn: Any, ticker: str, *, as_of_date: date | None = None):
        seen.append((f"smart:{ticker}", as_of_date))
        return layer("SMART_MONEY", 7.5, 15)

    def fake_crowd(_conn: Any, ticker: str, *, as_of_date: date | None = None):
        seen.append((f"crowd:{ticker}", as_of_date))
        return layer("CROWD", 7.5, 15)

    def fake_narrative(_conn: Any, ticker: str, *, as_of_date: date | None = None):
        seen.append((f"narrative:{ticker}", as_of_date))
        return layer("NARRATIVE", 5, 10)

    def fake_flow(_conn: Any, ticker: str, _price: pd.Series, *, as_of_date: date | None = None):
        seen.append((f"flow:{ticker}", as_of_date))
        return layer("FLOW", 7.5, 15)

    monkeypatch.setattr(cs, "_load_price", fake_price)
    monkeypatch.setattr(cs, "score_setup", fake_setup)
    monkeypatch.setattr(cs, "score_company", fake_company)
    monkeypatch.setattr(cs, "score_smart_money", fake_smart)
    monkeypatch.setattr(cs, "score_crowd", fake_crowd)
    monkeypatch.setattr(cs, "score_narrative", fake_narrative)
    monkeypatch.setattr(cs, "score_flow", fake_flow)
    monkeypatch.setattr(cs, "score_confirmation", lambda _price: layer("CONFIRM", 5, 10))

    report = cs.score_ticker(_Engine(_RecordingConn()), "ABC", as_of_date=as_of)

    assert report.ticker == "ABC"
    assert report.total_score == 50
    assert seen == [
        ("price:ABC", as_of),
        ("setup:ABC", as_of),
        ("company:ABC", as_of),
        ("smart:ABC", as_of),
        ("crowd:ABC", as_of),
        ("narrative:ABC", as_of),
        ("flow:ABC", as_of),
    ]


@pytest.mark.parametrize(
    ("total_score", "expected"),
    [
        (85, "CONVICTION"),
        (70, "FIRE"),
        (55, "SCALE"),
        (40, "PILOT"),
        (25, "WATCH"),
        (24, "PASS"),
    ],
)
def test_score_ticker_alert_level_mapping(
    monkeypatch: pytest.MonkeyPatch,
    total_score: int,
    expected: str,
) -> None:
    fraction = total_score / 100
    layers = (
        cs.LayerResult("SETUP", 20 * fraction, 20, 1.0, (), True),
        cs.LayerResult("COMPANY", 15 * fraction, 15, 1.0, (), True),
        cs.LayerResult("SMART_MONEY", 15 * fraction, 15, 1.0, (), True),
        cs.LayerResult("CROWD", 15 * fraction, 15, 1.0, (), True),
        cs.LayerResult("NARRATIVE", 10 * fraction, 10, 1.0, (), True),
        cs.LayerResult("FLOW", 15 * fraction, 15, 1.0, (), True),
        cs.LayerResult("CONFIRM", 10 * fraction, 10, 1.0, (), True),
    )

    monkeypatch.setattr(cs, "_load_price", lambda *_args, **_kwargs: _series([100.0] * 91))
    monkeypatch.setattr(cs, "score_setup", lambda *_args, **_kwargs: layers[0])
    monkeypatch.setattr(cs, "score_company", lambda *_args, **_kwargs: layers[1])
    monkeypatch.setattr(cs, "score_smart_money", lambda *_args, **_kwargs: layers[2])
    monkeypatch.setattr(cs, "score_crowd", lambda *_args, **_kwargs: layers[3])
    monkeypatch.setattr(cs, "score_narrative", lambda *_args, **_kwargs: layers[4])
    monkeypatch.setattr(cs, "score_flow", lambda *_args, **_kwargs: layers[5])
    monkeypatch.setattr(cs, "score_confirmation", lambda *_args, **_kwargs: layers[6])

    report = cs.score_ticker(_Engine(_RecordingConn()), "ABC")

    assert report.total_score == total_score
    assert report.alert_level == expected


def test_high_score_with_sparse_coverage_maps_to_fire(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layers = (
        cs.LayerResult("SETUP", 20, 20, 1.0, (), True),
        cs.LayerResult("COMPANY", 15, 15, 1.0, (), True),
        cs.LayerResult("SMART_MONEY", 15, 15, 1.0, (), True),
        cs.LayerResult("CROWD", 15, 15, 1.0, (), True),
        cs.LayerResult("NARRATIVE", 10, 10, 1.0, (), True),
        cs.LayerResult("FLOW", 15, 15, 1.0, (), False),
        cs.LayerResult("CONFIRM", 10, 10, 1.0, (), False),
    )

    monkeypatch.setattr(cs, "_load_price", lambda *_args, **_kwargs: _series([100.0] * 91))
    monkeypatch.setattr(cs, "score_setup", lambda *_args, **_kwargs: layers[0])
    monkeypatch.setattr(cs, "score_company", lambda *_args, **_kwargs: layers[1])
    monkeypatch.setattr(cs, "score_smart_money", lambda *_args, **_kwargs: layers[2])
    monkeypatch.setattr(cs, "score_crowd", lambda *_args, **_kwargs: layers[3])
    monkeypatch.setattr(cs, "score_narrative", lambda *_args, **_kwargs: layers[4])
    monkeypatch.setattr(cs, "score_flow", lambda *_args, **_kwargs: layers[5])
    monkeypatch.setattr(cs, "score_confirmation", lambda *_args, **_kwargs: layers[6])

    report = cs.score_ticker(_Engine(_RecordingConn()), "ABC")

    assert report.total_score == 100
    assert report.confidence_pct == pytest.approx(71.4)
    assert report.alert_level == "FIRE"


def test_scan_all_filters_ticker_universe_as_of_and_scores_same_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    as_of = date(2026, 5, 13)
    conn = _RecordingConn(rows=[("BBB",), ("AAA",)])
    seen: list[tuple[str, date | None]] = []

    def fake_score_ticker(_engine: Any, ticker: str, *, as_of_date: date | None = None):
        seen.append((ticker, as_of_date))
        score = 80 if ticker == "AAA" else 30
        return cs.ConvictionReport(ticker, score, score, (), "FIRE", "ts")

    monkeypatch.setattr(cs, "score_ticker", fake_score_ticker)

    reports = cs.scan_all(_Engine(conn), min_score=40, as_of_date=as_of)

    call = conn.calls[0]
    assert "rs.release_date <= :as_of" in call.sql
    assert "rs.obs_date <= :as_of" in call.sql
    assert call.params == {"as_of": as_of}
    assert seen == [("BBB", as_of), ("AAA", as_of)]
    assert [report.ticker for report in reports] == ["AAA"]
