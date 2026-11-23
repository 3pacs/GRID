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
from store.pit import retraction_cutoff


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


class _FeatureConn(_RecordingConn):
    """Answers the feature_registry id lookup from ``ids``, every other read with ``rows``."""

    def __init__(self, ids: dict[str, int], rows: list[tuple[Any, ...]] | None = None) -> None:
        super().__init__(rows)
        self.ids = ids

    def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> _Result:
        result = super().execute(stmt, params)
        if "FROM feature_registry fr WHERE fr.name = :n" in str(stmt):
            name = (params or {})["n"]
            return _Result([(self.ids[name],)] if name in self.ids else [])
        return result


class _NoReadConn:
    def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> _Result:
        raise AssertionError(f"unexpected read: {stmt}")


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


def test_load_latest_uses_release_and_vintage_as_of_filter() -> None:
    as_of = date(2026, 5, 13)
    conn = _FeatureConn({"vix_spot": 11}, rows=[(19.5,)])

    assert cs._load_latest(conn, "vix_spot", as_of_date=as_of) == 19.5

    lookup, call = conn.calls
    assert lookup.params == {"n": "vix_spot"}
    assert "rs.release_date <= :as_of" in call.sql
    assert "rs.vintage_date <= :as_of" in call.sql
    assert "rs.obs_date <= :as_of" in call.sql
    assert "resolved_series_retractions" in call.sql
    assert "rr.retracted_at <= :retraction_cutoff" in call.sql
    # The id is bound, not matched through a feature_registry join, so the
    # planner uses the feature's own statistics (2.7 s -> 2 ms on griddb).
    assert "rs.feature_id = :fid" in call.sql
    assert "feature_registry" not in call.sql
    assert "ORDER BY rs.obs_date DESC, rs.release_date DESC, rs.vintage_date DESC" in call.sql
    assert call.params == {
        "fid": 11,
        "as_of": as_of,
        "retraction_cutoff": retraction_cutoff(as_of),
    }


def test_load_latest_and_price_of_an_unregistered_feature_read_nothing_else() -> None:
    conn = _FeatureConn({})

    assert cs._load_latest(conn, "wiki_abc", as_of_date=date(2026, 5, 13)) is None
    assert cs._load_price(conn, "ABC", as_of_date=date(2026, 5, 13)).empty
    assert [c.params for c in conn.calls] == [{"n": "wiki_abc"}, {"n": "abc_full"}]


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


def test_load_price_uses_release_and_vintage_as_of_filter() -> None:
    as_of = date(2026, 5, 13)
    conn = _FeatureConn(
        {"abc_full": 42},
        rows=[
            (date(2026, 5, 12), 100.0),
            (date(2026, 5, 13), 101.0),
        ],
    )

    out = cs._load_price(conn, "ABC", as_of_date=as_of)

    lookup, call = conn.calls
    assert lookup.params == {"n": "abc_full"}
    assert "rs.release_date <= :as_of" in call.sql
    assert "rs.vintage_date <= :as_of" in call.sql
    assert "rs.obs_date <= :as_of" in call.sql
    assert "resolved_series_retractions" in call.sql
    assert "rs.feature_id = :fid" in call.sql
    assert "feature_registry" not in call.sql
    # One row per obs_date: the one that sorted last in (obs_date,
    # release_date, vintage_date) order, which the Python dedup used to keep.
    assert "SELECT DISTINCT ON (rs.obs_date)" in call.sql
    assert "ORDER BY rs.obs_date, rs.release_date DESC, rs.vintage_date DESC" in call.sql
    assert call.params == {
        "fid": 42,
        "as_of": as_of,
        "retraction_cutoff": retraction_cutoff(as_of),
    }
    assert list(out) == [100.0, 101.0]


def test_load_market_setup_reads_the_three_scored_inputs_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    as_of = date(2026, 5, 13)
    reads: list[tuple[str, date | None]] = []
    values = {"vix_spot": 21.0, "hy_oas_spread": 4.2, "yld_curve_2s10s": -0.1}

    def fake_latest(_conn: Any, name: str, *, as_of_date: date | None = None) -> float | None:
        reads.append((name, as_of_date))
        return values.get(name)

    monkeypatch.setattr(cs, "_load_latest", fake_latest)

    market = cs.load_market_setup(object(), as_of)

    assert market == cs.MarketSetup(as_of=as_of, vix=21.0, hy=4.2, yc=-0.1)
    # ofr_financial_stress was read and discarded; it is no longer read.
    assert reads == [("vix_spot", as_of), ("hy_oas_spread", as_of), ("yld_curve_2s10s", as_of)]


def test_score_setup_with_a_market_snapshot_reads_nothing_and_matches_a_direct_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    as_of = date(2026, 5, 13)
    values = {"vix_spot": 27.0, "hy_oas_spread": 4.5, "yld_curve_2s10s": -0.2}
    monkeypatch.setattr(cs, "_load_latest", lambda _c, name, **_kw: values.get(name))
    price = _series([100.0, 40.0])

    direct = cs.score_setup(object(), "ABC", price, as_of_date=as_of)
    market = cs.load_market_setup(object(), as_of)
    hoisted = cs.score_setup(_NoReadConn(), "ABC", price, as_of_date=as_of, market=market)

    assert hoisted == direct
    assert direct.score == 4 + 3 + 2 + 3  # DD -60 %, VIX 27, HY 4.5, curve inverted


def test_score_setup_refuses_a_market_snapshot_for_another_date() -> None:
    market = cs.MarketSetup(as_of=date(2026, 5, 12), vix=None, hy=None, yc=None)

    with pytest.raises(ValueError, match="market setup is for 2026-05-12"):
        cs.score_setup(_NoReadConn(), "ABC", _series([100.0]), as_of_date=date(2026, 5, 13), market=market)


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
    # 17 = drawdown 5 + VIX 5 + credit 4 + curve 3. planetary_stress_index is
    # supplied above but must contribute nothing (removed 2026-09-10).
    assert result.score == 17
    assert result.max_score == cs.SETUP_MAX_SCORE == 17
    assert not any("PSI" in signal for signal in result.signals)
    assert result.data_available is True
    assert any("[EXTREME]" in signal for signal in result.signals)


def test_score_company_returns_none_without_xbrl_writer() -> None:
    """No XBRL:* writer exists in raw_series, so COMPANY returns None (unavailable)."""
    conn = _RecordingConn()
    assert cs.score_company(conn, "ABC", as_of_date=date(2026, 5, 13)) is None
    assert conn.calls == []


def test_score_smart_money_scores_reported_ps_form4_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, date | None]] = []

    def fake_window(_conn: Any, ticker: str, as_of_date: date | None) -> cs.Form4PSWindow:
        calls.append((ticker, as_of_date))
        return cs.Form4PSWindow(
            start=date(2026, 2, 12), end=date(2026, 5, 12), txns=3.0, shares_valid=3.0,
            priced=3.0, net_shares=12_000.0, net_value=600_000.0,
        )

    monkeypatch.setattr(cs, "_form4_ps_window", fake_window)

    result = cs.score_smart_money(object(), "ABC", as_of_date=date(2026, 5, 13))

    assert calls == [("ABC", date(2026, 5, 13))]
    assert result.name == "SMART_MONEY"
    assert result.score == 5
    assert (
        "Insider reported P/S (non-derivative, venue unknown) net buy $600,000 "
        "(verified 2026-02-12..2026-05-12, 90d of 90d)"
    ) in result.signals
    assert result.trust == 0.85
    assert any("Institutional flow unavailable" in s for s in result.signals)
    assert result.data_available is True


def test_form4_ps_fields_never_include_all_code_or_legacy_series() -> None:
    assert not {"NET_SHARES", "NET_VALUE", "TXN_COUNT"} & set(cs._FORM4_PS_FIELDS)
    assert not any(f.startswith("OPEN_MKT") for f in cs._FORM4_PS_FIELDS)


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

    def fake_setup(
        _conn: Any,
        ticker: str,
        _price: pd.Series,
        *,
        as_of_date: date | None = None,
        market: cs.MarketSetup | None = None,
    ):
        seen.append((f"setup:{ticker}", as_of_date))
        assert market is None  # a lone score_ticker leaves the read to score_setup
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
    seen: list[tuple[str, date | None, cs.MarketSetup | None]] = []
    market = cs.MarketSetup(as_of=as_of, vix=30.0, hy=None, yc=None)
    market_reads: list[date | None] = []

    def fake_market(_conn: Any, as_of_date: date | None = None) -> cs.MarketSetup:
        market_reads.append(as_of_date)
        return market

    def fake_score_ticker(
        _engine: Any,
        ticker: str,
        *,
        as_of_date: date | None = None,
        market: cs.MarketSetup | None = None,
    ):
        seen.append((ticker, as_of_date, market))
        score = 80 if ticker == "AAA" else 30
        return cs.ConvictionReport(ticker, score, score, (), "FIRE", "ts")

    monkeypatch.setattr(cs, "load_market_setup", fake_market)
    monkeypatch.setattr(cs, "score_ticker", fake_score_ticker)

    reports = cs.scan_all(_Engine(conn), min_score=40, as_of_date=as_of)

    call = conn.calls[0]
    assert "rs.release_date <= :as_of" in call.sql
    assert "rs.vintage_date <= :as_of" in call.sql
    assert "rs.obs_date <= :as_of" in call.sql
    # More than 50 rows known at as_of, found without counting every row, in name order.
    assert "OFFSET 50 LIMIT 1" in call.sql
    assert "ORDER BY fr.name" in call.sql
    assert call.params == {"as_of": as_of}
    # One market snapshot for the scan, the same object for every ticker.
    assert market_reads == [as_of]
    assert seen == [("BBB", as_of, market), ("AAA", as_of, market)]
    assert [report.ticker for report in reports] == ["AAA"]
