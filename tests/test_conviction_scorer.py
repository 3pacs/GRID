"""Focused tests for alpha_research.conviction_scorer.

The tests use connection and engine doubles, and an in-memory SQLite
raw_series for the CROWD / NARRATIVE inputs (read through the real
store.observations reader). They guard the PIT query contract,
representative scorer buckets for all seven layers, and the public
entry-point alert level mapping.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import pytest
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
)
from sqlalchemy.pool import StaticPool

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
    conn = _RecordingConn(rows=[(19.5,)])

    assert cs._load_latest(conn, "vix_spot", as_of_date=as_of) == 19.5

    call = conn.calls[0]
    assert "rs.release_date <= :as_of" in call.sql
    assert "rs.vintage_date <= :as_of" in call.sql
    assert "rs.obs_date <= :as_of" in call.sql
    assert "resolved_series_retractions" in call.sql
    assert "rr.retracted_at <= :retraction_cutoff" in call.sql
    assert call.params == {
        "n": "vix_spot",
        "as_of": as_of,
        "retraction_cutoff": retraction_cutoff(as_of),
    }


def test_read_known_is_a_pull_evidence_read_from_the_writer_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    as_of = date(2026, 5, 13)
    calls: list[dict[str, Any]] = []

    def fake_known_at(conn: Any, series_id: str, **kwargs: Any) -> list[Any]:
        calls.append({"conn": conn, "series_id": series_id, **kwargs})
        return []

    monkeypatch.setattr(cs, "read_window_known_at", fake_known_at)
    conn = object()

    assert cs._read_known(conn, "put_call_ratio", "CBOE", as_of, 5) == []
    # No modeled lag: a row counts only once it was pulled by the end of as_of.
    assert calls == [{
        "conn": conn, "series_id": "put_call_ratio", "as_of": as_of, "lag": None,
        "source": "CBOE", "start": as_of - timedelta(days=5),
    }]


def test_prefix_range_loader_is_gone() -> None:
    # Under griddb's en_US.UTF-8 collation 'WHALE:SPY:600' < 'WHALE:SPY;' is
    # false, so the ">= prefix / < prefix+1" scan read none of the rows.
    assert not hasattr(cs, "_load_raw_latest")


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
    conn = _RecordingConn(
        rows=[
            (date(2026, 5, 12), 100.0),
            (date(2026, 5, 13), 101.0),
        ]
    )

    out = cs._load_price(conn, "ABC", as_of_date=as_of)

    call = conn.calls[0]
    assert "rs.release_date <= :as_of" in call.sql
    assert "rs.vintage_date <= :as_of" in call.sql
    assert "rs.obs_date <= :as_of" in call.sql
    assert "resolved_series_retractions" in call.sql
    assert call.params == {
        "n": "abc_full",
        "as_of": as_of,
        "retraction_cutoff": retraction_cutoff(as_of),
    }
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


def _obs(series_id: str, values: list[float], end: date = date(2026, 5, 13), step: int = 1) -> list[Any]:
    from store.observations import Observation

    first = end - timedelta(days=step * (len(values) - 1))
    return [
        Observation(series_id, first + timedelta(days=step * i), v, None, None)
        for i, v in enumerate(values)
    ]


def _install_known(monkeypatch: pytest.MonkeyPatch, known: dict[str, list[Any]]) -> list[tuple[str, str, int]]:
    reads: list[tuple[str, str, int]] = []

    def fake_read_known(_conn: Any, series_id: str, source: str, _as_of: date, days: int) -> list[Any]:
        reads.append((series_id, source, days))
        return known.get(series_id, [])

    monkeypatch.setattr(cs, "_read_known", fake_read_known)
    return reads


def test_score_crowd_buckets_ftd_and_put_call_extremes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ftd_id = "sec:ftd_balance:000000AB1"
    reads = _install_known(monkeypatch, {
        ftd_id: _obs(ftd_id, [5_000.0] * 30 + [20_000.0] * 10),
        "put_call_ratio": _obs("put_call_ratio", [1.30]),
    })

    result = cs.score_crowd(
        object(), "ABC", as_of_date=date(2026, 5, 13), ftd_series_ids={"ABC": (ftd_id,)},
    )

    assert result.name == "CROWD"
    assert result.score == 8  # FTD surge 5 + PCR 3; short interest scores nothing
    assert result.score == cs.LAYER_ATTAINABLE_SCORE["CROWD"]
    assert cs.SHORT_INTEREST_UNAVAILABLE in result.signals
    assert any("SQUEEZE SETUP" in signal for signal in result.signals)
    assert any("MAX BEARISH" in signal for signal in result.signals)
    assert result.data_available is True
    # Each input is read from its writer's own source.
    assert reads == [(ftd_id, "SEC_FTD", 90), ("put_call_ratio", "CBOE", 5)]


def test_score_crowd_reads_nothing_for_short_interest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reads = _install_known(monkeypatch, {})

    result = cs.score_crowd(object(), "ABC", as_of_date=date(2026, 5, 13), ftd_series_ids={})

    assert not any(sid.startswith("SHORT:") or "short_volume" in sid for sid, _, _ in reads)
    assert result.score == 0
    assert result.data_available is False
    assert cs.SHORT_INTEREST_UNAVAILABLE in result.signals


def test_score_narrative_buckets_news_surge_and_reports_wiki_gdelt_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sid = "TIINGO_NEWS:ABC:daily_count"
    # Trailing 7-day counts: 7 (1/day) for 60 days, then 49 (7/day).
    reads = _install_known(monkeypatch, {sid: _obs(sid, [7.0] * 60 + [49.0] * 31)})
    monkeypatch.setattr(cs, "_load_latest", lambda *_a, **_k: pytest.fail("wiki/GDELT must not be read"))

    result = cs.score_narrative(object(), "ABC", as_of_date=date(2026, 5, 13))

    assert result.name == "NARRATIVE"
    assert result.score == 5 == cs.LAYER_ATTAINABLE_SCORE["NARRATIVE"]
    assert any("News volume surging (7.0/day vs 1.0/day" in signal for signal in result.signals)
    assert cs.WIKI_UNAVAILABLE in result.signals and cs.GDELT_UNAVAILABLE in result.signals
    assert result.data_available is True
    assert reads == [(sid, "tiingo_news", 90)]


def test_score_flow_buckets_volatility_and_equity_put_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prices = _series([100.0] * 60 + [110.0, 121.0, 133.1, 146.41, 161.051])
    reads = _install_known(monkeypatch, {
        "equity_put_call_ratio": _obs("equity_put_call_ratio", [0.85]),
    })
    conn = _RecordingConn()

    result = cs.score_flow(conn, "ABC", prices, as_of_date=date(2026, 5, 13))

    assert result.name == "FLOW"
    assert result.score == 8 == cs.LAYER_ATTAINABLE_SCORE["FLOW"]  # volatility 5 + equity PCR 3
    assert any("Volatility spike" in signal for signal in result.signals)
    assert "Equity PCR=0.85 (2026-05-13) [BEARISH - contrarian buy]" in result.signals
    assert cs.WHALE_NOT_SCORED in result.signals
    assert result.data_available is True
    # The equity ratio is read from its writer's source; nothing reads WHALE:*.
    assert reads == [("equity_put_call_ratio", "CBOE", 5)]
    assert conn.calls == []


# ── CROWD / NARRATIVE / FLOW inputs on a real raw_series table ────────────
#
# SQLite stands in for griddb: the FTD symbol lookup is plain SQL, and the
# value reads go through the real store.observations.read_window_known_at.

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

_AS_OF = date(2026, 11, 20)
_SRC = {"SEC_FTD": 1, "CBOE": 2, "tiingo_news": 3, "SEC_EDGAR": 4}


def _evening(d: date) -> datetime:
    return datetime.combine(d, datetime.min.time()) + timedelta(hours=22)


class _RawDB:
    def __init__(self) -> None:
        self.engine = create_engine(
            "sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False},
        )
        md = MetaData()
        catalog = Table(
            "source_catalog", md,
            Column("id", Integer, primary_key=True), Column("name", String, nullable=False),
        )
        self._raw = Table(
            "raw_series", md,
            Column("series_id", String, nullable=False),
            Column("source_id", Integer, nullable=False),
            Column("obs_date", Date, nullable=False),
            Column("pull_timestamp", DateTime, nullable=False),
            Column("value", Float, nullable=False),
            Column("raw_payload", Text),
            Column("pull_status", String, nullable=False),
        )
        md.create_all(self.engine)
        with self.engine.begin() as c:
            c.execute(catalog.insert(), [{"id": i, "name": n} for n, i in _SRC.items()])

    def add(
        self, series_id: str, obs_date: date, value: float, *, source: str,
        pulled: datetime | None = None, symbol: str | None = None, status: str = "SUCCESS",
    ) -> None:
        with self.engine.begin() as c:
            c.execute(self._raw.insert(), [{
                "series_id": series_id, "source_id": _SRC[source], "obs_date": obs_date,
                "pull_timestamp": pulled or _evening(obs_date), "value": value,
                "raw_payload": json.dumps({"symbol": symbol} if symbol else {}),
                "pull_status": status,
            }])

    def ftd(self, cusip: str, symbol: str, balances: list[float], end: date, **kw: Any) -> str:
        sid = f"sec:ftd_balance:{cusip}"
        first = end - timedelta(days=len(balances) - 1)
        for i, v in enumerate(balances):
            self.add(sid, first + timedelta(days=i), v, source="SEC_FTD", symbol=symbol, **kw)
        return sid


def test_ftd_series_ids_maps_symbols_known_at_the_as_of_date() -> None:
    db = _RawDB()
    aapl = db.ftd("037833100", "AAPL", [100.0], _AS_OF - timedelta(days=20))
    # Pulled after the as-of date: not yet known.
    db.ftd("594918104", "MSFT", [100.0], _AS_OF - timedelta(days=20), pulled=_evening(_AS_OF + timedelta(days=1)))
    # Settled before the 90-day window, a failed pull, and another source's row.
    db.ftd("88160R101", "TSLA", [100.0], _AS_OF - timedelta(days=95))
    db.ftd("30303M102", "META", [0.0], _AS_OF - timedelta(days=5), status="FAILED")
    db.add("FTD:NVDA:qty", _AS_OF - timedelta(days=5), 1.0, source="SEC_EDGAR", symbol="NVDA")
    # One symbol on two CUSIPs, and the listed GOLD (the gold_full panel is futures).
    db.ftd("36467W109", "GME", [100.0], _AS_OF - timedelta(days=30))
    db.ftd("36467W208", "GME", [100.0], _AS_OF - timedelta(days=10))
    db.ftd("067901108", "GOLD", [100.0], _AS_OF - timedelta(days=10))
    # "_" in the prefix is a literal: a look-alike id never matches.
    db.add("sec:ftdXbalance:000000000", _AS_OF - timedelta(days=5), 1.0, source="SEC_FTD", symbol="AMD")

    with db.engine.connect() as conn:
        ids = cs._ftd_series_ids(
            conn, ["AAPL", "msft", "TSLA", "META", "NVDA", "GME", "GOLD", "AMD"], _AS_OF,
        )

    assert ids == {
        "AAPL": (aapl,),
        "GME": ("sec:ftd_balance:36467W109", "sec:ftd_balance:36467W208"),
    }


def test_score_crowd_reads_ftd_and_put_call_point_in_time() -> None:
    db = _RawDB()
    # 40 settlement dates ending 10 days before as-of: 30 at 5k, then 10 at 20k.
    sid = db.ftd("037833100", "AAPL", [5_000.0] * 30 + [20_000.0] * 10, _AS_OF - timedelta(days=10))
    # A revision of the last date pulled after as-of must not count.
    db.add(sid, _AS_OF - timedelta(days=10), 1.0, source="SEC_FTD", symbol="AAPL",
           pulled=_evening(_AS_OF + timedelta(days=2)))
    db.add("put_call_ratio", _AS_OF - timedelta(days=1), 1.25, source="CBOE")
    db.add("put_call_ratio", _AS_OF, 0.80, source="CBOE", pulled=_evening(_AS_OF + timedelta(days=1)))

    with db.engine.connect() as conn:
        result = cs.score_crowd(conn, "AAPL", as_of_date=_AS_OF)

    assert result.score == 8  # FTD surge 5 + PCR 1.25 -> 3
    assert result.data_available is True
    assert result.trust == cs.LAYER_FULL_TRUST["CROWD"]
    signals = " | ".join(result.signals)
    assert f"settled to {_AS_OF - timedelta(days=10)}" in signals
    assert f"PCR=1.25 ({_AS_OF - timedelta(days=1)}) [MAX BEARISH]" in signals


@pytest.mark.parametrize(
    ("setup", "expected"),
    [
        ("none", "No SEC FTD balance filed under AAPL"),
        ("stale", "SEC FTD balance stale for AAPL"),
        ("two_cusips", "FTD not scored (AAPL is filed under 2 CUSIPs"),
    ],
)
def test_score_crowd_ftd_fails_closed(setup: str, expected: str) -> None:
    db = _RawDB()
    if setup == "stale":
        db.ftd("037833100", "AAPL", [50_000.0] * 5, _AS_OF - timedelta(days=cs.FTD_FRESH_DAYS + 1))
    elif setup == "two_cusips":
        db.ftd("037833100", "AAPL", [50_000.0] * 5, _AS_OF - timedelta(days=10))
        db.ftd("037833209", "AAPL", [50_000.0] * 5, _AS_OF - timedelta(days=10))
    # Put/call older than the freshness window does not count either.
    db.add("put_call_ratio", _AS_OF - timedelta(days=cs.PCR_FRESH_DAYS + 1), 1.5, source="CBOE")

    with db.engine.connect() as conn:
        result = cs.score_crowd(conn, "AAPL", as_of_date=_AS_OF)

    assert result.score == 0
    assert result.data_available is False
    assert any(s.startswith(expected) for s in result.signals), result.signals
    assert any(s.startswith("Total put/call unavailable") for s in result.signals)


@pytest.mark.parametrize("ticker", ["GOLD", "BTC"])
def test_symbol_keyed_inputs_skip_a_ticker_whose_panel_is_another_instrument(ticker: str) -> None:
    # gold_full is gold futures and btc_full Binance BTCUSDT; SEC and Tiingo
    # file Barrick Gold and a bitcoin trust under the same symbols.
    db = _RawDB()
    db.ftd("067901108", ticker, [50_000.0] * 40, _AS_OF)
    db.add(f"TIINGO_NEWS:{ticker}:daily_count", _AS_OF, 49.0, source="tiingo_news")

    with db.engine.connect() as conn:
        crowd = cs.score_crowd(conn, ticker, as_of_date=_AS_OF)
        narrative = cs.score_narrative(conn, ticker, as_of_date=_AS_OF)

    assert (crowd.data_available, narrative.data_available) == (False, False)
    assert any(s.startswith("FTD not read") for s in crowd.signals)
    assert any(s.startswith("News not read") for s in narrative.signals)


def _news_db(values: list[float], end: date = _AS_OF) -> _RawDB:
    db = _RawDB()
    first = end - timedelta(days=len(values) - 1)
    for i, v in enumerate(values):
        db.add("TIINGO_NEWS:SPY:daily_count", first + timedelta(days=i), v, source="tiingo_news")
    return db


@pytest.mark.parametrize(
    ("values", "end", "score", "signal"),
    [
        # Trailing 7-day counts: 7 (1/day) for the oldest 30 days, then 21 (3/day).
        ([7.0] * 30 + [21.0] * 61, _AS_OF, 5.0, "News volume surging (3.0/day vs 1.0/day"),
        # 3/day is not above the moderate bar (the old rule read 21 as 21/day).
        ([21.0] * 91, _AS_OF, 0.0, "Light news coverage (3.0/day"),
        # A value at the fetch cap is a lower bound: moderate, never a surge
        # off a capped baseline.
        ([50.0] * 91, _AS_OF, 2.0, "Moderate news coverage (>=7.1/day"),
        # Fewer than a week of baseline pulls: no surge test.
        ([7.0] * 6 + [None] * 24 + [49.0] * 61, _AS_OF, 2.0, "Moderate news coverage (7.0/day"),
    ],
    ids=["surge", "units", "capped", "thin-baseline"],
)
def test_score_narrative_reads_tiingo_counts_as_trailing_7_day_totals(
    values: list[float | None], end: date, score: float, signal: str,
) -> None:
    db = _RawDB()
    first = end - timedelta(days=len(values) - 1)
    for i, v in enumerate(values):
        if v is not None:
            db.add("TIINGO_NEWS:SPY:daily_count", first + timedelta(days=i), v, source="tiingo_news")

    with db.engine.connect() as conn:
        result = cs.score_narrative(conn, "SPY", as_of_date=_AS_OF)

    assert result.score == score
    assert result.data_available is True
    assert any(s.startswith(signal) for s in result.signals), result.signals


def test_score_narrative_ignores_stale_and_unpulled_news_counts() -> None:
    db = _news_db([49.0] * 10, end=_AS_OF - timedelta(days=cs.NEWS_FRESH_DAYS + 1))
    db.add("TIINGO_NEWS:SPY:daily_count", _AS_OF, 49.0, source="tiingo_news",
           pulled=_evening(_AS_OF + timedelta(days=1)))
    # The legacy id carries nothing any more.
    db.add("NEWS:SPY:daily_count", _AS_OF, 49.0, source="tiingo_news")

    with db.engine.connect() as conn:
        result = cs.score_narrative(conn, "SPY", as_of_date=_AS_OF)

    assert result.score == 0
    assert result.data_available is False
    assert any(s.startswith("No fresh Tiingo news count for SPY") for s in result.signals)


def test_score_flow_counts_a_quiet_tape_as_measured() -> None:
    quiet = _series([100.0 + (i % 2) for i in range(61)])

    with _RawDB().engine.connect() as conn:
        measured = cs.score_flow(conn, "ABC", quiet, as_of_date=date(2026, 5, 13))
        short = cs.score_flow(conn, "ABC", quiet.tail(60), as_of_date=date(2026, 5, 13))

    assert (measured.score, measured.data_available, measured.trust) == (0, True, cs.LAYER_FULL_TRUST["FLOW"])
    assert any(s.startswith("No volatility spike") for s in measured.signals)
    # 59 returns: no 60-day baseline, so nothing was measured.
    assert (short.score, short.data_available) == (0, False)


def _flow_prices(n: int, end: date = _AS_OF) -> pd.Series:
    """A quiet tape (alternating +-1 %): 0 volatility points, measured from 61 prices."""
    return _series([100.0 + (i % 2) for i in range(n)], end=end)


def test_score_flow_reads_equity_put_call_point_in_time() -> None:
    db = _RawDB()
    db.add("equity_put_call_ratio", _AS_OF - timedelta(days=1), 0.86, source="CBOE")
    # Pulled after the as-of date: not known yet, so 0.86 is the newest.
    db.add("equity_put_call_ratio", _AS_OF, 0.55, source="CBOE", pulled=_evening(_AS_OF + timedelta(days=1)))
    # The legacy id, and the same id under another source, count for nothing.
    db.add("CBOE:equitypc", _AS_OF, 0.40, source="CBOE")
    db.add("equity_put_call_ratio", _AS_OF, 0.40, source="SEC_EDGAR")

    with db.engine.connect() as conn:
        result = cs.score_flow(conn, "SPY", _flow_prices(61), as_of_date=_AS_OF)

    assert result.score == 3
    assert (result.data_available, result.trust) == (True, cs.LAYER_FULL_TRUST["FLOW"])
    assert f"Equity PCR=0.86 ({_AS_OF - timedelta(days=1)}) [BEARISH - contrarian buy]" in result.signals


@pytest.mark.parametrize(
    ("value", "points", "signal"),
    [
        (0.81, 3, "Equity PCR=0.81 (2026-11-20) [BEARISH - contrarian buy]"),
        (0.80, 0, "Equity PCR=0.80 (2026-11-20)"),  # the rule is strictly above 0.8
        (0.62, 0, "Equity PCR=0.62 (2026-11-20)"),
    ],
)
def test_score_flow_equity_put_call_threshold(value: float, points: int, signal: str) -> None:
    db = _RawDB()
    db.add("equity_put_call_ratio", _AS_OF, value, source="CBOE")

    with db.engine.connect() as conn:
        result = cs.score_flow(conn, "SPY", _flow_prices(61), as_of_date=_AS_OF)

    assert result.score == points
    assert signal in result.signals


def test_score_flow_ignores_a_stale_equity_put_call() -> None:
    db = _RawDB()
    db.add("equity_put_call_ratio", _AS_OF - timedelta(days=cs.EQUITY_PCR_FRESH_DAYS + 1), 1.4, source="CBOE")

    with db.engine.connect() as conn:
        result = cs.score_flow(conn, "SPY", _flow_prices(61), as_of_date=_AS_OF)

    assert result.score == 0
    assert (
        f"Equity put/call unavailable (no equity_put_call_ratio in the {cs.EQUITY_PCR_FRESH_DAYS} days to {_AS_OF})"
        in result.signals
    )


def test_market_wide_equity_put_call_never_makes_flow_available() -> None:
    # 59 returns: no 60-day baseline, so this ticker's flow was not measured.
    # The market-wide ratio still adds its points (at the missing-layer trust),
    # as CROWD's total ratio does, but FLOW stays unavailable.
    db = _RawDB()
    db.add("equity_put_call_ratio", _AS_OF, 0.95, source="CBOE")

    with db.engine.connect() as conn:
        result = cs.score_flow(conn, "SPY", _flow_prices(60), as_of_date=_AS_OF)
        no_price = cs.score_flow(conn, "SPY", pd.Series(dtype=float), as_of_date=_AS_OF)

    assert (result.score, result.data_available, result.trust) == (3, False, 0.15)
    assert cs._layer_bound(result, cs.LAYER_MIN_SCORE) == 0.0  # not ticker evidence
    # Without a price panel FLOW is not scored at all, so nothing is read.
    assert (no_price.score, no_price.data_available) == (0, False)


def test_input_constants_match_their_writers() -> None:
    import inspect

    from ingestion import tiingo_news_pull
    from ingestion.altdata import cboe_indices, sec_ftd

    assert sec_ftd.SECFTDPuller.SOURCE_NAME == cs.FTD_SOURCE
    assert sec_ftd.SECFTDPuller.series_id("037833100") == f"{cs.FTD_SERIES_PREFIX}037833100"
    assert cboe_indices.CBOEIndicesPuller.SOURCE_NAME == cs.PCR_SOURCE == cs.EQUITY_PCR_SOURCE
    assert cboe_indices.PUT_CALL_SERIES_ID == cs.PCR_SERIES
    assert cboe_indices.EQUITY_PUT_CALL_SERIES_ID == cs.EQUITY_PCR_SERIES
    # Each id is the page ratio its rule was written for.
    assert cboe_indices.PUT_CALL_PAGE_SERIES[cs.PCR_SERIES][0] == "TOTAL PUT/CALL RATIO"
    assert cboe_indices.PUT_CALL_PAGE_SERIES[cs.EQUITY_PCR_SERIES][0] == "EQUITY PUT/CALL RATIO"
    assert tiingo_news_pull.TiingoNewsPuller.SOURCE_NAME.lower() == cs.NEWS_SOURCE
    pull_all = inspect.signature(tiingo_news_pull.TiingoNewsPuller.pull_all)
    assert pull_all.parameters["limit_per_ticker"].default == cs.NEWS_FETCH_CAP
    body = inspect.getsource(tiingo_news_pull.TiingoNewsPuller.pull_ticker_news)
    assert f"timedelta(days={cs.NEWS_TRAILING_DAYS})" in body
    assert '"sid": f"TIINGO_NEWS:{ticker}:daily_count"' in body
    assert cs.NEWS_SERIES.format(ticker="SPY") == "TIINGO_NEWS:SPY:daily_count"


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

    def fake_crowd(_conn: Any, ticker: str, *, as_of_date: date | None = None, ftd_series_ids: Any = None):
        seen.append((f"crowd:{ticker}", as_of_date))
        assert ftd_series_ids == {}  # looked up for the ticker on the engine's connection
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
    seen: list[tuple[str, date | None, Any]] = []
    lookups: list[tuple[list[str], date]] = []
    ftd_ids = {"AAA": ("sec:ftd_balance:000000AA1",)}

    def fake_ftd_ids(_conn: Any, tickers: list[str], at: date) -> dict[str, tuple[str, ...]]:
        lookups.append((list(tickers), at))
        return ftd_ids

    def fake_score_ticker(
        _engine: Any, ticker: str, *, as_of_date: date | None = None, ftd_series_ids: Any = None,
    ):
        seen.append((ticker, as_of_date, ftd_series_ids))
        score = 80 if ticker == "AAA" else 30
        return cs.ConvictionReport(ticker, score, score, (), "FIRE", "ts")

    monkeypatch.setattr(cs, "_ftd_series_ids", fake_ftd_ids)
    monkeypatch.setattr(cs, "score_ticker", fake_score_ticker)

    reports = cs.scan_all(_Engine(conn), min_score=40, as_of_date=as_of)

    call = conn.calls[0]
    assert "rs.release_date <= :as_of" in call.sql
    assert "rs.vintage_date <= :as_of" in call.sql
    assert "rs.obs_date <= :as_of" in call.sql
    assert call.params == {"as_of": as_of}
    # One FTD symbol lookup for the universe, handed to every ticker.
    assert lookups == [(["BBB", "AAA"], as_of)]
    assert seen == [("BBB", as_of, ftd_ids), ("AAA", as_of, ftd_ids)]
    assert [report.ticker for report in reports] == ["AAA"]
