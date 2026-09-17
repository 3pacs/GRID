"""End-to-end regression: migrated readers ignore FAILED zeros and duplicate vintages.

Runs the migrated module-level read functions against an in-memory SQLite
``raw_series`` (real SQL, no mocks) seeded with the production failure
shapes measured on griddb 2026-09-17:

* a FAILED marker row (``value 0``) dated the pull day, newer than the last
  real observation (WALCL, T10Y2Y, cftc.*.net_speculative);
* two SUCCESS vintages for one obs_date (``COMPUTED:fed_net_liquidity``,
  RRPONTSYD);
* a series stored under its bare FRED id (``T10Y2Y``) while a reader asked
  for ``FRED:T10Y2Y``.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
)

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

TODAY = date.today()
T0 = datetime(2026, 1, 1, 6, 0, 0)


def _engine(rows):
    engine = create_engine("sqlite://")
    md = MetaData()
    raw = Table(
        "raw_series", md,
        Column("series_id", String, nullable=False),
        Column("source_id", String, nullable=False),
        Column("obs_date", Date, nullable=False),
        Column("pull_timestamp", DateTime, nullable=False),
        Column("value", Float, nullable=False),
        Column("raw_payload", Text),
        Column("pull_status", String, nullable=False),
    )
    md.create_all(engine)
    with engine.begin() as c:
        c.execute(raw.insert(), rows)
    return engine


def _row(sid, d, v, status="SUCCESS", h=0):
    return {
        "series_id": sid, "source_id": "t", "obs_date": d,
        "pull_timestamp": T0 + timedelta(hours=h), "value": v,
        "raw_payload": "{}", "pull_status": status,
    }


def _daily(sid, n, start_value=100.0, step=1.0, days_back=0):
    """n daily SUCCESS rows ending ``days_back`` days before today."""
    out = []
    for i in range(n):
        d = TODAY - timedelta(days=days_back + (n - 1 - i))
        out.append(_row(sid, d, start_value + i * step, h=i))
    return out


# ── liquidity regime ─────────────────────────────────────────────────────

def test_liquidity_regime_read_series_drops_failed_zero_and_duplicate_vintage():
    from intelligence.liquidity_regime import _read_series

    sid = "COMPUTED:fed_net_liquidity"
    rows = _daily(sid, 5, start_value=5_800_000, step=1_000, days_back=1)
    rows.append(_row(sid, TODAY - timedelta(days=1), 5_857_278.6, h=100))  # second vintage, later pull
    rows.append(_row(sid, TODAY, 0.0, status="FAILED", h=101))
    got = _read_series(_engine(rows), sid, lookback_days=30)

    assert all(v != 0.0 for _, v in got), "FAILED zero leaked into the regime input"
    dates = [d for d, _ in got]
    assert len(dates) == len(set(dates)), "duplicate vintage leaked in"
    assert got[-1] == (TODAY - timedelta(days=1), 5_857_278.6)  # latest vintage wins


# ── CoT extremes ─────────────────────────────────────────────────────────

def test_cot_extremes_history_drops_failed_zero():
    from intelligence.cot_extremes import _read_series_history

    sid = "cftc.SP500.net_speculative"
    rows = [_row(sid, TODAY - timedelta(weeks=k), 100_000 + k * 500, h=k) for k in range(20, 0, -1)]
    rows.append(_row(sid, TODAY, 0.0, status="FAILED", h=99))
    got = _read_series_history(_engine(rows), sid, lookback_weeks=52)

    assert got and got[-1][1] != 0.0
    assert all(v > 0 for _, v in got)


# ── FCI ──────────────────────────────────────────────────────────────────

def test_fci_history_drops_failed_zero_and_partial():
    from intelligence.financial_conditions_index import _read_series_history

    sid = "BAMLH0A0HYM2"
    rows = _daily(sid, 10, start_value=3.0, step=0.01, days_back=1)
    rows.append(_row(sid, TODAY, 0.0, status="FAILED", h=50))
    rows.append(_row(sid, TODAY, 9.9, status="PARTIAL", h=51))
    got = _read_series_history(_engine(rows), sid, lookback_days=60)

    assert 0.0 not in got and 9.9 not in got
    assert len(got) == 10


# ── sentiment scorer ─────────────────────────────────────────────────────

def test_sentiment_yield_curve_reads_bare_fred_id_and_ignores_failed_zero():
    from intelligence.sentiment_scorer import _score_yield_curve

    rows = _daily("T10Y2Y", 3, start_value=0.50, step=0.01, days_back=1)
    rows.append(_row("T10Y2Y", TODAY, 0.0, status="FAILED", h=50))  # today's failed pull
    comp = _score_yield_curve(_engine(rows))

    assert comp.detail != "No data available"
    assert comp.raw_value == pytest.approx(0.52)
    assert comp.score > 0  # a 0 would have scored the curve as flat


def test_sentiment_yield_curve_with_only_failed_rows_reports_no_data():
    from intelligence.sentiment_scorer import _score_yield_curve

    comp = _score_yield_curve(_engine([_row("T10Y2Y", TODAY, 0.0, status="FAILED")]))
    assert comp.detail == "No data available" and comp.score == 0.0


def test_sentiment_volatility_ignores_failed_zero_vix():
    from intelligence.sentiment_scorer import _score_volatility

    rows = _daily("YF:^VIX:close", 3, start_value=18.0, step=0.5, days_back=1)
    rows.append(_row("YF:^VIX:close", TODAY, 0.0, status="FAILED", h=50))
    comp = _score_volatility(_engine(rows))

    assert comp.raw_value == pytest.approx(19.0)
    assert "VIX=19.0" in comp.detail


def test_sentiment_momentum_uses_distinct_dates_not_rows():
    """Two vintages of one close must not count as two trading days."""
    from intelligence.sentiment_scorer import _score_momentum

    sid = "YF:^GSPC:close"
    rows = _daily(sid, 30, start_value=100.0, step=1.0, days_back=0)
    # duplicate vintage for the newest date with the same value
    rows.append(_row(sid, TODAY, 129.0, h=200))
    comp = _score_momentum(_engine(rows))

    # 5 distinct days back from 129 is 124 -> +4.03%; a row-based read would
    # have used 125 (only 4 real days back).
    assert "5d=+4.03%" in comp.detail


# ── market diary ─────────────────────────────────────────────────────────

def test_market_diary_index_moves_skip_failed_zero():
    from intelligence.market_diary import _gather_market_moves

    sid = "YF:^GSPC:close"
    rows = _daily(sid, 3, start_value=5000.0, step=10.0, days_back=1)
    rows.append(_row(sid, TODAY, 0.0, status="FAILED", h=50))
    moves = _gather_market_moves(_engine(rows), TODAY)

    sp = moves["indices"].get("S&P 500")
    assert sp is not None
    assert sp["close"] == 5020.0 and sp["change"] == 10.0


# ── crowdedness / credit event ───────────────────────────────────────────

def test_short_interest_and_total_debt_are_none_when_only_failed_rows_exist():
    from intelligence.consensus_crowdedness import _read_short_interest
    from intelligence.credit_event_probability import _read_total_debt

    eng = _engine([
        _row("finra_short_interest:NVDA", TODAY, 0.0, status="FAILED"),
        _row("sec_xbrl:NVDA:total_debt", TODAY, 0.0, status="FAILED"),
    ])
    assert _read_short_interest(eng, "nvda") is None
    assert _read_total_debt(eng, "nvda") is None
