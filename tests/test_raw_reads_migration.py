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


def _engine(rows, *, preserve_capture_offsets=False):
    engine = create_engine("sqlite://")
    md = MetaData()
    source_catalog = Table(
        "source_catalog", md,
        Column("id", String, primary_key=True),
        Column("name", String, nullable=False),
    )
    raw = Table(
        "raw_series", md,
        Column("series_id", String, nullable=False),
        Column("source_id", String, nullable=False),
        Column("obs_date", Date, nullable=False),
        Column("pull_timestamp", Text if preserve_capture_offsets else DateTime, nullable=False),
        Column("value", Float, nullable=False),
        Column("raw_payload", Text),
        Column("pull_status", String, nullable=False),
    )
    md.create_all(engine)
    # Every row in this file's fixtures uses source_id="t" (a single stand-in
    # source) unless a test overrides it — store/observations.py now joins
    # source_catalog for provenance and mixed-source detection.
    source_ids = sorted({r["source_id"] for r in rows}) or ["yfinance"]
    with engine.begin() as c:
        c.execute(source_catalog.insert(), [{"id": s, "name": s} for s in source_ids])
        c.execute(raw.insert(), rows)
    return engine


def _row(sid, d, v, status="SUCCESS", h=0, source_id="yfinance"):
    # Every fixture in this file is single-source; "yfinance" also satisfies
    # the callers that now pass source="yfinance" explicitly for YF:* ids
    # (intelligence/sentiment_scorer.py, intelligence/market_diary.py).
    return {
        "series_id": sid, "source_id": source_id, "obs_date": d,
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

    sid = "cftc.13874A.net_speculative"
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


def _policy_yf_rows(sid, n, start_value=100.0, step=1.0, end_date=date(2026, 11, 20)):
    """``n`` policy-eligible NYSE trading-day rows ending on/before ``end_date``."""
    import json
    from datetime import timezone
    from ingestion.market_calendar import trading_days_between
    from price_close_contract import daily_capture_payload

    ticker, field = sid.split(":")[1], sid.split(":")[2]
    all_days = trading_days_between(date(2026, 1, 2), end_date)
    days = all_days[-n:]
    out = []
    for i, d in enumerate(days):
        captured_at = datetime(d.year, d.month, d.day, 6, tzinfo=timezone.utc) + timedelta(days=1)
        row = _row(sid, d, start_value + i * step, h=i)
        row["pull_timestamp"] = captured_at.isoformat(sep=" ")
        row["raw_payload"] = json.dumps(daily_capture_payload(ticker, field, d, captured_at))
        out.append(row)
    return out


def _pin_sentiment_now(monkeypatch, now_utc):
    from intelligence import sentiment_scorer

    class _FrozenDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return now_utc if tz is not None else now_utc.replace(tzinfo=None)

    monkeypatch.setattr(sentiment_scorer, "datetime", _FrozenDT)


def test_sentiment_volatility_ignores_failed_zero_vix(monkeypatch):
    import json
    from datetime import timezone
    from price_close_contract import daily_capture_payload
    from intelligence.sentiment_scorer import _score_volatility

    ref = date(2026, 11, 20)
    _pin_sentiment_now(monkeypatch, datetime(2026, 11, 21, 12, tzinfo=timezone.utc))
    rows = _policy_yf_rows("YF:^VIX:close", 3, start_value=18.0, step=0.5, end_date=date(2026, 11, 19))
    failed_at = datetime(2026, 11, 21, 6, tzinfo=timezone.utc)
    failed = _row("YF:^VIX:close", ref, 0.0, status="FAILED", h=50)
    failed["pull_timestamp"] = failed_at.isoformat(sep=" ")
    failed["raw_payload"] = json.dumps(daily_capture_payload("^VIX", "close", ref, failed_at))
    rows.append(failed)
    comp = _score_volatility(_engine(rows, preserve_capture_offsets=True))

    assert comp.raw_value == pytest.approx(19.0)
    assert "VIX=19.0" in comp.detail


def test_sentiment_momentum_uses_distinct_dates_not_rows(monkeypatch):
    """Two vintages of one close must not count as two trading days."""
    import json
    from datetime import timezone
    from price_close_contract import daily_capture_payload
    from intelligence.sentiment_scorer import _score_momentum

    sid = "YF:^GSPC:close"
    _pin_sentiment_now(monkeypatch, datetime(2026, 11, 21, 12, tzinfo=timezone.utc))
    rows = _policy_yf_rows(sid, 30, start_value=100.0, step=1.0, end_date=date(2026, 11, 20))
    # duplicate policy-eligible vintage for the newest date with the same value
    newest_d = rows[-1]["obs_date"]
    dup_ts = datetime(newest_d.year, newest_d.month, newest_d.day, 10, tzinfo=timezone.utc) + timedelta(days=1)
    dup = _row(sid, newest_d, 129.0, h=200)
    dup["pull_timestamp"] = dup_ts.isoformat(sep=" ")
    dup["raw_payload"] = json.dumps(daily_capture_payload("^GSPC", "close", newest_d, dup_ts))
    rows.append(dup)
    comp = _score_momentum(_engine(rows, preserve_capture_offsets=True))

    # 5 distinct trading days back from 129 is 124 -> +4.03%; a row-based read would
    # have used 125 (only 4 real days back).
    assert "5d=+4.03%" in comp.detail


def test_sentiment_reads_are_bounded_by_a_start_date(monkeypatch):
    """Every price read passes a start bound (no full-history scan)."""
    from datetime import timezone
    from intelligence import sentiment_scorer

    _pin_sentiment_now(monkeypatch, datetime(2026, 11, 21, 12, tzinfo=timezone.utc))
    seen = []
    real = sentiment_scorer.read_daily_policy_n

    def spy(conn, series_id, n, **kw):
        seen.append((series_id, n, kw.get("start")))
        return real(conn, series_id, n, **kw)

    monkeypatch.setattr(sentiment_scorer, "read_daily_policy_n", spy)
    engine = _engine(_policy_yf_rows("YF:^GSPC:close", 30), preserve_capture_offsets=True)
    sentiment_scorer._score_momentum(engine)
    sentiment_scorer._score_trend(engine)
    sentiment_scorer._score_breadth(engine)
    sentiment_scorer._score_credit_spread(engine)

    assert seen and all(start is not None for _, _, start in seen)
    today = sentiment_scorer.datetime.now(timezone.utc).date()
    for _, n, start in seen:
        assert (today - start).days >= n  # window always spans >= n calendar days


def test_sentiment_trend_still_sees_200_sessions_inside_the_bound(monkeypatch):
    from datetime import timezone
    from intelligence.sentiment_scorer import _recent_closes

    sid = "YF:^GSPC:close"
    _pin_sentiment_now(monkeypatch, datetime(2026, 11, 21, 12, tzinfo=timezone.utc))
    engine = _engine(_policy_yf_rows(sid, 204, end_date=date(2026, 11, 20)), preserve_capture_offsets=True)
    with engine.connect() as conn:
        assert len(_recent_closes(conn, sid, 200)) == 200


def test_sentiment_bound_excludes_closes_older_than_the_window(monkeypatch):
    from datetime import timezone
    from intelligence.sentiment_scorer import _recent_closes

    sid = "YF:HYG:close"
    _pin_sentiment_now(monkeypatch, datetime(2026, 11, 21, 12, tzinfo=timezone.utc))
    rows = _policy_yf_rows(sid, 3, end_date=date(2026, 11, 20))
    # Old 2026 policy-eligible close well outside the 21-session window.
    old_rows = _policy_yf_rows(sid, 1, start_value=50.0, end_date=date(2026, 1, 5))
    with _engine(rows + old_rows, preserve_capture_offsets=True).connect() as conn:
        got = _recent_closes(conn, sid, 21)
    assert len(got) == 3 and 50.0 not in [o.value for o in got]


# ── market diary ─────────────────────────────────────────────────────────

def test_market_diary_index_moves_skip_failed_zero(monkeypatch):
    import json
    from datetime import timezone
    from price_close_contract import daily_capture_payload
    from intelligence import market_diary
    from intelligence.market_diary import _gather_market_moves

    # Price reads are held behind GRID_MARKET_DIARY_PRICES_ENABLED (Wave 3
    # §4.1 fix #2) pending operator confirmation of the YF quarantine; flip
    # it on for this test so the FAILED-zero-skipping behaviour under test
    # is still exercised.
    monkeypatch.setattr(market_diary, "PRICES_ENABLED", True)

    # Fix this function's fixture day; other raw-reader tests keep module TODAY.
    TODAY = date(2026, 9, 25)  # Friday; the three price labels are supported sessions.
    sid = "YF:^GSPC:close"
    # A real close exists for TODAY (5020) alongside a same-day FAILED zero
    # (e.g. an earlier failed attempt superseded by a later successful
    # pull) -- the policy-selected reader excludes FAILED regardless of pull_timestamp,
    # so the real close must still win and obs_date must still equal
    # target_date (Wave 3 §4.1 fix #2: a stale/failed close must never be
    # reported as today's move).
    rows = [_row(sid, TODAY - timedelta(days=2 - i), 5000.0 + i * 10.0) for i in range(3)]
    for row in rows:
        d = row["obs_date"]
        captured_at = datetime(d.year, d.month, d.day, 6, tzinfo=timezone.utc) + timedelta(days=2)
        row["pull_timestamp"] = captured_at.isoformat(sep=" ")
        row["raw_payload"] = json.dumps(daily_capture_payload("^GSPC", "close", d, captured_at))
    failed = _row(sid, TODAY, 0.0, status="FAILED", h=50)
    failed_at = datetime(2026, 9, 29, 6, tzinfo=timezone.utc)
    failed["pull_timestamp"] = failed_at.isoformat(sep=" ")
    # Otherwise eligible: only FAILED status must exclude this newer zero.
    failed["raw_payload"] = json.dumps(daily_capture_payload("^GSPC", "close", TODAY, failed_at))
    monkeypatch.setitem(
        sqlite3.adapters, (datetime, sqlite3.PrepareProtocol),
        lambda d: d.isoformat(sep=" "),
    )
    rows.append(failed)
    moves = _gather_market_moves(_engine(rows, preserve_capture_offsets=True), TODAY)

    sp = moves["indices"].get("S&P 500")
    assert sp is not None
    assert sp["close"] == 5020.0 and sp["change"] == 10.0
    assert sp["obs_date"] == TODAY.isoformat()


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
