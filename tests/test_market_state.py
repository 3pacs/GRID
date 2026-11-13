"""intelligence.regime.market_state: the standardized daily market-state record (market-state/1).

Fixture: real ``raw_series`` + ``source_catalog`` on in-memory SQLite, every row backfilled by one pull (the way
griddb looks), plus a few rows pulled later to prove a later publication never changes an earlier record.
"""

from __future__ import annotations

import json
import math
import sqlite3
from datetime import date, datetime, timedelta

import pandas as pd
import pytest
from sqlalchemy import Column, Date, DateTime, Float, ForeignKey, Integer, MetaData, String, Table, Text, create_engine, text

from intelligence.regime import market_state as M

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

FRED, CBOE, YF = 1, 2, 3
BACKFILL = datetime(2026, 3, 24, 6, 0, 0)
START, END = date(2019, 1, 1), date(2026, 3, 20)
AS_OF = date(2025, 6, 10)


@pytest.fixture()
def engine():
    eng = create_engine("sqlite://")
    md = MetaData()
    Table("source_catalog", md, Column("id", Integer, primary_key=True), Column("name", String, nullable=False))
    Table("raw_series", md,
          Column("series_id", String, nullable=False), Column("source_id", Integer, ForeignKey("source_catalog.id")),
          Column("obs_date", Date, nullable=False), Column("pull_timestamp", DateTime, nullable=False),
          Column("value", Float, nullable=False), Column("raw_payload", Text), Column("pull_status", String, nullable=False))
    md.create_all(eng)
    with eng.begin() as c:
        c.execute(text("INSERT INTO source_catalog (id, name) VALUES (:i, :n)"),
                  [{"i": FRED, "n": "fred"}, {"i": CBOE, "n": "CBOE"}, {"i": YF, "n": "yfinance"}])
    return eng


def _ins(engine, rows):
    with engine.begin() as c:
        c.execute(text("INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, raw_payload, pull_status) "
                       "VALUES (:s, :src, :d, :ts, :v, '{}', 'SUCCESS')"), rows)


def _bdays(a, b):
    d = a
    while d <= b:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def _months(a, b):
    y, m = a.year, a.month
    while date(y, m, 1) <= b:
        yield date(y, m, 1)
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def _seed(engine):
    rows = []
    daily = {"CBOE:VIX": (CBOE, 18.0), "CBOE:VIX3M": (CBOE, 20.0), "BAMLH0A0HYM2": (FRED, 3.5), "BAMLC0A0CM": (FRED, 1.0),
             "T10Y2Y": (FRED, 0.5), "DFF": (FRED, 2.0), "T5YIE": (FRED, 2.2), "RRPONTSYD": (FRED, 500.0),
             "YF:SPY:close": (YF, 300.0)}
    for k, (sid, (src, base)) in enumerate(daily.items()):
        for i, d in enumerate(_bdays(START, END)):
            v = base * (1 + 0.1 * math.sin(i / 23.0 + k)) + (i * 0.05 if sid == "YF:SPY:close" else 0)
            rows.append({"s": sid, "src": src, "d": d, "ts": BACKFILL, "v": v})
    d, i = START + timedelta(days=(2 - START.weekday()) % 7), 0      # Wednesdays: H.4.1
    while d <= END:
        rows.append({"s": "WALCL", "src": FRED, "d": d, "ts": BACKFILL, "v": 7_000_000 + 10_000 * math.sin(i / 7)})
        rows.append({"s": "WTREGEN", "src": FRED, "d": d, "ts": BACKFILL, "v": 600_000 + 5_000 * math.cos(i / 5)})
        rows.append({"s": "ICSA", "src": FRED, "d": d + timedelta(days=3), "ts": BACKFILL, "v": 220_000 + 3_000 * math.sin(i / 4)})
        d, i = d + timedelta(days=7), i + 1
    for k, sid in enumerate(("UNRATE", "PAYEMS", "CPIAUCSL", "INDPRO", "UMCSENT")):
        for i, m in enumerate(_months(START, END)):
            rows.append({"s": sid, "src": FRED, "d": m, "ts": BACKFILL, "v": 100 + 3 * k + 0.2 * i + math.sin(i / 3 + k)})
    _ins(engine, rows)


CAL = {"cpi": [date(2025, 6, 11), date(2025, 7, 15)], "fomc": [date(2025, 6, 18)], "nfp": [date(2025, 6, 6)]}


def test_record_is_complete_and_every_reading_is_standardized(engine):
    _seed(engine)
    st = M.compute_market_state(engine, AS_OF, calendar=CAL).to_dict()
    assert st["rule"] == "market-state/1" and st["baseline_rule"] == "trailing-robust/1"
    missing = {k: v.get("reason") for k, v in st["variables"].items() if v["status"] != "ok"}
    assert missing == {"short_volume_ratio": "no observation known at as_of"}
    for k, v in st["variables"].items():
        if v["status"] == "ok":
            assert {"value", "median", "robust_z", "percentile", "label", "n", "obs_date"} <= set(v), k
            assert date.fromisoformat(v["obs_date"]) <= AS_OF, k
    assert st["spy_basis"] == "YF:SPY:close"
    assert set(st["pending"]) >= {"news_intensity", "release_surprise", "dix", "dealer_gamma"}


def test_monthly_value_waits_for_its_publication_lag(engine):
    _seed(engine)
    v = M.compute_market_state(engine, AS_OF, calendar=CAL).variables
    # CPI (50-day lag): May 2025 is not public by 2025-06-10, April 2025 is.
    assert v["cpi_yoy"]["obs_date"] == "2025-04-01"
    # Employment Situation (42-day lag): April 2025 is public by 2025-06-10.
    assert v["unemployment"]["obs_date"] == "2025-04-01"


def test_a_value_published_after_as_of_never_changes_the_record(engine):
    _seed(engine)
    before = M.compute_market_state(engine, AS_OF, calendar=CAL).to_dict()
    # A late correction of an old VIX close and a brand-new month of CPI, both pulled after AS_OF.
    late = datetime(2026, 4, 1, 12, 0)
    _ins(engine, [{"s": "CBOE:VIX", "src": CBOE, "d": AS_OF - timedelta(days=1), "ts": late, "v": 99.0},
                  {"s": "CPIAUCSL", "src": FRED, "d": date(2025, 6, 1), "ts": late, "v": 999.0}])
    after = M.compute_market_state(engine, AS_OF, calendar=CAL).to_dict()
    assert json.dumps(before, sort_keys=True) == json.dumps(after, sort_keys=True)


def test_net_liquidity_is_walcl_minus_tga_minus_rrp_in_millions_as_of_each_walcl_date():
    walcl = pd.Series({date(2025, 1, 1): 7_000_000.0, date(2025, 1, 8): 7_010_000.0})
    tga = pd.Series({date(2025, 1, 1): 700_000.0, date(2025, 1, 8): 650_000.0})
    rrp = pd.Series({date(2024, 12, 31): 100.0, date(2025, 1, 7): 120.0, date(2025, 1, 9): 999.0})
    nl = M.net_liquidity(walcl, tga, rrp)
    assert nl[date(2025, 1, 1)] == pytest.approx(7_000_000 - 700_000 - 100 * 1000)
    assert nl[date(2025, 1, 8)] == pytest.approx(7_010_000 - 650_000 - 120 * 1000)   # 01-09 RRP is after 01-08
    assert M.net_liquidity(walcl, pd.Series(dtype=float), rrp).empty


def test_events_today_next_and_coverage():
    ev = M.events_state(date(2025, 6, 11), CAL)
    assert ev["today"] == ["cpi"] and ev["scheduled_day"] is True
    assert ev["next"] == {"event": "fomc", "date": "2025-06-18", "days_away": 7}
    assert [x["event"] for x in ev["within_14d"]] == ["fomc"]
    assert ev["status"] == "ok"
    assert M.events_state(date(2025, 8, 1), CAL)["status"] == "stale_calendar"
    assert M.events_state(date(2025, 6, 11), {})["status"] == "unavailable"


def test_shipped_calendar_has_every_event_and_known_shutdown_dates():
    cal = M.load_calendar()
    assert set(cal) >= {"cpi", "ppi", "nfp", "gdp", "pce", "retail_sales", "fomc"}
    assert date(2025, 10, 24) in cal["cpi"] and date(2025, 10, 15) not in cal["cpi"]
    assert date(2025, 11, 20) in cal["nfp"] and date(2025, 10, 3) not in cal["nfp"]
    per_year = pd.Series([d.year for d in cal["fomc"] if 2011 <= d.year <= 2025]).value_counts()
    assert set(per_year.drop(2020, errors="ignore")) == {8} and per_year.get(2020) == 7


def test_storage_is_insert_only_unless_replace(engine):
    _seed(engine)
    st = M.compute_market_state(engine, AS_OF, calendar=CAL)
    assert M.store_market_state(engine, st) is True
    assert M.store_market_state(engine, st) is False
    assert M.store_market_state(engine, st, replace=True) is True
    with engine.connect() as c:
        rows = c.execute(text("SELECT as_of_date, rule, record FROM market_state_daily")).fetchall()
    assert len(rows) == 1 and rows[0][1] == "market-state/1"
    assert json.loads(rows[0][2])["as_of"] == AS_OF.isoformat()
