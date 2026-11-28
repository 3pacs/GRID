"""Parity of the ETF flow partial-bar repair with the live writer: the E1 evidence.

``evals/e1/test_provenance.py`` (DERIVED_BASIS_WRITERS) pins this whole file,
and every ``tests.*`` module it imports, by sha256, and runs the three tests
here itself, isolated (no conftest, no pytest config, no auto-loaded
plugins). Changing anything in this file or those modules needs a new E1
release. The fixtures and helpers here are shared with
``tests/test_etf_flow_partial_bar_repair.py``.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

import scripts.repair_etf_flow_partial_bars as mod
from ingestion.altdata import institutional_flows as inst
from tests import etf_flow_repair_support as m

SESSION_DAY = date(2026, 7, 20)               # New York's date when the repair runs
FROM = date(2026, 4, 1)
SETTLED_SAME_DAY = date(2026, 5, 12)          # written after the close, volume final; dividend ex 05-15 after it
SETTLED_NEXT_MORNING = date(2026, 6, 3)       # written before the next open, volume final
SHORT_NEXT_MORNING = date(2026, 5, 14)        # written before the next open, volume 3 % short
BEFORE_RANGE = date(2026, 3, 25)              # partial, but before --from
EX_DIVIDEND_OWN_DAY = date(2026, 5, 15)       # written mid-session on its ex-dividend date
EX_DIVIDEND_AFTER_CLOSE = date(2026, 6, 18)   # written after the close on its ex-dividend date, volume final


@pytest.fixture(autouse=True)
def _one_etf(monkeypatch):
    monkeypatch.setattr(inst, "ETF_FLOW_TICKERS", ["SPY"])
    monkeypatch.setattr(inst, "_YF_RATE_DELAY", 0)
    monkeypatch.setattr(inst, "_new_york_today", lambda: SESSION_DAY)
    monkeypatch.setattr(mod, "PROVIDER_DELAY_S", 0)
    monkeypatch.setattr(mod, "_sleep", lambda _s: None)


@pytest.fixture()
def market():
    # 06-19 is Juneteenth (closed): the market follows the exchange calendar.
    return m.Market(dividends={date(2026, 5, 15): 1.5, EX_DIVIDEND_AFTER_CLOSE: 1.2})


def _seed_history(engine, market: m.Market, tickers=("SPY",)) -> dict[date, str]:
    """The stored state before the repair; returns how each date was written."""
    kinds: dict[date, str] = {}
    for ticker in tickers:
        for d in market.sessions:
            if d < date(2026, 3, 20):
                continue
            if d == EX_DIVIDEND_AFTER_CLOSE:
                m.seed_row(engine, ticker, d, m.old_writer_values(market, d, None, seen_on=d), m.ny(d, 20))
                kinds[d] = "settled_after_close_ex_dividend"
            elif d == SETTLED_SAME_DAY:
                m.seed_row(engine, ticker, d, m.old_writer_values(market, d, None, seen_on=d), m.ny(d, 20))
                kinds[d] = "settled_same_day"
            elif d == SETTLED_NEXT_MORNING:
                m.seed_row(engine, ticker, d, m.old_writer_values(market, d, None),
                           m.ny(market.next_session(d), 4))
                kinds[d] = "settled_next_morning"
            elif d == SHORT_NEXT_MORNING:
                price = float(market.raw[pd.Timestamp(d)])
                m.seed_row(engine, ticker, d, m.old_writer_values(market, d, (0.97, price)),
                           m.ny(market.next_session(d), 4))
                kinds[d] = "short_next_morning"
            else:
                price = float(market.raw[pd.Timestamp(d)]) * 1.01
                m.seed_row(engine, ticker, d, m.old_writer_values(market, d, (0.3, price), seen_on=d),
                           m.ny(d, 14, 30))
                kinds[d] = "partial_same_day"
    return kinds


@pytest.fixture()
def world(market):
    engine = m.make_sqlite_engine()
    kinds = _seed_history(engine, market)
    return engine, market, kinds


def _plan(engine, market, **kw):
    return mod.build_plan(engine, m.FakeYF(market), from_date=FROM, session_day=SESSION_DAY,
                          now=datetime(2026, 7, 20, 15, tzinfo=timezone.utc), **kw)


def _planned(plan) -> dict[tuple[str, date], dict]:
    return {(i["series_id"], date.fromisoformat(i["obs_date"])): i for i in plan["items"]}


def _bars(market):
    return mod.fetch_settled_bars(m.FakeYF(market), "SPY", date(2026, 1, 1), SESSION_DAY)



# ── The three parity tests ───────────────────────────────────────────────


def test_a_repaired_value_is_what_the_fixed_writer_stores_from_the_settled_bars(world, monkeypatch):
    """Parity with the real writer (#1496) run the morning after each date."""
    engine, market, kinds = world
    planned = _planned(_plan(engine, market))
    checked = 0
    for d in (date(2026, 4, 1), date(2026, 5, 18), date(2026, 6, 22), SHORT_NEXT_MORNING):
        fresh = m.make_sqlite_engine()
        m.install_yfinance(monkeypatch, m.FakeYF(market, asof=d))
        monkeypatch.setattr(inst, "_new_york_today", lambda d=d: market.next_session(d))
        inst.InstitutionalFlowsPuller(fresh).pull_etf_only(start_date=d - timedelta(days=10))
        for metric in mod.METRICS:
            sid = f"ETF_FLOW:SPY:{metric}"
            (writer_row,) = m.vintages(fresh, sid, d)
            assert math.isclose(planned[(sid, d)]["value"], writer_row["value"], rel_tol=1e-9), (d, metric)
            assert planned[(sid, d)]["volume"] == writer_row["payload"]["volume"]
            checked += 1
    assert checked == 12
    # The original-versus-current adjustment factor is exercised, not 1: two
    # dividends were paid after 04-01 was written, and parity still holds.
    first = planned[("ETF_FLOW:SPY:5d", date(2026, 4, 1))]
    assert abs(first["basis_raw_close"] / first["basis_adj_close_now"] - 1) > 0.01


def test_an_after_close_row_on_its_ex_dividend_date_keeps_the_day_befores_basis(world):
    """Written after the close on its ex-date with final volume, the row is right as stored: the
    provider had not yet applied that dividend. With S = the ex-date itself it would look wrong."""
    engine, market, kinds = world
    planned = _planned(_plan(engine, market))
    d = EX_DIVIDEND_AFTER_CLOSE
    assert kinds[d] == "settled_after_close_ex_dividend"
    assert not any(key[1] == d for key in planned)                      # within tolerance: left alone
    bars = _bars(market)
    pos = {x: k for k, x in enumerate(bars.index)}
    (stored,) = m.vintages(engine, "ETF_FLOW:SPY:5d", d)
    basis = mod.price_basis_session(m.ny(d, 20))
    assert basis == market.sessions[market.sessions.index(d) - 1]
    right = mod.window_value(bars, "5d", pos[d], pos[basis])
    wrong = mod.window_value(bars, "5d", pos[d], pos[d])
    assert abs(right - stored["value"]) / abs(right) <= mod.TOLERANCE
    assert abs(wrong - stored["value"]) / abs(right) > 100 * mod.TOLERANCE


def test_a_missing_exchange_session_refuses_the_etf_but_a_holiday_does_not(world):
    engine, market, _ = world
    assert date(2026, 5, 25) not in market.sessions                     # Memorial Day: closed, not a gap
    assert _plan(engine, market)["tickers"]["SPY"]["status"] == "OK"
    ordinary = date(2026, 5, 20)
    missing = mod.build_plan(engine, m.FakeYF(market, drop={ordinary}), from_date=FROM, session_day=SESSION_DAY)
    assert missing["tickers"]["SPY"]["status"] == "REFUSED"
    assert ordinary.isoformat() in missing["tickers"]["SPY"]["missing_sessions"]
    assert missing["items"] == []
    closed = mod.build_plan(engine, m.FakeYF(market, closed_day_bars={date(2026, 5, 25)}),
                            from_date=FROM, session_day=SESSION_DAY)
    assert closed["tickers"]["SPY"]["status"] == "REFUSED"
    assert closed["tickers"]["SPY"]["unexpected_sessions"] == ["2026-05-25"]
    assert closed["items"] == []
    far = mod.build_plan(engine, m.FakeYF(market, drop={date(2026, 1, 7)}), from_date=FROM, session_day=SESSION_DAY)
    assert far["tickers"]["SPY"]["status"] == "OK"                      # a bar no stored row needs
