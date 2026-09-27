"""Look-ahead regression guard for the "latest observation" readers fixed in
the F8 future-dated-rows investigation
(docs/... GRID-FUTURE-DATED-ROWS-20260927.md).

Each reader below used an unbounded ``ORDER BY <date_col> DESC [LIMIT 1]``
(or an unbounded ``MAX(obs_date)``) to find "the latest" value. A row whose
date column is in the future — a deterministic AstroGrid ephemeris row
computed out to 2026-12-31, or a gov_contract row whose performance-start
date is weeks/months after the award was reported — would then be served
as "the latest observation", which is a look-ahead bug: the reader is
reporting data that, as of ``CURRENT_DATE``, has not been observed yet.

Every test here plants a future-dated "poison" row alongside a legitimate
past-dated row for the same key, then asserts the reader:
  1. never returns the poison (future) value, and
  2. still returns the legitimate (past) value — i.e. the fix is a filter,
     not a blanket break.

This mirrors the existing disposable-schema pattern used by
tests/test_watchlist_analysis_derivatives_readonly_pg.py: a fresh schema
on GRID_TEST_DB_URL, never the shared dev database.
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

pytestmark = pytest.mark.xdist_group("postgres")

_FUTURE = date(2026, 12, 31)
_PAST = date.today() - timedelta(days=2)


@pytest.fixture(scope="module")
def guard_engine():
    """Disposable-schema engine with the minimal tables every fixed reader
    in this file touches. Skips unless GRID_TEST_DB_URL points at a local
    throwaway database (never the shared dev DB)."""
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        pytest.skip("GRID_TEST_DB_URL is required for disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("Future-dated-reads guard requires a local disposable test database")

    schema = "future_cap_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})

    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE feature_registry (id SERIAL PRIMARY KEY, name TEXT UNIQUE, "
            "model_eligible BOOLEAN DEFAULT TRUE)"
        ))
        conn.execute(text(
            "CREATE TABLE resolved_series (feature_id INTEGER, obs_date DATE, value DOUBLE PRECISION)"
        ))
        conn.execute(text(
            "CREATE TABLE wealth_flows (from_actor TEXT, to_entity TEXT, "
            "amount_estimate DOUBLE PRECISION, confidence TEXT, evidence TEXT, flow_date DATE)"
        ))
        conn.execute(text(
            "CREATE TABLE signal_data (id SERIAL PRIMARY KEY, signal_type TEXT, signal_date DATE, "
            "ticker TEXT, actor TEXT, direction TEXT, magnitude DOUBLE PRECISION, "
            "confidence TEXT, description TEXT)"
        ))
        conn.execute(text(
            "CREATE TABLE options_daily_signals (ticker TEXT, signal_date DATE, "
            "put_call_ratio DOUBLE PRECISION, spot_price DOUBLE PRECISION, "
            "max_pain DOUBLE PRECISION, iv_atm DOUBLE PRECISION)"
        ))
        conn.execute(text(
            "CREATE TABLE dollar_flows (source_type TEXT, actor_name TEXT, ticker TEXT, "
            "amount_usd DOUBLE PRECISION, direction TEXT, confidence TEXT, "
            "flow_date DATE, evidence TEXT)"
        ))

    yield engine

    engine.dispose()
    with admin.begin() as conn:
        conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    admin.dispose()


def _add_feature(engine, name: str) -> int:
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO feature_registry (name) VALUES (:n) ON CONFLICT (name) DO NOTHING"),
            {"n": name},
        )
        return conn.execute(
            text("SELECT id FROM feature_registry WHERE name = :n"), {"n": name}
        ).fetchone()[0]


# ── ollama/celestial_briefing.py — _gather_celestial_state ─────────────────

def test_celestial_briefing_ignores_future_ephemeris_row(guard_engine):
    from ollama.celestial_briefing import _gather_celestial_state

    fid = _add_feature(guard_engine, "ephemeris_lunar_phase_guard")
    with guard_engine.begin() as conn:
        conn.execute(
            text("INSERT INTO resolved_series (feature_id, obs_date, value) VALUES "
                 "(:fid, :future, 999.0), (:fid, :past, 42.0)"),
            {"fid": fid, "future": _FUTURE, "past": _PAST},
        )

    state = _gather_celestial_state(guard_engine)
    entry = state["features"].get("ephemeris_lunar_phase_guard")
    assert entry is not None, "past-dated row should still surface"
    assert entry["value"] == pytest.approx(42.0)
    assert entry["obs_date"] == str(_PAST)


# ── oracle/psi_model.py — _load_latest_value ────────────────────────────────

def test_psi_model_load_latest_value_ignores_future_row(guard_engine):
    from oracle.psi_model import _load_latest_value

    fid = _add_feature(guard_engine, "psi_guard_feature")
    with guard_engine.begin() as conn:
        conn.execute(
            text("INSERT INTO resolved_series (feature_id, obs_date, value) VALUES "
                 "(:fid, :future, 999.0), (:fid, :past, 7.5)"),
            {"fid": fid, "future": _FUTURE, "past": _PAST},
        )

    assert _load_latest_value(guard_engine, "psi_guard_feature") == pytest.approx(7.5)


# ── api/routers/astrogrid_helpers.py — _get_latest_resolved ─────────────────

def test_astrogrid_get_latest_resolved_ignores_future_row(guard_engine):
    from api.routers.astrogrid_helpers import _get_latest_resolved

    fid = _add_feature(guard_engine, "astrogrid_guard_feature")
    with guard_engine.begin() as conn:
        conn.execute(
            text("INSERT INTO resolved_series (feature_id, obs_date, value) VALUES "
                 "(:fid, :future, 999.0), (:fid, :past, 12.0)"),
            {"fid": fid, "future": _FUTURE, "past": _PAST},
        )

    value, obs_date = _get_latest_resolved(guard_engine, "astrogrid_guard_feature")
    assert value == pytest.approx(12.0)
    assert obs_date == str(_PAST)


# ── api/routers/canvas.py — _load_wealth_flows ──────────────────────────────

def test_canvas_load_wealth_flows_ignores_future_row(guard_engine):
    from api.routers.canvas import _load_wealth_flows

    with guard_engine.begin() as conn:
        conn.execute(
            text("INSERT INTO wealth_flows (from_actor, to_entity, amount_estimate, "
                 "confidence, flow_date) VALUES "
                 "('actor-guard', 'entity-future', 999.0, 'confirmed', :future), "
                 "('actor-guard', 'entity-past', 55.0, 'confirmed', :past)"),
            {"future": _FUTURE, "past": _PAST},
        )

    edges = _load_wealth_flows(guard_engine, ["actor-guard"])
    amounts_by_target = {e["target"]: e["amount"] for e in edges}
    assert amounts_by_target.get("a:entity-past") == pytest.approx(55.0)
    assert "a:entity-future" not in amounts_by_target


# ── api/routers/canvas.py — _load_signals_for_ticker ────────────────────────

def test_canvas_load_signals_for_ticker_ignores_future_row(guard_engine):
    from api.routers.canvas import _load_signals_for_ticker

    with guard_engine.begin() as conn:
        conn.execute(
            text("INSERT INTO signal_data (signal_type, signal_date, ticker, actor, "
                 "direction, magnitude, confidence, description) VALUES "
                 "('insider_buy', :future, 'GRDX', 'actor-a', 'BUY', 999.0, 'confirmed', 'future poison'), "
                 "('insider_buy', :past, 'GRDX', 'actor-b', 'BUY', 5.0, 'confirmed', 'past legit')"),
            {"future": _FUTURE, "past": _PAST},
        )

    nodes, _edges, actor_ids = _load_signals_for_ticker(guard_engine, "GRDX", None, 50)
    descriptions = {n["description"] for n in nodes}
    assert "past legit" in descriptions
    assert "future poison" not in descriptions
    assert "actor-b" in actor_ids
    assert "actor-a" not in actor_ids


# ── analysis/money_flow.py — _get_put_call_ratio ────────────────────────────

def test_money_flow_put_call_ratio_ignores_future_row(guard_engine):
    from analysis.money_flow import _get_put_call_ratio

    with guard_engine.begin() as conn:
        conn.execute(
            text("INSERT INTO options_daily_signals (ticker, signal_date, put_call_ratio) VALUES "
                 "('GRDX', :future, 9.99), ('GRDX', :past, 0.42)"),
            {"future": _FUTURE, "past": _PAST},
        )

    assert _get_put_call_ratio(guard_engine, "GRDX") == "0.42"


# ── analysis/flow_thesis_data.py — _get_dealer_gamma_state ──────────────────

def test_flow_thesis_dealer_gamma_state_ignores_future_row(guard_engine):
    from analysis.flow_thesis_data import _get_dealer_gamma_state

    with guard_engine.begin() as conn:
        conn.execute(
            text("INSERT INTO options_daily_signals (ticker, signal_date, put_call_ratio, spot_price) VALUES "
                 "('SPY', :future, 9.99, 1.0), ('SPY', :past, 0.5, 600.0)"),
            {"future": _FUTURE, "past": _PAST},
        )

    result = _get_dealer_gamma_state(guard_engine)
    assert result["value"] == pytest.approx(0.5)


# ── analysis/flow_aggregator.py — compute_flow_momentum ─────────────────────

def test_flow_aggregator_momentum_ignores_future_row(guard_engine):
    from analysis.flow_aggregator import compute_flow_momentum

    with guard_engine.begin() as conn:
        conn.execute(
            text("INSERT INTO dollar_flows (source_type, actor_name, ticker, amount_usd, "
                 "direction, confidence, flow_date) VALUES "
                 "('insider', 'actor-x', 'GRDY', 999999.0, 'BUY', 'confirmed', :future)"),
            {"future": _FUTURE},
        )

    result = compute_flow_momentum(guard_engine, "GRDY", days=30)
    assert result["flow_count"] == 0
    assert result["signal"] == "no_data"

    with guard_engine.begin() as conn:
        conn.execute(
            text("INSERT INTO dollar_flows (source_type, actor_name, ticker, amount_usd, "
                 "direction, confidence, flow_date) VALUES "
                 "('insider', 'actor-x', 'GRDY', 100.0, 'BUY', 'confirmed', :past)"),
            {"past": _PAST},
        )

    result = compute_flow_momentum(guard_engine, "GRDY", days=30)
    assert result["flow_count"] == 1


# ── api/routers/actor_detail.py — _ticker_signals ───────────────────────────

def test_actor_detail_ticker_signals_ignores_future_row(guard_engine):
    from api.routers.actor_detail import _ticker_signals

    with guard_engine.begin() as conn:
        conn.execute(
            text("INSERT INTO options_daily_signals (ticker, signal_date, put_call_ratio, iv_atm) VALUES "
                 "('GRDZ', :future, 9.99, 0.9), ('GRDZ', :past, 0.5, 0.3)"),
            {"future": _FUTURE, "past": _PAST},
        )

    sigs = _ticker_signals(guard_engine, "GRDZ")
    assert sigs.get("options_signal") == "bullish"
