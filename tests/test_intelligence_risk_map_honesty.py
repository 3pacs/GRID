"""Honesty tests for ``GET /api/v1/intelligence/risk-map``.

Before this change every sub-system of ``_build_risk_map`` fell back to
literal placeholders when its query failed or returned nothing (VIX 20.0
at the 50th percentile, HY 400bp / IG 100bp / TED 0.30, average
correlation 0.5, ``risk_level: "moderate"``), ``realized_vs_implied`` was
the constant ``vix / (0.9 * vix)``, and a builder failure returned
``overall_risk_score: 0.5`` with "moderate" everywhere. None of that was
distinguishable from a real reading.

These tests pin the honest contract:

* a sub-system with no data is ``{"risk_level": "unknown",
  "available": False, "reason": ...}`` with no numeric fields;
* ``overall_risk_score`` is ``None`` when nothing could be measured and
  averages only the measured sub-systems otherwise;
* a builder failure is HTTP 503, not a placeholder payload;
* a measured sub-system reports ``available: True`` and real numbers.

No database is touched: the engine is a ``MagicMock`` and the dealer
gamma engine is stubbed.
"""

from __future__ import annotations

import asyncio
import sys
import types
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from api.routers import intelligence_risk as ir

SUBSYSTEMS = (
    "dealer_risk",
    "volatility_risk",
    "concentration_risk",
    "correlation_risk",
    "credit_risk",
    "liquidity_risk",
)


def _engine(side_effect):
    """Mock engine whose ``conn.execute(stmt, params)`` delegates to
    ``side_effect(sql_text, params)``, which returns a MagicMock that
    supports ``fetchone`` / ``fetchall``."""
    engine = MagicMock()
    conn = MagicMock()

    def execute(stmt, params=None, *args, **kwargs):
        sql = str(getattr(stmt, "text", stmt))
        return side_effect(sql, params or {})

    conn.execute.side_effect = execute
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine


def _res(rows=None, one=None):
    m = MagicMock()
    m.fetchall.return_value = rows if rows is not None else []
    m.fetchone.return_value = one if one is not None else (rows[0] if rows else None)
    return m


def _stub_dealer_gamma(monkeypatch, profile):
    """Install a fake ``physics.dealer_gamma`` whose engine returns ``profile``."""
    mod = types.ModuleType("physics.dealer_gamma")

    class DealerGammaEngine:  # test stub
        def __init__(self, engine):
            self.engine = engine

        def compute_gex_profile(self, ticker):
            return profile

    mod.DealerGammaEngine = DealerGammaEngine
    monkeypatch.setitem(sys.modules, "physics.dealer_gamma", mod)


@pytest.fixture(autouse=True)
def _clear_cache():
    ir._risk_map_cache.clear()
    yield
    ir._risk_map_cache.clear()


def test_every_subsystem_unavailable_when_nothing_is_measurable(monkeypatch):
    """Empty tables everywhere -> six honest unavailable blocks, null overall."""
    _stub_dealer_gamma(monkeypatch, {"error": "no options chain for SPY"})
    monkeypatch.setattr(ir, "get_db_engine", lambda: _engine(lambda sql, p: _res()))

    result = ir._build_risk_map()

    for key in SUBSYSTEMS:
        block = result[key]
        assert block["available"] is False, key
        assert block["risk_level"] == "unknown", key
        assert block["reason"], key
        # No placeholder numbers survive an unavailable read.
        numeric = {k: v for k, v in block.items() if isinstance(v, (int, float)) and not isinstance(v, bool)}
        assert numeric == {}, (key, numeric)

    assert result["overall_risk_score"] is None
    assert result["available_subsystems"] == 0
    assert len(result["unavailable_subsystems"]) == 6
    assert len(result["errors"]) == 6
    assert "unavailable" in result["risk_narrative"].lower()

    # The retired literals must not appear anywhere in the payload.
    flat = repr(result)
    for literal in ("20.0", "400", "'ig_spread': 100", "0.3", "'avg_cross_correlation': 0.5", "'overall_risk_score': 0.5"):
        assert literal not in flat, literal


def test_measured_subsystems_carry_real_numbers_and_overall_averages_only_them(monkeypatch):
    """VIX + HY present, everything else absent -> two measured blocks,
    overall is their mean, the other four are unavailable."""
    _stub_dealer_gamma(monkeypatch, {"error": "no chain"})

    def side_effect(sql, params):
        n = (params or {}).get("n", "")
        if n == "%vix%close%":
            if "INTERVAL '365 days'" in sql:
                # 10 readings, current 30 sits above 8 of them -> 90th pct
                return _res(rows=[(v,) for v in (12, 14, 15, 16, 18, 19, 20, 22, 30, 35)])
            return _res(one=(30.0, None))
        if n == "%hy%spread%":
            if "INTERVAL '30 days'" in sql:
                return _res(one=(300.0,))
            return _res(one=(400.0,))
        return _res()

    monkeypatch.setattr(ir, "get_db_engine", lambda: _engine(side_effect))
    result = ir._build_risk_map()

    vol = result["volatility_risk"]
    assert vol["available"] is True
    assert vol["vix"] == 30.0
    assert vol["vix_percentile_1y"] == 90
    assert vol["vix_percentile_sample"] == 10
    # No realized-vol series is read; the old constant 1.11 is gone.
    assert vol["realized_vs_implied"] is None

    credit = result["credit_risk"]
    assert credit["available"] is True
    assert credit["hy_spread"] == 400
    assert credit["ig_spread"] is None and credit["ted_spread"] is None
    assert credit["spread_direction"] == "widening"

    for key in ("dealer_risk", "concentration_risk", "correlation_risk", "liquidity_risk"):
        assert result[key]["available"] is False, key
        assert result[key]["risk_level"] == "unknown", key

    # overall = mean(v_score=0.9, cr_score=min(400/800,1)+0.15=0.65) = 0.775
    assert result["overall_risk_score"] == pytest.approx(0.78, abs=0.005)
    assert result["available_subsystems"] == 2
    assert sorted(result["unavailable_subsystems"]) == sorted(
        ["Dealer positioning", "Concentration", "Correlation", "Liquidity"]
    )


def test_builder_failure_is_503_not_a_placeholder_payload(monkeypatch):
    def boom():
        raise RuntimeError("database gone")

    monkeypatch.setattr(ir, "get_db_engine", boom)

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(ir.get_risk_map("test-token"))
    assert excinfo.value.status_code == 503
    assert "database gone" in str(excinfo.value.detail)
    # Nothing was cached for the next caller.
    assert ir._risk_map_cache.get(ir._RISK_MAP_CACHE_KEY) is None


def test_source_has_no_default_literals_left():
    """Regression guard on the source text: the old defaults must not reappear."""
    import inspect

    src = inspect.getsource(ir._build_risk_map)
    for banned in ("else 20.0", "else 400", "else 100", "else 0.3", "pct = 50", "vix_val * 0.9"):
        assert banned not in src, banned


def test_fully_unavailable_result_is_served_but_not_cached(monkeypatch):
    """No sub-system measured -> honest payload now, no 5-minute pin."""
    _stub_dealer_gamma(monkeypatch, {"error": "no chain"})
    monkeypatch.setattr(ir, "get_db_engine", lambda: _engine(lambda sql, p: _res()))

    result = asyncio.run(ir.get_risk_map("test-token"))

    assert result["overall_risk_score"] is None
    assert result["available_subsystems"] == 0
    assert ir._risk_map_cache.get(ir._RISK_MAP_CACHE_KEY) is None


def test_partially_measured_result_is_cached(monkeypatch):
    _stub_dealer_gamma(monkeypatch, {"error": "no chain"})

    def side_effect(sql, params):
        if (params or {}).get("n") == "%hy%spread%":
            return _res(one=(350.0,))
        return _res()

    monkeypatch.setattr(ir, "get_db_engine", lambda: _engine(side_effect))
    result = asyncio.run(ir.get_risk_map("test-token"))

    assert result["available_subsystems"] == 1
    assert ir._risk_map_cache.get(ir._RISK_MAP_CACHE_KEY) is result

