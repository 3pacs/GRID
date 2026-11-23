"""Honesty tests for ``GET /api/v1/intelligence/risk-map``.

Before an earlier change, every sub-system of ``_build_risk_map`` fell back
to literal placeholders when its query failed or returned nothing (VIX
20.0 at the 50th percentile, HY 400bp / IG 100bp / TED 0.30, average
correlation 0.5, ``risk_level: "moderate"``). ``realized_vs_implied`` was
the constant ``vix / (0.9 * vix)``, and a builder failure returned
``overall_risk_score: 0.5`` with "moderate" everywhere. None of that could
be told apart from a real reading.

Until 2026-11-23 the series inputs were ILIKE patterns, and each matched
several features. '%hy%spread%' took whichever of hy_oas_spread,
hy_spread_3m_chg, hy_spread_proxy and hy_spread was newest. '%tga%' took
tga_drawdown, a 30-day change. '%vix%close%' matched nothing. HY and IG
OAS are stored in percent but were scored and served as basis points.

These tests pin the contract:

* a sub-system with no data is ``{"risk_level": "unknown",
  "available": False, "reason": ...}`` with no numeric fields;
* each input reads one exact feature through ``PITStore.get_pit``
  (LATEST_AS_OF as of today). An input whose latest reading is older
  than ``_MAX_AGE_DAYS`` counts as missing;
* HY / IG are served and scored in basis points, and the Fed-assets drop
  threshold is in millions USD;
* ``overall_risk_score`` is ``None`` when nothing could be measured, and
  otherwise averages only the measured sub-systems;
* a builder failure is HTTP 503, not a placeholder payload.

No database is touched: ``PITStore`` is replaced by an in-memory fake, the
engine used by the concentration / correlation reads is a ``MagicMock``,
and the dealer gamma engine is stubbed.
``tests/test_intelligence_risk_map_series_pg.py`` runs the same build
against real SQL with decoy features.
"""

from __future__ import annotations

import asyncio
import inspect
import sys
import types
from datetime import date, timedelta
from unittest.mock import MagicMock

import pandas as pd
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
EXACT_INPUTS = {
    "vix_spot", "hy_oas_spread", "ig_oas_spread",
    "fed_balance_sheet", "reverse_repo_usage", "treasury_general_acct",
}
TODAY = date.today()


def _ago(days: int) -> date:
    return TODAY - timedelta(days=days)


def _engine(side_effect=None):
    """Mock engine whose ``conn.execute(stmt, params)`` delegates to
    ``side_effect(sql_text, params)``. The default returns no rows."""
    side_effect = side_effect or (lambda sql, p: _res())
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


class _FakePIT:
    """In-memory ``store.pit.PITStore``. Exact names map to ids, and
    ``get_pit`` returns one row per obs_date in the window."""

    def __init__(self, data: dict[str, list[tuple[date, float]]], *, fail: Exception | None = None):
        self.data = data
        self.fail = fail
        self.names_asked: list[str] | None = None
        self.calls: list[dict] = []

    def __call__(self, engine):  # stands in for the PITStore constructor
        return self

    def feature_ids_by_name(self, names):
        if self.fail is not None:
            raise self.fail
        self.names_asked = list(names)
        ordered = sorted(self.data)
        return {n: ordered.index(n) + 1 for n in names if n in self.data}

    def get_pit(self, feature_ids, as_of_date, vintage_policy="LATEST_AS_OF", *,
                start_date=None, end_date=None):
        self.calls.append({"fids": list(feature_ids), "as_of": as_of_date,
                           "policy": vintage_policy, "start": start_date, "end": end_date})
        ordered = sorted(self.data)
        rows = []
        for fid in feature_ids:
            for obs, value in self.data[ordered[fid - 1]]:
                if (start_date is None or obs >= start_date) and obs <= as_of_date:
                    rows.append((fid, obs, value, obs, obs))
        return pd.DataFrame(rows, columns=["feature_id", "obs_date", "value", "release_date", "vintage_date"])


def _install(monkeypatch, data=None, *, fail=None, engine=None, gex=None) -> _FakePIT:
    fake = _FakePIT(data or {}, fail=fail)
    monkeypatch.setattr(ir, "PITStore", fake)
    monkeypatch.setattr(ir, "get_db_engine", lambda: engine or _engine())
    _stub_dealer_gamma(monkeypatch, gex or {"error": "no options chain for SPY"})
    return fake


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


def _numeric(block: dict) -> dict:
    return {k: v for k, v in block.items() if isinstance(v, (int, float)) and not isinstance(v, bool)}


# griddb readings, 2026-11-23 (percent for OAS, millions USD for WALCL / WTREGEN,
# billions for RRP), placed at the same distance from today.
GRIDDB_LIKE = {
    "vix_spot": [(_ago(0), 30.24)],
    "hy_oas_spread": [(_ago(33), 3.38), (_ago(3), 3.76)],
    "ig_oas_spread": [(_ago(33), 0.91), (_ago(3), 0.98)],
    "fed_balance_sheet": [(_ago(33), 6_753_604.0), (_ago(5), 6_741_452.0)],
    "treasury_general_acct": [(_ago(33), 952_002.0), (_ago(5), 916_613.0)],
    "reverse_repo_usage": [(_ago(31), 0.20), (_ago(0), 0.35)],
}


def test_every_subsystem_unavailable_when_nothing_is_measurable(monkeypatch):
    """No series and empty tables -> six honest unavailable blocks, null overall."""
    _install(monkeypatch)

    result = ir._build_risk_map()

    for key in SUBSYSTEMS:
        block = result[key]
        assert block["available"] is False, key
        assert block["risk_level"] == "unknown", key
        assert block["reason"], key
        # No placeholder numbers survive an unavailable read.
        assert _numeric(block) == {}, (key, _numeric(block))

    assert result["overall_risk_score"] is None
    assert result["available_subsystems"] == 0
    assert len(result["unavailable_subsystems"]) == 6
    assert len(result["errors"]) == 6
    assert "unavailable" in result["risk_narrative"].lower()
    assert result["volatility_risk"]["reason"] == "no vix_spot rows in resolved_series"
    assert result["credit_risk"]["reason"] == "no hy_oas_spread rows in resolved_series"
    assert result["liquidity_risk"]["reason"] == "no fed_balance_sheet rows in resolved_series"


def test_inputs_are_exact_features_read_latest_as_of_today(monkeypatch):
    fake = _install(monkeypatch, GRIDDB_LIKE)

    ir._build_risk_map()

    assert set(fake.names_asked) == EXACT_INPUTS
    assert len(fake.calls) == 1, "one PIT read per build"
    call = fake.calls[0]
    assert call["policy"] == "LATEST_AS_OF"
    assert call["as_of"] == TODAY
    assert call["start"] == TODAY - timedelta(days=ir._INPUT_WINDOW_DAYS)
    assert sorted(call["fids"]) == list(range(1, len(GRIDDB_LIKE) + 1))


def test_griddb_readings_score_in_basis_points(monkeypatch):
    """The 2026-11-23 griddb values: HY 3.76 % is 376bp, not "4"."""
    _install(monkeypatch, GRIDDB_LIKE)

    result = ir._build_risk_map()

    credit = result["credit_risk"]
    assert credit["available"] is True
    assert credit["hy_spread"] == 376
    assert credit["hy_spread_as_of"] == _ago(3).isoformat()
    assert credit["ig_spread"] == 98
    assert credit["spread_direction"] == "widening"  # 376 > 338 * 1.05
    assert "ted_spread" not in credit  # discontinued series, not read
    assert credit["risk_level"] == "elevated"  # 376/800 + 0.15 = 0.62

    liq = result["liquidity_risk"]
    assert liq["available"] is True
    assert liq["fed_net_liquidity_change_1m"] == -12_152
    assert liq["fed_balance_sheet_as_of"] == _ago(5).isoformat()
    assert liq["tga_trend"] == "stable"  # 916,613 vs 952,002: within 5 %
    assert liq["reverse_repo_trend"] == "rising"

    vol = result["volatility_risk"]
    assert vol["available"] is True
    assert vol["vix"] == 30.2
    assert vol["vix_as_of"] == TODAY.isoformat()


def test_measured_subsystems_carry_real_numbers_and_overall_averages_only_them(monkeypatch):
    """VIX + HY present, everything else absent -> two measured blocks,
    overall is their mean, the other four are unavailable."""
    _install(monkeypatch, {
        # 10 daily readings in the last year; the current 30 sits at or above 9 of them.
        "vix_spot": [(_ago(300 - 30 * i), v) for i, v in enumerate((12, 14, 15, 16, 18, 19, 20, 22, 35))]
        + [(_ago(0), 30.0)],
        "hy_oas_spread": [(_ago(40), 3.00), (_ago(1), 4.00)],
    })

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
    assert credit["ig_spread"] is None
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


def test_vix_rank_uses_only_the_last_year(monkeypatch):
    _install(monkeypatch, {"vix_spot": [(_ago(500), 80.0), (_ago(370), 75.0), (_ago(200), 10.0), (_ago(0), 20.0)]})

    vol = ir._build_risk_map()["volatility_risk"]

    assert vol["vix_percentile_sample"] == 2  # 10.0 and 20.0
    assert vol["vix_percentile_1y"] == 100


@pytest.mark.parametrize(
    ("name", "subsystem", "age"),
    [
        ("vix_spot", "volatility_risk", 15),
        ("hy_oas_spread", "credit_risk", 15),
        ("fed_balance_sheet", "liquidity_risk", 22),
    ],
)
def test_stale_scored_input_is_unavailable(monkeypatch, name, subsystem, age):
    """A latest reading past its age limit is reported, not scored."""
    data = dict(GRIDDB_LIKE)
    data[name] = [(_ago(age + 40), 1.0), (_ago(age), 2.0)]
    _install(monkeypatch, data)

    block = ir._build_risk_map()[subsystem]

    assert block["available"] is False
    assert block["risk_level"] == "unknown"
    assert f"{name} latest reading {_ago(age).isoformat()} is {age} days old" in block["reason"]
    assert _numeric(block) == {}


def test_reading_at_the_age_limit_is_still_scored(monkeypatch):
    data = dict(GRIDDB_LIKE)
    data["hy_oas_spread"] = [(_ago(50), 3.0), (_ago(ir._MAX_AGE_DAYS["hy_oas_spread"]), 3.0)]
    _install(monkeypatch, data)

    assert ir._build_risk_map()["credit_risk"]["available"] is True


def test_stale_or_missing_secondary_inputs_report_unknown_not_values(monkeypatch):
    """TGA / RRP / IG do not block their sub-system; stale ones read as unknown / null."""
    data = dict(GRIDDB_LIKE)
    data["treasury_general_acct"] = [(_ago(260), 779_175.0), (_ago(236), 847_718.0)]  # like treasury_tga_balance
    data["ig_oas_spread"] = [(_ago(30), 0.9)]
    del data["reverse_repo_usage"]
    _install(monkeypatch, data)

    result = ir._build_risk_map()

    assert result["liquidity_risk"]["available"] is True
    assert result["liquidity_risk"]["tga_trend"] == "unknown"
    assert result["liquidity_risk"]["reverse_repo_trend"] == "unknown"
    assert result["credit_risk"]["available"] is True
    assert result["credit_risk"]["ig_spread"] is None


def test_fed_assets_drop_threshold_is_in_millions(monkeypatch):
    """A $60B monthly fall in WALCL (millions USD) takes the large-drop branch."""
    data = dict(GRIDDB_LIKE)
    data["fed_balance_sheet"] = [(_ago(33), 6_800_000.0), (_ago(5), 6_740_000.0)]
    _install(monkeypatch, data)

    liq = ir._build_risk_map()["liquidity_risk"]

    assert liq["fed_net_liquidity_change_1m"] == -60_000
    assert liq["risk_level"] == "elevated"  # 0.3 + 0.3; a small fall is 0.3 + 0.15


def test_tga_building_and_rrp_declining_move_the_score(monkeypatch):
    data = dict(GRIDDB_LIKE)
    data["fed_balance_sheet"] = [(_ago(33), 6_700_000.0), (_ago(5), 6_710_000.0)]  # rising: +0
    data["treasury_general_acct"] = [(_ago(33), 800_000.0), (_ago(5), 900_000.0)]  # building: +0.15
    data["reverse_repo_usage"] = [(_ago(33), 0.40), (_ago(0), 0.20)]  # declining: -0.1
    _install(monkeypatch, data)

    liq = ir._build_risk_map()["liquidity_risk"]

    assert liq["tga_trend"] == "building"
    assert liq["reverse_repo_trend"] == "declining"
    assert liq["risk_level"] == "moderate"  # 0.3 + 0.15 - 0.1 = 0.35


def test_liquidity_needs_a_month_back_reading(monkeypatch):
    data = dict(GRIDDB_LIKE)
    data["fed_balance_sheet"] = [(_ago(12), 6_700_000.0), (_ago(5), 6_710_000.0)]
    _install(monkeypatch, data)

    block = ir._build_risk_map()["liquidity_risk"]

    assert block["available"] is False
    assert "no fed_balance_sheet reading >= 30 days old" in block["reason"]


def test_input_read_failure_fails_each_series_subsystem_with_its_reason(monkeypatch):
    _install(monkeypatch, GRIDDB_LIKE, fail=RuntimeError("pit store down"))

    result = ir._build_risk_map()

    for key in ("volatility_risk", "credit_risk", "liquidity_risk"):
        assert result[key]["available"] is False, key
        assert result[key]["reason"] == "pit store down", key
    assert result["available_subsystems"] == 0


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


def test_source_has_no_default_literals_or_name_patterns_left():
    """Regression guard on the source text: the old defaults and the ILIKE
    pattern reads must not reappear."""
    src = inspect.getsource(ir._build_risk_map)
    for banned in ("else 20.0", "else 400", "else 100", "else 0.3", "pct = 50", "vix_val * 0.9",
                   "ILIKE", "%vix%", "%hy%", "%ig%", "%ted%", "%tga%", "%fed%", "%reverse_repo%",
                   "-50_000_000_000"):
        assert banned not in src, banned
    assert set(ir._MAX_AGE_DAYS) == EXACT_INPUTS


def test_fully_unavailable_result_is_served_but_not_cached(monkeypatch):
    """No sub-system measured -> honest payload now, no 5-minute pin."""
    _install(monkeypatch)

    result = asyncio.run(ir.get_risk_map("test-token"))

    assert result["overall_risk_score"] is None
    assert result["available_subsystems"] == 0
    assert ir._risk_map_cache.get(ir._RISK_MAP_CACHE_KEY) is None


def test_partially_measured_result_is_cached(monkeypatch):
    _install(monkeypatch, {"hy_oas_spread": [(_ago(2), 3.5)]})

    result = asyncio.run(ir.get_risk_map("test-token"))

    assert result["available_subsystems"] == 1
    assert result["credit_risk"]["spread_direction"] == "unknown"
    assert ir._risk_map_cache.get(ir._RISK_MAP_CACHE_KEY) is result
