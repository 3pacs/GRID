"""Honesty tests for ``GET /api/v1/intelligence/risk-map``.

History:

* **Placeholders.** Every sub-system of ``_build_risk_map`` once fell back
  to literal values when its query failed or returned nothing: VIX 20.0
  at the 50th percentile, HY 400bp / IG 100bp / TED 0.30, average
  correlation 0.5, ``risk_level: "moderate"``. ``realized_vs_implied`` was
  ``vix / (0.9 * vix)``, and a builder failure returned
  ``overall_risk_score: 0.5``.
* **Pattern reads.** Until 2026-11-23 the series inputs were ILIKE
  patterns, each matching several features. '%hy%spread%' took whichever
  of hy_oas_spread, hy_spread_3m_chg, hy_spread_proxy and hy_spread was
  newest. '%tga%' read tga_drawdown, a 30-day change, and '%vix%close%'
  matched nothing. HY / IG OAS, stored in percent, were scored as basis
  points.
* **Codex review of #1402 at 1d3174cf.** It found that:
  - the "net liquidity" field was a Fed total-assets change;
  - a weekly print accepted at 21 days old could be compared over 11 days;
  - one VIX point ranked itself at 100;
  - freshness was a 14/21-day gap, not session based;
  - ``vix_term_structure`` was a ``vix > 25`` label.

These tests pin the contract:

* a sub-system with no data is ``{"risk_level": "unknown",
  "available": False, "reason": ...}`` with no numeric fields;
* each input reads one exact feature through ``PITStore.get_pit``
  (LATEST_AS_OF as of the UTC cutoff);
* freshness is per cadence: daily inputs within 2 NYSE sessions, weekly
  H.4.1 inputs within one missed release;
* month-back comparisons are anchored to the current reading's date, with
  a 30-37 day span, and both dates are served;
* VIX is ranked only on NYSE sessions, and only when the year and its last
  21 sessions are covered;
* HY / IG are in basis points, and the Fed field is an honestly named
  total-assets change in USD;
* ``overall_risk_score`` averages only measured sub-systems, and a builder
  failure is HTTP 503.

No database is touched. ``PITStore`` is an in-memory fake, the
concentration / correlation engine is a ``MagicMock``, the dealer gamma
engine is stubbed, and the cutoff is fixed at Monday 2026-11-23.
``tests/test_intelligence_risk_map_series_pg.py`` runs the same build
against real SQL.
"""

from __future__ import annotations

import asyncio
import inspect
import math
import sys
import types
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest
from fastapi import HTTPException

from api.routers import intelligence_risk as ir
from ingestion.market_calendar import trading_days_between

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
CUTOFF = date(2026, 11, 23)  # a Monday session
D = date


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


def _install(monkeypatch, data=None, *, fail=None, cutoff=CUTOFF) -> _FakePIT:
    fake = _FakePIT(data or {}, fail=fail)
    monkeypatch.setattr(ir, "PITStore", fake)
    monkeypatch.setattr(ir, "_risk_cutoff", lambda: cutoff)
    monkeypatch.setattr(ir, "get_db_engine", lambda: _engine())
    _stub_dealer_gamma(monkeypatch, {"error": "no options chain for SPY"})
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


def _year_sessions(end: date = CUTOFF) -> list[date]:
    return trading_days_between(end - timedelta(days=364), end)


def _vix_year(current: float = 30.0, other: float = 20.0, end: date = CUTOFF) -> list[tuple[date, float]]:
    """A print on every NYSE session in the year to *end*, the last one *current*."""
    days = _year_sessions(end)
    return [(d, other) for d in days[:-1]] + [(days[-1], current)]


# griddb readings, 2026-11-23. OAS in percent, WALCL / WTREGEN in millions
# USD, RRP in billions. Each prior date is the latest reading at least 30
# days before the current one.
GRIDDB_LIKE = {
    "vix_spot": _vix_year(30.24, 20.0),
    "hy_oas_spread": [(D(2026, 10, 20), 3.38), (D(2026, 11, 20), 3.76)],
    "ig_oas_spread": [(D(2026, 10, 20), 0.91), (D(2026, 11, 20), 0.98)],
    "fed_balance_sheet": [(D(2026, 10, 14), 6_753_604.0), (D(2026, 11, 18), 6_741_452.0)],
    "treasury_general_acct": [(D(2026, 10, 14), 952_002.0), (D(2026, 11, 18), 916_613.0)],
    "reverse_repo_usage": [(D(2026, 10, 23), 0.20), (D(2026, 11, 23), 0.35)],
}


def _with(**changes):
    data = dict(GRIDDB_LIKE)
    for name, rows in changes.items():
        if rows is None:
            data.pop(name, None)
        else:
            data[name] = rows
    return data


# ── whole-build contract ─────────────────────────────────────────────────

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


def test_inputs_are_exact_features_read_latest_as_of_the_cutoff(monkeypatch):
    fake = _install(monkeypatch, GRIDDB_LIKE)

    ir._build_risk_map()

    assert set(fake.names_asked) == EXACT_INPUTS
    assert len(fake.calls) == 1, "one PIT read per build"
    call = fake.calls[0]
    assert call["policy"] == "LATEST_AS_OF"
    assert call["as_of"] == CUTOFF
    assert call["start"] == CUTOFF - timedelta(days=ir._INPUT_WINDOW_DAYS)
    assert sorted(call["fids"]) == list(range(1, len(GRIDDB_LIKE) + 1))


def test_cutoff_is_the_utc_date():
    """The resolver stamps vintage_date as the pull's UTC date."""
    before = datetime.now(timezone.utc).date()
    got = ir._risk_cutoff()
    assert got in {before, datetime.now(timezone.utc).date()}


def test_griddb_readings(monkeypatch):
    _install(monkeypatch, GRIDDB_LIKE)

    result = ir._build_risk_map()

    credit = result["credit_risk"]
    assert credit["available"] is True
    assert credit["hy_spread"] == 376  # 3.76 % in bp, not "4"
    assert credit["hy_spread_as_of"] == "2026-11-20"
    assert credit["hy_spread_prior_as_of"] == "2026-10-20"
    assert credit["ig_spread"] == 98
    assert credit["spread_direction"] == "widening"  # 376 > 338 * 1.05
    assert "ted_spread" not in credit  # discontinued series, not read
    assert credit["risk_level"] == "elevated"  # 376/800 + 0.15 = 0.62

    liq = result["liquidity_risk"]
    assert liq["available"] is True
    assert liq["fed_total_assets_change_1m_usd"] == -12_152_000_000
    assert liq["fed_total_assets_as_of"] == "2026-11-18"
    assert liq["fed_total_assets_prior_as_of"] == "2026-10-14"
    assert "fed_net_liquidity_change_1m" not in liq  # it was never net liquidity
    assert liq["tga_trend"] == "stable"  # 916,613 vs 952,002: within 5 %
    assert liq["reverse_repo_trend"] == "rising"

    vol = result["volatility_risk"]
    assert vol["available"] is True
    assert vol["vix"] == 30.2
    assert vol["vix_as_of"] == "2026-11-23"
    assert vol["vix_percentile_1y"] == 100
    assert vol["vix_percentile_sample"] == vol["vix_sessions_expected"] == len(_year_sessions())
    assert "vix_term_structure" not in vol  # was a vix > 25 label


def test_measured_subsystems_carry_real_numbers_and_overall_averages_only_them(monkeypatch):
    """VIX + HY present, everything else absent -> two measured blocks,
    overall is their mean, the other four are unavailable."""
    _install(monkeypatch, {
        "vix_spot": _vix_year(30.0, 20.0),
        "hy_oas_spread": [(D(2026, 10, 20), 3.00), (D(2026, 11, 20), 4.00)],
    })

    result = ir._build_risk_map()

    vol = result["volatility_risk"]
    assert vol["available"] is True
    assert vol["vix_percentile_1y"] == 100
    # No realized-vol series is read; the old constant 1.11 is gone.
    assert vol["realized_vs_implied"] is None

    credit = result["credit_risk"]
    assert credit["hy_spread"] == 400
    assert credit["ig_spread"] is None
    assert credit["spread_direction"] == "widening"

    for key in ("dealer_risk", "concentration_risk", "correlation_risk", "liquidity_risk"):
        assert result[key]["available"] is False, key

    # overall = mean(v_score = 1.0, cr_score = 400/800 + 0.15 = 0.65)
    assert result["overall_risk_score"] == pytest.approx(0.825, abs=0.006)
    assert result["available_subsystems"] == 2


# ── freshness (per cadence, explicit cutoff) ─────────────────────────────

@pytest.mark.parametrize(
    ("cadence", "cutoff", "floor"),
    [
        ("daily", D(2026, 11, 23), D(2026, 11, 19)),   # Mon: Fri, Thu still current
        ("daily", D(2026, 11, 27), D(2026, 11, 24)),   # day after Thanksgiving
        ("daily", D(2026, 11, 26), D(2026, 11, 23)),   # Thanksgiving, no session
        ("weekly", D(2026, 11, 23), D(2026, 11, 11)),  # latest released Wed 11-18, one miss
        ("weekly", D(2026, 11, 25), D(2026, 11, 11)),  # Wed: 11-25 not released until Thu
        ("weekly", D(2026, 11, 26), D(2026, 11, 18)),  # Thu: 11-25 released
    ],
)
def test_freshness_floor(cadence, cutoff, floor):
    assert ir._freshness_floor(cadence, cutoff) == floor


@pytest.mark.parametrize(
    ("name", "subsystem", "rows"),
    [
        ("vix_spot", "volatility_risk", _vix_year(end=D(2026, 11, 18))),
        ("hy_oas_spread", "credit_risk", [(D(2026, 10, 16), 3.0), (D(2026, 11, 18), 3.5)]),
        ("fed_balance_sheet", "liquidity_risk", [(D(2026, 10, 7), 6.7e6), (D(2026, 11, 4), 6.8e6)]),
    ],
)
def test_input_past_its_freshness_floor_is_unavailable(monkeypatch, name, subsystem, rows):
    _install(monkeypatch, _with(**{name: rows}))

    block = ir._build_risk_map()[subsystem]

    assert block["available"] is False
    assert block["risk_level"] == "unknown"
    assert f"{name} latest reading {rows[-1][0].isoformat()} is older than its" in block["reason"]
    assert _numeric(block) == {}


def test_weekly_print_one_release_late_is_scored_against_its_own_month(monkeypatch):
    """Stale but allowed: on 11-23 the 11-18 H.4.1 is missing, so 11-11 is
    current. The prior is the latest reading >= 30 days before 11-11
    (10-07, a 35-day span). Measured from the cutoff, it would have been
    10-21, a 21-day span."""
    weekly = [(D(2026, 10, 7), 6_800_000.0), (D(2026, 10, 14), 6_790_000.0),
              (D(2026, 10, 21), 6_780_000.0), (D(2026, 10, 28), 6_770_000.0),
              (D(2026, 11, 4), 6_760_000.0), (D(2026, 11, 11), 6_740_000.0)]
    _install(monkeypatch, _with(fed_balance_sheet=weekly))

    liq = ir._build_risk_map()["liquidity_risk"]

    assert liq["available"] is True
    assert liq["fed_total_assets_as_of"] == "2026-11-11"
    assert liq["fed_total_assets_prior_as_of"] == "2026-10-07"
    assert liq["fed_total_assets_change_1m_usd"] == -60_000_000_000
    assert liq["risk_level"] == "elevated"  # $60B fall > $50B: 0.3 + 0.3


def test_stale_or_missing_secondary_inputs_report_unknown_not_values(monkeypatch):
    """TGA / RRP / IG do not block their sub-system; stale ones read as unknown / null."""
    data = _with(
        treasury_general_acct=[(D(2026, 3, 4), 779_175.0), (D(2026, 4, 1), 847_718.0)],  # like treasury_tga_balance
        ig_oas_spread=[(D(2026, 10, 24), 0.9)],
        reverse_repo_usage=None,
    )
    _install(monkeypatch, data)

    result = ir._build_risk_map()

    assert result["liquidity_risk"]["available"] is True
    assert result["liquidity_risk"]["tga_trend"] == "unknown"
    assert result["liquidity_risk"]["reverse_repo_trend"] == "unknown"
    assert result["credit_risk"]["available"] is True
    assert result["credit_risk"]["ig_spread"] is None


# ── month-back anchoring ─────────────────────────────────────────────────

def test_month_back_is_anchored_to_the_current_reading():
    weekly = [(D(2026, 10, 7), 1.0), (D(2026, 10, 14), 2.0), (D(2026, 10, 21), 3.0), (D(2026, 11, 11), 4.0)]
    assert ir._month_back(weekly, D(2026, 11, 11)) == (D(2026, 10, 7), 1.0)


def test_month_back_never_spans_less_than_30_days():
    """Codex's case: current 11-02, prior 10-22. A cutoff-anchored read on
    11-23 would have compared them over 11 days."""
    assert ir._month_back([(D(2026, 10, 22), 1.0), (D(2026, 11, 2), 2.0)], D(2026, 11, 2)) is None


def test_month_back_rejects_a_span_over_37_days():
    assert ir._month_back([(D(2026, 9, 20), 1.0), (D(2026, 11, 2), 2.0)], D(2026, 11, 2)) is None
    assert ir._month_back([(D(2026, 9, 26), 1.0), (D(2026, 11, 2), 2.0)], D(2026, 11, 2)) == (D(2026, 9, 26), 1.0)


def test_liquidity_needs_a_month_back_reading(monkeypatch):
    _install(monkeypatch, _with(fed_balance_sheet=[(D(2026, 11, 4), 6.70e6), (D(2026, 11, 18), 6.71e6)]))

    block = ir._build_risk_map()["liquidity_risk"]

    assert block["available"] is False
    assert block["reason"] == "no fed_balance_sheet reading 30-37 days before 2026-11-18 to measure the 1m change"


def test_hy_direction_is_unknown_without_an_anchored_prior(monkeypatch):
    _install(monkeypatch, _with(hy_oas_spread=[(D(2026, 11, 2), 3.0), (D(2026, 11, 20), 3.76)]))

    credit = ir._build_risk_map()["credit_risk"]

    assert credit["spread_direction"] == "unknown"
    assert credit["hy_spread_prior_as_of"] is None


# ── VIX coverage (counted separately from the score) ─────────────────────

def test_vix_coverage_counts_sessions_only():
    sessions = _year_sessions()
    holiday_prints = [D(2026, 5, 25), D(2026, 9, 7)]  # Memorial Day, Labor Day
    cov = ir._vix_coverage(sessions + holiday_prints, CUTOFF)
    assert cov == {"expected": len(sessions), "covered": len(sessions),
                   "tail_expected": 21, "tail_covered": 21}
    assert ir._vix_coverage([CUTOFF], CUTOFF)["covered"] == 1


def test_vix_coverage_shortfall_boundary():
    expected = len(_year_sessions())
    need = math.ceil(0.9 * expected)
    ok = {"expected": expected, "covered": need, "tail_expected": 21, "tail_covered": 19}
    assert ir._vix_coverage_shortfall(ok, CUTOFF) is None
    assert "need %d" % need in ir._vix_coverage_shortfall({**ok, "covered": need - 1}, CUTOFF)
    assert "the last 21" in ir._vix_coverage_shortfall({**ok, "tail_covered": 18}, CUTOFF)


def test_one_vix_print_is_not_ranked(monkeypatch):
    _install(monkeypatch, {"vix_spot": [(CUTOFF, 30.0)]})

    block = ir._build_risk_map()["volatility_risk"]

    assert block["available"] is False
    assert block["reason"].startswith(f"vix_spot covers 1 of {len(_year_sessions())} NYSE sessions")
    assert block["reason"].endswith("not ranked")
    assert _numeric(block) == {}


def test_sparse_vix_tail_is_not_ranked(monkeypatch):
    """A complete old year with 3 of the last 21 sessions missing abstains."""
    year = _vix_year()
    tail_drop = {d for d, _ in year[-21:-1][:3]}
    _install(monkeypatch, {"vix_spot": [r for r in year if r[0] not in tail_drop]})

    block = ir._build_risk_map()["volatility_risk"]

    assert block["available"] is False
    assert "covers 18 of the last 21 NYSE sessions" in block["reason"]


def test_vix_holiday_prints_are_not_ranked(monkeypatch):
    """Cboe holiday prints are real, but they stay out of the sample and the
    coverage count. Ranked with them, 30.0 would sit at the 99th percentile."""
    year = _vix_year(30.0, 20.0) + [(D(2026, 5, 25), 99.0), (D(2026, 9, 7), 99.0)]
    _install(monkeypatch, {"vix_spot": sorted(year)})

    vol = ir._build_risk_map()["volatility_risk"]

    assert vol["vix_percentile_1y"] == 100
    assert vol["vix_percentile_sample"] == len(_year_sessions())


def test_current_vix_is_the_latest_session_print_on_a_holiday(monkeypatch):
    cutoff = D(2026, 11, 26)  # Thanksgiving
    year = _vix_year(25.0, 20.0, end=D(2026, 11, 25)) + [(cutoff, 50.0)]
    _install(monkeypatch, {"vix_spot": year}, cutoff=cutoff)

    vol = ir._build_risk_map()["volatility_risk"]

    assert vol["vix"] == 25.0
    assert vol["vix_as_of"] == "2026-11-25"


def test_vix_year_uses_only_the_last_365_days(monkeypatch):
    old = [(d, 80.0) for d in trading_days_between(D(2025, 9, 1), D(2025, 11, 20))]
    _install(monkeypatch, {"vix_spot": old + _vix_year(30.0, 20.0)})

    vol = ir._build_risk_map()["volatility_risk"]

    assert vol["vix_percentile_1y"] == 100
    assert vol["vix_percentile_sample"] == len(_year_sessions())


# ── units and score terms ────────────────────────────────────────────────

def test_tga_building_and_rrp_declining_move_the_score(monkeypatch):
    _install(monkeypatch, _with(
        fed_balance_sheet=[(D(2026, 10, 14), 6_700_000.0), (D(2026, 11, 18), 6_710_000.0)],  # rising: +0
        treasury_general_acct=[(D(2026, 10, 14), 800_000.0), (D(2026, 11, 18), 900_000.0)],  # building: +0.15
        reverse_repo_usage=[(D(2026, 10, 23), 0.40), (D(2026, 11, 23), 0.20)],  # declining: -0.1
    ))

    liq = ir._build_risk_map()["liquidity_risk"]

    assert liq["tga_trend"] == "building"
    assert liq["reverse_repo_trend"] == "declining"
    assert liq["risk_level"] == "moderate"  # 0.3 + 0.15 - 0.1 = 0.35


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
    """Regression guard on the source text: the old defaults, the ILIKE
    pattern reads and the level-threshold term structure must not reappear."""
    src = inspect.getsource(ir._build_risk_map)
    for banned in ("else 20.0", "else 400", "else 100", "else 0.3", "pct = 50", "vix_val * 0.9",
                   "ILIKE", "%vix%", "%hy%", "%ig%", "%ted%", "%tga%", "%fed%", "%reverse_repo%",
                   "vix_val > 25", "backwardation", "date.today()"):
        assert banned not in src, banned
    assert set(ir._CADENCE) == EXACT_INPUTS


def test_fully_unavailable_result_is_served_but_not_cached(monkeypatch):
    """No sub-system measured -> honest payload now, no 5-minute pin."""
    _install(monkeypatch)

    result = asyncio.run(ir.get_risk_map("test-token"))

    assert result["overall_risk_score"] is None
    assert result["available_subsystems"] == 0
    assert ir._risk_map_cache.get(ir._RISK_MAP_CACHE_KEY) is None


def test_partially_measured_result_is_cached(monkeypatch):
    _install(monkeypatch, {"hy_oas_spread": [(D(2026, 11, 20), 3.5)]})

    result = asyncio.run(ir.get_risk_map("test-token"))

    assert result["available_subsystems"] == 1
    assert result["credit_risk"]["spread_direction"] == "unknown"
    assert ir._risk_map_cache.get(ir._RISK_MAP_CACHE_KEY) is result
