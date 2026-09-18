"""Unit-normalisation tests for ingestion/altdata/fed_liquidity.py.

Regression coverage for the fix (2026-09-18): WALCL and WTREGEN are FRED
"Millions of U.S. Dollars"; RRPONTSYD is FRED "Billions of US Dollars"
(quoted, with source URLs, in the module's own docstring and
FRED_SERIES_UNIT_QUOTES). Before the fix, ``_compute_derived`` combined
all three with no conversion, making the RRPONTSYD term ~1000x too small
relative to WALCL/WTREGEN. These tests are pure -- no database, no
network -- against ``normalize_to_millions`` / ``series_unit`` /
``build_net_liquidity_series``.
"""

from __future__ import annotations

from datetime import date

import pytest

from ingestion.altdata.fed_liquidity import (
    FRED_SERIES_UNIT_QUOTES,
    UNIT_SCALE_TO_MILLIONS,
    build_net_liquidity_series,
    normalize_to_millions,
    series_unit,
)

# ---------------------------------------------------------------------------
# Unit table itself: the three real, cited units
# ---------------------------------------------------------------------------


def test_walcl_and_wtregen_are_millions_rrpontsyd_is_billions():
    """The documented units, exactly as confirmed on each series' own FRED page."""
    assert series_unit("WALCL") == "millions_usd"
    assert series_unit("WTREGEN") == "millions_usd"
    assert series_unit("RRPONTSYD") == "billions_usd"
    # Every entry carries a cited quote + URL, not just a bare unit key.
    for series_id in ("WALCL", "WTREGEN", "RRPONTSYD"):
        _, quote = FRED_SERIES_UNIT_QUOTES[series_id]
        assert "fred.stlouisfed.org" in quote
        assert "Dollars" in quote


def test_unit_scale_table_is_explicit_never_a_bare_multiplier():
    assert UNIT_SCALE_TO_MILLIONS["millions_usd"] == 1.0
    assert UNIT_SCALE_TO_MILLIONS["billions_usd"] == 1000.0


# ---------------------------------------------------------------------------
# normalize_to_millions: the three real units + unknown-unit + missing-value
# ---------------------------------------------------------------------------


def test_normalize_walcl_millions_is_unchanged():
    assert normalize_to_millions("WALCL", 7_500_000.0) == pytest.approx(7_500_000.0)


def test_normalize_wtregen_millions_is_unchanged():
    assert normalize_to_millions("WTREGEN", 700_000.0) == pytest.approx(700_000.0)


def test_normalize_rrpontsyd_billions_scales_by_1000():
    assert normalize_to_millions("RRPONTSYD", 300.0) == pytest.approx(300_000.0)


def test_normalize_returns_none_for_a_missing_value():
    assert normalize_to_millions("WALCL", None) is None


def test_normalize_returns_none_for_an_unknown_series_unit():
    """A series with no entry in FRED_SERIES_UNIT_QUOTES -- no value, no guess."""
    assert normalize_to_millions("SOME_UNDOCUMENTED_SERIES", 42.0) is None
    assert series_unit("SOME_UNDOCUMENTED_SERIES") is None


# ---------------------------------------------------------------------------
# build_net_liquidity_series: missing component -> no value, ever
# ---------------------------------------------------------------------------


def test_build_net_liquidity_series_computes_the_normalised_value():
    d = date(2026, 9, 3)
    result = build_net_liquidity_series(
        walcl={d: 7_500_000.0}, wtregen={d: 700_000.0}, rrp={d: 300.0}
    )
    assert result[d] == pytest.approx(7_500_000.0 - 700_000.0 - 300_000.0)


def test_build_net_liquidity_series_omits_dates_missing_any_component():
    d1 = date(2026, 9, 3)
    d2 = date(2026, 9, 10)
    # RRPONTSYD never appears at all -- no date should ever get a value,
    # not a value computed against a stale/forward-filled RRP.
    result = build_net_liquidity_series(
        walcl={d1: 7_500_000.0, d2: 7_600_000.0},
        wtregen={d1: 700_000.0, d2: 720_000.0},
        rrp={},
    )
    assert result == {}


def test_build_net_liquidity_series_forward_fills_the_weekly_vs_daily_mismatch():
    """WALCL/WTREGEN are weekly; RRPONTSYD is daily -- once all three have
    appeared at least once, later dates use the carried-forward value."""
    d1 = date(2026, 9, 3)
    d2 = date(2026, 9, 4)  # RRPONTSYD-only day; WALCL/WTREGEN carry forward
    result = build_net_liquidity_series(
        walcl={d1: 7_500_000.0}, wtregen={d1: 700_000.0}, rrp={d1: 300.0, d2: 310.0}
    )
    assert d1 in result and d2 in result
    assert result[d2] == pytest.approx(7_500_000.0 - 700_000.0 - 310_000.0)


# ---------------------------------------------------------------------------
# Regression: the pre-fix arithmetic would have been ~1000x off
# ---------------------------------------------------------------------------


def test_regression_pre_fix_arithmetic_was_materially_wrong_on_a_fixture():
    """Documents the defect's magnitude on a constructed, realistic fixture.

    Pre-fix, ``_compute_derived`` computed ``last_w - last_t - last_r`` with
    no unit conversion at all -- i.e. it used RRPONTSYD's raw billions
    figure as if it were already millions. That made the RRPONTSYD term
    ~1000x too small, so the "buggy" result below is only trivially
    different from omitting RRPONTSYD's contribution altogether.
    """
    walcl_m, wtregen_m, rrp_b = 7_500_000.0, 700_000.0, 300.0

    fixed = build_net_liquidity_series(
        walcl={date(2026, 9, 3): walcl_m},
        wtregen={date(2026, 9, 3): wtregen_m},
        rrp={date(2026, 9, 3): rrp_b},
    )[date(2026, 9, 3)]

    buggy = walcl_m - wtregen_m - rrp_b  # the pre-fix, unconverted arithmetic

    # The true RRP contribution is 300_000 (millions); the bug applied only
    # 300 of it -- an understatement of 299_700, i.e. essentially the whole
    # RRP term, confirming the ~1000x-off characterisation.
    assert fixed == pytest.approx(walcl_m - wtregen_m - rrp_b * 1000)
    assert abs(fixed - buggy) == pytest.approx(rrp_b * 999)
    assert abs(fixed - buggy) / abs(rrp_b * UNIT_SCALE_TO_MILLIONS["billions_usd"]) > 0.99
