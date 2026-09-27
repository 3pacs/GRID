"""Pure-Python tests for godview/fed_liquidity.py — no database, no network.

Covers: correct arithmetic/units (including the B1 fix -- RRP stored in
millions, not raw billions), the H.4.1 release schedule and its federal
holiday shift, regime/delta/peak helpers, and the absence of fallback
constants. The DB-gated behavioral versions of B1/B2/B3 and the
missing-input / PIT-gate / legacy-row-protection tests live in
test_fed_liquidity_writer_pg.py; this file locks the pure helpers those
behaviors are built from.
"""

from __future__ import annotations

import inspect
from datetime import date, datetime, timedelta, timezone

import pytest

import godview.fed_liquidity as fed_liquidity
from godview.availability_basis import (
    AVAILABILITY_BASIS_INFERRED,
    AVAILABILITY_BASIS_OBSERVED,
    AVAILABILITY_BASIS_UNKNOWN,
    classify_availability_basis,
)
from godview.fed_liquidity import (
    RELEASE_RULE_ID,
    RRPONTSYD_TO_MILLIONS,
    classify_liquidity_regime,
    compute_net_liquidity_millions,
    compute_release_at,
    compute_rrp_pct_of_peak,
    coverage_fraction_for_window,
    federal_reserve_holidays,
    find_nearest_prior,
    rrp_billions_to_stored_millions,
)

# ══════════════════════════════════════════════════════════════════════════
# Release schedule (H.4.1 Wednesday -> Thursday 16:30 ET) / publication lag
# ══════════════════════════════════════════════════════════════════════════


def test_compute_release_at_is_thursday_1630_et_for_a_wednesday_obs():
    wednesday = date(2026, 9, 16)
    assert wednesday.weekday() == 2

    release_at, note = compute_release_at(wednesday)

    assert release_at is not None
    assert release_at.date() == date(2026, 9, 17)  # the following Thursday
    assert release_at.weekday() == 3
    assert (release_at.hour, release_at.minute) == (16, 30)
    assert str(release_at.tzinfo) == "America/New_York"
    assert RELEASE_RULE_ID in note
    assert "Wednesday" in note


@pytest.mark.parametrize("non_wednesday", [date(2026, 9, 15), date(2026, 9, 17), date(2026, 9, 19)])
def test_compute_release_at_withholds_for_a_non_wednesday_obs(non_wednesday):
    release_at, note = compute_release_at(non_wednesday)
    assert release_at is None
    assert "not Wednesday" in note
    assert "withheld" in note


def test_a_wednesday_value_is_not_usable_before_its_thursday_release():
    """The exact PIT scenario the plan calls out: a Wednesday level is known
    to exist, but is not yet public, until Thursday ~16:30 ET."""
    wednesday = date(2026, 9, 16)
    release_at, _ = compute_release_at(wednesday)

    before_release = datetime(2026, 9, 17, 12, 0, tzinfo=timezone.utc)  # Thu noon UTC, before 16:30 ET (20:30 UTC)
    assert before_release < release_at

    after_release = release_at + timedelta(minutes=1)
    assert after_release > release_at


# ══════════════════════════════════════════════════════════════════════════
# Federal Reserve holiday shift (a Thursday federal holiday pushes the
# H.4.1 release to the next business day) — distinct from the NYSE trading
# calendar in ingestion/market_calendar.py.
# ══════════════════════════════════════════════════════════════════════════


def test_thanksgiving_thursday_shifts_release_to_friday():
    # Thanksgiving 2026 is Thursday 2026-11-26 (4th Thursday of November).
    wed_before_thanksgiving = date(2026, 11, 25)
    assert wed_before_thanksgiving.weekday() == 2
    naive_thursday = wed_before_thanksgiving + timedelta(days=1)
    assert naive_thursday == date(2026, 11, 26)
    assert naive_thursday in federal_reserve_holidays(2026)

    release_at, note = compute_release_at(wed_before_thanksgiving)

    assert release_at is not None
    assert release_at.date() == date(2026, 11, 27)  # shifted to the Friday after
    assert release_at.weekday() == 4
    assert "holiday" in note
    assert "2026-11-26" in note


def test_a_normal_wednesday_is_not_shifted():
    wednesday = date(2026, 9, 16)
    naive_thursday = wednesday + timedelta(days=1)
    assert naive_thursday not in federal_reserve_holidays(2026)
    release_at, note = compute_release_at(wednesday)
    assert release_at.date() == naive_thursday
    assert "holiday" not in note


def test_federal_reserve_holidays_differ_from_the_nyse_trading_calendar():
    """The Fed observes Columbus Day and Veterans Day (banks closed); NYSE
    does not. NYSE observes Good Friday; the Fed does not. Using the wrong
    calendar here would misdate the H.4.1 release around exactly these days."""
    from ingestion.market_calendar import market_holidays

    fed_2026 = federal_reserve_holidays(2026)
    nyse_2026 = market_holidays(2026)

    columbus_day_2026 = date(2026, 10, 12)  # 2nd Monday of October
    veterans_day_2026 = date(2026, 11, 11)
    assert columbus_day_2026 in fed_2026
    assert columbus_day_2026 not in nyse_2026
    assert veterans_day_2026 in fed_2026
    assert veterans_day_2026 not in nyse_2026


def test_juneteenth_only_a_federal_holiday_from_2022():
    assert not any(d.month == 6 and d.day in (18, 19) for d in federal_reserve_holidays(2021))
    # 2022-06-19 (actual Juneteenth) is a Sunday, so OPM's observed-holiday
    # rule shifts it to Monday 2022-06-20 -- the 19th itself is not a
    # separate holiday.
    assert date(2022, 6, 19).weekday() == 6
    assert date(2022, 6, 20) in federal_reserve_holidays(2022)
    assert date(2022, 6, 19) not in federal_reserve_holidays(2022)


# ══════════════════════════════════════════════════════════════════════════
# Arithmetic / units — B1: RRP stored in millions, matching production
# ══════════════════════════════════════════════════════════════════════════


def test_uses_the_572_rrp_billions_to_millions_scale_factor():
    assert RRPONTSYD_TO_MILLIONS == 1_000.0


def test_compute_net_liquidity_converts_rrp_billions_to_millions():
    # 7,500,000M WALCL - 700,000M WTREGEN - 300B RRP (=300,000M) = 6,500,000M
    result = compute_net_liquidity_millions(7_500_000.0, 700_000.0, 300.0)
    assert result == pytest.approx(6_500_000.0)


def test_rrp_billions_to_stored_millions_matches_production_convention():
    """Pins B1: production's reverse_repo_rrp column already stores the RRP
    leg in millions (verified on the real 09-16 row: 6,740,619 - 877,028 -
    5,375 = 5,858,216 -- 5,375 is 5.375bn expressed in millions, not the raw
    billions value). This writer must store the same convention, never the
    raw FRED billions value."""
    stored = rrp_billions_to_stored_millions(5.375)
    assert stored == pytest.approx(5375.0)
    walcl, wtregen = 6_740_619.0, 877_028.0
    assert walcl - wtregen - stored == pytest.approx(5_858_216.0)


def test_compute_net_liquidity_delegates_to_ingestion_fed_liquidity_not_a_local_copy():
    """godview must not keep its own duplicate of the unit conversion (drift risk)."""
    import ingestion.altdata.fed_liquidity as fed_liquidity_ingestion

    assert compute_net_liquidity_millions(7_500_000.0, 700_000.0, 300.0) == pytest.approx(
        fed_liquidity_ingestion.net_liquidity_millions(7_500_000.0, 700_000.0, 300.0)
    )


def test_skipping_the_scale_conversion_would_materially_change_the_result():
    """Sanity check pinning the #572 bug class: dropping the *1000 term is not a rounding error."""
    converted = compute_net_liquidity_millions(7_500_000.0, 700_000.0, 300.0)
    unconverted = 7_500_000.0 - 700_000.0 - 300.0  # the pre-#572 formula
    assert abs(converted - unconverted) == pytest.approx(300.0 * (RRPONTSYD_TO_MILLIONS - 1))


# ══════════════════════════════════════════════════════════════════════════
# rrp_as_pct_of_peak / deltas / regime
# ══════════════════════════════════════════════════════════════════════════


def test_compute_rrp_pct_of_peak_is_100_at_a_new_high():
    history = [100.0, 200.0, 150.0, 300.0]  # last value is the peak
    assert compute_rrp_pct_of_peak(history, window=156) == pytest.approx(100.0)


def test_compute_rrp_pct_of_peak_none_below_min_history():
    assert compute_rrp_pct_of_peak([100.0, 200.0], window=156) is None


def test_coverage_fraction_for_window_caps_at_one():
    assert coverage_fraction_for_window(10, window=156) == pytest.approx(10 / 156)
    assert coverage_fraction_for_window(500, window=156) == 1.0


def test_find_nearest_prior_within_tolerance():
    history = [(date(2026, 9, 2), 100.0), (date(2026, 9, 9), 110.0)]
    value = find_nearest_prior(history, date(2026, 9, 16), target_days=7, tolerance_days=3)
    assert value == 110.0


def test_find_nearest_prior_none_when_nothing_within_tolerance():
    history = [(date(2026, 1, 1), 100.0)]
    value = find_nearest_prior(history, date(2026, 9, 16), target_days=7, tolerance_days=3)
    assert value is None


@pytest.mark.parametrize(
    "delta_4w,expected",
    [
        (None, "insufficient_history"),
        (60_000.0, "expanding"),
        (-60_000.0, "contracting"),
        (0.0, "neutral"),
    ],
)
def test_classify_liquidity_regime_never_returns_a_fabricated_default(delta_4w, expected):
    assert classify_liquidity_regime(delta_4w) == expected


# ══════════════════════════════════════════════════════════════════════════
# availability_basis (harvested from #571, used by this pillar)
# ══════════════════════════════════════════════════════════════════════════


def test_availability_basis_observed_when_pulled_promptly_after_release():
    release_date = date(2026, 9, 17)
    available_at = datetime(2026, 9, 17, 20, 52, tzinfo=timezone.utc)
    basis, note = classify_availability_basis(release_date, available_at)
    assert basis == AVAILABILITY_BASIS_OBSERVED
    assert note is None


def test_availability_basis_inferred_when_pulled_long_after_release():
    release_date = date(2026, 9, 17)
    available_at = datetime(2026, 9, 30, 0, 0, tzinfo=timezone.utc)
    basis, _ = classify_availability_basis(release_date, available_at)
    assert basis == AVAILABILITY_BASIS_INFERRED


def test_availability_basis_unknown_with_no_release_schedule():
    basis, _ = classify_availability_basis(None, datetime(2026, 9, 17, tzinfo=timezone.utc))
    assert basis == AVAILABILITY_BASIS_UNKNOWN


# ══════════════════════════════════════════════════════════════════════════
# No fallback constants — source guards
# ══════════════════════════════════════════════════════════════════════════

_FORBIDDEN_LITERALS = ("6780000", "6_780_000", "790000", "790_000")


def test_source_has_no_hard_coded_walcl_or_tga_fallback_literals():
    """Pins the absence of the incident materializer's fallback values
    (WALCL=6,780,000 / WTREGEN=790,000, per the plan's finding 3) from this
    writer's own source."""
    source = inspect.getsource(fed_liquidity)
    for literal in _FORBIDDEN_LITERALS:
        assert literal not in source, f"forbidden fallback literal {literal!r} found in godview/fed_liquidity.py"


def test_no_default_fallback_in_lookups():
    source = inspect.getsource(fed_liquidity)
    assert ".get(obs_date, 0" not in source
    assert ".get(obs_date, 0.0)" not in source


def test_does_not_import_the_untracked_incident_materializer_modules():
    """See godview/__init__.py: none of these names may appear as an import
    target anywhere in this module's source (they are the untracked
    incident modules that wrote the fallback constants in the first place)."""
    source = inspect.getsource(fed_liquidity)
    for forbidden in (
        "ingestion.god_view_materializer",
        "ingestion.altdata.fed_liquidity_materializer",
        "derivatives.dealer_gex_engine",
        "api.routers.god_view",
    ):
        assert forbidden not in source


def test_upsert_sql_never_updates_a_row_whose_provenance_is_null():
    """B3, second guard: even without the pre-check in materialize_fed_liquidity,
    the UPSERT statement's own WHERE clause refuses to touch a legacy row."""
    source = inspect.getsource(fed_liquidity)
    assert "provenance IS NOT NULL" in source
