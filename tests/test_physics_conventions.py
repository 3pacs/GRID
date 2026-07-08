"""Unit tests for GRID physics convention validators."""

from __future__ import annotations

import pytest

from physics.conventions import (
    check_unit_compatibility,
    validate_convention,
    validate_feature_set,
)


pytestmark = pytest.mark.unit


def assert_warning_contains(warnings: list[str], expected: str) -> None:
    assert any(expected in warning for warning in warnings), warnings


@pytest.mark.parametrize(
    ("family", "valid_value", "low_value", "high_value"),
    [
        ("rates", 5.25, -2.01, 30.01),
        ("spreads", 250.0, -50.01, 5000.01),
        ("volatility", 18.0, -0.01, 200.01),
        ("momentum", 0.15, -1.01, 10.01),
        ("fx", 1.08, 0.0009, 50000.01),
        ("commodity", 75.0, -0.01, 10000.01),
    ],
)
def test_validate_convention_warns_for_values_outside_expected_ranges(
    family: str,
    valid_value: float,
    low_value: float,
    high_value: float,
) -> None:
    assert validate_convention(f"{family}_ok", valid_value, family) == []

    low_warnings = validate_convention(f"{family}_low", low_value, family)
    high_warnings = validate_convention(f"{family}_high", high_value, family)

    assert_warning_contains(low_warnings, "outside expected range")
    assert_warning_contains(high_warnings, "outside expected range")


def test_validate_convention_flags_unknown_family() -> None:
    warnings = validate_convention("mystery_signal", 1.0, "not_a_family")

    assert warnings == [
        "Unknown family 'not_a_family' for feature 'mystery_signal'"
    ]


def test_validate_convention_flags_rate_basis_point_heuristic() -> None:
    warnings = validate_convention("fed_funds", 525.0, "rates")

    assert_warning_contains(warnings, "outside expected range")
    assert_warning_contains(warnings, "looks like basis points")
    assert_warning_contains(warnings, "convention is percent for rates")


@pytest.mark.parametrize("spread_value", [0.25, -0.5])
def test_validate_convention_flags_spread_percent_heuristic(
    spread_value: float,
) -> None:
    warnings = validate_convention("credit_oas", spread_value, "spreads")

    assert_warning_contains(warnings, "looks like percent")
    assert_warning_contains(warnings, "convention is basis_points for spreads")


def test_validate_convention_flags_negative_volatility_heuristic() -> None:
    warnings = validate_convention("realized_vol", -3.0, "volatility")

    assert_warning_contains(warnings, "outside expected range")
    assert_warning_contains(warnings, "negative volatility")
    assert_warning_contains(warnings, "not physical")


def test_validate_convention_flags_extreme_single_period_returns() -> None:
    warnings = validate_convention("daily_log_return", 2.5, "returns")

    assert_warning_contains(warnings, "return 2.5")
    assert_warning_contains(warnings, "extreme")
    assert_warning_contains(warnings, "single-period value")


def test_validate_feature_set_aggregates_only_features_with_warnings() -> None:
    features = {
        "fed_funds": 5.25,
        "credit_oas": 0.25,
        "unknown_signal": 42.0,
        "realized_vol": None,
    }
    family_map = {
        "fed_funds": "rates",
        "credit_oas": "spreads",
        "realized_vol": "volatility",
    }

    warnings_by_feature = validate_feature_set(features, family_map)

    assert set(warnings_by_feature) == {"credit_oas", "unknown_signal"}
    assert_warning_contains(warnings_by_feature["credit_oas"], "looks like percent")
    assert_warning_contains(warnings_by_feature["unknown_signal"], "Unknown family")


def test_check_unit_compatibility_spread_warns_for_unit_and_annualization_mismatch() -> None:
    warnings = check_unit_compatibility(
        "fed_funds",
        "rates",
        "credit_oas",
        "spreads",
        "spread",
    )

    assert_warning_contains(warnings, "incompatible units 'percent' vs 'basis_points'")
    assert_warning_contains(warnings, "mixing annualized=True with annualized=False")


def test_check_unit_compatibility_spread_allows_matching_units_and_annualization() -> None:
    warnings = check_unit_compatibility(
        "credit_oas",
        "spreads",
        "cds_spread",
        "credit",
        "spread",
    )

    assert warnings == []


def test_check_unit_compatibility_spread_warns_for_annualization_only() -> None:
    warnings = check_unit_compatibility(
        "daily_log_return",
        "returns",
        "twelve_one_momentum",
        "momentum",
        "spread",
    )

    assert len(warnings) == 1
    assert_warning_contains(warnings, "mixing annualized=True with annualized=False")


def test_check_unit_compatibility_ratio_warns_for_two_dimensionless_inputs() -> None:
    warnings = check_unit_compatibility(
        "daily_log_return",
        "returns",
        "twelve_one_momentum",
        "momentum",
        "ratio",
    )

    assert_warning_contains(warnings, "ratio of two dimensionless")
    assert_warning_contains(warnings, "consider if spread is more appropriate")


def test_check_unit_compatibility_ratio_allows_dimensional_inputs() -> None:
    warnings = check_unit_compatibility(
        "fed_funds",
        "rates",
        "credit_oas",
        "spreads",
        "ratio",
    )

    assert warnings == []


def test_check_unit_compatibility_skips_unknown_families() -> None:
    warnings = check_unit_compatibility(
        "known_rate",
        "rates",
        "unknown_signal",
        "missing",
        "spread",
    )

    assert warnings == []
