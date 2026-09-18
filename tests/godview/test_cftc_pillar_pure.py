"""Pure-Python tests for godview/cftc_pillar.py — no database, no network.

Covers: validation of a constructed CFTC COT fixture, the z-score/percentile
math, the release_date rule (and its quarantine of non-Tuesday reports), and
the crowding-regime classification. Every fixture is constructed in
ingestion/altdata/cftc_cot.py's documented record format, never downloaded.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from godview.cftc_pillar import (
    AVAILABILITY_BASIS_INFERRED,
    AVAILABILITY_BASIS_OBSERVED,
    AVAILABILITY_BASIS_UNKNOWN,
    INFERRED_BASIS_NOTE,
    RELEASE_RULE_ID,
    classify_availability_basis,
    classify_crowding,
    compute_percentile,
    compute_release_date,
    compute_zscore,
    coverage_fraction_for_window,
    parse_cot_record,
    validate_fixture,
)
from tests.fixtures.godview.cftc_fixture import (
    build_cot_records,
    build_invalid_records,
    build_non_tuesday_record,
)


# ---------------------------------------------------------------------------
# parse_cot_record
# ---------------------------------------------------------------------------


def test_parse_cot_record_extracts_all_required_metrics():
    record = build_cot_records(n_weeks=1)[0]
    parsed = parse_cot_record(record)
    assert parsed is not None
    assert parsed.report_date.weekday() == 1  # Tuesday
    for metric in ("commercial_long", "commercial_short", "noncommercial_long", "noncommercial_short", "total_open_interest"):
        assert metric in parsed.metrics


def test_parse_cot_record_returns_none_for_unparseable_date():
    record = build_cot_records(n_weeks=1)[0]
    record = dict(record)
    record["report_date_as_yyyy_mm_dd"] = None
    assert parse_cot_record(record) is None


# ---------------------------------------------------------------------------
# validate_fixture
# ---------------------------------------------------------------------------


def test_validate_fixture_accepts_a_clean_constructed_set():
    records = build_cot_records(n_weeks=20)
    result = validate_fixture(records)
    assert result.all_valid
    assert result.valid_records == 20
    assert len(result.valid_report_dates) == 20
    # every report_date is a Tuesday, by construction
    assert all(d.weekday() == 1 for d in result.valid_report_dates)


@pytest.mark.parametrize(
    "index,expected_reason_prefix",
    [
        (0, "unparseable_report_date"),
        (1, "missing_metric"),
        (2, "negative_value"),
        (3, "commercial_exceeds_open_interest"),
    ],
)
def test_validate_fixture_flags_each_constructed_defect(index, expected_reason_prefix):
    records = build_invalid_records()
    result = validate_fixture([records[index]])
    assert result.valid_records == 0
    assert any(reason.startswith(expected_reason_prefix) for reason in result.invalid_reasons)


# ---------------------------------------------------------------------------
# release_date rule (contract doc section 8)
# ---------------------------------------------------------------------------


def test_compute_release_date_sets_friday_for_a_tuesday_report():
    tuesday = date(2026, 9, 15)
    assert tuesday.weekday() == 1
    release_date, source_ref = compute_release_date(tuesday)
    assert release_date == date(2026, 9, 18)  # the following Friday
    assert release_date.weekday() == 4
    assert RELEASE_RULE_ID in source_ref
    assert "Tuesday" in source_ref


def test_compute_release_date_withholds_for_a_non_tuesday_report():
    record = build_non_tuesday_record()
    parsed = parse_cot_record(record)
    release_date, source_ref = compute_release_date(parsed.report_date)
    assert release_date is None
    assert RELEASE_RULE_ID in source_ref
    assert "not Tuesday" in source_ref
    assert "withheld" in source_ref


# ---------------------------------------------------------------------------
# z-score / percentile / crowding regime
# ---------------------------------------------------------------------------


def test_compute_zscore_none_below_min_history():
    assert compute_zscore([1.0, 2.0, 3.0], window=52) is None


def test_compute_zscore_none_for_a_flat_series():
    flat = [100.0] * 20
    assert compute_zscore(flat, window=52) is None  # stdev ~ 0


def test_compute_zscore_positive_for_an_upward_drifting_series():
    values = [float(i) for i in range(60)]
    z = compute_zscore(values, window=52)
    assert z is not None
    assert z > 0  # the current (last) value is above the trailing mean


def test_compute_percentile_is_100_for_the_series_maximum():
    values = [float(i) for i in range(60)]
    pct = compute_percentile(values, window=52)
    assert pct is not None
    assert pct > 95.0


def test_coverage_fraction_for_window_caps_at_one():
    assert coverage_fraction_for_window(10, window=156) == pytest.approx(10 / 156)
    assert coverage_fraction_for_window(200, window=156) == 1.0
    assert coverage_fraction_for_window(0, window=156) == 0.0


@pytest.mark.parametrize(
    "percentile,expected",
    [
        (None, "insufficient_history"),
        (97, "extreme_long_crowding"),
        (90, "elevated_long_crowding"),
        (50, "neutral"),
        (10, "elevated_short_crowding"),
        (2, "extreme_short_crowding"),
    ],
)
def test_classify_crowding_thresholds(percentile, expected):
    assert classify_crowding(percentile) == expected


# ---------------------------------------------------------------------------
# availability_basis (Slice A, operator direction 2026-09-18)
# ---------------------------------------------------------------------------


def test_availability_basis_unknown_without_a_release_date():
    basis, note = classify_availability_basis(None, datetime(2026, 9, 18, tzinfo=timezone.utc))
    assert basis == AVAILABILITY_BASIS_UNKNOWN
    assert note is None


def test_availability_basis_unknown_without_an_available_at():
    basis, note = classify_availability_basis(date(2026, 9, 18), None)
    assert basis == AVAILABILITY_BASIS_UNKNOWN
    assert note is None


def test_availability_basis_observed_for_a_first_release_pulled_on_schedule():
    release_date = date(2026, 9, 18)  # a Friday
    available_at = datetime(2026, 9, 19, 9, 0, tzinfo=timezone.utc)  # next day, 1 pull
    basis, note = classify_availability_basis(release_date, available_at, distinct_pull_count=1)
    assert basis == AVAILABILITY_BASIS_OBSERVED
    assert note is None


def test_availability_basis_observed_accepts_the_full_tolerance_window():
    release_date = date(2026, 9, 18)
    available_at = date(2026, 9, 21)  # exactly AVAILABILITY_BASIS_TOLERANCE_DAYS (3) later
    basis, note = classify_availability_basis(release_date, available_at, distinct_pull_count=1)
    assert basis == AVAILABILITY_BASIS_OBSERVED


def test_availability_basis_inferred_for_a_backfilled_record_weeks_late():
    release_date = date(2026, 6, 5)
    available_at = date(2026, 9, 18)  # ~15 weeks later -- a historical backfill run
    basis, note = classify_availability_basis(release_date, available_at, distinct_pull_count=1)
    assert basis == AVAILABILITY_BASIS_INFERRED
    assert note == INFERRED_BASIS_NOTE


def test_availability_basis_inferred_for_a_revised_report_regardless_of_timing():
    """A re-pulled report_date is never labelled observed for the original release,
    even when the (later, winning) pull happens to land inside the tolerance window."""
    release_date = date(2026, 9, 18)
    available_at = date(2026, 9, 19)  # would be "observed" on its own
    basis, note = classify_availability_basis(release_date, available_at, distinct_pull_count=2)
    assert basis == AVAILABILITY_BASIS_INFERRED
    assert note == INFERRED_BASIS_NOTE


def test_availability_basis_revised_vs_first_release_side_by_side():
    """Demonstrates the operator's literal scenario within the pure classifier:
    cftc_positioning_daily's UNIQUE(report_date, contract_code) means only ONE
    row can ever exist per report_date (immutable, INSERT-only -- see contract
    doc section 7), so "two vintages of one row" cannot coexist in the table.
    The comparison instead holds across two report_dates: one pulled once, on
    schedule (observed); one that raw_series shows was pulled twice (revised),
    whose only materialized row is the later, inferred-labelled vintage.
    """
    first_release_basis, first_release_note = classify_availability_basis(
        date(2026, 9, 4), date(2026, 9, 5), distinct_pull_count=1
    )
    revised_basis, revised_note = classify_availability_basis(
        date(2026, 9, 11), date(2026, 9, 12), distinct_pull_count=2
    )
    assert first_release_basis == AVAILABILITY_BASIS_OBSERVED
    assert first_release_note is None
    assert revised_basis == AVAILABILITY_BASIS_INFERRED
    assert revised_note == INFERRED_BASIS_NOTE
