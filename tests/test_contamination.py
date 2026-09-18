"""Tests for evaluation.contamination.

Key property under test: ambiguous rows (no explicit evidence field,
or an evidence field with an unrecognized value) must land in
"unknown" rather than being guessed into a real class — including via
the disallowed scored_at-range shortcut.
"""

from __future__ import annotations

from evaluation.contamination import (
    ORIGINS,
    SCORING_VERSIONS,
    build_cohort_report,
    classify_origin,
    classify_row,
    classify_scoring_version,
)


def test_scoring_version_is_always_unknown_no_schema_field_exists():
    """schema.sql has no explicit scoring/evaluation-version column on
    any signal/prediction table — classify_scoring_version() must
    never claim otherwise, regardless of what's in the row."""
    rows = [
        {},
        {"scored_at": "2026-05-20T00:00:00Z"},
        {"scored_at": "2026-09-01T00:00:00Z", "weight_version": "v7"},
        {"model_version": "xgboost-3"},
    ]
    for row in rows:
        version, evidence_field = classify_scoring_version(row)
        assert version == "unknown"
        assert evidence_field is None


def test_scored_at_alone_never_promotes_a_row_out_of_unknown():
    """Explicitly guards against the disallowed shortcut: even a
    scored_at squarely inside what would be "the April synthetic
    batch" window must not, by itself, produce a non-unknown
    scoring_version."""
    row = {"scored_at": "2026-04-15T12:00:00Z"}
    result = classify_row(row)
    assert result.scoring_version == "unknown"


def test_origin_live_from_explicit_live_or_local_field():
    row = {"live_or_local": "live"}
    origin, evidence_field = classify_origin(row)
    assert origin == "live"
    assert evidence_field == "live_or_local"


def test_origin_unknown_for_non_live_live_or_local_values():
    """'local'/'archive'/'hybrid' are NOT documented as synonyms for
    backfill/synthetic anywhere in the codebase — must stay unknown
    rather than being guessed."""
    for value in ("local", "archive", "hybrid"):
        origin, evidence_field = classify_origin({"live_or_local": value})
        assert origin == "unknown", f"live_or_local={value!r} must not be guessed"
        assert evidence_field is None


def test_origin_explicit_field_trusted_when_recognized():
    for value in ("synthetic", "backfill", "live"):
        origin, evidence_field = classify_origin({"origin": value})
        assert origin == value
        assert evidence_field == "origin"


def test_origin_unrecognized_explicit_value_is_unknown():
    origin, evidence_field = classify_origin({"origin": "fabricated"})
    assert origin == "unknown"
    assert evidence_field is None


def test_ambiguous_rows_land_in_unknown_bucket():
    """A row with no evidence fields at all, and a row with only
    unrelated fields, both fall fully into unknown on both axes."""
    ambiguous_rows = [
        {},
        {"ticker": "AAPL", "signal_date": "2026-06-01"},
        {"note": "no version or origin info here"},
    ]
    for row in ambiguous_rows:
        result = classify_row(row)
        assert result.scoring_version == "unknown"
        assert result.origin == "unknown"


def test_cohort_report_counts_and_unknown_buckets():
    rows = [
        {"origin": "live"},
        {"origin": "synthetic"},
        {"live_or_local": "local"},  # unknown origin
        {},  # fully unknown
        {"origin": "backfill"},
    ]
    report = build_cohort_report(rows)

    assert report.total == 5
    # scoring_version is unknown for every row — no schema evidence exists.
    assert report.scoring_version_counts["unknown"] == 5
    assert report.unknown_scoring_version == 5

    assert report.origin_counts["live"] == 1
    assert report.origin_counts["synthetic"] == 1
    assert report.origin_counts["backfill"] == 1
    assert report.origin_counts["unknown"] == 2
    assert report.unknown_origin == 2

    # scoring_version is unknown for every row (no schema evidence exists
    # at all), so "fully unknown" reduces to "origin is unknown": the
    # {} row and the live_or_local="local" row.
    assert report.fully_unknown == 2

    # Every declared class appears in the counts, even at zero.
    for version in SCORING_VERSIONS:
        assert version in report.scoring_version_counts
    for origin in ORIGINS:
        assert origin in report.origin_counts


def test_build_cohort_report_never_mutates_input_rows():
    rows = [{"origin": "live"}, {"foo": "bar"}]
    snapshot = [dict(r) for r in rows]
    build_cohort_report(rows)
    assert rows == snapshot


def test_cohort_report_handles_empty_input():
    report = build_cohort_report([])
    assert report.total == 0
    assert report.fully_unknown == 0
    assert all(v == 0 for v in report.scoring_version_counts.values())
    assert all(v == 0 for v in report.origin_counts.values())
