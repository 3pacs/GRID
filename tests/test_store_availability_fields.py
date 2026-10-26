"""Contract tests for ``store/availability_fields.py`` (per-field record, W2b)."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from store import availability_fields as af


def test_measured_field_has_provenance_and_availability():
    r = af.measured_field(
        6_746_548.0,
        unit="musd",
        obs_date=date(2026, 9, 16),
        ingested_at=datetime(2026, 9, 16, 21, tzinfo=timezone.utc),
        source_catalog="fred",
        series_id="WALCL",
        coverage_fraction=1.0,
    )
    d = r.to_dict()
    assert d["availability"] == "available"
    assert d["provenance"] == "measured"
    assert d["value"] == 6_746_548.0
    assert d["obs_date"] == "2026-09-16"
    assert d["ingested_at"] == "2026-09-16T21:00:00+00:00"
    assert d["source_catalog"] == "fred" and d["series_id"] == "WALCL"
    assert d["coverage_fraction"] == 1.0


def test_derived_and_modeled_fields_are_available_but_not_measured():
    d1 = af.derived_field(1.23, calculation_version="spread_v2").to_dict()
    assert d1["availability"] == "available" and d1["provenance"] == "derived"

    d2 = af.modeled_field(0.42, calculation_version="black_scholes_v1").to_dict()
    assert d2["availability"] == "available" and d2["provenance"] == "modeled"


def test_unavailable_field_has_no_value_and_no_provenance():
    r = af.unavailable_field(af.STALE_FETCH_FAILED, source_catalog="fred", series_id="WALCL", value=999)
    d = r.to_dict()
    assert d["availability"] == "unavailable"
    assert d["provenance"] is None
    assert d["value"] is None  # never invented, even if a caller tried to pass one
    assert d["stale_reason"] == "fetch_failed"


def test_invalid_field_carries_no_provenance():
    r = af.invalid_field(af.STALE_PARSER_ERROR, value="not-a-number", source_catalog="census")
    d = r.to_dict()
    assert d["availability"] == "invalid"
    assert d["provenance"] is None
    assert d["stale_reason"] == "parser_error"


def test_never_configured_source_is_unavailable_not_fetch_failed():
    r = af.unavailable_field(af.STALE_NEVER_CONFIGURED, source_catalog="acme_widgets")
    assert r.availability == "unavailable"
    assert r.stale_reason == "never_configured"


def test_available_field_can_still_carry_a_stale_reason():
    # A successful pull from 40 days ago: the value is real and available,
    # but the page must still be able to say it is stale.
    r = af.measured_field(1.0, stale_reason=af.STALE_STALE, ingested_at=datetime(2026, 8, 1, tzinfo=timezone.utc))
    assert r.availability == "available"
    assert r.stale_reason == "stale"


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(availability="available", provenance=None),  # available needs a provenance
        dict(availability="unavailable", provenance="measured"),  # unavailable can't have one
        dict(availability="bogus"),
        dict(availability="available", provenance="measured", stale_reason="bogus"),
        dict(availability="available", provenance="measured", coverage_fraction=1.5),
    ],
)
def test_invalid_combinations_raise(kwargs):
    with pytest.raises(ValueError):
        af.FieldRecord(**kwargs)


def test_coverage_or_none_never_invents_zero():
    assert af.coverage_or_none(None, 10) is None
    assert af.coverage_or_none(5, None) is None
    assert af.coverage_or_none(5, 0) is None  # undefined ratio, not 0.0
    assert af.coverage_or_none(0, 10) == 0.0  # a real measured zero stays 0.0
    assert af.coverage_or_none(5, 10) == 0.5


def test_unknown_field_name_is_rejected():
    with pytest.raises(TypeError):
        af.measured_field(1.0, not_a_real_field=True)
