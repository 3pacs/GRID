"""Contract tests for ``store/availability.py``."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from store import availability as av


def test_unavailable_has_no_numbers_and_a_reason():
    p = av.unavailable("no VIX close series in resolved_series", source="resolved_series", vix=1, pct=2)
    assert p["available"] is False and p["status"] == "unavailable"
    assert p["reason"].startswith("no VIX")
    assert p["as_of"] is None
    assert p["vix"] is None and p["pct"] is None
    assert p["source"] == "resolved_series"
    numeric = {k: v for k, v in p.items() if isinstance(v, (int, float)) and not isinstance(v, bool)}
    assert numeric == {}


def test_unavailable_requires_a_reason():
    with pytest.raises(ValueError):
        av.unavailable("   ")


def test_available_carries_provenance_and_data_as_of_not_now():
    prov = av.Provenance(source="raw_series:WALCL", as_of=date(2026, 9, 16), vintage=datetime(2026, 9, 16, 21, tzinfo=timezone.utc))
    p = av.available(provenance=prov, walcl_musd=6_746_548.0)
    assert p["available"] is True and p["status"] == "ok"
    assert p["as_of"] == "2026-09-16"
    assert p["provenance"]["source"] == "raw_series:WALCL"
    assert p["provenance"]["estimated"] is False
    assert p["walcl_musd"] == 6_746_548.0


def test_partial_names_its_gaps():
    prov = av.Provenance(source="sector_health", as_of=date(2026, 9, 17))
    p = av.partial(provenance=prov, missing=["insider", "congress"], score=55.0)
    assert p["status"] == "partial" and p["missing"] == ["insider", "congress"]


def test_estimated_values_are_labelled():
    prov = av.Provenance(source="model:black_scholes", as_of=date(2026, 9, 17), basis="sigma=0.25 r=0.05", estimated=True)
    assert av.available(provenance=prov, entry_price=1.23)["provenance"]["estimated"] is True


def test_freshness_unknown_when_no_as_of():
    f = av.freshness(None, stale_after_days=3)
    assert f.stale is None and f.age_days is None and f.as_of is None


def test_freshness_ages_the_data_date():
    f = av.freshness(date(2026, 9, 10), stale_after_days=3, today=date(2026, 9, 17))
    assert f.age_days == 7 and f.stale is True
    f2 = av.freshness(datetime(2026, 9, 16, 12, tzinfo=timezone.utc), stale_after_days=3, today=date(2026, 9, 17))
    assert f2.age_days == 1 and f2.stale is False


def test_measured_or_none_never_invents_a_midpoint():
    assert av.measured_or_none(None) is None
    assert av.measured_or_none(0) == 0.0          # a real zero is kept, not rewritten
    assert av.measured_or_none("0.5") == 0.5
    assert av.measured_or_none("abc") is None
    assert av.measured_or_none(float("nan")) is None
    assert av.measured_or_none(float("inf")) is None


def test_mean_of_available_ignores_none_and_reports_n():
    assert av.mean_of_available([None, None]) == (None, 0)
    m, n = av.mean_of_available([0.2, None, 0.6])
    assert m == pytest.approx(0.4) and n == 2


def test_unavailable_is_never_cacheable_but_partial_is():
    assert av.cacheable(av.unavailable("x")).cache is False
    prov = av.Provenance(source="s", as_of=date(2026, 9, 17))
    assert av.cacheable(av.partial(provenance=prov, missing=["a"], v=1)).cache is True
    assert av.cacheable(av.available(provenance=prov, v=1)).cache is True
    assert av.is_unavailable(None) is True
