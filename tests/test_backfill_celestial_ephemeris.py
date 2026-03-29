from __future__ import annotations

from datetime import date

from scripts.backfill_celestial_ephemeris import compute_ephemeris_day


def test_compute_ephemeris_day_includes_extended_mystical_features() -> None:
    features = compute_ephemeris_day(date(2026, 3, 29))

    assert "ephemeris.hard_aspect_count" in features
    assert "ephemeris.soft_aspect_count" in features
    assert "ephemeris.lunar_age_days" in features
    assert "ephemeris.tithi_index" in features
    assert "ephemeris.phase_bucket" in features
    assert "ephemeris.nakshatra_pada" in features

    assert 0.0 <= features["ephemeris.lunar_phase"] <= 1.0
    assert 0.0 <= features["ephemeris.lunar_illumination"] <= 100.0
    assert 0.0 <= features["ephemeris.lunar_age_days"] <= 29.53059
    assert 0.0 <= features["ephemeris.nakshatra_index"] <= 26.0
    assert 1.0 <= features["ephemeris.nakshatra_pada"] <= 4.0
    assert 0.0 <= features["ephemeris.tithi_index"] <= 29.0
    assert 0.0 <= features["ephemeris.phase_bucket"] <= 7.0


def test_compute_ephemeris_day_supports_far_historical_dates() -> None:
    ancient = compute_ephemeris_day(date(1000, 1, 1))
    modern = compute_ephemeris_day(date(2026, 3, 29))

    assert ancient["ephemeris.mars.longitude"] != modern["ephemeris.mars.longitude"]
    assert 0.0 <= ancient["ephemeris.nakshatra_index"] <= 26.0
    assert 0.0 <= ancient["ephemeris.phase_bucket"] <= 7.0
