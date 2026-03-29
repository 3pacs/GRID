from normalization.entity_map import NEW_MAPPINGS_V2


def test_ephemeris_raw_series_map_to_canonical_feature_names() -> None:
    assert NEW_MAPPINGS_V2["ephemeris.hard_aspect_count"] == "ephemeris_hard_aspect_count"
    assert NEW_MAPPINGS_V2["ephemeris.soft_aspect_count"] == "ephemeris_soft_aspect_count"
    assert NEW_MAPPINGS_V2["ephemeris.lunar_age_days"] == "ephemeris_lunar_age_days"
    assert NEW_MAPPINGS_V2["ephemeris.tithi_index"] == "ephemeris_tithi_index"
    assert NEW_MAPPINGS_V2["ephemeris.phase_bucket"] == "ephemeris_phase_bucket"
    assert NEW_MAPPINGS_V2["ephemeris.nakshatra_pada"] == "ephemeris_nakshatra_pada"
