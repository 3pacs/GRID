"""Tests for normalization.entity_map.

Covers the critical paths flagged in the consolidated audit (CRITICAL #11):
the EntityMap class had 1054 LOC and zero coverage. These tests pin down
the runtime behavior so future edits can't silently regress mapping
resolution, suggestion, or duplicate detection.

The DB engine is mocked — entity_map's only real DB interaction is
loading feature_registry rows into an in-memory cache, which we feed
directly via fixtures.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from normalization import entity_map
from normalization.entity_map import EntityMap, NEW_MAPPINGS_V2, SEED_MAPPINGS


@pytest.fixture
def fake_engine_with_features():
    """Engine where feature_registry has a small known set of names."""
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn

    # Cover a sample from SEED_MAPPINGS so get_feature_id has hits.
    # Names taken directly from SEED_MAPPINGS so the test exercises the
    # real lookup path rather than a synthetic one.
    sample_features = [
        (1, "yld_curve_2s10s"),  # T10Y2Y maps to this
        (2, "sp500_full"),        # YF:^GSPC:close
        (3, "vix_spot"),          # YF:^VIX:close
        (4, "yc_10y"),            # DGS10
    ]
    conn.execute.return_value.fetchall.return_value = sample_features
    return engine


@pytest.fixture
def fake_engine_empty():
    """Engine where feature_registry is empty — exercises the
    degraded-path branch where mappings exist but no IDs resolve."""
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    conn.execute.return_value.fetchall.return_value = []
    return engine


def test_init_loads_feature_cache(fake_engine_with_features) -> None:
    em = EntityMap(db_engine=fake_engine_with_features)
    assert em._feature_cache == {
        "yld_curve_2s10s": 1,
        "sp500_full": 2,
        "vix_spot": 3,
        "yc_10y": 4,
    }


def test_get_feature_id_known_mapping(fake_engine_with_features) -> None:
    em = EntityMap(db_engine=fake_engine_with_features)
    # T10Y2Y → yld_curve_2s10s in SEED_MAPPINGS, id=1 in our cache.
    fid = em.get_feature_id("T10Y2Y")
    assert fid == 1


def test_get_feature_id_unknown_series(fake_engine_with_features) -> None:
    em = EntityMap(db_engine=fake_engine_with_features)
    # Made-up series id that's not in SEED_MAPPINGS at all.
    assert em.get_feature_id("BOGUS_SERIES_XYZ") is None


def test_get_feature_id_mapped_but_no_db_row(fake_engine_with_features) -> None:
    em = EntityMap(db_engine=fake_engine_with_features)
    # Pick a SEED_MAPPINGS entry whose feature name is not in our
    # fixture's small cache. The mapping resolves but the DB lookup
    # returns nothing — must return None, not crash.
    found_unmapped = False
    for series_id, feature_name in SEED_MAPPINGS.items():
        if feature_name not in em._feature_cache:
            assert em.get_feature_id(series_id) is None
            found_unmapped = True
            break
    assert found_unmapped, "Expected at least one SEED_MAPPINGS entry not in the test cache"


def test_get_all_mappings_returns_copy(fake_engine_with_features) -> None:
    em = EntityMap(db_engine=fake_engine_with_features)
    out = em.get_all_mappings()
    assert isinstance(out, dict)
    assert len(out) >= len(SEED_MAPPINGS)  # Includes V2 merge
    # Mutating the return must not mutate SEED_MAPPINGS.
    out["__TEST_MUTATION__"] = "should not leak"
    assert "__TEST_MUTATION__" not in SEED_MAPPINGS


def test_load_v2_mappings_idempotent(fake_engine_with_features) -> None:
    em = EntityMap(db_engine=fake_engine_with_features)
    # SEED_MAPPINGS is module-global; first __init__ already merged V2.
    # Re-running must not change the size further (no double-merge).
    before = dict(SEED_MAPPINGS)
    em.load_v2_mappings()
    after = dict(SEED_MAPPINGS)
    assert before == after


def test_suggest_mapping_returns_top_3(fake_engine_with_features) -> None:
    em = EntityMap(db_engine=fake_engine_with_features)
    suggestions = em.suggest_mapping("sp500")
    assert isinstance(suggestions, list)
    assert len(suggestions) <= 3
    # The closest match in our cache should be sp500_full.
    assert "sp500_full" in suggestions


def test_suggest_mapping_empty_cache(fake_engine_empty) -> None:
    em = EntityMap(db_engine=fake_engine_empty)
    # No features cached → no suggestions, but no crash.
    assert em.suggest_mapping("anything") == []


def test_suggest_mapping_normalises_special_chars(fake_engine_with_features) -> None:
    em = EntityMap(db_engine=fake_engine_with_features)
    # Verify the colon/caret/equals normalisation actually runs by
    # passing one of the noisy series ids and checking we still get a
    # ranked list back. We're testing the codepath, not the heuristic
    # quality of any specific suggestion.
    suggestions = em.suggest_mapping("YF:^VIX:close")
    assert isinstance(suggestions, list)
    assert len(suggestions) <= 3
    assert all(isinstance(s, str) for s in suggestions)


def test_seed_mappings_no_duplicates_in_v2_merge() -> None:
    """V2 mappings must not overwrite anything in SEED_MAPPINGS — the
    contract of load_v2_mappings is to *add* missing entries, not
    overwrite. Any overlap with a different value would be a silent
    semantic change."""
    for raw_id, v2_value in NEW_MAPPINGS_V2.items():
        if raw_id in SEED_MAPPINGS:
            assert SEED_MAPPINGS[raw_id] == v2_value, (
                f"V2 conflict on {raw_id}: SEED={SEED_MAPPINGS[raw_id]!r} "
                f"vs V2={v2_value!r}"
            )


def test_module_constants_are_dicts() -> None:
    """Sanity: the module-level mapping tables are dicts of
    str -> str. Catches accidental schema drift if someone wraps
    values in something fancier."""
    for name, table in (("SEED_MAPPINGS", SEED_MAPPINGS), ("NEW_MAPPINGS_V2", NEW_MAPPINGS_V2)):
        assert isinstance(table, dict), f"{name} must be a dict"
        for k, v in table.items():
            assert isinstance(k, str), f"{name}: non-str key {k!r}"
            assert isinstance(v, str), f"{name}[{k!r}]: non-str value {v!r}"


def test_init_idempotent_with_repeat_load(fake_engine_with_features) -> None:
    """Constructing two EntityMaps in the same process must not corrupt
    SEED_MAPPINGS — the V2 merge needs to remain a no-op on the second
    call."""
    n_before = len(SEED_MAPPINGS)
    EntityMap(db_engine=fake_engine_with_features)
    EntityMap(db_engine=fake_engine_with_features)
    assert len(SEED_MAPPINGS) == n_before


def test_module_loadable_without_db() -> None:
    """The entity_map module must import cleanly without a live DB.
    Catches the regression where someone wires a runtime DB call into
    module scope."""
    # The fixtures import it; this just asserts the SEED_MAPPINGS table
    # is non-trivial so the module didn't fail to populate.
    assert len(SEED_MAPPINGS) > 50, "SEED_MAPPINGS suspiciously small"
    assert "T10Y2Y" in SEED_MAPPINGS
    assert "VIX" in str(SEED_MAPPINGS) or any("vix" in v.lower() for v in SEED_MAPPINGS.values())


def test_difflib_imported() -> None:
    """suggest_mapping uses difflib; assert the module exposes it so the
    method can run without an ImportError."""
    assert hasattr(entity_map, "difflib")
