"""finra.ats_total_volume must resolve to a feature_registry row.

On 2026-10-20 03:25:36Z, after the FINRA ATS repair wrote the first 50
``finra.ats_total_volume`` rows griddb has ever held, grid-hermes logged::

    normalization.entity_map:get_feature_id:1308 -- Mapping exists
    (finra.ats_total_volume -> finra_ats_total_volume) but feature not in
    registry -- further misses for this series_id are counted, not logged

A ghost mapping (migrations 0062 / 0063, ``raw_sql_port_20260914``): the
static mapping exists, the registry row does not, and the resolver drops
every observation. ``migrations/versions/finra_ats_feature_20261020.py``
registers the row. These tests load the seed from that revision, pin it to
the mapping in ``normalization/entity_map.py``, and prove the resolution
path over a registry that holds exactly what the revision inserts.

The DB engine is mocked, as in tests/test_entity_map_pattern_resolution.py
-- the only real DB interaction is loading feature_registry into a cache.
"""
from __future__ import annotations

import importlib
import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from normalization.entity_map import NEW_MAPPINGS_V2, SEED_MAPPINGS, EntityMap

ALL_MAPPINGS = {**SEED_MAPPINGS, **NEW_MAPPINGS_V2}
REVISION = (
    Path(__file__).resolve().parents[1]
    / "migrations" / "versions" / "finra_ats_feature_20261020.py"
)
SERIES_ID = "finra.ats_total_volume"
FEATURE = "finra_ats_total_volume"


def _names_registered_by_revision() -> set[str]:
    """Feature names the revision inserts, parsed from its INSERT literal."""
    body = REVISION.read_text().split("VALUES", 1)[1]
    return set(re.findall(r"^\s*\('([a-z0-9_]+)',", body, flags=re.MULTILINE))


def _engine(features: list[tuple[int, str]]) -> MagicMock:
    """Mock engine whose feature_registry holds exactly ``features``."""
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    conn.execute.return_value.fetchall.return_value = list(features)
    return engine


class TestRevisionCoversTheGhost:
    def test_revision_registers_exactly_the_logged_feature(self):
        assert _names_registered_by_revision() == {FEATURE}

    def test_revision_exports_the_same_names_it_inserts(self):
        mod = importlib.import_module(
            "migrations.versions.finra_ats_feature_20261020"
        )
        assert set(mod.REGISTERED_FEATURES) == _names_registered_by_revision()

    def test_the_mapping_the_log_line_names_is_still_in_place(self):
        assert SEED_MAPPINGS[SERIES_ID] == FEATURE

    def test_every_registered_name_is_targeted_by_a_mapping(self):
        # The inverse of the ghost: a row nothing maps onto is dead weight.
        targets = set(ALL_MAPPINGS.values())
        for name in _names_registered_by_revision():
            assert name in targets, f"revision registers {name} but nothing maps to it"

    def test_revision_is_idempotent_and_parameter_free(self):
        src = REVISION.read_text()
        assert "ON CONFLICT (name) DO NOTHING" in src
        assert "%s" not in src and "{}" not in src

    def test_revision_id_fits_alembic_version_column(self):
        mod = importlib.import_module(
            "migrations.versions.finra_ats_feature_20261020"
        )
        assert len(mod.revision) <= 32
        assert mod.revision == REVISION.stem


class TestResolutionOverTheSeededRegistry:
    def test_series_resolves_once_the_seed_is_in_the_registry(self):
        # Registry = exactly what the revision inserts (ids are arbitrary).
        registry = [(500 + i, n) for i, n in enumerate(sorted(_names_registered_by_revision()))]
        em = EntityMap(db_engine=_engine(registry))

        feature_id = em.get_feature_id(SERIES_ID)

        assert feature_id == {n: i for i, n in registry}[FEATURE]
        report = em.missing_feature_report()
        assert report["lookups_missed"] == 0
        assert report["unregistered_features"] == []

    def test_without_the_seed_the_series_is_a_counted_miss(self):
        # The pre-migration state: mapping present, registry row absent.
        em = EntityMap(db_engine=_engine([(1, "yld_curve_2s10s")]))

        assert em.get_feature_id(SERIES_ID) is None
        assert em.get_feature_id(SERIES_ID) is None

        report = em.missing_feature_report()
        assert report["unregistered_features"] == [FEATURE]
        assert report["top_series"] == [[SERIES_ID, 2]]

    @pytest.mark.parametrize("sibling", ["finra.ats_dark_pct", "finra.short_interest_total"])
    def test_siblings_stay_mapped_but_deliberately_unregistered(self, sibling):
        # No writer today (receipt 2026-10-20): dark_pct has no source field in
        # weeklySummary; short interest still 400s. Mapping kept, row withheld.
        assert sibling in SEED_MAPPINGS
        assert SEED_MAPPINGS[sibling] not in _names_registered_by_revision()
