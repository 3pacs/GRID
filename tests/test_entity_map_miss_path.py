"""Tests for EntityMap's failed-lookup path.

``get_feature_id`` used to make every miss expensive and noisy: a name
absent from ``feature_registry`` re-ran ``_load_feature_cache()`` — a full
``feature_registry`` SELECT — and logged a warning, on *every* call. The
common miss is permanent (a SEED_MAPPINGS entry whose target was never
registered), and the resolver calls this once per ``(series_id, obs_date)``
group, so the same dead name was re-queried and re-warned thousands of
times per run.

Measured on griddb 2026-09-11 (ops-exec run 34550663319): 547,038 of
569,400 groups in a live 2-day window were misses of exactly this shape,
and the grouping loop cost 54.6s of a 61.5s dry run while both SQL scans
together cost 9.1s.

These tests pin the fix: one refresh per unknown name, one warning per
series_id, and a countable summary instead of a log flood.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from loguru import logger as log

from normalization.entity_map import SEED_MAPPINGS, EntityMap


# A real mapping whose target is deliberately absent from the fixture
# registry below — the live shape of the miss (YF:IEF:close -> ief_full,
# mapped but never registered). It arrives via NEW_MAPPINGS_V2, which
# EntityMap.__init__ merges into SEED_MAPPINGS, so it is only visible
# after an EntityMap has been constructed.
MAPPED_BUT_UNREGISTERED = "YF:IEF:close"


@pytest.fixture
def counting_engine():
    """Engine that counts how many times feature_registry is read.

    ``feature_registry`` holds one name that no SEED_MAPPINGS entry points
    at, so every mapping under test misses.
    """
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn

    calls = {"feature_registry": 0}

    def _execute(statement, *args, **kwargs):
        sql = str(statement)
        result = MagicMock()
        if "feature_registry" in sql:
            calls["feature_registry"] += 1
            result.fetchall.return_value = [(1, "some_other_feature")]
        else:
            result.fetchall.return_value = []
        return result

    conn.execute.side_effect = _execute
    engine.registry_reads = calls
    return engine


@pytest.fixture
def warnings_captured():
    records: list[str] = []
    sink_id = log.add(lambda msg: records.append(str(msg)), level="WARNING")
    yield records
    log.remove(sink_id)


def _reads(em: EntityMap, engine) -> int:
    """feature_registry reads attributable to lookups, not construction."""
    return engine.registry_reads["feature_registry"]


class TestRepeatedMissIsCheap:

    def test_repeated_miss_does_not_requery_the_registry(self, counting_engine):
        em = EntityMap(db_engine=counting_engine)
        assert SEED_MAPPINGS.get(MAPPED_BUT_UNREGISTERED), (
            "fixture assumes this series_id is mapped"
        )
        baseline = _reads(em, counting_engine)

        assert em.get_feature_id(MAPPED_BUT_UNREGISTERED) is None
        after_first = _reads(em, counting_engine)
        assert after_first == baseline + 1, (
            "the first miss should refresh the cache exactly once"
        )

        for _ in range(500):
            assert em.get_feature_id(MAPPED_BUT_UNREGISTERED) is None

        assert _reads(em, counting_engine) == after_first, (
            "500 further misses re-read feature_registry — this is the 54.6s"
        )

    def test_distinct_series_sharing_a_dead_target_refresh_once(
        self, counting_engine,
    ):
        """The negative cache keys on the feature name, not the series_id.

        Several SEED_MAPPINGS entries can point at the same unregistered
        name; the second one must not pay for another refresh.
        """
        em = EntityMap(db_engine=counting_engine)
        target = SEED_MAPPINGS[MAPPED_BUT_UNREGISTERED]
        siblings = [
            sid for sid, name in SEED_MAPPINGS.items() if name == target
        ]
        assert len(siblings) >= 1

        baseline = _reads(em, counting_engine)
        for sid in siblings:
            assert em.get_feature_id(sid) is None
        assert _reads(em, counting_engine) == baseline + 1

    def test_unmapped_series_id_never_touches_the_registry(self, counting_engine):
        """No SEED_MAPPINGS entry at all → nothing to look up."""
        em = EntityMap(db_engine=counting_engine)
        baseline = _reads(em, counting_engine)

        for _ in range(50):
            assert em.get_feature_id("NOT:A:MAPPED:SERIES") is None

        assert _reads(em, counting_engine) == baseline

    def test_cache_reload_gives_a_missing_name_another_chance(
        self, counting_engine,
    ):
        """A feature registered later must still resolve.

        The negative cache is cleared by _load_feature_cache, so it can
        never pin a name as absent past the point the registry changes.
        """
        em = EntityMap(db_engine=counting_engine)
        name = SEED_MAPPINGS[MAPPED_BUT_UNREGISTERED]

        assert em.get_feature_id(MAPPED_BUT_UNREGISTERED) is None
        assert name in em._unregistered_features

        # The feature gets registered; something reloads the cache.
        conn = counting_engine.connect.return_value.__enter__.return_value

        def _execute(statement, *args, **kwargs):
            result = MagicMock()
            if "feature_registry" in str(statement):
                counting_engine.registry_reads["feature_registry"] += 1
                result.fetchall.return_value = [(1, "some_other_feature"), (99, name)]
            else:
                result.fetchall.return_value = []
            return result

        conn.execute.side_effect = _execute
        em._load_feature_cache()

        assert em._unregistered_features == set()
        assert em.get_feature_id(MAPPED_BUT_UNREGISTERED) == 99


class TestRepeatedMissIsQuiet:

    def test_warning_is_emitted_once_per_series_id(
        self, counting_engine, warnings_captured,
    ):
        em = EntityMap(db_engine=counting_engine)
        warnings_captured.clear()

        for _ in range(100):
            em.get_feature_id(MAPPED_BUT_UNREGISTERED)

        hits = [r for r in warnings_captured if MAPPED_BUT_UNREGISTERED in r]
        assert len(hits) == 1, (
            f"expected one warning for this series_id, got {len(hits)}"
        )
        assert "not in registry" in hits[0]

    def test_each_distinct_series_id_still_gets_its_own_warning(
        self, counting_engine, warnings_captured,
    ):
        """Silencing repeats must not silence a genuinely new bad mapping."""
        em = EntityMap(db_engine=counting_engine)
        target = SEED_MAPPINGS[MAPPED_BUT_UNREGISTERED]
        siblings = [
            sid for sid, name in SEED_MAPPINGS.items() if name == target
        ][:3]
        warnings_captured.clear()

        for sid in siblings:
            for _ in range(10):
                em.get_feature_id(sid)

        for sid in siblings:
            assert sum(1 for r in warnings_captured if sid in r) == 1


class TestMissingFeatureReport:

    def test_report_counts_every_missed_lookup(self, counting_engine):
        em = EntityMap(db_engine=counting_engine)

        for _ in range(7):
            em.get_feature_id(MAPPED_BUT_UNREGISTERED)
        for _ in range(3):
            em.get_feature_id("NOT:A:MAPPED:SERIES")

        report = em.missing_feature_report()
        assert report["lookups_missed"] == 10
        assert report["series_ids"] == 2
        assert SEED_MAPPINGS[MAPPED_BUT_UNREGISTERED] in report["unregistered_features"]
        assert report["top_series"][0] == [MAPPED_BUT_UNREGISTERED, 7]

    def test_report_is_empty_when_nothing_missed(self, counting_engine):
        em = EntityMap(db_engine=counting_engine)
        report = em.missing_feature_report()
        assert report["lookups_missed"] == 0
        assert report["series_ids"] == 0
        assert report["top_series"] == []

    def test_top_series_is_capped(self, counting_engine):
        em = EntityMap(db_engine=counting_engine)
        for i in range(25):
            em.get_feature_id(f"UNKNOWN:{i}")
        assert len(em.missing_feature_report()["top_series"]) == 10
        assert em.missing_feature_report()["series_ids"] == 25


def test_successful_lookup_is_unaffected():
    """The hit path must not have acquired any new cost or silence."""
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    conn.execute.return_value.fetchall.return_value = [(1, "yld_curve_2s10s")]

    em = EntityMap(db_engine=engine)
    assert em.get_feature_id("T10Y2Y") == 1
    assert em.missing_feature_report()["lookups_missed"] == 0
