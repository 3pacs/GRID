"""One resolver miss count hides two populations; report them apart, without a no-loss claim.

grid-hermes 2026-10-22/23 (release 60446c6) logged every cycle "Resolution skipped
7211 lookup(s) across 7211 unmapped series_id(s); 1 mapped feature name(s) are
missing from feature_registry", and the Hermes issue called those ids "pulled
successfully and then dropped". Two different things sit in that count: series whose
static mapping targets an unregistered feature (rows dropped at resolution until the
feature is registered) and series with no mapping at all. The second group is itself
two shapes: entity-grained feeds with a feed separator (YF:<ticker>:<field>,
LEGISLATION:*, edgar:8k:*, EARNINGS:*) whose traced consumers read raw_series
directly, which proves nothing about untraced consumers or freshness; and
feature-shaped producer output with no mapping (the scheduled GDELT theme features
such as gdelt_opec_tone), whose consumers and materialization are unknown. These tests
pin the classification and the operator-facing wording. No database, no I/O.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from loguru import logger as log

from normalization import entity_map as entity_map_module
from normalization.entity_map import SEED_MAPPINGS, EntityMap
from normalization.resolver import Resolver
from scripts import hermes_data_integrity as integrity
from tests.test_resolver import FakeRow, _mock_engine

# Mapped via NEW_MAPPINGS_V2 (merged into SEED_MAPPINGS at construction); the
# registry fixture below deliberately lacks its target, the live miss shape.
MAPPED_BUT_UNREGISTERED = "YF:IEF:close"
NO_MAPPING = [
    ("LEGISLATION:29-hr235:reported", 3),   # from the 2026-10-22 journal (entity feed)
    ("EARNINGS:ZLAB:actual", 2),            # from the 2026-10-22 journal (entity feed)
    ("YF:ZZZZ:close", 1),                   # pattern fallback finds nothing registered
    ("edgar:8k:0001234567-26-000001", 1),
    ("gdelt_opec_tone", 2),                 # scheduled GDELT theme feature, no mapping
    ("gdelt_sanctions_russia_tone", 1),     # scheduled GDELT theme feature, no mapping
]


@pytest.fixture
def registry_engine():
    """feature_registry holds one name no mapping points at: every lookup misses."""
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn

    def _execute(statement, *_args, **_kwargs):
        result = MagicMock()
        result.fetchall.return_value = (
            [(1, "some_other_feature")] if "feature_registry" in str(statement) else []
        )
        return result

    conn.execute.side_effect = _execute
    return engine


@pytest.fixture
def warnings_captured():
    records: list[str] = []
    sink_id = log.add(lambda msg: records.append(str(msg)), level="WARNING")
    yield records
    log.remove(sink_id)


@pytest.mark.parametrize("series_id, family", [
    ("YF:SPY:close", "YF"),
    ("LEGISLATION:29-hr235:reported", "LEGISLATION"),
    ("hf_news.bloomberg_financial_news", "hf_news"),
    ("wikidata.person.Q42", "wikidata"),
    ("gdelt_opec_tone", "gdelt"),           # feature-shaped: first underscore segment
    ("fed_funds_rate", "fed"),
    ("T10Y2Y", "bare"),                     # no separator at all
    ("a.b:c", "a"),
    ("  edgar:8k:AAPL ", "edgar"),
])
def test_series_family_groups_by_first_separator(series_id: str, family: str) -> None:
    assert entity_map_module.series_family(series_id) == family


@pytest.mark.parametrize("series_id, shaped", [
    ("gdelt_opec_tone", True), ("T10Y2Y", True), ("fed_funds_rate", True),
    ("YF:SPY:close", False), ("hf_news.bloomberg", False), ("LEGISLATION:1:reported", False),
])
def test_is_feature_shaped(series_id: str, shaped: bool) -> None:
    assert entity_map_module.is_feature_shaped(series_id) is shaped


def _exercise(em: EntityMap) -> None:
    assert em.get_feature_id(MAPPED_BUT_UNREGISTERED) is None
    for series_id, hits in NO_MAPPING:
        assert series_id not in SEED_MAPPINGS, f"fixture assumes {series_id} is unmapped"
        for _ in range(hits):
            assert em.get_feature_id(series_id) is None


def test_report_separates_mapped_unregistered_from_no_mapping(registry_engine) -> None:
    em = EntityMap(db_engine=registry_engine)
    target = SEED_MAPPINGS[MAPPED_BUT_UNREGISTERED]
    _exercise(em)
    report = em.missing_feature_report()
    # Existing contract unchanged.
    assert report["lookups_missed"] == 11 and report["series_ids"] == 7
    assert report["unregistered_features"] == [target]
    assert report["top_series"][0] == ["LEGISLATION:29-hr235:reported", 3]
    # The population that drops rows, apart from the ones that do not resolve at all.
    assert report["mapped_unregistered_count"] == 1
    assert report["mapped_unregistered_series"] == [[MAPPED_BUT_UNREGISTERED, target, 1]]
    assert report["no_mapping_series_ids"] == 6
    assert report["no_mapping_feature_named"] == 2   # the two GDELT producer names stay visible
    assert report["by_family"] == [
        ["gdelt", 2, 3], ["LEGISLATION", 1, 3], ["EARNINGS", 1, 2], ["YF", 1, 1], ["edgar", 1, 1],
    ]


def test_empty_report_carries_the_new_keys(registry_engine) -> None:
    report = EntityMap(db_engine=registry_engine).missing_feature_report()
    assert report["lookups_missed"] == 0 and report["series_ids"] == 0
    assert report["mapped_unregistered_count"] == 0
    assert report["mapped_unregistered_series"] == []
    assert report["no_mapping_series_ids"] == 0 and report["no_mapping_feature_named"] == 0
    assert report["by_family"] == []


_LIVE_SHAPE = {
    "lookups_missed": 7211, "series_ids": 7211,
    "unregistered_features": ["ghost_feature"],
    "top_series": [["YF:AAPL:close", 9], ["LEGISLATION:29-hr235:reported", 3]],
    "mapped_unregistered_count": 1,
    "mapped_unregistered_series": [["SOME:series", "ghost_feature", 4]],
    "no_mapping_series_ids": 7210,
    "no_mapping_feature_named": 7,
    "by_family": [["YF", 4900, 4900], ["LEGISLATION", 1200, 1300], ["gdelt", 7, 7]],
}
NO_LOSS_CLAIMS = ("lose nothing", "loses nothing", "not data lost", "only one of them loses")


def test_describe_leads_with_the_actionable_population_without_a_no_loss_claim() -> None:
    title, detail = integrity.describe_unmapped_report(_LIVE_SHAPE, _LIVE_SHAPE["top_series"])
    assert title == (
        "1 series_id(s) mapped to 1 feature name(s) missing from feature_registry; "
        "7210 series_id(s) without entity_map entry"
    )
    assert "SOME:series -> ghost_feature (4)" in detail and "REGISTER_GHOST_FEATURES" in detail
    assert "YF (4900 ids, 4900 lookups)" in detail and "gdelt (7 ids, 7 lookups)" in detail
    assert "7 of them are feature-shaped ids" in detail and "producer output" in detail
    assert "not proof for untraced consumers or of freshness" in detail
    assert "dropped because entity_map has no entry" not in detail
    assert not any(claim in detail for claim in NO_LOSS_CLAIMS)
    assert detail.endswith("Worst offenders: YF:AAPL:close (9), LEGISLATION:29-hr235:reported (3).")


def test_describe_without_mapped_misses_is_plain() -> None:
    report = {
        "lookups_missed": 5, "series_ids": 2, "unregistered_features": [],
        "top_series": [["a:b", 3], ["c:d", 2]], "mapped_unregistered_count": 0,
        "mapped_unregistered_series": [], "no_mapping_series_ids": 2,
        "no_mapping_feature_named": 0, "by_family": [["a", 1, 3], ["c", 1, 2]],
    }
    title, detail = integrity.describe_unmapped_report(report, report["top_series"])
    assert title == "2 series_id(s) without entity_map entry skipped at resolution"
    assert "REGISTER_GHOST_FEATURES" not in detail and "a (1 ids, 3 lookups)" in detail
    assert "0 of them are feature-shaped ids" in detail
    assert not any(claim in detail for claim in NO_LOSS_CLAIMS)


def test_describe_keeps_actionable_names_on_the_pre_revision_report_shape() -> None:
    """A report without the partition counters must not be relabelled as all-no-mapping."""
    report = {"lookups_missed": 3, "series_ids": 3, "unregistered_features": ["x"],
              "top_series": [["s", 3]]}
    title, detail = integrity.describe_unmapped_report(report, report["top_series"])
    assert title == ("1 feature name(s) missing from feature_registry; series partition "
                     "counts unknown; 3 series_id(s) missed at resolution")
    assert "x" in detail and "REGISTER_GHOST_FEATURES" in detail
    assert "unknown for this report shape" in detail
    assert "without entity_map entry skipped" not in title
    assert detail.endswith("Worst offenders: s (3).")


def test_describe_marks_unknown_feature_shaped_count_when_absent() -> None:
    report = {k: v for k, v in _LIVE_SHAPE.items() if k != "no_mapping_feature_named"}
    _, detail = integrity.describe_unmapped_report(report, report["top_series"])
    assert "an unknown number of them are feature-shaped ids" in detail


def test_report_unmapped_series_logs_the_reworded_issue(registry_engine) -> None:
    recent = [(MAPPED_BUT_UNREGISTERED,), ("LEGISLATION:29-hr235:reported",),
              ("EARNINGS:ZLAB:actual",), ("gdelt_opec_tone",)]
    begin_conn = MagicMock()

    def _begin_execute(statement, *_args, **_kwargs):
        result = MagicMock()
        result.fetchall.return_value = recent if "FROM raw_series" in str(statement) else []
        return result

    begin_conn.execute.side_effect = _begin_execute
    registry_engine.begin.return_value.__enter__.return_value = begin_conn
    with patch("scripts.hermes_data_integrity.log_issue") as logged:
        out = integrity.report_unmapped_series(registry_engine, cycle_number=317)
    assert out["status"] == "ok"
    assert out["mapped_unregistered_count"] == 1 and out["no_mapping_series_ids"] == 3
    assert out["no_mapping_feature_named"] == 1
    kwargs = logged.call_args.kwargs
    assert kwargs["title"] == (
        "1 series_id(s) mapped to 1 feature name(s) missing from feature_registry; "
        "3 series_id(s) without entity_map entry"
    )
    target = SEED_MAPPINGS[MAPPED_BUT_UNREGISTERED]
    assert f"{MAPPED_BUT_UNREGISTERED} -> {target} (1)" in kwargs["detail"]
    assert "gdelt (1 ids, 1 lookups)" in kwargs["detail"]
    assert "1 of them are feature-shaped ids" in kwargs["detail"]
    assert "dropped because entity_map has no entry" not in kwargs["detail"]
    assert not any(claim in kwargs["detail"] for claim in NO_LOSS_CLAIMS)
    assert kwargs["cycle_number"] == 317 and kwargs["source"] == "entity_map"
    assert kwargs["severity"] == "WARNING" and kwargs["category"] == "data_integrity"


@patch("normalization.resolver.EntityMap")
def test_resolver_warning_names_both_populations(MockEntityMap, warnings_captured) -> None:
    mock_map = MagicMock()
    mock_map.get_feature_id.return_value = None
    mock_map.missing_feature_report.return_value = dict(_LIVE_SHAPE)
    MockEntityMap.return_value = mock_map
    pending = [FakeRow(("UNKNOWN", date(2026, 1, 1), 100.0, "s1",
                        datetime(2026, 1, 2, tzinfo=timezone.utc), 1, "Src1"))]
    engine, _ = _mock_engine(pending_rows=pending)
    summary = Resolver(db_engine=engine).resolve_pending()
    assert summary["resolved"] == 0
    assert summary["unmapped"]["mapped_unregistered_count"] == 1
    line = next(r for r in warnings_captured if "missing from feature_registry" in r)
    assert "1 series_id(s) mapped to 1 feature name(s) missing from feature_registry" in line
    assert "7210 series_id(s) have no entity_map entry" in line
    assert "['YF', 4900, 4900]" in line and "7211 lookup(s) skipped" in line
    assert "Resolution skipped 7211 lookup(s) across 7211 unmapped" not in line
