"""REGISTER_GHOST_FEATURES must register only with a family it can state, and say which.

The series the resolver reports as mapped-but-unregistered on grid-hermes (2026-10-23
bounded journal read: ``gdelt_trade_conflict_volume``, written every scheduled cycle by
ingestion/altdata/gdelt.py) is what ``register_ghost_features`` exists to fix. It used
to insert ``family='unclassified'``, which schema.sql's feature_registry CHECK rejects.
The command scans every absent static mapping target, not one name, and the family it
writes is persistent registry metadata that the resolver's conflict policy reads, so the
inference is limited to unanimity among registered siblings: schema.sql seeds
``fed_funds_rate`` as ``rates`` beside ``fed_*`` rows of ``sentiment`` and ``macro``,
and a majority vote would have persisted ``sentiment``. Ambiguity is reported, never
resolved silently. No database.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from scripts import hermes_data_integrity as integrity

GHOST = "gdelt_trade_conflict_volume"
FED_GHOST = "fed_funds_rate"
REGISTRY = [
    ("yc_10y2y", "rates"),
    ("gdelt_fed_tone", "sentiment"),
    ("gdelt_recession_tone", "sentiment"),
    ("gdelt_actor_powell_tone", "sentiment"),
    # The reviewer's counterexample: fed_* siblings disagree (schema.sql seeds).
    ("fed_speech_frequency", "sentiment"),
    ("fed_tone_7d_avg", "sentiment"),
    ("fed_funds_3m_chg", "rates"),
    ("fed_balance_sheet", "macro"),
    ("zz_bogus", "unclassified"),  # a non-conformant sibling must not be copied
]
MAPPINGS = {GHOST: GHOST, "T10Y2Y": "yc_10y2y", "gdelt_fed_tone": "gdelt_fed_tone",
            "FEDFUNDS": FED_GHOST}


class _Map:
    def __init__(self, db_engine=None) -> None:
        pass

    def get_all_mappings(self) -> dict[str, str]:
        return dict(MAPPINGS)


@pytest.fixture
def engine():
    eng = MagicMock()
    conn = MagicMock()
    eng.begin.return_value.__enter__.return_value = conn
    conn.inserts: list[dict] = []

    def _execute(statement, params=None):
        sql = str(statement)
        result = MagicMock()
        if "SELECT name, family FROM feature_registry" in sql:
            result.__iter__.return_value = iter(REGISTRY)
        elif "INSERT INTO feature_registry" in sql:
            conn.inserts.append({"sql": sql, "params": dict(params or {})})
        return result

    conn.execute.side_effect = _execute
    eng.conn = conn
    return eng


# ── family rule ─────────────────────────────────────────────────────────────

def test_unanimous_valid_siblings_give_the_family() -> None:
    assert integrity.ghost_feature_family(GHOST, dict(REGISTRY)) == "sentiment"
    assert integrity.ghost_feature_family("yc_butterfly_2_5_10", dict(REGISTRY)) == "rates"


def test_reviewers_counterexample_is_ambiguous_not_a_majority_vote() -> None:
    counts = integrity.sibling_families(FED_GHOST, dict(REGISTRY))
    assert counts == {"sentiment": 2, "rates": 1, "macro": 1}
    assert integrity.ghost_feature_family(FED_GHOST, dict(REGISTRY)) is None


def test_no_siblings_is_unknown_not_a_fallback_family() -> None:
    assert integrity.ghost_feature_family("brand_new_thing", dict(REGISTRY)) is None
    assert integrity.sibling_families("brand_new_thing", dict(REGISTRY)) == {}
    assert not hasattr(integrity, "GHOST_FALLBACK_FAMILY")


def test_invalid_sibling_family_blocks_inference_and_is_visible() -> None:
    assert integrity.sibling_families("zz_new", dict(REGISTRY)) == {"<invalid>": 1}
    assert integrity.ghost_feature_family("zz_new", dict(REGISTRY)) is None
    mixed = {"ab_one": "vol", "ab_two": "unclassified"}
    assert integrity.ghost_feature_family("ab_new", mixed) is None


def test_two_valid_families_even_when_one_dominates_is_ambiguous() -> None:
    registered = {"x_a": "vol", "x_b": "vol", "x_c": "vol", "x_d": "macro"}
    assert integrity.ghost_feature_family("x_new", registered) is None


def test_unclassified_is_not_a_registry_family() -> None:
    assert "unclassified" not in integrity.REGISTRY_FAMILIES


# ── command ─────────────────────────────────────────────────────────────────

def test_dry_run_names_resolved_families_and_unresolved_ghosts_without_writing(engine) -> None:
    with patch("normalization.entity_map.EntityMap", _Map), \
            patch("scripts.hermes_data_integrity.log_issue") as logged:
        out = integrity.register_ghost_features(engine, cycle_number=318, dry_run=True)
    assert out == {"status": "ok", "ghosts": [FED_GHOST, GHOST],
                   "families": {GHOST: "sentiment"},
                   "unresolved": {FED_GHOST: {"sentiment": 2, "rates": 1, "macro": 1}},
                   "registered": 0, "dry_run": True}
    assert engine.conn.inserts == []
    assert logged.call_count == 0


def test_registration_inserts_only_stated_families_and_reports_them(engine) -> None:
    with patch("normalization.entity_map.EntityMap", _Map), \
            patch("scripts.hermes_data_integrity.log_issue") as logged:
        out = integrity.register_ghost_features(engine, cycle_number=318)
    assert out["status"] == "ok" and out["registered"] == 1
    assert out["names"] == [GHOST] and out["registered_families"] == {GHOST: "sentiment"}
    assert out["unresolved"] == {FED_GHOST: {"sentiment": 2, "rates": 1, "macro": 1}}
    (insert,) = engine.conn.inserts
    assert insert["params"] == {"n": GHOST, "family": "sentiment"}
    assert insert["params"]["family"] in integrity.REGISTRY_FAMILIES
    assert "'unclassified'" not in insert["sql"]
    assert ":family" in insert["sql"] and "ON CONFLICT (name) DO NOTHING" in insert["sql"]
    kwargs = logged.call_args.kwargs
    assert kwargs["title"] == "Registered 1 ghost-mapped feature(s); 1 need an operator family choice"
    assert f"{GHOST} (sentiment)" in kwargs["detail"]
    assert "unclassified" not in kwargs["detail"]
    assert f"{FED_GHOST} (siblings: macro x1, rates x1, sentiment x2)" in kwargs["detail"]
    assert "Registration is not materialization" in kwargs["detail"]
    assert kwargs["cycle_number"] == 318 and kwargs["source"] == "entity_map"


def test_only_unresolved_ghosts_still_logs_the_choice_needed(engine) -> None:
    with patch("normalization.entity_map.EntityMap", _Map), \
            patch.dict(MAPPINGS, {GHOST: "gdelt_fed_tone"}, clear=False), \
            patch("scripts.hermes_data_integrity.log_issue") as logged:
        out = integrity.register_ghost_features(engine)
    assert out["registered"] == 0 and out["names"] == [] and list(out["unresolved"]) == [FED_GHOST]
    assert engine.conn.inserts == []
    assert logged.call_args.kwargs["title"] == "Registered 0 ghost-mapped feature(s); 1 need an operator family choice"


def test_no_ghosts_means_nothing_to_do(engine) -> None:
    with patch("normalization.entity_map.EntityMap", _Map), \
            patch.dict(MAPPINGS, {GHOST: "gdelt_fed_tone", "FEDFUNDS": "yc_10y2y"}, clear=False), \
            patch("scripts.hermes_data_integrity.log_issue") as logged:
        out = integrity.register_ghost_features(engine)
    assert out == {"status": "ok", "ghosts": [], "registered": 0}
    assert engine.conn.inserts == [] and logged.call_count == 0
