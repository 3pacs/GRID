"""Pure-Python proof for intelligence/actors/provenance.py's wire-mapping --
no database, no fixtures, part of the #596 remediation reconciliation (see
00-Agent-Reports/2026-09-22/claude__ANIK__grid-596-remediation-plan.md).

The property under test is the fail-closed design of actor_source(): every
provenance value except the three specifically-recognized, earned claims
('seed', 'observed', 'unconfirmed') must map to SOURCE_UNKNOWN -- including
PROVENANCE_UNKNOWN itself, an unrecognized string, and no stored value at all
for an id not on the static seed list. It must NEVER default to
SOURCE_OBSERVED for anything it does not specifically recognize -- that
unconditional-else was the original bug (see the module docstring's
"Resolution order" section).
"""

from __future__ import annotations

import pytest

from intelligence.actors.provenance import (
    PROVENANCE_OBSERVED,
    PROVENANCE_SEED,
    PROVENANCE_UNCONFIRMED,
    PROVENANCE_UNKNOWN,
    SEED_ACTOR_IDS,
    SEED_VINTAGE,
    SOURCE_CURATED_SEED,
    SOURCE_OBSERVED,
    SOURCE_UNCONFIRMED,
    SOURCE_UNKNOWN,
    actor_source,
    resolve_provenance,
    source_as_of,
    stamp_actor_node,
)

_NOT_A_SEED_ID = "definitely_not_on_the_seed_list_xyz"


def test_a_seed_list_id_with_no_stored_value_falls_back_to_seed():
    assert _NOT_A_SEED_ID not in SEED_ACTOR_IDS
    any_seed_id = next(iter(SEED_ACTOR_IDS))
    assert resolve_provenance(any_seed_id, stored=None) == PROVENANCE_SEED


def test_a_non_seed_id_with_no_stored_value_falls_back_to_unknown_not_observed():
    """The fallback used when no column was selected must never guess
    'observed' or 'unconfirmed' -- a bare id carries no evidence that would
    justify either claim. This was exactly the mistake a column DEFAULT of
    'observed' used to make."""
    assert resolve_provenance(_NOT_A_SEED_ID, stored=None) == PROVENANCE_UNKNOWN


@pytest.mark.parametrize("stored", [PROVENANCE_UNKNOWN, PROVENANCE_SEED, PROVENANCE_OBSERVED, PROVENANCE_UNCONFIRMED])
def test_resolve_provenance_prefers_the_stored_value_verbatim_over_any_fallback(stored):
    """Even a seed-list id must report its REAL stored state, not the
    seed-list fallback, whenever the caller selected the column."""
    any_seed_id = next(iter(SEED_ACTOR_IDS))
    assert resolve_provenance(any_seed_id, stored=stored) == stored


@pytest.mark.parametrize(
    "stored,expected",
    [
        (PROVENANCE_SEED, SOURCE_CURATED_SEED),
        (PROVENANCE_OBSERVED, SOURCE_OBSERVED),
        (PROVENANCE_UNCONFIRMED, SOURCE_UNCONFIRMED),
    ],
)
def test_actor_source_maps_each_earned_claim_to_its_own_wire_label(stored, expected):
    assert actor_source(_NOT_A_SEED_ID, stored=stored) == expected


def test_actor_source_fails_closed_to_unknown_for_the_literal_unknown_value():
    assert actor_source(_NOT_A_SEED_ID, stored=PROVENANCE_UNKNOWN) == SOURCE_UNKNOWN


@pytest.mark.parametrize("garbage", ["", "SEED", "Observed", "not_a_real_value", "null", "None", "0"])
def test_actor_source_fails_closed_to_unknown_for_any_unrecognized_value(garbage):
    """The decisive fail-closed proof: actor_source must never default to
    SOURCE_OBSERVED for a value it does not specifically recognize as one of
    the three earned claims -- this is the exact shape of the original bug
    (an unconditional `else: return SOURCE_OBSERVED`)."""
    assert actor_source(_NOT_A_SEED_ID, stored=garbage) == SOURCE_UNKNOWN
    assert actor_source(_NOT_A_SEED_ID, stored=garbage) != SOURCE_OBSERVED


def test_actor_source_for_a_non_seed_id_with_no_stored_value_is_unknown_not_observed():
    assert actor_source(_NOT_A_SEED_ID, stored=None) == SOURCE_UNKNOWN


def test_actor_source_for_a_seed_list_id_with_no_stored_value_falls_back_to_curated_seed():
    any_seed_id = next(iter(SEED_ACTOR_IDS))
    assert actor_source(any_seed_id, stored=None) == SOURCE_CURATED_SEED


@pytest.mark.parametrize("stored", [PROVENANCE_UNKNOWN, PROVENANCE_OBSERVED, PROVENANCE_UNCONFIRMED, "garbage"])
def test_source_as_of_is_none_for_every_state_except_seed(stored):
    assert source_as_of(_NOT_A_SEED_ID, stored=stored) is None


def test_source_as_of_reports_the_seed_vintage_for_a_seed_row_with_no_explicit_vintage():
    assert source_as_of(_NOT_A_SEED_ID, stored=PROVENANCE_SEED, vintage=None) == SEED_VINTAGE


def test_source_as_of_reports_an_explicit_vintage_when_given():
    assert source_as_of(_NOT_A_SEED_ID, stored=PROVENANCE_SEED, vintage="2026-01-01") == "2026-01-01"


def test_stamp_actor_node_never_writes_source_observed_for_an_unclassified_node():
    node = stamp_actor_node({}, _NOT_A_SEED_ID, stored=PROVENANCE_UNKNOWN)
    assert node["source"] == SOURCE_UNKNOWN
    assert node["source_as_of"] is None
    assert node["source"] != SOURCE_OBSERVED


def test_stamp_actor_node_reports_a_real_observation_correctly():
    node = stamp_actor_node({}, _NOT_A_SEED_ID, stored=PROVENANCE_OBSERVED)
    assert node["source"] == SOURCE_OBSERVED
    assert node["source_as_of"] is None
