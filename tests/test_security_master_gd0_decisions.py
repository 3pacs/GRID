"""Tests for the GD1 owner decisions adopted 2026-09-28 (GD0 §6 items 1-3).

See ``GRID-GD0-SECURITY-MASTER-AUDIT-20260927.md``'s appended decision
record for the full rationale. This file covers only the three decisions
that landed as code in ``intelligence/security_master.py``:

    1. Primary-sector tie-break (weight, then SIC hint if available, then
       alphabetical) -- :func:`propose_primary_sector_with_sic`.
    2. Delisting corroboration (SEC absence alone never flips is_active) --
       :func:`evaluate_delisting_candidate`.
    3. Canonical taxonomy crosswalk (sector_map_v1 is canonical; other
       vocabularies map onto it explicitly, never by string equality) --
       :func:`crosswalk_sector`.

Pure-function tests only -- no DB, no network.
"""

from __future__ import annotations

from intelligence import security_master as sm


# ── owner decision #1: primary-sector tie-break ────────────────────────────


def test_propose_with_sic_single_sector_is_never_a_tie():
    proposed, is_multi, method = sm.propose_primary_sector_with_sic({"Technology": 0.01})
    assert proposed == "Technology"
    assert is_multi is False
    assert method == "single_sector"


def test_propose_with_sic_clear_weight_winner_skips_sic():
    proposed, is_multi, method = sm.propose_primary_sector_with_sic(
        {"Technology": 0.05, "Communication Services": 0.09}
    )
    assert proposed == "Communication Services"
    assert is_multi is True
    assert method == "subsector_weight"


def test_propose_with_sic_no_hint_breaks_tie_alphabetically():
    proposed, is_multi, method = sm.propose_primary_sector_with_sic(
        {"Consumer Discretionary": 0.02, "Communication Services": 0.02}
    )
    assert proposed == "Communication Services"
    assert is_multi is True
    assert method == "subsector_weight+alpha"


def test_propose_with_sic_hint_breaks_tie_when_it_has_a_clear_winner():
    weights = {"Consumer Discretionary": 0.02, "Communication Services": 0.02}
    proposed, is_multi, method = sm.propose_primary_sector_with_sic(
        weights, sic_sector_hint={"Consumer Discretionary": 3, "Communication Services": 0}
    )
    assert proposed == "Consumer Discretionary"
    assert is_multi is True
    assert method == "subsector_weight+sic"


def test_propose_with_sic_hint_that_does_not_resolve_the_tie_falls_back_to_alpha():
    weights = {"Consumer Discretionary": 0.02, "Communication Services": 0.02}
    # Hint ties too (or is all-zero) -- must not fabricate a winner.
    proposed, _is_multi, method = sm.propose_primary_sector_with_sic(
        weights, sic_sector_hint={"Consumer Discretionary": 2, "Communication Services": 2}
    )
    assert proposed == "Communication Services"
    assert method == "subsector_weight+alpha"


def test_propose_with_sic_matches_legacy_function_when_no_hint_given():
    # Backward compatibility: identical output to propose_primary_sector
    # whenever no SIC hint is supplied (today's common case per GD0 §6
    # item 7 -- SIC coverage is partial).
    weights = {"Technology": 0.03, "Materials": 0.03, "Energy": 0.01}
    legacy_proposed, legacy_multi = sm.propose_primary_sector(weights)
    proposed, is_multi, _method = sm.propose_primary_sector_with_sic(weights)
    assert proposed == legacy_proposed
    assert is_multi == legacy_multi


def test_propose_with_sic_empty_weights():
    assert sm.propose_primary_sector_with_sic({}) == (None, False, "single_sector")


# ── owner decision #2: delisting corroboration ──────────────────────────────


def test_delisting_active_when_live_cik_found():
    result = sm.evaluate_delisting_candidate(has_live_cik=True)
    assert result.is_active is True
    assert result.delisted_reason is None
    assert result.delisted_basis is None


def test_delisting_stays_active_on_sec_absence_alone():
    # This is the CFLT/CYBR/JNPR/PSTG case from GD0 §2/§4: SEC absence is a
    # candidate signal, never proof on its own.
    result = sm.evaluate_delisting_candidate(has_live_cik=False)
    assert result.is_active is True
    assert result.delisted_reason is None
    assert result.delisted_basis == "candidate_sec_absence_only"


def test_delisting_flips_inactive_with_form_15_corroboration():
    result = sm.evaluate_delisting_candidate(
        has_live_cik=False,
        corroborating_evidence={"kind": "form_15", "reason": "acquired"},
    )
    assert result.is_active is False
    assert result.delisted_reason == "acquired"
    assert result.delisted_basis == "sec_absence+form_15"


def test_delisting_flips_inactive_with_manual_owner_confirmation_no_reason():
    result = sm.evaluate_delisting_candidate(
        has_live_cik=False,
        corroborating_evidence={"kind": "manual_owner_confirmed"},
    )
    assert result.is_active is False
    assert result.delisted_reason == "manual_owner_confirmed"
    assert result.delisted_basis == "sec_absence+manual_owner_confirmed"


def test_delisting_ignores_corroboration_when_cik_is_actually_live():
    # A live CIK always wins -- corroborating evidence for a delisting that
    # didn't happen must never flip an active security to inactive.
    result = sm.evaluate_delisting_candidate(
        has_live_cik=True,
        corroborating_evidence={"kind": "form_15", "reason": "acquired"},
    )
    assert result.is_active is True


# ── owner decision #3: canonical taxonomy crosswalk ─────────────────────────


def test_canonical_taxonomy_is_sector_map_v1():
    assert sm.CANONICAL_TAXONOMY == "sector_map_v1"
    assert sm.CANONICAL_TAXONOMY == sm.DEFAULT_TAXONOMY


def test_crosswalk_identity_for_canonical_taxonomy():
    assert sm.crosswalk_sector("sector_map_v1", "Technology") == "Technology"


def test_crosswalk_company_profiles_yahoo_consumer_labels_do_not_collide_by_string():
    # GD0 §6 item 3's exact example: Consumer Discretionary vs Consumer
    # Cyclical must not merge by name-string equality.
    assert sm.crosswalk_sector("company_profiles_yahoo", "Consumer Cyclical") == "Consumer Discretionary"
    assert sm.crosswalk_sector("company_profiles_yahoo", "Consumer Defensive") == "Consumer Staples"
    assert sm.crosswalk_sector("company_profiles_yahoo", "Financial Services") == "Financials"
    assert sm.crosswalk_sector("company_profiles_yahoo", "Semiconductors") == "Technology"


def test_crosswalk_fundamental_divergence_maps_named_sectors_through():
    assert sm.crosswalk_sector("fundamental_divergence_v1", "Technology") == "Technology"
    assert sm.crosswalk_sector("fundamental_divergence_v1", "Real Estate") == "Real Estate"


def test_crosswalk_unmapped_label_returns_none_not_a_guess():
    assert sm.crosswalk_sector("company_profiles_yahoo", "Some Unmapped Label") is None


def test_crosswalk_unknown_taxonomy_returns_none():
    assert sm.crosswalk_sector("some_unregistered_taxonomy", "Technology") is None
