"""GD0 §1.3 / §6 item 4 (owner decision, adopted 2026-09-28; **corrected**
2026-09-28 by the coordinator): the 13F **filer**-CIK space used to have
three mutually contradictory hardcoded maps -- ``ingestion/edgar.py``,
``ingestion/altdata/institutional_flows.py``, and
``ingestion/altdata/sec_13f_live.py`` -- that disagreed with each other on
the same CIK for different funds.

An earlier revision of this remediation derived all three from
``sec_13f_live.py``'s original 35-entry list alone, which removed the
contradictions but also shrank the tracked universe from ~50 (the union of
the three old lists) to 35 -- a coverage loss the owner never asked for. The
corrected fix builds ``ingestion/altdata/verified_13f_filers.py`` from the
**union** of all three old lists, checks every CIK against SEC's own
registrant data (``data.sec.gov/submissions``) or finds the correct one via
SEC's company-name search, and only drops a manager when no SEC-registered
filer could be found after multiple search attempts (2 of ~74: Balyasny
Asset Management, GIC Private Limited).

These tests confirm: the three modules all derive from the single verified
map (no independent copies), the specific cross-file CIK contradictions
GD0 found are actually fixed (not just relabeled to a smaller universe), and
coverage-critical managers the earlier revision silently dropped are back.
"""

from __future__ import annotations

from ingestion import edgar
from ingestion.altdata import institutional_flows
from ingestion.altdata.sec_13f_live import FILERS, filer_by_cik, filer_by_key
from ingestion.altdata.verified_13f_filers import (
    DROPPED_FILERS,
    VERIFIED_FILERS,
    filer_by_cik as verified_filer_by_cik,
)


def test_filer_by_cik_resolves_unpadded_and_zero_padded():
    f = filer_by_cik("1103804")
    assert f is not None
    assert f.display_name == "Viking Global Investors LP"
    assert filer_by_cik("0001103804") is f
    assert filer_by_cik(1103804) is f


def test_filer_by_cik_unknown_returns_none():
    assert filer_by_cik("9999999999") is None
    assert filer_by_cik("not-a-cik") is None


def test_sec_13f_live_filers_is_not_a_second_copy():
    # sec_13f_live.FILERS must be the exact same object/values as the
    # verified module's VERIFIED_FILERS -- a re-export, not a copy that can
    # drift back out of sync.
    assert FILERS == VERIFIED_FILERS


def test_institutional_flows_top_13f_filers_is_derived_from_verified_map():
    expected = {f.cik: f.display_name for f in VERIFIED_FILERS}
    assert institutional_flows.TOP_13F_FILERS == expected


def test_edgar_top_hedge_fund_ciks_is_derived_from_verified_map_zero_padded():
    expected = sorted(f.cik.zfill(10) for f in VERIFIED_FILERS)
    assert sorted(edgar.TOP_HEDGE_FUND_CIKS) == expected


def test_cik_1167483_resolves_to_tiger_global_everywhere():
    # GD0 §1.3's headline example: CIK 1167483 was claimed as three
    # different funds (Eton Park Capital / Elliott Management / Tiger
    # Global Management) across the three old files. It must now agree
    # everywhere it appears, matching SEC's own registrant name.
    verified = filer_by_cik("1167483")
    assert verified is not None
    assert verified.display_name == "Tiger Global Management LLC"
    assert institutional_flows.TOP_13F_FILERS["1167483"] == "Tiger Global Management LLC"
    assert "0001167483" in edgar.TOP_HEDGE_FUND_CIKS


def test_baupost_and_lone_pine_ciks_are_no_longer_swapped():
    # A finding beyond the original audit: the pre-existing sec_13f_live.py
    # map (the one the owner had confirmed correct) itself had Baupost
    # Group's and Lone Pine Capital's CIKs swapped. CIK 1061165 is actually
    # Lone Pine per SEC; CIK 1061768 is actually Baupost Group per SEC.
    baupost = filer_by_key("baupost")
    lone_pine = filer_by_key("lone_pine")
    assert baupost is not None and lone_pine is not None
    assert baupost.cik == "1061768"
    assert "BAUPOST" in baupost.display_name.upper()
    assert lone_pine.cik == "1061165"
    assert "LONE PINE" in lone_pine.display_name.upper()


def test_no_cik_maps_to_two_different_names_across_the_two_derived_maps():
    # Every CIK that appears in both derived maps must now name the same
    # fund -- the whole point of collapsing to a single source of truth.
    edgar_by_unpadded = {c.lstrip("0"): c for c in edgar.TOP_HEDGE_FUND_CIKS}
    for cik, name in institutional_flows.TOP_13F_FILERS.items():
        if cik in edgar_by_unpadded:
            padded = edgar_by_unpadded[cik]
            verified_name = filer_by_cik(padded).display_name
            assert name == verified_name, f"CIK {cik} disagrees: {name!r} vs {verified_name!r}"


# ── coverage: the union-based fix must not shrink the tracked universe ─────


def test_verified_universe_is_a_union_not_a_shrink():
    # The coordinator's core complaint: deriving from sec_13f_live's
    # original 35-entry list alone dropped real managers. The corrected map
    # must be at least as large as the largest of the three old lists (50),
    # not a subset of the smallest (35).
    assert len(VERIFIED_FILERS) >= 50


def test_previously_dropped_major_managers_are_back():
    # Managers the coordinator explicitly named as wrongly dropped by the
    # first cut of this fix.
    must_have_keys = {
        "vanguard", "state_street", "t_rowe_price", "fidelity_fmr",
        "temasek", "norges_bank",
    }
    present = {f.key for f in VERIFIED_FILERS}
    missing = must_have_keys - present
    assert not missing, f"still missing: {missing}"


def test_all_verified_filer_ciks_are_distinct():
    ciks = [f.cik for f in VERIFIED_FILERS]
    assert len(ciks) == len(set(ciks))


def test_all_verified_filer_keys_are_distinct():
    keys = [f.key for f in VERIFIED_FILERS]
    assert len(keys) == len(set(keys))


# ── dropped filers are documented, not silently omitted ────────────────────


def test_dropped_filers_have_search_evidence_and_are_not_in_the_verified_list():
    assert len(DROPPED_FILERS) >= 1
    verified_ciks = {f.cik for f in VERIFIED_FILERS}
    for dropped in DROPPED_FILERS:
        assert dropped["old_cik"] not in verified_ciks
        assert len(dropped["searched_terms"]) >= 2
        assert dropped["manager_label"]


def test_balyasny_and_gic_are_the_documented_drops():
    labels = {d["manager_label"] for d in DROPPED_FILERS}
    assert "Balyasny Asset Management" in labels
    assert "GIC Private Limited" in labels


def test_verified_filer_by_cik_matches_sec_13f_live_re_export():
    # The module-level helper and the re-exported one must agree.
    for f in VERIFIED_FILERS[:5]:
        assert verified_filer_by_cik(f.cik) == filer_by_cik(f.cik)
