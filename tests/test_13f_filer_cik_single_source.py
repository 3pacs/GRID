"""GD0 §1.3 / §6 item 4 (owner decision, adopted 2026-09-28): the 13F
**filer**-CIK space used to have three mutually contradictory hardcoded
maps -- ``ingestion/edgar.py``, ``ingestion/altdata/institutional_flows.py``,
and ``ingestion/altdata/sec_13f_live.py`` -- that disagreed with each other
on the same CIK for different funds. The decision retired the first two in
favor of ``sec_13f_live.FILERS`` (the 13F writer's own map, verified
correct).

These tests confirm the retirement actually happened: both modules now
derive their filer lookups from ``sec_13f_live.FILERS`` instead of carrying
an independent copy that can drift back out of sync.
"""

from __future__ import annotations

from ingestion import edgar
from ingestion.altdata import institutional_flows
from ingestion.altdata.sec_13f_live import FILERS, filer_by_cik


def test_filer_by_cik_resolves_unpadded_and_zero_padded():
    f = filer_by_cik("1103804")
    assert f is not None
    assert f.display_name == "Viking Global Investors"
    assert filer_by_cik("0001103804") is f
    assert filer_by_cik(1103804) is f


def test_filer_by_cik_unknown_returns_none():
    assert filer_by_cik("9999999999") is None
    assert filer_by_cik("not-a-cik") is None


def test_institutional_flows_top_13f_filers_is_derived_from_sec_13f_live():
    expected = {f.cik: f.display_name for f in FILERS}
    assert institutional_flows.TOP_13F_FILERS == expected


def test_edgar_top_hedge_fund_ciks_is_derived_from_sec_13f_live_zero_padded():
    expected = sorted(f.cik.zfill(10) for f in FILERS)
    assert sorted(edgar.TOP_HEDGE_FUND_CIKS) == expected


def test_cik_1167483_no_longer_contradicts_across_modules():
    # This is GD0 §1.3's headline example: CIK 1167483 was claimed as three
    # different funds (Eton Park Capital / Elliott Management / Tiger
    # Global Management) across the three files. It must now agree
    # everywhere it appears, matching sec_13f_live's verified entry.
    verified = filer_by_cik("1167483")
    assert verified is not None
    assert verified.display_name == "Tiger Global Management"
    assert institutional_flows.TOP_13F_FILERS["1167483"] == "Tiger Global Management"
    assert "0001167483" in edgar.TOP_HEDGE_FUND_CIKS


def test_no_cik_maps_to_two_different_names_across_the_two_derived_maps():
    # Every CIK that appears in both derived maps must now name the same
    # fund -- the whole point of collapsing to a single source of truth.
    edgar_by_unpadded = {c.lstrip("0"): c for c in edgar.TOP_HEDGE_FUND_CIKS}
    for cik, name in institutional_flows.TOP_13F_FILERS.items():
        if cik in edgar_by_unpadded:
            padded = edgar_by_unpadded[cik]
            verified_name = filer_by_cik(padded).display_name
            assert name == verified_name, f"CIK {cik} disagrees: {name!r} vs {verified_name!r}"
