"""intelligence/actors/sectors.py: word-boundary sector tagging (no DB).

The old tagger matched substrings ("craig" contains "ai" -> AI, "megan"
contains "gene" -> pharma) and scanned people's names. These pin the fix.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from intelligence.actors import sectors as sec


@pytest.mark.parametrize("name,title", [
    ("Craig Wright", ""),
    ("Megan Rapinoe", ""),
    ("Gold Family Trust", ""),     # person category below: names are not scanned
    ("Thai Nguyen", "Director"),
])
def test_person_names_never_imply_a_sector(name: str, title: str) -> None:
    sector, basis = sec.tag_sector("individual", name, title)
    assert sector == sec.OTHER
    assert basis == sec.BASIS_DEFAULT


def test_substrings_inside_words_do_not_match() -> None:
    # "ai" in "Craig", "gene" in "General", "oil" in "Boiler", "gas" in "Vegas".
    assert sec.sector_from_keywords("Craig General Boiler Vegas Holdings") is None


def test_whole_words_and_acronyms_match() -> None:
    assert sec.sector_from_keywords("Head of AI research") == "AI"
    assert sec.sector_from_keywords("machine learning platform") == "AI"
    assert sec.sector_from_keywords("Barrick Gold mining") == "Metals"
    assert sec.sector_from_keywords("an LNG exporter") == "Energy"
    assert sec.sector_from_keywords("FDA advisory panel") == "Gene Therapy & Pharma"


def test_acronyms_are_case_sensitive() -> None:
    # Lower-case "ai" (a name fragment, Portuguese "aí", ...) is not the acronym.
    assert sec.sector_from_keywords("ai holdings") is None
    assert sec.sector_from_keywords("AI Holdings") == "AI"


def test_title_prefix_is_a_direct_label() -> None:
    assert sec.tag_sector("institution", "SAP SE", "Technology — SAP") == ("Tech", sec.BASIS_TITLE)
    assert sec.tag_sector("institution", "Shell", "Energy — SHEL") == ("Energy", sec.BASIS_TITLE)
    assert sec.sector_from_title_prefix("#3 donor — 2024") is None


def test_organisation_names_are_scanned_but_bulk_categories_are_not() -> None:
    assert sec.tag_sector("corporation", "Cameco Uranium Corp", "") == ("Nuclear", sec.BASIS_KEYWORD)
    # ICIJ shells and PEPs keep their category sector whatever their name says.
    assert sec.tag_sector("icij_entity", "Gold Star AI Holdings Ltd", "Offshore Entity (BVI)") == (
        "Offshore", sec.BASIS_CATEGORY)
    assert sec.tag_sector("pep", "Minister of Energy", "") == ("Government", sec.BASIS_CATEGORY)
    assert sec.tag_sector("person_of_interest", "Oil Trader", "") == (sec.OTHER, sec.BASIS_DEFAULT)


def test_insider_title_and_board_seats_are_scanned() -> None:
    assert sec.tag_sector("insider", "Jensen Huang", "CEO, NVIDIA")[0] == "Semiconductors"
    assert sec.tag_sector("fund", "X", "", board_seats=["Coinbase"])[0] == "Crypto"
    assert sec.tag_sector("other", "Y", "", positions=["Uranium Royalty"])[0] == "Nuclear"
    assert sec.tag_sector("central_bank", "Fed", "") == ("Macro", sec.BASIS_CATEGORY)


def test_frame_tagger_matches_scalar_tagger() -> None:
    rows = [
        ("individual", "Craig Wright", "", "", ""),
        ("insider", "Jensen Huang", "CEO, NVIDIA", "", ""),
        ("institution", "SAP SE", "Technology — SAP", "", ""),
        ("corporation", "Barrick", "Gold mining", "", ""),
        ("icij_entity", "Energy Holdings Ltd", "Offshore Entity (BVI)", "", ""),
        ("pep", "Someone", "", "", ""),
        ("fund", "X", "", "", '["Coinbase"]'),
        ("unknown", "corp_AAPL", "", "", ""),
        (None, None, None, None, None),
    ]
    frame = pd.DataFrame(rows, columns=["category", "name", "title", "positions_text", "board_text"])
    sector, basis = sec.tag_sectors_frame(frame)
    for i, (cat, name, title, pos, board) in enumerate(rows):
        expected = sec.tag_sector(cat, name or "", title or "", pos or None, board or None)
        assert (sector.iloc[i], basis.iloc[i]) == expected, rows[i]


def test_neighbour_vote_fills_only_eligible_unknowns() -> None:
    unknown = 9
    # node 0 (unknown, eligible) has neighbours 1, 2 (sector 3) and 3 (sector 4)
    # node 4 (unknown, NOT eligible) neighbours node 1
    src = np.array([0, 0, 3, 4])
    dst = np.array([1, 2, 0, 1])
    codes = np.array([unknown, 3, 3, 4, unknown])
    eligible = np.array([True, False, False, False, False])
    out = sec.propagate_sectors(src, dst, codes, eligible, unknown_code=unknown)
    assert out.tolist() == [3, 3, 3, 4, unknown]
    assert codes.tolist() == [unknown, 3, 3, 4, unknown], "input must not be mutated"


def test_neighbour_vote_ties_break_to_lowest_code_and_respects_min_votes() -> None:
    unknown = 9
    src = np.array([0, 0])
    dst = np.array([1, 2])
    codes = np.array([unknown, 5, 2])
    eligible = np.array([True, False, False])
    assert sec.propagate_sectors(src, dst, codes, eligible, unknown_code=unknown)[0] == 2
    assert sec.propagate_sectors(src, dst, codes, eligible, unknown_code=unknown, min_votes=2)[0] == unknown
