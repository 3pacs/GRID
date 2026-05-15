"""Unit tests for oracle/citation_extractor.py.

Covers the alias / family-normalization pipeline that powers
`api/routers/chat.py:1450` — recording which features the LLM cited.
No DB or LLM dependency.

TIER 4 punch-list item — docs/PUNCH-LIST-2026-05-13.md [P2] item 13.
"""
from __future__ import annotations

import pytest

from oracle.citation_extractor import compute_citation_ratio, extract_citations


# ── Empty / boundary input ──────────────────────────────────────────────


@pytest.mark.parametrize("text", ["", "   ", "\n\n\t"])
def test_empty_text_returns_empty(text: str) -> None:
    # Whitespace-only is falsy under `if not llm_output` only for "" — the
    # other two strings hit the main path but match nothing.
    result = extract_citations(text, ["spy", "vix"])
    assert result == []


def test_empty_features_returns_empty() -> None:
    assert extract_citations("SPY rallied today on VIX collapse.", []) == []


def test_both_empty_returns_empty() -> None:
    assert extract_citations("", []) == []


# ── Exact-name match (case-insensitive) ─────────────────────────────────


def test_exact_match_lowercase() -> None:
    cited = extract_citations("spy closed higher", ["spy", "vix"])
    assert cited == ["spy"]


def test_exact_match_is_case_insensitive_on_text() -> None:
    cited = extract_citations("SPY closed HIGHER", ["spy"])
    assert cited == ["spy"]


def test_exact_match_is_case_insensitive_on_feature() -> None:
    cited = extract_citations("the spy etf rallied", ["SPY_ETF"])
    # Feature lower() = "spy_etf"; spaced = "spy etf". "spy etf" appears in text.
    assert cited == ["SPY_ETF"]


def test_no_match_returns_empty() -> None:
    assert extract_citations("bonds rallied", ["spy", "qqq"]) == []


# ── Underscore-to-space normalization ───────────────────────────────────


def test_underscore_feature_matched_via_spaced_form() -> None:
    # Feature "fed_funds" with text "fed funds" should match via the spaced
    # lookup branch, not the exact branch.
    cited = extract_citations("the fed funds rate moved up", ["fed_funds"])
    assert cited == ["fed_funds"]


def test_underscore_feature_also_matched_via_underscore_form() -> None:
    cited = extract_citations("the fed_funds rate moved up", ["fed_funds"])
    assert cited == ["fed_funds"]


def test_feature_without_underscores_skips_spaced_branch() -> None:
    # No underscore → spaced == feat.lower(), the `if spaced != feat.lower()`
    # branch is skipped. Pure exact-match behavior.
    cited = extract_citations("vix moved", ["vix"])
    assert cited == ["vix"]


# ── Alias match ─────────────────────────────────────────────────────────


def test_alias_fear_index_cites_vix() -> None:
    # "fear index" is in _ALIASES["vix"]; feature "vix" contains alias_key "vix".
    cited = extract_citations("the fear index spiked", ["vix"])
    assert cited == ["vix"]


def test_alias_sp500_cites_spy() -> None:
    cited = extract_citations("the s&p 500 broke 5000", ["spy"])
    assert cited == ["spy"]


def test_alias_yield_curve_cites_yld_curve() -> None:
    cited = extract_citations("the yield curve inverted", ["yld_curve"])
    assert cited == ["yld_curve"]


def test_alias_matches_all_features_containing_alias_key() -> None:
    # alias_key "vix" matches any feature whose name contains "vix".
    cited = extract_citations(
        "volatility index hit 30",
        ["vix_spot", "vix_3m", "spy"],
    )
    assert cited == ["vix_3m", "vix_spot"]  # sorted, spy not cited


def test_alias_key_absent_from_features_skips_alias_block() -> None:
    # No feature contains "btc"; the alias loop continues without citing.
    cited = extract_citations("bitcoin rallied 5%", ["spy", "qqq"])
    assert cited == []


def test_alias_uses_substring_not_word_boundary() -> None:
    # "vix" is a substring of "vixflavor" — current implementation cites it.
    # This pins the no-word-boundary behavior; a future tightening should
    # update this test in the same PR.
    cited = extract_citations("vixflavor of the day", ["vix"])
    assert cited == ["vix"]


# ── Family-level match ──────────────────────────────────────────────────


def test_family_match_skipped_when_families_arg_is_none() -> None:
    # Default `feature_families=None` → family branch is skipped entirely.
    cited = extract_citations("commodities rallied", ["wti_crude"])
    assert cited == []


def test_family_match_skipped_when_families_arg_is_empty() -> None:
    # `if feature_families:` treats {} as falsy.
    cited = extract_citations(
        "commodities rallied", ["wti_crude"], feature_families={}
    )
    assert cited == []


def test_family_match_cites_all_features_in_family() -> None:
    cited = extract_citations(
        "commodities rallied today",
        ["wti_crude", "brent", "gold_spot", "spy"],
        feature_families={
            "wti_crude": "commodity",
            "brent": "commodity",
            "gold_spot": "commodity",
            "spy": "equity",
        },
    )
    assert cited == ["brent", "gold_spot", "wti_crude"]


def test_family_match_does_not_cite_unrelated_family() -> None:
    cited = extract_citations(
        "high yield spreads widened",
        ["hyg", "spy"],
        feature_families={"hyg": "credit", "spy": "equity"},
    )
    # Alias "high yield spread" matches alias_key "hy_spread" only if a
    # feature contains "hy_spread" — none here. Family "credit" via alias
    # "high yield" matches; only hyg is in credit family.
    assert cited == ["hyg"]


def test_family_fear_alias_cites_sentiment_features() -> None:
    cited = extract_citations(
        "fear gripped markets overnight",
        ["aaii", "spy"],
        feature_families={"aaii": "sentiment", "spy": "equity"},
    )
    assert cited == ["aaii"]


# ── Combination / dedup ─────────────────────────────────────────────────


def test_dedup_across_exact_and_alias_paths() -> None:
    # "spy" hits exact match; "s&p 500" also hits the spy alias. One entry.
    cited = extract_citations("spy and the s&p 500 both rallied", ["spy"])
    assert cited == ["spy"]


def test_multiple_features_returns_sorted_list() -> None:
    cited = extract_citations(
        "spy and vix and qqq moved",
        ["zzz", "spy", "vix", "qqq"],
    )
    assert cited == ["qqq", "spy", "vix"]


def test_combined_exact_alias_and_family() -> None:
    cited = extract_citations(
        "the fear index spiked while commodities rallied broadly",
        ["vix", "gld", "wti_crude"],
        feature_families={"vix": "vol", "gld": "commodity", "wti_crude": "commodity"},
    )
    # vix via alias "fear index"; gld + wti_crude via family "commodity"
    # alias "commodities" (alias "gold" is not in the commodity family aliases).
    assert cited == ["gld", "vix", "wti_crude"]


# ── compute_citation_ratio ──────────────────────────────────────────────


def test_ratio_empty_available_is_zero() -> None:
    assert compute_citation_ratio(["spy"], []) == 0.0


def test_ratio_no_citations_is_zero() -> None:
    assert compute_citation_ratio([], ["spy", "vix"]) == 0.0


def test_ratio_full_coverage_is_one() -> None:
    assert compute_citation_ratio(["spy", "vix"], ["spy", "vix"]) == 1.0


def test_ratio_partial_coverage() -> None:
    assert compute_citation_ratio(["spy"], ["spy", "vix", "qqq", "gld"]) == 0.25
