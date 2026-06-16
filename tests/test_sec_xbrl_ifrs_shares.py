"""Tests for IFRS / 20-F / 6-K share extraction in the SEC XBRL shares puller.

Foreign private issuers (TSM, NVO, BP, ...) file Form 20-F / 6-K and report
share counts under the ``ifrs-full`` taxonomy, not us-gaap — so the puller
extracted 0 rows for them. These tests use facts dicts shaped like the real
SEC Company Facts payload (verified live against TSM/NVO) to confirm the IFRS
tags are now picked up, that us-gaap still wins when both exist, and that the
runner can seed the foreign-issuer universe.
"""

from __future__ import annotations

from datetime import date

from ingestion.altdata.sec_xbrl_shares import (
    FOREIGN_ISSUER_TICKERS,
    _FOREIGN_ISSUER_FORMS,
    _SHARES_TAG_SPECS,
    _extract_shares_entries,
    ifrs_shares_tag_map,
)


def _facts(taxonomy: str, tag: str, entries: list[dict]) -> dict:
    """Build a minimal SEC Company Facts dict for one tag (unit='shares')."""
    return {"facts": {taxonomy: {tag: {"units": {"shares": entries}}}}}


# ── Tag map coverage ──

def test_ifrs_tag_map_covers_known_foreign_issuer_tags():
    tags = ifrs_shares_tag_map()["ifrs-full"]
    # NVO uses NumberOfSharesOutstanding; TSM uses NumberOfSharesIssuedAndFullyPaid.
    assert "NumberOfSharesOutstanding" in tags
    assert "NumberOfSharesIssuedAndFullyPaid" in tags


def test_ifrs_specs_present_in_master_spec_list():
    pairs = set(_SHARES_TAG_SPECS)
    assert ("ifrs-full", "NumberOfSharesOutstanding") in pairs
    assert ("ifrs-full", "NumberOfSharesIssuedAndFullyPaid") in pairs


def test_foreign_issuer_forms_and_universe():
    assert {"20-F", "6-K"}.issubset(_FOREIGN_ISSUER_FORMS)
    for tk in ("TSM", "ASML", "BHP", "RIO", "NVO", "AZN", "BP"):
        assert tk in FOREIGN_ISSUER_TICKERS


# ── Extraction from IFRS-shaped facts (the 0-rows fix) ──

def test_extracts_tsm_ifrs_shares_issued_and_fully_paid():
    # TSM reports under ifrs-full only (no us-gaap/dei share count).
    facts = _facts(
        "ifrs-full", "NumberOfSharesIssuedAndFullyPaid",
        [
            {"val": 25_930_000_000, "filed": "2025-04-15", "form": "20-F"},
            {"val": 25_932_700_000, "filed": "2025-08-10", "form": "6-K"},
        ],
    )
    timeline = _extract_shares_entries(facts)
    assert timeline == [
        (date(2025, 4, 15), 25_930_000_000),
        (date(2025, 8, 10), 25_932_700_000),
    ]


def test_extracts_nvo_ifrs_shares_outstanding():
    facts = _facts(
        "ifrs-full", "NumberOfSharesOutstanding",
        [{"val": 4_460_000_000, "filed": "2025-02-05", "form": "20-F"}],
    )
    timeline = _extract_shares_entries(facts)
    assert timeline == [(date(2025, 2, 5), 4_460_000_000)]


def test_usgaap_preferred_over_ifrs_on_same_filed_date():
    # A dual filer reporting both us-gaap and ifrs on the same filed date:
    # us-gaap (higher priority) must win.
    facts = {
        "facts": {
            "us-gaap": {
                "CommonStockSharesOutstanding": {
                    "units": {"shares": [
                        {"val": 1_000, "filed": "2025-03-01", "form": "10-K"},
                    ]}
                }
            },
            "ifrs-full": {
                "NumberOfSharesOutstanding": {
                    "units": {"shares": [
                        {"val": 9_999, "filed": "2025-03-01", "form": "20-F"},
                    ]}
                }
            },
        }
    }
    timeline = _extract_shares_entries(facts)
    assert timeline == [(date(2025, 3, 1), 1_000)]


def test_ifrs_pointintime_preferred_over_ifrs_weighted_average():
    # NumberOfSharesIssuedAndFullyPaid (point-in-time) outranks the
    # AdjustedWeightedAverageShares fallback for the same date.
    facts = {
        "facts": {
            "ifrs-full": {
                "NumberOfSharesIssuedAndFullyPaid": {
                    "units": {"shares": [
                        {"val": 25_932_700_000, "filed": "2025-08-10", "form": "6-K"},
                    ]}
                },
                "AdjustedWeightedAverageShares": {
                    "units": {"shares": [
                        {"val": 25_929_700_000, "filed": "2025-08-10", "form": "20-F"},
                    ]}
                },
            }
        }
    }
    timeline = _extract_shares_entries(facts)
    assert timeline == [(date(2025, 8, 10), 25_932_700_000)]


def test_no_share_tags_returns_empty():
    # Only an unrelated ifrs metric -> no shares.
    facts = _facts("ifrs-full", "ParValuePerShare",
                   [{"val": 10, "filed": "2025-01-01", "form": "20-F"}])
    # ParValuePerShare is not in the spec list, so nothing is extracted.
    assert _extract_shares_entries(facts) == []


# ── Runner universe seeding ──

def test_runner_resolve_universe_merges_foreign_issuers():
    from scripts.run_sec_xbrl_shares import _resolve_universe

    # No tickers, no flag -> None (sector-map fallback).
    assert _resolve_universe(None, include_foreign=False) is None

    # Flag only -> the foreign-issuer set.
    uni = _resolve_universe(None, include_foreign=True)
    assert "TSM" in uni and "BP" in uni

    # Explicit + flag -> merged, explicit first, deduped.
    uni = _resolve_universe("AAPL,TSM", include_foreign=True)
    assert uni[0] == "AAPL"
    assert uni.count("TSM") == 1  # not duplicated despite being in both
