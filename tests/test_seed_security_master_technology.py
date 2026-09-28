"""Tests for ``scripts/seed_security_master_technology.py`` (GD1).

No network, no DB: SEC responses are injected mocks (mirrors
``tests/test_small_cap_enrichment.py``'s "No network (injected http_get)"
pattern) and the sector map is a small synthetic dict, never the real
25k-line ``sector_map_data.yaml``.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from scripts import seed_security_master_technology as seed


# ── SEC company_tickers.json parsing ───────────────────────────────────────


def test_parse_sec_company_tickers_basic():
    data = {
        "0": {"cik_str": 320193, "ticker": "aapl", "title": "Apple Inc."},
        "1": {"cik_str": 1045810, "ticker": "NVDA", "title": "NVIDIA CORP"},
    }
    parsed = seed.parse_sec_company_tickers(data)
    assert parsed["AAPL"] == {"cik": 320193, "name": "Apple Inc."}
    assert parsed["NVDA"] == {"cik": 1045810, "name": "NVIDIA CORP"}


def test_parse_sec_company_tickers_skips_blank_ticker():
    data = {"0": {"cik_str": 1, "ticker": "", "title": "Nobody"}}
    assert seed.parse_sec_company_tickers(data) == {}


def test_parse_sec_company_tickers_handles_bad_cik():
    data = {"0": {"cik_str": "not-a-number", "ticker": "ZZZ", "title": "Zzz Corp"}}
    parsed = seed.parse_sec_company_tickers(data)
    assert parsed["ZZZ"] == {"cik": None, "name": "Zzz Corp"}


def test_fetch_sec_company_tickers_uses_injected_http_get():
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}}
    http_get = MagicMock(return_value=resp)

    result = seed.fetch_sec_company_tickers(http_get)

    assert result == {"AAPL": {"cik": 320193, "name": "Apple Inc."}}
    http_get.assert_called_once()
    _, kwargs = http_get.call_args
    assert kwargs["headers"]["User-Agent"]  # the repo's contact-bearing SEC UA, not hardcoded here


def test_fetch_sec_company_tickers_http_failure_returns_empty():
    resp = MagicMock()
    resp.status_code = 503
    http_get = MagicMock(return_value=resp)
    assert seed.fetch_sec_company_tickers(http_get) == {}


def test_fetch_sec_company_tickers_network_exception_returns_empty():
    http_get = MagicMock(side_effect=ConnectionError("boom"))
    assert seed.fetch_sec_company_tickers(http_get) == {}


# ── sector universe ─────────────────────────────────────────────────────────


_SECTOR_MAP = {
    "Technology": {
        "subsectors": {
            "Semiconductors": {
                "weight": 0.05,
                "actors": [
                    {"name": "NVIDIA", "ticker": "NVDA", "weight": 0.18, "type": "company"},
                    {"name": "Some Sovereign Fund", "ticker": "XYZ", "weight": 0.5, "type": "fund"},
                ],
            },
            "Software": {
                "weight": 0.10,
                "actors": [
                    {"name": "Delisted Co", "ticker": "DELIST", "weight": 0.01, "type": "company"},
                ],
            },
        },
    },
    "Communication Services": {
        "subsectors": {
            "Internet": {
                "weight": 0.20,
                "actors": [
                    {"name": "NVIDIA", "ticker": "NVDA", "weight": 0.02, "type": "company"},
                ],
            },
        },
    },
}

_SEC_MAP = {
    "NVDA": {"cik": 1045810, "name": "NVIDIA CORP"},
    # DELIST intentionally absent — no live CIK, the CFLT/CYBR/JNPR/PSTG case.
}


def test_build_sector_universe_dedupes_and_excludes_non_company_actors():
    universe = seed.build_sector_universe(_SECTOR_MAP, "Technology")
    tickers = [row["ticker"] for row in universe]
    assert tickers == ["DELIST", "NVDA"]  # sorted, XYZ (fund) excluded


def test_build_sector_universe_unknown_sector_is_empty():
    assert seed.build_sector_universe(_SECTOR_MAP, "Nonexistent Sector") == []


# ── per-ticker plan ─────────────────────────────────────────────────────────


def test_build_ticker_plan_matched_single_sector_no_conflict():
    plan = seed.build_ticker_plan("DELIST", "Delisted Co", None, _SECTOR_MAP, date(2026, 9, 27))
    assert plan.has_cik is False
    assert plan.is_multi_sector is False
    assert plan.entity_id == "sm_tkr_DELIST"
    assert plan.security_master["is_active"] is True  # never flipped by this script
    assert plan.security_master["source"] == "sector_map"
    scheme_values = {(row["id_scheme"], row["id_value"]) for row in plan.security_identifiers}
    assert ("ticker", "DELIST") in scheme_values
    assert ("actor_corp", "corp_DELIST") in scheme_values
    assert len(plan.security_sector_membership) == 1
    assert plan.security_sector_membership[0]["conflict_flag"] is False


def test_build_ticker_plan_matched_cik_uses_cik_entity_id_and_sec_name():
    plan = seed.build_ticker_plan("NVDA", "NVIDIA", _SEC_MAP["NVDA"], _SECTOR_MAP, date(2026, 9, 27))
    assert plan.has_cik is True
    assert plan.entity_id == "sm_0001045810"
    assert plan.security_master["name"] == "NVIDIA CORP"
    assert plan.security_master["cik"] == 1045810
    scheme_values = {(row["id_scheme"], row["id_value"]) for row in plan.security_identifiers}
    assert ("cik", "1045810") in scheme_values


def test_build_ticker_plan_multi_sector_flags_conflict_on_every_row():
    plan = seed.build_ticker_plan("NVDA", "NVIDIA", _SEC_MAP["NVDA"], _SECTOR_MAP, date(2026, 9, 27))
    assert plan.is_multi_sector is True
    assert {row["sector"] for row in plan.security_sector_membership} == {"Technology", "Communication Services"}
    assert all(row["conflict_flag"] for row in plan.security_sector_membership)
    # Technology has the larger weight product (0.05*0.18=0.009 vs 0.20*0.02=0.004).
    assert plan.proposed_primary_sector == "Technology"
    primary_rows = [row for row in plan.security_sector_membership if row["is_primary"]]
    assert len(primary_rows) == 1
    assert primary_rows[0]["sector"] == "Technology"


# ── full plan + report ──────────────────────────────────────────────────────


def test_build_seed_plan_counts_and_buckets():
    plan = seed.build_seed_plan(_SECTOR_MAP, _SEC_MAP, sector="Technology", as_of=date(2026, 9, 27))
    assert plan["counts"]["total_tickers"] == 2
    assert plan["counts"]["matched_cik"] == 1
    assert plan["counts"]["unmatched_no_cik"] == 1
    assert plan["unmatched_no_cik"] == ["DELIST"]
    assert plan["counts"]["multi_sector"] == 1
    assert plan["multi_sector"][0]["ticker"] == "NVDA"
    assert plan["multi_sector"][0]["proposed_primary_sector"] == "Technology"
    # Row-shape sanity for the tables the migration creates.
    assert len(plan["rows"]["security_master"]) == 2
    assert len(plan["rows"]["security_identifiers"]) >= 2 * 2  # ticker + actor_corp at minimum


def test_build_seed_plan_is_a_dry_run_pure_function_no_side_effects(tmp_path, monkeypatch):
    """Building a plan must never touch the filesystem or a DB by itself —
    only write_report()/apply_plan() do I/O, and apply_plan is opt-in via
    --apply. Guard this with a chdir to a throwaway directory."""
    monkeypatch.chdir(tmp_path)
    seed.build_seed_plan(_SECTOR_MAP, _SEC_MAP, sector="Technology", as_of=date(2026, 9, 27))
    assert list(tmp_path.iterdir()) == []


def test_write_report_writes_json_and_markdown(tmp_path):
    plan = seed.build_seed_plan(_SECTOR_MAP, _SEC_MAP, sector="Technology", as_of=date(2026, 9, 27))
    json_path = seed.write_report(plan, tmp_path)
    assert json_path.exists()
    md_path = json_path.with_suffix(".md")
    assert md_path.exists()
    assert "DELIST" in md_path.read_text(encoding="utf-8")
    assert "NVDA" in md_path.read_text(encoding="utf-8")


def test_write_report_never_writes_outside_output_dir(tmp_path):
    plan = seed.build_seed_plan(_SECTOR_MAP, _SEC_MAP, sector="Technology", as_of=date(2026, 9, 27))
    target = tmp_path / "nested" / "reports"
    seed.write_report(plan, target)
    assert target.exists()
    assert len(list(target.iterdir())) == 2  # exactly the .json and .md


# ── owner decision #2 (adopted 2026-09-28): delisted_basis on SEC absence ──


def test_build_ticker_plan_unmatched_records_candidate_basis_but_stays_active():
    # DELIST has no live CIK in _SEC_MAP -- the CFLT/CYBR/JNPR/PSTG case.
    # GD0 §6 item 2's adopted rule: SEC absence alone is a candidate signal,
    # never sufficient to flip is_active.
    plan = seed.build_ticker_plan("DELIST", "Delisted Co", None, _SECTOR_MAP, date(2026, 9, 27))
    assert plan.security_master["is_active"] is True
    assert plan.security_master["delisted_reason"] is None
    assert plan.security_master["delisted_basis"] == "candidate_sec_absence_only"


def test_build_ticker_plan_matched_cik_has_no_delisting_basis():
    plan = seed.build_ticker_plan("NVDA", "NVIDIA", _SEC_MAP["NVDA"], _SECTOR_MAP, date(2026, 9, 27))
    assert plan.security_master["is_active"] is True
    assert plan.security_master["delisted_basis"] is None
