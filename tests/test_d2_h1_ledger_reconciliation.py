"""The D2-H1 ledger reconciliation (G5 free step 1) stays self-consistent and tied to the contract's window."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOC = json.loads((ROOT / "docs/research/d2-h1-ledger-reconciliation-v0.json").read_text(encoding="utf-8"))
CONTRACT = (ROOT / "docs/research/d0-research-contract-v0.md").read_text(encoding="utf-8")
REQUIRED = {"id", "family", "instrument", "data_kind", "window", "overlaps_target_window", "listed_in_contract_section_4", "note", "source"}


def test_target_window_is_the_contracts():
    w = DOC["target_window"]
    assert f"{w['first']} .. {w['last']}" in CONTRACT.replace("2018-01-02 ..", "2018-01-02 ..")
    assert (w["first"], w["last"]) == ("2018-01-02", "2022-11-11")


def test_every_row_is_complete_and_ids_are_unique():
    ids = [r["id"] for r in DOC["rows"]]
    assert len(ids) == len(set(ids))
    for r in DOC["rows"]:
        assert REQUIRED <= set(r), r["id"]
        assert isinstance(r["overlaps_target_window"], bool) and r["listed_in_contract_section_4"] in ("YES", "NO")


def test_the_finding_matches_the_rows():
    f = DOC["finding"]
    rows = {r["id"]: r for r in DOC["rows"]}
    assert f["section_4_omits"] == [i for i, r in rows.items() if r["listed_in_contract_section_4"] == "NO"]
    for i in f["overlaps"]:
        assert rows[i]["overlaps_target_window"] is True and rows[i]["data_kind"] in ("OPTION_PREMIUM", "SPY_INTRADAY_PRICE_PATH")


def test_any_option_premium_overlap_is_named_in_the_finding():
    hits = {r["id"] for r in DOC["rows"] if r["data_kind"] == "OPTION_PREMIUM" and r["overlaps_target_window"]}
    assert hits and hits <= set(DOC["finding"]["overlaps"])


def test_unread_items_are_listed_and_nothing_is_decided_here():
    assert DOC["not_read"] and all(x["reason"] for x in DOC["not_read"])
    assert DOC["status"] == "DRAFT_FOR_REVIEW" and "does not decide" in DOC["finding"]["needs_a_decision"]


def test_the_data_file_carries_no_private_paths():
    text = (ROOT / "docs/research/d2-h1-ledger-reconciliation-v0.json").read_text(encoding="utf-8")
    md = (ROOT / "docs/research/d2-h1-ledger-reconciliation-v0.md").read_text(encoding="utf-8")
    for bad in ("/home/", "~/", "gex-edge", "api-key"):
        assert bad not in text and bad not in md, bad
