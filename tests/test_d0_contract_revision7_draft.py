"""The D0 contract revision 7 draft (D2-H1 struck) applies mechanically to the signed revision 6 body and stays inert."""

from __future__ import annotations

import json
from pathlib import Path

from analysis import d0_manifest as M

ROOT = Path(__file__).resolve().parents[1]
DRAFT = json.loads((ROOT / "docs/research/d0-contract-revision-7-draft.json").read_text(encoding="utf-8"))
TEXT = (ROOT / M.CONTRACT_PATH).read_text(encoding="utf-8").replace("\r\n", "\n")


def apply(text: str) -> str:
    for c in DRAFT["changes"]:
        text = text.replace(c["anchor"], c["replacement"], 1)
    return text


def test_the_draft_is_not_effective_and_the_signed_body_is_untouched():
    assert DRAFT["status"] == "DRAFT_NOT_EFFECTIVE"
    assert M.body_sha256(M.contract_body(TEXT)) == M.D0_CONTRACT_BODY_SHA256 == DRAFT["base_body_sha256"]


def test_every_anchor_occurs_exactly_once_in_the_signed_body():
    for c in DRAFT["changes"]:
        assert TEXT.count(c["anchor"]) == 1, (c["section"], TEXT.count(c["anchor"]))


def test_applying_the_draft_yields_a_valid_body_with_the_new_rules():
    new = apply(TEXT)
    M.contract_body(new)  # markers still present exactly once
    assert new != TEXT and M.body_sha256(M.contract_body(new)) != M.D0_CONTRACT_BODY_SHA256
    assert "**revision 7**" in new and "### 10.5 Revision 7" in new
    assert "D2-H1 PASS **and** D2-H2 PASS" not in new  # the old GO rule is gone
    assert "FORWARD_PASS_UNREPLICATED" in new and "STRUCK in revision 7" in new
    assert "D2-H1 FAIL **or** D2-H2 FAIL" not in new


def test_no_live_rule_still_requires_a_struck_cell():
    new = apply(TEXT)
    go = next(ln for ln in new.splitlines() if ln.startswith("- **GO**"))
    retire = next(ln for ln in new.splitlines() if ln.startswith("- **RETIRE:**"))
    assert "not reachable" in go and "D2-H1" in go  # D2-H1 is named only to say it is struck
    assert "D2-H2 FAIL" in retire and "D2-H1 FAIL" not in retire


def test_what_changes_at_signing_is_listed_and_nothing_is_signed_here():
    assert len(DRAFT["at_signing_not_in_this_draft"]) >= 4
    assert any("D0_CONTRACT_BODY_SHA256" in x for x in DRAFT["at_signing_not_in_this_draft"])
    assert any("VERIFIER_ACCEPTED" in x for x in DRAFT["not_changed"])
    assert "signing and pinning need" in DRAFT["authority"]


def test_the_draft_carries_no_private_paths():
    text = (ROOT / "docs/research/d0-contract-revision-7-draft.json").read_text(encoding="utf-8")
    for bad in ("/home/", "~/", "api-key"):
        assert bad not in text
