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


B, E = "<!-- D2-H1-HISTORY-BEGIN -->", "<!-- D2-H1-HISTORY-END -->"


def live(new: str) -> str:
    """The rendered body's live rules: no revision log (section 10 on), no marked history block, no struck table row."""
    body = M.contract_body(new)
    body = body.split("\n## 10.", 1)[0]
    out, hist = [], False
    for ln in body.splitlines():
        if ln.strip() == B:
            assert not hist, "nested history block"
            hist = True
        elif ln.strip() == E:
            assert hist, "history block closed but never opened"
            hist = False
        elif not hist and "STRUCK in revision 7" not in ln:
            out.append(ln)
    assert not hist, "history block never closed"
    return "\n".join(out)


def section(text: str, start: str, end: str) -> str:
    return text[text.index(start): text.index(end)]


def test_applying_the_draft_yields_a_valid_body_with_the_new_rules():
    new = apply(TEXT)
    M.contract_body(new)  # markers still present exactly once
    assert new != TEXT and M.body_sha256(M.contract_body(new)) != M.D0_CONTRACT_BODY_SHA256
    assert "**revision 7**" in new and "### 10.5 Revision 7" in new
    assert new.count(B) == new.count(E) >= 4


def test_no_live_rule_names_a_struck_cell_except_to_say_it_is_struck():
    rules = live(apply(TEXT))
    leftovers = [ln for ln in rules.splitlines()
                 if ("D2-H1" in ln or "H1a" in ln or "H1b" in ln or "H1 cell" in ln) and "struck" not in ln.lower()]
    assert leftovers == []


def test_the_rendered_decision_rules_are_exactly_the_revision_7_ones():
    new = apply(TEXT)
    rules = live(new)
    s65 = section(rules, "### 6.5", "### 6.6")
    go = next(ln for ln in s65.splitlines() if ln.startswith("- **GO**"))
    hold = next(ln for ln in s65.splitlines() if ln.startswith("- **HOLD**"))
    prec = s65[s65.index("- **Precedence"): s65.index("- **RETIRE")]
    retire = next(ln for ln in s65.splitlines() if ln.startswith("- **RETIRE:**"))
    assert "not reachable" in go
    assert "D2-H2 FAIL" in retire and "FAIL" not in retire.replace("D2-H2 FAIL", "").replace("not FAIL", "")  # no other FAIL path
    assert "D2-H2 is UNDERPOWERED" in hold and "D2-H2 has PASSed" in hold and "FORWARD_PASS_UNREPLICATED" in hold
    assert "either" not in hold
    assert "A D2-H2 FAIL is RETIRE" in prec and "H1" not in prec and "any judged cell" not in prec
    assert "D2-H3 decides only its own GO/RETIRE" in s65
    assert "usage-based purchase" not in section(rules, "### 6.6", "### 6.7").split("Spend (revision 7)")[0]


def test_g5_row_is_closed_and_carries_no_directive_and_section_4_claims_nothing_untouched():
    rules = live(apply(TEXT))
    g5 = next(ln for ln in rules.splitlines() if ln.startswith("| G5"))
    assert "CLOSED" in g5 and "fetch manifest" in g5 and "No fetch manifest" in g5 and "approval" not in g5
    assert "Untouched options history exists" not in rules
    assert "family-specific reconciliation" in rules and "protected reservation" in rules


def test_a_revision_7_header_does_not_claim_the_old_signature_as_its_own():
    new = apply(TEXT)
    head = new.split("\n---\n", 1)[0]
    assert "**Signed 2026-11-14**" not in head
    assert "Revision 6 was signed 2026-11-14" in head and "Revision 7 is not signed" in head
    assert "signed revision 6 of this body" in head


def test_history_text_is_kept_not_deleted():
    new = apply(TEXT)
    for kept in ("D2-H1a and D2-H1b** are each tested separately", "At n = 300 per H1 cell", "Reconcile the full inspection ledger"):
        assert kept in new and kept in TEXT


def test_what_changes_at_signing_is_listed_and_nothing_is_signed_here():
    assert len(DRAFT["at_signing_not_in_this_draft"]) >= 4
    assert any("D0_CONTRACT_BODY_SHA256" in x for x in DRAFT["at_signing_not_in_this_draft"])
    assert any("VERIFIER_ACCEPTED" in x for x in DRAFT["not_changed"])
    assert "signing and pinning need" in DRAFT["authority"]


def test_the_draft_carries_no_private_paths():
    text = (ROOT / "docs/research/d0-contract-revision-7-draft.json").read_text(encoding="utf-8")
    for bad in ("/home/", "~/", "api-key"):
        assert bad not in text
