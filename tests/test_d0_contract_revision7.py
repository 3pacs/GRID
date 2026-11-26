"""The signed D0 contract revision 7 (D2-H1 struck): the live rules, headers and pins are the revision 7 ones."""

from __future__ import annotations

import json
from pathlib import Path

from analysis import d0_admission as A
from analysis import d0_manifest as M

ROOT = Path(__file__).resolve().parents[1]
FILE = (ROOT / M.CONTRACT_PATH).read_text(encoding="utf-8").replace("\r\n", "\n")
BODY = M.contract_body(FILE)
DRAFT = json.loads((ROOT / "docs/research/d0-contract-revision-7-draft.json").read_text(encoding="utf-8"))
B, E = "<!-- D2-H1-HISTORY-BEGIN -->", "<!-- D2-H1-HISTORY-END -->"


def live() -> str:
    """The live rules: no revision log (section 10 on), no marked history block, no struck table row."""
    out, hist = [], False
    for ln in BODY.split("\n## 10.", 1)[0].splitlines():
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


def test_the_signed_body_is_revision_7_and_pinned():
    assert M.body_sha256(BODY) == M.D0_CONTRACT_BODY_SHA256 != DRAFT["base_body_sha256"]
    assert "**revision 7**" in BODY and "### 10.6 Revision 7" in BODY and "**Owner signature (2026-11-26).**" in BODY
    assert BODY.count(B) == BODY.count(E) >= 4
    assert M.D0_CONTRACT_BODY_SHA256 not in FILE  # the hash is never inside the body


def test_every_draft_change_is_in_the_signed_text_except_the_reworded_status_lines():
    assert DRAFT["status"].startswith("APPLIED")
    for c in DRAFT["changes"]:
        if c["section"] in DRAFT["reworded_at_signing"]:
            continue
        assert c["replacement"] in FILE, c["section"]


def test_no_live_rule_names_a_struck_cell_except_to_say_it_is_struck():
    leftovers = [ln for ln in live().splitlines()
                 if ("D2-H1" in ln or "H1a" in ln or "H1b" in ln or "H1 cell" in ln) and "struck" not in ln.lower()]
    assert leftovers == []


def test_the_effective_decision_rules():
    rules = live()
    s65 = section(rules, "### 6.5", "### 6.6")
    go = next(ln for ln in s65.splitlines() if ln.startswith("- **GO**"))
    hold = next(ln for ln in s65.splitlines() if ln.startswith("- **HOLD**"))
    prec = s65[s65.index("- **Precedence"): s65.index("- **RETIRE")]
    retire = next(ln for ln in s65.splitlines() if ln.startswith("- **RETIRE:**"))
    assert "not reachable" in go
    assert "D2-H2 FAIL" in retire and "FAIL" not in retire.replace("D2-H2 FAIL", "").replace("not FAIL", "")
    assert "D2-H2 is UNDERPOWERED" in hold and "D2-H2 has PASSed" in hold and "FORWARD_PASS_UNREPLICATED" in hold
    assert "either" not in hold
    assert "A D2-H2 FAIL is RETIRE" in prec and "H1" not in prec and "any judged cell" not in prec
    assert "D2-H3 decides only its own GO/RETIRE" in s65
    assert "usage-based purchase" not in section(rules, "### 6.6", "### 6.7").split("Spend (revision 7)")[0]


def test_g5_is_closed_section_4_claims_nothing_untouched_and_3_1_is_aligned():
    rules = live()
    g5 = next(ln for ln in rules.splitlines() if ln.startswith("| G5"))
    assert "CLOSED" in g5 and "No fetch manifest" in g5 and "approval" not in g5
    assert "Untouched options history exists" not in rules
    assert "family-specific reconciliation" in rules and "protected reservation" in rules
    row = next(ln for ln in rules.splitlines() if ln.startswith("| OPRA cbbo-1m, near-spot 0DTE strikes"))
    assert "a **candidate** replication window" not in row and "not a candidate replication window" in row


def test_every_rendered_signature_display_is_revision_7_and_the_old_one_is_history():
    head = FILE.splitlines()[:22]
    shown = [ln for ln in head if "2026-11-14" in ln and "igned" in ln]
    assert shown
    for ln in shown:
        assert "revision 6" in ln.lower() or "superseded" in ln.lower(), ln[:120]
    assert "revision 7 owner-signed 2026-11-26" in FILE.splitlines()[0]
    assert "unsigned" not in FILE.split("\n---\n", 1)[0].lower()
    assert "### 10.5 Owner signature (2026-11-14)" in BODY and "### 10.6 Revision 7" in BODY


def test_old_h1_text_is_kept_as_history_not_deleted():
    for kept in ("D2-H1a and D2-H1b** are each tested separately", "At n = 300 per H1 cell", "Reconcile the full inspection ledger"):
        assert kept in BODY


def test_the_frozen_admission_spec_no_longer_supports_the_struck_cells():
    assert set(A.FROZEN_SPEC["hypotheses"]) == {"D1-F1", "D1-F2", "D2-H2", "D2-H3"}
    assert A.frozen_spec_errors() == [] and A.VERIFIER_ACCEPTED is False


def test_the_signature_is_not_a_cryptographic_signature_and_admits_nothing():
    m = json.loads((ROOT / M.MANIFEST_PATH).read_text(encoding="utf-8"))
    assert m["research_owner"]["signature"]["admits_nothing"] is True and m["reviewer"]["signed"] is False
    assert not any(h.get("status") == "ADMITTED" for h in m["hypotheses"])
    assert M.validate(m) == [] and M.contract_pin_errors(m) == []


def test_section_4_carries_the_reconciled_gamma_vol_disclosure():
    rows = [ln for ln in BODY.splitlines() if ln.startswith("| GEX gamma-vol")]
    assert len(rows) == 2
    part1, part2 = rows
    assert "part 1" in part1 and "outside the target window" in part1 and "2025-11-19" in part1
    assert "part 2" in part2 and "by date only" in part2 and "not option premium" in part2
    assert "2019-12-31" in part2 and "2020-01-02 .. 2026-11-17" in part2
