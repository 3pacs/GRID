"""Tests for the D0 manifest checker (every ADMITTED refused) and the owner-signed contract body pin."""

from __future__ import annotations

import copy
import json

from analysis import d0_manifest as M


def _real():
    return json.loads((M.REPO / M.MANIFEST_PATH).read_text(encoding="utf-8"))


def test_committed_manifest_is_owner_signed_pinned_and_admits_nothing():
    m = _real()
    assert M.validate(m) == []
    assert M.contract_pin_errors(m) == []
    assert m["research_owner"]["signed"] is True and m["signed_at"] == "2026-11-14"
    assert m["contract_body_sha256"] == M.D0_CONTRACT_BODY_SHA256
    assert m["reviewer"]["signed"] is False
    assert not any(i["admission_status"] == "ADMITTED" for i in m["inputs"])
    assert not any(h.get("status") == "ADMITTED" for h in m["hypotheses"])


def test_contract_body_hash_is_pinned_and_lf_stable():
    text = (M.REPO / M.CONTRACT_PATH).read_text(encoding="utf-8")
    assert M.body_sha256(M.contract_body(text)) == M.D0_CONTRACT_BODY_SHA256
    assert M.body_sha256(M.contract_body(text.replace("\n", "\r\n"))) == M.D0_CONTRACT_BODY_SHA256


def _contract_copy(tmp_path, text):
    (tmp_path / M.CONTRACT_PATH).parent.mkdir(parents=True)
    (tmp_path / M.CONTRACT_PATH).write_text(text, encoding="utf-8")
    return tmp_path


def test_any_body_edit_is_refused(tmp_path):
    text = (M.REPO / M.CONTRACT_PATH).read_text(encoding="utf-8")
    edited = text.replace("≥ 200 events in the top quintile", "≥ 150 events in the top quintile", 1)
    assert edited != text
    errs = M.contract_pin_errors(_real(), repo_root=_contract_copy(tmp_path, edited))
    assert any("!= pinned" in e for e in errs)


def test_text_outside_the_body_is_not_pinned(tmp_path):
    text = (M.REPO / M.CONTRACT_PATH).read_text(encoding="utf-8")
    assert M.contract_pin_errors(_real(), repo_root=_contract_copy(tmp_path, "# retitled\n" + text.split("\n", 1)[1])) == []


def test_missing_or_duplicate_markers_are_refused(tmp_path):
    text = (M.REPO / M.CONTRACT_PATH).read_text(encoding="utf-8")
    errs = M.contract_pin_errors(_real(), repo_root=_contract_copy(tmp_path, text + "\n" + M.BODY_END + "\n"))
    assert errs and "unreadable" in errs[0]


def test_manifest_signature_fields_must_match_the_pin():
    m = _real()
    m["contract_body_sha256"] = "a" * 64
    assert any("does not equal the pinned" in e for e in M.contract_pin_errors(m))
    m = _real()
    m["signed_at"] = "14 Nov 2026"
    assert any("not a real YYYY-MM-DD" in e for e in M.contract_pin_errors(m))
    m = _real()
    m["signed_at"] = "2026-02-30"
    assert any("not a real YYYY-MM-DD" in e for e in M.contract_pin_errors(m))
    m = _real()
    m["research_owner"]["signed"] = "true"
    assert any("research_owner.signed is not true" in e for e in M.contract_pin_errors(m))
    m = _real()
    m["research_owner"]["signed"] = "true"
    m["contract_body_sha256"] = "X" * 64
    assert any("lowercase sha256" in e for e in M.contract_pin_errors(m))


def _admitted_input():
    return {
        "id": "x", "admission_status": "ADMITTED", "missingness": "0 missing sessions 2011..2026",
        "clocks": {"first_availability_receipt": "sha256:abc", "durable_visibility": None},
        "receipts": {"query_sha256": "f" * 64},
    }


def _signed(m):
    m["signed_at"] = "2026-12-01"
    m["contract_body_sha256"] = "a" * 64
    m["research_owner"]["signed"] = True
    m["reviewer"]["signed"] = True
    m["reviewer"]["manifest_sha256"] = "b" * 64
    return m


def _unsigned(m):
    m["signed_at"] = None
    m["contract_body_sha256"] = None
    m["research_owner"]["signed"] = False
    return m


def test_admitted_while_unsigned_is_refused_with_every_reason():
    m = _unsigned(_real())
    inp = _admitted_input()
    inp["missingness"] = ""
    inp["clocks"] = {"first_availability_receipt": None, "durable_visibility": "UNPROVEN (ingested_at = txn start)"}
    inp["receipts"] = {}
    m["inputs"].append(inp)
    errs = " | ".join(M.validate(m))
    for needle in ("not yet accepted by the reviewer", "without an ADMITTED hypothesis", "missingness not measured",
                   "first-availability receipt: not a file receipt", "null or text is never an exemption",
                   "query receipt snapshot"):
        assert needle in errs, needle


def test_declared_prerequisites_do_not_authenticate_admission():
    m = _signed(_real())
    m["inputs"].append(_admitted_input())
    assert any("not yet accepted by the reviewer" in e for e in M.validate(m))


def test_unknown_status_and_unsigned_hypothesis_admission_are_refused():
    m = _real()
    bad = copy.deepcopy(m["inputs"][0])
    bad["admission_status"] = "APPROVED"
    m["inputs"].append(bad)
    m["hypotheses"][0] = dict(m["hypotheses"][0], status="ADMITTED")
    errs = " | ".join(M.validate(m))
    assert "not in admission_status_values" in errs
    assert "ADMITTED refused: the G14 verifier is not yet accepted by the reviewer" in errs
    assert "required_inputs must be a non-empty list" in errs


def test_cli_exit_codes(tmp_path):
    ok = tmp_path / "ok.json"
    ok.write_text(json.dumps(_real()))
    assert M.main(["--manifest", str(ok)]) == 0
    m = _real()
    m["inputs"].append(_admitted_input())
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps(m))
    assert M.main(["--manifest", str(bad)]) == 1


def test_signed_hypothesis_without_dependencies_is_refused():
    m = _signed(_real())
    m["hypotheses"][0]["status"] = "ADMITTED"
    assert M.validate(m)

def test_false_string_signatures_cannot_admit():
    m = _signed(_real())
    m["research_owner"]["signed"] = "false"
    m["reviewer"]["signed"] = "false"
    m["signed_at"] = "not-a-date"
    m["contract_body_sha256"] = "x"
    m["reviewer"]["manifest_sha256"] = "y"
    m["hypotheses"][0]["status"] = "ADMITTED"
    assert M.validate(m)

def test_null_visibility_cannot_admit_backfilled_input():
    m = _signed(_real())
    i = next(i for i in m["inputs"] if i["id"] == "grid.raw_series.CBOE:SKEW")
    i["admission_status"] = "ADMITTED"
    i["clocks"]["first_availability_receipt"] = "sha256:placeholder"
    i["clocks"]["durable_visibility"] = None
    assert M.validate(m)
