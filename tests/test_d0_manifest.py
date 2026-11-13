"""Tests for the D0 manifest validator: ADMITTED is refused without its prerequisites."""

from __future__ import annotations

import copy
import json

from analysis import d0_manifest as M


def _real():
    return json.loads((M.REPO / M.MANIFEST_PATH).read_text(encoding="utf-8"))


def test_committed_manifest_is_valid_and_admits_nothing_while_unsigned():
    m = _real()
    assert M.validate(m) == []
    assert m["signed_at"] is None and m["contract_body_sha256"] is None
    assert not any(i["admission_status"] == "ADMITTED" for i in m["inputs"])
    assert not any(h.get("status") == "ADMITTED" for h in m["hypotheses"])


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


def test_admitted_while_unsigned_is_refused_with_every_reason():
    m = _real()
    inp = _admitted_input()
    inp["missingness"] = ""
    inp["clocks"] = {"first_availability_receipt": None, "durable_visibility": "UNPROVEN (ingested_at = txn start)"}
    inp["receipts"] = {}
    m["inputs"].append(inp)
    errs = " | ".join(M.validate(m))
    for needle in ("contract not signed", "reviewer has not signed", "missingness", "first-availability receipt",
                   "durable visibility UNPROVEN", "file or query receipt"):
        assert needle in errs, needle


def test_admitted_with_all_prerequisites_passes():
    m = _signed(_real())
    m["inputs"].append(_admitted_input())
    assert M.validate(m) == []


def test_unknown_status_and_unsigned_hypothesis_admission_are_refused():
    m = _real()
    bad = copy.deepcopy(m["inputs"][0])
    bad["admission_status"] = "APPROVED"
    m["inputs"].append(bad)
    m["hypotheses"][0] = dict(m["hypotheses"][0], status="ADMITTED")
    errs = " | ".join(M.validate(m))
    assert "not in admission_status_values" in errs
    assert "hypothesis ADMITTED while the contract or manifest is unsigned" in errs


def test_cli_exit_codes(tmp_path):
    ok = tmp_path / "ok.json"
    ok.write_text(json.dumps(_real()))
    assert M.main(["--manifest", str(ok)]) == 0
    m = _real()
    m["inputs"].append(_admitted_input())
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps(m))
    assert M.main(["--manifest", str(bad)]) == 1
