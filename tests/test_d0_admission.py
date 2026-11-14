"""Tests for the D0 admission verifier (G14). Synthetic only: throwaway SSH keys in tmp_path.

One fixture builds a complete, correctly signed admission in a temporary repository. Each
test then breaks one prerequisite and checks that the verifier refuses it.
"""

from __future__ import annotations

import copy
from datetime import date
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

import pytest

from analysis import d0_admission as A
from analysis import d0_manifest as M

pytestmark = pytest.mark.skipif(shutil.which("ssh-keygen") is None, reason="ssh-keygen not installed")

TODAY = date(2026, 11, 20)
HYP = "D2-H2"
INP = "test.synthetic_input"


def _write(root: Path, rel: str, data: bytes) -> dict:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return {"path": rel, "sha256": hashlib.sha256(data).hexdigest()}


def _keygen(root: Path, name: str) -> tuple[Path, str]:
    key = root / "keys" / name
    key.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", name, "-f", str(key)], check=True)
    return key, Path(str(key) + ".pub").read_text().strip()


def _sign(root: Path, manifest: dict, hid: str, principal: str, key: Path) -> None:
    stmt = root / "tmp" / f"{hid}.{principal}.statement"
    stmt.parent.mkdir(parents=True, exist_ok=True)
    stmt.write_bytes(A.admission_statement(manifest, hid))
    sig = Path(str(stmt) + ".sig")
    sig.unlink(missing_ok=True)  # ssh-keygen -Y sign silently keeps an existing .sig (it prompts, exits 0)
    subprocess.run(["ssh-keygen", "-Y", "sign", "-f", str(key), "-n", A.NAMESPACE, str(stmt)],
                   check=True, capture_output=True, stdin=subprocess.DEVNULL)
    assert sig.is_file()
    dest = root / A.SIGNATURE_DIR / f"{hid}.{principal}.sig"
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(str(stmt) + ".sig", dest)


@pytest.fixture()
def world(tmp_path):
    root = tmp_path
    contract = (M.REPO / M.CONTRACT_PATH).read_bytes()
    _write(root, str(M.CONTRACT_PATH), contract)
    m = json.loads((M.REPO / M.MANIFEST_PATH).read_text(encoding="utf-8"))
    snap = _write(root, "receipts/q.result.json", b'{"rows": []}\n')
    query = _write(root, "receipts/q.sql", b"SELECT 1;\n")
    m["inputs"].append({
        "id": INP, "admission_status": "ADMITTED", "missingness": "0 of 120 sessions missing (receipt q)",
        "clocks": {
            "first_availability_receipt": _write(root, "receipts/first_availability.json", b'{"ok": 1}\n'),
            "durable_visibility": {"state": "qualified", "reason": "exact-version commit receipt",
                                   "receipt": _write(root, "receipts/visibility.json", b'{"v": 1}\n')},
        },
        "receipts": {"snapshot": snap["path"], "receipt_sha256": snap["sha256"], "query_sha256": query["sha256"]},
    })
    h = next(h for h in m["hypotheses"] if h["id"] == HYP)
    h.update(status="ADMITTED", required_inputs=[INP], blocking_gaps=[],
             coverage={"measured_n": 120, "receipt": _write(root, "receipts/coverage.json", b'{"n": 120}\n')},
             admission={"signed_at": "2026-11-15"})
    keys = {p: _keygen(root, p) for p in A.REQUIRED_PRINCIPALS}
    signers = "".join(f'{p} namespaces="{A.NAMESPACE}" {pub}\n' for p, (_, pub) in keys.items())
    _write(root, str(A.ALLOWED_SIGNERS), signers.encode())
    for p, (key, _) in keys.items():
        _sign(root, m, HYP, p, key)
    return {"root": root, "m": m, "keys": keys}


def _errs(w, m=None, accepted=True):
    return A.admission_errors(m if m is not None else w["m"], repo_root=w["root"], today=TODAY, accepted=accepted)


def _has(errs, needle):
    return any(needle in e for e in errs), errs


def test_a_complete_signed_admission_verifies(world):
    assert _errs(world) == []


def test_nothing_is_admitted_until_the_verifier_is_accepted(world):
    assert A.VERIFIER_ACCEPTED is False
    ok, errs = _has(_errs(world, accepted=False), "not yet accepted by the reviewer")
    assert ok, errs
    ok, errs = _has(A.admission_errors(world["m"], repo_root=world["root"], today=TODAY), "not yet accepted")
    assert ok, errs


def test_committed_manifest_admits_nothing_and_ships_no_keys():
    assert A.admission_errors(json.loads((M.REPO / M.MANIFEST_PATH).read_text(encoding="utf-8"))) == []
    lines = (M.REPO / A.ALLOWED_SIGNERS).read_text(encoding="utf-8").splitlines()
    assert all(ln.lstrip().startswith("#") or not ln.strip() for ln in lines)


def test_empty_allowed_signers_refuses(world):
    (world["root"] / A.ALLOWED_SIGNERS).write_text("# no keys\n")
    ok, errs = _has(_errs(world), "has no key in allowed_signers")
    assert ok, errs


def test_missing_principal_signature_refuses(world):
    (world["root"] / A.SIGNATURE_DIR / f"{HYP}.operator.sig").unlink()
    ok, errs = _has(_errs(world), "operator.sig missing")
    assert ok, errs


def test_a_key_cannot_sign_for_another_principal(world):
    _sign(world["root"], world["m"], HYP, "reviewer", world["keys"]["operator"][0])
    ok, errs = _has(_errs(world), "does not verify for 'reviewer'")
    assert ok, errs


def test_any_manifest_edit_after_signing_invalidates_signatures(world):
    m = copy.deepcopy(world["m"])
    next(h for h in m["hypotheses"] if h["id"] == HYP)["n_floor"] = 99
    ok, errs = _has(_errs(world, m), "does not verify")
    assert ok, errs


def test_reviewer_result_fields_are_outside_the_digest(world):
    m = copy.deepcopy(world["m"])
    m["reviewer"].update(signed=True, signed_at="2026-11-16", manifest_sha256=A.manifest_digest(world["m"]))
    assert A.manifest_digest(m) == A.manifest_digest(world["m"])
    assert _errs(world, m) == []
    m["reviewer"]["manifest_sha256"] = "c" * 64
    ok, errs = _has(_errs(world, m), "does not equal the manifest digest")
    assert ok, errs
    m["reviewer"]["signed"] = "true"
    ok, errs = _has(_errs(world, m), "reviewer.signed must be exactly true or false")
    assert ok, errs


@pytest.mark.parametrize("change, needle", [
    (lambda h, m: h.pop("required_inputs"), "required_inputs must be a non-empty list"),
    (lambda h, m: h.update(required_inputs=[]), "required_inputs must be a non-empty list"),
    (lambda h, m: h.update(required_inputs=[INP, INP]), "required_inputs must be a non-empty list"),
    (lambda h, m: h.update(required_inputs=["no.such.input"]), "unknown dependency"),
    (lambda h, m: h.update(n_floor=True), "n_floor must be a positive integer"),
    (lambda h, m: h["coverage"].update(measured_n=99), "below the sample floor"),
    (lambda h, m: h["coverage"].update(measured_n=True), "measured_n must be an integer"),
    (lambda h, m: h.pop("coverage"), "coverage not measured"),
    (lambda h, m: h.update(blocking_gaps=["G14"]), "blocking gap G14 is not closed"),
    (lambda h, m: h.update(blocking_gaps=["G99"]), "unknown blocking gap"),
    (lambda h, m: h.pop("blocking_gaps"), "blocking_gaps must be a list"),
    (lambda h, m: h.update(admission={"signed_at": "2026-02-30"}), "not a real YYYY-MM-DD"),
    (lambda h, m: h.update(admission={"signed_at": "2026-12-01"}), "in the future"),
    (lambda h, m: h.update(admission={"signed_at": "2026-11-01"}), "before the owner's signature"),
    (lambda h, m: m["research_owner"].update(signed="true"), "research_owner.signed must be exactly true"),
    (lambda h, m: m.update(signed_at="2026-13-01"), "not a real YYYY-MM-DD"),
    (lambda h, m: m.update(contract_body_sha256="a" * 64), "does not equal the pinned body hash"),
])
def test_each_hypothesis_prerequisite_is_enforced(world, change, needle):
    m = copy.deepcopy(world["m"])
    change(next(h for h in m["hypotheses"] if h["id"] == HYP), m)
    ok, errs = _has(_errs(world, m), needle)
    assert ok, errs


@pytest.mark.parametrize("change, needle", [
    (lambda i, r: i.update(admission_status="CANDIDATE_UNREVIEWED"), "required input is not ADMITTED"),
    (lambda i, r: i.update(missingness=""), "missingness not measured"),
    (lambda i, r: i.update(missingness="not measured"), "missingness not measured"),
    (lambda i, r: i["clocks"].update(first_availability_receipt="after close"), "not a file receipt"),
    (lambda i, r: i["clocks"].update(durable_visibility=None), "never an exemption"),
    (lambda i, r: i["clocks"].update(durable_visibility="UNPROVEN"), "never an exemption"),
    (lambda i, r: i["clocks"]["durable_visibility"].update(state="applicable"), "applies but is not qualified"),
    (lambda i, r: i["clocks"]["durable_visibility"].update(state="ok"), "not in"),
    (lambda i, r: i["clocks"]["durable_visibility"].update(reason=""), "has no reason"),
    (lambda i, r: (r / "receipts/visibility.json").write_bytes(b"changed\n"), "does not hash"),
    (lambda i, r: i["clocks"]["durable_visibility"]["receipt"].update(path="../outside.json"), "escapes the repository"),
    (lambda i, r: i["clocks"]["durable_visibility"]["receipt"].update(path="/etc/hostname"), "repository-relative"),
    (lambda i, r: i["receipts"].update(receipt_sha256="x"), "not a lowercase hex digest"),
    (lambda i, r: (r / "receipts/q.sql").unlink(), "does not exist"),
])
def test_each_input_prerequisite_is_enforced(world, change, needle):
    m = copy.deepcopy(world["m"])
    change(next(i for i in m["inputs"] if i["id"] == INP), world["root"])
    ok, errs = _has(_errs(world, m), needle)
    assert ok, errs


def test_an_admitted_input_needs_an_admitted_hypothesis(world):
    m = copy.deepcopy(world["m"])
    next(h for h in m["hypotheses"] if h["id"] == HYP)["status"] = "COLLECTING"
    ok, errs = _has(_errs(world, m), "without an ADMITTED hypothesis")
    assert ok, errs


def test_missing_ssh_keygen_refuses(world, monkeypatch):
    monkeypatch.setattr(A.shutil, "which", lambda name: None)
    ok, errs = _has(_errs(world), "ssh-keygen not found")
    assert ok, errs


def test_validate_is_the_single_entry_point(world):
    errs = M.validate(world["m"], repo_root=world["root"])
    assert any("not yet accepted by the reviewer" in e for e in errs)


def test_statement_cli_prints_the_signed_bytes(capsysbinary):
    assert A.main(["statement", "D1-F1"]) == 0
    out = capsysbinary.readouterr().out
    body = json.loads(out)
    assert body["hypothesis"] == "D1-F1" and body["contract_body_sha256"] == M.D0_CONTRACT_BODY_SHA256
    assert body["namespace"] == A.NAMESPACE


def _signers_with(world, lines):
    (world["root"] / A.ALLOWED_SIGNERS).write_text("".join(ln + "\n" for ln in lines))


@pytest.mark.parametrize("make, needle", [
    # one person holding both roles: the same key under both principals
    (lambda r, o: [f'reviewer namespaces="{A.NAMESPACE}" {r}', f'operator namespaces="{A.NAMESPACE}" {r}'],
     "same key is listed"),
    (lambda r, o: [f'reviewer,operator namespaces="{A.NAMESPACE}" {r}'], "no lists or wildcards"),
    (lambda r, o: [f'* namespaces="{A.NAMESPACE}" {r}'], "no lists or wildcards"),
    (lambda r, o: [f'reviewer cert-authority,namespaces="{A.NAMESPACE}" {r}', f'operator namespaces="{A.NAMESPACE}" {o}'],
     "options must be exactly"),
    (lambda r, o: [f'reviewer {r}', f'operator namespaces="{A.NAMESPACE}" {o}'], "options must be exactly"),
    (lambda r, o: [f'reviewer namespaces="{A.NAMESPACE}" {r}', f'reviewer namespaces="{A.NAMESPACE}" {o}',
                   f'operator namespaces="{A.NAMESPACE}" {o}'], "more than one key"),
    (lambda r, o: [f'reviewer namespaces="{A.NAMESPACE}" {r}', f'operator namespaces="{A.NAMESPACE}" {o}',
                   f'intruder namespaces="{A.NAMESPACE}" {o}'], "is not one of"),
])
def test_two_party_control_cannot_be_collapsed(world, make, needle):
    r, o = world["keys"]["reviewer"][1], world["keys"]["operator"][1]
    _signers_with(world, make(r, o))
    ok, errs = _has(_errs(world), needle)
    assert ok, errs


def test_operator_signature_under_both_names_with_one_key_is_refused(world):
    # the operator signs as both principals with one key listed twice
    r_key, o_key = world["keys"]["operator"][0], world["keys"]["operator"][0]
    o_pub = world["keys"]["operator"][1]
    _signers_with(world, [f'reviewer namespaces="{A.NAMESPACE}" {o_pub}', f'operator namespaces="{A.NAMESPACE}" {o_pub}'])
    _sign(world["root"], world["m"], HYP, "reviewer", r_key)
    _sign(world["root"], world["m"], HYP, "operator", o_key)
    ok, errs = _has(_errs(world), "same key is listed")
    assert ok, errs
