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
INP = "ledger.fly_shadow_1.fly_days"  # a frozen D2-H2 input, added to a tmp manifest copy only


def _write(root: Path, rel: str, data: bytes) -> dict:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return {"path": rel, "sha256": hashlib.sha256(data).hexdigest()}


def _write_json(root: Path, rel: str, obj: dict) -> dict:
    return _write(root, rel, (json.dumps(obj, indent=1, sort_keys=True) + "\n").encode())


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


def _synthetic_input(root: Path, iid: str, prefix: str) -> dict:
    snap = _write(root, f"{prefix}q.result.json", b'{"rows": []}\n')
    query = _write(root, f"{prefix}q.sql", b"SELECT 1;\n")
    return {
        "id": iid, "admission_status": "ADMITTED", "missingness": "0 of 120 sessions missing (receipt q)",
        "clocks": {
            "first_availability_receipt": _write(root, f"{prefix}first_availability.json", b'{"ok": 1}\n'),
            "durable_visibility": {"state": "qualified", "reason": "exact-version commit receipt",
                                   "receipt": _write(root, f"{prefix}visibility.json", b'{"v": 1}\n')},
        },
        "receipts": {"snapshot": snap["path"], "receipt_sha256": snap["sha256"], "query_sha256": query["sha256"]},
    }


def _g15_exception(root: Path) -> dict:
    return {
        "exception": "price_only", "source": "synthetic price source", "source_version": "synthetic-v1",
        "price_basis": "raw close", "retained_evidence": [_write(root, "receipts/g15/evidence.bin", b"synthetic\n")],
        "clocks_not_established": ["retrieval", "durable_visibility"],
        "forbidden_claims": ["C0 was public by 20:00 ET"],
        "qualified": True, "qualified_by": "reviewer", "clock_backfill": False, "outcome_read": False,
    }


def _closure(root: Path, gid: str, closed_at: str = "2026-11-15", **extra) -> dict:
    body = {"schema": A.GAP_CLOSURE_SCHEMA, "gap": gid, "state": A.CLOSED, "closed_at": closed_at, **extra}
    return {"state": A.CLOSED, "closed_at": closed_at,
            "evidence": _write_json(root, f"receipts/gaps/{gid}.closure.json", body)}


def _coverage_body(hid: str, n: int, g15_sha: str | None = None) -> dict:
    spec = A.FROZEN_SPEC["hypotheses"][hid]
    body = {"schema": A.COVERAGE_RECEIPT_SCHEMA, "hypothesis": hid,
            "scope": {"window": spec["scope"], "range": spec["window"][spec["scope"]]},
            "unit": spec["coverage_unit"], "inputs": list(spec["required_inputs"]), "measured_n": n,
            "measured_at": "2026-11-15"}
    if spec["cells"]:
        body["cells"] = {c: {"top_quintile_events": n, "distinct_event_weeks": 80} for c in spec["cells"]}
    if spec["judged_historical_d1"]:
        body["g15_closure_sha256"] = g15_sha
    return body


def _build(root: Path, hid: str, n: int) -> dict:
    """A synthetic tmp repository whose manifest copy carries every prerequisite for ``hid``."""
    spec = A.FROZEN_SPEC["hypotheses"][hid]
    _write(root, str(M.CONTRACT_PATH), (M.REPO / M.CONTRACT_PATH).read_bytes())
    m = json.loads((M.REPO / M.MANIFEST_PATH).read_text(encoding="utf-8"))
    # the synthetic world fixes its own owner date: its chronology must not move when the committed manifest is re-signed
    m["signed_at"] = "2026-11-14"
    for iid in spec["required_inputs"]:
        prefix = "receipts/" if iid == INP else f"receipts/{iid}/"
        inp = _synthetic_input(root, iid, prefix)
        m["inputs"] = [i for i in m["inputs"] if i["id"] != iid] + [inp]
    g15_sha = None
    for g in m["gaps"]:
        if g["id"] in spec["blocking_gaps"]:
            extra = _g15_exception(root) if g["id"] == "G15" else {}
            g["closure"] = _closure(root, g["id"], **extra)
            if g["id"] == "G15":
                g15_sha = g["closure"]["evidence"]["sha256"]
    h = next(h for h in m["hypotheses"] if h["id"] == hid)
    h.update(status="ADMITTED", required_inputs=list(spec["required_inputs"]),
             blocking_gaps=list(spec["blocking_gaps"]),
             coverage={"measured_n": n,
                       "receipt": _write_json(root, f"receipts/{hid}.coverage.json", _coverage_body(hid, n, g15_sha))},
             admission={"signed_at": "2026-11-15"})
    # the reviewer's date is inside the digest and the statement: it is set before anyone signs
    m["reviewer"]["signed_at"] = "2026-11-15"
    keys = {p: _keygen(root, p) for p in A.REQUIRED_PRINCIPALS}
    signers = "".join(f'{p} namespaces="{A.NAMESPACE}" {pub}\n' for p, (_, pub) in keys.items())
    _write(root, str(A.ALLOWED_SIGNERS), signers.encode())
    for p, (key, _) in keys.items():
        _sign(root, m, hid, p, key)
    # only the reviewer's result fields are outside the digest, so recording them keeps the signatures
    m["reviewer"].update(signed=True, manifest_sha256=A.manifest_digest(m))
    return {"root": root, "m": m, "keys": keys, "hid": hid}


def _resign(w: dict, m: dict) -> dict:
    """Re-sign ``m`` with both synthetic keys, so a refusal cannot be blamed on a stale signature."""
    for p, (key, _) in w["keys"].items():
        _sign(w["root"], m, w["hid"], p, key)
    m["reviewer"]["manifest_sha256"] = A.manifest_digest(m)
    return m


@pytest.fixture()
def world(tmp_path):
    return _build(tmp_path, HYP, 120)


@pytest.fixture()
def d1world(tmp_path):
    return _build(tmp_path, "D1-F1", 240)


def _errs(w, m=None, accepted=True):
    return A.admission_errors(m if m is not None else w["m"], repo_root=w["root"], today=TODAY, accepted=accepted)


def _refused(w, m, needle, base=None):
    """``m`` is refused for ``needle``; the unbroken synthetic world (``base``, its errors taken before
    any on-disk break) was not refused for it, so the break is what the verifier caught."""
    errs = _errs(w, m)
    assert errs and any(needle in e for e in errs), errs
    base = _errs(w) if base is None else base
    assert not any(needle in e for e in base), ("fixture already carries the refusal", base)


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
    assert A.DIGEST_EXCLUDED_REVIEWER_KEYS == ("signed", "manifest_sha256")
    m = copy.deepcopy(world["m"])
    m["reviewer"].update(signed=True, manifest_sha256=A.manifest_digest(world["m"]))
    assert A.manifest_digest(m) == A.manifest_digest(world["m"])
    assert _errs(world, m) == []
    dated = copy.deepcopy(m)
    dated["reviewer"]["signed_at"] = "2026-11-16"  # the reviewer's date is inside the digest
    assert A.manifest_digest(dated) != A.manifest_digest(world["m"])
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


# --- Review of bc52a40c (2026-11-21): synthetic refusal tests only. Each case breaks one thing in a
# tmp manifest copy, re-signs it where the break would otherwise only fail the signature, and must be
# refused. None of them admits anything; the committed manifest and allowed_signers are untouched.

def _hyp(m, hid):
    return next(h for h in m["hypotheses"] if h["id"] == hid)


def _gap(m, gid):
    return next(g for g in m["gaps"] if g["id"] == gid)


def _rewrite_coverage(w, m, hid, **changes):
    """Rewrite the coverage receipt file with ``changes`` and record its new hash (a valid receipt of bad content)."""
    h = _hyp(m, hid)
    body = json.loads((w["root"] / h["coverage"]["receipt"]["path"]).read_text())
    for key, val in changes.items():
        if val is _DROP:
            body.pop(key, None)
        else:
            body[key] = val
    h["coverage"]["receipt"] = _write_json(w["root"], f"receipts/{hid}.coverage.rewritten.json", body)
    return m


def _rewrite_closure(w, m, gid, **changes):
    g = _gap(m, gid)
    body = json.loads((w["root"] / g["closure"]["evidence"]["path"]).read_text())
    for key, val in changes.items():
        if val is _DROP:
            body.pop(key, None)
        else:
            body[key] = val
    g["closure"]["evidence"] = _write_json(w["root"], f"receipts/gaps/{gid}.closure.rewritten.json", body)
    return m


_DROP = object()


def test_committed_manifest_agrees_with_the_frozen_spec_and_its_pin():
    m = json.loads((M.REPO / M.MANIFEST_PATH).read_text(encoding="utf-8"))
    assert A.frozen_spec_errors() == []
    assert A.frozen_spec_digest() == A.FROZEN_SPEC_SHA256
    assert A.frozen_drift_errors(m) == []
    assert A.VERIFIER_ACCEPTED is False
    assert not any(h.get("status") == "ADMITTED" for h in m["hypotheses"])


def test_frozen_numbers_are_the_signed_contract_body():
    body = M.contract_body((M.REPO / M.CONTRACT_PATH).read_text(encoding="utf-8"))
    assert M.body_sha256(body) == M.D0_CONTRACT_BODY_SHA256
    for needle in ("≥ 200 events in the top quintile and ≥ 60 distinct event weeks per cell in the holdout",
                   "1 − 0.05/4 = 0.9875", "Holdout: 2019-01-01 .. 2025-12-31", "Development: acceptance dates 2012-01-01 .. 2018-12-31",
                   "[entry, +5 sessions] and [entry, +20 sessions]", "1 − 0.05/2 = 0.975",
                   "D2-H2 n = 100; D2-H3 n = 50", "levels (0.95) unchanged", "live Schwab chain, ledger `fly_days`",
                   "| D1-F1, D1-F2 history |", "| all D1 history |",
                   "every judged historical D1 result; the D1 holdout stays unopened"):
        assert needle in body, needle
    hyps = A.FROZEN_SPEC["hypotheses"]
    assert set(hyps) == {"D1-F1", "D1-F2", "D2-H2", "D2-H3"}  # D1-F3 is evidence-only; D2-H1a/b struck in revision 7
    assert all("G15" in hyps[h]["blocking_gaps"] and hyps[h]["judged_historical_d1"] for h in ("D1-F1", "D1-F2"))


def test_a_tampered_frozen_spec_refuses_everything(world, monkeypatch):
    spec = copy.deepcopy(A.FROZEN_SPEC)
    spec["hypotheses"][HYP]["n_floor"] = 10
    monkeypatch.setattr(A, "FROZEN_SPEC", spec)
    ok, errs = _has(_errs(world), "frozen spec digest")
    assert ok, errs
    assert any("frozen spec digest" in e for e in M.validate(world["m"], repo_root=world["root"]))


def test_an_unsupported_hypothesis_is_refused(world):
    m = copy.deepcopy(world["m"])
    _hyp(m, "D1-F3").update(status="ADMITTED", required_inputs=[INP], blocking_gaps=[],
                           n_floor=1, coverage=copy.deepcopy(_hyp(m, HYP)["coverage"]),
                           admission={"signed_at": "2026-11-15"})
    ok, errs = _has(_errs(world, m), "D1-F3: not a supported hypothesis")
    assert ok, errs


@pytest.mark.parametrize("change, needle", [
    # the manifest lowers its own floor and coverage together: the frozen floor still applies
    (lambda w, m: (_hyp(m, HYP).update(n_floor=50), _hyp(m, HYP)["coverage"].update(measured_n=60),
                   _rewrite_coverage(w, m, HYP, measured_n=60)), "below the sample floor 100 (frozen)"),
    (lambda w, m: (_hyp(m, HYP).update(n_floor=50), _hyp(m, HYP)["coverage"].update(measured_n=60),
                   _rewrite_coverage(w, m, HYP, measured_n=60)), "manifest n_floor 50 differs from the frozen spec"),
    # the manifest drops a frozen dependency: it is still required, and the drift is named
    (lambda w, m: _hyp(m, HYP).update(required_inputs=[INP]), "differ from the frozen required inputs"),
    (lambda w, m: (_hyp(m, HYP).update(required_inputs=[INP]),
                   next(i for i in m["inputs"] if i["id"] == "live.schwab.chains_quotes_bars")
                   .update(admission_status="FORWARD_ONLY")), "required input is not ADMITTED"),
    (lambda w, m: (_hyp(m, HYP).update(required_inputs=[INP]),
                   m.update(inputs=[i for i in m["inputs"] if i["id"] != "live.schwab.chains_quotes_bars"])),
     "frozen required input not in the manifest"),
    (lambda w, m: _hyp(m, HYP).update(one_sided_level=0.8), "manifest one_sided_level 0.8 differs"),
    (lambda w, m: _hyp(m, HYP).update(window={"forward_from": "2026-11-01"}), "manifest window"),
    (lambda w, m: _hyp(m, HYP).update(track="D1"), "manifest track 'D1' differs"),
])
def test_manifest_cannot_rewrite_frozen_floors_or_dependencies(world, change, needle):
    base = _errs(world)
    m = copy.deepcopy(world["m"])
    change(world, m)
    _refused(world, _resign(world, m), needle, base)


@pytest.mark.parametrize("change, needle", [
    (lambda w, m: _rewrite_coverage(w, m, HYP, measured_n=121), "receipt count 121 does not match coverage measured_n"),
    (lambda w, m: _rewrite_coverage(w, m, HYP, measured_n=True), "measured_n must be an integer"),
    (lambda w, m: (_hyp(m, HYP)["coverage"].update(measured_n=90), _rewrite_coverage(w, m, HYP, measured_n=90)),
     "below the frozen sample floor 100"),
    (lambda w, m: _rewrite_coverage(w, m, HYP, hypothesis="D2-H3"), "measured for hypothesis 'D2-H3'"),
    (lambda w, m: _rewrite_coverage(w, m, HYP, scope={"window": "forward_from", "range": "2026-11-01"}),
     "is not the frozen scope"),
    (lambda w, m: _rewrite_coverage(w, m, HYP, scope=_DROP), "is not the frozen scope"),
    (lambda w, m: _rewrite_coverage(w, m, HYP, unit="sessions"), "is not the frozen unit"),
    (lambda w, m: _rewrite_coverage(w, m, HYP, inputs=[INP]), "are not the frozen required inputs"),
    (lambda w, m: _rewrite_coverage(w, m, HYP, schema="d0-coverage-receipt/0"), "schema must be"),
    (lambda w, m: _rewrite_coverage(w, m, HYP, measured_at="2026-12-31"), "measured_at is in the future"),
    (lambda w, m: _hyp(m, HYP)["coverage"].update(
        receipt=_write(w["root"], "receipts/coverage.txt", b"n = 120\n")), "is not JSON"),
    (lambda w, m: _hyp(m, HYP)["coverage"].update(
        receipt=_write(w["root"], "receipts/coverage.list.json", b"[120]\n")), "is not a JSON object"),
])
def test_coverage_receipt_content_is_checked_not_only_its_bytes(world, change, needle):
    base = _errs(world)
    m = copy.deepcopy(world["m"])
    change(world, m)
    _refused(world, _resign(world, m), needle, base)


@pytest.mark.parametrize("change, needle", [
    # a status string that says closed never closes a gap
    (lambda w, m: (_gap(m, "G15").pop("closure"), _gap(m, "G15").update(status="closed 2026-11-15")),
     "blocking gap G15 is not closed: no typed closure"),
    (lambda w, m: (_gap(m, "G2").pop("closure"), _gap(m, "G2").update(status="CLOSED")),
     "blocking gap G2 is not closed: no typed closure"),
    (lambda w, m: _gap(m, "G3")["closure"].update(state="closed"), "closure.state must be exactly 'CLOSED'"),
    (lambda w, m: _gap(m, "G3")["closure"].update(state=True), "closure.state must be exactly 'CLOSED'"),
    (lambda w, m: _gap(m, "G3")["closure"].update(closed_at="2026-11-16"), "after the admission date"),
    (lambda w, m: _gap(m, "G3")["closure"].update(closed_at="2026-02-30"), "closed_at is not a real"),
    (lambda w, m: _gap(m, "G3")["closure"].update(evidence="docs/closure.md"), "not a file receipt"),
    (lambda w, m: (w["root"] / _gap(m, "G3")["closure"]["evidence"]["path"]).write_text("{}"), "does not hash"),
    (lambda w, m: _rewrite_closure(w, m, "G3", gap="G2"), "closure evidence is for gap 'G2', not G3"),
    (lambda w, m: _rewrite_closure(w, m, "G3", state="OPEN"), "closure evidence state must be exactly"),
    (lambda w, m: _rewrite_closure(w, m, "G3", schema="x"), "closure evidence schema must be"),
    (lambda w, m: _rewrite_closure(w, m, "G3", closed_at="2026-11-14"), "closed_at differs from the manifest"),
    # the manifest drops a frozen gap: it is still evaluated, and the omission is named
    (lambda w, m: _hyp(m, "D1-F1").update(blocking_gaps=["G2", "G3"]), "omits frozen blocking gap G15"),
    (lambda w, m: (_hyp(m, "D1-F1").update(blocking_gaps=[]), _gap(m, "G15").pop("closure")),
     "blocking gap G15 is not closed"),
])
def test_gaps_close_only_on_typed_evidence(d1world, change, needle):
    base = _errs(d1world)
    m = copy.deepcopy(d1world["m"])
    change(d1world, m)
    _refused(d1world, _resign(d1world, m), needle, base)


@pytest.mark.parametrize("field, value, needle", [
    ("exception", "full_availability", "exception must be exactly 'price_only'"),
    ("source", "", "source must be a non-empty string"),
    ("source_version", _DROP, "source_version must be a non-empty string"),
    ("price_basis", None, "price_basis must be a non-empty string"),
    ("clocks_not_established", [], "clocks_not_established must be a non-empty list"),
    ("forbidden_claims", "none", "forbidden_claims must be a non-empty list"),
    ("retained_evidence", [], "retained_evidence must be a non-empty list of file receipts"),
    ("retained_evidence", ["receipts/g15/evidence.bin"], "retained_evidence[0]: not a file receipt"),
    ("retained_evidence", [{"path": "receipts/g15/evidence.bin", "sha256": "0" * 64}], "does not hash"),
    ("qualified", 1, "qualified must be exactly true"),
    ("qualified", "true", "qualified must be exactly true"),
    ("qualified", _DROP, "qualified must be exactly true"),
    ("qualified_by", "operator", "qualified_by must be 'reviewer'"),
    ("clock_backfill", 0, "clock_backfill must be exactly false"),
    ("outcome_read", True, "outcome_read must be exactly false"),
    ("outcome_read", _DROP, "outcome_read must be exactly false"),
])
def test_judged_historical_d1_requires_the_typed_g15_exception(d1world, field, value, needle):
    m = _rewrite_closure(d1world, copy.deepcopy(d1world["m"]), "G15", **{field: value})
    _refused(d1world, _resign(d1world, m), needle)


@pytest.mark.parametrize("change, needle", [
    (lambda w, m: _rewrite_coverage(w, m, "D1-F1", g15_closure_sha256=_DROP),
     "g15_closure_sha256 does not name the frozen G15 closure"),
    (lambda w, m: _rewrite_coverage(w, m, "D1-F1", g15_closure_sha256=_gap(m, "G2")["closure"]["evidence"]["sha256"]),
     "g15_closure_sha256 does not name the frozen G15 closure"),
    (lambda w, m: _gap(m, "G15").pop("closure"), "requires the frozen G15 closure"),
    (lambda w, m: _rewrite_closure(w, m, "G15", qualified=False), "requires the frozen G15 closure"),
    (lambda w, m: _rewrite_coverage(w, m, "D1-F1", scope={"window": "dev", "range": ["2012-01-01", "2018-12-31"]}),
     "is not the frozen scope"),
    (lambda w, m: _rewrite_coverage(w, m, "D1-F1", cells={"+5": {"top_quintile_events": 240, "distinct_event_weeks": 80}}),
     "cells must be exactly the frozen horizons"),
    (lambda w, m: _rewrite_coverage(w, m, "D1-F1", cells={
        "+5": {"top_quintile_events": 240, "distinct_event_weeks": 59},
        "+20": {"top_quintile_events": 240, "distinct_event_weeks": 80}}), "cell +5 distinct_event_weeks 59 is below"),
    (lambda w, m: _rewrite_coverage(w, m, "D1-F1", cells={
        "+5": {"top_quintile_events": 240, "distinct_event_weeks": 80},
        "+20": {"top_quintile_events": 199, "distinct_event_weeks": 80}}), "cell +20 top_quintile_events 199 is below"),
    (lambda w, m: _rewrite_coverage(w, m, "D1-F1", cells={
        "+5": {"top_quintile_events": 240, "distinct_event_weeks": 80},
        "+20": {"top_quintile_events": 230, "distinct_event_weeks": 80}}), "is not the smallest cell count 230"),
    (lambda w, m: _rewrite_coverage(w, m, "D1-F1", cells={
        "+5": {"top_quintile_events": 240, "distinct_event_weeks": 80.0},
        "+20": {"top_quintile_events": 240, "distinct_event_weeks": 80}}), "distinct_event_weeks must be an integer"),
])
def test_d1_coverage_receipt_binds_cells_scope_and_the_g15_closure(d1world, change, needle):
    base = _errs(d1world)
    m = copy.deepcopy(d1world["m"])
    change(d1world, m)
    _refused(d1world, _resign(d1world, m), needle, base)


@pytest.mark.parametrize("value", [0, 1, 1.0, "true", "false", None, [True]])
def test_reviewer_signed_must_be_a_real_boolean(world, value):
    m = copy.deepcopy(world["m"])
    m["reviewer"]["signed"] = value  # outside the digest: the synthetic signatures stay valid
    _refused(world, m, "reviewer.signed must be exactly true or false")


def test_reviewer_signed_false_or_undated_is_refused(world):
    m = copy.deepcopy(world["m"])
    m["reviewer"]["signed"] = False
    _refused(world, m, "the reviewer has not signed the manifest")
    m = copy.deepcopy(world["m"])
    m["reviewer"]["signed_at"] = "2026-11-31"
    _refused(world, m, "reviewer.signed_at is not a real")


def test_one_is_not_true_for_the_reviewer_in_the_entry_point():
    m = json.loads((M.REPO / M.MANIFEST_PATH).read_text(encoding="utf-8"))
    m["reviewer"].update(signed=1, manifest_sha256="0" * 64)
    m["hypotheses"][0]["status"] = "ADMITTED"
    errs = M.validate(m)
    assert any("reviewer.signed must be exactly true or false" in e for e in errs), errs
    assert any("not yet accepted by the reviewer" in e for e in errs), errs


def test_a_manifest_drift_is_refused_by_validate_even_without_admission():
    m = json.loads((M.REPO / M.MANIFEST_PATH).read_text(encoding="utf-8"))
    _hyp(m, "D2-H2")["n_floor"] = 250
    _hyp(m, "D1-F2")["blocking_gaps"] = ["G2", "G3"]
    m["hypotheses"] = [h for h in m["hypotheses"] if h["id"] != "D2-H3"]
    errs = M.validate(m)
    for needle in ("D2-H2: manifest n_floor 250 differs", "D1-F2: manifest blocking_gaps omits frozen blocking gap G15",
                   "D2-H3: supported hypothesis missing from the manifest"):
        assert any(needle in e for e in errs), (needle, errs)


def test_statement_binds_the_frozen_spec(world):
    body = json.loads(A.admission_statement(world["m"], HYP))
    assert body["frozen_spec_sha256"] == A.FROZEN_SPEC_SHA256


# --- chronology: G15 closure <= measurement <= reviewer <= admission <= today, and no relabelling ---

def _dates(m, hid, *, owner=None, reviewer=None, admission=None):
    if owner is not None:
        m["signed_at"] = owner
    if reviewer is not None:
        m["reviewer"]["signed_at"] = reviewer
    if admission is not None:
        _hyp(m, hid)["admission"] = {"signed_at": admission}
    return m


def _move_g15(w, m, closed_at):
    """Close G15 on ``closed_at`` with fresh typed evidence, and point the D1 receipt at that evidence."""
    _gap(m, "G15")["closure"]["closed_at"] = closed_at
    _rewrite_closure(w, m, "G15", closed_at=closed_at)
    return _rewrite_coverage(w, m, w["hid"], g15_closure_sha256=_gap(m, "G15")["closure"]["evidence"]["sha256"])


@pytest.mark.parametrize("fixture", ["world", "d1world"])
def test_the_fixture_chronology_is_the_allowed_order(request, fixture):
    w = request.getfixturevalue(fixture)
    m, h = w["m"], _hyp(w["m"], w["hid"])
    cov = json.loads((w["root"] / h["coverage"]["receipt"]["path"]).read_text())
    assert m["signed_at"] <= m["reviewer"]["signed_at"] <= h["admission"]["signed_at"] <= TODAY.isoformat()
    assert cov["measured_at"] <= m["reviewer"]["signed_at"]
    if fixture == "d1world":
        assert _gap(m, "G15")["closure"]["closed_at"] <= cov["measured_at"]


def test_equal_dates_and_an_earlier_g15_closure_verify(d1world):
    m = copy.deepcopy(d1world["m"])
    _move_g15(d1world, m, "2026-11-14")
    _rewrite_coverage(d1world, m, "D1-F1", measured_at="2026-11-14")
    assert _errs(d1world, _resign(d1world, m)) == []


@pytest.mark.parametrize("change, needle", [
    # signing dates: real, never in the future, in the signing order
    (lambda w, m: _dates(m, HYP, owner="2026-11-21", reviewer="2026-11-21", admission="2026-11-21"),
     "manifest signed_at is in the future"),
    (lambda w, m: _dates(m, HYP, reviewer="2026-11-21"), "reviewer.signed_at is in the future"),
    (lambda w, m: _dates(m, HYP, reviewer="2026-11-21", admission="2026-11-21"), "reviewer.signed_at is in the future"),
    (lambda w, m: _dates(m, HYP, reviewer="2026-11-13"), "reviewer.signed_at is before the owner's signature"),
    (lambda w, m: _dates(m, HYP, reviewer="2026-11-16"), "admission.signed_at is before the reviewer's signature"),
    (lambda w, m: _dates(m, HYP, reviewer="2026-11-17", admission="2026-11-16"),
     "admission.signed_at is before the reviewer's signature"),
    # the measurement precedes the reviewer's signature and the admission
    (lambda w, m: _rewrite_coverage(w, m, HYP, measured_at="2026-11-16"), "measured_at is after the admission date"),
    (lambda w, m: (_dates(m, HYP, admission="2026-11-17"), _rewrite_coverage(w, m, HYP, measured_at="2026-11-16")),
     "measured_at is after the reviewer's signature"),
    (lambda w, m: (_dates(m, HYP, reviewer="2026-11-17", admission="2026-11-17"),
                   _rewrite_coverage(w, m, HYP, measured_at="2026-11-18")), "measured_at is after the admission date"),
    (lambda w, m: _rewrite_coverage(w, m, HYP, measured_at="2026-11-15T00:00"), "measured_at is not a real"),
    (lambda w, m: _rewrite_coverage(w, m, HYP, measured_at=_DROP), "measured_at is not a real"),
])
def test_signing_and_measurement_dates_run_in_order(world, change, needle):
    base = _errs(world)
    m = copy.deepcopy(world["m"])
    change(world, m)
    _refused(world, _resign(world, m), needle, base)


@pytest.mark.parametrize("change, needle", [
    # the receipt names the right G15 closure hash, but was measured before G15 closed
    (lambda w, m: _rewrite_coverage(w, m, "D1-F1", measured_at="2026-11-14"), "measured_at is before the G15 closure"),
    (lambda w, m: (_move_g15(w, m, "2026-11-15"), _rewrite_coverage(w, m, "D1-F1", measured_at="2026-11-01")),
     "measured_at is before the G15 closure"),
    # the G15 closure itself after the admission, with consistent evidence and receipt hash
    (lambda w, m: (_move_g15(w, m, "2026-11-16")), "blocking gap G15 is not closed: closure.closed_at is after"),
    # ... and a closure that is not frozen before the look closes nothing for the receipt to cite
    (lambda w, m: (_move_g15(w, m, "2026-11-16")), "a judged historical D1 result requires the frozen G15 closure"),
    (lambda w, m: (_move_g15(w, m, "2026-11-21")), "closure.closed_at is in the future"),
])
def test_a_judged_d1_count_is_measured_after_g15_closed(d1world, change, needle):
    base = _errs(d1world)
    m = copy.deepcopy(d1world["m"])
    change(d1world, m)
    _refused(d1world, _resign(d1world, m), needle, base)


@pytest.fixture()
def spread(world):
    """``world`` re-signed with distinct dates: owner 11-14, measured 11-15, reviewer 11-16, admission 11-17."""
    m = _dates(copy.deepcopy(world["m"]), HYP, reviewer="2026-11-16", admission="2026-11-17")
    _resign(world, m)
    assert _errs(world, m) == []
    return {**world, "m": m}


@pytest.mark.parametrize("change", [
    # each relabel keeps the chronology valid, so only the signed statement can catch it
    lambda w, m: _dates(m, HYP, reviewer="2026-11-15"),
    lambda w, m: _dates(m, HYP, reviewer="2026-11-17"),
    lambda w, m: _dates(m, HYP, admission="2026-11-16"),
    lambda w, m: _dates(m, HYP, admission="2026-11-18"),
    lambda w, m: _dates(m, HYP, owner="2026-11-13"),
    lambda w, m: _rewrite_coverage(w, m, HYP, measured_at="2026-11-14"),
])
def test_relabelling_a_date_after_signing_is_refused(spread, change):
    m = copy.deepcopy(spread["m"])
    change(spread, m)
    m["reviewer"]["manifest_sha256"] = A.manifest_digest(m)  # the relabeller recomputes the unsigned field
    errs = _errs(spread, m)
    assert any("does not verify for 'reviewer'" in e for e in errs), errs
    assert any("does not verify for 'operator'" in e for e in errs), errs
    assert not any("signed_at is" in e or "measured_at is" in e for e in errs), errs


def test_relabelling_the_reviewer_date_without_the_digest_is_refused(spread):
    m = copy.deepcopy(spread["m"])
    m["reviewer"]["signed_at"] = "2026-11-15"  # manifest_sha256 left as signed
    _refused(spread, m, "reviewer.manifest_sha256 does not equal the manifest digest")


def test_relabelling_the_g15_date_is_refused(d1world):
    m = copy.deepcopy(d1world["m"])
    _gap(m, "G15")["closure"]["closed_at"] = "2026-11-14"  # manifest only: evidence still says 11-15
    m["reviewer"]["manifest_sha256"] = A.manifest_digest(m)
    errs = _errs(d1world, m)
    assert any("G15" in e and "closed_at differs from the manifest" in e for e in errs), errs
    assert any("does not verify" in e for e in errs), errs


def test_relabelling_a_receipt_date_on_disk_is_refused(world):
    path = world["root"] / _hyp(world["m"], HYP)["coverage"]["receipt"]["path"]
    base = _errs(world)
    body = json.loads(path.read_text())
    body["measured_at"] = "2026-11-14"
    path.write_text(json.dumps(body, indent=1, sort_keys=True) + "\n")
    _refused(world, world["m"], "does not hash", base)


def test_statement_binds_every_signing_date(world):
    body = json.loads(A.admission_statement(world["m"], HYP))
    assert body["owner_signed_at"] == world["m"]["signed_at"]
    assert body["reviewer_signed_at"] == world["m"]["reviewer"]["signed_at"]
    assert body["admission_signed_at"] == _hyp(world["m"], HYP)["admission"]["signed_at"]


# ---- identity: duplicate ids and near-spelled ADMITTED (adversarial review of 2ef0dbbf, 2026-11-25) ----

def test_duplicate_input_id_is_refused(world):
    m = copy.deepcopy(world["m"])
    good = next(i for i in m["inputs"] if i["id"] == INP)
    bad = copy.deepcopy(good)
    bad["missingness"] = "not measured"
    m["inputs"] = [bad] + m["inputs"]  # the later, good entry used to shadow the bad one
    _refused(world, _resign(world, m), f"input id {INP!r} appears more than once", base=[])


def test_duplicate_gap_id_is_refused(d1world):
    m = copy.deepcopy(d1world["m"])
    g = next(g for g in m["gaps"] if g["id"] == "G15")
    bad = copy.deepcopy(g)
    bad["closure"] = {"status": "open"}
    m["gaps"] = [bad] + m["gaps"]
    _refused(d1world, _resign(d1world, m), "gap id 'G15' appears more than once", base=[])


def test_duplicate_hypothesis_id_is_refused(world):
    m = copy.deepcopy(world["m"])
    twin = copy.deepcopy(next(h for h in m["hypotheses"] if h["id"] == HYP))
    twin["status"] = "CANDIDATE_UNREVIEWED"
    m["hypotheses"] = [twin] + m["hypotheses"]  # the signed statement used to be built from the twin
    _refused(world, _resign(world, m), f"hypothesis id {HYP!r} appears more than once", base=[])


@pytest.mark.parametrize("label", ["admitted", "Admitted", "ADMITTED ", " ADMITTED"])
def test_near_spelled_admitted_status_is_refused(label):
    m = json.loads((M.REPO / M.MANIFEST_PATH).read_text(encoding="utf-8"))
    next(h for h in m["hypotheses"] if h["id"] == HYP)["status"] = label
    ok, errs = _has(A.admission_errors(m, today=TODAY), "is not exactly 'ADMITTED'")
    assert ok, errs


def test_committed_manifest_has_unique_ids():
    assert A.identity_errors(json.loads((M.REPO / M.MANIFEST_PATH).read_text(encoding="utf-8"))) == []


# ---- structure: a malformed shape is a structured refusal, never an exception (Codex review of cbe2cf47) ----

@pytest.mark.parametrize("bad", [None, [], "manifest", 7])
def test_a_manifest_that_is_not_a_mapping_is_refused(bad):
    errs = A.admission_errors(bad, today=TODAY)
    assert errs and "not a mapping" in errs[0]


@pytest.mark.parametrize("key", ["inputs", "gaps", "hypotheses"])
@pytest.mark.parametrize("bad", ["x", {"id": "H"}, 5, True])
def test_a_collection_that_is_not_a_list_is_refused(key, bad):
    m = json.loads((M.REPO / M.MANIFEST_PATH).read_text(encoding="utf-8"))
    m[key] = bad
    errs = A.admission_errors(m, today=TODAY)
    assert errs == [f"{key} is {type(bad).__name__}, not a list"]


@pytest.mark.parametrize("key", ["inputs", "gaps", "hypotheses"])
@pytest.mark.parametrize("bad", [None, "x", 3, ["a"], [{"id": "H"}]])
def test_a_member_that_is_not_a_mapping_is_refused_before_any_deeper_step(key, bad):
    m = json.loads((M.REPO / M.MANIFEST_PATH).read_text(encoding="utf-8"))
    m[key] = [bad] + m[key]
    errs = A.admission_errors(m, today=TODAY)
    if isinstance(bad, dict):
        assert not _has(errs, "not a mapping")[0]  # a well-formed (if incomplete) member is not a shape error
    else:
        assert errs == [f"{key}[0] is {type(bad).__name__}, not a mapping"]


def test_a_malformed_member_beside_an_admitted_hypothesis_stops_the_walk(world):
    m = copy.deepcopy(world["m"])
    m["hypotheses"].append("not a mapping")
    errs = A.admission_errors(_resign(world, m), repo_root=world["root"], today=TODAY)
    assert errs == [f"hypotheses[{len(m['hypotheses']) - 1}] is str, not a mapping"]


def test_a_null_or_absent_collection_is_still_accepted_as_empty():
    for m in ({}, {"inputs": None, "gaps": None, "hypotheses": None}):
        assert A.admission_errors(m, today=TODAY) == []


def _leaf_paths(o, pre=()):
    if isinstance(o, dict):
        for k, v in o.items():
            yield pre + (k,)
            yield from _leaf_paths(v, pre + (k,))
    elif isinstance(o, list):
        for i, v in enumerate(o[:3]):
            yield pre + (i,)
            yield from _leaf_paths(v, pre + (i,))


def test_no_wrong_typed_field_makes_the_verifier_raise(world):
    """Overwrite every field of a fully signed manifest, one at a time, with each wrong type: the
    verifier must answer with refusals (or accept), never with an exception."""
    raised = []
    wrong = [None, "x", 7, [], {}, ["a"], True, 1.5]
    for path in _leaf_paths(world["m"]):
        for bad in wrong:
            m = copy.deepcopy(world["m"])
            target = m
            for k in path[:-1]:
                target = target[k]
            target[path[-1]] = bad
            try:
                A.admission_errors(m, repo_root=world["root"], today=TODAY)
            except Exception as e:  # noqa: BLE001 - any exception is the defect under test
                raised.append((path, bad, type(e).__name__))
    assert not raised, raised[:10]


@pytest.mark.parametrize("bad", [[], {}, ["a"], 7, None, True])
@pytest.mark.parametrize("key", ["inputs", "gaps", "hypotheses"])
def test_a_non_string_id_is_refused_without_an_exception(key, bad):
    m = json.loads((M.REPO / M.MANIFEST_PATH).read_text(encoding="utf-8"))
    m[key][0]["id"] = bad
    errs = A.admission_errors(m, today=TODAY)
    label = {"inputs": "input", "gaps": "gap", "hypotheses": "hypothesis"}[key]
    assert errs == [f"{label} entry without a string id"]


@pytest.mark.parametrize("hid", ["D2-H1a", "D2-H1b"])
def test_a_struck_hypothesis_cannot_be_admitted_or_supported(world, hid):
    """Revision 7 struck D2-H1a and D2-H1b: no frozen spec, so an ADMITTED row is refused as unsupported."""
    assert A.frozen_hypothesis(hid) is None
    m = copy.deepcopy(world["m"])
    h = _hyp(m, hid)
    assert h["status"].startswith("STRUCK")  # the committed manifest says so
    h["status"] = "ADMITTED"
    errs = _errs(world, m)
    assert any(f"{hid}: not a supported hypothesis" in e for e in errs), errs


def test_the_committed_manifest_marks_both_struck_cells_and_admits_nothing():
    m = json.loads((M.REPO / M.MANIFEST_PATH).read_text(encoding="utf-8"))
    st = {h["id"]: h["status"] for h in m["hypotheses"]}
    assert st["D2-H1a"].startswith("STRUCK") and st["D2-H1b"].startswith("STRUCK")
    assert not any(h.get("status") == "ADMITTED" for h in m["hypotheses"]) and m["revision"] == 7
