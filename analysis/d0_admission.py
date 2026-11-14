"""D0 admission verifier (gap G14; #1129, #1142).

The signed D0 contract (``docs/research/d0-research-contract-v0.md`` §8, §9 G14) lets a
hypothesis carry ``ADMITTED`` only when an authenticated verifier says so. This module is
that verifier. It is fail-closed in three independent ways:

1. :data:`VERIFIER_ACCEPTED` is ``False`` until the reviewer has accepted this code. While it
   is ``False``, every ``ADMITTED`` label is refused, whatever else holds.
2. Every admission needs SSH signatures (``ssh-keygen -Y verify``, namespace
   :data:`NAMESPACE`) from each principal in :data:`REQUIRED_PRINCIPALS`, checked against
   the committed ``allowed_signers`` file. That file ships with no keys. The reviewer and
   the operator each add their own key; this module never creates or registers one.
3. Every other prerequisite is checked against bytes on disk, not against strings that
   claim them.

What a hypothesis labelled ``ADMITTED`` must carry:
- ``n_floor``: a positive integer (never a bool).
- ``required_inputs``: a non-empty list of unique input ids. Every id must exist in the
  manifest (unknown dependencies are refused), and every input must itself pass
  :func:`input_errors`.
- ``coverage``: ``{"measured_n": int >= n_floor, "receipt": <file receipt>}``.
- ``blocking_gaps``: a list (possibly empty) of gap ids. Each must exist and be closed
  (its ``status`` starts with ``"closed"``).
- ``admission``: ``{"signed_at": "YYYY-MM-DD"}``, a real calendar date, not before the
  owner's signature and not in the future.

The manifest as a whole must be owner-signed with the pinned contract body
(``analysis/d0_manifest.contract_pin_errors``). If ``reviewer.signed`` is true,
``reviewer.manifest_sha256`` must equal :func:`manifest_digest`.

What each signature covers: :func:`admission_statement`. This is canonical JSON of the
contract id, the pinned body hash, the hypothesis id, its admission date and the manifest
digest. The digest is canonical JSON of the whole manifest except ``reviewer.signed``,
``reviewer.signed_at`` and ``reviewer.manifest_sha256``, so recording the result after
signing does not change what was signed (no self-referential field). Any other manifest
edit after signing invalidates every signature.

A file receipt is ``{"path": <repo-relative path>, "sha256": <hex>}``. The file must
exist inside the repository and hash to that value; a string is never a receipt.

This module reads only repository files and runs ``ssh-keygen``. It never reads the
database, a provider or an outcome.
"""

from __future__ import annotations

import argparse
import copy
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
from typing import Any

from analysis import d0_manifest as M

NAMESPACE = "grid-d0-admission"
SIGNATURE_DIR = Path("docs/research/d0-admission")
ALLOWED_SIGNERS = SIGNATURE_DIR / "allowed_signers"
REQUIRED_PRINCIPALS: tuple[str, ...] = ("reviewer", "operator")
DIGEST_EXCLUDED_REVIEWER_KEYS: tuple[str, ...] = ("signed", "signed_at", "manifest_sha256")
VISIBILITY_OK = {"qualified", "not_applicable"}
VISIBILITY_STATES = VISIBILITY_OK | {"applicable"}

#: Set to True only after the reviewer has accepted this verifier (contract §9 G14), as its
#: own reviewed commit on the operator's go. Until then nothing can be ADMITTED.
VERIFIER_ACCEPTED = False

_SHA256 = re.compile(r"[0-9a-f]{64}")
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


def _today() -> date:
    return datetime.now(timezone.utc).date()


def parse_date(value: Any) -> date | None:
    """A real calendar date written exactly ``YYYY-MM-DD``; anything else is None."""
    if not isinstance(value, str) or not _DATE.fullmatch(value):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def canonical(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def manifest_digest(manifest: dict[str, Any]) -> str:
    """sha256 of the manifest's canonical JSON, without the reviewer's result fields."""
    m = copy.deepcopy(manifest)
    reviewer = m.get("reviewer")
    if isinstance(reviewer, dict):
        for key in DIGEST_EXCLUDED_REVIEWER_KEYS:
            reviewer.pop(key, None)
    return hashlib.sha256(canonical(m)).hexdigest()


def admission_statement(manifest: dict[str, Any], hypothesis_id: str) -> bytes:
    """The exact bytes each principal signs to admit one hypothesis."""
    hyp = next((h for h in manifest.get("hypotheses") or [] if h.get("id") == hypothesis_id), None)
    if hyp is None:
        raise KeyError(f"no hypothesis {hypothesis_id!r}")
    admission = hyp.get("admission") if isinstance(hyp.get("admission"), dict) else {}
    return canonical({
        "contract": manifest.get("contract"),
        "contract_body_sha256": M.D0_CONTRACT_BODY_SHA256,
        "hypothesis": hypothesis_id,
        "admission_signed_at": admission.get("signed_at"),
        "manifest_sha256": manifest_digest(manifest),
        "namespace": NAMESPACE,
    }) + b"\n"


def file_receipt_errors(rec: Any, repo_root: Path, label: str) -> list[str]:
    if not isinstance(rec, dict):
        return [f"{label}: not a file receipt (needs {{path, sha256}}; a string is not a receipt)"]
    path, want = rec.get("path"), rec.get("sha256")
    if not (isinstance(want, str) and _SHA256.fullmatch(want)):
        return [f"{label}: sha256 is not a lowercase hex digest"]
    if not isinstance(path, str) or not path or Path(path).is_absolute():
        return [f"{label}: path must be repository-relative"]
    root = Path(repo_root).resolve()
    full = (root / path).resolve()
    if root not in full.parents:
        return [f"{label}: path escapes the repository"]
    if not full.is_file():
        return [f"{label}: file {path} does not exist"]
    if hashlib.sha256(full.read_bytes()).hexdigest() != want:
        return [f"{label}: file {path} does not hash to its recorded sha256"]
    return []


def query_receipt_errors(rec: Any, repo_root: Path, label: str) -> list[str]:
    """The input's query receipt: snapshot bytes = receipt_sha256, query text = query_sha256."""
    if not isinstance(rec, dict):
        return [f"{label}: no query receipt"]
    snapshot = rec.get("snapshot")
    query = rec.get("query")
    if query is None and isinstance(snapshot, str) and snapshot.endswith(".result.json"):
        query = snapshot[: -len(".result.json")] + ".sql"
    return (file_receipt_errors({"path": snapshot, "sha256": rec.get("receipt_sha256")}, repo_root,
                                f"{label} snapshot")
            + file_receipt_errors({"path": query, "sha256": rec.get("query_sha256")}, repo_root,
                                  f"{label} query"))


def input_errors(inp: dict[str, Any], repo_root: Path) -> list[str]:
    iid = inp.get("id", "<no id>")
    errs: list[str] = []
    if inp.get("admission_status") != "ADMITTED":
        errs.append(f"{iid}: required input is not ADMITTED")
    miss = inp.get("missingness")
    if not (isinstance(miss, str) and miss.strip()) or re.search(r"\bun-?measured\b|\bnot measured\b", miss, re.I):
        errs.append(f"{iid}: missingness not measured")
    clocks = inp.get("clocks") if isinstance(inp.get("clocks"), dict) else {}
    errs += file_receipt_errors(clocks.get("first_availability_receipt"), repo_root,
                                f"{iid}: first-availability receipt")
    vis = clocks.get("durable_visibility")
    if not isinstance(vis, dict):
        errs.append(f"{iid}: durable visibility must be {{state, reason, receipt}}; null or text is never an exemption")
    else:
        state = vis.get("state")
        if state not in VISIBILITY_STATES:
            errs.append(f"{iid}: durable visibility state {state!r} not in {sorted(VISIBILITY_STATES)}")
        elif state not in VISIBILITY_OK:
            errs.append(f"{iid}: durable visibility applies but is not qualified")
        if not (isinstance(vis.get("reason"), str) and vis["reason"].strip()):
            errs.append(f"{iid}: durable visibility has no reason")
        errs += file_receipt_errors(vis.get("receipt"), repo_root, f"{iid}: durable-visibility receipt")
    errs += query_receipt_errors(inp.get("receipts"), repo_root, f"{iid}: query receipt")
    return errs


def _owner_errors(manifest: dict[str, Any], repo_root: Path) -> list[str]:
    errs = list(M.contract_pin_errors(manifest, repo_root=repo_root))
    if (manifest.get("research_owner") or {}).get("signed") is not True:
        errs.append("research owner has not signed (research_owner.signed must be exactly true)")
    if parse_date(manifest.get("signed_at")) is None:
        errs.append("manifest signed_at is not a real YYYY-MM-DD date")
    reviewer = manifest.get("reviewer") if isinstance(manifest.get("reviewer"), dict) else {}
    if reviewer.get("signed") is True and reviewer.get("manifest_sha256") != manifest_digest(manifest):
        errs.append("reviewer.manifest_sha256 does not equal the manifest digest")
    elif reviewer.get("signed") not in (True, False):
        errs.append("reviewer.signed must be exactly true or false")
    return errs


def verify_signature(statement: bytes, sig: Path, principal: str, signers: Path) -> str | None:
    """None when ``sig`` is a valid signature of ``statement`` by ``principal``; else why not."""
    exe = shutil.which("ssh-keygen")
    if exe is None:
        return "ssh-keygen not found; signatures cannot be verified"
    if not sig.is_file():
        return f"signature {sig.name} missing"
    if not signers.is_file():
        return "allowed_signers file missing"
    listed = [ln.split()[0] for ln in signers.read_text(encoding="utf-8").splitlines()
              if ln.strip() and not ln.lstrip().startswith("#")]
    if principal not in listed:
        return f"principal {principal!r} has no key in allowed_signers"
    try:
        run = subprocess.run([exe, "-Y", "verify", "-f", str(signers), "-I", principal, "-n", NAMESPACE,
                              "-s", str(sig)], input=statement, capture_output=True, timeout=20, check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        return f"ssh-keygen verify failed to run: {exc}"
    return None if run.returncode == 0 else f"signature {sig.name} does not verify for {principal!r}"


def hypothesis_errors(hyp: dict[str, Any], manifest: dict[str, Any], repo_root: Path,
                      today: date | None = None) -> list[str]:
    hid = hyp.get("id", "<no id>")
    today = today or _today()
    errs = [f"{hid}: {e}" for e in _owner_errors(manifest, repo_root)]
    floor = hyp.get("n_floor")
    if not (_is_int(floor) and floor > 0):
        errs.append(f"{hid}: n_floor must be a positive integer")
    inputs = {i.get("id"): i for i in manifest.get("inputs") or []}
    req = hyp.get("required_inputs")
    if not (isinstance(req, list) and req and all(isinstance(r, str) for r in req) and len(set(req)) == len(req)):
        errs.append(f"{hid}: required_inputs must be a non-empty list of unique input ids")
    else:
        for rid in req:
            if rid not in inputs:
                errs.append(f"{hid}: unknown dependency {rid!r}")
            else:
                errs += [f"{hid}: {e}" for e in input_errors(inputs[rid], repo_root)]
    cov = hyp.get("coverage") if isinstance(hyp.get("coverage"), dict) else None
    if cov is None:
        errs.append(f"{hid}: coverage not measured")
    else:
        n = cov.get("measured_n")
        if not _is_int(n):
            errs.append(f"{hid}: coverage measured_n must be an integer")
        elif _is_int(floor) and n < floor:
            errs.append(f"{hid}: coverage {n} is below the sample floor {floor}")
        errs += file_receipt_errors(cov.get("receipt"), repo_root, f"{hid}: coverage receipt")
    gaps = {g.get("id"): g for g in manifest.get("gaps") or []}
    blocking = hyp.get("blocking_gaps")
    if not (isinstance(blocking, list) and all(isinstance(g, str) for g in blocking)):
        errs.append(f"{hid}: blocking_gaps must be a list of gap ids (empty if none)")
    else:
        for gid in blocking:
            if gid not in gaps:
                errs.append(f"{hid}: unknown blocking gap {gid!r}")
            elif not str(gaps[gid].get("status") or "").lower().startswith("closed"):
                errs.append(f"{hid}: blocking gap {gid} is not closed")
    adm = hyp.get("admission") if isinstance(hyp.get("admission"), dict) else {}
    when, owner_when = parse_date(adm.get("signed_at")), parse_date(manifest.get("signed_at"))
    if when is None:
        errs.append(f"{hid}: admission.signed_at is not a real YYYY-MM-DD date")
    elif when > today:
        errs.append(f"{hid}: admission.signed_at is in the future")
    elif owner_when is not None and when < owner_when:
        errs.append(f"{hid}: admission.signed_at is before the owner's signature")
    statement = admission_statement(manifest, hid)
    root = Path(repo_root)
    for principal in REQUIRED_PRINCIPALS:
        why = verify_signature(statement, root / SIGNATURE_DIR / f"{hid}.{principal}.sig", principal,
                               root / ALLOWED_SIGNERS)
        if why:
            errs.append(f"{hid}: {why}")
    return errs


def admission_errors(manifest: dict[str, Any], repo_root: Path = M.REPO, today: date | None = None,
                     accepted: bool | None = None) -> list[str]:
    """Why any ADMITTED label in the manifest is not authenticated (empty = none, or all verified)."""
    accepted = VERIFIER_ACCEPTED if accepted is None else accepted
    errs: list[str] = []
    hyps = [h for h in manifest.get("hypotheses") or [] if h.get("status") == "ADMITTED"]
    covered: set[str] = set()
    for h in hyps:
        hid = h.get("id")
        if accepted is not True:
            errs.append(f"{hid}: ADMITTED refused: the G14 verifier is not yet accepted by the reviewer")
        errs += hypothesis_errors(h, manifest, repo_root, today)
        if isinstance(h.get("required_inputs"), list):
            covered.update(r for r in h["required_inputs"] if isinstance(r, str))
    for inp in manifest.get("inputs") or []:
        if inp.get("admission_status") == "ADMITTED":
            iid = inp.get("id")
            if accepted is not True:
                errs.append(f"{iid}: ADMITTED refused: the G14 verifier is not yet accepted by the reviewer")
            if iid not in covered:
                errs.append(f"{iid}: input ADMITTED without an ADMITTED hypothesis that requires it")
                errs += input_errors(inp, repo_root)
    return errs


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--manifest", type=Path, default=M.REPO / M.MANIFEST_PATH)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("digest", help="print the manifest digest that signatures bind")
    st = sub.add_parser("statement", help="print the exact bytes to sign for one hypothesis")
    st.add_argument("hypothesis")
    sub.add_parser("verify", help="verify every ADMITTED label (same check as analysis.d0_manifest)")
    args = ap.parse_args(argv)
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    if args.cmd == "digest":
        print(manifest_digest(manifest))
        return 0
    if args.cmd == "statement":
        sys.stdout.buffer.write(admission_statement(manifest, args.hypothesis))
        return 0
    errs = admission_errors(manifest)
    for e in errs:
        print(e)
    print("OK" if not errs else f"{len(errs)} error(s)")
    return 1 if errs else 0


if __name__ == "__main__":
    sys.exit(main())
