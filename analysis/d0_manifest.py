"""D0 provisional label checker (#1129, #1142), not an admission authority.

All ADMITTED labels fail closed until an authenticated verifier exists. Passing
this checker validates draft labels only, not signatures, receipts, dependencies
or coverage. The prerequisite checks below provide diagnostics, not admission.

The D0 research contract (``docs/research/d0-research-contract-v0.md`` §8) lets an
input carry ``ADMITTED`` only when all of these hold:
- the contract is signed and its body hash pinned;
- the reviewer has signed the manifest;
- the input has measured missingness;
- it has a first-availability receipt;
- its durable-visibility clock is not ``UNPROVEN``, where that clock applies;
- it has a file or query receipt.

Until the reviewer signs the manifest and the verifier exists, nothing may be
``ADMITTED``. An input label is never a hypothesis admission.

The contract body (LF bytes strictly between the ``D0-CONTRACT-BODY`` markers) was
signed by the research owner on 2026-11-14 and is pinned in
:data:`D0_CONTRACT_BODY_SHA256`, as ``analysis/gex_intraday_prereg.py`` pins its body.
Any body edit fails :func:`contract_pin_errors`. This module only reads the contract
and the manifest JSON. It never reads the database, a provider or an outcome.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
import sys
from typing import Any

REPO = Path(__file__).resolve().parents[1]
MANIFEST_PATH = Path("docs/research/d0-coverage-manifest-v0.json")
CONTRACT_PATH = Path("docs/research/d0-research-contract-v0.md")
BODY_START = "<!-- D0-CONTRACT-BODY-BEGIN -->"
BODY_END = "<!-- D0-CONTRACT-BODY-END -->"

#: sha256 of the owner-signed contract body (revision 6, signed 2026-11-14). Any edit to the
#: body is a new contract version and needs a new pin; it is never edited to match a changed body.
D0_CONTRACT_BODY_SHA256 = "7643c1e096d8c75567598d16ebf077a246e862c0c922a8f674e82574e2681285"

_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


class ContractError(ValueError):
    """The contract file has no single, ordered pair of body markers."""


def contract_body(text: str) -> str:
    """The text strictly between the body markers, after CRLF → LF."""
    text = text.replace("\r\n", "\n")
    if text.count(BODY_START) != 1 or text.count(BODY_END) != 1:
        raise ContractError("contract needs exactly one body start and one body end marker")
    start, end = text.index(BODY_START) + len(BODY_START), text.index(BODY_END)
    if end <= start:
        raise ContractError("contract body markers are out of order")
    return text[start:end]


def body_sha256(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def contract_pin_errors(manifest: dict[str, Any], repo_root: Path = REPO,
                        pinned: str = D0_CONTRACT_BODY_SHA256) -> list[str]:
    """Why the committed contract body or the manifest's signature fields disagree with the pin."""
    errs: list[str] = []
    try:
        actual = body_sha256(contract_body((Path(repo_root) / CONTRACT_PATH).read_text(encoding="utf-8")))
    except (OSError, ContractError) as exc:
        return [f"contract body unreadable: {exc}"]
    if actual != pinned:
        errs.append(f"contract body sha256 {actual} != pinned {pinned} (a body edit is a new version)")
    owner = manifest.get("research_owner") or {}
    if owner.get("signed") is True:
        if manifest.get("contract_body_sha256") != pinned:
            errs.append("manifest contract_body_sha256 does not equal the pinned body hash")
        if not (isinstance(manifest.get("signed_at"), str) and _DATE_RE.fullmatch(manifest["signed_at"])):
            errs.append("manifest signed_at is not a YYYY-MM-DD date")
    elif manifest.get("contract_body_sha256") is not None or manifest.get("signed_at") is not None:
        errs.append("manifest carries a body hash or signed_at but research_owner.signed is not true")
    h = manifest.get("contract_body_sha256")
    if h is not None and not (isinstance(h, str) and _SHA256_RE.fullmatch(h)):
        errs.append("manifest contract_body_sha256 is not a lowercase sha256 hex digest")
    return errs


def _contract_signed(m: dict[str, Any]) -> bool:
    return bool(m.get("signed_at")) and bool(m.get("contract_body_sha256")) and bool(
        (m.get("research_owner") or {}).get("signed")
    )


def _reviewer_signed(m: dict[str, Any]) -> bool:
    r = m.get("reviewer") or {}
    return bool(r.get("signed")) and bool(r.get("manifest_sha256"))


def input_admission_errors(inp: dict[str, Any], manifest: dict[str, Any]) -> list[str]:
    """Why this input may not be ADMITTED (empty list = prerequisites met)."""
    errs: list[str] = []
    iid = inp.get("id", "<no id>")
    errs.append(f"{iid}: ADMITTED unavailable: authenticated verifier not implemented")
    if not _contract_signed(manifest):
        errs.append(f"{iid}: contract not signed / body hash not pinned")
    if not _reviewer_signed(manifest):
        errs.append(f"{iid}: reviewer has not signed the manifest")
    if not str(inp.get("missingness") or "").strip():
        errs.append(f"{iid}: missingness not measured")
    clocks = inp.get("clocks") or {}
    if not clocks.get("first_availability_receipt"):
        errs.append(f"{iid}: no first-availability receipt (an assertion is not a receipt)")
    vis = clocks.get("durable_visibility")
    if isinstance(vis, str) and vis.upper().startswith("UNPROVEN"):
        errs.append(f"{iid}: durable visibility UNPROVEN")
    rec = inp.get("receipts") or {}
    if not (rec.get("query_sha256") or rec.get("receipt_sha256")):
        errs.append(f"{iid}: no file or query receipt")
    return errs


def validate(manifest: dict[str, Any]) -> list[str]:
    errs: list[str] = []
    allowed = set(manifest.get("admission_status_values") or [])
    if "CANDIDATE_UNREVIEWED" not in allowed:
        errs.append("admission_status_values must include CANDIDATE_UNREVIEWED")
    for inp in manifest.get("inputs") or []:
        status = inp.get("admission_status")
        if status not in allowed:
            errs.append(f"{inp.get('id')}: admission_status {status!r} not in admission_status_values")
        if status == "ADMITTED":
            errs.extend(input_admission_errors(inp, manifest))
    signed = _contract_signed(manifest) and _reviewer_signed(manifest)
    for h in manifest.get("hypotheses") or []:
        if h.get("status") == "ADMITTED":
            errs.append(f"{h.get('id')}: ADMITTED unavailable: authenticated verifier not implemented")
        if h.get("status") == "ADMITTED" and not signed:
            errs.append(f"{h.get('id')}: hypothesis ADMITTED while the contract or manifest is unsigned")
    return errs


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--manifest", type=Path, default=REPO / MANIFEST_PATH)
    args = ap.parse_args(argv)
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    errs = validate(manifest) + contract_pin_errors(manifest)
    for e in errs:
        print(e)
    print("OK" if not errs else f"{len(errs)} error(s)")
    return 1 if errs else 0


if __name__ == "__main__":
    sys.exit(main())
