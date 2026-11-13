"""D0 coverage manifest validator (#1129, #1142): refuse ``ADMITTED`` without its prerequisites.

The D0 research contract (``docs/research/d0-research-contract-v0.md`` §8) lets an
input carry ``ADMITTED`` only when all of these hold:
- the contract is signed and its body hash pinned;
- the reviewer has signed the manifest;
- the input has measured missingness;
- it has a first-availability receipt;
- its durable-visibility clock is not ``UNPROVEN``, where that clock applies;
- it has a file or query receipt.

While the contract is unsigned, nothing may be ``ADMITTED``. An input label is never a
hypothesis admission. This module only reads the manifest JSON. It never reads the
database, a provider or an outcome.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

REPO = Path(__file__).resolve().parents[1]
MANIFEST_PATH = Path("docs/research/d0-coverage-manifest-v0.json")


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
        if h.get("status") == "ADMITTED" and not signed:
            errs.append(f"{h.get('id')}: hypothesis ADMITTED while the contract or manifest is unsigned")
    return errs


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--manifest", type=Path, default=REPO / MANIFEST_PATH)
    args = ap.parse_args(argv)
    errs = validate(json.loads(args.manifest.read_text(encoding="utf-8")))
    for e in errs:
        print(e)
    print("OK" if not errs else f"{len(errs)} error(s)")
    return 1 if errs else 0


if __name__ == "__main__":
    sys.exit(main())
