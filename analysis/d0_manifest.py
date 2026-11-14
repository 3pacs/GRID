"""D0 manifest checker (#1129, #1142): labels, the contract body pin and admission.

:func:`validate` is the single entry point. It checks the status labels, the owner-signed
contract body pin (:func:`contract_pin_errors`), and every ``ADMITTED`` label through
the authenticated verifier in ``analysis/d0_admission.py`` (gap G14). That verifier is
fail-closed: nothing is admitted until the reviewer has accepted it, and until the
reviewer and the operator have signed with keys registered in ``allowed_signers``.

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
from datetime import date
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


def _real_date(value: Any) -> bool:
    if not (isinstance(value, str) and _DATE_RE.fullmatch(value)):
        return False
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


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
        if not _real_date(manifest.get("signed_at")):
            errs.append("manifest signed_at is not a real YYYY-MM-DD date")
    elif manifest.get("contract_body_sha256") is not None or manifest.get("signed_at") is not None:
        errs.append("manifest carries a body hash or signed_at but research_owner.signed is not true")
    h = manifest.get("contract_body_sha256")
    if h is not None and not (isinstance(h, str) and _SHA256_RE.fullmatch(h)):
        errs.append("manifest contract_body_sha256 is not a lowercase sha256 hex digest")
    return errs


def validate(manifest: dict[str, Any], repo_root: Path = REPO) -> list[str]:
    """Every reason the manifest is not acceptable as committed (empty list = acceptable)."""
    from analysis import d0_admission  # imported here: d0_admission imports this module

    errs: list[str] = []
    allowed = set(manifest.get("admission_status_values") or [])
    if "CANDIDATE_UNREVIEWED" not in allowed:
        errs.append("admission_status_values must include CANDIDATE_UNREVIEWED")
    for inp in manifest.get("inputs") or []:
        status = inp.get("admission_status")
        if status not in allowed:
            errs.append(f"{inp.get('id')}: admission_status {status!r} not in admission_status_values")
    errs += contract_pin_errors(manifest, repo_root=repo_root)
    errs += d0_admission.admission_errors(manifest, repo_root=repo_root)
    return errs


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--manifest", type=Path, default=REPO / MANIFEST_PATH)
    args = ap.parse_args(argv)
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    errs = validate(manifest)
    for e in errs:
        print(e)
    print("OK" if not errs else f"{len(errs)} error(s)")
    return 1 if errs else 0


if __name__ == "__main__":
    sys.exit(main())
