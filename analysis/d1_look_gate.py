"""Admission-before-read gate for a D-track look (contract §8, gap G14; research only).

A look reads a holdout. The contract admits a hypothesis only after an authenticated admission
(``analysis.d0_admission``), so nothing may touch a holdout before that check has passed. This
module is the one door: ``run_look`` verifies the admission first and only then calls the reader
it was given. The reader is injected, so the gate holds no data access of its own and tests can
prove, with a recording reader, that a denied look makes **zero** reads.

Fail-closed rules, each pinned by a test:

* No override. ``run_look`` has no ``accepted`` argument; whether the verifier is accepted is the
  committed ``VERIFIER_ACCEPTED`` flag and nothing else.
* The named hypothesis must itself be an ``ADMITTED`` row. ``admission_errors`` returns nothing
  for a manifest with no admitted rows, so an empty result alone is not permission.
* Any error from the verifier, for any ADMITTED row in the manifest, denies the look.
* A verifier that raises denies the look. A reader that is not callable denies it. A reader that
  raises is reported as ``READ_FAILED`` and is not retried.
* One call makes at most one read.

This gate does not make a look, score anything or know any real holdout. It does not remember
earlier looks: spent-period accounting (contract §4) is not its job.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, Mapping

from analysis import d0_admission as A
from analysis import d0_manifest as M

GATE_VERSION = "d1-look-gate-v1"
DENIED = "DENIED"
READ = "READ"
READ_FAILED = "READ_FAILED"


@dataclass(frozen=True)
class LookResult:
    """What one call did. ``events`` is the ordered log; a denied look has no ``read_started``."""

    status: str
    hypothesis: Any
    errors: tuple[str, ...]
    events: tuple[tuple[str, str], ...]
    data: Any = None


def _denials(manifest: Any, hypothesis: Any, reader: Any, repo_root: Path, today: date | None) -> list[str]:
    if not isinstance(hypothesis, str) or not hypothesis:
        return [f"hypothesis id must be a non-empty string, not {type(hypothesis).__name__}"]
    if A.frozen_hypothesis(hypothesis) is None:
        return [f"{hypothesis}: not a supported hypothesis (no frozen admission spec)"]
    if not callable(reader):
        return ["reader is not callable"]
    try:
        errs = A.admission_errors(manifest, repo_root=repo_root, today=today)
    except Exception as exc:  # noqa: BLE001 - a verifier that cannot answer has not admitted anything
        return [f"the admission verifier raised {type(exc).__name__}: {exc}"]
    if errs:
        return list(errs)
    rows = [h for h in manifest.get("hypotheses") or [] if isinstance(h, dict) and h.get("id") == hypothesis]
    if len(rows) != 1 or rows[0].get("status") != "ADMITTED":
        return [f"{hypothesis}: not an ADMITTED row in the manifest (nothing admitted is not permission to read)"]
    return []


def run_look(manifest: Mapping[str, Any], hypothesis: str, reader: Callable[[dict[str, Any]], Any],
             *, repo_root: Path = M.REPO, today: date | None = None) -> LookResult:
    """Verify the admission of ``hypothesis``; only then call ``reader`` once with the frozen request."""
    errs = _denials(manifest, hypothesis, reader, repo_root, today)
    if errs:
        return LookResult(DENIED, hypothesis, tuple(errs), (("denied", errs[0]),))
    spec = A.frozen_hypothesis(hypothesis)
    request = {"hypothesis": hypothesis, "window": spec["window"], "manifest_sha256": A.manifest_digest(manifest),
               "gate": GATE_VERSION}
    events = [("admission_verified", hypothesis), ("read_started", hypothesis)]
    try:
        data = reader(request)
    except Exception as exc:  # noqa: BLE001 - reported, never retried
        return LookResult(READ_FAILED, hypothesis, (f"reader raised {type(exc).__name__}: {exc}",), tuple(events))
    return LookResult(READ, hypothesis, (), tuple(events), data)
