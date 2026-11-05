"""Off-host witness for the E2 ledger: the VS1 vault-witness pattern, for E2's anchor file.

* :func:`export_anchors` appends to ``<vault_worktree>/<WITNESS_DIR>/<ledger>.anchors.jsonl``
  the anchor lines it lacks (refusing if that file witnesses another ledger). It
  writes the working-tree file only; committing and pushing it to ``main`` of the
  pinned vault remote is the operator's (or the vault mirror cron's) step, exactly
  as for VS1 (``analysis.panel_insider_density.export_anchors``).
* :func:`check_offhost` fetches the pinned branch into a private ref, walks every
  committed version of the witness file (``git log --first-parent --follow``) and
  requires: the path never moved or disappeared, every version is a strict
  line-prefix extension of the one before (append-only), and the tip equals the
  last version walked. It returns the committed content, which
  ``Ledger.verify(external_anchors=...)`` then checks against the ledger -- a
  rewritten, truncated or recomputed ledger fails against anchors held off-host.
"""

from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path

from evals.e2.chain import Ledger, raw_lines

WITNESS_REMOTE_URL = "https://github.com/3pacs/obsidian-vault.git"
WITNESS_BRANCH = "main"
WITNESS_DIR = "05-GRID/Paper-Log/e2"
WITNESS_REF = "refs/e2-witness/main"


class WitnessError(PermissionError):
    """The off-host witness is missing, rewritten, or witnesses another ledger."""


def witness_path(version: str) -> str:
    return f"{WITNESS_DIR}/e2_scoreboard_{version}.anchors.jsonl"


def export_anchors(board_dir: Path, version: str, vault_worktree: Path) -> list[str]:
    """Append the ledger's missing anchor lines to the witness file in a vault worktree."""
    ledger = Ledger(board_dir, version)
    check = ledger.verify()
    if not check["ok"]:
        raise WitnessError(f"refusing to export anchors of a broken ledger: {check['detail']}")
    local = list(raw_lines(ledger.anchor_path))
    path = Path(vault_worktree) / witness_path(version)
    existing = list(raw_lines(path))
    if existing != local[: len(existing)]:
        raise WitnessError("the witness file in the vault witnesses another E2 ledger; not appending")
    new = local[len(existing):]
    if new:
        path.parent.mkdir(parents=True, exist_ok=True)
        tail = path.read_bytes() if path.exists() else b""
        with open(path, "ab") as stream:
            if tail and not tail.endswith(b"\n"):
                stream.write(b"\n")
            for line in new:
                stream.write(line + b"\n")
    return [line.decode("utf-8") for line in new]


def _git(repo: Path, *argv: str, binary: bool = False):
    result = subprocess.run(["git", "-C", str(repo), *argv], capture_output=True, check=False, timeout=120)
    if result.returncode != 0:
        raise WitnessError(f"git {' '.join(argv[:2])} failed: {result.stderr.decode('utf-8', 'replace').strip()}")
    return result.stdout if binary else result.stdout.decode("utf-8")


def check_offhost(vault_repo: Path, version: str, *, remote_url: str | None = None) -> dict:
    """Fetch the pinned branch and return the committed witness file if its history is append-only."""
    url = WITNESS_REMOTE_URL if remote_url is None else remote_url
    rel = witness_path(version)
    repo = Path(vault_repo)
    _git(repo, "rev-parse", "--git-dir")
    _git(repo, "fetch", "--quiet", "--no-tags", "--no-write-fetch-head", url,
         f"+refs/heads/{WITNESS_BRANCH}:{WITNESS_REF}")
    tip = _git(repo, "rev-parse", "--verify", f"{WITNESS_REF}^{{commit}}").strip()
    log = _git(repo, "log", "--first-parent", "-m", "--follow", "--name-status", "--format=%x00%H",
               WITNESS_REF, "--", rel)
    entries = []
    for chunk in log.split("\x00")[1:]:
        head, *rest = chunk.strip("\n").split("\n")
        entries.append((head.strip(), [line.split("\t") for line in rest if line.strip()]))
    if not entries:
        raise WitnessError(f"{rel} is not on {WITNESS_BRANCH} of {url}")
    versions, previous = [], None
    for commit, changes in reversed(entries):
        for change in changes:
            status, paths = change[0], change[1:]
            if status.startswith(("R", "C")) or any(p != rel for p in paths):
                raise WitnessError(f"{commit[:12]}: the witness file came from another path ({paths})")
            if status.startswith("D"):
                raise WitnessError(f"{commit[:12]}: the witness file was deleted (not append-only)")
        content = _git(repo, "show", f"{commit}:{rel}", binary=True).replace(b"\r\n", b"\n")
        lines = [line for line in content.split(b"\n") if line]
        if previous is not None and not (len(lines) > len(previous) and lines[: len(previous)] == previous):
            raise WitnessError(f"{commit[:12]}: the witness file is not a strict line-prefix extension of its "
                               "previous version (truncated, edited or unchanged): not append-only")
        covered = json.loads(lines[-1]).get("records", 0) if lines else 0
        versions.append({"commit": commit, "lines": len(lines), "covered_records": covered})
        previous = lines
    at_tip = _git(repo, "show", f"{tip}:{rel}", binary=True).replace(b"\r\n", b"\n")
    if [line for line in at_tip.split(b"\n") if line] != previous:
        raise WitnessError("the witness file at the fetched tip is not its last walked version")
    return {"remote_url": url, "branch": WITNESS_BRANCH, "path": rel, "tip": tip, "versions": versions,
            "content": b"\n".join(previous or []) + b"\n"}


def verify_against_offhost(board_dir: Path, version: str, vault_repo: Path, *, remote_url: str | None = None) -> dict:
    """Ledger chain + local anchors + the committed off-host anchors, all consistent."""
    witness = check_offhost(vault_repo, version, remote_url=remote_url)
    with tempfile.TemporaryDirectory() as scratch:
        path = Path(scratch) / "witness.anchors.jsonl"
        path.write_bytes(witness["content"])
        check = Ledger(board_dir, version).verify(external_anchors=path)
    if not check["ok"]:
        raise WitnessError(f"off-host anchors do not witness this ledger: {check['detail']}")
    covered = witness["versions"][-1]["covered_records"]
    return {"ledger": check, "witnessed_records": covered, "tip": witness["tip"],
            "unwitnessed_records": check["records"] - covered}
