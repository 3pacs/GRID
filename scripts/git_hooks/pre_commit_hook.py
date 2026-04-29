#!/usr/bin/env python3
"""pre_commit_hook.py -- Enforce pre_create_check coverage on new .py files.

Rejects commits that add a new .py file whose filename-derived concept is
already covered by an existing module, UNLESS the commit message contains a
`GREP_PROOF:` line explaining what was checked and why it's not a duplicate.

Flow
----
1. Ask git for newly-added .py files (`--diff-filter=A`).
2. For each one, derive one or more candidate concepts from the filename:
     intelligence/supply_chokepoints.py  ->  ["chokepoint", "supply_chokepoint",
                                               "supply", "chokepoints", ...]
3. Run `scripts/pre_create_check.py <concept> --json --max-files 5`.
   - Exit 0 = coverage exists (file list non-empty). BLOCK unless GREP_PROOF.
   - Exit 1 = no coverage. Allow.
4. Read the commit message from $1 (Git passes the message file as argv[1] to
   `prepare-commit-msg` and `commit-msg`, but for the `pre-commit` hook there
   is no message file, so fall back to `.git/COMMIT_EDITMSG`).
5. Print a helpful block message listing the overlapping modules.

Exit codes
----------
    0 -- allow the commit
    1 -- block the commit (overlap detected, no GREP_PROOF)
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #

# Filename stems we never probe -- they are infrastructure, not concepts.
SKIP_STEM_PREFIXES = (
    "test_",
    "_test",
    "conftest",
    "__init__",
    "__main__",
)

# Filename stems that carry no concept -- skip the probe entirely.
SKIP_STEM_EXACT = {
    "utils",
    "helpers",
    "common",
    "base",
    "types",
    "constants",
    "config",
    "main",
    "app",
    "cli",
    "runner",
}

# Tokens to strip from filenames before turning them into concepts.
# These are either infrastructure suffixes (service, manager) or English
# adjectives that would match thousands of files if probed alone (unique,
# novel, new).
NOISE_TOKENS = {
    "service",
    "module",
    "manager",
    "handler",
    "runner",
    "worker",
    "engine",
    "util",
    "utils",
    "helper",
    "helpers",
    "core",
    "base",
    "main",
    "app",
    "new",
    "old",
    "v1",
    "v2",
    "v3",
    "v4",
    "v5",
    "tmp",
    "temp",
    # Generic adjectives / nouns too broad to probe meaningfully.
    "unique",
    "novel",
    "totally",
    "thing",
    "widget",
    "simple",
    "fast",
    "slow",
    "big",
    "small",
    "generic",
    "custom",
    "basic",
}

GREP_PROOF_RE = re.compile(r"^\s*GREP_PROOF:", re.MULTILINE | re.IGNORECASE)

MAX_FILES_PER_PROBE = 5


# --------------------------------------------------------------------------- #
# Data
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class OverlapHit:
    """A single overlap finding: a concept and the files that cover it."""

    path: str
    concept: str
    overlapping_files: tuple[tuple[str, int], ...]  # (path, line_count)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def run(cmd: list[str], cwd: Path | None = None) -> tuple[int, str, str]:
    """Run a subprocess and return (returncode, stdout, stderr)."""
    proc = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.returncode, proc.stdout, proc.stderr


def find_repo_root(start: Path) -> Path:
    """Walk up from `start` until we find a .git directory."""
    cur = start.resolve()
    while cur != cur.parent:
        if (cur / ".git").exists():
            return cur
        cur = cur.parent
    return start.resolve()


def get_added_py_files(repo_root: Path) -> list[str]:
    """Return repo-relative paths of staged, newly-added .py files."""
    rc, out, _err = run(
        ["git", "diff", "--cached", "--name-only", "--diff-filter=A"],
        cwd=repo_root,
    )
    if rc != 0:
        return []
    files: list[str] = []
    for line in out.splitlines():
        line = line.strip()
        if not line or not line.endswith(".py"):
            continue
        files.append(line)
    return files


def read_commit_message(repo_root: Path) -> str:
    """Read the pending commit message. Pre-commit hooks have no argv message
    file, so we look at .git/COMMIT_EDITMSG, which git populates before the
    hook runs when using `git commit -m`, and also when the editor closes."""
    # Allow tests / callers to override via env.
    override = os.environ.get("GRID_PRECOMMIT_MSG_FILE")
    if override and Path(override).exists():
        try:
            return Path(override).read_text(encoding="utf-8", errors="replace")
        except OSError:
            return ""
    candidate = repo_root / ".git" / "COMMIT_EDITMSG"
    if candidate.exists():
        try:
            return candidate.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return ""
    return ""


def has_grep_proof(message: str) -> bool:
    return bool(GREP_PROOF_RE.search(message or ""))


def candidate_concepts(rel_path: str) -> list[str]:
    """Derive a small, ranked list of candidate concepts from a filename.

    intelligence/supply_chokepoints.py ->
        ["supply_chokepoint", "chokepoint", "supply"]
    """
    stem = Path(rel_path).stem.lower()
    if stem.startswith(SKIP_STEM_PREFIXES) or stem in SKIP_STEM_EXACT:
        return []

    # Split on underscores and hyphens.
    parts = [p for p in re.split(r"[_\-]+", stem) if p]
    parts = [p for p in parts if p not in NOISE_TOKENS and len(p) > 2]
    if not parts:
        return []

    concepts: list[str] = []
    seen: set[str] = set()

    # 1. Full joined stem (most specific).
    joined = "_".join(parts)
    if joined and joined not in seen:
        concepts.append(joined)
        seen.add(joined)

    # 2. Each individual token, singularized (drop trailing 's' if >3 chars).
    for p in parts:
        singular = p[:-1] if len(p) > 3 and p.endswith("s") else p
        if singular not in seen:
            concepts.append(singular)
            seen.add(singular)

    return concepts


def probe_concept(
    repo_root: Path, concept: str
) -> tuple[bool, list[tuple[str, int]]]:
    """Run pre_create_check.py for `concept`.

    Returns (coverage_exists, [(path, line_count), ...]).
    """
    checker = repo_root / "scripts" / "pre_create_check.py"
    if not checker.exists():
        # Checker missing -- fail open rather than block every commit.
        return False, []
    cmd = [
        sys.executable,
        str(checker),
        concept,
        "--json",
        "--max-files",
        str(MAX_FILES_PER_PROBE),
    ]
    rc, stdout, _stderr = run(cmd, cwd=repo_root)
    # rc == 0 means coverage exists, rc == 1 means none. Other codes = error.
    if rc not in (0, 1):
        return False, []
    try:
        payload = json.loads(stdout)
    except (ValueError, json.JSONDecodeError):
        return False, []
    files = payload.get("files") or []
    overlap = [
        (f.get("path", "?"), int(f.get("line_count") or 0)) for f in files
    ]
    return bool(payload.get("coverage_exists")) and bool(overlap), overlap


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #


def evaluate_file(
    repo_root: Path, rel_path: str
) -> OverlapHit | None:
    """Return an OverlapHit if the new file overlaps existing coverage."""
    for concept in candidate_concepts(rel_path):
        exists, overlap = probe_concept(repo_root, concept)
        if not exists:
            continue
        # Filter out the new file itself (it won't exist yet on disk for
        # pre_create_check, but be defensive).
        overlap = [(p, ln) for (p, ln) in overlap if p != rel_path]
        if not overlap:
            continue
        return OverlapHit(
            path=rel_path,
            concept=concept,
            overlapping_files=tuple(overlap),
        )
    return None


def format_block_message(hits: Iterable[OverlapHit]) -> str:
    lines: list[str] = []
    lines.append("[pre-commit] BLOCKED: new .py file overlaps existing modules")
    lines.append("")
    for hit in hits:
        lines.append(f"{hit.path} appears to overlap (concept: {hit.concept!r}):")
        for path, line_count in hit.overlapping_files:
            lines.append(f"  - {path} ({line_count} LOC)")
        lines.append("")
    lines.append(
        "Fix: extend an existing module, OR add this line to your commit message:"
    )
    lines.append(
        "  GREP_PROOF: I checked X, Y, Z and they don't cover this concept because ..."
    )
    lines.append("")
    lines.append("Then re-commit.")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    repo_root = find_repo_root(Path.cwd())
    added = get_added_py_files(repo_root)
    if not added:
        return 0

    message = read_commit_message(repo_root)
    grep_proof = has_grep_proof(message)

    hits: list[OverlapHit] = []
    passes: list[str] = []
    for rel_path in added:
        hit = evaluate_file(repo_root, rel_path)
        if hit is None:
            passes.append(rel_path)
        else:
            hits.append(hit)

    for p in passes:
        print(f"[pre-commit] coverage OK for {p}")

    if not hits:
        return 0

    if grep_proof:
        # GREP_PROOF present -- allow, but still surface the overlaps.
        print(
            "[pre-commit] GREP_PROOF present, allowing despite "
            f"{len(hits)} overlap(s):"
        )
        for hit in hits:
            files_str = ", ".join(p for p, _ in hit.overlapping_files)
            print(f"  {hit.path} (concept={hit.concept}) overlaps: {files_str}")
        return 0

    sys.stderr.write(format_block_message(hits) + "\n")
    return 1


if __name__ == "__main__":
    sys.exit(main())
