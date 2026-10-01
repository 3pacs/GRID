"""Released-suite registry and freeze guard for every package under ``evals/``.

``evals/RELEASED.json`` is the append-only record of what was released::

    {"schema": 1, "entries": [<entry>, ...]}   # entries[i]["seq"] == i

Entry kinds:

* ``suite``  -- ``suite``, ``version``, ``path`` (e.g. ``evals/e0``),
  ``manifest_sha256`` (LF sha256 of ``<path>/MANIFEST.sha256``),
  ``released_in``, ``approved_by``, ``note``.
* ``guards`` -- ``version``, ``files`` ({repo path: LF sha256}): the guard's
  own code, tests and workflow. Only the latest ``guards`` entry is enforced.

``check`` compares a BASE tree (trusted: the PR base or the previous main
commit) with a HEAD tree (the proposed change) and enforces R1-R9 (see
``evals/README.md``). CI runs the BASE revision's copy of this file
(``.github/workflows/evals-freeze.yml``); the head tree is only read and
hashed, never imported or executed.

This module is deliberately stdlib-only and imports nothing from GRID or from
any other ``evals`` package, so it runs as a plain script under ``python -I``::

    python evals/released.py check --base-dir BASE --head-dir HEAD
    python evals/released.py lf-sha256 PATH [PATH ...]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
from pathlib import Path

SCHEMA = 1
REGISTRY = "evals/RELEASED.json"
MANIFEST_NAME = "MANIFEST.sha256"
EVALS_DIR = "evals"

SUITE_FIELDS = ("seq", "kind", "suite", "version", "path", "manifest_sha256",
                "released_in", "approved_by", "note")
GUARDS_FIELDS = ("seq", "kind", "version", "files")
GUARDS_KEY = "<guards>"

_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

# Manifest formats, matched to each suite's own manifest.py:
#  * "versioned" (evals/e0/manifest.py): has a "# version:" header; files whose
#    suffix is in BINARY_SUFFIXES are hashed raw, every other file CRLF->LF;
#    skips __pycache__ dirs and *.pyc.
#  * "plain" (evals/e1/manifest.py): no header; every file CRLF->LF; skips
#    __pycache__ and .pytest_cache dirs and *.pyc.
VERSIONED_BINARY_SUFFIXES = frozenset({".npz", ".gz", ".parquet"})
VERSIONED_SKIP_DIRS = frozenset({"__pycache__"})
PLAIN_SKIP_DIRS = frozenset({"__pycache__", ".pytest_cache"})


class GuardError(Exception):
    """The registry or a tree cannot be read safely (fail closed)."""


# --------------------------------------------------------------------------- hashing

def lf_sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data.replace(b"\r\n", b"\n")).hexdigest()


def raw_sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _check_no_symlink(root: Path, rel: str) -> Path:
    """``root/rel`` with no component (including ``root``-relative parents) a symlink.

    A symlink in the head tree could point the hash at trusted base content
    while the committed tree holds something else, so any symlink fails closed.
    """
    if not rel or rel.startswith("/") or "\\" in rel:
        raise GuardError(f"bad relative path {rel!r}")
    parts = rel.split("/")
    if any(p in ("", ".", "..") for p in parts):
        raise GuardError(f"bad relative path {rel!r}")
    current = Path(root)
    for part in parts:
        current = current / part
        try:
            mode = os.lstat(current).st_mode
        except FileNotFoundError:
            return current
        if stat.S_ISLNK(mode):
            raise GuardError(f"symlink not allowed: {rel}")
    return current


def _read_file(root: Path, rel: str) -> bytes | None:
    path = _check_no_symlink(root, rel)
    try:
        mode = os.lstat(path).st_mode
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(mode):
        raise GuardError(f"not a regular file: {rel}")
    return path.read_bytes()


def lf_sha256_file(root: Path, rel: str) -> str | None:
    data = _read_file(root, rel)
    return None if data is None else lf_sha256_bytes(data)


# --------------------------------------------------------------------------- manifests

def parse_manifest(text: str) -> tuple[str | None, dict[str, str]]:
    """``(version or None, {relpath: sha256})`` for either manifest format."""
    version, pinned = None, {}
    for line in text.replace("\r\n", "\n").split("\n"):
        if not line.strip():
            continue
        if line.startswith("#"):
            if line.startswith("# version:"):
                version = line.split(":", 1)[1].strip()
            continue
        digest, sep, rel = line.partition("  ")
        if not sep or not _HEX64.match(digest) or not rel:
            raise GuardError(f"malformed manifest line: {line[:120]!r}")
        if rel in pinned:
            raise GuardError(f"manifest pins {rel!r} twice")
        pinned[rel] = digest
    return version, pinned


def walk_suite(root: Path, suite_rel: str, skip_dirs: frozenset) -> list[str]:
    """Every file under ``root/suite_rel`` (relative to the suite, sorted).

    Skips ``skip_dirs`` and ``*.pyc`` exactly as the suites' own manifest
    walkers do, excludes only the suite's top-level MANIFEST.sha256, and fails
    closed on any symlink or non-regular file.
    """
    base = _check_no_symlink(root, suite_rel)
    out = []
    for dirpath, dirnames, filenames in os.walk(base, followlinks=False):
        here = Path(dirpath)
        rel_dir = here.relative_to(base)
        kept = []
        for name in sorted(dirnames):
            if os.path.islink(here / name):
                raise GuardError(f"symlink not allowed: {suite_rel}/{(rel_dir / name).as_posix()}")
            if name in skip_dirs:
                continue
            kept.append(name)
        dirnames[:] = kept
        for name in filenames:
            rel = (rel_dir / name).as_posix()
            full = here / name
            mode = os.lstat(full).st_mode
            if stat.S_ISLNK(mode):
                raise GuardError(f"symlink not allowed: {suite_rel}/{rel}")
            if name.endswith(".pyc"):
                continue
            if rel == MANIFEST_NAME:
                continue
            if not stat.S_ISREG(mode):
                raise GuardError(f"not a regular file: {suite_rel}/{rel}")
            out.append(rel)
    return sorted(out)


def verify_suite_tree(root: Path, suite_rel: str) -> tuple[str | None, list[str]]:
    """R4 for one suite path: ``(manifest version header, problems)``."""
    data = _read_file(root, f"{suite_rel}/{MANIFEST_NAME}")
    if data is None:
        return None, [f"{suite_rel}/{MANIFEST_NAME} is missing"]
    version, pinned = parse_manifest(data.decode("utf-8"))
    if version is not None:
        skip, binary = VERSIONED_SKIP_DIRS, VERSIONED_BINARY_SUFFIXES
    else:
        skip, binary = PLAIN_SKIP_DIRS, frozenset()
    actual = walk_suite(root, suite_rel, skip)
    problems = []
    for rel in sorted(set(pinned) - set(actual)):
        problems.append(f"missing: {suite_rel}/{rel}")
    for rel in sorted(set(actual) - set(pinned)):
        problems.append(f"unpinned: {suite_rel}/{rel}")
    for rel in sorted(set(actual) & set(pinned)):
        raw = _read_file(root, f"{suite_rel}/{rel}") or b""
        suffix = os.path.splitext(rel)[1].lower()
        digest = raw_sha256_bytes(raw) if suffix in binary else lf_sha256_bytes(raw)
        if digest != pinned[rel]:
            problems.append(f"changed: {suite_rel}/{rel}")
    return version, problems


# --------------------------------------------------------------------------- registry

def canonical(entry: dict) -> str:
    return json.dumps(entry, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def load_registry(root: Path, *, missing_ok: bool = False) -> list[dict]:
    data = _read_file(Path(root), REGISTRY)
    if data is None:
        if missing_ok:
            return []
        raise GuardError(f"{REGISTRY} is missing")
    try:
        doc = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise GuardError(f"{REGISTRY} is not valid JSON: {exc}") from None
    if not isinstance(doc, dict) or doc.get("schema") != SCHEMA:
        raise GuardError(f"{REGISTRY}: schema must be {SCHEMA}")
    if set(doc) != {"schema", "entries"} or not isinstance(doc["entries"], list):
        raise GuardError(f"{REGISTRY}: top level must be exactly {{schema, entries[]}}")
    for entry in doc["entries"]:
        if not isinstance(entry, dict):
            raise GuardError(f"{REGISTRY}: every entry must be an object")
    return doc["entries"]


def entry_key(entry: dict) -> str:
    return GUARDS_KEY if entry.get("kind") == "guards" else str(entry.get("path"))


def _validate_entry(entry: dict) -> list[str]:
    seq = entry.get("seq")
    kind = entry.get("kind")
    if kind == "suite":
        fields = SUITE_FIELDS
    elif kind == "guards":
        fields = GUARDS_FIELDS
    else:
        return [f"R2: entry {seq}: unknown kind {kind!r}"]
    problems = []
    if set(entry) != set(fields):
        problems.append(f"R2: entry {seq}: {kind} fields must be exactly {sorted(fields)}")
        return problems
    if not isinstance(entry["version"], str) or not _NAME.match(entry["version"]):
        problems.append(f"R2: entry {seq}: bad version {entry['version']!r}")
    if kind == "suite":
        for name in ("suite", "released_in", "approved_by", "note"):
            if not isinstance(entry[name], str) or not entry[name].strip():
                problems.append(f"R2: entry {seq}: {name} must be a non-empty string")
        if isinstance(entry["suite"], str) and not _NAME.match(entry["suite"]):
            problems.append(f"R2: entry {seq}: bad suite name {entry['suite']!r}")
        path = entry["path"]
        if (not isinstance(path, str) or not path.startswith(EVALS_DIR + "/")
                or any(p in ("", ".", "..") for p in path.split("/"))
                or "\\" in path or path.endswith("/")):
            problems.append(f"R2: entry {seq}: bad path {path!r} (must be evals/<dir>)")
        if not isinstance(entry["manifest_sha256"], str) or not _HEX64.match(entry["manifest_sha256"]):
            problems.append(f"R2: entry {seq}: manifest_sha256 must be 64 lowercase hex")
    else:
        files = entry["files"]
        if not isinstance(files, dict) or not files:
            problems.append(f"R2: entry {seq}: guards files must be a non-empty object")
        else:
            for rel, digest in files.items():
                if (not isinstance(rel, str) or rel.startswith("/") or "\\" in rel
                        or any(p in ("", ".", "..") for p in rel.split("/"))):
                    problems.append(f"R2: entry {seq}: bad guard path {rel!r}")
                if not isinstance(digest, str) or not _HEX64.match(digest):
                    problems.append(f"R2: entry {seq}: guard hash for {rel!r} must be 64 lowercase hex")
    return problems


def latest_by_key(entries: list[dict]) -> dict[str, dict]:
    latest: dict[str, dict] = {}
    for entry in entries:
        latest[entry_key(entry)] = entry
    return latest


# --------------------------------------------------------------------------- the guard

def check(base_dir: Path, head_dir: Path) -> list[str]:
    """Every rule violation of HEAD against BASE (empty list = pass)."""
    base_dir, head_dir = Path(base_dir), Path(head_dir)
    failures: list[str] = []
    try:
        base_entries = load_registry(base_dir, missing_ok=True)
    except GuardError as exc:
        return [f"base registry unreadable: {exc}"]
    try:
        head_entries = load_registry(head_dir)
    except GuardError as exc:
        return [f"R2: {exc}"]

    # R1: the base entries are an exact prefix of the head entries.
    for i, old in enumerate(base_entries):
        if i >= len(head_entries):
            failures.append(f"R1: released entry {old.get('seq', i)} changed/deleted (deleted)")
        elif canonical(old) != canonical(head_entries[i]):
            failures.append(f"R1: released entry {old.get('seq', i)} changed/deleted (changed)")

    # R2: schema (checked on load), contiguous seq from 0, well-formed entries.
    for i, entry in enumerate(head_entries):
        if entry.get("seq") != i or isinstance(entry.get("seq"), bool):
            failures.append(f"R2: entries[{i}] has seq {entry.get('seq')!r}; seq must be contiguous from 0")
        failures.extend(_validate_entry(entry))
    if any(f.startswith("R2:") for f in failures):
        return failures  # the rest of the rules need well-formed entries

    # R6: no (suite, version) twice; no guards version twice.
    seen: set = set()
    for entry in head_entries:
        ident = ("guards", entry["version"]) if entry["kind"] == "guards" else (entry["suite"], entry["version"])
        if ident in seen:
            failures.append(f"R6: duplicate release {ident[0]} {ident[1]} (entry {entry['seq']})")
        seen.add(ident)
    # One path holds one suite.
    path_suite: dict[str, str] = {}
    for entry in head_entries:
        if entry["kind"] == "suite":
            prior = path_suite.setdefault(entry["path"], entry["suite"])
            if prior != entry["suite"]:
                failures.append(f"R6: entry {entry['seq']}: path {entry['path']} already holds suite {prior}")

    # R8: at most one new entry per suite path (and one new guards entry) per change.
    new_entries = head_entries[len(base_entries):] if len(head_entries) > len(base_entries) else []
    counts: dict[str, int] = {}
    for entry in new_entries:
        counts[entry_key(entry)] = counts.get(entry_key(entry), 0) + 1
    for key, n in sorted(counts.items()):
        label = "guards" if key == GUARDS_KEY else key
        if n > 1:
            failures.append(f"R8: {n} new entries for {label} in one change; release one version at a time")

    latest = latest_by_key(head_entries)
    suite_paths = sorted(k for k in latest if k != GUARDS_KEY)

    for path in suite_paths:
        entry = latest[path]
        label = f"{entry['suite']} ({path})"
        try:
            # R9: a released suite path cannot disappear.
            target = _check_no_symlink(head_dir, path)
            if not target.is_dir():
                failures.append(f"R9: released suite {label} is missing from the head tree")
                continue
            # R3: the head manifest is the latest released one for this path.
            digest = lf_sha256_file(head_dir, f"{path}/{MANIFEST_NAME}")
            if digest is None:
                failures.append(f"R9: released suite {label} has no {MANIFEST_NAME} at head")
                continue
            if digest != entry["manifest_sha256"]:
                failures.append(
                    f"R3: {entry['suite']} manifest is not a released version: {path}/{MANIFEST_NAME} "
                    f"sha256 {digest[:12]} != latest released {entry['version']} "
                    f"{entry['manifest_sha256'][:12]}")
            # R4: every file under the path matches its manifest line.
            header, problems = verify_suite_tree(head_dir, path)
            if header is not None and header != entry["version"]:
                failures.append(f"R3: {path}/{MANIFEST_NAME} header says {header} but the latest "
                                f"released version for this path is {entry['version']}")
            for problem in problems:
                failures.append(f"R4: {label}: {problem}")
        except (GuardError, UnicodeDecodeError, OSError) as exc:
            failures.append(f"R4: {label}: {exc}")

    # R5: every file pinned by the latest guards entry matches at head.
    guards = latest.get(GUARDS_KEY)
    if guards is None:
        failures.append("R5: no guards entry is registered")
    else:
        for rel, want in sorted(guards["files"].items()):
            try:
                got = lf_sha256_file(head_dir, rel)
            except (GuardError, OSError) as exc:
                failures.append(f"R5: guard file {rel}: {exc}")
                continue
            if got is None:
                failures.append(f"R5: guard file {rel} is missing (pinned by {guards['version']})")
            elif got != want:
                failures.append(f"R5: guard file {rel} changed without a new guards entry "
                                f"(pinned by {guards['version']})")

    # R7: every MANIFEST.sha256 under evals/ belongs to a registered suite path.
    try:
        evals_root = _check_no_symlink(head_dir, EVALS_DIR)
        if evals_root.is_dir():
            for dirpath, dirnames, filenames in os.walk(evals_root, followlinks=False):
                dirnames[:] = sorted(d for d in dirnames if d not in PLAIN_SKIP_DIRS)
                if MANIFEST_NAME not in filenames:
                    continue
                rel_dir = Path(dirpath).relative_to(head_dir).as_posix()
                if rel_dir in latest:
                    continue
                if any(rel_dir.startswith(p + "/") for p in suite_paths):
                    continue  # inside a registered suite: R4 reports it as unpinned
                failures.append(f"R7: unreleased suite {rel_dir}: it has a {MANIFEST_NAME} but no "
                                f"entry in {REGISTRY}")
    except (GuardError, OSError) as exc:
        failures.append(f"R7: {exc}")
    return failures


# --------------------------------------------------------------------------- read API

def _default_root() -> Path:
    return Path(__file__).resolve().parent.parent


def entries(root: Path | None = None) -> list[dict]:
    return load_registry(root or _default_root())


def latest_entry(suite: str, path: str | None = None, root: Path | None = None) -> dict:
    """The latest released ``suite`` entry for ``suite`` (optionally at ``path``)."""
    found = None
    for entry in entries(root):
        if entry.get("kind") == "suite" and entry.get("suite") == suite:
            if path is None or entry.get("path") == path:
                found = entry
    if found is None:
        raise KeyError(f"no released entry for suite {suite!r}" + (f" at {path!r}" if path else ""))
    return found


# --------------------------------------------------------------------------- CLI

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="released.py", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_check = sub.add_parser("check", help="enforce R1-R9 of HEAD against BASE")
    p_check.add_argument("--base-dir", required=True, type=Path)
    p_check.add_argument("--head-dir", required=True, type=Path)
    p_hash = sub.add_parser("lf-sha256", help="print the CRLF->LF sha256 of files")
    p_hash.add_argument("paths", nargs="+", type=Path)
    args = parser.parse_args(argv)

    if args.cmd == "lf-sha256":
        for path in args.paths:
            print(f"{lf_sha256_bytes(path.read_bytes())}  {path.as_posix()}")
        return 0

    try:
        failures = check(args.base_dir, args.head_dir)
    except GuardError as exc:
        failures = [f"guard error: {exc}"]
    if failures:
        print(f"evals-freeze-guard: FAIL ({len(failures)} problem(s)); see evals/README.md")
        for failure in failures:
            print(f"  {failure}")
        return 1
    head_entries = load_registry(args.head_dir)
    print(f"evals-freeze-guard: OK ({len(head_entries)} released entries)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
