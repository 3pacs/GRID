"""Hash-pinning of every E2 file: scoring code, adapters, rules and cost model.

Same scheme as ``evals/e0/manifest.py``: ``MANIFEST.sha256`` lists
``<sha256>  <path>`` for every file under ``evals/e2`` (text hashed with CRLF
normalised to LF) under a ``# version:`` header. ``python -m evals.e2 run``
refuses to append to the ledger unless every pinned file matches, and the
ledger header carries the manifest's own sha256, so a ledger can only ever
be extended by the exact code that started it. The CI guard
(``tests/test_e2_manifest_guard.py``) pins the manifest hash per released
version (append-only), so a pinned file cannot change without a new version.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

PACKAGE = Path(__file__).resolve().parent
MANIFEST = PACKAGE / "MANIFEST.sha256"
EXCLUDED_NAMES = frozenset({"MANIFEST.sha256"})
EXCLUDED_DIRS = frozenset({"__pycache__", ".pytest_cache"})


class ManifestError(RuntimeError):
    """A pinned E2 file differs from its manifest entry."""


def file_sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def pinned_files(root: Path = PACKAGE) -> list[str]:
    out = []
    for path in sorted(Path(root).rglob("*")):
        if not path.is_file() or path.name in EXCLUDED_NAMES or path.suffix == ".pyc":
            continue
        if any(part in EXCLUDED_DIRS for part in path.relative_to(root).parts):
            continue
        out.append(path.relative_to(root).as_posix())
    return out


def render(version: str, root: Path = PACKAGE) -> str:
    lines = [
        "# E2 forward-scoreboard manifest (sha256, CRLF->LF for text files)",
        f"# version: {version}",
    ]
    lines += [f"{file_sha256(Path(root) / rel)}  {rel}" for rel in pinned_files(root)]
    return "\n".join(lines) + "\n"


def parse(text: str) -> tuple[str, dict[str, str]]:
    version, entries = None, {}
    for line in text.replace("\r\n", "\n").splitlines():
        if line.startswith("# version:"):
            version = line.split(":", 1)[1].strip()
        elif line and not line.startswith("#"):
            digest, rel = line.split("  ", 1)
            entries[rel] = digest
    if not version:
        raise ManifestError("manifest has no version header")
    return version, entries


def manifest_sha256(path: Path = MANIFEST) -> str:
    return file_sha256(path)


def verify(root: Path = PACKAGE, manifest: Path | None = None) -> dict:
    """Every pinned file present and matching; no unpinned file. Raises ManifestError."""
    manifest = manifest or Path(root) / "MANIFEST.sha256"
    if not manifest.is_file():
        raise ManifestError("MANIFEST.sha256 is missing")
    version, entries = parse(manifest.read_text(encoding="utf-8"))
    actual = set(pinned_files(root))
    problems = []
    for rel in sorted(set(entries) - actual):
        problems.append(f"missing: {rel}")
    for rel in sorted(actual - set(entries)):
        problems.append(f"unpinned: {rel}")
    for rel in sorted(actual & set(entries)):
        if file_sha256(Path(root) / rel) != entries[rel]:
            problems.append(f"changed: {rel}")
    if problems:
        raise ManifestError(
            f"E2 {version} manifest mismatch ({len(problems)}): " + "; ".join(problems[:20])
            + ". A pinned change is a new scoreboard version (see evals/e2/README.md)."
        )
    return {"version": version, "files": len(entries), "manifest_sha256": file_sha256(manifest)}


def write(version: str, root: Path = PACKAGE) -> str:
    text = render(version, root)
    (Path(root) / "MANIFEST.sha256").write_text(text, encoding="utf-8", newline="\n")
    return text
