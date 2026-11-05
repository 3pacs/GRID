"""Hash-pinning of every E0 file: code, config, seeds, structure and cached data.

``MANIFEST.sha256`` lists ``<sha256>  <path>`` for each pinned file (paths
relative to ``evals/e0``; text files hashed with CRLF normalised to LF so a
Windows checkout verifies too) under a ``# version:`` header. ``run`` refuses
to score unless every pinned file matches; the CI guard
(``tests/test_e0_benchmark.py``) additionally pins the manifest's own hash per
released version, so a pinned file cannot change without a new version.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

PACKAGE = Path(__file__).resolve().parent
MANIFEST = PACKAGE / "MANIFEST.sha256"
BINARY_SUFFIXES = frozenset({".npz", ".gz", ".parquet"})
EXCLUDED_NAMES = frozenset({"MANIFEST.sha256"})
EXCLUDED_DIRS = frozenset({"__pycache__"})


class ManifestError(RuntimeError):
    """A pinned E0 file differs from its manifest entry."""


def file_sha256(path: Path) -> str:
    data = Path(path).read_bytes()
    if Path(path).suffix.lower() not in BINARY_SUFFIXES:
        data = data.replace(b"\r\n", b"\n")
    return hashlib.sha256(data).hexdigest()


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
        "# E0 machinery-calibration benchmark manifest (sha256, CRLF->LF for text files)",
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
            f"E0 {version} manifest mismatch ({len(problems)}): " + "; ".join(problems[:20])
            + ". A pinned change is a new benchmark version (see evals/e0/README.md)."
        )
    return {"version": version, "files": len(entries), "manifest_sha256": file_sha256(manifest)}


def write(version: str, root: Path = PACKAGE) -> str:
    text = render(version, root)
    (Path(root) / "MANIFEST.sha256").write_text(text, encoding="utf-8", newline="\n")
    return text
