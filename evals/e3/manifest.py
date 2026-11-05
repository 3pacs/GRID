"""Hash pin for the E3 suite: ``MANIFEST.sha256`` over every file in ``evals/e3``.

Format (the E0 "versioned" format that the EVAL-E0H1 freeze guard reads): a
``# version:`` header, then one ``<sha256>  <path>`` line per file, sorted by
path, LF, where ``path`` is relative to ``evals/e3`` with ``/`` separators and
text files are hashed with CRLF normalised to LF (a Windows checkout with
``core.autocrlf=true`` hashes the same as Linux CI). ``__pycache__`` and
``*.pyc`` are skipped.

    python -m evals.e3.manifest            # print the manifest that would be written
    python -m evals.e3.manifest --check    # exit 1 if MANIFEST.sha256 is stale
    python -m evals.e3.manifest --write    # regenerate (a reviewed, versioned change)
"""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path

PACKAGE = Path(__file__).resolve().parent
MANIFEST_NAME = "MANIFEST.sha256"
BINARY_SUFFIXES = frozenset({".npz", ".gz", ".parquet"})
EXCLUDED_DIRS = frozenset({"__pycache__"})


class ManifestError(RuntimeError):
    """A pinned E3 file differs from its manifest entry."""


def file_sha256(path: Path) -> str:
    data = Path(path).read_bytes()
    if Path(path).suffix.lower() not in BINARY_SUFFIXES:
        data = data.replace(b"\r\n", b"\n")
    return hashlib.sha256(data).hexdigest()


def pinned_files(root: Path = PACKAGE) -> list[str]:
    out = []
    # sort by the POSIX relative path: Path ordering is case-insensitive on Windows only
    for path in sorted(Path(root).rglob("*"), key=lambda p: p.relative_to(root).as_posix()):
        if not path.is_file() or path.name == MANIFEST_NAME or path.suffix == ".pyc":
            continue
        if any(part in EXCLUDED_DIRS for part in path.relative_to(root).parts):
            continue
        out.append(path.relative_to(root).as_posix())
    return out


def render(version: str, root: Path = PACKAGE) -> str:
    lines = [
        "# E3 hill-climb harness manifest (sha256, CRLF->LF for text files)",
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


def manifest_sha256(root: Path = PACKAGE) -> str:
    return file_sha256(Path(root) / MANIFEST_NAME)


def verify(root: Path = PACKAGE) -> dict:
    """Every pinned file present and matching; no unpinned file. Raises ManifestError."""
    manifest = Path(root) / MANIFEST_NAME
    if not manifest.is_file():
        raise ManifestError("MANIFEST.sha256 is missing")
    version, entries = parse(manifest.read_text(encoding="utf-8"))
    actual = set(pinned_files(root))
    problems = [f"missing: {rel}" for rel in sorted(set(entries) - actual)]
    problems += [f"unpinned: {rel}" for rel in sorted(actual - set(entries))]
    problems += [
        f"changed: {rel}"
        for rel in sorted(actual & set(entries))
        if file_sha256(Path(root) / rel) != entries[rel]
    ]
    if problems:
        raise ManifestError(
            f"E3 {version} manifest mismatch ({len(problems)}): " + "; ".join(problems[:20])
            + ". A pinned change is a new suite version (see evals/e3/README.md)."
        )
    return {"version": version, "files": len(entries), "manifest_sha256": manifest_sha256(root)}


def main(argv: list[str]) -> int:
    from evals.e3 import VERSION

    manifest = PACKAGE / MANIFEST_NAME
    rendered = render(VERSION)
    if "--write" in argv:
        manifest.write_bytes(rendered.encode("utf-8"))
        print(f"wrote {manifest} ({len(parse(rendered)[1])} files, {VERSION})")
        return 0
    if "--check" in argv:
        current = manifest.read_bytes().decode("utf-8").replace("\r\n", "\n") if manifest.exists() else ""
        if current != rendered:
            print("MANIFEST.sha256 is stale; review the eval change, then run --write")
            return 1
        print("MANIFEST.sha256 matches")
        return 0
    sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
