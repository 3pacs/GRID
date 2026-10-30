"""Hash pin for the E1 suite: ``MANIFEST.sha256`` over every file in ``evals/e1``.

Format (one line per file, sorted by path, LF): ``<sha256>  <path>`` where
``path`` is relative to ``evals/e1`` with ``/`` separators and the hash is
over the file's bytes with CRLF normalised to LF (a Windows checkout with
``core.autocrlf=true`` must hash the same as Linux CI).

    python -m evals.e1.manifest            # print the manifest that would be written
    python -m evals.e1.manifest --check    # exit 1 if MANIFEST.sha256 is stale
    python -m evals.e1.manifest --write    # regenerate (a reviewed eval change)
"""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path

SUITE_DIR = Path(__file__).resolve().parent
MANIFEST_NAME = "MANIFEST.sha256"
_IGNORED_DIRS = frozenset({"__pycache__", ".pytest_cache"})


def lf_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def suite_files(root: Path = SUITE_DIR) -> list[str]:
    out = []
    for path in root.rglob("*"):
        if not path.is_file() or path.name == MANIFEST_NAME:
            continue
        rel = path.relative_to(root)
        if any(part in _IGNORED_DIRS for part in rel.parts) or path.suffix == ".pyc":
            continue
        out.append(rel.as_posix())
    return sorted(out)


def render(root: Path = SUITE_DIR) -> str:
    return "".join(f"{lf_sha256(root / rel)}  {rel}\n" for rel in suite_files(root))


def parse(text: str) -> dict[str, str]:
    pinned: dict[str, str] = {}
    for line in text.replace("\r\n", "\n").splitlines():
        if not line.strip():
            continue
        digest, _, rel = line.partition("  ")
        pinned[rel] = digest
    return pinned


def main(argv: list[str]) -> int:
    manifest = SUITE_DIR / MANIFEST_NAME
    rendered = render()
    if "--write" in argv:
        manifest.write_bytes(rendered.encode())
        print(f"wrote {manifest} ({len(parse(rendered))} files)")
        return 0
    if "--check" in argv:
        current = manifest.read_bytes().decode().replace("\r\n", "\n") if manifest.exists() else ""
        if current != rendered:
            print("MANIFEST.sha256 is stale; review the eval change, then run --write")
            return 1
        print("MANIFEST.sha256 matches")
        return 0
    sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
