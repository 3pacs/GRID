"""Hash-pinning of every E0 v2 file: v1's manifest logic, parameterised on this package.

Same format as ``evals/e0/MANIFEST.sha256`` (``# version:`` header; text files
hashed CRLF->LF, ``.npz``/``.gz``/``.parquet`` raw), so the registry guard
(``evals/released.py``) reads both identically.
"""

from __future__ import annotations

from pathlib import Path

from evals.e0 import manifest as _v1

PACKAGE = Path(__file__).resolve().parent
MANIFEST = PACKAGE / "MANIFEST.sha256"
ManifestError = _v1.ManifestError
file_sha256 = _v1.file_sha256
parse = _v1.parse


def pinned_files(root: Path = PACKAGE) -> list[str]:
    return _v1.pinned_files(root)


def render(version: str, root: Path = PACKAGE) -> str:
    return _v1.render(version, root)


def manifest_sha256(path: Path = MANIFEST) -> str:
    return _v1.manifest_sha256(path)


def verify(root: Path = PACKAGE, manifest: Path | None = None) -> dict:
    return _v1.verify(root, manifest)


def verify_builds_on() -> dict:
    """The frozen e0-v1 package v2 imports must be exactly the released e0-v1."""
    from evals.e0v2 import BUILDS_ON_MANIFEST_SHA256, BUILDS_ON_VERSION

    verified = _v1.verify()
    if verified["version"] != BUILDS_ON_VERSION or verified["manifest_sha256"] != BUILDS_ON_MANIFEST_SHA256:
        raise ManifestError(f"evals/e0 is not the e0-v1 that e0-v2 builds on: {verified}")
    return verified


def write(version: str, root: Path = PACKAGE) -> str:
    return _v1.write(version, root)
