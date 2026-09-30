"""E0 is frozen: every pinned file must match MANIFEST.sha256, and the manifest
itself must be the one released for its version.

Changing anything under ``evals/e0/`` (code, config, seeds, structure, cached
data) is a new benchmark version: bump ``evals.e0.VERSION`` and ``config.json``,
run ``python -m evals.e0 manifest --write --version e0-vN`` and ADD the new
manifest's sha256 below (never edit a released entry). See evals/e0/README.md.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from evals.e0 import VERSION, manifest

#: version -> sha256 of evals/e0/MANIFEST.sha256 (LF). Append-only.
RELEASED_MANIFESTS = {
    "e0-v1": "db0318ccf5e8dddb055bbaa068e3a6312476bc6d415dcb512698ef78ee0128ab",
}


def test_every_pinned_file_matches_the_manifest():
    result = manifest.verify()
    assert result["version"] == VERSION


def test_manifest_is_the_released_one_for_its_version():
    version, _ = manifest.parse(manifest.MANIFEST.read_text(encoding="utf-8"))
    assert version == VERSION, "evals.e0.VERSION and the manifest header differ"
    assert version in RELEASED_MANIFESTS, (
        f"{version} is not a released E0 version: add it to RELEASED_MANIFESTS (append-only)"
    )
    assert manifest.manifest_sha256() == RELEASED_MANIFESTS[version], (
        f"MANIFEST.sha256 changed without a version bump: {version} was released with a different "
        "manifest. Bump evals.e0.VERSION/config.json and register the new version instead."
    )


def test_config_version_matches_package():
    config = json.loads((manifest.PACKAGE / "config.json").read_text(encoding="utf-8"))
    assert config["version"] == VERSION


def _copy(tmp_path: Path) -> Path:
    root = tmp_path / "e0"
    shutil.copytree(manifest.PACKAGE, root, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    return root


def test_guard_detects_a_changed_pinned_file(tmp_path):
    root = _copy(tmp_path)
    manifest.verify(root)
    config = root / "config.json"
    config.write_text(config.read_text(encoding="utf-8").replace('"base": 20260930', '"base": 1'),
                      encoding="utf-8")
    with pytest.raises(manifest.ManifestError, match="changed: config.json"):
        manifest.verify(root)


def test_guard_detects_added_and_removed_files(tmp_path):
    root = _copy(tmp_path)
    (root / "sneaky.py").write_text("x = 1\n", encoding="utf-8")
    with pytest.raises(manifest.ManifestError, match="unpinned: sneaky.py"):
        manifest.verify(root)
    (root / "sneaky.py").unlink()
    (root / "scorer.py").unlink()
    with pytest.raises(manifest.ManifestError, match="missing: scorer.py"):
        manifest.verify(root)


def test_line_endings_do_not_break_verification(tmp_path):
    root = _copy(tmp_path)
    path = root / "scorer.py"
    path.write_bytes(path.read_bytes().replace(b"\r\n", b"\n").replace(b"\n", b"\r\n"))
    manifest.verify(root)
