"""verify_preserved_checkout in scripts/deploy_release_swap.sh must read only.

The function checks that the running scheduler's release, the recovery release
and the retained rollback release are each the approved checkout: its own
root, the approved HEAD and tree, and no tracked file changed. It ran
``git diff --quiet HEAD --`` for the last part. ``git diff`` refreshes and
rewrites ``.git/index`` when a tracked file has stale stat info, and it ignores
``--no-optional-locks``, so a deploy could write into the checkout of the
process it is meant to leave alone. The function now reads ``git status``.

The function is nested inside the swap script's preservation block, which also
reads systemd state, so these tests extract the exact function text from the
script and run it against fixture repositories. That keeps the tests bound to
the shipped code rather than a copy.
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "deploy_release_swap.sh"


def _function_source() -> str:
    text = SCRIPT.read_text(encoding="utf-8")
    match = re.search(
        r"^  verify_preserved_checkout\(\) \{\n.*?^  \}\n", text, re.S | re.M
    )
    assert match, "verify_preserved_checkout() not found in deploy_release_swap.sh"
    return match.group(0)


def _git(cwd: Path, *args: str) -> str:
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@example.invalid",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@example.invalid",
    }
    return subprocess.run(
        ["git", "-C", str(cwd), *args],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    ).stdout.strip()


def _checkout(tmp_path: Path) -> tuple[Path, str, str]:
    rel = tmp_path / "rel"
    rel.mkdir()
    _git(rel, "init", "-q")
    (rel / "f.txt").write_text("a", encoding="utf-8")
    _git(rel, "add", "f.txt")
    _git(rel, "commit", "-q", "-m", "a")
    return rel.resolve(), _git(rel, "rev-parse", "HEAD"), _git(rel, "rev-parse", "HEAD^{tree}")


def _verify(dir_: Path, sha: str, tree: str) -> subprocess.CompletedProcess:
    script = (
        "set -euo pipefail\n"
        + _function_source()
        + 'verify_preserved_checkout scheduler "$1" "$2" "$3"\n'
        + "echo VERIFIED\n"
    )
    return subprocess.run(
        ["bash", "-c", script, "verify", str(dir_), sha, tree],
        capture_output=True,
        text=True,
        timeout=20,
    )


@pytest.mark.unit
def test_function_uses_git_status_not_git_diff() -> None:
    src = _function_source()
    assert "status --porcelain" in src
    assert "diff --quiet" not in src


@pytest.mark.unit
def test_clean_checkout_verifies(tmp_path: Path) -> None:
    rel, sha, tree = _checkout(tmp_path)
    result = _verify(rel, sha, tree)
    assert result.returncode == 0, result.stderr
    assert "VERIFIED" in result.stdout


@pytest.mark.unit
def test_verifying_never_rewrites_the_preserved_index(tmp_path: Path) -> None:
    # Tracked file mtime 2 s past the index: outside git's racy window, so
    # `git diff` would deterministically refresh and rewrite the index here.
    rel, sha, tree = _checkout(tmp_path)
    index = rel / ".git" / "index"
    future = index.stat().st_mtime_ns + 2_000_000_000
    os.utime(rel / "f.txt", ns=(future, future))
    stamp = (index.stat().st_mtime_ns, index.read_bytes())

    result = _verify(rel, sha, tree)

    assert result.returncode == 0, result.stderr
    assert (index.stat().st_mtime_ns, index.read_bytes()) == stamp
    assert not (rel / ".git" / "index.lock").exists()


@pytest.mark.unit
def test_modified_tracked_file_refuses(tmp_path: Path) -> None:
    rel, sha, tree = _checkout(tmp_path)
    (rel / "f.txt").write_text("edited", encoding="utf-8")
    result = _verify(rel, sha, tree)
    assert result.returncode == 5
    assert "VERIFIED" not in result.stdout


@pytest.mark.unit
def test_staged_change_whose_file_was_restored_refuses(tmp_path: Path) -> None:
    rel, sha, tree = _checkout(tmp_path)
    (rel / "f.txt").write_text("staged edit", encoding="utf-8")
    _git(rel, "add", "f.txt")
    (rel / "f.txt").write_text("a", encoding="utf-8")
    result = _verify(rel, sha, tree)
    assert result.returncode == 5
    assert "VERIFIED" not in result.stdout


@pytest.mark.unit
def test_deleted_tracked_file_refuses(tmp_path: Path) -> None:
    rel, sha, tree = _checkout(tmp_path)
    (rel / "f.txt").unlink()
    result = _verify(rel, sha, tree)
    assert result.returncode == 5
    assert "VERIFIED" not in result.stdout


@pytest.mark.unit
def test_untracked_output_does_not_refuse(tmp_path: Path) -> None:
    rel, sha, tree = _checkout(tmp_path)
    (rel / "pwa_dist.js").write_text("generated", encoding="utf-8")
    result = _verify(rel, sha, tree)
    assert result.returncode == 0, result.stderr
