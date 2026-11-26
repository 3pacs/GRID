"""Tests for scripts/resolve_pinned_release.sh and its use in deploy.yml.

The grid-intelligence activation step used to write
``WorkingDirectory=$DEPLOY_PATH`` into its drop-in. ``$DEPLOY_PATH`` is the
mutable ``grid_release`` symlink that every push deploy moves, so with
``Restart=always`` a crash restart would have put the loop on whatever main
was at that moment, without anyone choosing it. On 2026-11-26 the drop-in held
the symlink while the process ran release 62112241. The scheduler step already
pinned the exact release directory; these tests hold the intelligence step to
the same rule.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "resolve_pinned_release.sh"
DEPLOY_YML = REPO_ROOT / ".github" / "workflows" / "deploy.yml"


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


def _release(root: Path, name: str) -> tuple[Path, str]:
    """A release dir under ``<root>/grid_release.releases`` holding one commit."""
    rel = root / "grid_release.releases" / name
    rel.mkdir(parents=True)
    _git(rel, "init", "-q")
    (rel / "f.txt").write_text(name, encoding="utf-8")
    _git(rel, "add", "f.txt")
    _git(rel, "commit", "-q", "-m", name)
    return rel, _git(rel, "rev-parse", "HEAD")


def _run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", str(SCRIPT), *args], capture_output=True, text=True, timeout=20
    )


@pytest.mark.unit
def test_resolves_symlink_to_exact_release_dir(tmp_path: Path) -> None:
    rel, sha = _release(tmp_path, "a")
    link = tmp_path / "grid_release"
    link.symlink_to(rel)

    result = _run(str(link), sha)

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == str(rel.resolve())
    assert result.stdout.strip() != str(link)


@pytest.mark.unit
def test_refuses_when_release_is_not_the_expected_commit(tmp_path: Path) -> None:
    rel, _ = _release(tmp_path, "a")
    link = tmp_path / "grid_release"
    link.symlink_to(rel)

    result = _run(str(link), "0" * 40)

    assert result.returncode == 7
    assert result.stdout == ""
    assert "REFUSED" in result.stderr


@pytest.mark.unit
def test_refuses_target_outside_the_releases_root(tmp_path: Path) -> None:
    (tmp_path / "grid_release.releases").mkdir()
    stray = tmp_path / "elsewhere"
    stray.mkdir()
    _git(stray, "init", "-q")
    (stray / "f.txt").write_text("x", encoding="utf-8")
    _git(stray, "add", "f.txt")
    _git(stray, "commit", "-q", "-m", "x")
    sha = _git(stray, "rev-parse", "HEAD")
    link = tmp_path / "grid_release"
    link.symlink_to(stray)

    result = _run(str(link), sha)

    assert result.returncode == 5
    assert result.stdout == ""


@pytest.mark.unit
def test_refuses_a_modified_tracked_file_under_the_same_head(tmp_path: Path) -> None:
    rel, sha = _release(tmp_path, "a")
    (rel / "f.txt").write_text("edited after checkout", encoding="utf-8")
    link = tmp_path / "grid_release"
    link.symlink_to(rel)

    result = _run(str(link), sha)

    assert result.returncode == 9
    assert result.stdout == ""
    assert "tracked files" in result.stderr


@pytest.mark.unit
def test_refuses_a_staged_change_under_the_same_head(tmp_path: Path) -> None:
    rel, sha = _release(tmp_path, "a")
    (rel / "f.txt").write_text("staged edit", encoding="utf-8")
    _git(rel, "add", "f.txt")
    link = tmp_path / "grid_release"
    link.symlink_to(rel)

    result = _run(str(link), sha)

    assert result.returncode == 9
    assert result.stdout == ""


@pytest.mark.unit
def test_untracked_files_do_not_block_a_clean_checkout(tmp_path: Path) -> None:
    # Release trees carry untracked build output (pwa_dist and the like).
    rel, sha = _release(tmp_path, "a")
    (rel / "build-output.js").write_text("generated", encoding="utf-8")
    link = tmp_path / "grid_release"
    link.symlink_to(rel)

    result = _run(str(link), sha)

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == str(rel.resolve())


@pytest.mark.unit
def test_refuses_a_directory_that_is_not_its_checkout_root(tmp_path: Path) -> None:
    # The releases root itself is the checkout; "a" is only a subdirectory of it.
    root = tmp_path / "grid_release.releases"
    sub = root / "a"
    sub.mkdir(parents=True)
    _git(root, "init", "-q")
    (sub / "f.txt").write_text("a", encoding="utf-8")
    _git(root, "add", "a/f.txt")
    _git(root, "commit", "-q", "-m", "a")
    sha = _git(root, "rev-parse", "HEAD")
    link = tmp_path / "grid_release"
    link.symlink_to(sub)

    result = _run(str(link), sha)

    assert result.returncode == 6
    assert result.stdout == ""
    assert "root of its checkout" in result.stderr


@pytest.mark.unit
@pytest.mark.parametrize("bad", ["", "abc", "A" * 40, "g" * 40, "0" * 39])
def test_refuses_anything_but_a_full_lowercase_sha(tmp_path: Path, bad: str) -> None:
    rel, _ = _release(tmp_path, "a")
    link = tmp_path / "grid_release"
    link.symlink_to(rel)

    result = _run(str(link), bad)

    assert result.returncode != 0
    assert result.stdout == ""


@pytest.mark.unit
def test_refuses_missing_link(tmp_path: Path) -> None:
    (tmp_path / "grid_release.releases").mkdir()
    result = _run(str(tmp_path / "grid_release"), "0" * 40)
    assert result.returncode != 0
    assert result.stdout == ""


def _intelligence_restart_run() -> str:
    workflow = yaml.safe_load(DEPLOY_YML.read_text(encoding="utf-8"))
    for step in workflow["jobs"]["deploy"]["steps"]:
        if (step.get("name") or "").startswith("Restart grid-intelligence"):
            return step["run"]
    raise AssertionError("deploy.yml has no 'Restart grid-intelligence' step")


@pytest.mark.unit
def test_intelligence_drop_in_pins_the_resolved_release_not_the_symlink() -> None:
    run = _intelligence_restart_run()

    assert "resolve_pinned_release.sh" in run
    assert '"WorkingDirectory=$INTELLIGENCE_RELEASE"' in run
    assert '"WorkingDirectory=$DEPLOY_PATH"' not in run, (
        "the intelligence drop-in must not point at the mutable grid_release "
        "symlink; a Restart=always recovery would then move it onto new code"
    )


@pytest.mark.unit
def test_intelligence_step_checks_the_loaded_working_directory_before_restart() -> None:
    run = _intelligence_restart_run()
    check = run.index("systemctl show -p WorkingDirectory --value grid-intelligence")
    restart = run.index("systemctl restart grid-intelligence")
    reload = run.index("systemctl daemon-reload")
    assert reload < check < restart
