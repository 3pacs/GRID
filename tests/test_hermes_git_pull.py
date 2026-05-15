"""Tests for ``scripts.hermes_operator.git_pull``.

Regression coverage for the 2026-05-13 safety patch. Pre-fix the
function used ``git pull --rebase`` with a non-rebase fallback, which:

  * Could silently rewrite operator-applied local commits via the rebase
    replay.
  * Would happily merge ``main`` into a feature branch when the rebase
    fallback fired, polluting the operator's in-progress work.

Post-fix: skip when on a non-target branch, use ``--ff-only`` on the
target branch, never fall back to a destructive merge.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

import scripts.hermes_operator as ho


# ── Helpers ────────────────────────────────────────────────────────────────


def _mock_git(responses: dict[tuple[str, ...], tuple[int, str]]):
    """Build a fake _git() that returns the configured tuple for each
    argv it receives.

    ``responses`` keys are tuples of the git args (without "git"),
    values are (returncode, combined_output).
    """

    def fake(args, cwd=None):  # signature matches scripts.hermes_operator._git
        key = tuple(args)
        if key in responses:
            return responses[key]
        # Unmatched calls explode loudly so tests stay honest about what
        # the function under test is actually doing.
        raise AssertionError(f"unexpected _git call: {args!r}")

    return fake


# ── Sanity / no-op paths ───────────────────────────────────────────────────


def test_git_pull_skips_when_sync_disabled():
    with patch.object(ho, "GIT_SYNC_ENABLED", False):
        assert ho.git_pull() == {"skipped": "disabled"}


def test_git_pull_skips_when_not_a_worktree():
    responses = {
        ("rev-parse", "--is-inside-work-tree"): (128, "fatal: not a git repository"),
    }
    with patch.object(ho, "GIT_SYNC_ENABLED", True), patch.object(ho, "_git", _mock_git(responses)):
        out = ho.git_pull()
    assert out["skipped"] == "not_a_git_worktree"


# ── The load-bearing new behaviour: non-target branch is skipped ──────────


def test_git_pull_skips_on_non_main_branch():
    # The pre-fix code would rebase main onto our feature branch here —
    # the exact wedge that ate cherry-picks. The new code refuses.
    responses = {
        ("rev-parse", "--is-inside-work-tree"): (0, "true"),
        ("rev-parse", "--abbrev-ref", "HEAD"): (0, "feat/some-hotfix"),
    }
    with patch.object(ho, "GIT_SYNC_ENABLED", True), \
         patch.object(ho, "GIT_BRANCH", "main"), \
         patch.object(ho, "_git", _mock_git(responses)):
        out = ho.git_pull()

    assert out == {"skipped": "non_target_branch", "branch": "feat/some-hotfix"}


# ── Main branch + fast-forward succeeds ───────────────────────────────────


def test_git_pull_fast_forwards_on_main():
    responses = {
        ("rev-parse", "--is-inside-work-tree"): (0, "true"),
        ("rev-parse", "--abbrev-ref", "HEAD"): (0, "main"),
        ("pull", "--ff-only", "origin", "main"): (0, "Updating abc..def\nFast-forward\n"),
    }
    with patch.object(ho, "GIT_SYNC_ENABLED", True), \
         patch.object(ho, "GIT_BRANCH", "main"), \
         patch.object(ho, "GIT_REMOTE", "origin"), \
         patch.object(ho, "_git", _mock_git(responses)):
        out = ho.git_pull()

    assert out["status"] == "ok"
    assert "Fast-forward" in out["output"]


# ── Main branch + can't fast-forward → bail, don't merge ──────────────────


def test_git_pull_does_not_merge_when_ff_only_fails():
    # Local main has its own commits not in origin/main; ff-only refuses.
    # Pre-fix this was where the dangerous `pull` (without --rebase)
    # fallback fired and merged the remote into the local divergent tree.
    # The new code returns failed_non_ff and leaves the tree alone.
    responses = {
        ("rev-parse", "--is-inside-work-tree"): (0, "true"),
        ("rev-parse", "--abbrev-ref", "HEAD"): (0, "main"),
        ("pull", "--ff-only", "origin", "main"): (
            1,
            "fatal: Not possible to fast-forward, aborting.",
        ),
    }
    with patch.object(ho, "GIT_SYNC_ENABLED", True), \
         patch.object(ho, "GIT_BRANCH", "main"), \
         patch.object(ho, "GIT_REMOTE", "origin"), \
         patch.object(ho, "_git", _mock_git(responses)):
        out = ho.git_pull()

    assert out["status"] == "failed_non_ff"
    assert "fast-forward" in out["output"].lower()
    # The assertion in _mock_git would have triggered if any other _git
    # call (a fallback `pull` without --ff-only) had been attempted.


# ── Dirty working tree on main → ff-only refuses, no merge attempted ──────


def test_git_pull_does_not_attempt_fallback_when_dirty_on_main():
    responses = {
        ("rev-parse", "--is-inside-work-tree"): (0, "true"),
        ("rev-parse", "--abbrev-ref", "HEAD"): (0, "main"),
        ("pull", "--ff-only", "origin", "main"): (
            1,
            "error: Your local changes to the following files would be overwritten by merge",
        ),
    }
    with patch.object(ho, "GIT_SYNC_ENABLED", True), \
         patch.object(ho, "GIT_BRANCH", "main"), \
         patch.object(ho, "GIT_REMOTE", "origin"), \
         patch.object(ho, "_git", _mock_git(responses)):
        out = ho.git_pull()

    assert out["status"] == "failed_non_ff"
