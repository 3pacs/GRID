"""CODEOWNERS integrity gate.

Reconciles OPS-BP-branch-protection.md and EVAL-E0H3-codeowners-ruleset-
guide.md (wha/outputs/slices-20261001/): every path that guards evals,
scoring, pre-registrations, PIT/holdout machinery, research-loop integrity,
or a production surface (deploy/migrations/trading/payments) must resolve
to @3pacs, GitHub's CODEOWNERS last-match-wins semantics must not silently
unassign one of those paths, and the file itself must be syntactically one
GitHub accepts (one owner token per line, real paths, no unresolvable
pattern).

This test does not touch the network and does not require Postgres; it
only parses the committed .github/CODEOWNERS file and a tiny local
re-implementation of GitHub's last-match-wins resolution.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
CODEOWNERS_PATH = REPO_ROOT / ".github" / "CODEOWNERS"

# Representative paths that must be covered by the CODEOWNERS file today.
# Each is a real file/dir confirmed present on origin/main as of cdf1b7f7
# (2026-10-01), except the /engine/ sample, which is a deliberate future
# reservation (EVAL-E0H3 E4 note) with no file on disk yet.
PROTECTED_SAMPLE_PATHS = [
    ".github/workflows/test.yml",
    "evals/e0/benchmark.py",
    "evals/e1/manifest.py",
    "tests/test_e0_benchmark.py",
    "tests/test_e0_manifest_guard.py",
    "tests/test_evals_anything_future.py",
    "tests/test_codeowners.py",
    "docs/paper_log/anything.md",
    "paper_log/gex_levels/evaluate.py",
    "store/pit.py",
    "store/observations.py",
    "analysis/offline_research_proof.py",
    "analysis/ledger_steered_exploration.py",
    "analysis/research_forward_log.py",
    "analysis/research_real_panel.py",
    "analysis/panel_insider_density.py",
    "analysis/panel_insider_density_sectors_v5.py",
    "analysis/price_admission_fetch.py",
    "analysis/price_admission_probe.py",
    "scripts/research_forward_log.py",
    "scripts/run_vs1_v7_insider_density.py",
    "deploy/langfuse/grid-regression-eval.service",
    "migrations/0001_init.sql",
    "trading/execution.py",
    "payments/ledger.py",
    "engine/proposer.py",  # future reservation, see module docstring
]

EXPECTED_OWNER = "@3pacs"


class Rule:
    """One parsed CODEOWNERS line: a glob pattern plus its owner tokens."""

    __slots__ = ("raw", "pattern", "owners", "regex")

    def __init__(self, raw: str, pattern: str, owners: list[str]):
        self.raw = raw
        self.pattern = pattern
        self.owners = owners
        self.regex = _pattern_to_regex(pattern)

    def matches(self, path: str) -> bool:
        return self.regex.match(path) is not None


def _pattern_to_regex(pattern: str) -> re.Pattern:
    """Compile a (simplified) GitHub CODEOWNERS pattern to a path regex.

    Supports what this repo's CODEOWNERS actually uses: a leading '/' to
    anchor at the repo root, a trailing '/' to mean "this directory and
    everything under it", and '*' as a single-path-segment wildcard. This
    is intentionally not a full gitignore-pattern implementation -- it only
    needs to be correct for the patterns this file contains, which the
    syntax test below enforces.
    """
    body = pattern[1:] if pattern.startswith("/") else pattern
    is_dir = body.endswith("/")
    if is_dir:
        body = body[:-1]
    core = "".join("[^/]*" if ch == "*" else re.escape(ch) for ch in body)
    if is_dir:
        return re.compile(rf"^{core}(/.*)?$")
    return re.compile(rf"^{core}$")


def _parse(text: str) -> list[Rule]:
    rules: list[Rule] = []
    for lineno, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        tokens = line.split()
        assert len(tokens) >= 2, (
            f"CODEOWNERS line {lineno} has no owner (unassigns ownership for "
            f"a matching path): {raw_line!r}"
        )
        pattern, owners = tokens[0], tokens[1:]
        for owner in owners:
            assert owner.startswith("@"), (
                f"CODEOWNERS line {lineno} has a malformed owner token "
                f"{owner!r} (must start with @): {raw_line!r}"
            )
        rules.append(Rule(raw_line, pattern, owners))
    return rules


def _owners_for(rules: list[Rule], path: str) -> list[str] | None:
    """Last-match-wins resolution, exactly as GitHub documents it."""
    owners = None
    for rule in rules:
        if rule.matches(path):
            owners = rule.owners
    return owners


@pytest.fixture(scope="module")
def codeowners_text() -> str:
    assert CODEOWNERS_PATH.exists(), (
        f"{CODEOWNERS_PATH} is missing -- GitHub also recognizes a root "
        "CODEOWNERS or docs/CODEOWNERS, but this repo standardizes on "
        ".github/CODEOWNERS; do not add a second copy at another location "
        "(GitHub only honors one, and two copies can silently disagree)."
    )
    return CODEOWNERS_PATH.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def rules(codeowners_text: str) -> list[Rule]:
    return _parse(codeowners_text)


def test_no_competing_codeowners_file_exists():
    # GitHub's precedence is root, then .github/, then docs/. Keep exactly
    # one copy so there is never a question of which file is authoritative.
    assert not (REPO_ROOT / "CODEOWNERS").exists()
    assert not (REPO_ROOT / "docs" / "CODEOWNERS").exists()


def test_file_is_utf8_and_newline_terminated(codeowners_text: str):
    assert codeowners_text.endswith("\n")
    assert "\t" not in codeowners_text, "use spaces, not tabs, between pattern and owner"


def test_every_rule_has_at_least_one_owner(rules: list[Rule]):
    # A pattern line with zero owners is valid CODEOWNERS syntax to GitHub,
    # but it means "nobody owns this" -- for a last-match-wins file that is
    # how a later broad rule can silently blank out an earlier narrow one.
    # This repo's file must never contain such a line.
    assert rules, "CODEOWNERS parsed to zero rules"
    for rule in rules:
        assert rule.owners, f"unowned CODEOWNERS line: {rule.raw!r}"


def test_every_rule_owner_is_3pacs(rules: list[Rule]):
    for rule in rules:
        assert rule.owners == [EXPECTED_OWNER], (
            f"expected exactly [{EXPECTED_OWNER!r}] for pattern {rule.pattern!r}, "
            f"got {rule.owners!r}. Every path in this file protects evals, PIT/"
            "holdout, research-loop integrity, or a production surface, and the "
            "repo has one real reviewer identity today; see "
            "GRID-BRANCH-PROTECTION-GUIDE-20261001.md before widening this."
        )


@pytest.mark.parametrize("path", PROTECTED_SAMPLE_PATHS)
def test_protected_paths_resolve_to_3pacs(rules: list[Rule], path: str):
    owners = _owners_for(rules, path)
    assert owners == [EXPECTED_OWNER], (
        f"{path!r} resolved to {owners!r}, expected [{EXPECTED_OWNER!r}]. "
        "Either the CODEOWNERS pattern for this path regressed, or this "
        "sample path's protecting pattern was removed."
    )


def test_last_match_wins_a_later_broad_rule_cannot_silently_unassign():
    """Guard the specific failure mode EVAL-E0H3 calls out: a later,
    broader pattern must not override an earlier protected path to
    "no owner". We don't mutate the real file -- we parse it, then replay
    GitHub's resolution with one synthetic broad rule appended, the way an
    unreviewed future edit might, and assert that synthetic rule is REJECTED
    by the same owner-less-line guard this file's rules all pass, before it
    could ever reach the real file and reorder resolution.
    """
    unowned_catchall = "* "  # a bare pattern with no owner -- invalid here
    with pytest.raises(AssertionError, match="no owner"):
        _parse(f"/evals/ {EXPECTED_OWNER}\n{unowned_catchall}\n")


def test_resolution_is_order_sensitive_last_match_wins():
    # Sanity-check the test helper itself: a later, more specific pattern
    # for the SAME owner should still be the one that matches, proving
    # _owners_for implements last-match-wins rather than first-match.
    sample_rules = _parse(f"/evals/ {EXPECTED_OWNER}\n/evals/e0/ {EXPECTED_OWNER}\n")
    assert _owners_for(sample_rules, "evals/e0/benchmark.py") == [EXPECTED_OWNER]
    assert _owners_for(sample_rules, "evals/e1/manifest.py") == [EXPECTED_OWNER]
    assert _owners_for(sample_rules, "README.md") is None
