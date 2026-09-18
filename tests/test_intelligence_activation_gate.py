"""Tests for scripts/intelligence_activation_gate.sh.

Mirrors tests/test_scheduler_activation_gate.py exactly, for the same
reason: this gate is what .github/workflows/deploy.yml consults before
restarting grid-intelligence, and it makes no claim about verifying
current idleness -- only that a human explicitly acknowledged the
interruption risk. These tests exercise that decision in isolation from
the workflow itself.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "intelligence_activation_gate.sh"


def _run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", str(SCRIPT), *args],
        capture_output=True,
        text=True,
        timeout=10,
    )


class TestIntelligenceActivationGate:
    def test_proceeds_when_acknowledged(self):
        result = _run("true")
        assert result.returncode == 0
        assert "PROCEED" in result.stdout

    def test_refuses_when_not_acknowledged(self):
        result = _run("false")
        assert result.returncode == 1
        assert "REFUSED" in result.stderr

    def test_refuses_when_argument_missing(self):
        # deploy.yml always passes the input, but the script must not
        # treat "no argument at all" as implicit acknowledgment.
        result = _run()
        assert result.returncode == 1
        assert "REFUSED" in result.stderr

    @pytest.mark.parametrize("value", ["True", "TRUE", "1", "yes", "", "garbage"])
    def test_refuses_anything_other_than_exact_lowercase_true(self, value):
        # GitHub Actions renders a boolean workflow_dispatch input as the
        # literal string "true"/"false" -- guard against any other spelling
        # silently being treated as acknowledgment.
        result = _run(value)
        assert result.returncode == 1

    def test_refusal_message_does_not_claim_an_automated_safety_check(self):
        # The whole point of this gate: it must not imply it verified
        # anything about current intelligence-loop state on the caller's
        # behalf.
        result = _run("false")
        combined = (result.stdout + result.stderr).lower()
        assert "verified" not in combined
        assert "cannot prove current idleness" in combined or "cannot prove" in combined

    def test_refusal_message_mentions_the_no_catchup_risk(self):
        # Specific to this gate (not present in the scheduler one): the
        # schedule library silently skips a missed daily/weekly occurrence
        # rather than deferring it. A human acknowledging "interruption"
        # without knowing this could reasonably assume a missed job just
        # runs late.
        result = _run("false")
        combined = (result.stdout + result.stderr).lower()
        assert "skip" in combined

    def test_does_not_error_under_set_euo_pipefail_with_no_args(self):
        # The script itself runs under `set -euo pipefail`; `${1:-false}`
        # must not trip that on a missing positional argument.
        result = _run()
        assert result.returncode == 1
