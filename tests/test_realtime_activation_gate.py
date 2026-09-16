"""Tests for scripts/realtime_activation_gate.sh.

This gate is what .github/workflows/deploy.yml consults before restarting
grid-realtime, now that activation is opt-in (see
tests/test_deploy_restarts_realtime.py for the workflow-shape guards, and
docs/TODO-REALTIME-CANDLE-CORRECTNESS.md for why: the candle merge across a
restart has tested, known-unresolved correctness gaps in close-selection and
Yahoo volume reconstruction). The gate does not claim to have verified
anything about those gaps being safe -- it only checks for an explicit human
acknowledgment of the risk, the same shape as
scripts/scheduler_activation_gate.sh.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "realtime_activation_gate.sh"


def _run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", str(SCRIPT), *args],
        capture_output=True,
        text=True,
        timeout=10,
    )


class TestRealtimeActivationGate:
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

    def test_refusal_message_names_the_actual_known_gap(self):
        # This gate exists for a specific, traced reason (candle merge
        # correctness across a restart) -- not a generic "restarts are
        # scary" disclaimer. The refusal should say so, not just refuse.
        result = _run("false")
        combined = (result.stdout + result.stderr).lower()
        assert "candle" in combined
        assert "close" in combined or "volume" in combined

    def test_does_not_error_under_set_euo_pipefail_with_no_args(self):
        # The script itself runs under `set -euo pipefail`; `${1:-false}`
        # must not trip that on a missing positional argument.
        result = _run()
        assert result.returncode == 1
