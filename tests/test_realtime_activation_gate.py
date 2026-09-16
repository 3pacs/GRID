"""Tests for scripts/realtime_activation_gate.sh.

Mirrors tests/test_scheduler_activation_gate.py for the same reason: this
gate is what .github/workflows/deploy.yml consults before restarting
grid-realtime, and it must not claim to have verified anything about
current candle-boundary or WebSocket-message state on the caller's
behalf -- only that a human explicitly acknowledged the interruption risk.
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
        result = _run()
        assert result.returncode == 1
        assert "REFUSED" in result.stderr

    @pytest.mark.parametrize("value", ["True", "TRUE", "1", "yes", "", "garbage"])
    def test_refuses_anything_other_than_exact_lowercase_true(self, value):
        result = _run(value)
        assert result.returncode == 1

    def test_refusal_message_does_not_claim_an_automated_safety_check(self):
        result = _run("false")
        combined = (result.stdout + result.stderr).lower()
        assert "verified" not in combined
        assert "cannot prove" in combined

    def test_does_not_error_under_set_euo_pipefail_with_no_args(self):
        result = _run()
        assert result.returncode == 1
