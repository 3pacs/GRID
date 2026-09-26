"""AUTORESEARCH_ENABLED off-by-default research gate (2026-09-19).

Dependency for #579 (fable/autoresearch-subtype-20260919): that PR fixes the
data-load query so a gate-passing autoresearch run can proceed past
_load_research_context for the first time. AUTORESEARCH_ENABLED existed in
config.py since before this change but was a dead flag -- nothing in the
codebase read it (verified by grep), so it never gated anything. This test
file locks in that it is now:

  1. Checked FIRST in scripts/hermes_fixers.py::maybe_run_autoresearch,
     before the 12h cooldown / dry_run checks -- so a disabled deployment
     never touches state.last_autoresearch and never calls
     scripts.autoresearch.run_autoresearch (no research_run row, no
     hypothesis_registry row, nothing).
  2. Default False, so deploying #579 alone cannot start a research run --
     the controller must explicitly set AUTORESEARCH_ENABLED=true.
  3. A normal pydantic-settings env var: AUTORESEARCH_ENABLED=true flips it.

No DB, no network -- everything here is monkeypatched or a plain
Settings()/OperatorState() construction. Run with:

    DB_PASSWORD=x PYTHONUTF8=1 python -m pytest tests/test_autoresearch_gate.py -q
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import scripts.autoresearch as autoresearch  # noqa: E402
import scripts.hermes_fixers as hermes_fixers  # noqa: E402
from scripts.hermes_health import OperatorState  # noqa: E402


# ── maybe_run_autoresearch: disabled by default ───────────────────────────

class TestMaybeRunAutoresearchDisabledByDefault:
    def test_skips_without_touching_run_autoresearch_or_last_autoresearch(self, monkeypatch):
        """With default settings (AUTORESEARCH_ENABLED=False), the gate
        check must fire before anything else in maybe_run_autoresearch --
        proven here by monkeypatching run_autoresearch to raise: if the
        gate did not short-circuit first, this test would fail with that
        exception instead of asserting the skipped result.
        """
        from config import settings

        monkeypatch.setattr(settings, "AUTORESEARCH_ENABLED", False)

        def _boom(**kwargs):
            raise AssertionError("run_autoresearch must not be called when disabled")

        monkeypatch.setattr(autoresearch, "run_autoresearch", _boom)

        state = OperatorState()
        state.last_autoresearch = None  # would otherwise pass the 12h cooldown too

        result = hermes_fixers.maybe_run_autoresearch(state, dry_run=False)

        assert result == {"status": "skipped", "reason": "disabled"}
        # Untouched -- a disabled run must not look like a run that happened.
        assert state.last_autoresearch is None

    def test_leaves_a_previously_set_last_autoresearch_unchanged(self, monkeypatch):
        from config import settings
        from datetime import datetime, timezone

        monkeypatch.setattr(settings, "AUTORESEARCH_ENABLED", False)
        monkeypatch.setattr(
            autoresearch, "run_autoresearch",
            lambda **kwargs: (_ for _ in ()).throw(AssertionError("must not be called")),
        )

        state = OperatorState()
        sentinel = datetime(2026, 1, 1, tzinfo=timezone.utc)
        state.last_autoresearch = sentinel

        result = hermes_fixers.maybe_run_autoresearch(state, dry_run=False)

        assert result == {"status": "skipped", "reason": "disabled"}
        assert state.last_autoresearch is sentinel


# ── maybe_run_autoresearch: enabled proceeds as before ─────────────────────

class TestMaybeRunAutoresearchEnabledProceeds:
    def test_enabled_reaches_run_autoresearch(self, monkeypatch):
        from config import settings

        monkeypatch.setattr(settings, "AUTORESEARCH_ENABLED", True)

        captured: dict[str, Any] = {}

        def _fake_run_autoresearch(**kwargs):
            captured.update(kwargs)
            return {"status": "ok", "iterations": 2, "iterations_run": 2, "passed": False}

        monkeypatch.setattr(autoresearch, "run_autoresearch", _fake_run_autoresearch)

        state = OperatorState()
        state.last_autoresearch = None

        result = hermes_fixers.maybe_run_autoresearch(state, dry_run=False)

        assert captured, "run_autoresearch should have been called"
        assert result["status"] == "ok"
        assert state.last_autoresearch is not None
        assert state.hypotheses_tested == 2

    def test_enabled_still_honours_the_12h_cooldown(self, monkeypatch):
        """The gate is a new, earlier check -- it must not disturb the
        pre-existing 12h-since-last-run behaviour once enabled."""
        from config import settings
        from datetime import datetime, timezone

        monkeypatch.setattr(settings, "AUTORESEARCH_ENABLED", True)
        monkeypatch.setattr(
            autoresearch, "run_autoresearch",
            lambda **kwargs: (_ for _ in ()).throw(AssertionError("must not be called within cooldown")),
        )

        state = OperatorState()
        state.last_autoresearch = datetime.now(timezone.utc)  # just ran

        result = hermes_fixers.maybe_run_autoresearch(state, dry_run=False)

        assert result is None


# ── config.Settings default ────────────────────────────────────────────────

class TestSettingsDefault:
    def test_default_is_false_when_env_var_unset(self, monkeypatch):
        monkeypatch.delenv("AUTORESEARCH_ENABLED", raising=False)

        from config import Settings

        assert Settings().AUTORESEARCH_ENABLED is False

    def test_env_var_true_flips_it(self, monkeypatch):
        monkeypatch.setenv("AUTORESEARCH_ENABLED", "true")

        from config import Settings

        assert Settings().AUTORESEARCH_ENABLED is True
