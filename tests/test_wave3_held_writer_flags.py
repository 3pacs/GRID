"""Wave 3 held-writer job flags must be real, declared settings that default
OFF (owner decision 2026-09-28, GRID-WAVE3-HELD-WRITERS-TRIAGE-20260927.md
§6 "Held-category writers still running in grid-intelligence").

``intelligence/scheduler.py`` reads each flag via
``getattr(_s, "GRID_ENABLE_..._JOB", False)``. With ``extra="ignore"`` on
``Settings``, pydantic-settings only binds environment variables to
*declared* fields, so — as with ``GRID_ALLOW_PAID_LLM`` before it — the
flag must be declared on the model or the documented opt-in path silently
does nothing. See ``tests/test_scheduler.py`` for the scheduler-registration
behavior these flags gate.
"""
from __future__ import annotations

import pytest

import config

FLAGS = [
    "GRID_ENABLE_SCANNER_WEIGHTS_JOB",
    "GRID_ENABLE_BULK_HYPOTHESIS_JOB",
    "GRID_ENABLE_LEGACY_PAPER_TRADING_JOB",
]


@pytest.mark.parametrize("flag", FLAGS)
def test_flag_is_declared_and_defaults_off(flag: str) -> None:
    assert flag in config.Settings.model_fields
    assert config.Settings.model_fields[flag].default is False
    assert getattr(config.settings, flag) is False


@pytest.mark.parametrize("flag", FLAGS)
def test_flag_binds_from_environment(monkeypatch, flag: str) -> None:
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.setenv(flag, "true")
    fresh = config.Settings(_env_file=None)
    assert getattr(fresh, flag) is True
