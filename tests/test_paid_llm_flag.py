"""``GRID_ALLOW_PAID_LLM`` must be a real, declared setting.

``llm/router.py::_paid_llm_allowed`` reads ``getattr(settings,
"GRID_ALLOW_PAID_LLM", False)``. With ``extra="ignore"`` on ``Settings``,
pydantic-settings only binds environment variables to *declared* fields, so
until the field existed the documented escape hatch could never be opened
(LEVER-PACKAGE.md §4.1 / §7 T0.7).
"""
from __future__ import annotations

import config


def test_flag_is_declared_and_defaults_off() -> None:
    assert "GRID_ALLOW_PAID_LLM" in config.Settings.model_fields
    assert config.Settings.model_fields["GRID_ALLOW_PAID_LLM"].default is False
    assert config.settings.GRID_ALLOW_PAID_LLM is False


def test_flag_binds_from_environment(monkeypatch) -> None:
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.setenv("GRID_ALLOW_PAID_LLM", "true")
    fresh = config.Settings(_env_file=None)
    assert fresh.GRID_ALLOW_PAID_LLM is True


def test_router_gate_honours_declared_flag(monkeypatch) -> None:
    from llm import router

    monkeypatch.setattr(config.settings, "GRID_ALLOW_PAID_LLM", True)
    assert router._paid_llm_allowed() is True
    monkeypatch.setattr(config.settings, "GRID_ALLOW_PAID_LLM", False)
    assert router._paid_llm_allowed() is False
