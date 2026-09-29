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

import pydantic
import pytest

import config

FLAGS = [
    "GRID_ENABLE_SCANNER_WEIGHTS_JOB",
    "GRID_ENABLE_BULK_HYPOTHESIS_JOB",
    "GRID_ENABLE_LEGACY_PAPER_TRADING_JOB",
    # Not a Wave 3 flag, but the same default-off job gate on the same
    # blank-value validator (owner decision 2026-09-29): the Taiwan Strait
    # OSINT job in intelligence/scheduler.py.
    "GRID_ENABLE_TAIWAN_STRAIT_OSINT_JOB",
    # Same default-off gate (2026-09-29): LME warehouse job, whose LME URLs
    # sit behind a Cloudflare managed challenge.
    "GRID_ENABLE_LME_WAREHOUSE_JOB",
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


class TestWave3HeldWriterFlagCoercion:
    """A blank/whitespace value must resolve to False (held), never crash
    the app at startup -- the same defect #699 fixed for
    GRID_ALLOW_PAID_LLM, and the same shared before-validator
    (``config.Settings._coerce_blank_bool_flag``) now covers these three
    flags too. See tests/test_paid_llm_gate.py::
    TestGridAllowPaidLlmFlagCoercion for the sibling coverage on
    GRID_ALLOW_PAID_LLM itself."""

    @staticmethod
    def _build(flag: str, value):
        return config.Settings(**{flag: value})

    @pytest.mark.parametrize("flag", FLAGS)
    def test_empty_string_coerces_to_false(self, flag: str) -> None:
        assert getattr(self._build(flag, ""), flag) is False

    @pytest.mark.parametrize("flag", FLAGS)
    def test_whitespace_only_coerces_to_false(self, flag: str) -> None:
        assert getattr(self._build(flag, "   "), flag) is False

    @pytest.mark.parametrize("flag", FLAGS)
    @pytest.mark.parametrize(
        "value", ["0", "false", "False", "FALSE", "no", "NO", "off", "OFF"]
    )
    def test_falsy_strings_coerce_to_false(self, flag: str, value: str) -> None:
        assert getattr(self._build(flag, value), flag) is False

    @pytest.mark.parametrize("flag", FLAGS)
    @pytest.mark.parametrize(
        "value", ["1", "true", "True", "TRUE", "yes", "YES", "on", "ON"]
    )
    def test_truthy_strings_coerce_to_true(self, flag: str, value: str) -> None:
        assert getattr(self._build(flag, value), flag) is True

    @pytest.mark.parametrize("flag", FLAGS)
    def test_garbage_string_still_raises(self, flag: str) -> None:
        with pytest.raises(pydantic.ValidationError):
            self._build(flag, "maybe")
