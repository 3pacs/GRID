"""Tests for agents.personas — investor persona system."""

from __future__ import annotations

import pytest

from agents.personas import (
    InvestorPersona,
    format_persona_context,
    get_persona,
    list_personas,
)


# ---------------------------------------------------------------------------
# get_persona
# ---------------------------------------------------------------------------


class TestGetPersona:
    """Tests for get_persona()."""

    @pytest.mark.parametrize("name", [
        "balanced",
        "value_investor",
        "momentum_trader",
        "macro_strategist",
        "contrarian",
    ])
    def test_returns_correct_persona(self, name: str) -> None:
        persona = get_persona(name)
        assert persona.name == name

    def test_unknown_name_defaults_to_balanced(self) -> None:
        persona = get_persona("nonexistent_persona")
        assert persona.name == "balanced"

    def test_empty_string_defaults_to_balanced(self) -> None:
        persona = get_persona("")
        assert persona.name == "balanced"


# ---------------------------------------------------------------------------
# list_personas
# ---------------------------------------------------------------------------


class TestListPersonas:
    """Tests for list_personas()."""

    def test_returns_all_five(self) -> None:
        names = list_personas()
        assert len(names) == 5

    def test_contains_expected_names(self) -> None:
        names = set(list_personas())
        expected = {"balanced", "value_investor", "momentum_trader", "macro_strategist", "contrarian"}
        assert names == expected


# ---------------------------------------------------------------------------
# format_persona_context
# ---------------------------------------------------------------------------


class TestFormatPersonaContext:
    """Tests for format_persona_context()."""

    def test_includes_system_prompt(self) -> None:
        persona = get_persona("balanced")
        ctx = format_persona_context(persona)
        assert persona.system_prompt_overlay in ctx

    def test_includes_persona_name(self) -> None:
        persona = get_persona("value_investor")
        ctx = format_persona_context(persona)
        assert "VALUE_INVESTOR" in ctx

    def test_includes_risk_multiplier(self) -> None:
        persona = get_persona("momentum_trader")
        ctx = format_persona_context(persona)
        assert "1.3x" in ctx

    def test_includes_min_conviction(self) -> None:
        persona = get_persona("contrarian")
        ctx = format_persona_context(persona)
        assert "70%" in ctx

    def test_includes_signal_weights(self) -> None:
        persona = get_persona("macro_strategist")
        ctx = format_persona_context(persona)
        assert "cross_reference: 2.0x" in ctx


# ---------------------------------------------------------------------------
# Persona validity
# ---------------------------------------------------------------------------


class TestPersonaValidity:
    """Validate all personas have well-formed fields."""

    @pytest.fixture(params=[
        "balanced",
        "value_investor",
        "momentum_trader",
        "macro_strategist",
        "contrarian",
    ])
    def persona(self, request: pytest.FixtureRequest) -> InvestorPersona:
        return get_persona(request.param)

    def test_signal_weights_all_positive(self, persona: InvestorPersona) -> None:
        for source, weight in persona.signal_weights.items():
            assert weight > 0, f"{persona.name}: {source} weight must be > 0, got {weight}"

    def test_risk_multiplier_positive(self, persona: InvestorPersona) -> None:
        assert persona.risk_multiplier > 0, f"{persona.name}: risk_multiplier must be > 0"

    def test_min_conviction_in_range(self, persona: InvestorPersona) -> None:
        assert 0.0 <= persona.min_conviction <= 1.0, (
            f"{persona.name}: min_conviction must be 0.0-1.0, got {persona.min_conviction}"
        )

    def test_has_description(self, persona: InvestorPersona) -> None:
        assert len(persona.description) > 0

    def test_has_system_prompt_overlay(self, persona: InvestorPersona) -> None:
        assert len(persona.system_prompt_overlay) > 0

    def test_has_signal_weights(self, persona: InvestorPersona) -> None:
        assert len(persona.signal_weights) > 0
