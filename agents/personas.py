"""
Investor persona system for TradingAgents.

Defines preset investor personas that shape how the agent system
weighs different signal sources, manages risk, and frames its
analysis. Each persona injects a system prompt overlay and
configures signal weight multipliers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from loguru import logger as log


@dataclass
class InvestorPersona:
    """An investor persona that shapes agent behaviour.

    Attributes:
        name: Unique persona identifier.
        description: Human-readable description of the investing style.
        system_prompt_overlay: Injected into the agent's system prompt.
        signal_weights: source_type -> multiplier (1.0 = normal weight).
        risk_multiplier: Scales position sizing (1.0 = normal).
        min_conviction: Minimum conviction score to act (0.0-1.0).
    """

    name: str
    description: str
    system_prompt_overlay: str
    signal_weights: dict[str, float] = field(default_factory=dict)
    risk_multiplier: float = 1.0
    min_conviction: float = 0.3


# ---------------------------------------------------------------------------
# Built-in personas
# ---------------------------------------------------------------------------

_PERSONAS: dict[str, InvestorPersona] = {}


def _register(persona: InvestorPersona) -> None:
    _PERSONAS[persona.name] = persona


_register(InvestorPersona(
    name="balanced",
    description="Default balanced analyst — equal weights across all signal sources.",
    system_prompt_overlay=(
        "You are a balanced macro analyst. Weigh all signal sources equally. "
        "Look for convergence across fundamentals, technicals, and sentiment "
        "before making a recommendation."
    ),
    signal_weights={
        "fundamentals": 1.0,
        "technicals": 1.0,
        "sentiment": 1.0,
        "momentum": 1.0,
        "social": 1.0,
        "insider_filings": 1.0,
        "darkpool": 1.0,
        "scanner": 1.0,
        "cross_reference": 1.0,
        "fed_liquidity": 1.0,
        "prediction_markets": 1.0,
        "trust_scorer": 1.0,
    },
    risk_multiplier=1.0,
    min_conviction=0.3,
))

_register(InvestorPersona(
    name="value_investor",
    description="Deep value investor — fundamentals and insider buying over momentum.",
    system_prompt_overlay=(
        "You are a deep value investor in the tradition of Benjamin Graham. "
        "Focus on intrinsic value, margin of safety, and insider buying patterns. "
        "Ignore short-term momentum and social media noise. Only recommend "
        "positions where price is significantly below fundamental value."
    ),
    signal_weights={
        "fundamentals": 2.0,
        "technicals": 0.8,
        "sentiment": 0.6,
        "momentum": 0.5,
        "social": 0.3,
        "insider_filings": 1.8,
        "darkpool": 1.0,
        "scanner": 0.8,
        "cross_reference": 1.2,
        "fed_liquidity": 1.0,
        "prediction_markets": 0.8,
        "trust_scorer": 1.2,
    },
    risk_multiplier=0.7,
    min_conviction=0.6,
))

_register(InvestorPersona(
    name="momentum_trader",
    description="Momentum trader — technicals, dark pool flow, and options activity.",
    system_prompt_overlay=(
        "You are a momentum trader. Focus on technical indicators, dark pool "
        "flow, options activity, and price action. Don't fight the tape. "
        "Ride trends and cut losers quickly."
    ),
    signal_weights={
        "fundamentals": 0.5,
        "technicals": 2.0,
        "sentiment": 1.2,
        "momentum": 1.8,
        "social": 1.0,
        "insider_filings": 0.8,
        "darkpool": 1.8,
        "scanner": 1.5,
        "cross_reference": 0.6,
        "fed_liquidity": 0.8,
        "prediction_markets": 1.0,
        "trust_scorer": 0.8,
    },
    risk_multiplier=1.3,
    min_conviction=0.4,
))

_register(InvestorPersona(
    name="macro_strategist",
    description="Global macro strategist — cross-reference, Fed liquidity, prediction markets.",
    system_prompt_overlay=(
        "You are a global macro strategist. Focus on the lie detector "
        "cross-reference signals, Fed liquidity conditions, prediction market "
        "probabilities, and regime transitions. Individual stock technicals "
        "are noise \u2014 macro drives everything."
    ),
    signal_weights={
        "fundamentals": 1.2,
        "technicals": 0.5,
        "sentiment": 0.8,
        "momentum": 0.6,
        "social": 0.5,
        "insider_filings": 0.8,
        "darkpool": 0.8,
        "scanner": 0.6,
        "cross_reference": 2.0,
        "fed_liquidity": 1.8,
        "prediction_markets": 1.5,
        "trust_scorer": 1.2,
    },
    risk_multiplier=1.0,
    min_conviction=0.5,
))

_register(InvestorPersona(
    name="contrarian",
    description="Contrarian investor — inverts consensus, focuses on trust score divergences.",
    system_prompt_overlay=(
        "You are a contrarian investor. When consensus is strongly bullish, "
        "look for reasons to be bearish, and vice versa. Focus on trust score "
        "divergences, prediction market extremes, and sentiment extremes as "
        "reversal signals. The crowd is usually right \u2014 except at turning points."
    ),
    signal_weights={
        "fundamentals": 1.0,
        "technicals": 0.8,
        "sentiment": 1.2,
        "momentum": 0.7,
        "social": 1.3,
        "insider_filings": 1.2,
        "darkpool": 1.0,
        "scanner": 1.0,
        "cross_reference": 1.5,
        "fed_liquidity": 1.0,
        "prediction_markets": 1.5,
        "trust_scorer": 2.0,
    },
    risk_multiplier=0.8,
    min_conviction=0.7,
))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_persona(name: str) -> InvestorPersona:
    """Return persona by name, defaulting to balanced for unknown names.

    Parameters:
        name: Persona identifier (e.g. 'balanced', 'value_investor').

    Returns:
        The matching InvestorPersona, or the balanced persona if *name*
        is not recognised.
    """
    persona = _PERSONAS.get(name)
    if persona is None:
        log.warning(
            "Unknown persona '{n}' — falling back to balanced",
            n=name,
        )
        persona = _PERSONAS["balanced"]
    return persona


def list_personas() -> list[str]:
    """Return the names of all available personas."""
    return list(_PERSONAS.keys())


def format_persona_context(persona: InvestorPersona) -> str:
    """Format persona overlay for injection into agent prompts.

    Produces a block of text containing the persona's system prompt
    overlay, its signal weight configuration, risk multiplier, and
    minimum conviction threshold.

    Parameters:
        persona: The investor persona to format.

    Returns:
        A multi-line string ready for prompt injection.
    """
    weights_lines = "\n".join(
        f"  - {source}: {weight:.1f}x"
        for source, weight in sorted(persona.signal_weights.items())
    )
    return (
        f"=== INVESTOR PERSONA: {persona.name.upper()} ===\n"
        f"{persona.system_prompt_overlay}\n\n"
        f"Signal weight multipliers:\n{weights_lines}\n\n"
        f"Risk multiplier: {persona.risk_multiplier:.1f}x\n"
        f"Minimum conviction to act: {persona.min_conviction:.0%}\n"
        f"=== END PERSONA ===\n"
    )
