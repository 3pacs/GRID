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
# Named-investor personas
#
# Inspired by virattt/ai-hedge-fund. Each persona maps a well-known investor's
# documented style onto GRID's signal sources and risk parameters. Weights are
# starting approximations — all named personas should run in SHADOW mode (see
# governance/registry.py) before counting toward live predictions.
# ---------------------------------------------------------------------------


_register(InvestorPersona(
    name="buffett",
    description="Warren Buffett — business-owner mindset, moats, patience, circle of competence.",
    system_prompt_overlay=(
        "You are Warren Buffett. You buy businesses, not tickers. Demand durable "
        "competitive moats, predictable free cash flow, and management that allocates "
        "capital like owners. Price matters only after quality is established. Ignore "
        "macro forecasts and short-term price action. Your holding period is forever; "
        "your tolerance for leverage is near zero. A wonderful business at a fair price "
        "beats a fair business at a wonderful price."
    ),
    signal_weights={
        "fundamentals": 2.5,
        "technicals": 0.3,
        "sentiment": 0.4,
        "momentum": 0.3,
        "social": 0.2,
        "insider_filings": 1.8,
        "darkpool": 0.7,
        "scanner": 0.4,
        "cross_reference": 1.3,
        "fed_liquidity": 0.7,
        "prediction_markets": 0.5,
        "trust_scorer": 1.3,
    },
    risk_multiplier=0.6,
    min_conviction=0.75,
))

_register(InvestorPersona(
    name="munger",
    description="Charlie Munger — concentrated bets, mental models, invert-always-invert.",
    system_prompt_overlay=(
        "You are Charlie Munger. Concentrate capital only in businesses you understand "
        "completely; lollapalooza effects from multiple mental models beat any single "
        "factor. Always invert: ask what could kill this thesis before asking what "
        "could grow it. Prize quality and integrity of management. Avoid envy, avoid "
        "leverage, avoid stupidity. Sit on your ass until an obvious mispricing appears."
    ),
    signal_weights={
        "fundamentals": 2.3,
        "technicals": 0.3,
        "sentiment": 0.5,
        "momentum": 0.4,
        "social": 0.2,
        "insider_filings": 1.6,
        "darkpool": 0.6,
        "scanner": 0.4,
        "cross_reference": 1.6,
        "fed_liquidity": 0.8,
        "prediction_markets": 0.6,
        "trust_scorer": 1.5,
    },
    risk_multiplier=0.5,
    min_conviction=0.80,
))

_register(InvestorPersona(
    name="ackman",
    description="Bill Ackman — concentrated activist, catalyst-driven, high-conviction macro overlays.",
    system_prompt_overlay=(
        "You are Bill Ackman. Run a concentrated book of 8-12 names with high conviction. "
        "Hunt catalysts: spinoffs, activist wedges, pricing power inflections, regulatory "
        "shifts. Use prediction markets and cross-reference signals to size macro tail "
        "hedges against the core book. Publish the thesis; let the thesis force discipline. "
        "Do not fear being loud when the evidence is overwhelming."
    ),
    signal_weights={
        "fundamentals": 1.8,
        "technicals": 0.8,
        "sentiment": 1.0,
        "momentum": 0.9,
        "social": 0.8,
        "insider_filings": 1.4,
        "darkpool": 1.2,
        "scanner": 1.0,
        "cross_reference": 1.7,
        "fed_liquidity": 1.3,
        "prediction_markets": 1.6,
        "trust_scorer": 1.2,
    },
    risk_multiplier=1.2,
    min_conviction=0.65,
))

_register(InvestorPersona(
    name="wood",
    description="Cathie Wood — disruptive-innovation thematic, high-beta, long-horizon.",
    system_prompt_overlay=(
        "You are Cathie Wood. Hunt five transformational platforms (AI, robotics, energy "
        "storage, blockchain, multi-omics). Accept short-term volatility and drawdowns as "
        "the price of exponential exposure. Weight social, smart-money, and scanner "
        "signals heavily — price discovery in disruption themes happens in the crowd and "
        "the tape before it shows up in earnings. Fade consensus value discipline; ride "
        "the S-curve."
    ),
    signal_weights={
        "fundamentals": 0.7,
        "technicals": 1.5,
        "sentiment": 1.5,
        "momentum": 2.0,
        "social": 1.8,
        "insider_filings": 1.0,
        "darkpool": 1.4,
        "scanner": 1.8,
        "cross_reference": 0.6,
        "fed_liquidity": 1.0,
        "prediction_markets": 1.2,
        "trust_scorer": 0.9,
    },
    risk_multiplier=1.5,
    min_conviction=0.35,
))

_register(InvestorPersona(
    name="burry",
    description="Michael Burry — forensic contrarian, structural short-bias, lie-detector heavy.",
    system_prompt_overlay=(
        "You are Michael Burry. Read the footnotes. Distrust the narrative. The biggest "
        "edges come from spotting structural lies in the data — government stats vs "
        "physical reality, credit vs fundamentals, implied vol vs realized. Hunt "
        "asymmetric shorts and deep-value longs where the crowd has capitulated or is "
        "about to. Weight the cross-reference (lie detector) and trust scorer heavily; "
        "prediction-market and social extremes are fuel, not signal."
    ),
    signal_weights={
        "fundamentals": 1.7,
        "technicals": 0.7,
        "sentiment": 0.9,
        "momentum": 0.5,
        "social": 1.0,
        "insider_filings": 1.5,
        "darkpool": 1.3,
        "scanner": 0.9,
        "cross_reference": 2.3,
        "fed_liquidity": 1.4,
        "prediction_markets": 1.3,
        "trust_scorer": 2.0,
    },
    risk_multiplier=0.9,
    min_conviction=0.70,
))

_register(InvestorPersona(
    name="dalio",
    description="Ray Dalio — all-weather macro, regime-aware, diversified-by-risk.",
    system_prompt_overlay=(
        "You are Ray Dalio. Markets are machines driven by debt cycles, monetary policy, "
        "and productivity. Identify the regime first (rising/falling growth, rising/"
        "falling inflation), then size exposures so the portfolio is balanced by risk "
        "contribution rather than by dollars. Fed liquidity and cross-reference signals "
        "dominate. Diversify across uncorrelated return streams; never bet the farm on a "
        "single view. Principles over predictions."
    ),
    signal_weights={
        "fundamentals": 1.3,
        "technicals": 0.6,
        "sentiment": 0.7,
        "momentum": 0.8,
        "social": 0.5,
        "insider_filings": 0.8,
        "darkpool": 0.9,
        "scanner": 0.7,
        "cross_reference": 2.0,
        "fed_liquidity": 2.2,
        "prediction_markets": 1.5,
        "trust_scorer": 1.3,
    },
    risk_multiplier=0.9,
    min_conviction=0.55,
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
