"""
A2A Agent Card — JSON capability descriptor.

An Agent Card is a JSON document that describes an agent's identity,
capabilities, supported input/output modes, and authentication requirements.
External agents discover GRID via this card (served at /.well-known/agent.json).

Spec reference: Google A2A Protocol (Linux Foundation open standard).
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any

from loguru import logger as log


@dataclass
class AgentSkill:
    """A specific capability an agent can perform.

    Attributes:
        id: Unique skill identifier.
        name: Human-readable skill name.
        description: What this skill does.
        tags: Searchable tags for discovery.
        examples: Example prompts that invoke this skill.
    """

    id: str
    name: str
    description: str
    tags: list[str] = field(default_factory=list)
    examples: list[str] = field(default_factory=list)


@dataclass
class AgentCard:
    """A2A Agent Card describing an agent's capabilities.

    Attributes:
        name: Agent name.
        description: What this agent does.
        url: Base URL where the agent can be reached.
        version: Agent version string.
        skills: List of capabilities.
        input_modes: Supported input content types.
        output_modes: Supported output content types.
        auth_schemes: Authentication methods accepted.
        payment: Payment configuration (x402 pricing, etc.).
    """

    name: str
    description: str
    url: str
    version: str = "1.0.0"
    skills: list[AgentSkill] = field(default_factory=list)
    input_modes: list[str] = field(default_factory=lambda: ["text/plain", "application/json"])
    output_modes: list[str] = field(default_factory=lambda: ["text/plain", "application/json"])
    auth_schemes: list[str] = field(default_factory=lambda: ["bearer"])
    payment: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to A2A-compliant JSON dict."""
        card = asdict(self)
        if card["payment"] is None:
            del card["payment"]
        return card


def build_grid_agent_card(base_url: str) -> AgentCard:
    """Build the GRID trading intelligence Agent Card.

    This is served at /.well-known/agent.json so external agents
    can discover GRID's capabilities.

    Parameters:
        base_url: Public URL of the GRID API (e.g. https://grid.stepdad.finance).

    Returns:
        AgentCard: GRID's capability descriptor.
    """
    skills = [
        AgentSkill(
            id="forecast",
            name="Time-Series Forecast",
            description=(
                "Generate probabilistic forecasts for financial time series "
                "using Google TimesFM. Returns point predictions with "
                "calibrated confidence intervals."
            ),
            tags=["forecast", "timeseries", "prediction", "finance"],
            examples=[
                "Forecast SPY for the next 7 days",
                "What is the 7-day outlook for gold prices?",
            ],
        ),
        AgentSkill(
            id="oracle_prediction",
            name="Oracle Prediction",
            description=(
                "Generate scored trading predictions with full signal provenance, "
                "anti-signal analysis, and confidence intervals. "
                "Predictions are immutably logged and scored post-expiry."
            ),
            tags=["prediction", "trading", "oracle", "signals"],
            examples=[
                "What is the oracle's view on AAPL this week?",
                "Generate a prediction for BTC with anti-signals",
            ],
        ),
        AgentSkill(
            id="regime_detection",
            name="Market Regime Detection",
            description=(
                "Identify the current market regime (risk-on, risk-off, "
                "transition, crisis) using unsupervised clustering on "
                "macro indicators."
            ),
            tags=["regime", "macro", "clustering", "market"],
            examples=[
                "What regime is the market in right now?",
                "Detect current macro regime",
            ],
        ),
        AgentSkill(
            id="signal_analysis",
            name="Signal Analysis",
            description=(
                "Analyze trading signals across 464+ data sources with "
                "trust scoring, source auditing, and cross-reference "
                "lie detection (government stats vs physical reality)."
            ),
            tags=["signals", "analysis", "trust", "cross-reference"],
            examples=[
                "What signals are active for tech sector?",
                "Cross-reference CPI data with physical indicators",
            ],
        ),
        AgentSkill(
            id="actor_network",
            name="Actor Network Query",
            description=(
                "Query the financial actor network — 495 named actors "
                "with wealth flow tracking, congressional trades, "
                "lobbying disclosure, and campaign finance mapping."
            ),
            tags=["actors", "network", "intelligence", "flows"],
            examples=[
                "Who are the key actors moving energy markets?",
                "Show congressional trades in semiconductors",
            ],
        ),
        AgentSkill(
            id="options_flow",
            name="Options Flow Analysis",
            description=(
                "Analyze unusual options activity, dark pool prints, "
                "dealer gamma positioning, and generate specific trade "
                "recommendations with Kelly-criterion sizing."
            ),
            tags=["options", "flow", "gamma", "trading"],
            examples=[
                "What unusual options flow is showing up today?",
                "Analyze dealer gamma for SPY",
            ],
        ),
    ]

    from config import settings

    card = AgentCard(
        name="GRID Trading Intelligence",
        description=(
            "Systematic multi-agent trading intelligence platform. "
            "Ingests 464+ macro/market signals, performs regime detection, "
            "generates scored predictions, and tracks actor networks."
        ),
        url=base_url,
        version="4.0.0",
        skills=skills,
        input_modes=["text/plain", "application/json"],
        output_modes=["text/plain", "application/json"],
        auth_schemes=["bearer"],
    )

    # Add x402 payment info if enabled
    if settings.X402_ENABLED:
        card.payment = {
            "protocol": "x402",
            "network": settings.X402_NETWORK,
            "token": settings.X402_TOKEN,
            "receiver": settings.X402_RECEIVER_ADDRESS,
            "pricing": {
                "forecast": settings.X402_PRICE_FORECAST,
                "oracle_prediction": settings.X402_PRICE_PREDICTION,
                "signal_analysis": settings.X402_PRICE_SIGNAL,
                "regime_detection": settings.X402_PRICE_REGIME,
                "actor_network": settings.X402_PRICE_ACTOR,
                "options_flow": settings.X402_PRICE_OPTIONS,
            },
        }

    log.info(
        "A2A Agent Card built — {n} skills, url={url}",
        n=len(skills),
        url=base_url,
    )

    return card
