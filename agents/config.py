"""
TradingAgents LLM provider configuration.

Builds a TradingAgents-compatible config dict from GRID settings,
supporting Hyperspace (local), OpenAI, and Anthropic providers.
"""

from __future__ import annotations

from typing import Any

from loguru import logger as log

from config import settings


def build_agent_config() -> dict[str, Any]:
    """Build TradingAgents config from GRID settings.

    Returns a dict suitable for ``TradingAgentsGraph(config=...)``.
    Selects LLM provider based on ``AGENTS_LLM_PROVIDER`` setting.
    Falls back to Hyperspace if the configured provider is unavailable.
    """
    provider = settings.AGENTS_LLM_PROVIDER.lower()
    model = settings.AGENTS_LLM_MODEL

    config: dict[str, Any] = {
        "debate_rounds": settings.AGENTS_DEBATE_ROUNDS,
        "online_tools": False,
    }

    if provider == "openai":
        if not settings.AGENTS_OPENAI_API_KEY:
            log.warning("AGENTS_OPENAI_API_KEY not set, falling back to hyperspace")
            return _hyperspace_config(config)
        config["llm_provider"] = "openai"
        config["deep_think_llm"] = model if model != "auto" else "gpt-4o"
        config["quick_think_llm"] = "gpt-4o-mini"
        config["openai_api_key"] = settings.AGENTS_OPENAI_API_KEY
        log.info("Agent LLM: OpenAI ({m})", m=config["deep_think_llm"])

    elif provider == "anthropic":
        if not settings.AGENTS_ANTHROPIC_API_KEY:
            log.warning("AGENTS_ANTHROPIC_API_KEY not set, falling back to hyperspace")
            return _hyperspace_config(config)
        config["llm_provider"] = "anthropic"
        config["deep_think_llm"] = model if model != "auto" else "claude-sonnet-4-6"
        config["quick_think_llm"] = "claude-haiku-4-5-20251001"
        config["anthropic_api_key"] = settings.AGENTS_ANTHROPIC_API_KEY
        log.info("Agent LLM: Anthropic ({m})", m=config["deep_think_llm"])

    else:
        config = _hyperspace_config(config)

    return config


def _hyperspace_config(config: dict[str, Any]) -> dict[str, Any]:
    """Configure TradingAgents to use the local Hyperspace node."""
    config["llm_provider"] = "openai"
    config["openai_api_key"] = "not-needed"
    config["openai_api_base"] = settings.HYPERSPACE_BASE_URL
    model = settings.AGENTS_LLM_MODEL
    config["deep_think_llm"] = model if model != "auto" else settings.HYPERSPACE_CHAT_MODEL
    config["quick_think_llm"] = config["deep_think_llm"]
    log.info("Agent LLM: Hyperspace local ({m})", m=config["deep_think_llm"])
    return config
