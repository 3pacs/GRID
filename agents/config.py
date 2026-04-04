"""
TradingAgents LLM provider configuration.

Builds a TradingAgents-compatible config dict from GRID settings,
supporting Hyperspace (local), OpenAI, and Anthropic providers.
"""

from __future__ import annotations

from typing import Any

from loguru import logger as log

from config import settings


def scale_debate_rounds(position_size: float = 0.0) -> int:
    """Scale debate rounds based on position size.

    Bigger positions get more debate rounds for higher conviction.
    Linearly interpolates between MIN and MAX rounds based on how
    position_size compares to the scale threshold.

    Parameters:
        position_size: Fraction of capital (0.0-1.0).

    Returns:
        int: Number of debate rounds to use.
    """
    min_rounds = settings.AGENTS_MIN_DEBATE_ROUNDS
    max_rounds = settings.AGENTS_MAX_DEBATE_ROUNDS
    threshold = settings.AGENTS_DEBATE_SCALE_THRESHOLD

    if threshold <= 0 or position_size <= 0:
        return min_rounds

    ratio = min(1.0, position_size / threshold)
    rounds = min_rounds + round(ratio * (max_rounds - min_rounds))
    return max(min_rounds, min(max_rounds, rounds))


def build_agent_config(position_size: float = 0.0) -> dict[str, Any]:
    """Build TradingAgents config from GRID settings.

    Returns a dict suitable for ``TradingAgentsGraph(config=...)``.
    Selects LLM provider based on ``AGENTS_LLM_PROVIDER`` setting.
    Falls back to llama.cpp local if the configured provider is unavailable.

    Supported providers: llamacpp (default), hyperspace, openai, anthropic.

    Parameters:
        position_size: Optional position size for debate round scaling.
    """
    provider = settings.AGENTS_LLM_PROVIDER.lower()
    model = settings.AGENTS_LLM_MODEL

    debate_rounds = scale_debate_rounds(position_size) if position_size > 0 else settings.AGENTS_DEBATE_ROUNDS

    config: dict[str, Any] = {
        "debate_rounds": debate_rounds,
        "online_tools": False,
    }

    if provider == "openai":
        if not settings.AGENTS_OPENAI_API_KEY:
            log.warning("AGENTS_OPENAI_API_KEY not set, falling back to llamacpp")
            return _llamacpp_config(config)
        config["llm_provider"] = "openai"
        config["deep_think_llm"] = model if model != "auto" else "gpt-4o"
        config["quick_think_llm"] = "gpt-4o-mini"
        # Set API key via env var — never pass in config dict to avoid
        # third-party package logging the key at debug level
        import os
        os.environ["OPENAI_API_KEY"] = settings.AGENTS_OPENAI_API_KEY
        config["openai_api_key"] = "set-via-env"
        log.info("Agent LLM: OpenAI ({m})", m=config["deep_think_llm"])

    elif provider == "anthropic":
        if not settings.AGENTS_ANTHROPIC_API_KEY:
            log.warning("AGENTS_ANTHROPIC_API_KEY not set, falling back to llamacpp")
            return _llamacpp_config(config)
        config["llm_provider"] = "anthropic"
        config["deep_think_llm"] = model if model != "auto" else "claude-sonnet-4-6"
        config["quick_think_llm"] = "claude-haiku-4-5-20251001"
        # Set API key via env var — never pass in config dict
        import os
        os.environ["ANTHROPIC_API_KEY"] = settings.AGENTS_ANTHROPIC_API_KEY
        config["anthropic_api_key"] = "set-via-env"
        log.info("Agent LLM: Anthropic ({m})", m=config["deep_think_llm"])

    elif provider == "hyperspace":
        config = _hyperspace_config(config)

    else:
        # Default: llamacpp
        config = _llamacpp_config(config)

    return config


def _llamacpp_config(config: dict[str, Any]) -> dict[str, Any]:
    """Configure TradingAgents to use the local llama.cpp server.

    llama-server exposes an OpenAI-compatible API, so we use the
    openai provider with a custom base URL pointing at localhost:8080.
    Verifies server availability before configuring.
    """
    # Verify llama.cpp server is reachable
    try:
        from llm.router import get_llm, Tier
        client = get_llm(Tier.LOCAL)
        if not client.is_available:
            log.warning("LLM router: no provider available")
    except Exception as exc:
        log.warning("LLM router init failed: {e}", e=str(exc))

    config["llm_provider"] = "openai"
    config["openai_api_key"] = "not-needed"
    config["openai_api_base"] = settings.LLAMACPP_BASE_URL + "/v1"
    model = settings.AGENTS_LLM_MODEL
    config["deep_think_llm"] = model if model != "auto" else settings.LLAMACPP_CHAT_MODEL
    config["quick_think_llm"] = config["deep_think_llm"]
    log.info("Agent LLM: llama.cpp local ({m})", m=config["deep_think_llm"])
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
