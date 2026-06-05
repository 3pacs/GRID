"""Hermes — GRID's analyst bridge to a fine-tuned grid-analyst-v1.

A hosted OpenAI reasoning model wrapped with spend accounting and a local
analyst fallback. ``prompts.SYSTEM`` is the fine-tune target for the future
specialised model; Hermes is the bridge to it.

Typical use::

    from intelligence.hermes import HermesAgent

    agent = HermesAgent()
    result = agent.analyze("What is the lever behind today's USDJPY move?")
    print(result.source, result.text)
"""

from __future__ import annotations

from .agent import AnalysisResult, HermesAgent
from .config import HermesConfig, load_hermes_config
from .prompts import SYSTEM, SYSTEM_VERSION, build_messages
from .provider import HermesProvider, HermesResponse, TokenUsage
from .spend import SpendLedger

__all__ = [
    "AnalysisResult",
    "HermesAgent",
    "HermesConfig",
    "HermesProvider",
    "HermesResponse",
    "SpendLedger",
    "SYSTEM",
    "SYSTEM_VERSION",
    "TokenUsage",
    "build_messages",
    "load_hermes_config",
]
