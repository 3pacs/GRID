"""
Dual-LLM task router for GRID.

Routes inference tasks to the most cost-effective model tier:
  - QUICK: screening, classification, formatting → cheap/fast model
  - DEEP: analysis, debate, risk assessment → expensive/thorough model

Inspired by TradingAgents' deep_think_llm / quick_think_llm split.
The existing get_client() singleton is untouched for backwards compatibility.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Any

from loguru import logger as log


class TaskComplexity(str, Enum):
    QUICK = "QUICK"
    DEEP = "DEEP"


# Keywords that indicate a quick/simple task
_QUICK_PATTERNS: list[str] = [
    r"\bformat\b", r"\blist\b", r"\bclassify\b", r"\bcategorize\b",
    r"\bextract\b", r"\bparse\b", r"\bsummariz\b", r"\bconvert\b",
    r"\btranslat\b", r"\bcount\b", r"\bfilter\b", r"\bsort\b",
    r"\bvalidat\b", r"\bnormali[sz]\b", r"\bjson\b", r"\bcsv\b",
    r"\blabel\b", r"\btag\b",
]

# Keywords that indicate a deep/complex task
_DEEP_PATTERNS: list[str] = [
    r"\banalys[ei]\b", r"\bdebat\b", r"\brisk\b", r"\brecommend\b",
    r"\bstrateg\b", r"\bforecast\b", r"\bpredict\b", r"\bexplain\b",
    r"\breason\b", r"\bevaluat\b", r"\bassess\b", r"\bcompare\b",
    r"\bcritiq\b", r"\bsynthesize\b", r"\bweigh\b", r"\bcounterargument\b",
    r"\bdiagnos\b", r"\binvestigat\b", r"\bpostmortem\b", r"\bregime\b",
    r"\bcross.?referenc\b", r"\bcalibrat\b",
]

_QUICK_RE = re.compile("|".join(_QUICK_PATTERNS), re.IGNORECASE)
_DEEP_RE = re.compile("|".join(_DEEP_PATTERNS), re.IGNORECASE)


def classify_task(prompt: str) -> TaskComplexity:
    """Classify a prompt's complexity tier using keyword heuristics.

    Parameters:
        prompt: The user/system prompt text to classify.

    Returns:
        TaskComplexity.QUICK or TaskComplexity.DEEP
    """
    # Count pattern matches
    quick_hits = len(_QUICK_RE.findall(prompt))
    deep_hits = len(_DEEP_RE.findall(prompt))

    # Bias toward DEEP for safety — if unclear, use the better model
    if deep_hits > 0:
        return TaskComplexity.DEEP
    if quick_hits > 0:
        return TaskComplexity.QUICK

    # Default: DEEP (safer — avoids bad analysis from weak model)
    return TaskComplexity.DEEP


class TaskRouter:
    """Routes LLM tasks to appropriate model tier.

    Maintains two client references (quick and deep) with fallback
    to the other tier and ultimately to the existing singleton.

    Parameters:
        quick_client: Client for fast/cheap tasks. May be None.
        deep_client: Client for complex analysis tasks. May be None.
    """

    def __init__(
        self,
        quick_client: Any = None,
        deep_client: Any = None,
    ) -> None:
        self._quick = quick_client
        self._deep = deep_client

    @property
    def quick_client(self) -> Any:
        return self._quick

    @property
    def deep_client(self) -> Any:
        return self._deep

    def route(
        self,
        messages: list[dict[str, str]],
        complexity: TaskComplexity | None = None,
        temperature: float = 0.3,
        num_predict: int = 2000,
        **kwargs: Any,
    ) -> str | None:
        """Route a chat request to the appropriate model tier.

        Parameters:
            messages: Chat messages (role/content dicts).
            complexity: Explicit complexity override. If None, auto-classifies.
            temperature: Sampling temperature.
            num_predict: Max tokens to generate.

        Returns:
            str: Model response, or None if all backends unavailable.
        """
        if complexity is None:
            # Auto-classify from the last user message
            prompt_text = " ".join(
                m["content"] for m in messages if m["role"] in ("user", "system")
            )
            complexity = classify_task(prompt_text)

        # Select primary and fallback clients
        if complexity == TaskComplexity.QUICK:
            primary, fallback = self._quick, self._deep
            tier_label = "QUICK"
        else:
            primary, fallback = self._deep, self._quick
            tier_label = "DEEP"

        # Try primary
        if primary is not None and getattr(primary, "is_available", False):
            log.debug("Router: {tier} → {m}", tier=tier_label, m=getattr(primary, "model", "unknown"))
            result = primary.chat(
                messages=messages,
                temperature=temperature,
                num_predict=num_predict,
                **kwargs,
            )
            if result is not None:
                return result
            log.debug("Router: primary ({tier}) returned None, trying fallback", tier=tier_label)

        # Try fallback tier
        if fallback is not None and getattr(fallback, "is_available", False):
            alt_tier = "DEEP" if tier_label == "QUICK" else "QUICK"
            log.debug("Router: fallback {tier} → {m}", tier=alt_tier, m=getattr(fallback, "model", "unknown"))
            result = fallback.chat(
                messages=messages,
                temperature=temperature,
                num_predict=num_predict,
                **kwargs,
            )
            if result is not None:
                return result

        # Final fallback: existing singleton
        log.debug("Router: both tiers unavailable, falling back to get_client() singleton")
        from ollama.client import get_client
        client = get_client()
        if client is not None and getattr(client, "is_available", False):
            return client.chat(
                messages=messages,
                temperature=temperature,
                num_predict=num_predict,
                **kwargs,
            )

        log.warning("Router: no LLM backend available")
        return None


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_router_instance: TaskRouter | None = None


def get_router() -> TaskRouter:
    """Return a cached TaskRouter singleton.

    Builds quick and deep clients based on config settings.
    """
    global _router_instance
    if _router_instance is not None:
        return _router_instance

    from config import settings

    if not settings.LLM_ROUTER_ENABLED:
        # Router disabled — wrap get_client() as both tiers
        from ollama.client import get_client
        client = get_client()
        _router_instance = TaskRouter(quick_client=client, deep_client=client)
        return _router_instance

    quick_client = None
    deep_client = None

    quick_provider = settings.LLM_QUICK_PROVIDER.lower()
    deep_provider = settings.LLM_DEEP_PROVIDER.lower()

    # Build quick client
    if quick_provider == "llamacpp":
        from llamacpp.client import get_client as get_llamacpp
        quick_client = get_llamacpp()
    elif quick_provider == "ollama":
        from ollama.client import OllamaClient
        quick_client = OllamaClient(
            base_url=settings.OLLAMA_BASE_URL,
            model=settings.OLLAMA_CHAT_MODEL,
            timeout=settings.OLLAMA_TIMEOUT_SECONDS,
        )
    elif quick_provider == "openai":
        from ollama.client import OpenAIClient
        key = settings.OPENAI_API_KEY or settings.AGENTS_OPENAI_API_KEY
        if key:
            quick_client = OpenAIClient(
                api_key=key,
                base_url=settings.OPENAI_BASE_URL,
                model="gpt-4o-mini",
                timeout=settings.OPENAI_TIMEOUT_SECONDS,
            )

    # Build deep client
    if deep_provider == "anthropic":
        key = settings.AGENTS_ANTHROPIC_API_KEY
        if key:
            from ollama.client import OpenAIClient
            deep_client = OpenAIClient(
                api_key=key,
                base_url="https://api.anthropic.com/v1",
                model="claude-sonnet-4-6",
                timeout=180,
            )
    elif deep_provider == "openai":
        key = settings.OPENAI_API_KEY or settings.AGENTS_OPENAI_API_KEY
        if key:
            from ollama.client import OpenAIClient
            deep_client = OpenAIClient(
                api_key=key,
                base_url=settings.OPENAI_BASE_URL,
                model="gpt-4o",
                timeout=settings.OPENAI_TIMEOUT_SECONDS,
            )
    elif deep_provider == "llamacpp":
        from llamacpp.client import get_client as get_llamacpp
        deep_client = get_llamacpp()
    elif deep_provider == "hyperspace":
        from ollama.client import OpenAIClient
        deep_client = OpenAIClient(
            api_key="not-needed",
            base_url=settings.HYPERSPACE_BASE_URL,
            model=settings.HYPERSPACE_CHAT_MODEL if settings.HYPERSPACE_CHAT_MODEL != "auto" else "hermes",
            timeout=settings.HYPERSPACE_TIMEOUT_SECONDS,
        )

    _router_instance = TaskRouter(quick_client=quick_client, deep_client=deep_client)

    log.info(
        "LLM router initialised — quick={q} ({qa}), deep={d} ({da})",
        q=quick_provider,
        qa="available" if quick_client and getattr(quick_client, "is_available", False) else "unavailable",
        d=deep_provider,
        da="available" if deep_client and getattr(deep_client, "is_available", False) else "unavailable",
    )

    return _router_instance
