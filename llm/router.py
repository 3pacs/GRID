"""
GRID LLM Router — 3-tier taxonomy.

Provides a single entry point for all LLM calls across the system.
Routes to the correct backend based on task complexity:

  LOCAL  — Formatting, classification, extraction, summarization, tagging,
           JSON/CSV transforms, news parsing, health checks.
           Provider: configured by LLM_LOCAL_PROVIDER

  REASON — Analysis, synthesis, thesis evaluation, regime detection,
           causation narratives, postmortems, forensics, company analysis.
           Provider: configured by LLM_REASON_PROVIDER

  ORACLE — Agent arena debates, high-stakes trading signals, sleuth
           investigations, research agent deep dives.
           Provider: configured by LLM_ORACLE_PROVIDER

Usage:
    from llm.router import get_llm, Tier

    client = get_llm()                    # REASON tier (default)
    client = get_llm(Tier.LOCAL)          # cheap local inference
    client = get_llm(Tier.REASON)         # analysis-grade local
    client = get_llm(Tier.ORACLE)         # cloud heavy-hitter
    client = get_llm(provider="anthropic") # explicit provider override
"""

from __future__ import annotations

import time
from enum import Enum
from typing import Any

import requests
from loguru import logger as log

# Throttle the "No LLM provider available" log so a degraded LLM stack
# (e.g. all local models offline + no OpenRouter key) doesn't spam errors.jsonl
# with hundreds of entries per minute.  One entry per ~5 minutes is enough.
_NO_PROVIDER_LOG_INTERVAL_S = 300.0
_no_provider_last_logged: float = 0.0
_no_provider_suppressed: int = 0


class Tier(str, Enum):
    """LLM task tier — determines which provider to use.

    LOCAL  — formatting, extraction, tagging, transforms, health checks
    REASON — analysis, synthesis, regime detection, postmortems
    ORACLE — debates, high-stakes signals, sleuth investigations
    DEFAULT — alias for REASON (backwards compat during migration)
    """
    LOCAL = "local"
    REASON = "reason"
    ORACLE = "oracle"
    DEFAULT = "reason"  # alias — existing get_llm() calls keep working


# Cache clients to avoid re-init on every call
_client_cache: dict[str, Any] = {}


def _gemma_or_default(settings: Any, key: str, legacy_key: str, default: str) -> str:
    """Return configured provider, with legacy Gemma-primary fallback for unset keys."""
    configured = getattr(settings, key, None) or getattr(settings, legacy_key, None)
    if configured:
        return configured
    if getattr(settings, "GEMMA_PRIMARY", False) and getattr(settings, "GEMMA_ENABLED", False):
        return "gemma"
    return default


def _fallback_chain(tier: Tier, provider: str) -> list[str]:
    """Tier-aware fallback order for keeping node work bounded to sane tasks."""
    if tier == Tier.LOCAL:
        chain = ["llamacpp_quick", "llamacpp_z4", "llamacpp", "gemma", "ollama", "openrouter", "openai"]
    elif tier == Tier.ORACLE:
        chain = ["llamacpp_oracle", "llamacpp", "llamacpp_z4", "gemma", "openrouter", "openai"]
    else:
        chain = ["llamacpp_quick", "llamacpp", "llamacpp_z4", "gemma", "ollama", "openrouter", "openai"]
    return [candidate for candidate in chain if candidate != provider]


def get_llm(
    tier: Tier = Tier.DEFAULT,
    provider: str | None = None,
) -> Any:
    """Return an LLM client for the given tier or provider.

    Falls back through the chain: requested → default → ollama → None.

    Args:
        tier: Task tier (LOCAL, REASON, ORACLE, or DEFAULT alias).
        provider: Explicit provider override (anthropic, ollama, llamacpp, openai, openrouter).

    Returns:
        An LLMClient-compatible object, or a minimal fallback.
    """
    from config import settings

    if provider is None:
        if tier == Tier.LOCAL:
            provider = _gemma_or_default(settings, "LLM_LOCAL_PROVIDER", "LLM_QUICK_PROVIDER", "llamacpp")
        elif tier == Tier.ORACLE:
            provider = getattr(settings, "LLM_ORACLE_PROVIDER", None) \
                or getattr(settings, "LLM_DEEP_PROVIDER", "openrouter")
        else:
            # REASON and DEFAULT both land here
            provider = _gemma_or_default(settings, "LLM_REASON_PROVIDER", "LLM_DEFAULT_PROVIDER", "llamacpp")

    # Return cached client if available and still healthy
    if provider in _client_cache:
        client = _client_cache[provider]
        if getattr(client, "is_available", True):
            return client

    client = _create_client(provider)
    if client is not None and getattr(client, "is_available", True):
        _client_cache[provider] = client
        return client

    for fallback in _fallback_chain(tier, provider):
        if fallback in _client_cache:
            fb_client = _client_cache[fallback]
        else:
            fb_client = _create_client(fallback)
        if fb_client is not None and getattr(fb_client, "is_available", False):
            log.warning(
                "LLM provider {p} unavailable, falling back to {fb}",
                p=provider, fb=fallback,
            )
            _client_cache[fallback] = fb_client
            return fb_client

    # Throttled "no provider available" log. Falling back to _NullClient is
    # graceful, so a per-call ERROR just buries real issues — emit once per
    # ~5min window with the suppressed count, and include tier/provider so
    # the operator can tell which path is dark.
    global _no_provider_last_logged, _no_provider_suppressed
    now = time.time()
    if now - _no_provider_last_logged >= _NO_PROVIDER_LOG_INTERVAL_S:
        if _no_provider_suppressed:
            log.error(
                "No LLM provider available (tier={t}, provider={p}) "
                "[{n} suppressed in last {s:.0f}s]",
                t=tier, p=provider,
                n=_no_provider_suppressed,
                s=now - _no_provider_last_logged,
            )
        else:
            log.error(
                "No LLM provider available (tier={t}, provider={p})",
                t=tier, p=provider,
            )
        _no_provider_last_logged = now
        _no_provider_suppressed = 0
    else:
        _no_provider_suppressed += 1
    return _NullClient()


def _create_client(provider: str) -> Any:
    """Instantiate an LLM client for the given provider."""
    from config import settings

    if provider == "huggingface":
        return _create_hf_client(settings)
    elif provider == "anthropic":
        return _create_anthropic_client(settings)
    elif provider == "ollama":
        return _create_ollama_client(settings)
    elif provider == "llamacpp":
        return _create_llamacpp_client(settings)
    elif provider == "openai":
        return _create_openai_client(settings)
    elif provider == "openrouter":
        return _create_openrouter_client(settings)
    elif provider == "llamacpp_oracle":
        return _create_llamacpp_oracle_client(settings)
    elif provider == "llamacpp_quick":
        return _create_llamacpp_quick_client(settings)
    elif provider == "llamacpp_z4":
        return _create_llamacpp_z4_client(settings)
    elif provider == "gemma":
        return _create_gemma_client(settings)
    elif provider == "bitnet":
        return _create_bitnet_client(settings)
    else:
        log.error("Unknown LLM provider: {p}", p=provider)
        return None


def _create_hf_client(settings: Any) -> Any:
    """Create a HuggingFace Inference API client.

    HF Inference API is OpenAI-compatible, so we use the same
    LlamaCppClient (which talks to any OpenAI-compatible endpoint).
    """
    api_key = settings.HF_API_KEY
    if not api_key:
        log.warning("No HF_API_KEY set — HuggingFace unavailable")
        return None

    return HuggingFaceClient(
        api_key=api_key,
        base_url=settings.HF_BASE_URL,
        model=settings.HF_CHAT_MODEL,
        timeout=settings.HF_TIMEOUT_SECONDS,
    )


def _create_anthropic_client(settings: Any) -> Any:
    """Create a Claude API client wrapped in LLMClient interface."""
    api_key = settings.ANTHROPIC_API_KEY or settings.AGENTS_ANTHROPIC_API_KEY
    if not api_key:
        log.warning("No ANTHROPIC_API_KEY set — Claude unavailable")
        return None

    return AnthropicClient(
        api_key=api_key,
        model=settings.ANTHROPIC_CHAT_MODEL,
        timeout=settings.ANTHROPIC_TIMEOUT_SECONDS,
    )


def _create_ollama_client(settings: Any) -> Any:
    """Create an Ollama client."""
    if not settings.OLLAMA_ENABLED:
        return None
    try:
        from ollama.client import OllamaClient
        return OllamaClient(
            base_url=settings.OLLAMA_BASE_URL,
            model=settings.OLLAMA_CHAT_MODEL,
        )
    except Exception as exc:
        log.debug("Ollama client init failed: {e}", e=str(exc))
        return None


def _create_llamacpp_client(settings: Any) -> Any:
    """Create a llama.cpp client."""
    if not settings.LLAMACPP_ENABLED:
        return None
    try:
        from llamacpp.client import LlamaCppClient
        return LlamaCppClient(
            base_url=settings.LLAMACPP_BASE_URL,
            model=settings.LLAMACPP_CHAT_MODEL,
        )
    except Exception as exc:
        log.debug("llama.cpp client init failed: {e}", e=str(exc))
        return None


def _create_llamacpp_oracle_client(settings: Any) -> Any:
    """Create a llama.cpp client for the ORACLE Blackwell server (port 8081)."""
    if not getattr(settings, "LLAMACPP_ORACLE_ENABLED", False):
        return None
    try:
        from llamacpp.client import LlamaCppClient
        return LlamaCppClient(
            base_url=getattr(settings, "LLAMACPP_ORACLE_BASE_URL", "http://localhost:8081"),
            model=getattr(settings, "LLAMACPP_ORACLE_CHAT_MODEL", "gemma-4-31B-it-Q4_K_M"),
            timeout=getattr(settings, "LLAMACPP_ORACLE_TIMEOUT_SECONDS", 300),
            default_num_predict=getattr(settings, "LLAMACPP_ORACLE_NUM_PREDICT", 10000),
            min_num_predict=getattr(settings, "LLAMACPP_ORACLE_MIN_NUM_PREDICT", 10000),
        )
    except Exception as exc:
        log.debug("llama.cpp oracle client init failed: {e}", e=str(exc))
        return None


def _create_llamacpp_quick_client(settings: Any) -> Any:
    """Create a llama.cpp client for the QUICK-tier remote server (redbox, Qwen3-14B).

    Opt-in via provider="llamacpp_quick" on get_llm() — not added to the automatic
    fallback chain, so enabling LLAMACPP_QUICK_ENABLED alone does not re-route
    existing traffic.
    """
    if not getattr(settings, "LLAMACPP_QUICK_ENABLED", False):
        return None
    try:
        from llamacpp.client import LlamaCppClient
        return LlamaCppClient(
            base_url=getattr(settings, "LLAMACPP_QUICK_BASE_URL", "http://100.126.129.45:8080"),
            model=getattr(settings, "LLAMACPP_QUICK_CHAT_MODEL", "qwen3-14b"),
            timeout=getattr(settings, "LLAMACPP_QUICK_TIMEOUT_SECONDS", 120),
        )
    except Exception as exc:
        log.debug("llama.cpp quick client init failed: {e}", e=str(exc))
        return None


def _create_llamacpp_z4_client(settings: Any) -> Any:
    """Create a llama.cpp client for the gridz4 REASON-tier remote server."""
    if not getattr(settings, "LLAMACPP_Z4_ENABLED", False):
        return None
    try:
        from llamacpp.client import LlamaCppClient
        return LlamaCppClient(
            base_url=getattr(settings, "LLAMACPP_Z4_BASE_URL", "http://gridz4:8080"),
            model=getattr(
                settings,
                "LLAMACPP_Z4_CHAT_MODEL",
                "Qwen3.5-9B-Claude-Opus-Reasoning-v2.Q4_K_M.gguf",
            ),
            timeout=getattr(settings, "LLAMACPP_Z4_TIMEOUT_SECONDS", 180),
        )
    except Exception as exc:
        log.debug("llama.cpp z4 client init failed: {e}", e=str(exc))
        return None


def _create_gemma_client(settings: Any) -> Any:
    """Create a Gemma 3 27B QAT client."""
    if not getattr(settings, "GEMMA_ENABLED", False):
        return None
    try:
        from gemma.client import GemmaClient
        return GemmaClient(
            base_url=settings.GEMMA_BASE_URL,
            model=settings.GEMMA_CHAT_MODEL,
            embed_model=settings.GEMMA_EMBED_MODEL,
            timeout=settings.GEMMA_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        log.debug("Gemma client init failed: {e}", e=str(exc))
        return None


def _create_bitnet_client(settings: Any) -> Any:
    """Create a BitNet 1-bit LLM client (disabled by default)."""
    if not getattr(settings, "BITNET_ENABLED", False):
        return None
    try:
        from bitnet.client import BitNetClient
        return BitNetClient(
            base_url=settings.BITNET_BASE_URL,
            model=settings.BITNET_CHAT_MODEL,
            embed_model=settings.BITNET_EMBED_MODEL,
            timeout=settings.BITNET_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        log.debug("BitNet client init failed: {e}", e=str(exc))
        return None


def _create_openai_client(settings: Any) -> Any:
    """Create an OpenAI client."""
    if not settings.OPENAI_API_KEY:
        return None

    return OpenAIClient(
        api_key=settings.OPENAI_API_KEY,
        base_url=settings.OPENAI_BASE_URL,
        model=settings.OPENAI_CHAT_MODEL,
        timeout=settings.OPENAI_TIMEOUT_SECONDS,
    )


def _create_openrouter_client(settings: Any) -> Any:
    """Create an OpenRouter client (OpenAI-compatible, routes to Claude Sonnet)."""
    api_key = getattr(settings, "OPENROUTER_API_KEY", "")
    if not api_key:
        log.warning("No OPENROUTER_API_KEY set — OpenRouter unavailable")
        return None

    return OpenAIClient(
        api_key=api_key,
        base_url=getattr(settings, "OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"),
        model=getattr(settings, "OPENROUTER_CHAT_MODEL", "anthropic/claude-sonnet-4"),
        timeout=getattr(settings, "OPENROUTER_TIMEOUT_SECONDS", 120),
    )


class _OpenAICompatibleClient:
    """Shared base for OpenAI-compatible REST clients.

    Subclasses must set `_log_prefix` and `_health_provider` as class
    attributes, and may override `_extra_payload_fields()` to inject
    provider-specific payload keys (e.g. ``{"stream": False}``).
    """

    _log_prefix: str = "API"
    _health_provider: str = "openai-compatible"

    def __init__(
        self,
        api_key: str,
        base_url: str,
        model: str,
        timeout: int = 120,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout = timeout
        self.is_available = bool(api_key)
        self._knowledge_cache: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Hook for subclasses — return extra keys merged into the payload.
    # ------------------------------------------------------------------
    def _extra_payload_fields(self) -> dict[str, Any]:
        return {}

    def chat(
        self,
        messages: list[dict[str, str]],
        model: str | None = None,
        temperature: float = 0.3,
        num_predict: int = 4096,
        system_knowledge: list[str] | None = None,
        extra_metadata: dict | None = None,
    ) -> str | None:
        if not self.is_available:
            return None

        payload: dict[str, Any] = {
            "model": model or self.model,
            "messages": messages,
            "max_tokens": num_predict,
            "temperature": temperature,
            **self._extra_payload_fields(),
        }

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        start = time.monotonic()
        try:
            resp = requests.post(
                f"{self.base_url}/chat/completions",
                json=payload,
                headers=headers,
                timeout=self.timeout,
            )
            latency_ms = (time.monotonic() - start) * 1000

            if resp.status_code >= 400:
                log.warning(
                    "{prefix} API {status} ({l:.0f}ms): {body}",
                    prefix=self._log_prefix,
                    status=resp.status_code, l=latency_ms,
                    body=resp.text[:300],
                )
                return None

            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            usage = data.get("usage", {})
            log.debug(
                "{prefix} chat — model={m}, latency={l:.0f}ms, in={i}, out={o}",
                prefix=self._log_prefix,
                m=data.get("model", "?"), l=latency_ms,
                i=usage.get("prompt_tokens", "?"),
                o=usage.get("completion_tokens", "?"),
            )

            # Log to feedback loop for self-learning
            try:
                from llm.feedback_loop import log_llm_call
                sys_msg = next((m["content"] for m in messages if m["role"] == "system"), "")
                usr_msg = next((m["content"] for m in messages if m["role"] == "user"), "")
                log_llm_call(
                    module=self._health_provider,
                    tier="unknown",
                    system_prompt=sys_msg,
                    user_prompt=usr_msg[:2000],
                    output=content[:2000],
                    context_tokens=usage.get("prompt_tokens", 0),
                    output_tokens=usage.get("completion_tokens", 0),
                    latency_ms=int(latency_ms),
                    model=data.get("model", self.model),
                    provider=self._health_provider,
                    metadata=dict(extra_metadata) if extra_metadata else None,
                )
            except Exception:
                pass  # never let logging break inference

            return content

        except Exception as exc:
            latency_ms = (time.monotonic() - start) * 1000
            log.warning(
                "{prefix} API failed ({l:.0f}ms): {err}",
                prefix=self._log_prefix,
                l=latency_ms, err=str(exc),
            )
            return None

    def generate(
        self,
        prompt: str,
        model: str | None = None,
        system: str | None = None,
        temperature: float = 0.3,
        num_predict: int = 4096,
    ) -> str | None:
        messages: list[dict[str, str]] = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})
        return self.chat(messages, model=model, temperature=temperature,
                         num_predict=num_predict)

    def embed(self, texts: list[str], model: str | None = None) -> list[list[float]] | None:
        return None

    def health_check(self) -> dict[str, Any]:
        return {"available": self.is_available, "provider": self._health_provider,
                "model": self.model, "endpoint": self.base_url}

    def list_models(self) -> list[dict[str, Any]]:
        return [{"name": self.model, "provider": self._health_provider}]

    def get_model_names(self) -> list[str]:
        return [self.model]

    def pull_model(self, model_name: str) -> bool:
        return True

    def load_knowledge(self, doc_name: str) -> str | None:
        from knowledge.loader import load_knowledge_doc
        return load_knowledge_doc(self._knowledge_cache, doc_name)

    def load_all_knowledge(self) -> str:
        from knowledge.loader import load_all_knowledge_docs
        return load_all_knowledge_docs(self._knowledge_cache)


class OpenAIClient(_OpenAICompatibleClient):
    """OpenAI API client conforming to the LLMClient protocol."""

    _log_prefix = "OpenAI"
    _health_provider = "openai"

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.openai.com/v1",
        model: str = "gpt-4o-mini",
        timeout: int = 120,
    ) -> None:
        super().__init__(api_key=api_key, base_url=base_url, model=model, timeout=timeout)


class HuggingFaceClient(_OpenAICompatibleClient):
    """HuggingFace Inference API client (OpenAI-compatible)."""

    _log_prefix = "HF"
    _health_provider = "huggingface"

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://router.huggingface.co/together/v1",
        model: str = "Qwen/Qwen2.5-72B-Instruct",
        timeout: int = 120,
    ) -> None:
        super().__init__(api_key=api_key, base_url=base_url, model=model, timeout=timeout)

    def _extra_payload_fields(self) -> dict[str, Any]:
        return {"stream": False}


class AnthropicClient:
    """Claude API client conforming to the LLMClient protocol.

    Uses the Anthropic Messages API directly via requests (no SDK dependency).
    """

    def __init__(
        self,
        api_key: str,
        model: str = "claude-sonnet-4-6",
        timeout: int = 120,
    ) -> None:
        self.api_key = api_key
        self.model = model
        self.timeout = timeout
        self.base_url = "https://api.anthropic.com/v1"
        self.is_available = bool(api_key)
        self._knowledge_cache: dict[str, str] = {}

    def chat(
        self,
        messages: list[dict[str, str]],
        model: str | None = None,
        temperature: float = 0.3,
        num_predict: int = 4096,
        system_knowledge: list[str] | None = None,
        extra_metadata: dict | None = None,
    ) -> str | None:
        """Send chat to Claude API."""
        if not self.is_available:
            return None

        # Extract system message if present
        system_text = ""
        chat_messages = []
        for m in messages:
            if m["role"] == "system":
                system_text += m["content"] + "\n"
            else:
                chat_messages.append(m)

        if not chat_messages:
            return None

        payload: dict[str, Any] = {
            "model": model or self.model,
            "max_tokens": num_predict,
            "temperature": temperature,
            "messages": chat_messages,
        }
        if system_text.strip():
            payload["system"] = system_text.strip()

        headers = {
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }

        start = time.monotonic()
        try:
            resp = requests.post(
                f"{self.base_url}/messages",
                json=payload,
                headers=headers,
                timeout=self.timeout,
            )
            latency_ms = (time.monotonic() - start) * 1000

            if resp.status_code >= 400:
                log.warning(
                    "Claude API {status} ({l:.0f}ms): {body}",
                    status=resp.status_code, l=latency_ms,
                    body=resp.text[:300],
                )
                return None

            data = resp.json()
            content_blocks = data.get("content", [])
            text = "".join(
                b["text"] for b in content_blocks if b.get("type") == "text"
            )

            usage = data.get("usage", {})
            log.debug(
                "Claude chat — model={m}, latency={l:.0f}ms, in={i}, out={o}",
                m=data.get("model", "?"),
                l=latency_ms,
                i=usage.get("input_tokens", "?"),
                o=usage.get("output_tokens", "?"),
            )

            # Log to feedback loop for self-learning
            try:
                from llm.feedback_loop import log_llm_call
                log_llm_call(
                    module="anthropic",
                    tier="ORACLE",
                    system_prompt=system_text[:2000],
                    user_prompt=chat_messages[0]["content"][:2000] if chat_messages else "",
                    output=text[:2000],
                    context_tokens=usage.get("input_tokens", 0),
                    output_tokens=usage.get("output_tokens", 0),
                    latency_ms=int(latency_ms),
                    model=data.get("model", self.model),
                    provider="anthropic",
                    metadata=dict(extra_metadata) if extra_metadata else None,
                )
            except Exception:
                pass

            return text

        except Exception as exc:
            latency_ms = (time.monotonic() - start) * 1000
            log.warning(
                "Claude API failed ({l:.0f}ms): {err}",
                l=latency_ms, err=str(exc),
            )
            return None

    def generate(
        self,
        prompt: str,
        model: str | None = None,
        system: str | None = None,
        temperature: float = 0.3,
        num_predict: int = 4096,
    ) -> str | None:
        """Single-turn generation."""
        messages: list[dict[str, str]] = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})
        return self.chat(messages, model=model, temperature=temperature,
                         num_predict=num_predict)

    def embed(
        self,
        texts: list[str],
        model: str | None = None,
    ) -> list[list[float]] | None:
        """Anthropic doesn't offer embeddings — return None."""
        return None

    def health_check(self) -> dict[str, Any]:
        return {
            "available": self.is_available,
            "provider": "anthropic",
            "model": self.model,
            "endpoint": self.base_url,
        }

    def list_models(self) -> list[dict[str, Any]]:
        return [{"name": self.model, "provider": "anthropic"}]

    def get_model_names(self) -> list[str]:
        return [self.model]

    def pull_model(self, model_name: str) -> bool:
        return True  # Cloud models don't need pulling

    def load_knowledge(self, doc_name: str) -> str | None:
        from knowledge.loader import load_knowledge_doc
        return load_knowledge_doc(self._knowledge_cache, doc_name)

    def load_all_knowledge(self) -> str:
        from knowledge.loader import load_all_knowledge_docs
        return load_all_knowledge_docs(self._knowledge_cache)


class _NullClient:
    """Fallback client when no LLM is available."""

    is_available = False

    def chat(self, *args: Any, **kwargs: Any) -> None:
        return None

    def generate(self, *args: Any, **kwargs: Any) -> None:
        return None

    def embed(self, *args: Any, **kwargs: Any) -> None:
        return None

    def health_check(self) -> dict[str, Any]:
        return {"available": False, "provider": "none"}

    def list_models(self) -> list[dict[str, Any]]:
        return []

    def get_model_names(self) -> list[str]:
        return []

    def pull_model(self, model_name: str) -> bool:
        return False

    def load_knowledge(self, doc_name: str) -> str | None:
        return None

    def load_all_knowledge(self) -> str:
        return ""
