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
    QUICK  — short, latency-sensitive classification/extraction calls
             (news sentiment, tagging) on a small fast model; primary is
             threadripper's 8–14B llama-server (``llamacpp_threadripper``)
    REASON — analysis, synthesis, regime detection, postmortems
    ORACLE — debates, high-stakes signals
    RESEARCH — the research desk: Sleuth investigations, the research agent,
             postmortems, backtest review. Primary is x99-hex's 27B
             (``llamacpp_x99``, 2 slots), so it stays off the 3090's single
             oracle slot (plan item 0.2, 2026-11-09)
    DEFAULT — alias for REASON (backwards compat during migration)
    """
    LOCAL = "local"
    QUICK = "quick"
    REASON = "reason"
    ORACLE = "oracle"
    RESEARCH = "research"
    BATCH = "batch"
    DEFAULT = "reason"  # alias — existing get_llm() calls keep working


# Cache clients to avoid re-init on every call
_client_cache: dict[str, Any] = {}
_PROVIDER_BACKOFF_UNTIL: dict[str, float] = {}
_PROVIDER_BACKOFF_REASONS: dict[str, str] = {}
_PROVIDER_AUTH_BACKOFF_S = 1800.0
_PROVIDER_RATE_BACKOFF_S = 300.0


def _mark_provider_unavailable(provider: str, reason: str, cooldown_s: float) -> None:
    """Temporarily suppress providers that are known-bad for this process."""
    until = time.time() + cooldown_s
    _PROVIDER_BACKOFF_UNTIL[provider] = until
    _PROVIDER_BACKOFF_REASONS[provider] = reason
    log.warning(
        "LLM provider {p} disabled for {s:.0f}s: {r}",
        p=provider, s=cooldown_s, r=reason,
    )


def _provider_in_backoff(provider: str) -> bool:
    until = _PROVIDER_BACKOFF_UNTIL.get(provider)
    if not until:
        return False
    if time.time() >= until:
        _PROVIDER_BACKOFF_UNTIL.pop(provider, None)
        _PROVIDER_BACKOFF_REASONS.pop(provider, None)
        return False
    return True


# ---------------------------------------------------------------------------
# Provider health policy (2026-11-06).
#
# A provider whose last call was refused — its health probe failed at client
# construction, or an embed() call raised / came back empty — is skipped for
# LLM_PROVIDER_COOLDOWN_SECONDS. Every local-node client is also built with a
# LLM_CONNECT_TIMEOUT_SECONDS TCP connect timeout. Together these keep a dead
# node from stalling every call: before this, a black-holed koala cost 5 s per
# get_llm()/embed() call because the router rebuilt (and re-probed) every
# unavailable Ollama client on each pass through the chain.
# ---------------------------------------------------------------------------
_DEFAULT_CONNECT_TIMEOUT_S = 2.0
_DEFAULT_PROVIDER_COOLDOWN_S = 120.0
# Providers already warned as unreachable in this process; cleared when the
# provider answers again so the next outage warns once more.
_UNREACHABLE_WARNED: set[str] = set()


def _connect_timeout(settings: Any, key: str = "LLM_CONNECT_TIMEOUT_SECONDS") -> float:
    """TCP connect timeout for local-node clients, clamped to (0, 30] s.

    ``key`` lets a node behind Tailscale's DERP relay (1.4–2 s RTT from
    grid-svr: threadripper, x99-hex) use its own, longer connect timeout; a
    blank/invalid per-node value falls back to the global one.
    """
    if key != "LLM_CONNECT_TIMEOUT_SECONDS":
        raw = getattr(settings, key, None)
        try:
            if raw is not None and raw != "" and float(raw) > 0:
                return min(float(raw), 30.0)
        except (TypeError, ValueError):
            pass
        return _connect_timeout(settings)
    try:
        value = float(getattr(settings, "LLM_CONNECT_TIMEOUT_SECONDS", _DEFAULT_CONNECT_TIMEOUT_S))
    except (TypeError, ValueError):
        return _DEFAULT_CONNECT_TIMEOUT_S
    if not value > 0:  # also rejects NaN
        return _DEFAULT_CONNECT_TIMEOUT_S
    return min(value, 30.0)


def _provider_cooldown_s() -> float:
    """Seconds a refused provider is skipped, clamped to [0, 3600]."""
    from config import settings

    try:
        value = float(getattr(settings, "LLM_PROVIDER_COOLDOWN_SECONDS", _DEFAULT_PROVIDER_COOLDOWN_S))
    except (TypeError, ValueError):
        return _DEFAULT_PROVIDER_COOLDOWN_S
    if not value >= 0:  # also rejects NaN
        return _DEFAULT_PROVIDER_COOLDOWN_S
    return min(value, 3600.0)


def _mark_provider_refused(provider: str, reason: str) -> None:
    """Cool down a provider whose last call was refused.

    Warns once per outage (repeats go to debug) — a node that stays down must
    not refill errors.jsonl, and it is an operational condition, not a bug.
    """
    cooldown_s = _provider_cooldown_s()
    if cooldown_s <= 0:
        return
    _PROVIDER_BACKOFF_UNTIL[provider] = time.time() + cooldown_s
    _PROVIDER_BACKOFF_REASONS[provider] = reason
    if provider in _UNREACHABLE_WARNED:
        log.debug("LLM provider {p} still refusing ({r})", p=provider, r=reason)
        return
    _UNREACHABLE_WARNED.add(provider)
    log.warning(
        "LLM provider {p} refused its last call ({r}) — skipped for {s:.0f}s "
        "(further refusals logged at debug until it answers)",
        p=provider, r=reason, s=cooldown_s,
    )


def _mark_provider_answered(provider: str) -> None:
    """Re-arm the once-per-outage warning after a provider answers."""
    if provider in _UNREACHABLE_WARNED:
        _UNREACHABLE_WARNED.discard(provider)
        log.info("LLM provider {p} answering again", p=provider)


def _build_client(provider: str) -> Any:
    """``_create_client`` plus the refusal cooldown.

    A factory that returns ``None`` is disabled/unconfigured (no socket was
    opened), so it is not cooled down. A client object whose health probe
    failed (``is_available`` false) is a refusal and is.
    """
    client = _create_client(provider)
    if client is None:
        return None
    if getattr(client, "is_available", False):
        _mark_provider_answered(provider)
    else:
        _mark_provider_refused(provider, "health probe failed")
    return client


# (provider, fallback) pairs already warned in this process. A dead primary
# (LLM_LOCAL_PROVIDER=llamacpp_quick pointing at a retired host) made every
# get_llm() call log "LLM provider ... unavailable, falling back to ..."
# (76x/day on grid-hermes). Warn once per pair per outage; repeats go to
# debug, and the pair re-arms when the primary is seen available again.
_FALLBACK_WARNED: set[tuple[str, str]] = set()


def _fallback_warning(provider: str, fallback: str | None) -> None:
    """Log a provider fallback once per outage (``fallback=None`` re-arms)."""
    if fallback is None:
        for pair in [p for p in _FALLBACK_WARNED if p[0] == provider]:
            _FALLBACK_WARNED.discard(pair)
        return
    pair = (provider, fallback)
    if pair in _FALLBACK_WARNED:
        log.debug("LLM provider {p} unavailable, using {fb}", p=provider, fb=fallback)
        return
    _FALLBACK_WARNED.add(pair)
    log.warning(
        "LLM provider {p} unavailable, falling back to {fb} "
        "(further fallbacks for this pair logged at debug until {p} recovers)",
        p=provider, fb=fallback,
    )


def supported_chat_kwargs(client: Any, **wanted: Any) -> dict[str, Any]:
    """The subset of ``wanted`` keyword arguments ``client.chat`` accepts.

    Lets callers pass provider-specific options (``enable_thinking`` on the
    llama.cpp client) without a TypeError on clients that do not take them
    (Ollama, cloud clients, _NullClient, test doubles).
    """
    import inspect

    try:
        params = inspect.signature(client.chat).parameters
    except (TypeError, ValueError, AttributeError):
        return {}
    if any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()):
        # **kwargs swallows anything; only forward what a real llama.cpp
        # style signature would name explicitly.
        return {}
    return {k: v for k, v in wanted.items() if k in params}


def _gemma_or_default(settings: Any, key: str, legacy_key: str, default: str) -> str:
    """Return configured provider, with legacy Gemma-primary fallback for unset keys."""
    configured = getattr(settings, key, None) or getattr(settings, legacy_key, None)
    if configured:
        return configured
    if getattr(settings, "GEMMA_PRIMARY", False) and getattr(settings, "GEMMA_ENABLED", False):
        return "gemma"
    return default


def _fallback_chain(tier: Tier, provider: str) -> list[str]:
    """Tier-aware fallback order for keeping node work bounded to sane tasks.

    NOTE: ``llamacpp_batch`` is intentionally OPT-IN only. It is fast at
    reasoning but slow (~5 tok/sec on CPU), so we never auto-add it to
    interactive tier fallbacks — interactive callers would block on it.
    Only ``Tier.BATCH`` reaches it.

    NOTE: ``gemma`` was dropped from every chain on 2026-09-10. Its only
    endpoint was ``GEMMA_BASE_URL=http://localhost:8080`` — the CPU-only
    llama.cpp unit that the operator retired ("there should be no CPU-only
    Qwens").

    NOTE (2026-11-06 fleet audit): ``ollama_koala``, ``ollama_ocr``,
    ``ollama_z400`` and ``ollama_panda`` are gone from every chain — none of
    those nodes answers (koala and ocr-node cost 5 s timeouts per call).
    ``llamacpp_quick`` is gone too: its default host (redbox :8080) is dead
    and the live .env points it at gridz4:8080, which ``llamacpp_z4`` already
    covers. See :data:`_RETIRED_PROVIDERS`.

    Real topology behind the remaining names:

    * ``llamacpp_z4``     gridz4:8080 llama-server, Qwen3.8-27B — primary.
    * ``llamacpp_oracle`` grid-svr RTX 3090, 127.0.0.1:18081 — the only
      independent GPU: ORACLE's fallback and REASON's second choice.
    * ``llamacpp``        100.75.185.36:8086, a socket proxy to gridz4:8086 —
      same box as ``llamacpp_z4``, so it only helps if one gridz4 port dies.
    * ``ollama``          grid-svr's own Ollama (shares the 3090).
    * ``llamacpp_threadripper`` threadripper's RTX A2000 llama-server, a
      small fast model (W2-9, 2026-11-06) — QUICK's primary and LOCAL's
      first fallback after gridz4. Off unless LLAMACPP_THREADRIPPER_ENABLED.
    * ``llamacpp_x99``    x99-hex :8086, Qwen3.8-27B (S5, 2026-11-06) — a
      third independent 27B, but across the DERP relay, so it sits after
      ``llamacpp`` in REASON/ORACLE: the live .env has LLAMACPP_Z4_ENABLED=
      false, which makes the gridz4 :8086 proxy the real gridz4 path, and an
      on-site gridz4 should answer before a relayed node. Off unless
      LLAMACPP_X99_ENABLED.
    """
    if tier == Tier.QUICK:
        chain = ["llamacpp_threadripper", "llamacpp_z4", "llamacpp", "ollama",
                 "gemini", "openrouter", "openai"]
    elif tier == Tier.LOCAL:
        chain = ["llamacpp_z4", "llamacpp_threadripper", "llamacpp", "ollama",
                 "gemini", "openrouter", "openai"]
    elif tier == Tier.ORACLE:
        chain = ["llamacpp_z4", "llamacpp_oracle", "llamacpp", "llamacpp_x99",
                 "gemini", "openrouter", "openai"]
    elif tier == Tier.RESEARCH:
        # x99-hex first (idle 2-slot 27B, measured 22 tok/s from grid-svr
        # 2026-11-09), then the gridz4 proxy; the 3090 is the last local
        # choice so research never queues ahead of the oracle.
        chain = ["llamacpp_x99", "llamacpp", "llamacpp_z4", "llamacpp_oracle", "ollama",
                 "gemini", "openrouter", "openai"]
    elif tier == Tier.BATCH:
        chain = ["llamacpp_batch", "llamacpp_oracle", "llamacpp_z4", "gemini", "openrouter", "openai"]
    else:
        # REASON / DEFAULT — gridz4 first, the 3090 second, x99-hex (relayed)
        # after the gridz4 proxy.
        chain = ["llamacpp_z4", "llamacpp_oracle", "llamacpp", "llamacpp_x99", "ollama",
                 "gemini", "openrouter", "openai"]
    return [candidate for candidate in chain if candidate != provider]


# Nodes that no longer answer (fleet audit 2026-11-05). Kept as names so tests
# and reviewers can assert they never creep back into an automatic chain; the
# factories still exist for an explicit, opted-in ``provider=`` call.
_RETIRED_PROVIDERS = frozenset(
    {"ollama_koala", "ollama_ocr", "ollama_z400", "ollama_panda", "llamacpp_quick", "gemma"}
)


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
        elif tier == Tier.QUICK:
            # Not LLM_QUICK_PROVIDER: that legacy key is LOCAL's fallback.
            provider = getattr(settings, "LLM_QUICK_TIER_PROVIDER", None) or "llamacpp_threadripper"
        elif tier == Tier.ORACLE:
            provider = getattr(settings, "LLM_ORACLE_PROVIDER", None) \
                or getattr(settings, "LLM_DEEP_PROVIDER", "openrouter")
        elif tier == Tier.RESEARCH:
            provider = getattr(settings, "LLM_RESEARCH_PROVIDER", None) or "llamacpp_x99"
        elif tier == Tier.BATCH:
            provider = getattr(settings, "LLM_BATCH_PROVIDER", None) or "llamacpp_batch"
        else:
            # REASON and DEFAULT both land here
            provider = _gemma_or_default(settings, "LLM_REASON_PROVIDER", "LLM_DEFAULT_PROVIDER", "llamacpp")

    # Return cached client if available and still healthy
    if provider in _client_cache and not _provider_in_backoff(provider):
        client = _client_cache[provider]
        if getattr(client, "is_available", True):
            return client

    client = None
    if not _provider_in_backoff(provider):
        client = _build_client(provider)
        if client is not None and getattr(client, "is_available", True):
            _client_cache[provider] = client
            _fallback_warning(provider, None)
            return client

    for fallback in _fallback_chain(tier, provider):
        if _provider_in_backoff(fallback):
            continue
        if fallback in _client_cache:
            fb_client = _client_cache[fallback]
        else:
            fb_client = _build_client(fallback)
        if fb_client is not None and getattr(fb_client, "is_available", False):
            _fallback_warning(provider, fallback)
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


def _embed_chain() -> list[str]:
    """Provider names for embeddings, in order, from ``EMBED_PROVIDER_CHAIN``."""
    from config import settings

    raw = getattr(settings, "EMBED_PROVIDER_CHAIN", "") or ""
    chain = [name.strip() for name in raw.split(",") if name.strip()]
    return chain or ["ollama_z4", "ollama_i7gen10", "ollama"]


def embed(
    texts: list[str],
    model: str | None = None,
) -> list[list[float]] | None:
    """Embed ``texts`` on the first tailnet GPU node that answers.

    Walks ``EMBED_PROVIDER_CHAIN`` (default gridz4 → i7gen10 → grid-svr
    Ollama). gridz4 leads; i7gen10's RTX 2060 is the second separate tailnet
    node (added 2026-11-06, replacing the dead koala/z400 entries); grid-svr
    is deliberately last because its Ollama shares the RTX 3090 with the
    ORACLE llama-server, so embedding batches there would evict the 27B chat
    model. A node that refuses (probe fails, embed raises or returns nothing)
    is cooled down for ``LLM_PROVIDER_COOLDOWN_SECONDS``.

    Before 2026-09-10 embeddings went to the CPU-only llama.cpp unit on
    grid-svr :8080, which was never started with ``--embeddings`` and answered
    every call with HTTP 501. That unit is retired; this is its replacement.

    Args:
        texts: Strings to embed. An empty list short-circuits to ``[]``.
        model: Embedding model override. Defaults to each provider's
            configured embed model (``nomic-embed-text`` on every node).

    Returns:
        One vector per input text, or ``None`` when no provider answered.
        Never raises — callers treat ``None`` as "embeddings unavailable"
        and degrade gracefully.
    """
    if not texts:
        return []

    tried: list[str] = []
    for provider in _embed_chain():
        if _provider_in_backoff(provider):
            continue
        try:
            client = _client_cache.get(provider) or _build_client(provider)
        except Exception as exc:  # a broken provider must not sink the chain
            log.debug("Embed provider {p} init failed: {e}", p=provider, e=str(exc))
            continue
        if client is None or not getattr(client, "is_available", False):
            continue

        _client_cache[provider] = client
        tried.append(provider)
        try:
            vectors = client.embed(texts, model=model)
        except Exception as exc:
            log.warning("Embed via {p} raised: {e}", p=provider, e=str(exc))
            _client_cache.pop(provider, None)
            _mark_provider_refused(provider, "embed raised")
            continue
        if not vectors:
            # e.g. the node is up but the embed model is not pulled (404).
            _client_cache.pop(provider, None)
            _mark_provider_refused(provider, "embed returned nothing")
            continue
        _mark_provider_answered(provider)
        log.debug(
            "Embedded {n} texts via {p} — dim={d}",
            n=len(texts), p=provider,
            d=len(vectors[0]) if vectors[0] else 0,
        )
        return vectors

    # Graceful degradation: warning, not error — an offline embed node is an
    # operational condition, not an application bug (CLAUDE.md log-level rule).
    log.warning(
        "No embedding provider answered (chain={c}, reachable={t}) — "
        "returning None",
        c=",".join(_embed_chain()), t=",".join(tried) or "none",
    )
    return None


# Paid/hosted LLM providers — billed per token. Disabled by default per the local-first rule:
# GRID's fallback chains end in openrouter/openai, so a redeploy that restored API keys would
# silently re-leak. This choke-point keeps paid OFF even if a key reappears. To intentionally
# re-enable, set GRID_ALLOW_PAID_LLM=1 in the environment.
_PAID_PROVIDERS = frozenset({"openai", "openrouter", "anthropic", "huggingface", "gemini"})


def _paid_llm_allowed() -> bool:
    """True only if paid LLM use is explicitly opted in via GRID_ALLOW_PAID_LLM."""
    from config import settings
    return bool(getattr(settings, "GRID_ALLOW_PAID_LLM", False))


def _create_client(provider: str) -> Any:
    """Instantiate an LLM client for the given provider."""
    from config import settings

    if provider in _PAID_PROVIDERS and not _paid_llm_allowed():
        log.warning(
            "Paid LLM provider {p} blocked — GRID_ALLOW_PAID_LLM not set (local-first)",
            p=provider,
        )
        return None

    if provider == "huggingface":
        return _create_hf_client(settings)
    elif provider == "anthropic":
        return _create_anthropic_client(settings)
    elif provider == "ollama":
        return _create_ollama_client(settings)
    elif provider == "ollama_panda":
        return _create_ollama_panda_client(settings)
    elif provider == "ollama_ocr":
        return _create_ollama_ocr_client(settings)
    elif provider == "ollama_koala":
        return _create_ollama_koala_client(settings)
    elif provider == "ollama_z400":
        return _create_ollama_z400_client(settings)
    elif provider == "ollama_z4":
        return _create_ollama_z4_client(settings)
    elif provider == "ollama_i7gen10":
        return _create_ollama_i7gen10_client(settings)
    elif provider == "llamacpp":
        return _create_llamacpp_client(settings)
    elif provider == "openai":
        return _create_openai_client(settings)
    elif provider == "openrouter":
        return _create_openrouter_client(settings)
    elif provider == "llamacpp_oracle":
        return _create_llamacpp_oracle_client(settings)
    elif provider == "llamacpp_batch":
        return _create_llamacpp_batch_client(settings)
    elif provider == "llamacpp_quick":
        return _create_llamacpp_quick_client(settings)
    elif provider == "llamacpp_z4":
        return _create_llamacpp_z4_client(settings)
    elif provider == "llamacpp_threadripper":
        return _create_llamacpp_threadripper_client(settings)
    elif provider == "llamacpp_x99":
        return _create_llamacpp_x99_client(settings)
    elif provider == "gemini":
        return _create_gemini_client(settings)
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
            connect_timeout=_connect_timeout(settings),
        )
    except Exception as exc:
        log.debug("Ollama client init failed: {e}", e=str(exc))
        return None


def _create_ollama_panda_client(settings: Any) -> Any:
    """Ollama on the panda node (2× P100 16GB) — qwen2.5:32b."""
    if not getattr(settings, "OLLAMA_PANDA_ENABLED", False):
        return None
    base_url = getattr(settings, "OLLAMA_PANDA_BASE_URL", "")
    if not base_url:
        return None
    try:
        from ollama.client import OllamaClient
        return OllamaClient(
            base_url=base_url,
            model=getattr(settings, "OLLAMA_PANDA_CHAT_MODEL", "qwen2.5:32b"),
            timeout=getattr(settings, "OLLAMA_PANDA_TIMEOUT_SECONDS", 240),
            connect_timeout=_connect_timeout(settings),
        )
    except Exception as exc:
        log.debug("Ollama panda client init failed: {e}", e=str(exc))
        return None


def _create_ollama_koala_client(settings: Any) -> Any:
    """Ollama on koala (2× GTX TITAN X Maxwell 12GB) — gemma2:9b on card 0.

    Card 1 on koala is reserved for Whisper + Kokoro TTS, served by
    separate processes. Embeddings are also served from this Ollama
    instance via OLLAMA_KOALA_EMBED_MODEL.
    """
    if not getattr(settings, "OLLAMA_KOALA_ENABLED", False):
        return None
    try:
        from ollama.client import OllamaClient
        return OllamaClient(
            base_url=getattr(settings, "OLLAMA_KOALA_BASE_URL", "http://koala:11434"),
            model=getattr(settings, "OLLAMA_KOALA_CHAT_MODEL", "gemma2:9b"),
            embed_model=getattr(settings, "OLLAMA_KOALA_EMBED_MODEL", "nomic-embed-text"),
            timeout=getattr(settings, "OLLAMA_KOALA_TIMEOUT_SECONDS", 120),
            connect_timeout=_connect_timeout(settings),
        )
    except Exception as exc:
        log.debug("Ollama koala client init failed: {e}", e=str(exc))
        return None


def _create_ollama_ocr_client(settings: Any) -> Any:
    """Ollama on the ocr-node (2× 8GB Ampere) — gemma2:9b for general use.

    The vision models on ocr-node (qwen2.5vl:7b, minicpm-v:8b) are not
    routed through the standard chain; vision callers should target
    ``ollama_ocr`` directly with ``model=`` override or call Ollama
    directly on this URL.
    """
    if not getattr(settings, "OLLAMA_OCR_ENABLED", False):
        return None
    try:
        from ollama.client import OllamaClient
        return OllamaClient(
            base_url=getattr(settings, "OLLAMA_OCR_BASE_URL", "http://ocr-node:11434"),
            model=getattr(settings, "OLLAMA_OCR_CHAT_MODEL", "gemma2:9b"),
            timeout=getattr(settings, "OLLAMA_OCR_TIMEOUT_SECONDS", 120),
            connect_timeout=_connect_timeout(settings),
        )
    except Exception as exc:
        log.debug("Ollama ocr client init failed: {e}", e=str(exc))
        return None


def _create_ollama_z400_client(settings: Any) -> Any:
    """Ollama on the z400 node (12GB GPU) — qwen2.5:7b-instruct-q4_K_M.

    z400 sits in the cluster alongside panda/ocr/koala. Its 12GB card
    holds a 7B Q4 model comfortably (~5GB resident). Best fit for
    high-throughput narrative tasks (postmortem narration, signal
    interpretation) where the 7B class is enough and we want low
    per-request latency. minicpm-v / qwen2.5vl are also present for
    vision tasks (callers must override ``model=``).
    """
    if not getattr(settings, "OLLAMA_Z400_ENABLED", False):
        return None
    try:
        from ollama.client import OllamaClient
        return OllamaClient(
            base_url=getattr(settings, "OLLAMA_Z400_BASE_URL", "http://z400:11434"),
            model=getattr(settings, "OLLAMA_Z400_CHAT_MODEL", "qwen2.5:7b-instruct-q4_K_M"),
            embed_model=getattr(settings, "OLLAMA_Z400_EMBED_MODEL", "nomic-embed-text"),
            timeout=getattr(settings, "OLLAMA_Z400_TIMEOUT_SECONDS", 120),
            connect_timeout=_connect_timeout(settings),
        )
    except Exception as exc:
        log.debug("Ollama z400 client init failed: {e}", e=str(exc))
        return None


def _create_ollama_z4_client(settings: Any) -> Any:
    """Ollama on gridz4 (:11434) — the primary embedding node.

    Distinct from ``llamacpp_z4``, which is the llama.cpp chat server on the
    same box at ``gridz4:8080``. This is the Ollama daemon beside it, serving
    ``nomic-embed-text`` (verified from grid-svr 2026-09-10: peer active on a
    direct connection).

    It heads ``EMBED_PROVIDER_CHAIN``; i7gen10 (``ollama_i7gen10``) is the
    second node and grid-svr's own Ollama the last resort.
    """
    if not getattr(settings, "OLLAMA_Z4_ENABLED", False):
        return None
    try:
        from ollama.client import OllamaClient
        return OllamaClient(
            base_url=getattr(settings, "OLLAMA_Z4_BASE_URL", "http://gridz4:11434"),
            model=getattr(settings, "OLLAMA_Z4_CHAT_MODEL", "qwen3:8b"),
            embed_model=getattr(settings, "OLLAMA_Z4_EMBED_MODEL", "nomic-embed-text"),
            timeout=getattr(settings, "OLLAMA_Z4_TIMEOUT_SECONDS", 120),
            connect_timeout=_connect_timeout(settings),
        )
    except Exception as exc:
        log.debug("Ollama gridz4 client init failed: {e}", e=str(exc))
        return None


def _create_ollama_i7gen10_client(settings: Any) -> Any:
    """Ollama on i7gen10 (RTX 2060 6 GB) — the second embedding node.

    Added 2026-11-06 in place of the dead koala/z400 embed entries. Its
    Ollama binds the tailnet IP (OLLAMA_HOST=100.98.109.92:11434), not
    localhost, and serves qwen2.5:7b-instruct; ``nomic-embed-text`` has to be
    pulled there before embeds succeed — until then an embed returns nothing
    and :func:`embed` cools the node down and moves on.
    """
    if not getattr(settings, "OLLAMA_I7GEN10_ENABLED", False):
        return None
    base_url = getattr(settings, "OLLAMA_I7GEN10_BASE_URL", "") or ""
    if not base_url.strip():
        return None
    try:
        from ollama.client import OllamaClient
        return OllamaClient(
            base_url=base_url,
            model=getattr(settings, "OLLAMA_I7GEN10_CHAT_MODEL", "qwen2.5:7b-instruct"),
            embed_model=getattr(settings, "OLLAMA_I7GEN10_EMBED_MODEL", "nomic-embed-text"),
            timeout=getattr(settings, "OLLAMA_I7GEN10_TIMEOUT_SECONDS", 120),
            connect_timeout=_connect_timeout(settings),
        )
    except Exception as exc:
        log.debug("Ollama i7gen10 client init failed: {e}", e=str(exc))
        return None


def _create_llamacpp_client(settings: Any) -> Any:
    """Create a llama.cpp client."""
    if not settings.LLAMACPP_ENABLED:
        return None
    try:
        from llamacpp.client import LlamaCppClient
        # Honour LLAMACPP_TIMEOUT_SECONDS like llamacpp.client.get_client();
        # without it the client fell back to its 120 s default and the
        # configured 300 s was silently ignored on every router path.
        return LlamaCppClient(
            base_url=settings.LLAMACPP_BASE_URL,
            model=settings.LLAMACPP_CHAT_MODEL,
            timeout=getattr(settings, "LLAMACPP_TIMEOUT_SECONDS", 120),
            connect_timeout=_connect_timeout(settings),
        )
    except Exception as exc:
        log.debug("llama.cpp client init failed: {e}", e=str(exc))
        return None


def _create_llamacpp_oracle_client(settings: Any) -> Any:
    """llama.cpp client for grid-svr's RTX 3090 (127.0.0.1:18081, alias "oracle").

    Served by grid-hermes-3090-oracle.service: OpenAI-compatible
    llama-server, Qwen3.8-27B Q4_K_M, one slot, n_ctx 8192, loopback only.
    The previous :8081 shim is gone. This is the only GPU in the router that
    is independent of gridz4, so it is ORACLE's fallback and REASON's second
    choice (see :func:`_fallback_chain`).
    """
    if not getattr(settings, "LLAMACPP_ORACLE_ENABLED", False):
        return None
    try:
        from llamacpp.client import LlamaCppClient
        return LlamaCppClient(
            base_url=getattr(settings, "LLAMACPP_ORACLE_BASE_URL", "http://127.0.0.1:18081"),
            model=getattr(settings, "LLAMACPP_ORACLE_CHAT_MODEL", "oracle"),
            timeout=getattr(settings, "LLAMACPP_ORACLE_TIMEOUT_SECONDS", 300),
            default_num_predict=getattr(settings, "LLAMACPP_ORACLE_NUM_PREDICT", 6000),
            min_num_predict=getattr(settings, "LLAMACPP_ORACLE_MIN_NUM_PREDICT", 0),
            connect_timeout=_connect_timeout(settings),
        )
    except Exception as exc:
        log.debug("llama.cpp oracle client init failed: {e}", e=str(exc))
        return None


def _create_llamacpp_batch_client(settings: Any) -> Any:
    """Create a llama.cpp client for the BATCH-tier CPU server.

    Targets DeepSeek-V4-Flash 158B running on grid-svr CPU at port 8082.
    Slow (~5 tok/sec) but free + powerful — only invoked by ``Tier.BATCH``
    callers (deep dives, audio briefing scripts, regression evals).

    Defaults the timeout to 600s because batch jobs are expected to be slow.
    """
    if not getattr(settings, "LLAMACPP_BATCH_ENABLED", False):
        return None
    try:
        from llamacpp.client import LlamaCppClient
        return LlamaCppClient(
            base_url=getattr(settings, "LLAMACPP_BATCH_BASE_URL", "http://localhost:8082"),
            model=getattr(settings, "LLAMACPP_BATCH_CHAT_MODEL", "DeepSeekV4-Flash-158B-Q4_K_M"),
            timeout=getattr(settings, "LLAMACPP_BATCH_TIMEOUT_SECONDS", 600),
            connect_timeout=_connect_timeout(settings),
        )
    except Exception as exc:
        log.debug("llama.cpp batch client init failed: {e}", e=str(exc))
        return None


def _create_llamacpp_quick_client(settings: Any) -> Any:
    """Create a llama.cpp client for the QUICK-tier provider.

    Opt-in via provider="llamacpp_quick" on get_llm() (or LLM_LOCAL_PROVIDER)
    — not in any automatic fallback chain. Its old redbox default
    (100.126.129.45:8080) is dead, so there is no default host: a blank
    LLAMACPP_QUICK_BASE_URL returns None without opening a socket.
    """
    if not getattr(settings, "LLAMACPP_QUICK_ENABLED", False):
        return None
    base_url = getattr(settings, "LLAMACPP_QUICK_BASE_URL", "") or ""
    if not base_url.strip():
        return None
    try:
        from llamacpp.client import LlamaCppClient
        return LlamaCppClient(
            base_url=base_url,
            model=getattr(settings, "LLAMACPP_QUICK_CHAT_MODEL", "qwen3.8-27b"),
            timeout=getattr(settings, "LLAMACPP_QUICK_TIMEOUT_SECONDS", 120),
            connect_timeout=_connect_timeout(settings),
        )
    except Exception as exc:
        log.debug("llama.cpp quick client init failed: {e}", e=str(exc))
        return None


def _create_llamacpp_z4_client(settings: Any) -> Any:
    """Create a llama.cpp client for the gridz4 REASON-tier remote server.

    z4 serves Qwen3.8-27B Q4_K_M (reasoning off).  Keep this client bounded:
    the generic llama.cpp client adds reasoning headroom for thinking models,
    which turns default calls into 6K-token generations and can monopolize
    both gridz4 slots.
    """
    if not getattr(settings, "LLAMACPP_Z4_ENABLED", False):
        return None
    try:
        from llamacpp.client import LlamaCppClient
        return LlamaCppClient(
            base_url=getattr(settings, "LLAMACPP_Z4_BASE_URL", "http://gridz4:8080"),
            model=getattr(
                settings,
                "LLAMACPP_Z4_CHAT_MODEL",
                "Qwen3.8-27B-Q4_K_M",
            ),
            timeout=getattr(settings, "LLAMACPP_Z4_TIMEOUT_SECONDS", 180),
            default_num_predict=getattr(settings, "LLAMACPP_Z4_NUM_PREDICT", 512),
            min_num_predict=getattr(settings, "LLAMACPP_Z4_MIN_NUM_PREDICT", 0),
            reasoning_headroom=getattr(settings, "LLAMACPP_Z4_REASONING_HEADROOM", 0),
            connect_timeout=_connect_timeout(settings),
        )
    except Exception as exc:
        log.debug("llama.cpp z4 client init failed: {e}", e=str(exc))
        return None


def _create_llamacpp_threadripper_client(settings: Any) -> Any:
    """llama.cpp client for threadripper's RTX A2000 fast model (QUICK tier).

    threadripper serves a small model (8–14B) for short classification and
    extraction calls, bound to its tailnet IP like the fleet's other
    llama-servers. Reasoning is off server-side and the client adds no
    reasoning headroom, so a QUICK call stays a few hundred tokens.

    Off by default: a disabled flag or a blank URL returns ``None`` without
    opening a socket. When enabled, ``_build_client`` applies the usual
    refusal cooldown, so a dark threadripper costs one connect timeout per
    LLM_PROVIDER_COOLDOWN_SECONDS, not one per call.
    """
    if not getattr(settings, "LLAMACPP_THREADRIPPER_ENABLED", False):
        return None
    base_url = getattr(settings, "LLAMACPP_THREADRIPPER_BASE_URL", "") or ""
    if not base_url.strip():
        return None
    try:
        from llamacpp.client import LlamaCppClient
        return LlamaCppClient(
            base_url=base_url,
            model=getattr(settings, "LLAMACPP_THREADRIPPER_CHAT_MODEL", "quick"),
            timeout=getattr(settings, "LLAMACPP_THREADRIPPER_TIMEOUT_SECONDS", 60),
            default_num_predict=getattr(settings, "LLAMACPP_THREADRIPPER_NUM_PREDICT", 512),
            min_num_predict=0,
            reasoning_headroom=0,
            connect_timeout=_connect_timeout(settings, "LLAMACPP_THREADRIPPER_CONNECT_TIMEOUT_SECONDS"),
        )
    except Exception as exc:
        log.debug("llama.cpp threadripper client init failed: {e}", e=str(exc))
        return None


def _create_llamacpp_x99_client(settings: Any) -> Any:
    """llama.cpp client for x99-hex's Qwen3.8-27B (S5, 2026-11-06).

    x99-hex (site B) serves the same model, build family and flags as gridz4
    (alias ``Qwen3.8-27B-Q4_K_M``, 2 slots, 32k ctx, reasoning off, ~22 tok/s)
    on :8086. It is independent of both gridz4 and the 3090, but calls from
    grid-svr cross Tailscale's relay (1.4–2 s RTT, ~3 MB/s), so it is a
    fallback only — after the gridz4 proxy in REASON/ORACLE — with a longer
    request and connect timeout than the on-site nodes.

    Off by default; a disabled flag or blank URL returns ``None`` without a
    socket, and ``_build_client`` cools it down on a failed probe.
    """
    if not getattr(settings, "LLAMACPP_X99_ENABLED", False):
        return None
    base_url = getattr(settings, "LLAMACPP_X99_BASE_URL", "") or ""
    if not base_url.strip():
        return None
    try:
        from llamacpp.client import LlamaCppClient
        return LlamaCppClient(
            base_url=base_url,
            model=getattr(settings, "LLAMACPP_X99_CHAT_MODEL", "Qwen3.8-27B-Q4_K_M"),
            timeout=getattr(settings, "LLAMACPP_X99_TIMEOUT_SECONDS", 360),
            default_num_predict=getattr(settings, "LLAMACPP_X99_NUM_PREDICT", 512),
            min_num_predict=0,
            reasoning_headroom=0,
            connect_timeout=_connect_timeout(settings, "LLAMACPP_X99_CONNECT_TIMEOUT_SECONDS"),
        )
    except Exception as exc:
        log.debug("llama.cpp x99-hex client init failed: {e}", e=str(exc))
        return None


def _create_gemini_client(settings: Any) -> Any:
    """Create a Google Gemini client (Generative AI REST API).

    Paid provider — ``_create_client`` already refused this call unless
    ``GRID_ALLOW_PAID_LLM`` is set, so reaching here means the operator opted
    in. Sits ahead of ``openrouter`` in every chat chain per the operator's
    2026-09-10 direction to prefer a frontier model over CPU inference.
    """
    api_key = getattr(settings, "GEMINI_API_KEY", "")
    if not api_key:
        log.warning("No GEMINI_API_KEY set — Gemini unavailable")
        return None

    return GeminiClient(
        api_key=api_key,
        base_url=getattr(
            settings,
            "GEMINI_BASE_URL",
            "https://generativelanguage.googleapis.com/v1beta",
        ),
        model=getattr(settings, "GEMINI_CHAT_MODEL", "gemini-2.5-flash"),
        timeout=getattr(settings, "GEMINI_TIMEOUT_SECONDS", 120),
    )


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
                if resp.status_code in {401, 402, 403}:
                    self.is_available = False
                    _mark_provider_unavailable(
                        self._health_provider,
                        f"HTTP {resp.status_code}",
                        _PROVIDER_AUTH_BACKOFF_S,
                    )
                elif resp.status_code == 429:
                    self.is_available = False
                    _mark_provider_unavailable(
                        self._health_provider,
                        "HTTP 429",
                        _PROVIDER_RATE_BACKOFF_S,
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


class GeminiClient:
    """Google Gemini client conforming to the LLMClient protocol.

    Talks to the Generative AI REST API via ``requests`` (no SDK dependency,
    matching :class:`AnthropicClient`). Gemini's payload shape differs from
    OpenAI's in three ways this class translates:

    * messages are ``contents`` entries with ``parts``, not ``content`` strings;
    * the assistant role is spelled ``model``, not ``assistant``;
    * a system message goes in a top-level ``systemInstruction``, not inline.

    Every failure path returns ``None`` so callers degrade gracefully — the
    router's chain simply moves on to the next provider.
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://generativelanguage.googleapis.com/v1beta",
        model: str = "gemini-2.5-flash",
        timeout: int = 120,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout = timeout
        self.is_available = bool(api_key)
        self._knowledge_cache: dict[str, str] = {}

    @staticmethod
    def _to_gemini_payload(
        messages: list[dict[str, str]],
    ) -> tuple[list[dict[str, Any]], str]:
        """Split OpenAI-style messages into Gemini contents + system text."""
        contents: list[dict[str, Any]] = []
        system_text = ""
        for message in messages:
            role = message.get("role", "user")
            content = message.get("content", "")
            if role == "system":
                system_text += content + "\n"
                continue
            contents.append(
                {
                    "role": "model" if role == "assistant" else "user",
                    "parts": [{"text": content}],
                }
            )
        return contents, system_text.strip()

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

        contents, system_text = self._to_gemini_payload(messages)
        if not contents:
            return None

        model_name = model or self.model
        payload: dict[str, Any] = {
            "contents": contents,
            "generationConfig": {
                "temperature": temperature,
                "maxOutputTokens": num_predict,
            },
        }
        if system_text:
            payload["systemInstruction"] = {"parts": [{"text": system_text}]}

        start = time.monotonic()
        try:
            resp = requests.post(
                f"{self.base_url}/models/{model_name}:generateContent",
                json=payload,
                # Key travels as a header, never in the URL — query strings leak
                # into proxy and access logs.
                headers={
                    "x-goog-api-key": self.api_key,
                    "Content-Type": "application/json",
                },
                timeout=self.timeout,
            )
            latency_ms = (time.monotonic() - start) * 1000

            if resp.status_code >= 400:
                log.warning(
                    "Gemini API {status} ({l:.0f}ms): {body}",
                    status=resp.status_code, l=latency_ms,
                    body=resp.text[:300],
                )
                if resp.status_code in {401, 402, 403}:
                    self.is_available = False
                    _mark_provider_unavailable(
                        "gemini", f"HTTP {resp.status_code}", _PROVIDER_AUTH_BACKOFF_S
                    )
                elif resp.status_code == 429:
                    self.is_available = False
                    _mark_provider_unavailable(
                        "gemini", "HTTP 429", _PROVIDER_RATE_BACKOFF_S
                    )
                return None

            data = resp.json()
            candidates = data.get("candidates") or []
            if not candidates:
                # A prompt blocked by safety filters comes back 200 with no
                # candidates — that is a miss, not a crash.
                log.warning(
                    "Gemini returned no candidates: {r}",
                    r=str(data.get("promptFeedback", ""))[:200],
                )
                return None

            parts = candidates[0].get("content", {}).get("parts", [])
            text_out = "".join(p.get("text", "") for p in parts)
            if not text_out:
                return None

            usage = data.get("usageMetadata", {})
            log.debug(
                "Gemini chat — model={m}, latency={l:.0f}ms, in={i}, out={o}",
                m=model_name, l=latency_ms,
                i=usage.get("promptTokenCount", "?"),
                o=usage.get("candidatesTokenCount", "?"),
            )

            try:
                from llm.feedback_loop import log_llm_call
                log_llm_call(
                    module="gemini",
                    tier="unknown",
                    system_prompt=system_text[:2000],
                    user_prompt=contents[0]["parts"][0]["text"][:2000],
                    output=text_out[:2000],
                    context_tokens=usage.get("promptTokenCount", 0),
                    output_tokens=usage.get("candidatesTokenCount", 0),
                    latency_ms=int(latency_ms),
                    model=model_name,
                    provider="gemini",
                    metadata=dict(extra_metadata) if extra_metadata else None,
                )
            except Exception:
                pass  # never let logging break inference

            return text_out

        except Exception as exc:
            latency_ms = (time.monotonic() - start) * 1000
            log.warning(
                "Gemini API failed ({l:.0f}ms): {err}", l=latency_ms, err=str(exc)
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

    def embed(
        self,
        texts: list[str],
        model: str | None = None,
    ) -> list[list[float]] | None:
        """Not wired — GRID embeddings stay on tailnet GPU nodes.

        See ``EMBED_PROVIDER_CHAIN`` / :func:`embed`. Sending embedding
        batches to a paid frontier API would bill per call for vectors that
        koala and z400 produce for free.
        """
        return None

    def health_check(self) -> dict[str, Any]:
        return {"available": self.is_available, "provider": "gemini",
                "model": self.model, "endpoint": self.base_url}

    def list_models(self) -> list[dict[str, Any]]:
        return [{"name": self.model, "provider": "gemini"}]

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
