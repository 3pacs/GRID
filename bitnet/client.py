"""
GRID BitNet client — 1-bit LLM inference.

Talks to a BitNet.cpp server's OpenAI-compatible API.  BitNet models use
ternary weights ({-1, 0, +1}, ~1.58 bits per weight) for dramatically
faster CPU inference at lower energy cost.

Same public interface as LlamaCppClient so the router can swap it in
transparently.  Designed for LOCAL-tier tasks where speed matters more
than peak reasoning depth.

Reference: https://github.com/microsoft/BitNet
"""

from __future__ import annotations

import time
from typing import Any

import requests
from loguru import logger as log

from knowledge.loader import inject_knowledge, load_all_knowledge_docs, load_knowledge_doc

# Module-level cached singleton
_client_instance: BitNetClient | None = None


class BitNetClient:
    """Client for a BitNet.cpp OpenAI-compatible inference server.

    Every public method catches exceptions and returns a safe default
    so GRID never crashes due to the BitNet server being offline.

    Attributes:
        base_url: Base URL of the BitNet server (e.g. http://localhost:8090).
        model: Model alias reported by the server.
        embed_model: Embedding model alias.
        timeout: HTTP request timeout in seconds.
        is_available: Whether the server responded at init.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8090",
        model: str = "bitnet-b1.58-2B-4T",
        embed_model: str = "bitnet-b1.58-2B-4T",
        timeout: int = 120,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.embed_model = embed_model
        self.timeout = timeout
        self.is_available: bool = False
        self._knowledge_cache: dict[str, str] = {}

        # Health probe
        try:
            resp = requests.get(f"{self.base_url}/health", timeout=5)
            self.is_available = resp.status_code == 200
        except Exception:
            self.is_available = False

        # Fetch context size from server props (llama.cpp-compatible)
        self._ctx_size = 2048  # BitNet models are typically smaller context
        if self.is_available:
            try:
                props = requests.get(f"{self.base_url}/props", timeout=5).json()
                self._ctx_size = props.get("default_generation_settings", {}).get("n_ctx", 2048)
                log.info("BitNet ctx_size={c}", c=self._ctx_size)
            except Exception:
                log.debug("Failed to fetch BitNet ctx_size, using default", exc_info=True)

        if self.is_available:
            log.info("BitNet server connected — {url}", url=self.base_url)
        else:
            log.warning(
                "BitNet server not available at {url} — GRID will operate without it",
                url=self.base_url,
            )

    # ------------------------------------------------------------------
    # Knowledge loading (delegates to knowledge.loader)
    # ------------------------------------------------------------------
    def load_knowledge(self, doc_name: str) -> str | None:
        """Load a knowledge .md file."""
        return load_knowledge_doc(self._knowledge_cache, doc_name)

    def load_all_knowledge(self) -> str:
        """Load and concatenate all knowledge .md files."""
        return load_all_knowledge_docs(self._knowledge_cache)

    # ------------------------------------------------------------------
    # Chat completion (OpenAI-compatible)
    # ------------------------------------------------------------------
    def chat(
        self,
        messages: list[dict[str, str]],
        model: str | None = None,
        temperature: float = 0.3,
        num_predict: int = 2000,
        system_knowledge: list[str] | None = None,
    ) -> str | None:
        """Send a chat completion request to the BitNet server.

        Parameters:
            messages: List of message dicts with ``role`` and ``content`` keys.
            model: Model override (defaults to self.model).
            temperature: Sampling temperature.
            num_predict: Maximum tokens to generate.
            system_knowledge: Knowledge doc names to inject into system prompt.

        Returns:
            The assistant's response text, or None if unavailable.
        """
        if not self.is_available:
            # Retry health check — server may have started after us
            try:
                resp = requests.get(f"{self.base_url}/health", timeout=3)
                self.is_available = resp.status_code == 200
            except Exception:
                log.debug("BitNet health re-check failed", exc_info=True)
            if not self.is_available:
                return None

        # Inject knowledge into system message if requested
        messages = inject_knowledge(messages, system_knowledge, self._knowledge_cache)

        payload: dict[str, Any] = {
            "model": model or self.model,
            "messages": messages,
            "max_tokens": num_predict,
            "temperature": temperature,
            "stream": False,
        }

        # Clamp max_tokens so prompt + generation fits in context
        total_prompt_chars = sum(len(m["content"]) for m in messages)
        est_prompt_tokens = total_prompt_chars // 3  # conservative
        max_ctx = self._ctx_size or 2048
        available_for_gen = max(256, max_ctx - est_prompt_tokens - 64)
        if payload["max_tokens"] > available_for_gen:
            log.debug(
                "BitNet clamping max_tokens {orig} -> {clamped} (est prompt={p} tok)",
                orig=payload["max_tokens"], clamped=available_for_gen,
                p=est_prompt_tokens,
            )
            payload["max_tokens"] = available_for_gen

        start = time.monotonic()
        try:
            resp = requests.post(
                f"{self.base_url}/v1/chat/completions",
                json=payload,
                timeout=self.timeout,
            )
            latency_ms = (time.monotonic() - start) * 1000

            if resp.status_code >= 400:
                error_body = ""
                try:
                    error_body = resp.text[:500]
                except Exception:
                    log.debug("Failed to read BitNet error response body", exc_info=True)
                log.warning(
                    "BitNet chat {status} ({l:.0f}ms): {body}",
                    status=resp.status_code, l=latency_ms, body=error_body,
                )
                return None

            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            model_used = data.get("model", model or self.model)
            tokens = data.get("usage", {})
            log.debug(
                "BitNet chat — model={m}, latency={l:.0f}ms, prompt_tok={p}, gen_tok={g}",
                m=model_used,
                l=latency_ms,
                p=tokens.get("prompt_tokens", "?"),
                g=tokens.get("completion_tokens", "?"),
            )

            # Log to feedback loop
            try:
                from llm.feedback_loop import log_llm_call
                sys_msg = next((m["content"] for m in messages if m["role"] == "system"), "")
                usr_msg = next((m["content"] for m in messages if m["role"] == "user"), "")
                log_llm_call(
                    module="bitnet",
                    tier="LOCAL",
                    system_prompt=sys_msg[:2000],
                    user_prompt=usr_msg[:2000],
                    output=content[:2000],
                    context_tokens=tokens.get("prompt_tokens", 0),
                    output_tokens=tokens.get("completion_tokens", 0),
                    latency_ms=int(latency_ms),
                    model=model_used,
                    provider="bitnet",
                )
            except Exception:
                pass  # never let logging break inference

            return content

        except Exception as exc:
            latency_ms = (time.monotonic() - start) * 1000
            log.warning(
                "BitNet chat failed ({l:.0f}ms): {err}",
                l=latency_ms,
                err=str(exc),
            )
            return None

    # ------------------------------------------------------------------
    # Generate (single-turn — mapped to chat with one message)
    # ------------------------------------------------------------------
    def generate(
        self,
        prompt: str,
        model: str | None = None,
        system: str | None = None,
        temperature: float = 0.3,
        num_predict: int = 2000,
    ) -> str | None:
        """Single-turn generation via chat endpoint."""
        messages: list[dict[str, str]] = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        return self.chat(
            messages,
            model=model,
            temperature=temperature,
            num_predict=num_predict,
        )

    # ------------------------------------------------------------------
    # Embeddings
    # ------------------------------------------------------------------
    def embed(
        self,
        texts: list[str],
        model: str | None = None,
    ) -> list[list[float]] | None:
        """Generate embeddings. BitNet models may not support this."""
        if not self.is_available:
            return None

        payload: dict[str, Any] = {
            "model": model or self.embed_model,
            "input": texts,
        }

        try:
            resp = requests.post(
                f"{self.base_url}/v1/embeddings",
                json=payload,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            data = resp.json()

            embeddings = [item["embedding"] for item in data["data"]]
            log.debug(
                "BitNet embed — {n} texts, dim={d}",
                n=len(texts),
                d=len(embeddings[0]) if embeddings else 0,
            )
            return embeddings

        except Exception as exc:
            log.warning("BitNet embed failed: {err}", err=str(exc))
            return None

    # ------------------------------------------------------------------
    # Model management
    # ------------------------------------------------------------------
    def list_models(self) -> list[dict[str, Any]]:
        """List models loaded by the server."""
        try:
            resp = requests.get(f"{self.base_url}/v1/models", timeout=5)
            resp.raise_for_status()
            return resp.json().get("data", [])
        except Exception as exc:
            log.debug("Could not list BitNet models: {err}", err=str(exc))
            return []

    def get_model_names(self) -> list[str]:
        """Return just the model name strings."""
        return [m.get("id", "") for m in self.list_models()]

    def pull_model(self, model_name: str) -> bool:
        """No-op — BitNet server loads models at startup."""
        log.warning(
            "pull_model not supported for BitNet — "
            "restart the server with the desired model"
        )
        return False

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------
    def health_check(self) -> dict[str, Any]:
        """Return a structured health-check result."""
        result: dict[str, Any] = {
            "available": False,
            "latency_ms": None,
            "models": [],
            "endpoint": self.base_url,
            "provider": "bitnet",
        }

        start = time.monotonic()
        try:
            resp = requests.get(f"{self.base_url}/health", timeout=5)
            latency = (time.monotonic() - start) * 1000
            if resp.status_code == 200:
                data = (
                    resp.json()
                    if resp.headers.get("content-type", "").startswith("application/json")
                    else {}
                )
                result["available"] = data.get("status", "ok") == "ok"
                result["latency_ms"] = round(latency, 1)
                result["models"] = self.get_model_names()
        except Exception:
            log.debug("BitNet health check failed — server may be offline", exc_info=True)

        self.is_available = result["available"]
        return result


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

def get_client() -> BitNetClient:
    """Return a cached BitNetClient singleton."""
    global _client_instance
    if _client_instance is None:
        from config import settings

        _client_instance = BitNetClient(
            base_url=settings.BITNET_BASE_URL,
            model=settings.BITNET_CHAT_MODEL,
            embed_model=settings.BITNET_EMBED_MODEL,
            timeout=settings.BITNET_TIMEOUT_SECONDS,
        )
    return _client_instance


if __name__ == "__main__":
    client = get_client()
    hc = client.health_check()
    print(f"Available: {hc['available']}")
    print(f"Latency:   {hc['latency_ms']}ms")
    print(f"Models:    {hc['models']}")
    print(f"Endpoint:  {hc['endpoint']}")
