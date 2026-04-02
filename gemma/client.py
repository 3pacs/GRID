"""
GRID Gemma 3 27B QAT client.

Talks to Gemma 3 served via llama.cpp (or any OpenAI-compatible server)
on a dedicated port. Same LLMClient protocol as LlamaCppClient so it
plugs into the router, task queue, and all existing GRID inference paths.

Key advantages over the current Hermes/llama3 setup:
  - 128K-token context window (vs 8K) for full hypothesis set analysis
  - Multimodal input (can ingest dashboard screenshots)
  - QAT quantisation — runs on a single RTX 3090 at near-FP16 quality
  - Function calling support for direct griddb queries

Deployment:
  llama-server -m gemma-3-27b-it-qat-q4_0.gguf \\
    --port 8081 --ctx-size 131072 --n-gpu-layers 99

  Or via Ollama:
    ollama run gemma3:27b-it-qat
"""

from __future__ import annotations

import time
from typing import Any

import requests
from loguru import logger as log

from knowledge.loader import inject_knowledge, load_all_knowledge_docs, load_knowledge_doc

# Module-level cached singleton
_client_instance: GemmaClient | None = None


class GemmaClient:
    """Client for Gemma 3 27B QAT via OpenAI-compatible API.

    Drop-in alongside LlamaCppClient — same public interface
    (chat, generate, embed, health_check) so all GRID code works unchanged.

    Attributes:
        base_url: Base URL of the Gemma server (e.g. http://localhost:8081).
        model: Model alias (e.g. "gemma-3-27b-it").
        embed_model: Embedding model alias.
        timeout: HTTP request timeout in seconds.
        is_available: Whether the server responded at init.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8081",
        model: str = "gemma-3-27b-it",
        embed_model: str = "gemma-3-27b-it",
        timeout: int = 180,
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

        # Fetch context size from server
        self._ctx_size = 131072  # Gemma 3 supports 128K
        if self.is_available:
            try:
                props = requests.get(f"{self.base_url}/props", timeout=5).json()
                self._ctx_size = props.get(
                    "default_generation_settings", {}
                ).get("n_ctx", 131072)
                log.info("Gemma ctx_size={c}", c=self._ctx_size)
            except Exception:
                log.debug("Failed to fetch Gemma ctx_size, using 128K default")

        if self.is_available:
            log.info("Gemma 3 server connected — {url}", url=self.base_url)
        else:
            log.warning(
                "Gemma 3 server not available at {url} — GRID will operate without it",
                url=self.base_url,
            )

    # ------------------------------------------------------------------
    # Knowledge loading (delegates to knowledge.loader)
    # ------------------------------------------------------------------
    def load_knowledge(self, doc_name: str) -> str | None:
        """Load a knowledge .md file by name.

        Parameters:
            doc_name: Filename (with or without .md extension).

        Returns:
            str: Document contents, or None if not found.
        """
        return load_knowledge_doc(self._knowledge_cache, doc_name)

    def load_all_knowledge(self) -> str:
        """Load and concatenate all knowledge .md files.

        Returns:
            str: Combined knowledge context.
        """
        return load_all_knowledge_docs(self._knowledge_cache)

    # ------------------------------------------------------------------
    # Chat completion (OpenAI-compatible)
    # ------------------------------------------------------------------
    def chat(
        self,
        messages: list[dict[str, str]],
        model: str | None = None,
        temperature: float = 0.3,
        num_predict: int = 4000,
        system_knowledge: list[str] | None = None,
    ) -> str | None:
        """Send a chat completion request to the Gemma server.

        Parameters:
            messages: List of message dicts with ``role`` and ``content`` keys.
            model: Model override (defaults to self.model).
            temperature: Sampling temperature.
            num_predict: Maximum tokens to generate.
            system_knowledge: List of knowledge doc names to inject.

        Returns:
            str: The assistant's response text, or None if unavailable.
        """
        if not self.is_available:
            try:
                resp = requests.get(f"{self.base_url}/health", timeout=3)
                self.is_available = resp.status_code == 200
            except Exception:
                log.debug("Gemma health re-check failed")
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

        # Clamp max_tokens to fit within context window
        total_prompt_chars = sum(len(m.get("content", "")) for m in messages)
        est_prompt_tokens = total_prompt_chars // 3
        max_ctx = self._ctx_size
        available_for_gen = max(256, max_ctx - est_prompt_tokens - 64)
        if payload["max_tokens"] > available_for_gen:
            log.debug(
                "Gemma: clamping max_tokens {orig} → {clamped}",
                orig=payload["max_tokens"],
                clamped=available_for_gen,
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
                    pass
                log.warning(
                    "Gemma chat {status} ({l:.0f}ms): {body}",
                    status=resp.status_code,
                    l=latency_ms,
                    body=error_body,
                )
                return None

            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            model_used = data.get("model", model or self.model)
            tokens = data.get("usage", {})
            log.debug(
                "Gemma chat — model={m}, latency={l:.0f}ms, prompt_tok={p}, gen_tok={g}",
                m=model_used,
                l=latency_ms,
                p=tokens.get("prompt_tokens", "?"),
                g=tokens.get("completion_tokens", "?"),
            )
            return content

        except Exception as exc:
            latency_ms = (time.monotonic() - start) * 1000
            log.warning(
                "Gemma chat failed ({l:.0f}ms): {err}",
                l=latency_ms,
                err=str(exc),
            )
            return None

    # ------------------------------------------------------------------
    # Generate (single-turn)
    # ------------------------------------------------------------------
    def generate(
        self,
        prompt: str,
        model: str | None = None,
        system: str | None = None,
        temperature: float = 0.3,
        num_predict: int = 4000,
    ) -> str | None:
        """Single-turn generation via chat endpoint.

        Parameters:
            prompt: The user prompt.
            model: Model override.
            system: System prompt.
            temperature: Sampling temperature.
            num_predict: Max tokens.

        Returns:
            str: Generated text, or None if unavailable.
        """
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
        """Generate embeddings via the /v1/embeddings endpoint.

        Parameters:
            texts: Strings to embed.
            model: Embedding model override.

        Returns:
            list[list[float]]: One embedding vector per input, or None.
        """
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
                "Gemma embed — {n} texts, dim={d}",
                n=len(texts),
                d=len(embeddings[0]) if embeddings else 0,
            )
            return embeddings

        except Exception as exc:
            log.warning("Gemma embed failed: {err}", err=str(exc))
            return None

    # ------------------------------------------------------------------
    # Model management
    # ------------------------------------------------------------------
    def list_models(self) -> list[dict[str, Any]]:
        """List models loaded by the server.

        Returns:
            list[dict]: Model metadata dicts, or empty list.
        """
        try:
            resp = requests.get(f"{self.base_url}/v1/models", timeout=5)
            resp.raise_for_status()
            return resp.json().get("data", [])
        except Exception as exc:
            log.debug("Could not list Gemma models: {err}", err=str(exc))
            return []

    def get_model_names(self) -> list[str]:
        """Return just the model name strings.

        Returns:
            list[str]: Model IDs loaded by the server.
        """
        return [m.get("id", "") for m in self.list_models()]

    def pull_model(self, model_name: str) -> bool:
        """No-op — Gemma models are loaded at server startup.

        Returns:
            bool: Always False (not supported).
        """
        log.warning(
            "pull_model not supported for Gemma — "
            "restart the server with the desired model"
        )
        return False

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------
    def health_check(self) -> dict[str, Any]:
        """Return a structured health-check result.

        Returns:
            dict: Keys ``available``, ``latency_ms``, ``models``, ``endpoint``,
                  ``slots_idle``, ``slots_processing``.
        """
        result: dict[str, Any] = {
            "available": False,
            "latency_ms": None,
            "models": [],
            "endpoint": self.base_url,
            "slots_idle": None,
            "slots_processing": None,
        }

        start = time.monotonic()
        try:
            resp = requests.get(f"{self.base_url}/health", timeout=5)
            latency = (time.monotonic() - start) * 1000
            if resp.status_code == 200:
                data = (
                    resp.json()
                    if resp.headers.get("content-type", "").startswith(
                        "application/json"
                    )
                    else {}
                )
                result["available"] = data.get("status", "ok") == "ok"
                result["latency_ms"] = round(latency, 1)
                result["slots_idle"] = data.get("slots_idle")
                result["slots_processing"] = data.get("slots_processing")
                result["models"] = self.get_model_names()
        except Exception:
            log.debug("Gemma health check failed — server may be offline")

        self.is_available = result["available"]
        return result

    # ------------------------------------------------------------------
    # Gemma-specific: multimodal chat (images + text)
    # ------------------------------------------------------------------
    def chat_with_image(
        self,
        prompt: str,
        image_base64: str,
        model: str | None = None,
        temperature: float = 0.3,
        num_predict: int = 4000,
    ) -> str | None:
        """Send a multimodal chat request with an image.

        Gemma 3 supports vision — useful for analysing GRID dashboard
        screenshots, chart patterns, and visual anomaly detection.

        Parameters:
            prompt: Text prompt describing what to analyse.
            image_base64: Base64-encoded image data.
            model: Model override.
            temperature: Sampling temperature.
            num_predict: Max tokens.

        Returns:
            str: The assistant's response, or None if unavailable.
        """
        if not self.is_available:
            return None

        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/png;base64,{image_base64}",
                        },
                    },
                ],
            }
        ]

        payload: dict[str, Any] = {
            "model": model or self.model,
            "messages": messages,
            "max_tokens": num_predict,
            "temperature": temperature,
            "stream": False,
        }

        start = time.monotonic()
        try:
            resp = requests.post(
                f"{self.base_url}/v1/chat/completions",
                json=payload,
                timeout=self.timeout,
            )
            latency_ms = (time.monotonic() - start) * 1000

            if resp.status_code >= 400:
                log.warning(
                    "Gemma vision chat {status} ({l:.0f}ms)",
                    status=resp.status_code,
                    l=latency_ms,
                )
                return None

            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            log.debug("Gemma vision chat — latency={l:.0f}ms", l=latency_ms)
            return content

        except Exception as exc:
            latency_ms = (time.monotonic() - start) * 1000
            log.warning(
                "Gemma vision chat failed ({l:.0f}ms): {err}",
                l=latency_ms,
                err=str(exc),
            )
            return None


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------


def get_client() -> GemmaClient:
    """Return a cached GemmaClient singleton.

    Returns:
        GemmaClient: Shared client instance.
    """
    global _client_instance
    if _client_instance is None:
        from config import settings

        _client_instance = GemmaClient(
            base_url=settings.GEMMA_BASE_URL,
            model=settings.GEMMA_CHAT_MODEL,
            embed_model=settings.GEMMA_EMBED_MODEL,
            timeout=settings.GEMMA_TIMEOUT_SECONDS,
        )
    return _client_instance


if __name__ == "__main__":
    client = get_client()
    hc = client.health_check()
    print(f"Available: {hc['available']}")
    print(f"Latency:   {hc['latency_ms']}ms")
    print(f"Models:    {hc['models']}")
    print(f"Endpoint:  {hc['endpoint']}")
