"""
GRID LLM client.

Provides a shared client interface across OpenAI, llama.cpp, and Ollama.
All methods return safe fallbacks on failure so GRID never depends on
any single LLM backend for core operations.
"""

from __future__ import annotations

import os
import time
from typing import Any

import requests
from loguru import logger as log

from knowledge.loader import inject_knowledge, load_all_knowledge_docs, load_knowledge_doc

# Module-level cached singleton
_client_instance: Any | None = None


class OllamaClient:
    """Client for the local Ollama API.

    Wraps chat, generate, embeddings, and model management endpoints.
    Every public method catches all exceptions and returns a safe default
    so GRID never crashes due to Ollama being unavailable.

    Attributes:
        base_url: Base URL of the Ollama API (default http://localhost:11434).
        model: Default model to use for chat/generate.
        embed_model: Model to use for embeddings.
        timeout: HTTP request timeout in seconds.
        is_available: Whether Ollama responded to the initial health check.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:11434",
        model: str = "llama3.1:8b",
        embed_model: str = "nomic-embed-text",
        timeout: int = 300,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.embed_model = embed_model
        self.timeout = timeout
        self.is_available: bool = False
        self._knowledge_cache: dict[str, str] = {}

        # Health check
        try:
            resp = requests.get(f"{self.base_url}/api/tags", timeout=5)
            self.is_available = resp.status_code == 200
        except Exception:
            self.is_available = False

        if self.is_available:
            log.info("Ollama client connected — {url}", url=self.base_url)
        else:
            log.warning(
                "Ollama not available at {url} — GRID will operate without it",
                url=self.base_url,
            )

    # ------------------------------------------------------------------
    # Knowledge loading
    # ------------------------------------------------------------------
    def load_knowledge(self, doc_name: str) -> str | None:
        """Load a knowledge .md file from the knowledge directory.

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
    # Chat completion (native API)
    # ------------------------------------------------------------------
    def chat(
        self,
        messages: list[dict[str, str]],
        model: str | None = None,
        temperature: float = 0.3,
        num_predict: int = 2000,
        system_knowledge: list[str] | None = None,
    ) -> str | None:
        """Send a chat completion request to Ollama.

        Parameters:
            messages: List of message dicts with ``role`` and ``content`` keys.
            model: Model override (defaults to self.model).
            temperature: Sampling temperature.
            num_predict: Maximum tokens to generate.
            system_knowledge: List of knowledge doc names to inject into
                the system prompt as context.

        Returns:
            str: The assistant's response text, or None if unavailable.
        """
        if not self.is_available:
            return None

        messages = inject_knowledge(messages, system_knowledge, self._knowledge_cache)

        payload: dict[str, Any] = {
            "model": model or self.model,
            "messages": messages,
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": num_predict,
            },
        }

        start = time.monotonic()
        try:
            resp = requests.post(
                f"{self.base_url}/api/chat",
                json=payload,
                timeout=self.timeout,
            )
            latency_ms = (time.monotonic() - start) * 1000
            resp.raise_for_status()
            data = resp.json()

            content = data.get("message", {}).get("content", "")
            model_used = data.get("model", model or self.model)
            log.debug(
                "Ollama chat — model={m}, latency={l:.0f}ms",
                m=model_used,
                l=latency_ms,
            )
            return content

        except Exception as exc:
            latency_ms = (time.monotonic() - start) * 1000
            log.warning(
                "Ollama chat failed ({l:.0f}ms): {err}",
                l=latency_ms,
                err=str(exc),
            )
            return None

    # ------------------------------------------------------------------
    # Generate (single-turn, no conversation)
    # ------------------------------------------------------------------
    def generate(
        self,
        prompt: str,
        model: str | None = None,
        system: str | None = None,
        temperature: float = 0.3,
        num_predict: int = 2000,
    ) -> str | None:
        """Send a single-turn generate request.

        Parameters:
            prompt: The user prompt.
            model: Model override.
            system: System prompt.
            temperature: Sampling temperature.
            num_predict: Max tokens.

        Returns:
            str: Generated text, or None if unavailable.
        """
        if not self.is_available:
            return None

        payload: dict[str, Any] = {
            "model": model or self.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": num_predict,
            },
        }
        if system:
            payload["system"] = system

        start = time.monotonic()
        try:
            resp = requests.post(
                f"{self.base_url}/api/generate",
                json=payload,
                timeout=self.timeout,
            )
            latency_ms = (time.monotonic() - start) * 1000
            resp.raise_for_status()
            data = resp.json()
            log.debug("Ollama generate — latency={l:.0f}ms", l=latency_ms)
            return data.get("response", "")

        except Exception as exc:
            latency_ms = (time.monotonic() - start) * 1000
            log.warning(
                "Ollama generate failed ({l:.0f}ms): {err}",
                l=latency_ms,
                err=str(exc),
            )
            return None

    # ------------------------------------------------------------------
    # Embeddings
    # ------------------------------------------------------------------
    def embed(
        self,
        texts: list[str],
        model: str | None = None,
    ) -> list[list[float]] | None:
        """Generate embeddings for a list of texts.

        Parameters:
            texts: Strings to embed.
            model: Embedding model override.

        Returns:
            list[list[float]]: One embedding vector per input, or None.
        """
        if not self.is_available:
            return None

        model_name = model or self.embed_model
        embeddings: list[list[float]] = []

        try:
            for text in texts:
                resp = requests.post(
                    f"{self.base_url}/api/embeddings",
                    json={"model": model_name, "prompt": text},
                    timeout=self.timeout,
                )
                resp.raise_for_status()
                data = resp.json()
                embeddings.append(data["embedding"])

            log.debug(
                "Ollama embed — {n} texts, dim={d}",
                n=len(texts),
                d=len(embeddings[0]) if embeddings else 0,
            )
            return embeddings

        except Exception as exc:
            log.warning("Ollama embed failed: {err}", err=str(exc))
            return None

    # ------------------------------------------------------------------
    # Model management
    # ------------------------------------------------------------------
    def list_models(self) -> list[dict[str, Any]]:
        """List locally available models.

        Returns:
            list[dict]: Model metadata dicts, or empty list.
        """
        try:
            resp = requests.get(f"{self.base_url}/api/tags", timeout=5)
            resp.raise_for_status()
            return resp.json().get("models", [])
        except Exception as exc:
            log.debug("Could not list Ollama models: {err}", err=str(exc))
            return []

    def get_model_names(self) -> list[str]:
        """Return just the model name strings.

        Returns:
            list[str]: Model names available locally.
        """
        return [m.get("name", "") for m in self.list_models()]

    def pull_model(self, model_name: str) -> bool:
        """Pull a model from the Ollama registry.

        Parameters:
            model_name: Model to pull (e.g. "llama3.1:8b").

        Returns:
            bool: True if pull succeeded.
        """
        log.info("Pulling Ollama model: {m}", m=model_name)
        try:
            resp = requests.post(
                f"{self.base_url}/api/pull",
                json={"name": model_name, "stream": False},
                timeout=600,  # Models can be large
            )
            resp.raise_for_status()
            log.info("Model pull complete: {m}", m=model_name)
            return True
        except Exception as exc:
            log.error("Model pull failed for {m}: {err}", m=model_name, err=str(exc))
            return False

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------
    def health_check(self) -> dict[str, Any]:
        """Return a structured health-check result.

        Returns:
            dict: Keys ``available``, ``latency_ms``, ``models``, ``endpoint``.
        """
        result: dict[str, Any] = {
            "available": False,
            "latency_ms": None,
            "models": [],
            "endpoint": self.base_url,
        }

        start = time.monotonic()
        try:
            resp = requests.get(f"{self.base_url}/api/tags", timeout=5)
            latency = (time.monotonic() - start) * 1000
            if resp.status_code == 200:
                result["available"] = True
                result["latency_ms"] = round(latency, 1)
                result["models"] = [
                    m.get("name", "") for m in resp.json().get("models", [])
                ]
        except Exception:
            pass

        self.is_available = result["available"]
        return result


class OpenAIClient:
    """Client for OpenAI-compatible chat and embedding APIs."""

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.openai.com/v1",
        model: str = "gpt-4o-mini",
        embed_model: str = "text-embedding-3-small",
        timeout: int = 120,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.embed_model = embed_model
        self.timeout = timeout
        self.is_available: bool = False
        self._knowledge_cache: dict[str, str] = {}

        if not self.api_key:
            log.warning("OpenAI API key not configured — falling back to local LLMs")
            return

        health = self.health_check()
        self.is_available = health["available"]
        if self.is_available:
            log.info("OpenAI client connected — {url} ({model})", url=self.base_url, model=self.model)
        else:
            log.warning("OpenAI not available at {url} — falling back to local LLMs", url=self.base_url)

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def load_knowledge(self, doc_name: str) -> str | None:
        return load_knowledge_doc(self._knowledge_cache, doc_name)

    def load_all_knowledge(self) -> str:
        return load_all_knowledge_docs(self._knowledge_cache)

    def chat(
        self,
        messages: list[dict[str, str]],
        model: str | None = None,
        temperature: float = 0.3,
        num_predict: int = 2000,
        system_knowledge: list[str] | None = None,
    ) -> str | None:
        if not self.is_available:
            return None

        messages = inject_knowledge(messages, system_knowledge, self._knowledge_cache)
        payload: dict[str, Any] = {
            "model": model or self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": num_predict,
        }

        start = time.monotonic()
        try:
            resp = requests.post(
                f"{self.base_url}/chat/completions",
                headers=self._headers(),
                json=payload,
                timeout=self.timeout,
            )
            latency_ms = (time.monotonic() - start) * 1000
            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            log.debug(
                "OpenAI chat — model={m}, latency={l:.0f}ms",
                m=data.get("model", model or self.model),
                l=latency_ms,
            )
            return content
        except Exception as exc:
            latency_ms = (time.monotonic() - start) * 1000
            log.warning(
                "OpenAI chat failed ({l:.0f}ms): {err}",
                l=latency_ms,
                err=str(exc),
            )
            self.is_available = False
            return None

    def generate(
        self,
        prompt: str,
        model: str | None = None,
        system: str | None = None,
        temperature: float = 0.3,
        num_predict: int = 2000,
    ) -> str | None:
        messages = [{"role": "user", "content": prompt}]
        if system:
            messages.insert(0, {"role": "system", "content": system})
        return self.chat(
            messages=messages,
            model=model,
            temperature=temperature,
            num_predict=num_predict,
        )

    def embed(
        self,
        texts: list[str],
        model: str | None = None,
    ) -> list[list[float]] | None:
        if not self.is_available:
            return None

        try:
            resp = requests.post(
                f"{self.base_url}/embeddings",
                headers=self._headers(),
                json={"model": model or self.embed_model, "input": texts},
                timeout=self.timeout,
            )
            resp.raise_for_status()
            data = resp.json()
            embeddings = [item["embedding"] for item in data.get("data", [])]
            log.debug(
                "OpenAI embed — {n} texts, dim={d}",
                n=len(texts),
                d=len(embeddings[0]) if embeddings else 0,
            )
            return embeddings
        except Exception as exc:
            log.warning("OpenAI embed failed: {err}", err=str(exc))
            self.is_available = False
            return None

    def list_models(self) -> list[dict[str, Any]]:
        try:
            resp = requests.get(
                f"{self.base_url}/models",
                headers=self._headers(),
                timeout=10,
            )
            resp.raise_for_status()
            return resp.json().get("data", [])
        except Exception as exc:
            log.debug("Could not list OpenAI models: {err}", err=str(exc))
            return []

    def get_model_names(self) -> list[str]:
        return [m.get("id", "") for m in self.list_models()]

    def pull_model(self, model_name: str) -> bool:
        log.debug("OpenAI models are remote; pull skipped for {m}", m=model_name)
        return False

    def health_check(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "available": False,
            "latency_ms": None,
            "models": [],
            "endpoint": self.base_url,
        }

        if not self.api_key:
            return result

        start = time.monotonic()
        try:
            resp = requests.get(
                f"{self.base_url}/models",
                headers=self._headers(),
                timeout=10,
            )
            latency = (time.monotonic() - start) * 1000
            if resp.status_code == 200:
                result["available"] = True
                result["latency_ms"] = round(latency, 1)
                result["models"] = [m.get("id", "") for m in resp.json().get("data", [])]
        except Exception:
            pass

        self.is_available = result["available"]
        return result


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

def get_client() -> Any:
    """Return the best available cached LLM client singleton.

    Provider order:
    1. OpenAI when an API key is configured
    2. llama.cpp when the local server is enabled and reachable
    3. Ollama as the final local fallback
    """
    global _client_instance
    if _client_instance is None:
        from config import settings

        openai_key = (
            settings.OPENAI_API_KEY
            or settings.AGENTS_OPENAI_API_KEY
            or os.getenv("OPENAI_API_KEY", "")
        )
        if openai_key:
            client = OpenAIClient(
                api_key=openai_key,
                base_url=settings.OPENAI_BASE_URL,
                model=settings.OPENAI_CHAT_MODEL,
                embed_model=settings.OPENAI_EMBED_MODEL,
                timeout=settings.OPENAI_TIMEOUT_SECONDS,
            )
            if client.is_available:
                _client_instance = client

        if _client_instance is None and settings.LLAMACPP_ENABLED:
            from llamacpp.client import LlamaCppClient

            client = LlamaCppClient(
                base_url=settings.LLAMACPP_BASE_URL,
                model=settings.LLAMACPP_CHAT_MODEL,
                embed_model=settings.LLAMACPP_EMBED_MODEL,
                timeout=settings.LLAMACPP_TIMEOUT_SECONDS,
            )
            if client.is_available:
                _client_instance = client

        if _client_instance is None:
            _client_instance = OllamaClient(
                base_url=settings.OLLAMA_BASE_URL,
                model=settings.OLLAMA_CHAT_MODEL,
                embed_model=settings.OLLAMA_EMBED_MODEL,
                timeout=settings.OLLAMA_TIMEOUT_SECONDS,
            )
    return _client_instance


if __name__ == "__main__":
    client = get_client()
    hc = client.health_check()
    print(f"Available: {hc['available']}")
    print(f"Latency:   {hc['latency_ms']}ms")
    print(f"Models:    {hc['models']}")
    print(f"Endpoint:  {hc['endpoint']}")
