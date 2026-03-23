"""
GRID Ollama client.

Wraps the local Ollama REST API at localhost:11434 for chat completions,
embeddings, and model management. All methods return ``None`` on failure —
GRID must never depend on Ollama availability for core operations.

Ollama exposes an OpenAI-compatible endpoint at /v1/ AND a native API
at /api/. This client uses the native API for richer control (streaming,
context management, model pulling).
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import requests
from loguru import logger as log

# Module-level cached singleton
_client_instance: OllamaClient | None = None

# Knowledge docs directory (sibling to this file)
_KNOWLEDGE_DIR = Path(__file__).parent / "knowledge"


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
        if doc_name in self._knowledge_cache:
            return self._knowledge_cache[doc_name]

        if not doc_name.endswith(".md"):
            doc_name += ".md"

        path = _KNOWLEDGE_DIR / doc_name
        if not path.exists():
            log.debug("Knowledge doc not found: {p}", p=path)
            return None

        content = path.read_text(encoding="utf-8")
        self._knowledge_cache[doc_name] = content
        log.debug("Loaded knowledge doc: {p} ({n} chars)", p=doc_name, n=len(content))
        return content

    def load_all_knowledge(self) -> str:
        """Load and concatenate all knowledge .md files.

        Returns:
            str: Combined knowledge context.
        """
        if not _KNOWLEDGE_DIR.exists():
            return ""

        parts: list[str] = []
        for path in sorted(_KNOWLEDGE_DIR.glob("*.md")):
            content = self.load_knowledge(path.name)
            if content:
                parts.append(f"--- {path.stem} ---\n{content}")

        combined = "\n\n".join(parts)
        log.info("Loaded {n} knowledge docs ({c} total chars)", n=len(parts), c=len(combined))
        return combined

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

        # Inject knowledge into system message if requested
        if system_knowledge:
            knowledge_parts = []
            for doc in system_knowledge:
                content = self.load_knowledge(doc)
                if content:
                    knowledge_parts.append(content)
            if knowledge_parts:
                knowledge_block = "\n\n".join(knowledge_parts)
                # Prepend knowledge to first system message, or create one
                has_system = any(m["role"] == "system" for m in messages)
                if has_system:
                    for m in messages:
                        if m["role"] == "system":
                            m["content"] = (
                                f"{m['content']}\n\n"
                                f"## Reference Knowledge\n\n{knowledge_block}"
                            )
                            break
                else:
                    messages.insert(0, {
                        "role": "system",
                        "content": f"## Reference Knowledge\n\n{knowledge_block}",
                    })

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


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

def get_client() -> OllamaClient:
    """Return a cached LLM client singleton.

    When ``LLAMACPP_ENABLED`` is True (the default), returns a
    ``LlamaCppClient`` from the ``llamacpp`` module instead of the
    Ollama client.  Both expose the same interface (chat, generate,
    embed, health_check, load_knowledge, etc.) so all downstream
    code works unchanged.

    Returns:
        OllamaClient | LlamaCppClient: Shared client instance.
    """
    global _client_instance
    if _client_instance is None:
        from config import settings

        if settings.LLAMACPP_ENABLED:
            from llamacpp.client import LlamaCppClient

            _client_instance = LlamaCppClient(  # type: ignore[assignment]
                base_url=settings.LLAMACPP_BASE_URL,
                model=settings.LLAMACPP_CHAT_MODEL,
                embed_model=settings.LLAMACPP_EMBED_MODEL,
                timeout=settings.LLAMACPP_TIMEOUT_SECONDS,
            )
        else:
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
