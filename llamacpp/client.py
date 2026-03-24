"""
GRID llama.cpp server client.

Talks directly to llama-server's OpenAI-compatible API at localhost:8080.
Drop-in replacement for the Ollama client — same public interface
(chat, generate, embed, health_check) so all existing reasoner and
briefing code works unchanged.

No Ollama wrapper overhead. Direct llama.cpp on bare metal.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import requests
from loguru import logger as log

# Module-level cached singleton
_client_instance: LlamaCppClient | None = None

# Knowledge docs directory (shared with ollama module)
_KNOWLEDGE_DIR = Path(__file__).parent.parent / "ollama" / "knowledge"


class LlamaCppClient:
    """Client for the llama-server OpenAI-compatible API.

    Wraps chat completions, embeddings, model listing, and health
    monitoring.  Every public method catches exceptions and returns
    a safe default so GRID never crashes due to the LLM being offline.

    Attributes:
        base_url: Base URL of llama-server (e.g. http://localhost:8080).
        model: Model alias reported by llama-server.
        embed_model: Embedding model alias (same server, or separate).
        timeout: HTTP request timeout in seconds.
        is_available: Whether the server responded at init.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        model: str = "hermes",
        embed_model: str = "hermes",
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

        if self.is_available:
            log.info("llama.cpp server connected — {url}", url=self.base_url)
        else:
            log.warning(
                "llama.cpp server not available at {url} — GRID will operate without it",
                url=self.base_url,
            )

    # ------------------------------------------------------------------
    # Knowledge loading (reuses ollama/knowledge/ docs)
    # ------------------------------------------------------------------
    def load_knowledge(self, doc_name: str) -> str | None:
        """Load a knowledge .md file.

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
        """Send a chat completion request to llama-server.

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
            # Retry health check — server may have started after us
            try:
                resp = requests.get(f"{self.base_url}/health", timeout=3)
                self.is_available = resp.status_code == 200
            except Exception:
                pass
            if not self.is_available:
                return None

        # Inject knowledge into system message if requested
        # Uses TF-IDF + orthogonality selection to pick only the most
        # relevant, non-redundant docs that fit within the token budget.
        if system_knowledge:
            from knowledge.selector import select_and_format

            # Build candidate dict from requested docs
            candidates: dict[str, str] = {}
            for doc in system_knowledge:
                content = self.load_knowledge(doc)
                if content:
                    candidates[doc] = content

            # Extract prompt text for relevance scoring
            prompt_text = " ".join(
                m["content"] for m in messages if m["role"] in ("user", "system")
            )

            # Budget: ~3K tokens worth of knowledge (~12K chars)
            # leaves room for prompt + generation within 4096 ctx
            knowledge_block = select_and_format(
                prompt_text, candidates, char_budget=12000, max_docs=4,
            )

            if knowledge_block:
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
            "max_tokens": num_predict,
            "temperature": temperature,
            "stream": False,
        }

        # Clamp max_tokens so prompt + generation fits in context
        # Rough estimate: 4 chars ≈ 1 token
        total_prompt_chars = sum(len(m["content"]) for m in messages)
        est_prompt_tokens = total_prompt_chars // 3  # conservative
        # Leave headroom for the model's context window (default 4096)
        max_ctx = 4096
        available_for_gen = max(256, max_ctx - est_prompt_tokens - 64)
        if payload["max_tokens"] > available_for_gen:
            log.debug(
                "Clamping max_tokens {orig} → {clamped} (est prompt={p} tok)",
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
                    pass
                log.warning(
                    "llama.cpp chat {status} ({l:.0f}ms): {body}",
                    status=resp.status_code, l=latency_ms, body=error_body,
                )
                return None

            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            model_used = data.get("model", model or self.model)
            tokens = data.get("usage", {})
            log.debug(
                "llama.cpp chat — model={m}, latency={l:.0f}ms, prompt_tok={p}, gen_tok={g}",
                m=model_used,
                l=latency_ms,
                p=tokens.get("prompt_tokens", "?"),
                g=tokens.get("completion_tokens", "?"),
            )
            return content

        except Exception as exc:
            latency_ms = (time.monotonic() - start) * 1000
            log.warning(
                "llama.cpp chat failed ({l:.0f}ms): {err}",
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
        """Generate embeddings for a list of texts.

        Uses llama-server's /v1/embeddings endpoint. Requires
        the server to be started with an embedding-capable model
        or a separate embedding server.

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
                "llama.cpp embed — {n} texts, dim={d}",
                n=len(texts),
                d=len(embeddings[0]) if embeddings else 0,
            )
            return embeddings

        except Exception as exc:
            log.warning("llama.cpp embed failed: {err}", err=str(exc))
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
            log.debug("Could not list llama.cpp models: {err}", err=str(exc))
            return []

    def get_model_names(self) -> list[str]:
        """Return just the model name strings.

        Returns:
            list[str]: Model IDs loaded by the server.
        """
        return [m.get("id", "") for m in self.list_models()]

    def pull_model(self, model_name: str) -> bool:
        """No-op — llama-server loads models at startup.

        Use scripts/start_llamacpp.sh --model /path/to/model.gguf
        to change the model.

        Returns:
            bool: Always False (not supported).
        """
        log.warning(
            "pull_model not supported for llama.cpp — "
            "restart the server with the desired model"
        )
        return False

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------
    def health_check(self) -> dict[str, Any]:
        """Return a structured health-check result.

        llama-server exposes /health which returns JSON with:
        - status: "ok" | "loading model" | "error"
        - slots_idle / slots_processing (if --metrics enabled)

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
                data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
                result["available"] = data.get("status", "ok") == "ok"
                result["latency_ms"] = round(latency, 1)
                result["slots_idle"] = data.get("slots_idle")
                result["slots_processing"] = data.get("slots_processing")
                result["models"] = self.get_model_names()
        except Exception:
            pass

        self.is_available = result["available"]
        return result

    # ------------------------------------------------------------------
    # Server metrics (llama-server specific)
    # ------------------------------------------------------------------
    def get_metrics(self) -> dict[str, Any] | None:
        """Fetch Prometheus-style metrics from llama-server.

        Only available if server was started with --metrics flag.

        Returns:
            dict: Parsed metrics, or None if unavailable.
        """
        try:
            resp = requests.get(f"{self.base_url}/metrics", timeout=5)
            if resp.status_code != 200:
                return None

            # Parse Prometheus text format into a simple dict
            metrics: dict[str, Any] = {}
            for line in resp.text.strip().split("\n"):
                if line.startswith("#"):
                    continue
                parts = line.split(" ", 1)
                if len(parts) == 2:
                    try:
                        metrics[parts[0]] = float(parts[1])
                    except ValueError:
                        metrics[parts[0]] = parts[1]

            return metrics

        except Exception:
            return None


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

def get_client() -> LlamaCppClient:
    """Return a cached LlamaCppClient singleton.

    Returns:
        LlamaCppClient: Shared client instance.
    """
    global _client_instance
    if _client_instance is None:
        from config import settings

        _client_instance = LlamaCppClient(
            base_url=settings.LLAMACPP_BASE_URL,
            model=settings.LLAMACPP_CHAT_MODEL,
            embed_model=settings.LLAMACPP_EMBED_MODEL,
            timeout=settings.LLAMACPP_TIMEOUT_SECONDS,
        )
    return _client_instance


if __name__ == "__main__":
    client = get_client()
    hc = client.health_check()
    print(f"Available: {hc['available']}")
    print(f"Latency:   {hc['latency_ms']}ms")
    print(f"Models:    {hc['models']}")
    print(f"Endpoint:  {hc['endpoint']}")
    print(f"Slots:     idle={hc['slots_idle']}, processing={hc['slots_processing']}")

    metrics = client.get_metrics()
    if metrics:
        print(f"Metrics:   {len(metrics)} entries")
