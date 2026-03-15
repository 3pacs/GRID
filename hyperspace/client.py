# PRIVACY BOUNDARY: This module uses Hyperspace for local inference
# and embeddings only. No GRID signal logic, feature values, discovered
# cluster structures, or hypothesis details are sent to the network.
"""
GRID Hyperspace API client.

Wraps the local OpenAI-compatible endpoint at localhost:8080 provided by the
Hyperspace node.  All methods return ``None`` on failure — GRID must never
depend on Hyperspace availability for core operations.
"""

from __future__ import annotations

import time
from typing import Any

import requests
from loguru import logger as log

# Module-level cached singleton
_client_instance: HyperspaceClient | None = None


class HyperspaceClient:
    """Client for the Hyperspace local OpenAI-compatible API.

    Wraps chat completions, embeddings, and model listing endpoints.
    Every public method catches all exceptions and returns a safe default
    (``None`` or empty list) so GRID never crashes due to Hyperspace.

    Attributes:
        base_url: Base URL of the local Hyperspace API.
        timeout: HTTP request timeout in seconds.
        is_available: Whether the endpoint responded to the initial health check.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8080/v1",
        timeout: int = 30,
    ) -> None:
        """Initialise the Hyperspace client and run a lightweight health check.

        Parameters:
            base_url: Root URL of the OpenAI-compatible API (e.g. ``http://localhost:8080/v1``).
            timeout: HTTP timeout in seconds for all requests.
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.is_available: bool = False

        # Lightweight probe — do not raise on failure
        try:
            resp = requests.get(
                f"{self.base_url}/models",
                timeout=5,
            )
            self.is_available = resp.status_code == 200
        except Exception:
            self.is_available = False

        if self.is_available:
            log.info("Hyperspace client connected — {url}", url=self.base_url)
        else:
            log.warning(
                "Hyperspace node not available at {url} — GRID will operate without it",
                url=self.base_url,
            )

    # ------------------------------------------------------------------
    # Chat completion
    # ------------------------------------------------------------------
    def chat(
        self,
        messages: list[dict[str, str]],
        model: str = "auto",
        max_tokens: int = 1000,
        temperature: float = 0.3,
        stream: bool = False,
    ) -> str | None:
        """Send a chat completion request to the Hyperspace node.

        Parameters:
            messages: List of message dicts with ``role`` and ``content`` keys.
            model: Model name to use (``"auto"`` lets the node choose).
            max_tokens: Maximum tokens in the response.
            temperature: Sampling temperature.
            stream: Whether to stream the response (not yet supported — ignored).

        Returns:
            str: The assistant's response text, or ``None`` if unavailable.
        """
        if not self.is_available:
            return None

        payload: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "stream": False,  # Streaming not implemented in this client
        }

        start = time.monotonic()
        try:
            resp = requests.post(
                f"{self.base_url}/chat/completions",
                json=payload,
                timeout=self.timeout,
            )
            latency_ms = (time.monotonic() - start) * 1000
            resp.raise_for_status()
            data = resp.json()

            content = data["choices"][0]["message"]["content"]
            model_used = data.get("model", model)
            log.debug(
                "Hyperspace chat — model={m}, latency={l:.0f}ms, tokens={t}",
                m=model_used,
                l=latency_ms,
                t=data.get("usage", {}).get("total_tokens", "?"),
            )
            return content

        except Exception as exc:
            latency_ms = (time.monotonic() - start) * 1000
            log.warning(
                "Hyperspace chat failed ({l:.0f}ms): {err}",
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
        model: str = "all-MiniLM-L6-v2",
    ) -> list[list[float]] | None:
        """Generate embeddings for a list of texts.

        Parameters:
            texts: Strings to embed.
            model: Embedding model name.

        Returns:
            list[list[float]]: One embedding vector per input text,
                or ``None`` if unavailable.
        """
        if not self.is_available:
            return None

        payload: dict[str, Any] = {
            "model": model,
            "input": texts,
        }

        try:
            resp = requests.post(
                f"{self.base_url}/embeddings",
                json=payload,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            data = resp.json()

            embeddings = [item["embedding"] for item in data["data"]]
            log.debug(
                "Hyperspace embed — {n} texts, dim={d}",
                n=len(texts),
                d=len(embeddings[0]) if embeddings else 0,
            )
            return embeddings

        except Exception as exc:
            log.warning("Hyperspace embed failed: {err}", err=str(exc))
            return None

    # ------------------------------------------------------------------
    # Model listing
    # ------------------------------------------------------------------
    def get_available_models(self) -> list[str]:
        """List models available on the local Hyperspace node.

        Returns:
            list[str]: Model name strings, or empty list if unavailable.
        """
        try:
            resp = requests.get(
                f"{self.base_url}/models",
                timeout=5,
            )
            resp.raise_for_status()
            data = resp.json()
            return [m["id"] for m in data.get("data", [])]
        except Exception as exc:
            log.debug("Could not list Hyperspace models: {err}", err=str(exc))
            return []

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------
    def health_check(self) -> dict[str, Any]:
        """Return a structured health-check result.

        Returns:
            dict: Keys ``available`` (bool), ``latency_ms`` (float | None),
                  ``models`` (list[str]), ``endpoint`` (str).
        """
        result: dict[str, Any] = {
            "available": False,
            "latency_ms": None,
            "models": [],
            "endpoint": self.base_url,
        }

        start = time.monotonic()
        try:
            resp = requests.get(f"{self.base_url}/models", timeout=5)
            latency = (time.monotonic() - start) * 1000
            if resp.status_code == 200:
                result["available"] = True
                result["latency_ms"] = round(latency, 1)
                result["models"] = [
                    m["id"] for m in resp.json().get("data", [])
                ]
        except Exception:
            pass

        self.is_available = result["available"]
        return result


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

def get_client() -> HyperspaceClient:
    """Return a cached HyperspaceClient singleton.

    Creates the client on first call.  Subsequent calls return the same
    instance.

    Returns:
        HyperspaceClient: Shared client instance.
    """
    global _client_instance
    if _client_instance is None:
        from config import settings

        _client_instance = HyperspaceClient(
            base_url=settings.HYPERSPACE_BASE_URL,
            timeout=settings.HYPERSPACE_TIMEOUT_SECONDS,
        )
    return _client_instance


if __name__ == "__main__":
    client = get_client()
    hc = client.health_check()
    print(f"Available: {hc['available']}")
    print(f"Latency:   {hc['latency_ms']}ms")
    print(f"Models:    {hc['models']}")
    print(f"Endpoint:  {hc['endpoint']}")
