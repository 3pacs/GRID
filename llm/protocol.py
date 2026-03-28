"""
GRID LLM Client Protocol.

Defines the unified interface that all LLM clients must satisfy:
OllamaClient, OpenAIClient, LlamaCppClient. Uses structural subtyping
(typing.Protocol) so existing clients comply without modification.

Usage for type checking:
    from llm.protocol import LLMClient

    def my_function(client: LLMClient) -> str:
        return client.chat([{"role": "user", "content": "Hello"}])
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class LLMClient(Protocol):
    """Structural protocol for all GRID LLM clients.

    Any class with these methods and attributes is a valid LLMClient,
    regardless of whether it explicitly inherits this protocol.
    """

    is_available: bool

    def chat(
        self,
        messages: list[dict[str, str]],
        model: str | None = None,
        temperature: float = 0.3,
        num_predict: int = 2000,
        system_knowledge: list[str] | None = None,
    ) -> str | None:
        """Send a chat completion request.

        Parameters:
            messages: List of message dicts with ``role`` and ``content``.
            model: Model override.
            temperature: Sampling temperature.
            num_predict: Maximum tokens to generate.
            system_knowledge: Knowledge doc names to inject.

        Returns:
            str: The assistant's response, or None if unavailable.
        """
        ...

    def generate(
        self,
        prompt: str,
        model: str | None = None,
        system: str | None = None,
        temperature: float = 0.3,
        num_predict: int = 2000,
    ) -> str | None:
        """Single-turn generation.

        Parameters:
            prompt: The user prompt.
            model: Model override.
            system: System prompt.
            temperature: Sampling temperature.
            num_predict: Max tokens.

        Returns:
            str: Generated text, or None if unavailable.
        """
        ...

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
            list[list[float]]: One embedding per input, or None.
        """
        ...

    def health_check(self) -> dict[str, Any]:
        """Return structured health-check result.

        Returns:
            dict: Keys include ``available``, ``latency_ms``, ``models``, ``endpoint``.
        """
        ...

    def list_models(self) -> list[dict[str, Any]]:
        """List available models.

        Returns:
            list[dict]: Model metadata dicts.
        """
        ...

    def get_model_names(self) -> list[str]:
        """Return just the model name strings.

        Returns:
            list[str]: Model names.
        """
        ...

    def pull_model(self, model_name: str) -> bool:
        """Pull/download a model.

        Parameters:
            model_name: Model to pull.

        Returns:
            bool: True if pull succeeded.
        """
        ...

    def load_knowledge(self, doc_name: str) -> str | None:
        """Load a knowledge document by name.

        Parameters:
            doc_name: Filename (with or without .md).

        Returns:
            str: Document contents, or None if not found.
        """
        ...

    def load_all_knowledge(self) -> str:
        """Load and concatenate all knowledge documents.

        Returns:
            str: Combined knowledge context.
        """
        ...
