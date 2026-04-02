"""
GRID Gemma 3 integration.

Provides a Gemma 3 27B QAT client that runs locally via llama.cpp or Ollama,
exposing the same LLMClient protocol used by all GRID LLM backends.

Gemma 3 27B QAT features:
  - 128K-token context window
  - Multimodal (text + images)
  - QAT (Quantization-Aware Training) for efficient GPU inference
  - Function calling support
  - Runs on a single RTX 3090 (24 GB VRAM)
"""

from gemma.client import GemmaClient, get_client

__all__ = ["GemmaClient", "get_client"]
