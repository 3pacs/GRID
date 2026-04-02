"""
GRID Gemma 3 270M — Tiny Task-Specific Models.

Gemma 3 270M is a 270-million parameter model designed for task-specific
fine-tuning. Once specialized, it executes classification, extraction,
and summarisation tasks at near-zero inference cost on CPU.

GRID deploys up to 3 specialised 270M models:
  - signal_classifier: Classifies incoming signals by domain/urgency
  - anomaly_narrator: Generates one-line anomaly summaries
  - edgar_extractor: Structured extraction from SEC filings

These run on CPU while the 27B analyst and TimesFM share the GPU.

Deployment:
  # Each model runs as a separate llama-server on CPU
  llama-server -m gemma-3-270m-signal-classifier.gguf --port 8082 --threads 4
  llama-server -m gemma-3-270m-anomaly-narrator.gguf --port 8083 --threads 4
  llama-server -m gemma-3-270m-edgar-extractor.gguf --port 8084 --threads 4

  Or via Ollama:
    ollama run gemma3:270m-signal
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import requests
from loguru import logger as log


@dataclass(frozen=True)
class MicroModelConfig:
    """Configuration for a single Gemma 270M micro model.

    Attributes:
        name: Model purpose identifier.
        base_url: Server URL for this model.
        model: Model name/alias.
        system_prompt: Default system prompt for this specialisation.
        max_tokens: Max tokens to generate (keep low for speed).
        temperature: Sampling temperature (low for deterministic tasks).
    """

    name: str
    base_url: str
    model: str
    system_prompt: str
    max_tokens: int = 256
    temperature: float = 0.1


# Pre-configured micro models for GRID
SIGNAL_CLASSIFIER = MicroModelConfig(
    name="signal_classifier",
    base_url="http://localhost:8082",
    model="gemma-3-270m-signal",
    system_prompt=(
        "You are a financial signal classifier. Given a signal description, "
        "classify it into exactly one category and urgency level.\n\n"
        "Categories: rates, credit, equity, volatility, flows, macro, "
        "geopolitical, insider, options, crypto, commodities, fx\n\n"
        "Urgency: critical (act now), high (within hours), "
        "medium (within day), low (informational)\n\n"
        "Respond in exactly this format:\n"
        "CATEGORY: <category>\nURGENCY: <urgency>\nREASON: <one sentence>"
    ),
    max_tokens=64,
    temperature=0.0,
)

ANOMALY_NARRATOR = MicroModelConfig(
    name="anomaly_narrator",
    base_url="http://localhost:8083",
    model="gemma-3-270m-anomaly",
    system_prompt=(
        "You are an anomaly narrator for a trading system. "
        "Given anomaly data (z-scores, values, context), write a single "
        "concise sentence describing what happened and why it matters. "
        "Be specific about numbers and direction. No hedging."
    ),
    max_tokens=128,
    temperature=0.1,
)

EDGAR_EXTRACTOR = MicroModelConfig(
    name="edgar_extractor",
    base_url="http://localhost:8084",
    model="gemma-3-270m-edgar",
    system_prompt=(
        "You are a structured data extractor for SEC EDGAR filings. "
        "Extract the requested fields from the filing text and return "
        "them as a JSON object. Only include fields that are explicitly "
        "stated in the text. Use null for missing fields."
    ),
    max_tokens=512,
    temperature=0.0,
)


class GemmaMicroClient:
    """Client for a single Gemma 270M micro model.

    Lightweight wrapper for task-specific inference. Each micro model
    runs on its own CPU-bound server for parallel processing.

    Parameters:
        config: MicroModelConfig for this specialisation.
        timeout: HTTP timeout in seconds.
    """

    def __init__(
        self,
        config: MicroModelConfig,
        timeout: int = 15,
    ) -> None:
        self.config = config
        self.timeout = timeout
        self.is_available: bool = False

        # Health probe
        try:
            resp = requests.get(f"{config.base_url}/health", timeout=3)
            self.is_available = resp.status_code == 200
        except Exception:
            self.is_available = False

        if self.is_available:
            log.info(
                "Gemma micro '{name}' connected — {url}",
                name=config.name,
                url=config.base_url,
            )
        else:
            log.debug(
                "Gemma micro '{name}' not available at {url}",
                name=config.name,
                url=config.base_url,
            )

    def run(self, input_text: str) -> str | None:
        """Run the specialised task on input text.

        Parameters:
            input_text: The text to process.

        Returns:
            str: Model output, or None if unavailable.
        """
        if not self.is_available:
            # Retry health check
            try:
                resp = requests.get(f"{self.config.base_url}/health", timeout=3)
                self.is_available = resp.status_code == 200
            except Exception:
                pass
            if not self.is_available:
                return None

        messages = [
            {"role": "system", "content": self.config.system_prompt},
            {"role": "user", "content": input_text},
        ]

        payload: dict[str, Any] = {
            "model": self.config.model,
            "messages": messages,
            "max_tokens": self.config.max_tokens,
            "temperature": self.config.temperature,
            "stream": False,
        }

        start = time.monotonic()
        try:
            resp = requests.post(
                f"{self.config.base_url}/v1/chat/completions",
                json=payload,
                timeout=self.timeout,
            )
            latency_ms = (time.monotonic() - start) * 1000

            if resp.status_code >= 400:
                log.warning(
                    "Gemma micro '{name}' error {status} ({l:.0f}ms)",
                    name=self.config.name,
                    status=resp.status_code,
                    l=latency_ms,
                )
                return None

            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            log.debug(
                "Gemma micro '{name}' — {l:.0f}ms",
                name=self.config.name,
                l=latency_ms,
            )
            return content

        except Exception as exc:
            latency_ms = (time.monotonic() - start) * 1000
            log.warning(
                "Gemma micro '{name}' failed ({l:.0f}ms): {err}",
                name=self.config.name,
                l=latency_ms,
                err=str(exc),
            )
            return None

    def health_check(self) -> dict[str, Any]:
        """Return structured health-check result."""
        result: dict[str, Any] = {
            "name": self.config.name,
            "available": False,
            "endpoint": self.config.base_url,
            "model": self.config.model,
            "latency_ms": None,
        }

        start = time.monotonic()
        try:
            resp = requests.get(f"{self.config.base_url}/health", timeout=3)
            latency = (time.monotonic() - start) * 1000
            if resp.status_code == 200:
                result["available"] = True
                result["latency_ms"] = round(latency, 1)
        except Exception:
            pass

        self.is_available = result["available"]
        return result


class GemmaMicroPool:
    """Pool of specialised Gemma 270M micro models.

    Manages the three task-specific models and provides a unified
    interface for signal classification, anomaly narration, and
    structured extraction.

    Parameters:
        configs: Optional custom configs. Defaults to the three standard models.
    """

    def __init__(
        self,
        configs: list[MicroModelConfig] | None = None,
    ) -> None:
        if configs is None:
            configs = [SIGNAL_CLASSIFIER, ANOMALY_NARRATOR, EDGAR_EXTRACTOR]

        self._clients: dict[str, GemmaMicroClient] = {}
        for cfg in configs:
            self._clients[cfg.name] = GemmaMicroClient(cfg)

    def classify_signal(self, signal_text: str) -> str | None:
        """Classify a signal's domain and urgency.

        Parameters:
            signal_text: Signal description to classify.

        Returns:
            str: Classification result (CATEGORY/URGENCY/REASON), or None.
        """
        client = self._clients.get("signal_classifier")
        if client is None:
            return None
        return client.run(signal_text)

    def narrate_anomaly(self, anomaly_data: str) -> str | None:
        """Generate a one-line anomaly summary.

        Parameters:
            anomaly_data: Anomaly details (z-scores, values, context).

        Returns:
            str: One-sentence anomaly narrative, or None.
        """
        client = self._clients.get("anomaly_narrator")
        if client is None:
            return None
        return client.run(anomaly_data)

    def extract_edgar(self, filing_text: str, fields: str) -> str | None:
        """Extract structured data from an SEC filing.

        Parameters:
            filing_text: Raw filing text.
            fields: Comma-separated field names to extract.

        Returns:
            str: JSON string of extracted fields, or None.
        """
        client = self._clients.get("edgar_extractor")
        if client is None:
            return None
        prompt = f"Extract these fields: {fields}\n\nFiling text:\n{filing_text}"
        return client.run(prompt)

    def get_client(self, name: str) -> GemmaMicroClient | None:
        """Get a specific micro client by name."""
        return self._clients.get(name)

    def health_check(self) -> dict[str, Any]:
        """Return health status for all micro models."""
        return {
            name: client.health_check()
            for name, client in self._clients.items()
        }

    @property
    def available_count(self) -> int:
        """Number of micro models currently available."""
        return sum(1 for c in self._clients.values() if c.is_available)


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_pool_instance: GemmaMicroPool | None = None


def get_micro_pool() -> GemmaMicroPool:
    """Return a cached GemmaMicroPool singleton."""
    global _pool_instance
    if _pool_instance is None:
        from config import settings

        configs = []
        if settings.GEMMA_MICRO_CLASSIFIER_URL:
            configs.append(MicroModelConfig(
                name="signal_classifier",
                base_url=settings.GEMMA_MICRO_CLASSIFIER_URL,
                model=SIGNAL_CLASSIFIER.model,
                system_prompt=SIGNAL_CLASSIFIER.system_prompt,
                max_tokens=SIGNAL_CLASSIFIER.max_tokens,
                temperature=SIGNAL_CLASSIFIER.temperature,
            ))
        if settings.GEMMA_MICRO_NARRATOR_URL:
            configs.append(MicroModelConfig(
                name="anomaly_narrator",
                base_url=settings.GEMMA_MICRO_NARRATOR_URL,
                model=ANOMALY_NARRATOR.model,
                system_prompt=ANOMALY_NARRATOR.system_prompt,
                max_tokens=ANOMALY_NARRATOR.max_tokens,
                temperature=ANOMALY_NARRATOR.temperature,
            ))
        if settings.GEMMA_MICRO_EXTRACTOR_URL:
            configs.append(MicroModelConfig(
                name="edgar_extractor",
                base_url=settings.GEMMA_MICRO_EXTRACTOR_URL,
                model=EDGAR_EXTRACTOR.model,
                system_prompt=EDGAR_EXTRACTOR.system_prompt,
                max_tokens=EDGAR_EXTRACTOR.max_tokens,
                temperature=EDGAR_EXTRACTOR.temperature,
            ))

        _pool_instance = GemmaMicroPool(configs if configs else None)
    return _pool_instance
