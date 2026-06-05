"""HermesAgent — the GRID analyst bridge.

Primary path: a hosted OpenAI reasoning model via :class:`HermesProvider`.
Fallback path: the local analyst (llama.cpp / Ollama via ``llm.router`` REASON
tier — the same local model the legacy ``llama3.2`` analyst calls use) so a
GRID run never hard-depends on the network. If both paths are unavailable the
agent returns a result with ``source == "unavailable"`` and ``text is None``
rather than raising.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from loguru import logger as log

from .config import HermesConfig, load_hermes_config
from .prompts import build_messages
from .provider import HermesProvider, HermesResponse


@dataclass(frozen=True)
class AnalysisResult:
    """Outcome of an analyst call, annotated with which path served it."""

    text: str | None
    source: str  # "hermes" | "local" | "unavailable"
    model: str | None = None
    cost_usd: float = 0.0
    latency_ms: float = 0.0
    reasoning_tokens: int = 0

    @property
    def ok(self) -> bool:
        return self.text is not None


class HermesAgent:
    """Analyst that prefers Hermes (cloud reasoning) and falls back to local."""

    def __init__(
        self,
        config: HermesConfig | None = None,
        provider: HermesProvider | None = None,
    ) -> None:
        self.config = config or load_hermes_config()
        self.provider = provider or HermesProvider(self.config)

    # -- core -----------------------------------------------------------------
    def analyze(
        self,
        prompt: str,
        *,
        context: str | None = None,
        system: str | None = None,
        temperature: float | None = None,
        max_completion_tokens: int | None = None,
        allow_fallback: bool = True,
    ) -> AnalysisResult:
        """Run an analyst prompt, returning text plus provenance.

        Tries Hermes first; on ``None`` (unconfigured, capped, or errored)
        falls back to the local REASON-tier analyst unless ``allow_fallback``
        is False.
        """
        messages = build_messages(prompt, context=context, system=system)

        resp: HermesResponse | None = self.provider.complete(
            messages,
            temperature=temperature,
            max_completion_tokens=max_completion_tokens,
        )
        if resp is not None:
            return AnalysisResult(
                text=resp.text, source="hermes", model=resp.model,
                cost_usd=resp.cost_usd, latency_ms=resp.latency_ms,
                reasoning_tokens=resp.usage.reasoning_tokens,
            )

        if not allow_fallback:
            return AnalysisResult(text=None, source="unavailable")

        return self._local_fallback(messages)

    def score_hypothesis(
        self,
        hypothesis: str,
        *,
        context: str | None = None,
    ) -> dict[str, Any]:
        """Score a single hypothesis, returning a structured verdict.

        Asks the analyst for strict JSON; on parse failure the raw text is
        returned under ``raw`` so callers can decide how to handle it. This is
        the unit the batch hypothesis scorer calls per candidate.
        """
        prompt = (
            "Score this trading hypothesis. Apply the lever/condition standard. "
            "Return ONLY JSON with keys: probability (0-1), direction "
            '("up"|"down"|"flat"), conviction (0-1), lever (string), '
            "rationale (string), invalidation (string).\n\n"
            f"HYPOTHESIS:\n{hypothesis}"
        )
        result = self.analyze(prompt, context=context)
        verdict: dict[str, Any] = {
            "source": result.source,
            "model": result.model,
            "cost_usd": result.cost_usd,
            "reasoning_tokens": result.reasoning_tokens,
        }
        if not result.ok:
            verdict["error"] = "no analyst available"
            return verdict
        parsed = _parse_json(result.text or "")
        if parsed is None:
            verdict["raw"] = result.text
        else:
            verdict.update(parsed)
        return verdict

    # -- fallback -------------------------------------------------------------
    def _local_fallback(self, messages: list[dict[str, str]]) -> AnalysisResult:
        """Route to the local analyst via the GRID LLM router (REASON tier)."""
        try:
            from llm.router import Tier, get_llm

            tier = _tier_from_str(self.config.fallback_tier, Tier)
            client = get_llm(tier)
            if client is None or not getattr(client, "is_available", False):
                log.debug("Hermes local fallback unavailable (tier={t})", t=self.config.fallback_tier)
                return AnalysisResult(text=None, source="unavailable")
            text = client.chat(messages, temperature=0.3)
            if not text:
                return AnalysisResult(text=None, source="unavailable")
            model = getattr(client, "model", None)
            log.info("Hermes served by local fallback (model={m})", m=model)
            return AnalysisResult(text=text.strip(), source="local", model=model)
        except Exception as exc:  # router/import problems must not raise
            log.warning("Hermes local fallback errored: {e}", e=str(exc))
            return AnalysisResult(text=None, source="unavailable")


def _tier_from_str(name: str, tier_enum: Any) -> Any:
    """Map a config string to a router ``Tier`` (default REASON)."""
    try:
        return tier_enum(name.strip().lower())
    except Exception:
        return tier_enum.REASON


def _parse_json(text: str) -> dict[str, Any] | None:
    """Best-effort JSON extraction (handles bare objects and ```json fences)."""
    text = text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if "\n" in text:
            text = text.split("\n", 1)[1]
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        obj = json.loads(text[start : end + 1])
        return obj if isinstance(obj, dict) else None
    except (ValueError, TypeError):
        return None
