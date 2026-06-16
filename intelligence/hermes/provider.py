"""Hermes OpenAI provider.

Thin wrapper over the OpenAI Python SDK (>=1.40) that:
  - speaks Chat Completions so it works for both standard and reasoning models,
  - extracts and logs ``reasoning_tokens`` (present on o-series / gpt-5 models),
  - estimates per-call USD cost and records it in a daily :class:`SpendLedger`,
  - refuses to call once the day's spend cap is hit (returns ``None`` so the
    agent can fall back to the local analyst),
  - never raises into the caller — any failure logs a warning and returns
    ``None`` (graceful degradation, matching the rest of GRID's LLM layer).

The SDK import is lazy so importing this module (and running ``cli ping``)
works even when ``openai`` is not installed.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from loguru import logger as log

from .config import HermesConfig, load_hermes_config
from .spend import SpendLedger


@dataclass(frozen=True)
class TokenUsage:
    """Token accounting for a single call."""

    prompt_tokens: int = 0
    completion_tokens: int = 0
    reasoning_tokens: int = 0
    total_tokens: int = 0


@dataclass(frozen=True)
class HermesResponse:
    """Result of a successful Hermes call."""

    text: str
    model: str
    usage: TokenUsage
    cost_usd: float
    latency_ms: float
    provider: str = "hermes"


class HermesProvider:
    """OpenAI-backed analyst provider with spend accounting."""

    def __init__(self, config: HermesConfig | None = None, ledger: SpendLedger | None = None) -> None:
        self.config = config or load_hermes_config()
        self.ledger = ledger or SpendLedger(self.config.ledger_path)

    # -- availability ---------------------------------------------------------
    @property
    def is_available(self) -> bool:
        """True when enabled, has a key, and is under the daily cap."""
        if not (self.config.enabled and self.config.configured):
            return False
        if self.ledger.would_exceed(self.config.daily_spend_cap_usd):
            return False
        return True

    def _client(self):  # pragma: no cover - trivial SDK construction
        from openai import OpenAI

        return OpenAI(
            api_key=self.config.api_key,
            base_url=self.config.base_url,
            timeout=self.config.timeout_seconds,
        )

    # -- cost -----------------------------------------------------------------
    def estimate_cost(self, usage: TokenUsage) -> float:
        """Estimate USD cost for ``usage`` (reasoning tokens bill as output)."""
        c = self.config
        cost = (
            usage.prompt_tokens / 1_000_000 * c.price_input_per_mtok
            + usage.completion_tokens / 1_000_000 * c.price_output_per_mtok
        )
        return round(cost, 6)

    # -- main call ------------------------------------------------------------
    def complete(
        self,
        messages: list[dict[str, str]],
        *,
        model: str | None = None,
        temperature: float | None = None,
        max_completion_tokens: int | None = None,
    ) -> HermesResponse | None:
        """Run one chat completion. Returns ``None`` on any failure/cap/unavailable."""
        if not self.config.enabled or not self.config.configured:
            log.debug("Hermes provider unavailable (enabled={e}, key={k})",
                      e=self.config.enabled, k=bool(self.config.api_key))
            return None
        if self.ledger.would_exceed(self.config.daily_spend_cap_usd):
            log.warning(
                "Hermes daily spend cap reached (${s:.4f} >= ${c:.4f}) — skipping call",
                s=self.ledger.spend_today(), c=self.config.daily_spend_cap_usd,
            )
            return None

        mdl = model or self.config.model
        kwargs = self._build_kwargs(mdl, messages, temperature, max_completion_tokens)

        start = time.monotonic()
        try:
            resp = self._call_with_retry(kwargs)
        except Exception as exc:
            latency = (time.monotonic() - start) * 1000
            log.warning("Hermes call failed ({l:.0f}ms): {e}", l=latency, e=str(exc))
            return None

        latency = (time.monotonic() - start) * 1000
        text = (resp.choices[0].message.content or "").strip() if resp.choices else ""
        usage = self._extract_usage(resp)
        cost = self.estimate_cost(usage)
        self.ledger.record(cost)

        log.info(
            "Hermes ok — model={m} {l:.0f}ms in={i} out={o} reasoning={r} "
            "cost=${c:.4f} spend_today=${s:.4f}",
            m=getattr(resp, "model", mdl), l=latency,
            i=usage.prompt_tokens, o=usage.completion_tokens,
            r=usage.reasoning_tokens, c=cost, s=self.ledger.spend_today(),
        )
        return HermesResponse(
            text=text, model=getattr(resp, "model", mdl), usage=usage,
            cost_usd=cost, latency_ms=latency,
        )

    # -- helpers --------------------------------------------------------------
    def _build_kwargs(
        self,
        model: str,
        messages: list[dict[str, str]],
        temperature: float | None,
        max_completion_tokens: int | None,
    ) -> dict:
        kwargs: dict = {
            "model": model,
            "messages": messages,
            "max_completion_tokens": max_completion_tokens or self.config.max_completion_tokens,
        }
        # temperature is optional: reasoning models reject non-default values,
        # so only send it when explicitly configured/requested.
        temp = temperature if temperature is not None else self.config.temperature
        if temp is not None:
            kwargs["temperature"] = temp
        if self.config.reasoning_effort:
            kwargs["reasoning_effort"] = self.config.reasoning_effort
        if self.config.extra_params:
            kwargs.update(self.config.extra_params)
        return kwargs

    def _call_with_retry(self, kwargs: dict):
        """Call the SDK; on an 'unsupported param' 400, strip it and retry once."""
        client = self._client()
        try:
            return client.chat.completions.create(**kwargs)
        except Exception as exc:
            msg = str(exc).lower()
            stripped = dict(kwargs)
            changed = False
            for param in ("temperature", "reasoning_effort"):
                if param in stripped and param in msg and (
                    "unsupported" in msg or "not support" in msg or "does not support" in msg
                ):
                    stripped.pop(param, None)
                    changed = True
            if not changed:
                raise
            log.warning("Hermes retrying without unsupported params: {p}",
                        p=sorted(set(kwargs) - set(stripped)))
            return client.chat.completions.create(**stripped)

    @staticmethod
    def _extract_usage(resp) -> TokenUsage:
        u = getattr(resp, "usage", None)
        if u is None:
            return TokenUsage()
        details = getattr(u, "completion_tokens_details", None)
        reasoning = getattr(details, "reasoning_tokens", 0) or 0 if details else 0
        return TokenUsage(
            prompt_tokens=getattr(u, "prompt_tokens", 0) or 0,
            completion_tokens=getattr(u, "completion_tokens", 0) or 0,
            reasoning_tokens=int(reasoning),
            total_tokens=getattr(u, "total_tokens", 0) or 0,
        )
