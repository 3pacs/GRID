"""Configuration for the Hermes analyst bridge.

Reads from the GRID global ``settings`` object when available, falling back
to environment variables so the package and its CLI stay importable in a
degraded environment (e.g. before ``config`` deps are installed). All values
are plain data — no side effects at import time.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

# Conservative default pricing (USD per 1M tokens) used to *estimate* spend
# for the daily cap. Override per deployment via HERMES_PRICE_*_PER_MTOK once
# the production model is chosen. These are intentionally not-too-cheap so the
# cap errs on the safe side if a price is stale.
_DEFAULT_PRICE_INPUT_PER_MTOK = 2.50
_DEFAULT_PRICE_OUTPUT_PER_MTOK = 10.00


def _settings() -> Any | None:
    """Best-effort load of the GRID settings singleton (never raises)."""
    try:
        from config import settings  # type: ignore
        return settings
    except Exception:
        return None


def _get(name: str, default: Any) -> Any:
    """Read ``name`` from GRID settings, else env, else ``default``.

    Env values are coerced to the type of ``default`` (str/int/float/bool).
    """
    s = _settings()
    if s is not None and hasattr(s, name):
        return getattr(s, name)
    raw = os.getenv(name)
    if raw is None:
        return default
    if isinstance(default, bool):
        return raw.strip().lower() in ("1", "true", "yes", "on")
    if isinstance(default, int):
        try:
            return int(raw)
        except ValueError:
            return default
    if isinstance(default, float):
        try:
            return float(raw)
        except ValueError:
            return default
    return raw


@dataclass(frozen=True)
class HermesConfig:
    """Resolved Hermes settings."""

    enabled: bool
    api_key: str
    base_url: str
    model: str
    timeout_seconds: int
    max_completion_tokens: int
    temperature: float | None
    reasoning_effort: str | None
    daily_spend_cap_usd: float
    ledger_path: str
    price_input_per_mtok: float
    price_output_per_mtok: float
    fallback_tier: str
    # Backend selection: "openai" (API key, per-token) or "codex" (the Codex
    # CLI, ChatGPT-subscription auth — the only path to GPT-5.5).
    backend: str = "openai"
    codex_bin: str = "codex"
    codex_model: str = ""          # blank -> Codex CLI default (GPT-5.5)
    codex_timeout_seconds: int = 240
    codex_extra_args: str = ""     # extra `codex exec` flags (shlex-split)
    extra_params: dict[str, Any] = field(default_factory=dict)

    @property
    def configured(self) -> bool:
        """True when an API key is present (a real call is possible)."""
        return bool(self.api_key)


def load_hermes_config() -> HermesConfig:
    """Build a :class:`HermesConfig` from GRID settings / environment."""
    # The Hermes key falls back to the shared OPENAI key so operators only set
    # it once unless they want a dedicated billing key for the analyst bridge.
    api_key = _get("HERMES_API_KEY", "") or _get("OPENAI_API_KEY", "")

    temp_raw = _get("HERMES_TEMPERATURE", "")
    try:
        temperature: float | None = float(temp_raw) if str(temp_raw) != "" else None
    except (TypeError, ValueError):
        temperature = None

    effort = str(_get("HERMES_REASONING_EFFORT", "")).strip() or None

    return HermesConfig(
        enabled=bool(_get("HERMES_ENABLED", True)),
        api_key=api_key,
        base_url=_get("HERMES_BASE_URL", "") or _get("OPENAI_BASE_URL", "https://api.openai.com/v1"),
        model=_get("HERMES_MODEL", "gpt-4o"),
        timeout_seconds=int(_get("HERMES_TIMEOUT_SECONDS", 120)),
        max_completion_tokens=int(_get("HERMES_MAX_COMPLETION_TOKENS", 4096)),
        temperature=temperature,
        reasoning_effort=effort,
        daily_spend_cap_usd=float(_get("HERMES_DAILY_SPEND_CAP_USD", 0.0)),
        ledger_path=_get("HERMES_LEDGER_PATH", "outputs/hermes/spend_ledger.json"),
        price_input_per_mtok=float(_get("HERMES_PRICE_INPUT_PER_MTOK", _DEFAULT_PRICE_INPUT_PER_MTOK)),
        price_output_per_mtok=float(_get("HERMES_PRICE_OUTPUT_PER_MTOK", _DEFAULT_PRICE_OUTPUT_PER_MTOK)),
        fallback_tier=str(_get("HERMES_FALLBACK_TIER", "reason")),
        backend=str(_get("HERMES_BACKEND", "openai")).strip().lower() or "openai",
        codex_bin=str(_get("HERMES_CODEX_BIN", "codex")) or "codex",
        codex_model=str(_get("HERMES_CODEX_MODEL", "")),
        codex_timeout_seconds=int(_get("HERMES_CODEX_TIMEOUT_SECONDS", 240)),
        codex_extra_args=str(_get("HERMES_CODEX_EXTRA_ARGS", "")),
    )
