"""Endpoint registry + model quality gate for LLM autoresearch.

Enumerates every OpenAI-compatible LLM endpoint GRID can reach (the
grid-svr llama.cpp servers, the Ollama fleet nodes, and the gemma micro
models) and applies a *quality bar* so the autoresearch loop never wastes
budget tuning a model that is too weak to be useful.

Design rule (operator directive): a low-quality LLM does more harm than
good. The default bar is **Qwen 3.6 or newer** (or an explicitly
allow-listed equivalent). Models below the bar are excluded unless the
caller opts in with ``include_below_bar=True``.

Everything here is read-only and import-safe: it reads ``config.settings``
defensively via ``getattr`` so a missing setting never raises.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

# Minimum acceptable Qwen version. Expressed as a float so "3.6" > "3.0".
DEFAULT_QWEN_BAR = 3.6

# Model families/aliases explicitly accepted as meeting the bar regardless
# of the Qwen heuristic (e.g. a frontier model from another family). Lower-cased
# substring match against the served model name.
EQUIVALENT_ALLOWLIST: tuple[str, ...] = (
    "qwen3.6",
    "qwen3-next",
    "qwen4",
    "deepseek-v3",
    "deepseek-r1",
    "llama-4",
    "nemotron-4",
)


@dataclass(frozen=True)
class Endpoint:
    """A single reachable LLM serving endpoint.

    Attributes:
        name: Stable identifier (e.g. ``grid-svr-llamacpp``).
        base_url: Base URL of the OpenAI-compatible server.
        model: Served model name/alias.
        host: Logical host (grid-svr, panda, koala, ...).
        kind: ``llamacpp`` | ``ollama`` | ``micro``.
        meets_bar: Whether ``model`` clears the quality bar.
        bar_note: Human-readable reason for the eligibility decision.
        role: Optional usage role (chat/oracle/embed/task).
    """

    name: str
    base_url: str
    model: str
    host: str
    kind: str
    meets_bar: bool
    bar_note: str
    role: str = "chat"


def _qwen_version(model: str) -> float | None:
    """Extract the Qwen major.minor version from a model name.

    ``Qwen3.6-27B`` -> 3.6, ``Qwen3-32B`` -> 3.0, ``qwen2.5:7b`` -> 2.5.
    Returns None if the name is not a Qwen model.
    """
    low = model.lower()
    if "qwen" not in low:
        return None
    # Match the first number after "qwen", allowing "qwen3.6", "qwen3-32b",
    # "qwen3:8b", "qwen2.5". A bare major ("qwen3") is treated as X.0.
    m = re.search(r"qwen[^0-9]*?(\d+)(?:\.(\d+))?", low)
    if not m:
        return None
    major = int(m.group(1))
    minor = int(m.group(2)) if m.group(2) else 0
    return float(f"{major}.{minor}")


def assess_model(model: str, qwen_bar: float = DEFAULT_QWEN_BAR) -> tuple[bool, str]:
    """Decide whether ``model`` clears the quality bar.

    Returns ``(meets_bar, note)``. The note explains the decision so the
    operator can see exactly why a model was included or skipped.
    """
    low = model.lower()
    for allowed in EQUIVALENT_ALLOWLIST:
        if allowed in low:
            return True, f"allow-listed equivalent ('{allowed}')"

    ver = _qwen_version(model)
    if ver is not None:
        if ver >= qwen_bar:
            return True, f"Qwen {ver:g} >= bar {qwen_bar:g}"
        return False, f"Qwen {ver:g} < bar {qwen_bar:g} — below quality bar"

    # Non-Qwen, non-allow-listed family: cannot be compared on the Qwen
    # scale, so default to below-bar and ask the operator to review.
    return False, "non-Qwen family, not allow-listed — review before tuning"


def _setting(name: str, default=None):
    """Read ``config.settings.<name>`` defensively."""
    try:
        from config import settings
    except Exception:
        return default
    return getattr(settings, name, default)


# (name, url_setting, model_setting_or_literal, host, kind, role)
# model field: if it names a settings attribute it is resolved, else used literally.
_KNOWN_ENDPOINTS: tuple[tuple[str, str, str, str, str, str], ...] = (
    ("grid-svr-llamacpp", "LLAMACPP_BASE_URL", "LLAMACPP_CHAT_MODEL", "grid-svr", "llamacpp", "chat"),
    ("grid-svr-llamacpp-oracle", "LLAMACPP_ORACLE_BASE_URL", "LLAMACPP_CHAT_MODEL", "grid-svr", "llamacpp", "oracle"),
    ("ollama-local", "OLLAMA_BASE_URL", "OLLAMA_CHAT_MODEL", "grid-svr", "ollama", "chat"),
    ("ollama-ocr", "OLLAMA_OCR_BASE_URL", "OLLAMA_OCR_CHAT_MODEL", "ocr-node", "ollama", "chat"),
    ("ollama-koala", "OLLAMA_KOALA_BASE_URL", "OLLAMA_KOALA_CHAT_MODEL", "koala", "ollama", "chat"),
    ("ollama-z400", "OLLAMA_Z400_BASE_URL", "OLLAMA_Z400_CHAT_MODEL", "z400", "ollama", "chat"),
    ("gemma-signal-classifier", "GEMMA_MICRO_CLASSIFIER_URL", "gemma-4-e4b-signal-classifier", "grid-svr", "micro", "task"),
    ("gemma-anomaly-narrator", "GEMMA_MICRO_NARRATOR_URL", "gemma-4-e4b-anomaly-narrator", "grid-svr", "micro", "task"),
    ("gemma-edgar-extractor", "GEMMA_MICRO_EXTRACTOR_URL", "gemma-4-e4b-edgar-extractor", "grid-svr", "micro", "task"),
    ("gemma-knowledge-mapper", "GEMMA_MICRO_MAPPER_URL", "gemma-4-e4b-knowledge-mapper", "grid-svr", "micro", "task"),
)


def discover_endpoints(qwen_bar: float = DEFAULT_QWEN_BAR) -> list[Endpoint]:
    """Build the full endpoint list from ``config.settings``.

    Only endpoints whose URL setting is populated are returned. The quality
    bar is assessed but not enforced here — use :func:`eligible_endpoints`
    to filter.
    """
    out: list[Endpoint] = []
    for name, url_setting, model_field, host, kind, role in _KNOWN_ENDPOINTS:
        base_url = _setting(url_setting)
        if not base_url:
            continue
        # Resolve model: prefer a settings attribute of that name, else literal.
        model = _setting(model_field) or model_field
        meets, note = assess_model(str(model), qwen_bar)
        # Gemma micro models are narrow task fine-tunes, not general reasoners.
        # They are exempt from the Qwen bar but flagged so the operator decides.
        if kind == "micro":
            meets, note = False, "task-specific micro model — exempt from general bar; review"
        out.append(
            Endpoint(
                name=name,
                base_url=str(base_url).rstrip("/"),
                model=str(model),
                host=host,
                kind=kind,
                meets_bar=meets,
                bar_note=note,
                role=role,
            )
        )
    return out


def eligible_endpoints(
    include_below_bar: bool = False,
    qwen_bar: float = DEFAULT_QWEN_BAR,
) -> list[Endpoint]:
    """Return endpoints to tune.

    By default only models that clear the quality bar are returned (a weak
    LLM does more harm than good). Pass ``include_below_bar=True`` to tune
    everything regardless — useful only for baseline measurement.
    """
    eps = discover_endpoints(qwen_bar)
    if include_below_bar:
        return eps
    return [e for e in eps if e.meets_bar]
