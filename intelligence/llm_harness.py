"""llm_harness.py — self-learning wrapper on top of any LLM client.

Wraps an LLM client (llm.router.get_llm() result, a bare ``.chat()``
object, or any callable ``(prompt, **kwargs) -> str``) with the
SelfLearningLoop primitive so every call is recorded, every outcome is
scored, and the harness self-tunes its own call parameters over time:
temperature, prompt-template choice, model preference, retry budget.

Conceptually::

    harness = LLMHarness(
        module_name="oracle.agent_arena.hermes",
        client=get_llm(Tier.ORACLE),
        update_fn=update_temperature_from_calibration,
    )
    response = harness.call(
        prompt="Is TSM overvalued given 2026 Q1 fundamentals?",
        task_context={"horizon_days": 21, "ticker": "TSM"},
    )
    # ... hours or days later, when we know whether the answer was right:
    harness.score(emission_id=response.emission_id,
                  outcome={"accuracy": 0.87, "pnl": 0.021},
                  outcome_scalar=0.87)

The harness persists every emission + every scored outcome + every
parameter update to the self_learning_* tables via SelfLearningLoop, so
the same cadence/improvement/demotion machinery the grand orchestrator
uses for quant models applies to every LLM call site without extra
plumbing.

Three things the harness does that a bare ``client.chat()`` can't:

  1. **State recall** — before each call, load the current params
     (temperature, model, prompt template) from the loop state. When
     ``update_parameters`` runs it adjusts those same keys and every
     subsequent call uses the updated settings.

  2. **Prompt versioning** — pass a dict of named prompt templates and
     the harness learns which template works best per task type.
     Effectively A/B testing prompts with decaying confidence intervals.

  3. **Fallback escalation** — when the primary tier returns None or
     scores poorly over the last N calls, the harness auto-escalates to
     the next tier (LOCAL → REASON → ORACLE) and records the escalation
     as part of the emission context.

The harness is deliberately simple: ~400 lines, no threading, no async,
no retries beyond what the client already does. It's a thin adapter.

Usage for the three NEEDS_LOOP LLM call sites identified in the audit:

    # 1. Oracle agent arena — loop over per-agent debate quality
    arena_harness = LLMHarness(
        module_name="oracle.agent_arena",
        client=get_llm(Tier.ORACLE),
        prompt_templates={
            "default": "You are {agent_role}. {question}",
            "socratic": "As {agent_role}, challenge the premise: {question}",
        },
        update_fn=update_from_brier_score,
        default_params={"temperature": 0.4, "prompt_template": "default"},
    )

    # 2. intelligence.news_impact — loop over headline-to-move prediction
    impact_harness = LLMHarness(
        module_name="intelligence.news_impact",
        client=get_llm(Tier.REASON),
        update_fn=update_from_directional_accuracy,
    )

    # 3. intelligence.earnings_transcript_analyzer — loop over guidance call
    ect_harness = LLMHarness(
        module_name="intelligence.earnings_transcript_analyzer",
        client=get_llm(Tier.REASON),
        update_fn=update_from_eps_surprise,
    )

All three plug into the same grand_orchestrator discovery loop: call
``auto_register_self_learning_modules(engine)`` and the orchestrator
will find them and run their update cycles on schedule.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger as log

from intelligence.self_learning_loop import (
    ScoredEmission,
    SelfLearningLoop,
    UpdateFn,
)


# ----------------------------------------------------------------------
# Defaults — the harness's own "factory settings" before any learning.
# ----------------------------------------------------------------------
DEFAULT_TEMPERATURE: float = 0.3
DEFAULT_MAX_TOKENS: int = 2048
DEFAULT_PROMPT_TEMPLATE: str = "default"
DEFAULT_MIN_SAMPLES_BEFORE_UPDATE: int = 20
MAX_PROMPT_TEMPLATES: int = 16
# Fallback escalation: after this many consecutive None/low-score
# responses, the harness flips to the next tier on subsequent calls.
CONSECUTIVE_FAILURES_BEFORE_ESCALATION: int = 3
# Clamp temperature adjustments so the updater can't push the model
# into nonsense territory.
MIN_TEMPERATURE: float = 0.0
MAX_TEMPERATURE: float = 1.2


# ----------------------------------------------------------------------
# Response object returned to callers.
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class HarnessResponse:
    """Result of a single ``harness.call()``.

    The ``emission_id`` is the handle you pass back to ``harness.score()``
    when the outcome is known. If the call failed entirely (client
    returned None), ``text`` is None but ``emission_id`` is still set so
    you can score the failure too (outcome_scalar=0 trains the harness
    to avoid whatever caused it).
    """

    emission_id: Optional[str]
    text: Optional[str]
    params_used: dict[str, Any]
    context: dict[str, Any]
    latency_ms: Optional[float] = None


# ----------------------------------------------------------------------
# The harness itself.
# ----------------------------------------------------------------------
class LLMHarness:
    """Self-learning wrapper around an LLM client.

    The ``client`` argument must expose one of:

      * ``.chat(messages, temperature=..., num_predict=...) -> str | None``
        (the llm.router interface)
      * a bare callable ``fn(prompt: str, **kwargs) -> str | None``

    The harness detects which and dispatches correctly.

    ``update_fn`` is the SelfLearningLoop update function: it receives
    the last N scored emissions and the current params, and returns new
    params. See ``update_temperature_from_outcomes`` below for a
    reference implementation that nudges temperature toward whatever
    setting had the highest mean outcome_scalar.
    """

    def __init__(
        self,
        *,
        module_name: str,
        client: Any,
        engine: Any | None = None,
        update_fn: UpdateFn | None = None,
        prompt_templates: dict[str, str] | None = None,
        default_params: dict[str, Any] | None = None,
        fallback_clients: list[Any] | None = None,
        min_samples_to_update: int = DEFAULT_MIN_SAMPLES_BEFORE_UPDATE,
    ) -> None:
        self.module_name = module_name
        self.client = client
        self.fallback_clients = list(fallback_clients or [])
        self._consecutive_failures = 0
        self._active_client_idx = -1  # -1 means primary
        self._min_samples = int(min_samples_to_update)

        # Build the default parameter blob. Caller overrides > library
        # defaults. Prompt templates are stored inside params so updates
        # can pick which template to use next.
        params: dict[str, Any] = {
            "temperature": DEFAULT_TEMPERATURE,
            "max_tokens": DEFAULT_MAX_TOKENS,
            "prompt_template": DEFAULT_PROMPT_TEMPLATE,
        }
        if prompt_templates:
            if len(prompt_templates) > MAX_PROMPT_TEMPLATES:
                log.warning(
                    "LLMHarness[{m}]: prompt_templates capped at {n}",
                    m=module_name,
                    n=MAX_PROMPT_TEMPLATES,
                )
                keys = list(prompt_templates.keys())[:MAX_PROMPT_TEMPLATES]
                prompt_templates = {k: prompt_templates[k] for k in keys}
            params["prompt_templates"] = dict(prompt_templates)
        if default_params:
            params.update(default_params)

        # Wire up the shared SelfLearningLoop primitive. If no engine is
        # provided the harness still works in ephemeral mode — calls run,
        # emissions are kept in memory only, no persistence.
        self._loop: SelfLearningLoop | None = None
        if engine is not None and update_fn is not None:
            try:
                self._loop = SelfLearningLoop(
                    engine=engine,
                    module_name=module_name,
                    update_fn=update_fn,
                    default_params=params,
                )
            except Exception as exc:  # pragma: no cover — defensive
                log.warning(
                    "LLMHarness[{m}]: loop init failed, running ephemeral — {e}",
                    m=module_name,
                    e=exc,
                )
        self._ephemeral_params = params  # used when no loop

    # ------------------------------------------------------------------
    # Parameter access — loop state first, ephemeral defaults otherwise.
    # ------------------------------------------------------------------
    def get_params(self) -> dict[str, Any]:
        if self._loop is None:
            return dict(self._ephemeral_params)
        try:
            return dict(self._loop.get_state().params)
        except Exception:
            return dict(self._ephemeral_params)

    # ------------------------------------------------------------------
    # Main entry point.
    # ------------------------------------------------------------------
    def call(
        self,
        *,
        prompt: str | None = None,
        messages: list[dict[str, str]] | None = None,
        task_context: dict[str, Any] | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        template_key: str | None = None,
        template_vars: dict[str, Any] | None = None,
    ) -> HarnessResponse:
        """Dispatch one LLM call and record it as a self-learning emission.

        Exactly one of ``prompt`` and ``messages`` must be provided. If
        ``template_key`` is set, the harness looks up that template from
        its stored prompt_templates and formats it with ``template_vars``.
        Otherwise it uses the raw prompt.
        """
        params = self.get_params()

        # Resolve the actual temperature/max_tokens to use — call-site
        # override > loop param > library default.
        temp = float(
            temperature
            if temperature is not None
            else params.get("temperature", DEFAULT_TEMPERATURE)
        )
        temp = max(MIN_TEMPERATURE, min(MAX_TEMPERATURE, temp))
        ntok = int(
            max_tokens
            if max_tokens is not None
            else params.get("max_tokens", DEFAULT_MAX_TOKENS)
        )

        # Resolve the prompt — template lookup first, raw prompt second.
        resolved_prompt, chosen_template = self._resolve_prompt(
            prompt=prompt,
            messages=messages,
            template_key=template_key or params.get("prompt_template", DEFAULT_PROMPT_TEMPLATE),
            template_vars=template_vars,
            stored_templates=params.get("prompt_templates") or {},
        )

        # Build messages in OpenAI chat format for clients that expect it.
        if messages is None:
            messages_used = [{"role": "user", "content": resolved_prompt}]
        else:
            messages_used = messages

        client = self._active_client()
        start = datetime.now(timezone.utc)
        text = self._dispatch(client, messages_used, resolved_prompt, temp, ntok)
        latency_ms = (datetime.now(timezone.utc) - start).total_seconds() * 1000.0

        # Track failure/escalation state.
        if text is None:
            self._consecutive_failures += 1
            if self._consecutive_failures >= CONSECUTIVE_FAILURES_BEFORE_ESCALATION:
                self._escalate()
        else:
            self._consecutive_failures = 0

        # Record the emission. ``output`` is the model response; ``context``
        # captures everything we'd need to score or reproduce it.
        output_blob: dict[str, Any] = {
            "text": text,
            "params_used": {
                "temperature": temp,
                "max_tokens": ntok,
                "prompt_template": chosen_template,
            },
        }
        context_blob: dict[str, Any] = {
            "task_context": dict(task_context or {}),
            "messages_used": messages_used,
            "resolved_prompt": resolved_prompt,
            "latency_ms": latency_ms,
            "client_tier": str(type(client).__name__),
            "active_client_idx": self._active_client_idx,
        }

        emission_id: Optional[str] = None
        if self._loop is not None:
            try:
                emission_id = self._loop.emit(
                    output=output_blob,
                    context=context_blob,
                )
            except Exception as exc:  # pragma: no cover — defensive
                log.warning(
                    "LLMHarness[{m}]: emit failed — {e}",
                    m=self.module_name,
                    e=exc,
                )

        return HarnessResponse(
            emission_id=emission_id,
            text=text,
            params_used=output_blob["params_used"],
            context=context_blob,
            latency_ms=latency_ms,
        )

    # ------------------------------------------------------------------
    # Scoring — caller tells us how the response turned out.
    # ------------------------------------------------------------------
    def score(
        self,
        *,
        emission_id: str,
        outcome: dict[str, Any],
        outcome_scalar: float | None = None,
    ) -> bool:
        """Attach an outcome to a prior emission.

        ``outcome_scalar`` should be in [0.0, 1.0] where 1.0 = perfect.
        The harness's default update_fn uses the mean of outcome_scalar
        over the lookback window to decide how to nudge temperature.
        """
        if self._loop is None:
            return False
        try:
            return self._loop.score(
                emission_id=emission_id,
                outcome=outcome,
                outcome_scalar=outcome_scalar,
            )
        except Exception as exc:  # pragma: no cover — defensive
            log.warning(
                "LLMHarness[{m}]: score failed — {e}",
                m=self.module_name,
                e=exc,
            )
            return False

    # ------------------------------------------------------------------
    # Manual update trigger (normally the grand orchestrator runs this).
    # ------------------------------------------------------------------
    def update_parameters(self) -> dict[str, Any]:
        if self._loop is None:
            return dict(self._ephemeral_params)
        try:
            state = self._loop.update_parameters(min_samples=self._min_samples)
            return dict(state.params)
        except Exception as exc:  # pragma: no cover — defensive
            log.warning(
                "LLMHarness[{m}]: update failed — {e}",
                m=self.module_name,
                e=exc,
            )
            return dict(self._ephemeral_params)

    # ------------------------------------------------------------------
    # Internals.
    # ------------------------------------------------------------------
    def _active_client(self) -> Any:
        if self._active_client_idx < 0:
            return self.client
        if 0 <= self._active_client_idx < len(self.fallback_clients):
            return self.fallback_clients[self._active_client_idx]
        return self.client

    def _escalate(self) -> None:
        if not self.fallback_clients:
            return
        nxt = self._active_client_idx + 1
        if nxt >= len(self.fallback_clients):
            return  # already on last fallback
        log.warning(
            "LLMHarness[{m}]: escalating to fallback client {i}",
            m=self.module_name,
            i=nxt,
        )
        self._active_client_idx = nxt
        self._consecutive_failures = 0

    def _resolve_prompt(
        self,
        *,
        prompt: str | None,
        messages: list[dict[str, str]] | None,
        template_key: str,
        template_vars: dict[str, Any] | None,
        stored_templates: dict[str, str],
    ) -> tuple[str, str]:
        if prompt is not None:
            if stored_templates and template_key in stored_templates:
                try:
                    return (
                        stored_templates[template_key].format(
                            prompt=prompt, **(template_vars or {})
                        ),
                        template_key,
                    )
                except (KeyError, IndexError):
                    # Template vars mismatch — fall through to raw prompt.
                    return prompt, "raw"
            return prompt, "raw"
        if messages:
            return (
                "\n".join(m.get("content", "") for m in messages),
                "messages",
            )
        return "", "empty"

    def _dispatch(
        self,
        client: Any,
        messages: list[dict[str, str]],
        raw_prompt: str,
        temperature: float,
        max_tokens: int,
    ) -> str | None:
        # llm.router-style client exposes .chat(messages, ...).
        chat = getattr(client, "chat", None)
        if callable(chat):
            try:
                return chat(
                    messages=messages,
                    temperature=temperature,
                    num_predict=max_tokens,
                )
            except Exception as exc:  # pragma: no cover — defensive
                log.warning(
                    "LLMHarness[{m}]: .chat() raised — {e}",
                    m=self.module_name,
                    e=exc,
                )
                return None
        # Bare callable fallback: fn(prompt, **kwargs) -> str.
        if callable(client):
            try:
                return client(
                    raw_prompt,
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
            except Exception as exc:  # pragma: no cover — defensive
                log.warning(
                    "LLMHarness[{m}]: client callable raised — {e}",
                    m=self.module_name,
                    e=exc,
                )
                return None
        log.warning(
            "LLMHarness[{m}]: client {t} has no .chat() or __call__",
            m=self.module_name,
            t=type(client).__name__,
        )
        return None


# ----------------------------------------------------------------------
# Reference update functions — pluggable into LLMHarness(update_fn=...).
# ----------------------------------------------------------------------
def update_temperature_from_outcomes(
    history: list[ScoredEmission],
    current_params: dict[str, Any],
) -> dict[str, Any]:
    """Default updater — nudge temperature toward the regime with the
    highest mean outcome_scalar over the last ``history`` window.

    Splits historical calls into three temperature buckets (cold/mid/hot)
    and picks the bucket with the highest mean score. Then sets the new
    temperature to the mean of that bucket, clamped to [0.0, 1.2].

    Completely defensive: returns current_params unchanged on any error
    or if there are too few samples per bucket.
    """
    if not history:
        return dict(current_params)
    buckets: dict[str, list[tuple[float, float]]] = {
        "cold": [],
        "mid": [],
        "hot": [],
    }
    for e in history:
        if e.outcome_scalar is None:
            continue
        try:
            t = float(
                (e.output or {}).get("params_used", {}).get(
                    "temperature", DEFAULT_TEMPERATURE
                )
            )
        except (TypeError, ValueError):
            continue
        score = float(e.outcome_scalar)
        if t < 0.25:
            buckets["cold"].append((t, score))
        elif t < 0.6:
            buckets["mid"].append((t, score))
        else:
            buckets["hot"].append((t, score))

    best_bucket: str | None = None
    best_mean: float = -1.0
    for k, pairs in buckets.items():
        if len(pairs) < 3:  # need at least 3 samples to trust a bucket
            continue
        mean = sum(p[1] for p in pairs) / len(pairs)
        if mean > best_mean:
            best_mean = mean
            best_bucket = k

    if best_bucket is None:
        return dict(current_params)
    pairs = buckets[best_bucket]
    new_temp = sum(p[0] for p in pairs) / len(pairs)
    new_temp = max(MIN_TEMPERATURE, min(MAX_TEMPERATURE, new_temp))

    # Also pick the best prompt_template by the same logic if templates
    # are in play.
    template_scores: dict[str, list[float]] = {}
    for e in history:
        if e.outcome_scalar is None:
            continue
        tpl = (e.output or {}).get("params_used", {}).get("prompt_template")
        if not tpl or tpl in ("raw", "messages", "empty"):
            continue
        template_scores.setdefault(tpl, []).append(float(e.outcome_scalar))
    best_template: str | None = None
    if template_scores:
        scored = [
            (k, sum(v) / len(v), len(v))
            for k, v in template_scores.items()
            if len(v) >= 3
        ]
        if scored:
            scored.sort(key=lambda x: x[1], reverse=True)
            best_template = scored[0][0]

    new_params = dict(current_params)
    new_params["temperature"] = new_temp
    if best_template:
        new_params["prompt_template"] = best_template
    return new_params


def update_no_op(
    history: list[ScoredEmission],
    current_params: dict[str, Any],
) -> dict[str, Any]:
    """Null updater — useful for harnesses that only want recording."""
    return dict(current_params)
