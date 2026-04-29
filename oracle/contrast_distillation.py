"""
Oracle Contrast-Distillation — ReasoningBank-style strategic lesson extraction.

Inspired by ReasoningBank's MaTTS-parallel concept: when parallel rollouts of
the same prediction DISAGREE, the disagreement itself is signal. An LLM-as-judge
contrasts the divergent rollouts to extract a sharper strategic lesson than any
single rollout produces. That lesson then feeds the memory bank for future
retrieval.

This module is pure logic — it does NOT touch the database. Persistence is
handled by ``intelligence.reasoning_bank.write_reasoning_lesson``.

Public surface
--------------
- ``ContrastResult`` — frozen dataclass with the distilled lesson.
- ``compute_divergence(rollouts)`` — pure-numeric divergence score in [0, 1].
- ``distill_contrast(rollouts, ...)`` — ask the LLM to contrast rollouts and
  return a ``ContrastResult``, or ``None`` if divergence is too low / on any
  failure.

Defensive contract
------------------
Every external boundary (LLM call, JSON parsing, attribute access on rollout
dicts) is wrapped in ``try/except``. A failure here MUST never break the live
prediction path — callers should treat ``None`` as "skip the lesson" and move
on.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Callable

from loguru import logger as log


# ── Public dataclass ────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ContrastResult:
    """Distilled strategic lesson from a set of disagreeing rollouts."""

    title: str
    description: str
    content: str
    confidence: float
    divergence_score: float


# ── Divergence metric ───────────────────────────────────────────────────────

def _coerce_float(value: Any, default: float = 0.5) -> float:
    """Best-effort float coercion; returns ``default`` on any failure."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_verdict(verdict: Any) -> str:
    """Map a verdict-ish value to one of bullish/bearish/neutral."""
    if not isinstance(verdict, str):
        return "neutral"
    v = verdict.strip().lower()
    if v in ("bullish", "up", "long", "call", "buy"):
        return "bullish"
    if v in ("bearish", "down", "short", "put", "sell"):
        return "bearish"
    return "neutral"


def compute_divergence(rollouts: list[dict]) -> float:
    """Numeric divergence across rollout predictions, in [0, 1].

    Formula: ``0.5 * clip(variance(prob_up) * 4, 0, 1) + 0.5 *
    fraction_of_minority_verdicts``. Variance of two extreme probs (0 and 1) is
    0.25, so the *4 factor maps unanimity → 0 and extreme-split → 1.
    """
    if not isinstance(rollouts, list) or len(rollouts) < 2:
        return 0.0

    try:
        probs: list[float] = []
        verdicts: list[str] = []
        for r in rollouts:
            if not isinstance(r, dict):
                continue
            probs.append(_coerce_float(r.get("prob_up"), default=0.5))
            verdicts.append(_normalize_verdict(r.get("verdict")))

        if len(probs) < 2:
            return 0.0

        mean = sum(probs) / len(probs)
        variance = sum((p - mean) ** 2 for p in probs) / len(probs)
        prob_term = max(0.0, min(1.0, variance * 4.0))

        # Fraction of minority verdicts: 1 - (count_of_majority / total).
        directional = [v for v in verdicts if v != "neutral"]
        if directional:
            bull = sum(1 for v in directional if v == "bullish")
            bear = sum(1 for v in directional if v == "bearish")
            majority = max(bull, bear)
            verdict_term = 1.0 - (majority / len(directional))
        else:
            verdict_term = 0.0

        score = 0.5 * prob_term + 0.5 * verdict_term
        return max(0.0, min(1.0, score))
    except Exception as exc:  # noqa: BLE001 — defensive boundary
        log.debug("compute_divergence failed: {}", exc)
        return 0.0


# ── LLM-driven distillation ─────────────────────────────────────────────────

_MAX_OUTPUT_TOKENS = 512


def _format_rollout_summary(rollouts: list[dict]) -> str:
    """Format rollouts as a compact text block for the prompt."""
    lines: list[str] = []
    for idx, r in enumerate(rollouts, start=1):
        if not isinstance(r, dict):
            continue
        verdict = _normalize_verdict(r.get("verdict"))
        prob_up = _coerce_float(r.get("prob_up"), default=0.5)
        confidence = _coerce_float(r.get("confidence"), default=0.0)
        model_name = r.get("model_name") or r.get("name") or f"model_{idx}"
        contributions = r.get("top_contributions") or r.get("signals") or []
        if isinstance(contributions, list):
            contrib_str = ", ".join(str(c) for c in contributions[:5])
        else:
            contrib_str = str(contributions)[:200]
        lines.append(
            f"  Rollout {idx} [{model_name}]: "
            f"verdict={verdict}, prob_up={prob_up:.3f}, "
            f"confidence={confidence:.3f}, top_signals=[{contrib_str}]"
        )
    return "\n".join(lines)


def _build_prompt(
    rollouts: list[dict],
    *,
    ticker: str,
    horizon: str,
    regime: str | None,
    divergence: float,
) -> str:
    """Build the contrast-distillation prompt for the REASON-tier LLM."""
    rollout_block = _format_rollout_summary(rollouts)
    regime_str = regime or "UNKNOWN"
    return (
        "You are an oracle meta-analyst. Multiple model rollouts produced "
        "predictions for the same target, and they DISAGREE. Your job is to "
        "extract the strategic insight that the disagreement itself reveals.\n\n"
        f"Target:    {ticker}\n"
        f"Horizon:   {horizon}\n"
        f"Regime:    {regime_str}\n"
        f"Divergence score: {divergence:.3f} (0=unanimous, 1=maximally split)\n\n"
        "Rollouts:\n"
        f"{rollout_block}\n\n"
        "These rollouts disagree. What strategic insight does this disagreement "
        "reveal? Focus on the structural reason for the split (regime ambiguity, "
        "signal conflict, time-horizon mismatch, etc.) rather than picking a "
        "winner. Output JSON ONLY with these exact keys:\n"
        '  {"title": "<short headline, <=80 chars>", '
        '"description": "<one-sentence summary of the disagreement>", '
        '"content": "<3-6 sentence distilled lesson>"}\n'
        "Output JSON only — no commentary, no markdown fences."
    )


def _strip_json_fences(raw: str) -> str:
    """Strip common markdown fences and leading/trailing junk from LLM JSON."""
    s = raw.strip()
    if s.startswith("```"):
        # remove leading fence (```json or ```)
        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.IGNORECASE)
        if s.endswith("```"):
            s = s[: -3]
    s = s.strip()
    # If there is leading prose, try to grab the first {...} block.
    if not s.startswith("{"):
        match = re.search(r"\{.*\}", s, flags=re.DOTALL)
        if match:
            s = match.group(0)
    return s


def _parse_lesson_json(raw: str) -> dict[str, str] | None:
    """Parse the LLM's JSON output into a {title, description, content} dict."""
    try:
        cleaned = _strip_json_fences(raw)
        data = json.loads(cleaned)
        if not isinstance(data, dict):
            return None
        title = str(data.get("title", "")).strip()
        description = str(data.get("description", "")).strip()
        content = str(data.get("content", "")).strip()
        if not (title and description and content):
            return None
        return {
            "title": title[:200],
            "description": description[:500],
            "content": content[:4000],
        }
    except (ValueError, TypeError) as exc:
        log.debug("contrast lesson JSON parse failed: {}", exc)
        return None


def distill_contrast(
    rollouts: list[dict],
    *,
    ticker: str,
    horizon: str,
    regime: str | None,
    llm_caller: Callable[[str], str] | None = None,
    min_divergence: float = 0.25,
) -> ContrastResult | None:
    """Contrast disagreeing rollouts via an LLM and return a distilled lesson.

    Returns ``None`` when:
    - divergence is below ``min_divergence``,
    - ``llm_caller`` is ``None`` or returns an empty / falsy value,
    - JSON parsing fails,
    - any underlying call raises.

    The function is fully defensive: a failure here MUST never propagate to the
    caller (the live prediction path).
    """
    try:
        divergence = compute_divergence(rollouts)
    except Exception as exc:  # noqa: BLE001 — defensive boundary
        log.debug("distill_contrast: divergence calc failed: {}", exc)
        return None

    if divergence < min_divergence:
        return None
    if llm_caller is None:
        return None

    try:
        prompt = _build_prompt(
            rollouts,
            ticker=ticker,
            horizon=horizon,
            regime=regime,
            divergence=divergence,
        )
    except Exception as exc:  # noqa: BLE001 — defensive boundary
        log.debug("distill_contrast: prompt build failed: {}", exc)
        return None

    try:
        raw = llm_caller(prompt)
    except Exception as exc:  # noqa: BLE001 — defensive boundary
        log.debug("distill_contrast: llm_caller raised: {}", exc)
        return None

    if not raw or not isinstance(raw, str):
        return None

    parsed = _parse_lesson_json(raw)
    if parsed is None:
        return None

    # Confidence: scale with divergence sharpness — disagreement that is sharp
    # (high divergence) yields a more actionable lesson. Bounded to [0.3, 0.95].
    confidence = max(0.3, min(0.95, 0.3 + 0.65 * divergence))

    try:
        return ContrastResult(
            title=parsed["title"],
            description=parsed["description"],
            content=parsed["content"],
            confidence=round(confidence, 4),
            divergence_score=round(divergence, 4),
        )
    except Exception as exc:  # noqa: BLE001 — defensive boundary
        log.debug("distill_contrast: ContrastResult construction failed: {}", exc)
        return None


__all__ = [
    "ContrastResult",
    "compute_divergence",
    "distill_contrast",
]
