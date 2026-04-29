"""
Tests for ``oracle.contrast_distillation``.

The module is fully defensive — every external boundary returns ``None``
on failure rather than propagating exceptions. These tests verify both
the happy path and each defensive boundary.
"""

from __future__ import annotations

import json
from typing import Callable

import pytest

from oracle.contrast_distillation import (
    ContrastResult,
    compute_divergence,
    distill_contrast,
)


# ── compute_divergence ──────────────────────────────────────────────────────


@pytest.mark.unit
def test_compute_divergence_unanimous_is_zero() -> None:
    """All-bullish, identical prob_up rollouts produce ~0 divergence."""
    rollouts = [
        {"prob_up": 0.7, "verdict": "bullish", "confidence": 0.8},
        {"prob_up": 0.7, "verdict": "bullish", "confidence": 0.8},
        {"prob_up": 0.7, "verdict": "bullish", "confidence": 0.8},
    ]
    div = compute_divergence(rollouts)
    assert div == pytest.approx(0.0, abs=1e-9)


@pytest.mark.unit
def test_compute_divergence_split_is_high() -> None:
    """3 up + 2 down with split probs should yield divergence > 0.3."""
    rollouts = [
        {"prob_up": 0.85, "verdict": "bullish", "confidence": 0.9},
        {"prob_up": 0.80, "verdict": "bullish", "confidence": 0.85},
        {"prob_up": 0.75, "verdict": "bullish", "confidence": 0.8},
        {"prob_up": 0.20, "verdict": "bearish", "confidence": 0.7},
        {"prob_up": 0.15, "verdict": "bearish", "confidence": 0.75},
    ]
    div = compute_divergence(rollouts)
    assert div > 0.3
    assert div <= 1.0


@pytest.mark.unit
def test_compute_divergence_handles_short_list() -> None:
    assert compute_divergence([]) == 0.0
    assert compute_divergence([{"prob_up": 0.5, "verdict": "neutral"}]) == 0.0


@pytest.mark.unit
def test_compute_divergence_handles_garbage_input() -> None:
    """Non-dict and missing fields fall back to neutral defaults."""
    div = compute_divergence(
        [
            {"prob_up": "not-a-number", "verdict": None},
            {"prob_up": None, "verdict": 42},
            "this is not a dict",  # type: ignore[list-item]
        ]
    )
    assert 0.0 <= div <= 1.0


# ── distill_contrast: short-circuit paths ────────────────────────────────────


@pytest.mark.unit
def test_distill_contrast_returns_none_when_divergence_too_low() -> None:
    """Below min_divergence, llm_caller is never invoked and None is returned."""
    rollouts = [
        {"prob_up": 0.55, "verdict": "bullish", "confidence": 0.7},
        {"prob_up": 0.55, "verdict": "bullish", "confidence": 0.7},
    ]

    calls: list[str] = []

    def caller(_prompt: str) -> str:
        calls.append(_prompt)
        return "{}"

    result = distill_contrast(
        rollouts,
        ticker="TSM",
        horizon="7d",
        regime="NEUTRAL",
        llm_caller=caller,
        min_divergence=0.5,
    )
    assert result is None
    assert calls == []  # caller never invoked


@pytest.mark.unit
def test_distill_contrast_returns_none_when_caller_is_none() -> None:
    """No LLM caller → no lesson, even if divergence is high."""
    rollouts = [
        {"prob_up": 0.9, "verdict": "bullish", "confidence": 0.9},
        {"prob_up": 0.1, "verdict": "bearish", "confidence": 0.9},
    ]
    result = distill_contrast(
        rollouts,
        ticker="TSM",
        horizon="7d",
        regime="HIGH_VOL",
        llm_caller=None,
    )
    assert result is None


# ── distill_contrast: happy path ────────────────────────────────────────────


@pytest.mark.unit
def test_distill_contrast_returns_result_on_valid_json() -> None:
    """High divergence + valid JSON → ContrastResult."""
    rollouts = [
        {
            "prob_up": 0.85,
            "verdict": "bullish",
            "confidence": 0.9,
            "model_name": "ratio_pulse",
            "top_contributions": ["yield_curve", "vix_term"],
        },
        {
            "prob_up": 0.15,
            "verdict": "bearish",
            "confidence": 0.85,
            "model_name": "macro_drift",
            "top_contributions": ["dxy", "credit_spread"],
        },
        {
            "prob_up": 0.20,
            "verdict": "bearish",
            "confidence": 0.8,
            "model_name": "flow_thesis",
            "top_contributions": ["dark_pool", "etf_flow"],
        },
    ]

    payload = {
        "title": "Models split on TSM 7d under HIGH_VOL+CRISIS regime",
        "description": "Ratio model is bullish while macro and flow models are bearish.",
        "content": (
            "The disagreement reflects a regime ambiguity: short-term "
            "ratio signals point up while longer-horizon macro and flow "
            "signals point down. In HIGH_VOL+CRISIS regimes this typically "
            "resolves bearish within the 7d horizon. Treat the bullish "
            "rollout as the minority view and demand confirmation before "
            "sizing."
        ),
    }

    seen: list[str] = []

    def caller(prompt: str) -> str:
        seen.append(prompt)
        return json.dumps(payload)

    result = distill_contrast(
        rollouts,
        ticker="TSM",
        horizon="7d",
        regime="HIGH_VOL+CRISIS",
        llm_caller=caller,
        min_divergence=0.25,
    )

    assert isinstance(result, ContrastResult)
    assert result.title == payload["title"]
    assert result.description == payload["description"]
    assert result.content == payload["content"]
    assert 0.0 <= result.confidence <= 1.0
    assert 0.0 <= result.divergence_score <= 1.0
    assert result.divergence_score > 0.25
    # The prompt actually contains the rollout details.
    assert seen and "TSM" in seen[0] and "HIGH_VOL+CRISIS" in seen[0]


@pytest.mark.unit
def test_distill_contrast_strips_markdown_fences() -> None:
    """LLMs that wrap JSON in ```json fences are still parsed correctly."""
    rollouts = [
        {"prob_up": 0.9, "verdict": "bullish", "confidence": 0.9},
        {"prob_up": 0.1, "verdict": "bearish", "confidence": 0.9},
    ]
    payload = {
        "title": "Split on direction",
        "description": "Two rollouts disagree sharply.",
        "content": (
            "Rollout one is bullish; rollout two is bearish. "
            "Sharp prob_up split with similar confidence implies the "
            "models are using different feature subsets. Defer until a "
            "tie-breaking signal arrives."
        ),
    }
    fenced = "```json\n" + json.dumps(payload) + "\n```"

    def caller(_prompt: str) -> str:
        return fenced

    result = distill_contrast(
        rollouts,
        ticker="AAPL",
        horizon="1d",
        regime=None,
        llm_caller=caller,
    )
    assert isinstance(result, ContrastResult)
    assert result.title == payload["title"]


# ── distill_contrast: defensive boundaries ──────────────────────────────────


@pytest.mark.unit
def test_distill_contrast_returns_none_when_caller_raises() -> None:
    """Any exception from the LLM caller is swallowed, returning None."""
    rollouts = [
        {"prob_up": 0.9, "verdict": "bullish", "confidence": 0.9},
        {"prob_up": 0.1, "verdict": "bearish", "confidence": 0.9},
    ]

    def boom(_prompt: str) -> str:
        raise RuntimeError("LLM unavailable")

    result = distill_contrast(
        rollouts,
        ticker="TSM",
        horizon="7d",
        regime="NEUTRAL",
        llm_caller=boom,
    )
    assert result is None


@pytest.mark.unit
def test_distill_contrast_returns_none_on_malformed_json() -> None:
    """LLM returns junk → defensive None, no exception bubbles."""
    rollouts = [
        {"prob_up": 0.9, "verdict": "bullish", "confidence": 0.9},
        {"prob_up": 0.1, "verdict": "bearish", "confidence": 0.9},
    ]

    def caller(_prompt: str) -> str:
        return "this is definitely not json {{{{"

    result = distill_contrast(
        rollouts,
        ticker="TSM",
        horizon="7d",
        regime="NEUTRAL",
        llm_caller=caller,
    )
    assert result is None


@pytest.mark.unit
def test_distill_contrast_returns_none_on_empty_caller_output() -> None:
    """Empty string from the caller is treated as failure."""
    rollouts = [
        {"prob_up": 0.9, "verdict": "bullish", "confidence": 0.9},
        {"prob_up": 0.1, "verdict": "bearish", "confidence": 0.9},
    ]

    def caller(_prompt: str) -> str:
        return ""

    result = distill_contrast(
        rollouts,
        ticker="TSM",
        horizon="7d",
        regime="NEUTRAL",
        llm_caller=caller,
    )
    assert result is None


@pytest.mark.unit
def test_distill_contrast_returns_none_when_keys_missing() -> None:
    """Valid JSON but missing required keys → None."""
    rollouts = [
        {"prob_up": 0.9, "verdict": "bullish", "confidence": 0.9},
        {"prob_up": 0.1, "verdict": "bearish", "confidence": 0.9},
    ]

    def caller(_prompt: str) -> str:
        return json.dumps({"title": "only a title"})

    result = distill_contrast(
        rollouts,
        ticker="TSM",
        horizon="7d",
        regime="NEUTRAL",
        llm_caller=caller,
    )
    assert result is None


@pytest.mark.unit
def test_distill_contrast_caller_signature_is_str_to_str() -> None:
    """Sanity: the caller protocol is a Callable[[str], str]."""

    def caller(prompt: str) -> str:
        assert isinstance(prompt, str)
        return json.dumps(
            {
                "title": "x",
                "description": "y",
                "content": "z" * 30,
            }
        )

    rollouts = [
        {"prob_up": 0.9, "verdict": "bullish", "confidence": 0.9},
        {"prob_up": 0.1, "verdict": "bearish", "confidence": 0.9},
    ]

    typed_caller: Callable[[str], str] = caller
    result = distill_contrast(
        rollouts,
        ticker="TSM",
        horizon="7d",
        regime=None,
        llm_caller=typed_caller,
    )
    assert isinstance(result, ContrastResult)
