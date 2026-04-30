"""
LLM narrator — plain-English trade thesis briefing.

Consumes a ``TradeProvenanceReport`` and an optional ``StressTestReport``
and produces a short, operator-readable thesis that someone can skim
in 15 seconds and decide. This is the "explain to me why I should
trust this" layer — a counterpart to CAT-181 ``llm_red_team`` (which
explains why you *shouldn't*).

Design notes
------------

- Works without an LLM by default — a pure-Python template composer
  (``compose_template_narrative``) is the production path. Any deploy
  without ``llm_client`` wired still gets a structured, readable
  briefing.
- When ``llm_client`` is passed, the module fills a prompt with the
  provenance + stress data and asks the model to rewrite the template
  as a natural-language briefing. Gracefully falls back to the
  template on any LLM failure — never raises.
- Output is strictly bounded: target ~200 words, hard cap at ~400.
  The briefing is NOT meant to replace the provenance report; it's
  the tl;dr above it.

Functions
---------

- ``compose_template_narrative(provenance, stress=None) -> str`` — pure,
  no LLM, always works.
- ``narrate_trade(provenance, stress=None, *, llm_client=None) -> NarrativeReport`` —
  main entry. Calls the LLM if provided; falls back to template.
- ``build_narrative_prompt(provenance, stress) -> str`` — pure prompt
  builder for the LLM path.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

# Best-effort Langfuse tracing — no-op when SDK absent / keys missing.
try:
    from langfuse import (
        get_client as _lf_get_client,
        observe as _lf_observe,
    )
except Exception:  # pragma: no cover — optional dep
    def _lf_observe(*args, **kwargs):  # type: ignore[no-redef]
        def _decorator(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _decorator

    def _lf_get_client():  # type: ignore[no-redef]
        return None


def _lf_set_input(**kwargs) -> None:
    """Set explicit input on the active span. Never raises."""
    try:
        client = _lf_get_client()
        if client is not None:
            client.update_current_span(input=kwargs)
    except Exception:
        pass


# ── Constants ─────────────────────────────────────────────────────────────

TARGET_WORDS: int = 200
MAX_WORDS: int = 400


# ── Data class ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class NarrativeReport:
    """Structured narrative output."""

    ticker: str
    headline: str                   # one-line summary (e.g. "LONG TSM, high conviction")
    thesis: str                     # the composed multi-paragraph briefing
    source: str                     # 'template' or 'llm'
    word_count: int
    generated_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "headline": self.headline,
            "thesis": self.thesis,
            "source": self.source,
            "word_count": self.word_count,
            "generated_at": self.generated_at,
        }


# ── Pure template composer ───────────────────────────────────────────────


def _format_signal_line(evidence: Any) -> str:
    """Format one SignalEvidence row as a one-line string."""
    source = getattr(evidence, "signal_source", "unknown")
    weight = getattr(evidence, "shapley_weight", 0.0)
    classification = getattr(evidence, "classification", "unknown")
    card = getattr(evidence, "scorecard", None)
    brier = getattr(card, "running_brier", None) if card is not None else None
    brier_str = f"Brier={brier:.3f}" if brier is not None else "cold-start"
    return f"{source} (weight {weight:.2f}, {classification}, {brier_str})"


def _top_signals_block(signal_evidence: list, top_n: int = 3) -> str:
    """Return a bullet-list of the top-N signals by Shapley weight."""
    if not signal_evidence:
        return "  - (no signal evidence available)"
    sorted_evidence = sorted(
        signal_evidence,
        key=lambda e: getattr(e, "shapley_weight", 0.0),
        reverse=True,
    )[:top_n]
    return "\n".join(f"  - {_format_signal_line(ev)}" for ev in sorted_evidence)


def _headline_from_verdict(verdict: str, direction: str, ticker: str) -> str:
    """Build the one-line headline."""
    verdict_upper = (verdict or "no_trade").upper()
    direction_word = {"bullish": "LONG", "bearish": "SHORT"}.get(
        (direction or "").lower(), "FLAT"
    )
    if verdict_upper == "NO_TRADE":
        return f"NO TRADE — {ticker}"
    return f"{direction_word} {ticker} — {verdict_upper} conviction"


def _robustness_block(stress: Any | None) -> str:
    """Return a brief stress-test summary line."""
    if stress is None:
        return "Stress test: not run"
    label = getattr(stress, "robustness_label", "unknown")
    score = getattr(stress, "robustness_score", None)
    break_count = getattr(stress, "break_count", 0)
    if score is None:
        return f"Stress test: {label}"
    return (
        f"Stress test: {label.upper()} (score {score:.2f}, "
        f"{break_count} verdict-breaking perturbations)"
    )


def compose_template_narrative(
    provenance: Any,
    stress: Any | None = None,
) -> str:
    """Pure-Python multi-paragraph briefing from a provenance report.

    Does NOT call an LLM. Always works. The template is deterministic
    so unit tests can assert on exact substrings.
    """
    ticker = getattr(provenance, "ticker", "") or ""
    direction = getattr(provenance, "direction", "") or ""
    verdict = getattr(provenance, "verdict", "no_trade") or "no_trade"
    confidence = float(getattr(provenance, "confidence", 0.0) or 0.0)
    confidence_lower = float(getattr(provenance, "confidence_lower", 0.0) or 0.0)
    confidence_upper = float(getattr(provenance, "confidence_upper", 0.0) or 0.0)
    horizon = int(getattr(provenance, "horizon_days", 7) or 7)
    aggregate = float(getattr(provenance, "aggregate_conviction", 0.0) or 0.0)
    regime = getattr(provenance, "regime", "") or "unknown"
    fci_regime = getattr(provenance, "fci_regime", "") or "unknown"

    causation = getattr(provenance, "causation", None)
    lever = getattr(causation, "lever", "unspecified") if causation else "unspecified"
    flow = getattr(causation, "flow_direction", "neutral") if causation else "neutral"
    actor = getattr(causation, "actor", "unspecified") if causation else "unspecified"

    signal_evidence = list(getattr(provenance, "signal_evidence", []) or [])
    fragility = float(getattr(provenance, "fragility_multiplier", 1.0) or 1.0)
    disagreement = float(getattr(provenance, "disagreement_score", 0.0) or 0.0)
    red_team_risk = float(getattr(provenance, "red_team_epistemic_risk", 0.0) or 0.0)
    crowd_aligned = bool(getattr(provenance, "crowd_aligned", False))
    market_implied = float(getattr(provenance, "market_implied_prob", 0.0) or 0.0)
    fudge_alerts = list(getattr(provenance, "shipping_fudge_alerts", []) or [])

    # Paragraph 1: the setup
    p1 = (
        f"{ticker}: {direction} call over {horizon}d, verdict {verdict.upper()}. "
        f"Raw oracle confidence {confidence:.2f} "
        f"(CI {confidence_lower:.2f}-{confidence_upper:.2f}), "
        f"aggregate conviction after penalty stack {aggregate:.2f}. "
        f"Liquidity regime {regime}, FCI regime {fci_regime}."
    )

    # Paragraph 2: the lever, flow, actor
    if lever and lever != "unspecified":
        p2 = (
            f"Lever: {lever} by {actor} ({flow} flow). This is the named "
            f"cause — a specific action by a specific party affecting a "
            f"specific valve, not a reading of ambient conditions."
        )
    else:
        p2 = (
            f"WARNING: no named lever — the call is riding conditions "
            f"({regime}/{fci_regime}) rather than a specific action. Treat "
            f"with skepticism per the user causation SOP."
        )

    # Paragraph 3: evidence
    top_signals = _top_signals_block(signal_evidence)
    p3 = f"Top contributing signals:\n{top_signals}"

    # Paragraph 4: penalties and conflicts
    penalty_parts = []
    if disagreement > 0.05:
        penalty_parts.append(f"model disagreement {disagreement:.2f}")
    if fragility < 0.98:
        penalty_parts.append(f"Shapley fragility multiplier {fragility:.2f}")
    if red_team_risk > 0.1:
        penalty_parts.append(f"LLM red-team risk {red_team_risk:.2f}")
    if crowd_aligned:
        penalty_parts.append("aligned with crowd (contrarian edge at risk)")
    if fudge_alerts:
        penalty_parts.append(f"{len(fudge_alerts)} active shipping fudge alerts")

    if penalty_parts:
        p4 = "Penalty stack flagged: " + ", ".join(penalty_parts) + "."
    else:
        p4 = "No material penalties — all conviction layers align."

    # Paragraph 5: market-implied probability context
    if market_implied > 0:
        edge = confidence - market_implied
        if abs(edge) < 0.05:
            p5 = (
                f"Market-implied probability {market_implied:.2f} — oracle "
                f"and options market are aligned, no edge vs consensus."
            )
        elif edge > 0:
            p5 = (
                f"Market-implied probability {market_implied:.2f} — oracle "
                f"is {edge:+.2f} ahead of the options market. Positive edge."
            )
        else:
            p5 = (
                f"Market-implied probability {market_implied:.2f} — options "
                f"market disagrees by {-edge:.2f}. Oracle contrarian vs consensus."
            )
    else:
        p5 = "Market-implied probability: not available."

    # Paragraph 6: stress test
    p6 = _robustness_block(stress)

    # Assemble with consistent spacing
    return "\n\n".join([p1, p2, p3, p4, p5, p6])


# ── LLM prompt builder ───────────────────────────────────────────────────


def build_narrative_prompt(
    provenance: Any,
    stress: Any | None = None,
) -> str:
    """Construct the LLM prompt. Pure function — testable without any
    LLM."""
    baseline = compose_template_narrative(provenance, stress)
    return (
        "You are GRID's trade-narrator. Below is a structured trade "
        "provenance report for a single ticker. Rewrite it as a "
        f"~{TARGET_WORDS}-word operator briefing in plain English. "
        "Preserve every fact exactly — do NOT invent numbers or "
        "signals. Do NOT recommend anything the report does not "
        "support. Keep the lever → flow → actor causation chain "
        "explicit. Hard cap: 4 paragraphs, no bullet lists. Output "
        "ONLY the briefing text, no preamble.\n\n"
        "=== PROVENANCE REPORT ===\n"
        f"{baseline}\n"
        "=== END PROVENANCE REPORT ===\n"
    )


# ── Word counter ─────────────────────────────────────────────────────────


def _count_words(text: str) -> int:
    return len([w for w in text.split() if w.strip()])


# ── LLM invocation (duck-typed) ──────────────────────────────────────────


def _invoke_llm_for_narrative(
    llm_client: Any,
    prompt: str,
) -> str | None:
    """Duck-type the LLM client: prefer ``.generate(prompt)``, fall back
    to ``.chat([{role, content}])``. Returns the raw text or None on
    any failure.
    """
    if llm_client is None:
        return None
    try:
        gen = getattr(llm_client, "generate", None)
        if callable(gen):
            out = gen(prompt)
            return str(out) if out is not None else None
    except Exception:  # noqa: BLE001
        pass
    try:
        chat = getattr(llm_client, "chat", None)
        if callable(chat):
            out = chat([{"role": "user", "content": prompt}])
            # Many clients return dicts; try to pull .content
            if isinstance(out, dict):
                out = out.get("content") or out.get("text")
            return str(out) if out is not None else None
    except Exception:  # noqa: BLE001
        pass
    return None


# ── Main entry ───────────────────────────────────────────────────────────


@_lf_observe(name="llm-narrator-trade", capture_input=False)
def narrate_trade(
    provenance: Any,
    stress: Any | None = None,
    *,
    llm_client: Any = None,
) -> NarrativeReport:
    """Produce a narrative briefing for a trade.

    If ``llm_client`` is passed and responds, the LLM text is used.
    Otherwise the template is used. On ANY LLM failure the template
    is used. This function never raises.
    """
    ticker = getattr(provenance, "ticker", "") or ""
    direction = getattr(provenance, "direction", "") or ""
    verdict = getattr(provenance, "verdict", "no_trade") or "no_trade"
    headline = _headline_from_verdict(verdict, direction, ticker)

    # Explicit input — provenance/stress are large, only identifying fields are useful.
    _lf_set_input(
        ticker=ticker,
        direction=direction,
        verdict=verdict,
        has_stress=stress is not None,
        has_llm=llm_client is not None,
    )

    template_text = compose_template_narrative(provenance, stress)

    source = "template"
    thesis = template_text
    if llm_client is not None:
        try:
            prompt = build_narrative_prompt(provenance, stress)
            llm_text = _invoke_llm_for_narrative(llm_client, prompt)
            if llm_text and llm_text.strip():
                # Cap the word count defensively
                words = llm_text.split()
                if len(words) > MAX_WORDS:
                    llm_text = " ".join(words[:MAX_WORDS]) + "..."
                thesis = llm_text.strip()
                source = "llm"
        except Exception:  # noqa: BLE001
            thesis = template_text
            source = "template"

    return NarrativeReport(
        ticker=ticker,
        headline=headline,
        thesis=thesis,
        source=source,
        word_count=_count_words(thesis),
        generated_at=datetime.now(timezone.utc).isoformat(),
    )
