"""
Bull/Bear Debate Agent — LLM-powered adversarial analysis.

Inspired by TradingAgents (ICAIF 2025): structured debate between
bull and bear perspectives using local Ollama model, with a
moderator that synthesizes the final view.

Uses the local llama.cpp endpoint (same as Hermes) to avoid
external API dependencies.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date
from typing import Any

import requests
from loguru import logger as log

try:
    from config import settings
    LLAMACPP_URL = settings.LLAMACPP_BASE_URL
except Exception:
    LLAMACPP_URL = os.getenv("LLAMACPP_URL", "http://localhost:8080")
DEBATE_TEMPERATURE = 0.4
MAX_TOKENS = 512


@dataclass(frozen=True)
class DebateResult:
    ticker: str
    bull_case: str
    bear_case: str
    moderator_verdict: str  # "bullish", "bearish", "neutral"
    confidence: float       # [0, 1]
    key_risks: list[str]
    key_catalysts: list[str]


def _llm_complete(prompt: str, temperature: float = DEBATE_TEMPERATURE) -> str:
    """Call the local llama.cpp completion endpoint."""
    try:
        resp = requests.post(
            f"{LLAMACPP_URL}/completion",
            json={
                "prompt": prompt,
                "n_predict": MAX_TOKENS,
                "temperature": temperature,
                "stop": ["</response>", "\n\n\n"],
            },
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json().get("content", "").strip()
    except Exception as e:
        log.warning("LLM call failed: {e}", e=str(e))
        return ""


def run_debate(
    ticker: str,
    signals: dict[str, Any],
    regime: str = "neutral",
    journal_summary: dict[str, Any] | None = None,
) -> DebateResult:
    """
    Run a bull/bear debate for a single ticker.

    Inputs:
      - ticker: e.g., "AAPL"
      - signals: dict of signal_name → value/direction from alpha_research
      - regime: current market regime from credit cycle / VIX
      - journal_summary: recent decision journal performance summary
    """
    signal_text = "\n".join(f"  - {k}: {v}" for k, v in signals.items())
    journal_text = ""
    if journal_summary:
        journal_text = (
            f"\nRecent track record: {journal_summary.get('helped_rate', 0):.0%} hit rate "
            f"over {journal_summary.get('total_decisions', 0)} decisions. "
            f"Avg outcome: {journal_summary.get('avg_outcome_value', 0):.4f}"
        )

    # Bull case
    bull_prompt = f"""You are a senior equity analyst making the BULL case for {ticker}.

Current market regime: {regime}
Alpha signals:
{signal_text}
{journal_text}

Make your strongest 3-4 point bull case in 150 words or less. Focus on catalysts, momentum, and signal alignment. Be specific and quantitative where possible.

<response>"""

    bull_case = _llm_complete(bull_prompt)

    # Bear case
    bear_prompt = f"""You are a senior risk analyst making the BEAR case for {ticker}.

Current market regime: {regime}
Alpha signals:
{signal_text}
{journal_text}

The bull argues: {bull_case[:200]}

Make your strongest 3-4 point bear case in 150 words or less. Focus on risks, overvaluation signals, and regime concerns. Be specific and quantitative where possible.

<response>"""

    bear_case = _llm_complete(bear_prompt)

    # Moderator synthesis
    mod_prompt = f"""You are a portfolio manager moderating a bull/bear debate on {ticker}.

Regime: {regime}
Signals: {signal_text}

BULL: {bull_case[:300]}
BEAR: {bear_case[:300]}

Respond with ONLY valid JSON (no markdown, no explanation):
{{"verdict": "bullish"|"bearish"|"neutral", "confidence": 0.0-1.0, "key_risks": ["..."], "key_catalysts": ["..."]}}

<response>"""

    mod_raw = _llm_complete(mod_prompt, temperature=0.2)

    # Parse moderator response
    verdict = "neutral"
    confidence = 0.5
    key_risks: list[str] = []
    key_catalysts: list[str] = []

    try:
        # Extract JSON from response
        json_start = mod_raw.find("{")
        json_end = mod_raw.rfind("}") + 1
        if json_start >= 0 and json_end > json_start:
            parsed = json.loads(mod_raw[json_start:json_end])
            verdict = parsed.get("verdict", "neutral")
            confidence = float(parsed.get("confidence", 0.5))
            key_risks = parsed.get("key_risks", [])[:5]
            key_catalysts = parsed.get("key_catalysts", [])[:5]
    except (json.JSONDecodeError, ValueError):
        log.warning("Failed to parse moderator JSON for {t}", t=ticker)

    return DebateResult(
        ticker=ticker,
        bull_case=bull_case,
        bear_case=bear_case,
        moderator_verdict=verdict,
        confidence=confidence,
        key_risks=key_risks,
        key_catalysts=key_catalysts,
    )


def run_debate_batch(
    tickers: list[str],
    signals_by_ticker: dict[str, dict[str, Any]],
    regime: str = "neutral",
    journal_summary: dict[str, Any] | None = None,
) -> list[DebateResult]:
    """Run debate for multiple tickers sequentially."""
    results = []
    for ticker in tickers:
        try:
            result = run_debate(
                ticker,
                signals_by_ticker.get(ticker, {}),
                regime=regime,
                journal_summary=journal_summary,
            )
            results.append(result)
            log.info(
                "Debate {t}: {v} (confidence={c:.2f})",
                t=ticker, v=result.moderator_verdict, c=result.confidence,
            )
        except Exception as e:
            log.warning("Debate failed for {t}: {e}", t=ticker, e=str(e))
    return results
