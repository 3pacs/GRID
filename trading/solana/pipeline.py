"""
Solana 4-agent trading pipeline.

Inspired by AutoHedge's Director → Quant → Risk → Execution workflow
(https://github.com/The-Swarm-Corporation/AutoHedge, MIT), but rebuilt on
top of GRID's existing LLM router so we don't pull in the Swarms framework
as a hard dependency.

Each stage is a plain Python function that calls ``llm.router.get_llm(...)``
and parses JSON out of the response. That keeps the pipeline:

  * deterministic and testable (mock the LLM client, assert on JSON)
  * aligned with GRID's 3-tier LLM taxonomy (LOCAL / REASON / ORACLE)
  * decoupled from any single vendor API

The pipeline intentionally produces a ``PipelineDecision`` dataclass rather
than executing the trade itself — execution goes through
:mod:`trading.solana.executor` which paper-trades first.
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log

from trading.solana.jupiter_client import JupiterClient

# ----------------------------------------------------------------------
# Prompts — ported from AutoHedge's autohedge/prompts.py and trimmed to
# fit GRID's local Gemma-class models (smaller context windows than GPT-4.1)
# ----------------------------------------------------------------------

DIRECTOR_PROMPT = """You are the Trading Director of GRID's Solana desk.
Given the user's task, decide which Solana token (mint address) to analyse
and draft a 1-paragraph trading thesis. Respond with ONLY a JSON object:

{
  "mint": "<base58 mint address>",
  "symbol": "<ticker>",
  "thesis": "<1 paragraph thesis>"
}
Task: {task}
"""

QUANT_PROMPT = """You are a Quantitative Analyst.
Given the thesis and the latest price data, produce scores in [0,1] for
technical strength, trend, volatility (1 = low vol), and probability of
thesis success. Respond with ONLY a JSON object:

{
  "technical_score": <float>,
  "trend_strength": <float>,
  "volatility": <float>,
  "probability_score": <float>,
  "notes": "<one sentence>"
}

Thesis: {thesis}
Symbol: {symbol}
Price data: {price_snapshot}
"""

RISK_PROMPT = """You are a Risk Manager. Given the thesis and quant scores,
decide a position size in [0, 0.1] (fraction of portfolio, hard cap 10%%)
and a stop-loss percent in [0.02, 0.2]. Respond with ONLY a JSON object:

{
  "position_size": <float>,
  "stop_loss_pct": <float>,
  "risk_score": <float 0-1, 1 = safest>,
  "veto": <true|false>,
  "reason": "<one sentence>"
}

Thesis: {thesis}
Quant: {quant}
"""

EXECUTION_PROMPT = """You are an Execution Agent. Given the thesis, quant,
and risk decision, emit the final trade instruction. Respond with ONLY a
JSON object:

{
  "action": "BUY" | "SELL" | "HOLD",
  "symbol": "<ticker>",
  "mint": "<base58 mint>",
  "quote_mint": "<base58 mint of funding token>",
  "size_fraction": <float 0-1>,
  "stop_loss_pct": <float>,
  "take_profit_pct": <float>,
  "rationale": "<one sentence>"
}

Thesis: {thesis}
Quant: {quant}
Risk: {risk}
"""


# ----------------------------------------------------------------------
# Data classes
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class PipelineDecision:
    """Structured output from :meth:`SolanaPipeline.run`.

    A decision is considered *actionable* when ``action`` is BUY or SELL
    and ``risk_veto`` is False. Otherwise consumers should log and skip.
    """

    generated_at: str
    task: str
    symbol: str
    mint: str
    thesis: str
    action: str
    size_fraction: float
    stop_loss_pct: float
    take_profit_pct: float
    risk_score: float
    risk_veto: bool
    quant: dict[str, Any] = field(default_factory=dict)
    risk: dict[str, Any] = field(default_factory=dict)
    execution: dict[str, Any] = field(default_factory=dict)
    price_snapshot: dict[str, Any] = field(default_factory=dict)

    @property
    def actionable(self) -> bool:
        return self.action in {"BUY", "SELL"} and not self.risk_veto

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ----------------------------------------------------------------------
# Pipeline
# ----------------------------------------------------------------------
# Protocol signature for the LLM client we depend on — matches the
# ``chat`` method on every client exposed by :mod:`llm.router`.
from typing import Protocol


class _LLMProtocol(Protocol):
    def chat(
        self,
        messages: list[dict[str, str]],
        model: str | None = None,
        temperature: float = 0.3,
        num_predict: int = 4096,
    ) -> str | None: ...


_JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)


def _parse_json_block(text: str | None, stage: str) -> dict[str, Any]:
    """Extract the first JSON object from an LLM response."""
    if not text:
        raise ValueError(f"{stage}: empty response from LLM")
    match = _JSON_BLOCK_RE.search(text)
    if not match:
        raise ValueError(f"{stage}: no JSON object in response: {text[:200]}")
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"{stage}: malformed JSON ({exc}): {match.group(0)[:200]}"
        ) from exc


class SolanaPipeline:
    """Run a 4-stage Director/Quant/Risk/Execution pipeline.

    The pipeline is intentionally stateless — create one per task so that
    concurrent runs don't share buffers. Dependencies can all be injected
    to keep tests trivial.
    """

    def __init__(
        self,
        llm: _LLMProtocol | None = None,
        jupiter: JupiterClient | None = None,
    ) -> None:
        if llm is None:
            # Imported lazily so the module is cheap to import in tests.
            from llm.router import Tier, get_llm

            llm = get_llm(Tier.REASON)
        self._llm = llm
        self._jupiter = jupiter  # may be None; we'll build one lazily

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run(self, task: str) -> PipelineDecision:
        """Run Director → Quant → Risk → Execution for the given task.

        Raises:
            ValueError: if any stage returns malformed JSON or the director
                fails to produce a mint + thesis.
        """
        if not task:
            raise ValueError("task must be non-empty")

        log.info("SolanaPipeline.run — task={t}", t=task[:120])

        director = self._run_director(task)
        mint = director.get("mint")
        symbol = director.get("symbol") or "UNKNOWN"
        thesis = director.get("thesis") or ""
        if not mint:
            raise ValueError("director did not return a mint address")

        price_snapshot = self._fetch_price_snapshot(mint)

        quant = self._run_quant(thesis, symbol, price_snapshot)
        risk = self._run_risk(thesis, quant)
        execution = self._run_execution(thesis, quant, risk)

        action = (execution.get("action") or "HOLD").upper()
        size_fraction = float(execution.get("size_fraction", 0.0))
        stop_loss_pct = float(
            execution.get("stop_loss_pct", risk.get("stop_loss_pct", 0.05))
        )
        take_profit_pct = float(execution.get("take_profit_pct", 0.1))
        risk_score = float(risk.get("risk_score", 0.5))
        risk_veto = bool(risk.get("veto", False))

        return PipelineDecision(
            generated_at=datetime.now(timezone.utc).isoformat(),
            task=task,
            symbol=symbol,
            mint=mint,
            thesis=thesis,
            action=action,
            size_fraction=size_fraction,
            stop_loss_pct=stop_loss_pct,
            take_profit_pct=take_profit_pct,
            risk_score=risk_score,
            risk_veto=risk_veto,
            quant=quant,
            risk=risk,
            execution=execution,
            price_snapshot=price_snapshot,
        )

    # ------------------------------------------------------------------
    # Stages
    # ------------------------------------------------------------------
    def _chat(self, prompt: str, stage: str) -> dict[str, Any]:
        response = self._llm.chat(
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            num_predict=1024,
        )
        return _parse_json_block(response, stage)

    def _run_director(self, task: str) -> dict[str, Any]:
        return self._chat(DIRECTOR_PROMPT.replace("{task}", task), "director")

    def _run_quant(
        self,
        thesis: str,
        symbol: str,
        price_snapshot: dict[str, Any],
    ) -> dict[str, Any]:
        prompt = (
            QUANT_PROMPT.replace("{thesis}", thesis)
            .replace("{symbol}", symbol)
            .replace("{price_snapshot}", json.dumps(price_snapshot, default=str))
        )
        return self._chat(prompt, "quant")

    def _run_risk(
        self, thesis: str, quant: dict[str, Any]
    ) -> dict[str, Any]:
        prompt = (
            RISK_PROMPT.replace("{thesis}", thesis)
            .replace("{quant}", json.dumps(quant, default=str))
        )
        return self._chat(prompt, "risk")

    def _run_execution(
        self,
        thesis: str,
        quant: dict[str, Any],
        risk: dict[str, Any],
    ) -> dict[str, Any]:
        prompt = (
            EXECUTION_PROMPT.replace("{thesis}", thesis)
            .replace("{quant}", json.dumps(quant, default=str))
            .replace("{risk}", json.dumps(risk, default=str))
        )
        return self._chat(prompt, "execution")

    # ------------------------------------------------------------------
    # Price snapshot
    # ------------------------------------------------------------------
    def _fetch_price_snapshot(self, mint: str) -> dict[str, Any]:
        """Pull a minimal price dict for the given mint.

        Returns an empty dict (not None) on any failure so downstream
        stages still get a valid JSON value.
        """
        try:
            client = self._jupiter or JupiterClient()
            own_client = self._jupiter is None
            try:
                data = client.get_token_price(mint)
            finally:
                if own_client:
                    client.close()
            return data.get(mint, {}) if isinstance(data, dict) else {}
        except Exception as exc:  # noqa: BLE001 — graceful degradation
            log.warning(
                "Jupiter price snapshot failed for {m}: {e}",
                m=mint,
                e=str(exc),
            )
            return {}
