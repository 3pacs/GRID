"""
CAT-181 — LLM red-team loop per oracle prediction.

Contract
--------
After the Oracle produces a prediction (ticker, direction, horizon, score,
top contributing signals), this module spins up a lightweight LLM "red team"
that produces exactly 3 strongest counter-arguments against the prediction,
grades each counter on severity + plausibility in [0, 1], and returns an
aggregate ``epistemic_risk_score`` equal to the mean of the top-2 counter
grades. A high epistemic_risk_score is a signal to the Oracle that it should
dampen confidence on that prediction (wiring happens in a follow-up commit
on ``oracle/engine.py``; this module must not import or patch the engine).

Rules
-----
- ZERO paid-API calls. The LLM client defaults to ``llm.router.get_llm(Tier.LOCAL)``
  (cheapest local path, Gemma/llamacpp) and callers may inject any duck-typed
  client with a ``.generate(prompt) -> str | None`` method for testing.
- ALWAYS graceful-degrade. Any exception raised by the client, any non-JSON
  response, any malformed counter list, etc. MUST yield an empty
  ``RedTeamReport`` with ``epistemic_risk_score=0.0``. Never bubble up.
- The red team runs AFTER ``oracle.engine.OracleEngine.predict()`` returns,
  never blocks the hot path.
- Deterministic aggregation: ``compute_epistemic_risk`` is a pure function.

Data model
----------
- ``CounterArgument``  — frozen dataclass {text, severity, plausibility, grade}.
- ``RedTeamReport``    — frozen dataclass {ticker, prediction_score, counters,
                         epistemic_risk_score, to_dict()}.

Public API
----------
- ``build_red_team_prompt(ticker, direction, horizon_days, score, signal_summaries) -> str``
- ``parse_red_team_response(raw) -> list[CounterArgument]``
- ``compute_epistemic_risk(counters) -> float``
- ``red_team_prediction(ticker, direction, horizon_days, score, signal_summaries, *, llm_client=None) -> RedTeamReport``
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

from loguru import logger as log


# ── Constants ───────────────────────────────────────────────────────────────

# Number of counter-arguments requested from the LLM.
NUM_COUNTERS: int = 3

# Number of top-graded counters averaged into the epistemic risk score.
TOP_K_FOR_RISK: int = 2

# Weight split between severity and plausibility when computing a counter's
# composite grade. 0.5/0.5 is the neutral default — both axes matter equally.
SEVERITY_WEIGHT: float = 0.5
PLAUSIBILITY_WEIGHT: float = 0.5


# ── Data classes ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CounterArgument:
    """A single counter-argument produced by the red team.

    Attributes
    ----------
    text:
        The counter-argument narrative (human readable).
    severity:
        How damaging this counter would be if true, in [0, 1].
        1.0 = catastrophic (thesis completely broken), 0.0 = negligible.
    plausibility:
        How likely this counter is to actually materialize, in [0, 1].
        1.0 = near-certain, 0.0 = implausible.
    grade:
        Composite grade = ``SEVERITY_WEIGHT * severity + PLAUSIBILITY_WEIGHT * plausibility``.
        Callers may pass a pre-computed grade; ``parse_red_team_response``
        will compute it when absent.
    """

    text: str
    severity: float
    plausibility: float
    grade: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "text": self.text,
            "severity": self.severity,
            "plausibility": self.plausibility,
            "grade": self.grade,
        }


@dataclass(frozen=True)
class RedTeamReport:
    """Aggregate red-team output attached to an Oracle prediction."""

    ticker: str
    prediction_score: float
    counters: tuple[CounterArgument, ...] = field(default_factory=tuple)
    epistemic_risk_score: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "prediction_score": self.prediction_score,
            "counters": [c.to_dict() for c in self.counters],
            "epistemic_risk_score": self.epistemic_risk_score,
        }


# ── Helpers ─────────────────────────────────────────────────────────────────


def _clamp01(value: Any) -> float:
    """Coerce a value to a float in [0, 1]; falls back to 0.0 on failure."""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return 0.0
    if f != f:  # NaN check without importing math
        return 0.0
    if f < 0.0:
        return 0.0
    if f > 1.0:
        return 1.0
    return f


def _composite_grade(severity: float, plausibility: float) -> float:
    return _clamp01(
        SEVERITY_WEIGHT * severity + PLAUSIBILITY_WEIGHT * plausibility
    )


# ── Prompt construction ─────────────────────────────────────────────────────


def build_red_team_prompt(
    ticker: str,
    direction: str,
    horizon_days: int,
    score: float,
    signal_summaries: list[str],
) -> str:
    """Build a structured prompt for the local LLM.

    The prompt asks for exactly ``NUM_COUNTERS`` counter-arguments in strict
    JSON format — no prose, no markdown. We include an explicit JSON schema
    and an example shape to keep the local model on the rails.
    """
    signals_block = (
        "\n".join(f"  - {s}" for s in signal_summaries)
        if signal_summaries
        else "  (no contributing signals provided)"
    )

    return f"""You are a skeptical quantitative analyst on a red team.
Your job is to attack the following prediction and produce the {NUM_COUNTERS}
STRONGEST counter-arguments against it.

PREDICTION UNDER REVIEW
  ticker:        {ticker}
  direction:     {direction}
  horizon_days:  {horizon_days}
  score:         {score:.4f}

TOP CONTRIBUTING SIGNALS
{signals_block}

INSTRUCTIONS
  1. Think of the {NUM_COUNTERS} most damaging, most plausible reasons this
     prediction could be wrong.
  2. Grade each counter on:
       severity     — how badly it breaks the thesis if true, in [0, 1].
       plausibility — how likely it is to actually materialize, in [0, 1].
  3. Return ONLY a JSON object. No prose before or after. No markdown fences.

OUTPUT SCHEMA
{{
  "counters": [
    {{"text": "<counter 1>", "severity": <0..1>, "plausibility": <0..1>}},
    {{"text": "<counter 2>", "severity": <0..1>, "plausibility": <0..1>}},
    {{"text": "<counter 3>", "severity": <0..1>, "plausibility": <0..1>}}
  ]
}}
"""


# ── Response parsing ────────────────────────────────────────────────────────


# Matches the first balanced-looking JSON object in a blob. Local models often
# hedge with a preamble ("Sure, here is my analysis: { ... }") or wrap the
# JSON in a ```json ... ``` fence. We strip fences first, then greedy-match
# the outermost { ... } so a trailing prose tail is also tolerated.
_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)
_JSON_OBJ_RE = re.compile(r"\{.*\}", re.DOTALL)


def _extract_json_blob(raw: str) -> str | None:
    """Locate a JSON object inside a potentially messy LLM response."""
    if not raw or not isinstance(raw, str):
        return None

    stripped = raw.strip()
    if not stripped:
        return None

    # 1. If wrapped in a markdown code fence, prefer the fenced contents.
    fence_match = _FENCE_RE.search(stripped)
    if fence_match:
        candidate = fence_match.group(1).strip()
        if candidate:
            return candidate

    # 2. Otherwise find the first outermost {...} blob.
    obj_match = _JSON_OBJ_RE.search(stripped)
    if obj_match:
        return obj_match.group(0)

    return None


def parse_red_team_response(raw: str) -> list[CounterArgument]:
    """Tolerantly parse an LLM response into ``CounterArgument`` objects.

    Returns ``[]`` on ANY failure (empty input, missing JSON, malformed
    schema, etc.) — never raises.
    """
    blob = _extract_json_blob(raw)
    if blob is None:
        return []

    try:
        payload = json.loads(blob)
    except (json.JSONDecodeError, ValueError):
        return []

    if not isinstance(payload, dict):
        return []

    raw_counters = payload.get("counters")
    if not isinstance(raw_counters, list):
        return []

    counters: list[CounterArgument] = []
    for item in raw_counters:
        if not isinstance(item, dict):
            continue
        text = item.get("text")
        if not isinstance(text, str) or not text.strip():
            continue
        severity = _clamp01(item.get("severity"))
        plausibility = _clamp01(item.get("plausibility"))
        grade_raw = item.get("grade")
        grade = (
            _clamp01(grade_raw)
            if grade_raw is not None
            else _composite_grade(severity, plausibility)
        )
        counters.append(
            CounterArgument(
                text=text.strip(),
                severity=severity,
                plausibility=plausibility,
                grade=grade,
            )
        )

    return counters


# ── Aggregation ─────────────────────────────────────────────────────────────


def compute_epistemic_risk(counters: list[CounterArgument]) -> float:
    """Aggregate counter grades into a single epistemic risk score.

    Deterministic rule: sort counters by grade descending, take the top
    ``TOP_K_FOR_RISK`` (default 2), return their arithmetic mean. Empty
    input returns 0.0.
    """
    if not counters:
        return 0.0

    ordered = sorted(counters, key=lambda c: c.grade, reverse=True)
    top = ordered[:TOP_K_FOR_RISK]
    if not top:
        return 0.0
    return _clamp01(sum(c.grade for c in top) / len(top))


# ── Main entrypoint ─────────────────────────────────────────────────────────


def _default_llm_client() -> Any:
    """Lazy-import the local-tier LLM client so tests can stay pure."""
    from llm.router import Tier, get_llm

    return get_llm(Tier.LOCAL)


def _invoke_llm(client: Any, prompt: str) -> str | None:
    """Call whichever method the client exposes: ``generate`` or ``chat``."""
    if client is None:
        return None

    generate = getattr(client, "generate", None)
    if callable(generate):
        return generate(prompt)

    chat = getattr(client, "chat", None)
    if callable(chat):
        return chat([{"role": "user", "content": prompt}])

    return None


def red_team_prediction(
    ticker: str,
    direction: str,
    horizon_days: int,
    score: float,
    signal_summaries: list[str],
    *,
    llm_client: Any = None,
) -> RedTeamReport:
    """Run the red-team loop for a single oracle prediction.

    Parameters
    ----------
    ticker:
        Ticker the prediction is for.
    direction:
        Direction string from the prediction (e.g. "CALL", "PUT", "LONG").
    horizon_days:
        Prediction horizon in days.
    score:
        The oracle's raw prediction score / confidence in [0, 1].
    signal_summaries:
        One-line summaries of the top contributing signals. May be empty.
    llm_client:
        Optional duck-typed LLM client. Must expose either ``.generate(prompt)``
        or ``.chat(messages)``. When ``None`` (default) we lazy-import
        ``llm.router.get_llm(Tier.LOCAL)``. Tests inject a mock.

    Returns
    -------
    A ``RedTeamReport``. On ANY failure (client raise, empty response, parse
    error) the report has an empty counter tuple and
    ``epistemic_risk_score=0.0``. This method NEVER raises.
    """
    empty_report = RedTeamReport(
        ticker=ticker,
        prediction_score=score,
        counters=tuple(),
        epistemic_risk_score=0.0,
    )

    try:
        client = llm_client if llm_client is not None else _default_llm_client()
    except Exception as exc:  # pragma: no cover — import-level failure only
        log.warning("red_team_prediction: failed to obtain LLM client: {e}", e=exc)
        return empty_report

    prompt = build_red_team_prompt(
        ticker=ticker,
        direction=direction,
        horizon_days=horizon_days,
        score=score,
        signal_summaries=signal_summaries,
    )

    try:
        raw_response = _invoke_llm(client, prompt)
    except Exception as exc:
        log.warning(
            "red_team_prediction: LLM invocation raised for {t}: {e}",
            t=ticker, e=exc,
        )
        return empty_report

    if not raw_response:
        return empty_report

    counters = parse_red_team_response(raw_response)
    if not counters:
        return empty_report

    risk = compute_epistemic_risk(counters)

    return RedTeamReport(
        ticker=ticker,
        prediction_score=score,
        counters=tuple(counters),
        epistemic_risk_score=risk,
    )
