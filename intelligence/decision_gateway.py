"""
Decision gateway — the capstone "should I trade this?" wrapper.

Runs the full confidence stack for a single ticker in one call:

  1. oracle.engine.EnsemblePredictor.predict(ticker)     → EnsemblePrediction
  2. intelligence.llm_red_team.red_team_prediction(...)  → RedTeamReport
  3. intelligence.signal_provenance.build_provenance_report → TradeProvenanceReport
  4. intelligence.pattern_library.build_pattern_match_report → PatternMatchReport
  5. intelligence.counterfactual_stress.run_stress_test → StressTestReport
  6. trading.trade_ticket_generator.generate_ticket     → TradeTicket | None

Each stage is independently try/except-wrapped so a single broken
component yields a partial `DecisionResponse`, not a crash. The
response always carries a unified verdict string chosen by combining
all sub-verdicts via a deterministic "worst wins" rule.

Design notes
------------

- Every downstream module is imported locally inside the call so a
  broken import in one consumer can't break the gateway — it just
  degrades that stage's output to None with a reason string.
- The gateway is pure orchestration: NO calibration or scoring logic
  lives here. All formulas live in the sub-modules. This file is
  intentionally thin so it's easy to audit.
- The unified verdict rule is documented explicitly so operators can
  reason about it without reading each sub-module.

Unified verdict rule (deterministic)
------------------------------------

Inputs:
  provenance.verdict          ∈ {no_trade, low, medium, high}
  pattern.confidence_signal   ∈ [0.0, 1.0]
  stress.robustness_label     ∈ {robust, moderate, fragile}
  stress.robustness_score     ∈ [0.0, 1.0]
  red_team_risk               ∈ [0.0, 1.0]
  ticket_generated            ∈ {True, False}

Rule:
  - If provenance.verdict == 'no_trade' → 'no_trade'
  - If red_team_risk >= 0.8 → 'no_trade' (LLM vetoed)
  - If stress.robustness_label == 'fragile' AND stress.robustness_score < 0.5
      → downgrade one level (high→medium, medium→low, low→no_trade)
  - If pattern.confidence_signal > 0.0 AND pattern.confidence_signal < 0.3
      → downgrade one level (the historical base rate disagrees)
  - Otherwise: keep provenance.verdict

  Ticket availability has NO influence on verdict — it's a separate
  field reflecting whether the generator was willing to produce a
  concrete action. A verdict='high' with ticket=None is still a high-
  conviction signal, but it failed generator preconditions (e.g.
  missing current_price).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy.engine import Engine


# ── Unified verdict helpers (pure, deterministic) ────────────────────────


_VERDICT_ORDER: tuple[str, ...] = ("no_trade", "low", "medium", "high")


def _downgrade_verdict(verdict: str) -> str:
    """Return the next-weaker verdict in the ordering."""
    try:
        idx = _VERDICT_ORDER.index(verdict)
    except ValueError:
        return "no_trade"
    return _VERDICT_ORDER[max(0, idx - 1)]


def combine_verdict(
    provenance_verdict: str,
    pattern_confidence: float | None,
    stress_label: str | None,
    stress_score: float | None,
    red_team_risk: float,
) -> tuple[str, list[str]]:
    """Apply the unified verdict rule. Returns (final_verdict, reasons)
    where reasons documents every downgrade applied.

    Pure function — easy to unit-test and reason about.
    """
    reasons: list[str] = []
    verdict = provenance_verdict if provenance_verdict in _VERDICT_ORDER else "no_trade"

    if verdict == "no_trade":
        reasons.append("provenance verdict is no_trade")
        return "no_trade", reasons

    if red_team_risk >= 0.8:
        reasons.append(
            f"LLM red-team vetoed (epistemic_risk={red_team_risk:.2f} >= 0.8)"
        )
        return "no_trade", reasons

    if (
        stress_label == "fragile"
        and stress_score is not None
        and stress_score < 0.5
    ):
        new_verdict = _downgrade_verdict(verdict)
        reasons.append(
            f"counterfactual stress fragile (score={stress_score:.2f}) "
            f"→ downgrade {verdict} to {new_verdict}"
        )
        verdict = new_verdict

    if (
        pattern_confidence is not None
        and 0.0 < pattern_confidence < 0.3
    ):
        new_verdict = _downgrade_verdict(verdict)
        reasons.append(
            f"historical pattern library disagrees "
            f"(base_rate_confidence={pattern_confidence:.2f} < 0.3) "
            f"→ downgrade {verdict} to {new_verdict}"
        )
        verdict = new_verdict

    if not reasons:
        reasons.append("all conviction layers align with provenance verdict")
    return verdict, reasons


# ── Response dataclass ────────────────────────────────────────────────────


@dataclass(frozen=True)
class DecisionResponse:
    """Unified decision for a single ticker. Every sub-report is an
    optional-typed field so partial responses are valid."""

    ticker: str
    generated_at: str
    horizon_days: int

    prediction: Any | None              # oracle EnsemblePrediction
    red_team_report: Any | None         # intelligence.llm_red_team.RedTeamReport
    provenance_report: Any | None       # intelligence.signal_provenance.TradeProvenanceReport
    pattern_report: Any | None          # intelligence.pattern_library.PatternMatchReport
    stress_report: Any | None           # intelligence.counterfactual_stress.StressTestReport
    trade_ticket: Any | None            # trading.trade_ticket_generator.TradeTicket

    unified_verdict: str                # no_trade / low / medium / high
    verdict_reasons: list[str]
    stage_errors: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        def _ser(obj: Any) -> Any:
            if obj is None:
                return None
            to_dict = getattr(obj, "to_dict", None)
            if callable(to_dict):
                return to_dict()
            # Fallback: only pull known attrs
            return {k: getattr(obj, k, None) for k in dir(obj) if not k.startswith("_")}

        return {
            "ticker": self.ticker,
            "generated_at": self.generated_at,
            "horizon_days": self.horizon_days,
            "prediction": _ser(self.prediction),
            "red_team_report": _ser(self.red_team_report),
            "provenance_report": _ser(self.provenance_report),
            "pattern_report": _ser(self.pattern_report),
            "stress_report": _ser(self.stress_report),
            "trade_ticket": _ser(self.trade_ticket),
            "unified_verdict": self.unified_verdict,
            "verdict_reasons": list(self.verdict_reasons),
            "stage_errors": dict(self.stage_errors),
        }


# ── Stage runners (each try/except-wrapped) ──────────────────────────────


def _run_prediction(engine: Engine, ticker: str, horizon_days: int) -> tuple[Any | None, str | None]:
    try:
        from oracle.engine import EnsemblePredictor
        predictor = EnsemblePredictor(engine)
        return predictor.predict(ticker.upper(), horizon=horizon_days), None
    except Exception as exc:  # noqa: BLE001
        err = f"oracle.predict failed: {exc}"
        log.debug("decision_gateway: {e}", e=err)
        return None, err


def _run_red_team(prediction: Any, *, llm_client: Any = None) -> tuple[Any | None, float, str | None]:
    if prediction is None:
        return None, 0.0, "skipped — no prediction"
    try:
        from intelligence.llm_red_team import red_team_prediction

        signal_summaries: list[str] = []
        model_votes = getattr(prediction, "model_votes", []) or []
        for vote in model_votes:
            if isinstance(vote, dict):
                name = vote.get("model_name") or vote.get("source") or "unknown"
                weight = vote.get("weight") or vote.get("vote_weight") or 0.0
                signal_summaries.append(f"{name} weight={weight:.2f}")
        if not signal_summaries:
            signal_summaries.append(
                f"top={getattr(prediction, 'shapley_top_contributor', 'unknown')}"
            )

        report = red_team_prediction(
            ticker=getattr(prediction, "ticker", ""),
            direction=getattr(prediction, "direction", "") or "neutral",
            horizon_days=int(getattr(prediction, "horizon", 7) or 7),
            score=float(getattr(prediction, "confidence", 0.0) or 0.0),
            signal_summaries=signal_summaries,
            llm_client=llm_client,
        )
        risk = float(getattr(report, "epistemic_risk_score", 0.0) or 0.0)
        return report, risk, None
    except Exception as exc:  # noqa: BLE001
        err = f"llm_red_team failed: {exc}"
        log.debug("decision_gateway: {e}", e=err)
        return None, 0.0, err


def _run_provenance(
    engine: Engine,
    prediction: Any,
    red_team_risk: float,
) -> tuple[Any | None, str | None]:
    if prediction is None:
        return None, "skipped — no prediction"
    try:
        from intelligence.signal_provenance import build_provenance_report
        return build_provenance_report(
            engine,
            prediction=prediction,
            red_team_epistemic_risk=red_team_risk,
        ), None
    except Exception as exc:  # noqa: BLE001
        err = f"signal_provenance failed: {exc}"
        log.debug("decision_gateway: {e}", e=err)
        return None, err


def _run_pattern_library(
    engine: Engine,
    ticker: str,
) -> tuple[Any | None, float | None, str | None]:
    try:
        from intelligence.pattern_library import build_pattern_match_report
        report = build_pattern_match_report(engine, ticker=ticker)
        confidence = getattr(report, "confidence_signal", None)
        if confidence is not None:
            confidence = float(confidence)
        return report, confidence, None
    except Exception as exc:  # noqa: BLE001
        err = f"pattern_library failed: {exc}"
        log.debug("decision_gateway: {e}", e=err)
        return None, None, err


def _run_stress_test(
    provenance_report: Any,
) -> tuple[Any | None, str | None, float | None, str | None]:
    if provenance_report is None:
        return None, None, None, "skipped — no provenance report"
    try:
        from intelligence.counterfactual_stress import run_stress_test
        report = run_stress_test(provenance_report)
        label = getattr(report, "robustness_label", None)
        score = getattr(report, "robustness_score", None)
        if score is not None:
            score = float(score)
        return report, label, score, None
    except Exception as exc:  # noqa: BLE001
        err = f"counterfactual_stress failed: {exc}"
        log.debug("decision_gateway: {e}", e=err)
        return None, None, None, err


def _run_trade_ticket(
    provenance_report: Any,
    *,
    account_size_usd: float,
    current_price: float | None,
    vol_30d: float | None,
    instrument: str,
    unified_verdict: str,
) -> tuple[Any | None, str | None]:
    if provenance_report is None:
        return None, "skipped — no provenance report"
    if unified_verdict in ("no_trade", "low"):
        return None, f"verdict={unified_verdict} below ticket threshold"
    if current_price is None or current_price <= 0:
        return None, "current_price missing or non-positive"
    try:
        from trading.trade_ticket_generator import generate_ticket
        ticket = generate_ticket(
            provenance_report,
            account_size_usd=float(account_size_usd),
            current_price=float(current_price),
            instrument=instrument,
            vol_30d=vol_30d,
        )
        return ticket, None
    except Exception as exc:  # noqa: BLE001
        err = f"trade_ticket_generator failed: {exc}"
        log.debug("decision_gateway: {e}", e=err)
        return None, err


# ── Main gateway ──────────────────────────────────────────────────────────


def should_i_trade(
    engine: Engine,
    ticker: str,
    *,
    account_size_usd: float,
    current_price: float | None = None,
    vol_30d: float | None = None,
    horizon_days: int = 7,
    instrument: str = "equity",
    llm_client: Any = None,
) -> DecisionResponse:
    """Run the full confidence stack for ``ticker`` and return a
    unified decision response.

    Any single stage failing degrades that stage to None but never
    raises — the operator always gets a response, even if partial.
    """
    stage_errors: dict[str, str] = {}

    prediction, err = _run_prediction(engine, ticker, horizon_days)
    if err:
        stage_errors["prediction"] = err

    red_team_report, red_team_risk, err = _run_red_team(prediction, llm_client=llm_client)
    if err:
        stage_errors["red_team"] = err

    provenance_report, err = _run_provenance(engine, prediction, red_team_risk)
    if err:
        stage_errors["provenance"] = err

    pattern_report, pattern_confidence, err = _run_pattern_library(engine, ticker)
    if err:
        stage_errors["pattern_library"] = err

    stress_report, stress_label, stress_score, err = _run_stress_test(provenance_report)
    if err:
        stage_errors["counterfactual_stress"] = err

    # Provenance verdict is the base — default to no_trade if provenance failed
    base_verdict = getattr(provenance_report, "verdict", "no_trade") or "no_trade"
    unified_verdict, reasons = combine_verdict(
        base_verdict,
        pattern_confidence,
        stress_label,
        stress_score,
        red_team_risk,
    )

    ticket, err = _run_trade_ticket(
        provenance_report,
        account_size_usd=account_size_usd,
        current_price=current_price,
        vol_30d=vol_30d,
        instrument=instrument,
        unified_verdict=unified_verdict,
    )
    if err:
        stage_errors["trade_ticket"] = err

    return DecisionResponse(
        ticker=ticker.upper(),
        generated_at=datetime.now(timezone.utc).isoformat(),
        horizon_days=horizon_days,
        prediction=prediction,
        red_team_report=red_team_report,
        provenance_report=provenance_report,
        pattern_report=pattern_report,
        stress_report=stress_report,
        trade_ticket=ticket,
        unified_verdict=unified_verdict,
        verdict_reasons=reasons,
        stage_errors=stage_errors,
    )
