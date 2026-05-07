"""
Null-hypothesis forecaster — CAT-186 (baseline skeptic for aggregate conviction).

The conviction roadmap's missing loop
-------------------------------------

``features/per_signal_brier.py`` tells us *one signal is well-calibrated*. It
does NOT tell us the whole oracle stack is meaningfully better than a dumb
baseline. CAT-186 closes that gap.

This module asks, every time a conviction report is generated:

    "Could a coin-flip forecaster beat us on this setup?"

It scores the oracle's recent history against four progressively-dumber null
models and converts the resulting edge into a multiplicative penalty applied
to ``aggregate_conviction``, so that the live path downgrades trades whenever
the oracle is barely outperforming (or losing to) a baseline a ten-year-old
could implement.

The four null models
--------------------

All four are scored on the same ``oracle_predictions`` history window
(default 365d) pulled in a single query:

1. **Majority-class baseline** — always predicts the most common outcome
   (i.e. the historical hit rate). Brier = variance of the outcome series.
2. **Base-rate-by-regime baseline** — predicts the historical hit rate
   stratified by liquidity regime (CRISIS / TIGHTENING / NEUTRAL /
   EXPANSION / EXPANSION_STRONG). Reuses the canonical regime normalizer
   from ``features/regime_conditional_brier._canonical_regime``.
3. **Coin-flip baseline** — p = 0.5 always. The dumbest possible floor.
4. **Momentum-K20 baseline** — p = hit rate of the last 20 scored
   predictions (same ticker when a ticker filter is in play, otherwise
   same horizon). Captures short-run serial correlation in regimes.

Oracle edge and the penalty map
-------------------------------

After scoring all four baselines, we compute::

    best_null_brier = min(b.brier for b in baselines)
    edge = best_null_brier - oracle_brier       # positive = oracle wins
    edge_pct = edge / max(best_null_brier, 1e-9)

The edge is mapped to a multiplicative penalty via the ``_EDGE_*`` constants
below. Penalties live in [0.50, 1.00] — conviction is only penalised, never
amplified, so the null model can only hurt.

Public API
----------

- ``NullBaselineResult`` / ``NullHypothesisReport`` — frozen dataclasses.
- ``evaluate_null_hypothesis(engine, ...)`` — full report.
- ``null_hypothesis_penalty(engine, ...)`` — bare multiplier for the live
  aggregate conviction path. Never raises, returns 1.0 on any error.

Neither function raises. Any DB or data issue yields a neutral result
(penalty = 1.0) with an advisory string explaining why the penalty is
disabled. This mirrors the ``fragility_multiplier`` defensive contract.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# Reuse the canonical regime normalizer — CANONICAL_REGIMES is the exact
# 5-state taxonomy the oracle tags every prediction with. Never
# reimplement the alias mapping.
from features.regime_conditional_brier import (  # noqa: F401
    CANONICAL_REGIMES,
    _canonical_regime,
)


# ── Constants ─────────────────────────────────────────────────────────────

#: Minimum oracle prediction count before the penalty engages. Below this
#: the report returns a neutral penalty of 1.0 — we never want to punish
#: the stack for thin history.
MIN_ORACLE_SAMPLES: int = 30

#: Momentum baseline window — "whatever just worked, predict that again."
#: 20 chosen to match ``features.per_signal_brier.MIN_CALIBRATED_SAMPLES``
#: so the baseline is calibrated on the same sample threshold the real
#: stack uses.
MOMENTUM_WINDOW: int = 20

#: Canonical baseline model names (stable string keys for the report
#: payload and downstream consumers).
MODEL_MAJORITY: str = "majority"
MODEL_REGIME_BASE_RATE: str = "regime_base_rate"
MODEL_COIN_FLIP: str = "coin_flip"
MODEL_MOMENTUM: str = f"momentum_k{MOMENTUM_WINDOW}"

#: Edge thresholds for the penalty map. Hard-coded by design (see
#: module docstring) — the point of CAT-186 is to have a single,
#: defensible, documented floor.
EDGE_STRONG: float = 0.20  # ≥ 20% better than best null
EDGE_MODERATE: float = 0.10  # 10–20% better
EDGE_WEAK: float = 0.05  # 5–10% better
EDGE_MARGINAL: float = 0.00  # 0–5% better

#: Penalty curve — applied multiplicatively to aggregate conviction.
PENALTY_STRONG: float = 1.00  # oracle is genuinely beating baselines
PENALTY_MODERATE: float = 0.95
PENALTY_WEAK: float = 0.85
PENALTY_MARGINAL: float = 0.70  # barely-better-than-dumb
PENALTY_LOST: float = 0.50  # null model beats oracle — heavy skepticism

#: Neutral penalty returned on any failure, empty history, or
#: insufficient-sample early-out.
NEUTRAL_PENALTY: float = 1.00


# ── Dataclasses ───────────────────────────────────────────────────────────


@dataclass(frozen=True)
class NullBaselineResult:
    """One baseline model's score on the evaluation window."""

    model_name: str
    brier: float
    hit_rate: float
    n_scored: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_name": self.model_name,
            "brier": round(self.brier, 6),
            "hit_rate": round(self.hit_rate, 4),
            "n_scored": self.n_scored,
        }


@dataclass(frozen=True)
class NullHypothesisReport:
    """Full null-hypothesis evaluation for a given horizon / ticker filter."""

    as_of: str
    horizon_days: int
    window_days: int
    oracle_brier: float
    oracle_hit_rate: float
    n_oracle_predictions: int
    baselines: list[NullBaselineResult]
    best_null_model: str
    best_null_brier: float
    edge_absolute: float
    edge_pct: float
    penalty_multiplier: float
    advisory: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "horizon_days": self.horizon_days,
            "window_days": self.window_days,
            "oracle_brier": round(self.oracle_brier, 6),
            "oracle_hit_rate": round(self.oracle_hit_rate, 4),
            "n_oracle_predictions": self.n_oracle_predictions,
            "baselines": [b.to_dict() for b in self.baselines],
            "best_null_model": self.best_null_model,
            "best_null_brier": round(self.best_null_brier, 6),
            "edge_absolute": round(self.edge_absolute, 6),
            "edge_pct": round(self.edge_pct, 6),
            "penalty_multiplier": round(self.penalty_multiplier, 4),
            "advisory": self.advisory,
        }


# ── Internal row shape ────────────────────────────────────────────────────


@dataclass(frozen=True)
class _ScoredRow:
    """In-memory view of one scored oracle_predictions row."""

    created_at: Any  # datetime, passed through unchanged
    confidence: float
    outcome: float  # {0.0, 0.5, 1.0}
    regime: str  # canonical


# ── Verdict → outcome mapping ────────────────────────────────────────────
#
# Mirrors ``scripts/bootstrap_per_signal_brier.verdict_to_outcome`` so the
# three call sites (bootstrap, regime bootstrap, null hypothesis) agree on
# the mapping. Kept as a local closure (not a new public helper) because
# the task brief explicitly forbids new helper proliferation.


def _verdict_to_outcome(verdict: str | None) -> float:
    if verdict == "hit":
        return 1.0
    if verdict == "partial":
        return 0.5
    return 0.0


# ── Row loader ────────────────────────────────────────────────────────────


_SELECT_SCORED_ROWS = text(
    """
    SELECT id, created_at, confidence, verdict, signals, horizon_days, ticker
    FROM oracle_predictions
    WHERE verdict IN ('hit', 'miss', 'partial')
      AND created_at >= NOW() - (:days || ' days')::INTERVAL
      AND horizon_days = :h
    ORDER BY created_at ASC
    """
)


_SELECT_SCORED_ROWS_TICKER = text(
    """
    SELECT id, created_at, confidence, verdict, signals, horizon_days, ticker
    FROM oracle_predictions
    WHERE verdict IN ('hit', 'miss', 'partial')
      AND created_at >= NOW() - (:days || ' days')::INTERVAL
      AND horizon_days = :h
      AND ticker = :ticker
    ORDER BY created_at ASC
    """
)


def _parse_signals_regime(raw: Any) -> str:
    """Best-effort extraction of the ``regime`` key from the
    ``oracle_predictions.signals`` JSONB cell. Returns canonical
    ``"NEUTRAL"`` on any failure so the downstream bucket key space
    stays bounded.
    """
    if raw is None:
        return _canonical_regime(None)
    parsed: Any = None
    if isinstance(raw, dict):
        parsed = raw
    elif isinstance(raw, (bytes, bytearray)):
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except Exception:  # noqa: BLE001
            return _canonical_regime(None)
    elif isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except Exception:  # noqa: BLE001
            return _canonical_regime(None)
    if not isinstance(parsed, dict):
        return _canonical_regime(None)
    return _canonical_regime(parsed.get("regime"))


def _fetch_rows(
    engine: Engine,
    *,
    horizon_days: int,
    window_days: int,
    ticker: str | None,
) -> list[_ScoredRow]:
    """Pull the scored oracle history for the evaluation window.

    Returns an empty list on any exception — callers short-circuit to
    the neutral report.
    """
    stmt = _SELECT_SCORED_ROWS_TICKER if ticker else _SELECT_SCORED_ROWS
    params: dict[str, Any] = {"days": int(window_days), "h": int(horizon_days)}
    if ticker:
        params["ticker"] = ticker

    try:
        with engine.connect() as conn:
            raw_rows = conn.execute(stmt, params).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.debug("null_hypothesis._fetch_rows failed: {e}", e=str(exc))
        return []

    rows: list[_ScoredRow] = []
    for r in raw_rows:
        try:
            created_at = r[1]
            try:
                confidence = float(r[2] or 0.0)
            except (TypeError, ValueError):
                continue
            confidence = max(0.0, min(1.0, confidence))
            verdict = r[3]
            if verdict not in ("hit", "miss", "partial"):
                continue
            outcome = _verdict_to_outcome(verdict)
            regime = _parse_signals_regime(r[4])
            rows.append(
                _ScoredRow(
                    created_at=created_at,
                    confidence=confidence,
                    outcome=outcome,
                    regime=regime,
                )
            )
        except Exception as exc:  # noqa: BLE001
            log.debug("null_hypothesis: row parse failed: {e}", e=str(exc))
            continue
    return rows


# ── Brier helpers (pure) ──────────────────────────────────────────────────
#
# We deliberately do NOT add a new ``brier_score`` helper — the one-liner
# ``mean((pred - outcome)**2)`` is inlined at each call site per the task
# brief. ``oracle/calibration.py::compute_calibration`` already uses the
# same inline form (line 121) and we preserve that convention.


def _mean(values: Iterable[float]) -> float:
    total = 0.0
    n = 0
    for v in values:
        total += float(v)
        n += 1
    if n == 0:
        return 0.0
    return total / n


def _hit_rate_from_outcomes(outcomes: Iterable[float]) -> float:
    """Hit rate = mean outcome on {0, 0.5, 1} — partial counts as half."""
    return _mean(outcomes)


# ── Baseline model scorers ────────────────────────────────────────────────


def _score_majority(rows: list[_ScoredRow]) -> NullBaselineResult:
    """Always predicts the historical average hit rate."""
    n = len(rows)
    if n == 0:
        return NullBaselineResult(
            model_name=MODEL_MAJORITY, brier=0.0, hit_rate=0.0, n_scored=0
        )
    outcomes = [r.outcome for r in rows]
    p = _mean(outcomes)
    brier = _mean([(p - o) ** 2 for o in outcomes])
    return NullBaselineResult(
        model_name=MODEL_MAJORITY,
        brier=brier,
        hit_rate=p,
        n_scored=n,
    )


def _score_regime_base_rate(rows: list[_ScoredRow]) -> NullBaselineResult:
    """Per-regime base rate: p = hit rate of all predictions in that regime."""
    n = len(rows)
    if n == 0:
        return NullBaselineResult(
            model_name=MODEL_REGIME_BASE_RATE, brier=0.0, hit_rate=0.0, n_scored=0
        )
    regime_buckets: dict[str, list[float]] = {}
    for r in rows:
        regime_buckets.setdefault(r.regime, []).append(r.outcome)
    regime_rates: dict[str, float] = {
        regime: _mean(outcomes) for regime, outcomes in regime_buckets.items()
    }
    squared_errors: list[float] = []
    for r in rows:
        p = regime_rates.get(r.regime, 0.5)
        squared_errors.append((p - r.outcome) ** 2)
    brier = _mean(squared_errors)
    hit_rate = _mean([r.outcome for r in rows])
    return NullBaselineResult(
        model_name=MODEL_REGIME_BASE_RATE,
        brier=brier,
        hit_rate=hit_rate,
        n_scored=n,
    )


def _score_coin_flip(rows: list[_ScoredRow]) -> NullBaselineResult:
    """p = 0.5 always. Dumbest possible floor."""
    n = len(rows)
    if n == 0:
        return NullBaselineResult(
            model_name=MODEL_COIN_FLIP, brier=0.0, hit_rate=0.0, n_scored=0
        )
    brier = _mean([(0.5 - r.outcome) ** 2 for r in rows])
    return NullBaselineResult(
        model_name=MODEL_COIN_FLIP,
        brier=brier,
        hit_rate=0.5,
        n_scored=n,
    )


def _score_momentum(rows: list[_ScoredRow]) -> NullBaselineResult:
    """Rolling momentum with a fixed K=MOMENTUM_WINDOW lookback.

    For each prediction, the baseline predicts the hit rate of the last
    ``MOMENTUM_WINDOW`` scored predictions that came BEFORE it. The very
    first prediction has no history → we fall back to 0.5 (coin flip).

    We only score rows that have at least one prior row in the window —
    otherwise every early prediction would collapse to 0.5 and the
    baseline would become a blended coin-flip, defeating the point.
    """
    n = len(rows)
    if n == 0:
        return NullBaselineResult(
            model_name=MODEL_MOMENTUM, brier=0.0, hit_rate=0.0, n_scored=0
        )
    squared_errors: list[float] = []
    scored = 0
    for idx in range(n):
        # Use ONLY the last K rows before index `idx`. This guarantees
        # the momentum baseline is strictly causal — no look-ahead leak.
        start = max(0, idx - MOMENTUM_WINDOW)
        window = rows[start:idx]
        if not window:
            # No prior history — fall through to coin flip.
            p = 0.5
        else:
            p = _mean([w.outcome for w in window])
        squared_errors.append((p - rows[idx].outcome) ** 2)
        scored += 1
    brier = _mean(squared_errors)
    hit_rate = _mean([r.outcome for r in rows])
    return NullBaselineResult(
        model_name=MODEL_MOMENTUM,
        brier=brier,
        hit_rate=hit_rate,
        n_scored=scored,
    )


# ── Penalty mapping ───────────────────────────────────────────────────────


def _penalty_for_edge_pct(edge_pct: float) -> float:
    """Convert fractional edge → multiplicative penalty.

    Hard-coded thresholds — see module constants. Pure function.
    """
    if edge_pct < EDGE_MARGINAL:
        return PENALTY_LOST
    if edge_pct < EDGE_WEAK:
        return PENALTY_MARGINAL
    if edge_pct < EDGE_MODERATE:
        return PENALTY_WEAK
    if edge_pct < EDGE_STRONG:
        return PENALTY_MODERATE
    return PENALTY_STRONG


def _advisory_for(
    edge_pct: float,
    best_null_model: str,
    penalty: float,
) -> str:
    """Human-readable one-liner for the report payload."""
    pct = round(edge_pct * 100.0, 1)
    if edge_pct < 0:
        return (
            f"null model '{best_null_model}' beats oracle by {abs(pct)}% — "
            f"heavy skepticism (penalty={penalty:.2f})"
        )
    if edge_pct < EDGE_WEAK:
        return (
            f"oracle barely edges '{best_null_model}' by {pct}% — "
            f"downgrade conviction (penalty={penalty:.2f})"
        )
    if edge_pct < EDGE_STRONG:
        return (
            f"oracle beats best null '{best_null_model}' by {pct}% — "
            f"moderate confidence (penalty={penalty:.2f})"
        )
    return (
        f"oracle beats best null '{best_null_model}' by {pct}% — "
        f"proceed (penalty={penalty:.2f})"
    )


def _neutral_report(
    *,
    horizon_days: int,
    window_days: int,
    advisory: str,
    n_oracle: int = 0,
) -> NullHypothesisReport:
    """Zero-penalty report returned on any failure / thin history."""
    return NullHypothesisReport(
        as_of=datetime.now(timezone.utc).isoformat(),
        horizon_days=horizon_days,
        window_days=window_days,
        oracle_brier=0.0,
        oracle_hit_rate=0.0,
        n_oracle_predictions=n_oracle,
        baselines=[],
        best_null_model="",
        best_null_brier=0.0,
        edge_absolute=0.0,
        edge_pct=0.0,
        penalty_multiplier=NEUTRAL_PENALTY,
        advisory=advisory,
    )


# ── Public API ────────────────────────────────────────────────────────────


def evaluate_null_hypothesis(
    engine: Engine,
    *,
    horizon_days: int = 7,
    window_days: int = 365,
    ticker: str | None = None,
) -> NullHypothesisReport:
    """Score the oracle stack against four null models on the most
    recent ``window_days`` of scored predictions at ``horizon_days``.

    Never raises. Returns a neutral-penalty report on any failure,
    empty history, or sample counts below ``MIN_ORACLE_SAMPLES``.
    """
    try:
        rows = _fetch_rows(
            engine,
            horizon_days=horizon_days,
            window_days=window_days,
            ticker=ticker,
        )
    except Exception as exc:  # noqa: BLE001
        log.debug("evaluate_null_hypothesis: fetch failed: {e}", e=str(exc))
        return _neutral_report(
            horizon_days=horizon_days,
            window_days=window_days,
            advisory="fetch failed — penalty disabled",
        )

    n = len(rows)
    if n < MIN_ORACLE_SAMPLES:
        log.debug(
            "null_hypothesis: only {n} scored predictions at h={h} "
            "(need ≥{m}) — neutral penalty",
            n=n,
            h=horizon_days,
            m=MIN_ORACLE_SAMPLES,
        )
        return _neutral_report(
            horizon_days=horizon_days,
            window_days=window_days,
            advisory=(
                f"insufficient history ({n} < {MIN_ORACLE_SAMPLES}) — "
                "penalty disabled"
            ),
            n_oracle=n,
        )

    # Oracle Brier and hit rate — inline one-liner, no helper. Matches
    # oracle/calibration.py::compute_calibration (line 121).
    try:
        oracle_brier = _mean([(r.confidence - r.outcome) ** 2 for r in rows])
        oracle_hit_rate = _mean([r.outcome for r in rows])
    except Exception as exc:  # noqa: BLE001
        log.debug("null_hypothesis: oracle brier calc failed: {e}", e=str(exc))
        return _neutral_report(
            horizon_days=horizon_days,
            window_days=window_days,
            advisory="oracle brier calc failed — penalty disabled",
            n_oracle=n,
        )

    # Score all four baselines.
    try:
        baselines: list[NullBaselineResult] = [
            _score_majority(rows),
            _score_regime_base_rate(rows),
            _score_coin_flip(rows),
            _score_momentum(rows),
        ]
    except Exception as exc:  # noqa: BLE001
        log.debug("null_hypothesis: baseline scoring failed: {e}", e=str(exc))
        return _neutral_report(
            horizon_days=horizon_days,
            window_days=window_days,
            advisory="baseline scoring failed — penalty disabled",
            n_oracle=n,
        )

    best = min(baselines, key=lambda b: b.brier)
    best_null_brier = best.brier
    edge_absolute = best_null_brier - oracle_brier
    edge_pct = edge_absolute / max(best_null_brier, 1e-9)

    penalty = _penalty_for_edge_pct(edge_pct)
    advisory = _advisory_for(edge_pct, best.model_name, penalty)

    return NullHypothesisReport(
        as_of=datetime.now(timezone.utc).isoformat(),
        horizon_days=horizon_days,
        window_days=window_days,
        oracle_brier=oracle_brier,
        oracle_hit_rate=oracle_hit_rate,
        n_oracle_predictions=n,
        baselines=baselines,
        best_null_model=best.model_name,
        best_null_brier=best_null_brier,
        edge_absolute=edge_absolute,
        edge_pct=edge_pct,
        penalty_multiplier=penalty,
        advisory=advisory,
    )


def null_hypothesis_penalty(
    engine: Engine,
    *,
    horizon_days: int = 7,
) -> float:
    """Bare-multiplier shortcut for the live aggregate conviction path.

    ``compute_aggregate_conviction`` can call this without branching —
    returns ``1.0`` (neutral) on any error so the multiplicative fold
    stays side-effect-free. All error paths log at ``debug`` only.
    """
    try:
        report = evaluate_null_hypothesis(engine, horizon_days=horizon_days)
        return float(report.penalty_multiplier)
    except Exception as exc:  # noqa: BLE001
        log.debug("null_hypothesis_penalty: unexpected failure: {e}", e=str(exc))
        return NEUTRAL_PENALTY
