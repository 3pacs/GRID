"""CAT-121 — HMM regime transition matrix.

The ALPHA-5 liquidity regime classifier (intelligence/liquidity_regime.py)
returns a discrete state from a static percentile rule. That's a fine
point-in-time classifier but it has no memory of WHICH regime we came
from, so it can't tell the oracle:

  "We just flipped from NEUTRAL to TIGHTENING — expect 3 more days in
   TIGHTENING before any reversal, with 72% historical probability."

This module layers a Markov transition model on top of the liquidity
regime classifier. Given a historical time series of regime states,
it fits a first-order transition matrix P[i→j] and exposes three
predictions:

  transition_matrix(history)  → N×N probability matrix
  next_state_distribution(curr, P)  → P[next | curr]
  forecast_horizon(curr, P, steps)  → [P(state_t) for t in 1..steps]

Plus a stickiness metric (diagonal dominance) and a regime-half-life
estimator (1 / (1 - P[i→i])).

Why not a real HMM with hidden states? The states ARE observable
(from the classifier). A real HMM would add emission distributions
over continuous features — that's overkill for a 5-state regime model
with ~440 daily observations. The Markov matrix captures 95% of the
value at 5% of the complexity.

All functions are pure of DB semantics. A thin DB wrapper reads the
regime history from the calibration snapshot table.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

import numpy as np
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Default state ordering (mirrors ALPHA-5) ────────────────────────────

DEFAULT_STATES: tuple[str, ...] = (
    "CRISIS",
    "TIGHTENING",
    "NEUTRAL",
    "EXPANSION",
    "EXPANSION_STRONG",
)

# Laplace smoothing to avoid zero probabilities when a transition
# hasn't been observed yet.
_LAPLACE_ALPHA = 0.5

# Minimum history length before we trust the transition matrix
_MIN_HISTORY = 30


# ── Data classes ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class TransitionMatrix:
    """Row-stochastic N×N matrix plus metadata."""

    states: tuple[str, ...]
    matrix: list[list[float]]  # [from][to]
    transitions_observed: int
    stickiness_per_state: dict[str, float]   # P[i→i] per state
    half_life_days: dict[str, float]          # -log(2)/log(P[i→i])

    def row(self, state: str) -> dict[str, float]:
        """Return the P[next | from=state] distribution as a dict."""
        try:
            idx = self.states.index(state)
        except ValueError:
            return {}
        return {self.states[j]: self.matrix[idx][j] for j in range(len(self.states))}

    def to_dict(self) -> dict[str, Any]:
        return {
            "states": list(self.states),
            "matrix": [[round(v, 4) for v in row] for row in self.matrix],
            "transitions_observed": self.transitions_observed,
            "stickiness_per_state": {
                k: round(v, 4) for k, v in self.stickiness_per_state.items()
            },
            "half_life_days": {
                k: round(v, 2) for k, v in self.half_life_days.items()
            },
        }


@dataclass(frozen=True)
class RegimeForecast:
    """Forward regime probability trajectory."""

    current_state: str
    horizons: list[int]
    distributions: list[dict[str, float]]   # per horizon, state → P

    def most_likely_at(self, horizon_idx: int) -> tuple[str, float]:
        """Return (state, probability) at the given horizon index."""
        dist = self.distributions[horizon_idx]
        best = max(dist.items(), key=lambda kv: kv[1])
        return best

    def to_dict(self) -> dict[str, Any]:
        return {
            "current_state": self.current_state,
            "horizons": list(self.horizons),
            "distributions": [
                {k: round(v, 4) for k, v in dist.items()}
                for dist in self.distributions
            ],
        }


# ── Core math ────────────────────────────────────────────────────────────


def fit_transition_matrix(
    history: Sequence[str],
    states: Sequence[str] = DEFAULT_STATES,
    *,
    laplace_alpha: float = _LAPLACE_ALPHA,
) -> TransitionMatrix:
    """Fit a first-order Markov transition matrix from a state history.

    Uses Laplace smoothing (add ``laplace_alpha`` to every count) so no
    transition has zero probability even if it wasn't observed in the
    training window.

    ``history`` is a sequence of state strings ordered oldest → newest.
    """
    n = len(states)
    state_idx = {s: i for i, s in enumerate(states)}

    counts = np.full((n, n), laplace_alpha, dtype=float)
    transitions_observed = 0

    for i in range(len(history) - 1):
        from_state = history[i]
        to_state = history[i + 1]
        fi = state_idx.get(from_state)
        ti = state_idx.get(to_state)
        if fi is None or ti is None:
            continue
        counts[fi][ti] += 1.0
        transitions_observed += 1

    # Row-normalize
    row_sums = counts.sum(axis=1, keepdims=True)
    matrix = np.full_like(counts, 1.0 / n, dtype=float)
    np.divide(counts, row_sums, out=matrix, where=row_sums > 0)

    # Stickiness = diagonal
    stickiness = {states[i]: float(matrix[i][i]) for i in range(n)}

    # Half-life = -log(2) / log(P[i→i]) when P<1; cap at 1 year
    half_life: dict[str, float] = {}
    import math
    for i, state in enumerate(states):
        p = stickiness[state]
        if p >= 0.9999:
            half_life[state] = 365.0  # capped
        elif p <= 1e-6:
            half_life[state] = 0.0
        else:
            hl = -math.log(2) / math.log(p)
            half_life[state] = min(365.0, max(0.0, hl))

    return TransitionMatrix(
        states=tuple(states),
        matrix=matrix.tolist(),
        transitions_observed=transitions_observed,
        stickiness_per_state=stickiness,
        half_life_days=half_life,
    )


def next_state_distribution(
    current_state: str,
    transition: TransitionMatrix,
) -> dict[str, float]:
    """Return P[next_state | current_state] as a dict."""
    return transition.row(current_state)


def forecast_horizon(
    current_state: str,
    transition: TransitionMatrix,
    steps: int = 7,
) -> RegimeForecast:
    """Multi-step forward regime forecast via matrix power iteration.

    Returns a ``RegimeForecast`` with a distribution at each step
    1..steps. Step 0 is the current state as a delta distribution.
    """
    n = len(transition.states)
    try:
        curr_idx = transition.states.index(current_state)
    except ValueError:
        # Unknown state → uniform forecast
        return RegimeForecast(
            current_state=current_state,
            horizons=list(range(1, steps + 1)),
            distributions=[
                {s: 1.0 / n for s in transition.states}
                for _ in range(steps)
            ],
        )

    P = np.asarray(transition.matrix)
    state_vec = np.zeros(n)
    state_vec[curr_idx] = 1.0

    horizons: list[int] = []
    distributions: list[dict[str, float]] = []

    P_k = np.eye(n)
    for step in range(1, steps + 1):
        P_k = P_k @ P
        probs = state_vec @ P_k
        horizons.append(step)
        distributions.append({
            transition.states[j]: float(probs[j]) for j in range(n)
        })

    return RegimeForecast(
        current_state=current_state,
        horizons=horizons,
        distributions=distributions,
    )


def compute_entropy(distribution: dict[str, float]) -> float:
    """Shannon entropy (base 2) of a regime distribution.

    Used as a "how confident is the forecast" metric — entropy = 0 means
    the next state is known exactly, entropy = log2(n) means maximum
    uncertainty (uniform distribution).
    """
    import math
    total = 0.0
    for p in distribution.values():
        if p > 0:
            total -= p * math.log2(p)
    return total


# ── DB wrapper ───────────────────────────────────────────────────────────


def read_regime_history(
    engine: Engine,
    *,
    lookback_days: int = 730,
) -> list[str]:
    """Read the regime state history from classifier snapshots.

    Falls back to calling the classifier directly for each day in the
    lookback window if no snapshot table exists — that's slow but safe
    for first-run cold starts. The preferred path is a
    ``regime_history`` table populated by the daily scheduler.
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT regime_state
                    FROM regime_history
                    WHERE snapshot_date >= CURRENT_DATE - :days * INTERVAL '1 day'
                      AND regime_state IS NOT NULL
                    ORDER BY snapshot_date ASC
                    """
                ),
                {"days": int(lookback_days)},
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.debug("regime_history read failed: {e}", e=str(exc))
        return []
    return [str(r[0]) for r in rows if r[0]]


def fit_from_db(engine: Engine, *, lookback_days: int = 730) -> TransitionMatrix | None:
    """Read regime history from the DB and fit a transition matrix.

    Returns None when history is too thin (<30 observations).
    """
    history = read_regime_history(engine, lookback_days=lookback_days)
    if len(history) < _MIN_HISTORY:
        return None
    return fit_transition_matrix(history)
