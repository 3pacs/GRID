"""CAT-121 — HMM regime transition matrix tests."""
from __future__ import annotations

from unittest.mock import MagicMock


from intelligence.hmm_regime_transitions import (
    DEFAULT_STATES,
    compute_entropy,
    fit_from_db,
    fit_transition_matrix,
    forecast_horizon,
    next_state_distribution,
    read_regime_history,
)


def _build_engine(rows):
    eng = MagicMock()
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    result = MagicMock()
    result.fetchall.return_value = [(r,) for r in rows]
    conn.execute.return_value = result
    eng.connect.return_value = conn
    return eng


class TestFitTransitionMatrix:
    def test_empty_history_returns_uniform_ish(self):
        tm = fit_transition_matrix([], DEFAULT_STATES)
        assert tm.transitions_observed == 0
        for row in tm.matrix:
            assert abs(sum(row) - 1.0) < 1e-6

    def test_single_transition(self):
        tm = fit_transition_matrix(
            ["NEUTRAL", "EXPANSION"],
            DEFAULT_STATES,
            laplace_alpha=0.0,
        )
        assert tm.transitions_observed == 1
        row = tm.row("NEUTRAL")
        assert row["EXPANSION"] == 1.0

    def test_sticky_regime_high_diagonal(self):
        history = ["NEUTRAL"] * 30
        tm = fit_transition_matrix(history, DEFAULT_STATES)
        assert tm.stickiness_per_state["NEUTRAL"] > 0.9

    def test_half_life_computed(self):
        history = ["NEUTRAL"] * 100 + ["EXPANSION"] * 10
        tm = fit_transition_matrix(history, DEFAULT_STATES)
        assert tm.half_life_days["NEUTRAL"] > 10

    def test_all_rows_stochastic(self):
        import random
        random.seed(42)
        history = [random.choice(DEFAULT_STATES) for _ in range(100)]
        tm = fit_transition_matrix(history, DEFAULT_STATES)
        for row in tm.matrix:
            assert abs(sum(row) - 1.0) < 1e-6

    def test_laplace_prevents_zero_probs(self):
        tm = fit_transition_matrix(
            ["NEUTRAL", "EXPANSION"],
            DEFAULT_STATES,
        )
        row = tm.row("CRISIS")
        for p in row.values():
            assert p > 0


class TestNextStateDistribution:
    def test_returns_row_dict(self):
        history = ["NEUTRAL"] * 20 + ["EXPANSION"]
        tm = fit_transition_matrix(history, DEFAULT_STATES)
        dist = next_state_distribution("NEUTRAL", tm)
        assert abs(sum(dist.values()) - 1.0) < 1e-6
        assert "NEUTRAL" in dist
        assert "EXPANSION" in dist

    def test_unknown_state_empty(self):
        tm = fit_transition_matrix(["NEUTRAL"], DEFAULT_STATES)
        assert next_state_distribution("UNKNOWN", tm) == {}


class TestForecastHorizon:
    def test_forecast_length(self):
        history = ["NEUTRAL"] * 20
        tm = fit_transition_matrix(history, DEFAULT_STATES)
        fc = forecast_horizon("NEUTRAL", tm, steps=5)
        assert len(fc.distributions) == 5
        assert fc.horizons == [1, 2, 3, 4, 5]

    def test_forecast_sums_to_one(self):
        history = ["NEUTRAL"] * 20
        tm = fit_transition_matrix(history, DEFAULT_STATES)
        fc = forecast_horizon("NEUTRAL", tm, steps=3)
        for dist in fc.distributions:
            assert abs(sum(dist.values()) - 1.0) < 1e-6

    def test_unknown_state_uniform(self):
        tm = fit_transition_matrix(["NEUTRAL"], DEFAULT_STATES)
        fc = forecast_horizon("BOGUS", tm, steps=2)
        n = len(DEFAULT_STATES)
        for dist in fc.distributions:
            for p in dist.values():
                assert abs(p - 1.0 / n) < 1e-6

    def test_most_likely_at(self):
        history = ["NEUTRAL"] * 50
        tm = fit_transition_matrix(history, DEFAULT_STATES)
        fc = forecast_horizon("NEUTRAL", tm, steps=3)
        state, prob = fc.most_likely_at(0)
        assert state == "NEUTRAL"
        assert prob > 0.5

    def test_forecast_to_dict(self):
        history = ["NEUTRAL"] * 20
        tm = fit_transition_matrix(history, DEFAULT_STATES)
        fc = forecast_horizon("NEUTRAL", tm, steps=2)
        d = fc.to_dict()
        assert "current_state" in d
        assert "horizons" in d
        assert "distributions" in d


class TestComputeEntropy:
    def test_zero_entropy_for_delta(self):
        dist = {"A": 1.0, "B": 0.0, "C": 0.0}
        assert compute_entropy(dist) == 0.0

    def test_max_entropy_for_uniform(self):
        import math
        dist = {s: 0.2 for s in DEFAULT_STATES}
        assert abs(compute_entropy(dist) - math.log2(5)) < 1e-6

    def test_empty_distribution(self):
        assert compute_entropy({}) == 0.0


class TestTransitionMatrixSerialization:
    def test_to_dict_shape(self):
        history = ["NEUTRAL"] * 20 + ["EXPANSION"]
        tm = fit_transition_matrix(history, DEFAULT_STATES)
        d = tm.to_dict()
        for k in ("states", "matrix", "transitions_observed",
                  "stickiness_per_state", "half_life_days"):
            assert k in d
        assert len(d["matrix"]) == len(DEFAULT_STATES)


class TestDBWrappers:
    def test_read_regime_history_empty(self):
        eng = _build_engine([])
        assert read_regime_history(eng) == []

    def test_read_regime_history_populated(self):
        eng = _build_engine(["NEUTRAL", "TIGHTENING", "NEUTRAL", "EXPANSION"])
        assert read_regime_history(eng) == ["NEUTRAL", "TIGHTENING", "NEUTRAL", "EXPANSION"]

    def test_db_error_returns_empty(self):
        eng = MagicMock()
        eng.connect.side_effect = RuntimeError("down")
        assert read_regime_history(eng) == []

    def test_fit_from_db_insufficient_history(self):
        eng = _build_engine(["NEUTRAL"] * 10)
        assert fit_from_db(eng) is None

    def test_fit_from_db_sufficient(self):
        history = ["NEUTRAL"] * 50 + ["EXPANSION"] * 10
        eng = _build_engine(history)
        tm = fit_from_db(eng)
        assert tm is not None
        assert tm.transitions_observed > 0
