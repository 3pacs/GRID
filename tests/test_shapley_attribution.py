"""ALPHA-9 — Shapley attribution tests.

Pure-function tests on intelligence/shapley_attribution.py. Exact Shapley
over small ensembles, leave-one-out for large ones, fragility multiplier
behavior, edge cases.
"""
from __future__ import annotations


from intelligence.shapley_attribution import (
    attribute_votes,
    shapley_exact,
    shapley_leave_one_out,
)


# ── shapley_exact ─────────────────────────────────────────────────────────


class TestShapleyExact:
    def test_single_contributor(self):
        weights = {"a": 5.0}
        result = shapley_exact(["a"], lambda s: sum(weights[c] for c in s))
        assert abs(result["a"] - 5.0) < 1e-6

    def test_two_equal_contributors(self):
        weights = {"a": 3.0, "b": 3.0}
        result = shapley_exact(["a", "b"], lambda s: sum(weights[c] for c in s))
        assert abs(result["a"] - 3.0) < 1e-6
        assert abs(result["b"] - 3.0) < 1e-6

    def test_three_unequal_contributors(self):
        weights = {"a": 5.0, "b": 3.0, "c": 1.0}
        result = shapley_exact(["a", "b", "c"], lambda s: sum(weights[c] for c in s))
        # Additive value function → Shapley equals raw weight
        assert abs(result["a"] - 5.0) < 1e-6
        assert abs(result["b"] - 3.0) < 1e-6
        assert abs(result["c"] - 1.0) < 1e-6

    def test_efficiency_axiom(self):
        """Sum of Shapley values = v(full) - v(empty)."""
        weights = {"a": 2.0, "b": 4.0, "c": 6.0, "d": 1.0}
        result = shapley_exact(list(weights), lambda s: sum(weights[c] for c in s))
        assert abs(sum(result.values()) - sum(weights.values())) < 1e-6

    def test_empty_input(self):
        assert shapley_exact([], lambda s: 0) == {}


# ── shapley_leave_one_out ─────────────────────────────────────────────────


class TestShapleyLeaveOneOut:
    def test_additive_matches_exact(self):
        """For an additive value function, leave-one-out should match exact."""
        weights = {"a": 1.0, "b": 2.0, "c": 3.0}
        loo = shapley_leave_one_out(list(weights), lambda s: sum(weights[c] for c in s))
        exact = shapley_exact(list(weights), lambda s: sum(weights[c] for c in s))
        for k in weights:
            assert abs(loo[k] - exact[k]) < 1e-6

    def test_zero_value_function(self):
        result = shapley_leave_one_out(["a", "b"], lambda s: 0)
        assert result == {"a": 0.0, "b": 0.0}

    def test_efficiency_axiom_after_rescale(self):
        weights = {"a": 1.0, "b": 2.0, "c": 3.0}
        result = shapley_leave_one_out(list(weights), lambda s: sum(weights[c] for c in s))
        assert abs(sum(result.values()) - sum(weights.values())) < 1e-6


# ── attribute_votes integration ───────────────────────────────────────────


class TestAttributeVotes:
    def test_balanced_low_herfindahl(self):
        votes = [
            {"model_name": "m1", "vote_weight": 1.0},
            {"model_name": "m2", "vote_weight": 1.0},
            {"model_name": "m3", "vote_weight": 1.0},
            {"model_name": "m4", "vote_weight": 1.0},
        ]
        attr = attribute_votes(votes)
        # 4 equal contributors → Herfindahl = 4 * (0.25)^2 = 0.25
        assert abs(attr.herfindahl - 0.25) < 1e-6
        assert attr.fragility_multiplier == 1.0  # below threshold
        assert attr.top_share == 0.25

    def test_concentrated_high_herfindahl_dampens(self):
        votes = [
            {"model_name": "dominant", "vote_weight": 10.0},
            {"model_name": "tiny1", "vote_weight": 0.5},
            {"model_name": "tiny2", "vote_weight": 0.5},
        ]
        attr = attribute_votes(votes)
        # Dominant has 10/11 ≈ 0.91 share → Herfindahl ≈ 0.83
        assert attr.top_contributor == "dominant"
        assert attr.top_share > 0.85
        assert attr.herfindahl > 0.7
        assert attr.fragility_multiplier < 1.0
        assert attr.fragility_multiplier >= 0.5  # floor

    def test_fragility_floor_at_half(self):
        votes = [
            {"model_name": "only", "vote_weight": 100.0},
            {"model_name": "noop", "vote_weight": 0.0},
        ]
        attr = attribute_votes(votes)
        # Single dominant → Herfindahl=1.0 → max shrink
        assert abs(attr.fragility_multiplier - 0.5) < 1e-6

    def test_empty_votes(self):
        attr = attribute_votes([])
        assert attr.n == 0
        assert attr.fragility_multiplier == 1.0
        assert attr.contributions == {}

    def test_dedupes_by_model_name(self):
        votes = [
            {"model_name": "m1", "vote_weight": 1.0},
            {"model_name": "m1", "vote_weight": 2.0},  # same name, second vote
            {"model_name": "m2", "vote_weight": 1.5},
        ]
        attr = attribute_votes(votes)
        assert attr.n == 2
        assert "m1" in attr.contributions
        # m1's combined weight should be 3.0
        assert abs(attr.contributions["m1"] - 3.0) < 1e-6

    def test_negative_weights_clamped(self):
        votes = [
            {"model_name": "m1", "vote_weight": -1.0},
            {"model_name": "m2", "vote_weight": 2.0},
        ]
        attr = attribute_votes(votes)
        assert attr.contributions["m1"] == 0.0

    def test_to_dict_shape(self):
        votes = [
            {"model_name": "m1", "vote_weight": 1.0},
            {"model_name": "m2", "vote_weight": 1.0},
        ]
        d = attribute_votes(votes).to_dict()
        for k in ("contributions", "total", "herfindahl", "top_contributor",
                  "top_share", "fragility_multiplier", "n"):
            assert k in d


class TestLargeEnsembleUsesLeaveOneOut:
    def test_above_max_n_uses_loo(self):
        """An ensemble of n=15 should fall through to LOO without crashing."""
        votes = [
            {"model_name": f"m{i}", "vote_weight": float(i + 1)}
            for i in range(15)
        ]
        attr = attribute_votes(votes)
        assert attr.n == 15
        # Sum should equal the total weight (efficiency axiom)
        assert abs(attr.total - sum(i + 1 for i in range(15))) < 1e-6
