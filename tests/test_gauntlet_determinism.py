"""Regression tests for gauntlet seed determinism.

Locks down PUNCH-LIST-2026-05-13.md (Auditor 2026-06-07 — alpha_research/ [P1]):
`permutation_test` and `subsample_stability` previously used un-seeded
`np.random.permutation` / `np.random.shuffle`, so the same signal + returns
input produced different `permutation_p` and `subsample_stability` values
across runs — which could flip the ROBUST / MARGINAL / UNSTABLE verdict
at `gauntlet.py:257`. These tests guarantee that, given a fixed `seed`,
the gauntlet's stochastic outputs are bit-identical across calls.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from alpha_research.validation.gauntlet import (
    permutation_test,
    run_gauntlet,
    subsample_stability,
)


@pytest.fixture
def signal_panel() -> pd.DataFrame:
    """Deterministic synthetic signal: 200 days x 12 tickers."""
    rng = np.random.default_rng(7)
    dates = pd.bdate_range("2024-01-01", periods=200)
    tickers = [f"T{i:02d}" for i in range(12)]
    return pd.DataFrame(
        rng.normal(0.0, 1.0, size=(len(dates), len(tickers))),
        index=dates,
        columns=tickers,
    )


@pytest.fixture
def forward_returns_panel(signal_panel: pd.DataFrame) -> pd.DataFrame:
    """Forward returns weakly correlated with the signal for realistic IC."""
    rng = np.random.default_rng(11)
    noise = rng.normal(0.0, 0.02, size=signal_panel.shape)
    weak_signal_corr = 0.001 * signal_panel.values
    return pd.DataFrame(
        weak_signal_corr + noise,
        index=signal_panel.index,
        columns=signal_panel.columns,
    )


class TestPermutationTestSeed:
    def test_same_seed_produces_identical_p_value(
        self, signal_panel, forward_returns_panel
    ):
        p1, sr1 = permutation_test(
            signal_panel, forward_returns_panel, n_shuffles=25, top_n=3, seed=42
        )
        p2, sr2 = permutation_test(
            signal_panel, forward_returns_panel, n_shuffles=25, top_n=3, seed=42
        )
        assert p1 == p2
        assert sr1 == sr2

    def test_different_seeds_produce_different_p_value(
        self, signal_panel, forward_returns_panel
    ):
        p1, _ = permutation_test(
            signal_panel, forward_returns_panel, n_shuffles=25, top_n=3, seed=1
        )
        p2, _ = permutation_test(
            signal_panel, forward_returns_panel, n_shuffles=25, top_n=3, seed=2
        )
        # Different RNG seeds should sample different null distributions;
        # 25 shuffles on a noisy panel is more than enough to separate them.
        assert p1 != p2

    def test_default_seed_is_deterministic_without_global_rng_changes(
        self, signal_panel, forward_returns_panel
    ):
        # Bracket the call with global-RNG perturbations. A correct
        # implementation uses default_rng(seed) and ignores the global state,
        # so the bracketed call must match an un-bracketed call.
        np.random.seed(123)
        np.random.normal(size=10_000)  # advance the legacy global RNG
        p_bracketed, _ = permutation_test(
            signal_panel, forward_returns_panel, n_shuffles=20, top_n=3
        )

        np.random.seed(999)
        np.random.normal(size=42)  # different global state
        p_unbracketed, _ = permutation_test(
            signal_panel, forward_returns_panel, n_shuffles=20, top_n=3
        )

        assert p_bracketed == p_unbracketed


class TestSubsampleStabilitySeed:
    def test_same_seed_produces_identical_stability(
        self, signal_panel, forward_returns_panel
    ):
        s1 = subsample_stability(
            signal_panel, forward_returns_panel, n_splits=15, top_n=3, seed=42
        )
        s2 = subsample_stability(
            signal_panel, forward_returns_panel, n_splits=15, top_n=3, seed=42
        )
        assert s1 == s2

    def test_default_seed_is_deterministic_without_global_rng_changes(
        self, signal_panel, forward_returns_panel
    ):
        np.random.seed(123)
        np.random.shuffle(list(range(100)))  # advance legacy global RNG
        s_bracketed = subsample_stability(
            signal_panel, forward_returns_panel, n_splits=15, top_n=3
        )

        np.random.seed(999)
        np.random.shuffle(list(range(7)))
        s_unbracketed = subsample_stability(
            signal_panel, forward_returns_panel, n_splits=15, top_n=3
        )

        assert s_bracketed == s_unbracketed


class TestRunGauntletSeed:
    def test_same_seed_produces_identical_verdict_and_metrics(
        self, signal_panel, forward_returns_panel
    ):
        r1 = run_gauntlet(
            signal_panel,
            forward_returns_panel,
            n_models_tested=1,
            top_n=3,
            n_permutations=20,
            n_subsample_splits=10,
            seed=42,
        )
        r2 = run_gauntlet(
            signal_panel,
            forward_returns_panel,
            n_models_tested=1,
            top_n=3,
            n_permutations=20,
            n_subsample_splits=10,
            seed=42,
        )
        assert r1.verdict == r2.verdict
        assert r1.permutation_p == r2.permutation_p
        assert r1.subsample_stability == r2.subsample_stability
        assert r1.observed_sharpe == r2.observed_sharpe
        assert r1.cv_consistency == r2.cv_consistency

    def test_seed_threads_into_permutation_and_subsample(
        self, signal_panel, forward_returns_panel
    ):
        # Calling run_gauntlet with a specific seed must match calling the
        # two stochastic sub-tests directly with the same seed. This guards
        # against future regressions where one branch forgets to forward
        # the seed kwarg.
        gauntlet = run_gauntlet(
            signal_panel,
            forward_returns_panel,
            n_models_tested=1,
            top_n=3,
            n_permutations=20,
            n_subsample_splits=10,
            seed=99,
        )
        direct_p, _ = permutation_test(
            signal_panel, forward_returns_panel, n_shuffles=20, top_n=3, seed=99
        )
        direct_s = subsample_stability(
            signal_panel, forward_returns_panel, n_splits=10, top_n=3, seed=99
        )
        assert gauntlet.permutation_p == direct_p
        assert gauntlet.subsample_stability == direct_s
