"""
Tests for trading/solana/exit_learner.py.

The learner is I/O-bound only through the store, so we mock the store
and drive the bandit with seeded RNG for full determinism.
"""

from __future__ import annotations

import random
from unittest.mock import MagicMock

import pytest

from trading.solana.exit_learner import (
    REWARD_CLIP_HIGH,
    REWARD_CLIP_LOW,
    ExitLearner,
    _clip_reward,
)
from trading.solana.exit_policy import SEED_VARIANTS
from trading.solana.exit_state import VariantStatsRow


# ----------------------------------------------------------------------
# Reward clipping
# ----------------------------------------------------------------------
def test_clip_reward_within_range():
    assert _clip_reward(0.5) == 0.5
    assert _clip_reward(-0.5) == -0.5


def test_clip_reward_high_tail():
    assert _clip_reward(50.0) == REWARD_CLIP_HIGH


def test_clip_reward_low_tail():
    assert _clip_reward(-10.0) == REWARD_CLIP_LOW


def test_clip_reward_nan():
    assert _clip_reward(float("nan")) == 0.0


# ----------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------
def _stats(variant_id: str, n: int, mean: float, m2: float = 0.0) -> VariantStatsRow:
    return VariantStatsRow(
        variant_id=variant_id,
        source_type="unknown",
        n_samples=n,
        reward_mean=mean,
        reward_m2=m2,
        total_pnl_pct=mean * n,
        wins=n if mean > 0 else 0,
        losses=0 if mean > 0 else n,
        last_reward=mean,
    )


@pytest.fixture()
def mock_store():
    store = MagicMock()
    store.get_variant_stats.return_value = None  # all arms unseen by default
    return store


# ----------------------------------------------------------------------
# ensure_variants
# ----------------------------------------------------------------------
def test_ensure_variants_upserts_every_seed(mock_store):
    learner = ExitLearner(store=mock_store, random_state=random.Random(0))
    learner.ensure_variants()
    assert mock_store.upsert_variant.call_count == len(SEED_VARIANTS)
    variant_ids = {c.kwargs["variant_id"] for c in mock_store.upsert_variant.call_args_list}
    assert variant_ids == {v.variant_id for v in SEED_VARIANTS}


def test_ensure_variants_fans_out_source_types(mock_store):
    learner = ExitLearner(store=mock_store, random_state=random.Random(0))
    learner.ensure_variants(source_types=("smart_money", "kol"))
    # 4 variants * 2 source types = 8 upserts
    assert mock_store.upsert_variant.call_count == 8


# ----------------------------------------------------------------------
# select_policy
# ----------------------------------------------------------------------
def test_select_policy_with_no_data_still_returns_a_variant(mock_store):
    learner = ExitLearner(store=mock_store, random_state=random.Random(42))
    policy = learner.select_policy()
    assert policy in SEED_VARIANTS


def test_select_policy_is_deterministic_with_fixed_rng(mock_store):
    # Two learners with the same seed and empty stats should agree.
    l1 = ExitLearner(store=mock_store, random_state=random.Random(7))
    l2 = ExitLearner(store=mock_store, random_state=random.Random(7))
    assert l1.select_policy().variant_id == l2.select_policy().variant_id


def test_select_policy_favours_arm_with_much_higher_mean(mock_store):
    # Arm A is a clear winner with lots of evidence; the rest are mediocre.
    def stats_by(variant_id, source_type):
        if variant_id == "balanced":
            return _stats("balanced", n=200, mean=1.5, m2=20.0)
        return _stats(variant_id, n=200, mean=-0.05, m2=20.0)

    mock_store.get_variant_stats.side_effect = stats_by
    picks = []
    for seed in range(50):
        l = ExitLearner(store=mock_store, random_state=random.Random(seed))
        picks.append(l.select_policy().variant_id)
    # Balanced should win the majority of draws when its posterior mean
    # is well separated from the rest.
    assert picks.count("balanced") >= 40


def test_select_policy_explores_when_arms_tied(mock_store):
    # All arms identical — Thompson should produce variety across seeds.
    def stats_by(variant_id, source_type):
        return _stats(variant_id, n=20, mean=0.2, m2=5.0)

    mock_store.get_variant_stats.side_effect = stats_by
    picks = set()
    for seed in range(20):
        l = ExitLearner(store=mock_store, random_state=random.Random(seed))
        picks.add(l.select_policy().variant_id)
    # Should see at least 3 of 4 arms over 20 seeds.
    assert len(picks) >= 3


def test_select_policy_falls_back_on_unknown_variant(mock_store):
    # Simulate a stale DB variant_id that's not in SEED_VARIANTS.
    learner = ExitLearner(store=mock_store, random_state=random.Random(0))
    # Patch _sample_posteriors so we force the broken variant to win.
    from trading.solana.exit_learner import VariantPosterior

    learner._sample_posteriors = lambda src: [  # type: ignore[assignment]
        VariantPosterior(
            variant_id="ghost_variant",
            source_type="unknown",
            n_samples=0,
            mean=0.0,
            stddev=1.0,
            sampled=999.0,
        )
    ]
    policy = learner.select_policy()
    assert policy in SEED_VARIANTS  # fallback to first configured variant


# ----------------------------------------------------------------------
# record_outcome
# ----------------------------------------------------------------------
def test_record_outcome_clips_and_forwards(mock_store):
    learner = ExitLearner(store=mock_store, random_state=random.Random(0))
    reward = learner.record_outcome(
        variant_id="balanced", source_type="unknown", pnl_pct=42.0
    )
    assert reward == REWARD_CLIP_HIGH
    call = mock_store.update_variant_stats.call_args.kwargs
    assert call["variant_id"] == "balanced"
    assert call["new_reward"] == REWARD_CLIP_HIGH
    assert call["pnl_pct"] == 42.0


def test_record_outcome_passes_raw_pnl_pct_for_stats(mock_store):
    learner = ExitLearner(store=mock_store, random_state=random.Random(0))
    learner.record_outcome("balanced", "unknown", pnl_pct=0.25)
    call = mock_store.update_variant_stats.call_args.kwargs
    assert call["new_reward"] == pytest.approx(0.25)
    assert call["pnl_pct"] == pytest.approx(0.25)


# ----------------------------------------------------------------------
# Observability
# ----------------------------------------------------------------------
def test_stats_snapshot_reports_every_variant(mock_store):
    def stats_by(variant_id, source_type):
        if variant_id == "balanced":
            return _stats("balanced", n=10, mean=0.3, m2=1.0)
        return None

    mock_store.get_variant_stats.side_effect = stats_by
    learner = ExitLearner(store=mock_store, random_state=random.Random(0))
    snapshot = learner.stats_snapshot()
    ids = {row["variant_id"] for row in snapshot}
    assert ids == {v.variant_id for v in SEED_VARIANTS}
    balanced = next(r for r in snapshot if r["variant_id"] == "balanced")
    assert balanced["n_samples"] == 10
    assert balanced["reward_mean"] == 0.3
    assert balanced["win_rate"] == 1.0  # all wins in our _stats helper


def test_debug_posteriors_returns_one_per_variant(mock_store):
    learner = ExitLearner(store=mock_store, random_state=random.Random(0))
    posteriors = learner.debug_posteriors()
    assert len(posteriors) == len(SEED_VARIANTS)
    assert {p.variant_id for p in posteriors} == {v.variant_id for v in SEED_VARIANTS}


# ----------------------------------------------------------------------
# Constructor validation
# ----------------------------------------------------------------------
def test_constructor_rejects_empty_variants(mock_store):
    with pytest.raises(ValueError):
        ExitLearner(store=mock_store, variants=(), random_state=random.Random(0))


def test_constructor_requires_store_or_engine():
    with pytest.raises(ValueError):
        ExitLearner()
