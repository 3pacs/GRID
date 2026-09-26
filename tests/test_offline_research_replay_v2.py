"""Reproduce the vault #101 vein-scan v2 result through the integrated contract.

Input: the 39-series panel from obsidian-vault 532883a68
(grid-vein-scan-signals-20260923.csv, sha256 b8014421...), committed as a test
fixture. v2 reported 2,640 trials; horizon-spaced: 1,140 testable, 0 BH
survivors (fwd10/fwd20 untestable); weekly + block-permutation null: 2,280
testable, 0 BH survivors. This replay uses 10,000 permutations like v2 and a
whole-run BH denominator (2,640), which can only be stricter than v2's
testable-only denominator. Exploratory replay: nothing here is evidence.
"""

from pathlib import Path

import pytest

from scripts.replay_vein_scan_v2 import V2_SHA256, csv_sha256, replay

CSV = (
    Path(__file__).parent
    / "fixtures/offline_research/grid-vein-scan-signals-20260923.csv"
)


def test_fixture_is_the_v2_panel():
    assert csv_sha256(CSV) == V2_SHA256


@pytest.mark.parametrize(
    "sampling, testable, untestable_by_horizon, blocks",
    [
        ("horizon_spaced", 1140, {1: 90, 5: 90, 10: 660, 20: 660}, [1]),
        ("fixed_step_block_null", 2280, {1: 90, 5: 90, 10: 90, 20: 90}, [1, 2, 4]),
    ],
)
@pytest.mark.slow
def test_replay_reproduces_v2_zero_survivors(
    sampling, testable, untestable_by_horizon, blocks
):
    result = replay(CSV, sampling, perms=10000, seed=20260924)
    assert result["trials_attempted"] == 2640
    assert result["trials_testable"] == testable
    assert result["untestable_by_horizon"] == untestable_by_horizon
    assert result["blocks"] == blocks
    assert result["holdout_start"] == "2026-02-09"
    assert result["discovery_first_decision"] == "2025-03-03"
    assert result["bh10_survivors"] == result["bh05_survivors"] == 0
    assert result["bh10_survivors_testable_only_denominator"] == 0
    assert result["selected"] == 0
    assert result["candidate_eligible"] == (sampling == "horizon_spaced")
    assert result["holdout_checks"] == result["candidates"] == 0
    assert result["state"] == "EXPLORATORY_REPLAY_ONLY"
