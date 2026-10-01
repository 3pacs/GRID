"""E0 v1 at the decision-relevant operating point: planted IC 0.01 (EVAL-E0C1).

``tests/test_e0_benchmark.py`` checks planted power only at IC 0.05 (the
pinned ``ci`` profile), which any working pipeline passes. The VS1 gate is
decided at IC 0.01 (v7 was stopped on E0's 0.414 there), so this file tests
E0 at that point, composed from E0's public functions exactly as
``evals.e0.benchmark._scenario_job`` wires them. ``evals/e0`` is frozen
(e0-v1): nothing here adds a profile or edits the package.

Per scenario (the headline ``factor_t_garch_exposed`` and ``gaussian_idio``):

1. the plant scale for IC 0.01 is calibrated on the released pilot worlds
   (pilot seed, pilot_sims and pilot_scale from config) and must reproduce the
   released scorecard's scale; the realised mean rank IC on FRESH worlds (the
   scored worlds' base seed, disjoint from the pilot seed) must be 0.01 +/- 0.0015;
2. a deterministic regression pin: the exact Holm and BH hit counts at IC 0.01
   (and the null primary-trial Holm count) for SIMS worlds at PERMS sign flips;
3. that power must be statistically consistent with the published e0-v1
   scorecard (500 sims, 999 flips);
4. at IC 0 the primary trial's Holm rejection rate is at most 0.05 + 3 SE.

The whole file takes ~14 s on ubuntu-latest (the two per-scenario setups are
computed once and shared). Synthetic outcomes only: the structure is SEC
feature geometry; no price, return or IC of any VS1 window is read, and there
is no DB access.
"""

from __future__ import annotations

import json
import math
from functools import cache
from pathlib import Path

import numpy as np
import pytest

from evals.e0 import benchmark, runners, scorer
from evals.e0.structure import load_structure

REPO = Path(__file__).resolve().parents[1]
#: The committed e0-v1 full-profile scorecard (also GRID-E0-BENCHMARK-V1-20260930.scorecard.json,
#: sha256 4b47f918433524d4ab36b93e6472ee557e6617bbfe7567e5c182d4834a7cd62a).
SCORECARD = REPO / "docs" / "paper_log" / "vs1-v7-stop-e0-scorecard.json"

TARGET_IC = 0.01
SIMS = 60
PERMS = 199
SCENARIOS = ("factor_t_garch_exposed", "gaussian_idio")
REALISED_IC_TOLERANCE = 0.0015

MACHINERY_CHANGE = "a machinery change: re-run E0 and get owner sign-off; do not re-pin casually"

#: Exact hit counts out of SIMS worlds at PERMS flips (base seed 20260930, measured 2026-10-01 on
#: numpy 2.5.2 / scipy 1.18.1, Python 3.13, and checked on CI's Python 3.11). "holm" / "bh": the
#: planted primary trial A90|fwd5 selected with the planted sign at IC 0.01 (Holm at the run alpha,
#: BH at bh_q, over all four declared trials). "null_holm_primary": the primary trial selected by
#: Holm (any sign) on the IC 0 worlds.
PINNED_HITS = {
    "factor_t_garch_exposed": {"holm": 16, "bh": 25, "null_holm_primary": 1},
    "gaussian_idio": {"holm": 36, "bh": 45, "null_holm_primary": 2},
}


@cache
def _config() -> dict:
    return benchmark.load_config()


@cache
def _structure():
    return load_structure(_config())


def _selection(config: dict) -> runners.Selection:
    sel = config["selection"]
    return runners.Selection(ledger_q=sel["ledger_q"], run_k=sel["run_k"], bh_q=sel["bh_q"],
                             direction=sel["direction"])


@cache
def _run(name: str) -> dict:
    """One scenario at IC {0, 0.01}: calibration, raw records and the scorer's view (computed once)."""
    config = _config()
    structure = _structure()
    selection = _selection(config)
    planted = config["planted"]["planted_trials"]
    scenario = config["scenarios"][name]
    scales = runners.calibrate_scales(
        structure, name, scenario, [TARGET_IC], planted,
        pilot_sims=config["planted"]["pilot_sims"], pilot_scale=config["planted"]["pilot_scale"],
        pilot_seed=config["seeds"]["pilot"],
    )
    rows = runners.run_scenario(
        structure, name, scenario, ic_grid=[0.0, TARGET_IC], sims=SIMS, null_sims=SIMS, perms=PERMS,
        selection=selection, planted_trials=planted, scales=scales, base_seed=config["seeds"]["base"],
    )
    scored = scorer.score_scenario(rows, primary=structure.primary_trial, planted=planted,
                                   threshold=runners.raw_threshold(selection, len(structure.trials)),
                                   scales=scales, direction=selection.direction)
    return {"scales": scales, "rows": rows, "scored": scored}


def _published(name: str) -> dict:
    card = json.loads(SCORECARD.read_text(encoding="utf-8"))
    assert card["version"] == "e0-v1" and card["profile"]["name"] == "full"
    return card


def _published_row(name: str) -> dict:
    card = _published(name)
    return next(r for r in card["scenarios"][name]["power_curve"] if abs(r["target_ic"] - TARGET_IC) < 1e-12)


def _hits(name: str) -> dict:
    structure = _structure()
    primary = structure.primary_trial
    direction = _config()["selection"]["direction"]
    run = _run(name)
    planted = [sim["trials"][primary] for sim in run["rows"][f"{TARGET_IC:g}"]]
    null = [sim["trials"][primary] for sim in run["rows"]["0"]]

    def right_sign(rec):  # the scorer's rule: a detection must carry the planted sign
        return rec["mean_ic"] is not None and np.sign(rec["mean_ic"]) == direction

    return {
        "holm": sum(bool(r["holm"] and right_sign(r)) for r in planted),
        "bh": sum(bool(r["bh"] and right_sign(r)) for r in planted),
        "null_holm_primary": sum(bool(r["holm"]) for r in null),
        "planted_sims": len(planted),
        "null_sims": len(null),
    }


@pytest.mark.parametrize("name", SCENARIOS)
def test_realised_ic_recovers_target_on_fresh_worlds(name):
    config = _config()
    assert config["version"] == "e0-v1"
    # Fresh worlds: the scored worlds are seeded [base, scenario, sim], the pilot worlds
    # [pilot, scenario, sim]; distinct first seed words give disjoint seed sequences.
    assert config["seeds"]["base"] != config["seeds"]["pilot"]
    run = _run(name)
    primary = _structure().primary_trial
    # The calibration is the released one: same pilot worlds, same scale as the e0-v1 scorecard.
    released = _published(name)["planted_scale_calibration"][name][primary]["scales"][f"{TARGET_IC:g}"]
    assert run["scales"][primary]["scales"][f"{TARGET_IC:g}"] == pytest.approx(released, rel=1e-9), MACHINERY_CHANGE
    row = next(r for r in run["scored"]["power_curve"] if r["target_ic"] == TARGET_IC)
    assert row["sims"] == SIMS
    assert abs(row["realized_mean_ic"] - TARGET_IC) <= REALISED_IC_TOLERANCE, (
        f"{name}: realised mean rank IC {row['realized_mean_ic']:.5f} on {SIMS} fresh worlds is not "
        f"{TARGET_IC} +/- {REALISED_IC_TOLERANCE}: the plant calibration does not transfer off the pilot worlds"
    )


@pytest.mark.parametrize("name", SCENARIOS)
def test_ic01_power_is_pinned(name):
    hits = _hits(name)
    assert hits["planted_sims"] == SIMS and hits["null_sims"] == SIMS
    observed = {k: hits[k] for k in PINNED_HITS[name]}
    assert observed == PINNED_HITS[name], (
        f"{name}: E0 hit counts at IC {TARGET_IC} ({SIMS} sims, {PERMS} flips) moved from "
        f"{PINNED_HITS[name]} to {observed}. This is {MACHINERY_CHANGE}. The generator, "
        "analysis/panel_insider_density.measure_trial, the Holm/BH adapters or numpy RNG behaviour changed."
    )


@pytest.mark.parametrize("name", SCENARIOS)
def test_ic01_power_consistent_with_released_scorecard(name):
    published = _published_row(name)
    p_pub = published["power_holm_run_alpha"]["rate"]
    se_pub = published["power_holm_run_alpha"]["se"]
    assert published["sims"] == 500 and se_pub == pytest.approx(0.022, abs=0.001)
    expected = {"factor_t_garch_exposed": 0.414, "gaussian_idio": 0.496}[name]
    assert p_pub == pytest.approx(expected, abs=1e-12)
    rate = _hits(name)["holm"] / SIMS
    bound = 3 * math.sqrt(p_pub * (1 - p_pub) / SIMS) + 3 * se_pub
    assert abs(rate - p_pub) <= bound, (
        f"{name}: Holm power at IC {TARGET_IC} is {rate:.3f} over {SIMS} sims vs the released {p_pub:.3f} "
        f"(bound {bound:.3f}): E0 v1 no longer reproduces its own scorecard"
    )


@pytest.mark.parametrize("name", SCENARIOS)
def test_null_rejection_rate_at_ic0(name):
    alpha = _selection(_config()).holm_alpha
    assert alpha == pytest.approx(0.05)
    rate = _hits(name)["null_holm_primary"] / SIMS
    bound = alpha + 3 * math.sqrt(alpha * (1 - alpha) / SIMS)
    assert rate <= bound, f"{name}: primary-trial Holm rejection rate {rate:.3f} at IC 0 exceeds {bound:.3f}"
