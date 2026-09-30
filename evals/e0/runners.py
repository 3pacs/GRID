"""Planted-signal and null runners over a structure (synthetic outcomes only).

One simulation = one synthetic world on the structure's calendar, one full
declared trial family scored the way VS1 discovery scores it: every trial
through :func:`evals.e0.machinery.measure`, then Holm at the ledger run alpha
(VS1's selection) and BH at ``bh_q`` over all declared trials.

Common random numbers: simulation ``s`` of a scenario uses the same world for
every IC in the grid; the planted trial's label differs only by the planted
component. The IC = 0 row is the null runner (every trial an exact null).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Mapping, Sequence

import numpy as np

from evals.e0 import generator, machinery
from evals.e0.structure import Structure


def stable_code(name: str) -> int:
    """A platform-independent 32-bit integer for a name (seed material)."""
    return int(hashlib.sha256(name.encode("utf-8")).hexdigest()[:8], 16)


def world_rng(base_seed: int, scenario: str, sim: int) -> np.random.Generator:
    return np.random.default_rng([base_seed, stable_code(scenario), sim])


@dataclass(frozen=True)
class Selection:
    ledger_q: float
    run_k: int
    bh_q: float
    direction: int

    @property
    def holm_alpha(self) -> float:
        return machinery.run_alpha(self.run_k, self.ledger_q)


def raw_threshold(selection: Selection, n_trials: int) -> float:
    """v1 Stage-0 power threshold: the smallest Holm threshold of the family."""
    return selection.holm_alpha / n_trials


def calibrate_scales(
    structure: Structure,
    scenario_name: str,
    scenario: Mapping,
    targets: Sequence[float],
    planted_trials: Sequence[str],
    *,
    pilot_sims: int,
    pilot_scale: float,
    pilot_seed: int,
    iterations: int = 3,
) -> dict[str, dict]:
    """Per planted trial and target IC: the plant scale ``c`` with E[realized mean rank IC] = target.

    Seeded pilot worlds (disjoint from the scored worlds) are fixed; the
    planted increment ``g(c) = mean IC(label + c * plant) - mean IC(label)`` on
    those same worlds removes their common null noise. ``c`` starts from the
    secant at ``pilot_scale`` and is refined by ``iterations`` secant steps
    ``c <- c * target / g(c)``.
    """
    out = {}
    for trial in planted_trials:
        ts = structure.trials[trial]
        z = ts.standardized_ranks()
        worlds = []
        for s in range(pilot_sims):
            rng = world_rng(pilot_seed, scenario_name, s)
            world = generator.simulate_world(structure, scenario, rng)
            worlds.append(generator.base_label(world, ts, scenario, rng))

        def mean_ic(scale: float) -> float:
            values = [np.nanmean(machinery.rank_ic_series(ts.feature, generator.plant(lab, sd, z, scale)))
                      for lab, sd in worlds]
            return float(np.mean(values))

        null_ic = mean_ic(0.0)
        increment = mean_ic(pilot_scale) - null_ic
        if not increment > 0:
            raise ValueError(f"{scenario_name}/{trial}: pilot produced no positive IC increment")
        scales, achieved = {}, {}
        for target in targets:
            c = target * pilot_scale / increment
            g = increment * c / pilot_scale
            for _ in range(iterations):
                g = mean_ic(c) - null_ic
                if not g > 0:
                    break
                c = c * target / g
            scales[f"{target:g}"] = float(c)
            achieved[f"{target:g}"] = float(g)
        out[trial] = {
            "pilot_sims": pilot_sims,
            "pilot_scale": pilot_scale,
            "pilot_null_mean_ic": null_ic,
            "pilot_increment_at_pilot_scale": increment,
            "scales": scales,
            "pilot_increment_before_last_step": achieved,
        }
    return out


def family_decision(records: Mapping[str, dict], selection: Selection) -> dict[str, dict]:
    """Holm at the run alpha and BH at bh_q over every declared trial (untestable at p = 1)."""
    trials = list(records)
    pvalues = [records[t]["p"] if records[t]["status"] == "tested" else 1.0 for t in trials]
    holm = machinery.holm(pvalues)
    bh = machinery.bh(pvalues)
    out = {}
    for t, h, b in zip(trials, holm, bh):
        tested = records[t]["status"] == "tested"
        out[t] = {"holm": tested and h <= selection.holm_alpha, "bh": tested and b <= selection.bh_q}
    return out


def run_scenario(
    structure: Structure,
    scenario_name: str,
    scenario: Mapping,
    *,
    ic_grid: Sequence[float],
    sims: int,
    null_sims: int,
    perms: int,
    selection: Selection,
    planted_trials: Sequence[str],
    scales: Mapping[str, Mapping],
    base_seed: int,
) -> dict:
    """Raw per-simulation records for one scenario (scored by :mod:`evals.e0.scorer`)."""
    trials = list(structure.trials)
    z = {t: structure.trials[t].standardized_ranks() for t in planted_trials}
    rows: dict[str, list] = {f"{ic:g}": [] for ic in ic_grid}
    total = max(null_sims if 0.0 in ic_grid else 0, sims if any(ic != 0 for ic in ic_grid) else 0)
    for s in range(total):
        rng = world_rng(base_seed, scenario_name, s)
        world = generator.simulate_world(structure, scenario, rng)
        base = {t: generator.base_label(world, structure.trials[t], scenario, rng) for t in trials}
        null_records = {
            t: machinery.measure(structure.trials[t].feature, base[t][0], structure.trials[t].horizon, t, perms=perms)
            for t in trials
            if t not in planted_trials
        }
        for ic in ic_grid:
            if s >= (null_sims if ic == 0 else sims):
                continue
            records = dict(null_records)
            for t in planted_trials:
                ts = structure.trials[t]
                if ic == 0:
                    label = base[t][0]
                else:
                    label = generator.plant(base[t][0], base[t][1], z[t], scales[t]["scales"][f"{ic:g}"])
                records[t] = machinery.measure(ts.feature, label, ts.horizon, t, perms=perms)
            records = {t: records[t] for t in trials}
            decision = family_decision(records, selection)
            rows[f"{ic:g}"].append({
                "sim": s,
                "trials": {
                    t: {
                        "p": float(records[t]["p"]),
                        "mean_ic": records[t]["mean_ic"],
                        "status": records[t]["status"],
                        "n": records[t]["n"],
                        "block": records[t]["block"],
                        "holm": decision[t]["holm"],
                        "bh": decision[t]["bh"],
                    }
                    for t in trials
                },
            })
    return rows
