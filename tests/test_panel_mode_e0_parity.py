"""E0 parity (GD6 acceptance 4.1, 4.2): panel mode's statistic is the one E0 v1 grades.

On ``evals/e0/data/vs1_v7_technology_structure.npz`` with E0 synthetic outcomes
(``evals.e0.generator``, three seeds, all four trials), panel mode's per-trial
record equals ``evals.e0.machinery.measure`` byte for byte, and the machinery
fingerprint is the one the E0 v1 scorecard recorded (``machinery_sha256``),
under the pinned e0-v1 manifest. Synthetic outcomes only; no price is read.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from analysis import offline_research_proof as orp
from analysis import panel_mode as pm
from evals.e0 import generator
from evals.e0 import machinery as e0m
from evals.e0 import manifest as e0_manifest
from evals.e0.benchmark import load_config
from evals.e0.runners import world_rng
from evals.e0.structure import load_structure
from tests.panel_mode_support import construct, dumps, run_spec

REPO = Path(__file__).resolve().parents[1]
E0_V1_MANIFEST_SHA256 = "75489d5091d82af64312f4523c41bdb52b0951a11e017644a022baa08a172822"
E0_V1_SCORECARD = REPO / "docs" / "paper_log" / "vs1-v7-stop-e0-scorecard.json"
SCENARIO = "factor_t_garch_exposed"
PERMS = 199  # the E0 ci profile


@pytest.fixture(scope="module")
def e0():
    config = load_config()
    return config, load_structure(config)


def _world_labels(config, structure, sim: int, scenario: str = SCENARIO) -> dict:
    spec = config["scenarios"][scenario]
    rng = world_rng(config["seeds"]["base"], scenario, sim)
    world = generator.simulate_world(structure, spec, rng)
    return {t: generator.base_label(world, structure.trials[t], spec, rng)[0] for t in structure.trials}


def test_machinery_and_manifest_unchanged():
    assert e0_manifest.verify()["manifest_sha256"] == E0_V1_MANIFEST_SHA256
    card = json.loads(E0_V1_SCORECARD.read_text(encoding="utf-8"))
    assert card["manifest"]["manifest_sha256"] == E0_V1_MANIFEST_SHA256
    assert e0m.machinery_fingerprint() == card["machinery_sha256"]


@pytest.mark.parametrize("sim", [0, 1, 2])
def test_per_trial_record_byte_identical_to_e0(e0, sim):
    config, structure = e0
    labels = _world_labels(config, structure, sim)
    for trial, ts in structure.trials.items():
        old = e0m.measure(ts.feature, labels[trial], ts.horizon, trial, perms=PERMS)
        new = pm.e0_measure(ts.feature, labels[trial], ts.horizon, trial, perms=PERMS)
        for k in old:
            assert dumps(old[k]) == dumps(new[k]), (sim, trial, k)
        for k in ("mean_ic", "p", "p_one_sided_positive", "status", "block"):
            assert k in old


def _e0_constructs():
    return (construct("A90", 90, confirmatory=(5,), channels=("synthetic",)),
            construct("A30", 30, confirmatory=(), channels=("synthetic",)))


def _e0_run(seed_tag: str):
    return run_spec(run_id=f"e0-{seed_tag}", discovery_start="2101-01-01T00:00:00+00:00",
                    split="2200-01-01T00:00:00+00:00", end="2201-01-01T00:00:00+00:00",
                    perms=PERMS, prereg=orp.digest(["e0-synthetic", seed_tag]))


_SKELETON: dict = {}


def _e0_panels(structure, labels) -> dict:
    """The structure's panels on the synthetic 2101+ calendar, with this world's labels."""
    if structure.name not in _SKELETON:
        _SKELETON[structure.name] = {t: pm.synthetic_decisions(ts.feature.shape[0], ts.horizon)
                                     for t, ts in structure.trials.items()}
    panels = {}
    for trial, ts in structure.trials.items():
        decided, ends = _SKELETON[structure.name][trial]
        panels[trial] = pm.TrialPanel(trial=trial, window="discovery", horizon=ts.horizon, decision_at=decided,
                                      label_end=ends, entities=[f"E{j}" for j in range(ts.feature.shape[1])],
                                      feature=ts.feature, label=labels[trial])
    return {"Technology": panels}


def test_discover_panel_on_e0_world_matches_e0_family_decision(e0):
    """discover_panel's ledger = E0's measure + Holm/BH over the same four declared trials."""
    from evals.e0.runners import Selection, family_decision

    config, structure = e0
    labels = _world_labels(config, structure, 0)
    run = _e0_run("w0")
    guard = pm.OutcomeWindowGuard()  # synthetic calendar (2101+): outside every quarantine window
    frozen = pm.discover_panel(_e0_constructs(), run, _e0_panels(structure, labels), inputs={"e0": "w0"},
                               guard=guard, sensitivity=False)
    ledger = {t["trial"]: t for t in frozen["payload"]["ledger"]}
    records = {t: e0m.measure(structure.trials[t].feature, labels[t], structure.trials[t].horizon, t, perms=PERMS)
               for t in ("A90|fwd5", "A90|fwd20", "A30|fwd5", "A30|fwd20")}
    sel = config["selection"]
    decision = family_decision(records, Selection(ledger_q=sel["ledger_q"], run_k=sel["run_k"], bh_q=sel["bh_q"],
                                                  direction=sel["direction"]))
    for t, rec in records.items():
        for k in ("mean_ic", "p", "p_one_sided_positive", "status", "block", "n"):
            assert dumps(rec[k]) == dumps(ledger[t][k]), (t, k)
        assert ledger[t]["selected"] == decision[t]["holm"]
        assert (ledger[t]["bh_adjusted_p"] <= sel["bh_q"] and ledger[t]["status"] == "tested") == decision[t]["bh"]
    assert frozen["payload"]["promotion_allowed"] is False


NULL_WORLDS = 50
NULL_CHUNKS = 5
_NULL_P: dict = {}


@pytest.mark.parametrize("chunk", range(NULL_CHUNKS))
def test_null_calibration_smoke(e0, chunk):
    """Fifty E0 null worlds through discover_panel: realized size at 0.05 within E0 v1's published band.

    E0 stays the authority; this only shows the panel-mode path is not miscalibrated. The worlds run
    in five chunks (CI's per-test timeout); the last chunk checks the pooled rate against the pooled
    null size E0 v1 published for its headline scenario, +/- 2 binomial standard errors of this
    smoke's sample, and against E0's own pass rule (rate <= 0.05 + 2 se).
    """
    config, structure = e0
    guard = pm.OutcomeWindowGuard()
    per = NULL_WORLDS // NULL_CHUNKS
    for sim in range(chunk * per, (chunk + 1) * per):
        if sim in _NULL_P:
            continue
        labels = _world_labels(config, structure, sim)
        frozen = pm.discover_panel(_e0_constructs(), _e0_run(f"null{sim}"), _e0_panels(structure, labels),
                                   inputs={"e0": f"null{sim}"}, guard=guard, sensitivity=False)
        _NULL_P[sim] = [t["p"] for t in frozen["payload"]["ledger"] if t["status"] == "tested"]
    if chunk < NULL_CHUNKS - 1:
        return
    missing = [s for s in range(NULL_WORLDS) if s not in _NULL_P]
    if missing:
        pytest.skip(f"run the whole module for the pooled check ({len(missing)} worlds not run here)")
    card = json.loads(E0_V1_SCORECARD.read_text(encoding="utf-8"))
    published = card["headline"]["null_size_at_0.05"]
    ps = [p for s in range(NULL_WORLDS) for p in _NULL_P[s]]
    rate = float(np.mean([p <= 0.05 for p in ps]))
    se = (0.05 * 0.95 / len(ps)) ** 0.5
    assert len(ps) == 4 * NULL_WORLDS
    assert abs(rate - published) <= 2 * se + 1e-12, (rate, published)
    assert rate <= 0.05 + 2 * se
