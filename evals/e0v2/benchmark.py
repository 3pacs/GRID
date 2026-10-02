"""Assemble the E0 v2 scorecard (v1's runners/scorer on the v2 config).

The planted and null runners, scorer, structure, machinery adapter, seeds and
selection are e0-v1's, imported unchanged; only the config (scenarios,
profiles, headline) differs. Two additions:

* ``vs1_design_crosscheck`` -- the VS1 v8 confirmatory rule (A90|fwd5 alone,
  one-sided positive block sign-flip p <= 0.05; ``analysis.panel_insider_density_v8.e0_power``)
  on the committed v7 geometry, which is v8's registered 2011-10-01 screen row.
  v1's two realistic scenarios are re-run as reproduction controls (registered
  0.780 / 0.730). INFORMATION ONLY: v8's binding Stage-0 is the one its body names;
* ``replication`` -- v1's replication restricted to dates before 2007-11-01.
"""

from __future__ import annotations

import json
import math
import time
from collections.abc import Mapping
from pathlib import Path

import numpy as np

from evals.e0 import generator, machinery, runners, scorer
from evals.e0.benchmark import _round, _within
from evals.e0.benchmark import write_scorecard as _write_scorecard
from evals.e0.structure import Structure, load_structure
from evals.e0v2 import VERSION, manifest, replication
from evals.e0v2.manifest import PACKAGE


def load_config() -> dict:
    return json.loads((PACKAGE / "config.json").read_text(encoding="utf-8"))


def _profile(config: Mapping, name: str) -> dict:
    prof = dict(config["profiles"][name])
    prof.setdefault("ic_grid", config["planted"]["ic_grid"])
    prof.setdefault("null_sims", prof["sims"])
    return prof


def _selection(config: Mapping) -> runners.Selection:
    sel = config["selection"]
    return runners.Selection(ledger_q=sel["ledger_q"], run_k=sel["run_k"], bh_q=sel["bh_q"], direction=sel["direction"])


def calibration_check(config: Mapping) -> dict:
    """The committed EVAL-E0C2 receipt is the pinned one (sha256 and cutoff)."""
    spec = config["calibration"]
    path = PACKAGE / spec["receipt"]
    sha = manifest.file_sha256(path)
    if sha != spec["receipt_sha256"]:
        raise manifest.ManifestError(f"calibration receipt sha256 {sha} != pinned {spec['receipt_sha256']}")
    receipt = json.loads(path.read_text(encoding="utf-8"))
    if receipt["input"]["cutoff_exclusive"] != spec["cutoff_exclusive"]:
        raise manifest.ManifestError("calibration receipt cutoff differs from the config")
    return {"receipt_sha256": sha, "cutoff_exclusive": spec["cutoff_exclusive"],
            "fitted": receipt["fit"]["params"], "input_npz_sha256": receipt["input"]["sha256"]}


def scale_for(structure: Structure, config: Mapping, name: str, scenario: Mapping, targets, pilot_sims: int) -> dict:
    return runners.calibrate_scales(
        structure, name, scenario, targets, config["planted"]["planted_trials"],
        pilot_sims=pilot_sims, pilot_scale=config["planted"]["pilot_scale"], pilot_seed=config["seeds"]["pilot"],
    )


def v8_rule_power(structure: Structure, name: str, scenario: Mapping, *, scale: float, sims: int, perms: int,
                  base_seed: int, alpha: float) -> dict:
    """``panel_insider_density_v8.e0_power``'s loop on this structure: one-sided positive p <= alpha."""
    primary = structure.primary_trial
    ts = structure.trials[primary]
    z = ts.standardized_ranks()
    hits, usable, realized = 0, [], []
    for s in range(sims):
        rng = runners.world_rng(base_seed, name, s)
        world = generator.simulate_world(structure, scenario, rng)
        base = {t: generator.base_label(world, structure.trials[t], scenario, rng) for t in structure.trials}
        label = generator.plant(base[primary][0], base[primary][1], z, scale)
        record = machinery.measure(ts.feature, label, ts.horizon, primary, perms=perms)
        if record["status"] == "tested":
            hits += record["p_one_sided_positive"] <= alpha
            realized.append(record["mean_ic"])
        usable.append(record["n"])
    se = math.sqrt(hits / sims * (1 - hits / sims) / sims) if sims else None
    return {"power": hits / sims, "se": se, "sims": sims, "perms": perms, "plant_scale": float(scale),
            "usable_dates": int(np.median(usable)), "realized_mean_ic": float(np.mean(realized)) if realized else None}


def _scenario_job(args: tuple) -> tuple[str, dict, dict]:
    name, profile_name = args
    config = load_config()
    prof = _profile(config, profile_name)
    structure = load_structure(config)
    selection = _selection(config)
    threshold = runners.raw_threshold(selection, len(structure.trials))
    planted = config["planted"]["planted_trials"]
    targets = [ic for ic in prof["ic_grid"] if ic != 0]
    scenario = config["scenarios"][name]
    scales = scale_for(structure, config, name, scenario, targets, prof.get("pilot_sims", config["planted"]["pilot_sims"]))
    rows = runners.run_scenario(
        structure, name, scenario, ic_grid=prof["ic_grid"], sims=prof["sims"], null_sims=prof["null_sims"],
        perms=prof["perms"], selection=selection, planted_trials=planted, scales=scales,
        base_seed=config["seeds"]["base"],
    )
    scored = scorer.score_scenario(rows, primary=structure.primary_trial, planted=planted, threshold=threshold,
                                   scales=scales, direction=selection.direction)
    scored["note"] = scenario.get("note")
    return name, scored, scales


def _crosscheck_job(args: tuple) -> tuple[str, dict]:
    name, sims = args
    config = load_config()
    spec = config["vs1_design_crosscheck"]
    structure = load_structure(config)
    scenario = config["scenarios"][name]
    ic = float(spec["target_ic"])
    scale = scale_for(structure, config, name, scenario, [ic], config["planted"]["pilot_sims"])
    scale = scale[structure.primary_trial]["scales"][f"{ic:g}"]
    return name, v8_rule_power(structure, name, scenario, scale=scale, sims=sims, perms=int(spec["perms"]),
                               base_seed=config["seeds"]["base"], alpha=float(spec["alpha_one_sided"]))


def _pmap(fn, items: list, jobs: int) -> list:
    if jobs > 1 and len(items) > 1:
        from concurrent.futures import ProcessPoolExecutor

        with ProcessPoolExecutor(max_workers=min(jobs, len(items))) as pool:
            return list(pool.map(fn, items))
    return [fn(x) for x in items]


def run(profile: str = "full", *, replicate: bool = True, crosscheck: bool | None = None, jobs: int = 1,
        scenarios: list[str] | None = None, log=print) -> dict:
    builds_on = manifest.verify_builds_on()
    verified = manifest.verify()
    config = load_config()
    if config["version"] != VERSION or verified["version"] != VERSION:
        raise manifest.ManifestError("config, manifest and package versions differ")
    if config["builds_on"]["manifest_sha256"] != builds_on["manifest_sha256"]:
        raise manifest.ManifestError("config builds_on differs from the verified e0-v1 manifest")
    calibration = calibration_check(config)
    prof = _profile(config, profile)
    if scenarios:
        prof["scenarios"] = list(scenarios)
    if crosscheck is None:
        crosscheck = profile == "full"
    structure = load_structure(config)
    selection = _selection(config)
    threshold = runners.raw_threshold(selection, len(structure.trials))
    started = time.time()
    jobs_args: list = [("scenario", (name, profile)) for name in prof["scenarios"]]
    if crosscheck:
        spec = config["vs1_design_crosscheck"]
        jobs_args += [("crosscheck", (name, int(spec["sims"]))) for name in spec["scenarios"]]
    results = _pmap(_dispatch, jobs_args, jobs)
    scored, scale_cal, cross = {}, {}, {}
    for kind, res in results:
        if kind == "scenario":
            name, sc, sca = res
            scored[name], scale_cal[name] = sc, sca
        else:
            cross[res[0]] = res[1]
    log(f"[e0v2] {len(results)} job(s) done ({time.time() - started:.0f}s)")

    headline_name = config["headline_scenario"] if config["headline_scenario"] in scored else next(iter(scored))
    head = scored[headline_name]
    card = {
        "benchmark": config["benchmark"],
        "version": VERSION,
        "manifest": verified,
        "builds_on": builds_on,
        "calibration": calibration,
        "machinery_sha256": machinery.machinery_fingerprint(),
        "profile": {"name": profile, **prof},
        "structure": {
            "name": structure.name,
            "sessions": structure.n_sessions,
            "entities": structure.n_entities,
            "trials": {t: {"horizon": ts.horizon, "decisions": int(ts.feature.shape[0])}
                       for t, ts in structure.trials.items()},
            "primary_trial": structure.primary_trial,
        },
        "selection": {"holm_run_alpha": selection.holm_alpha, "bh_q": selection.bh_q,
                      "raw_threshold": threshold, "direction": selection.direction},
        "planted_scale_calibration": scale_cal,
        "scenarios": scored,
        "headline": {
            "scenario": headline_name,
            "headline_scenario_configured": config["headline_scenario"],
            "power_ic_0.01_holm": scorer.power_at(head, 0.01, "power_holm_run_alpha"),
            "power_ic_0.02_holm": scorer.power_at(head, 0.02, "power_holm_run_alpha"),
            "power_ic_0.01_raw_threshold": scorer.power_at(head, 0.01, "power_raw_threshold"),
            "power_ic_0.02_raw_threshold": scorer.power_at(head, 0.02, "power_raw_threshold"),
            "power_ic_0.01_bh": scorer.power_at(head, 0.01, "power_bh"),
            "power_ic_0.02_bh": scorer.power_at(head, 0.02, "power_bh"),
            "empirical_fdr_bh_all": head["fdr"]["all_simulations"]["fdr_bh"]["rate"],
            "empirical_fdr_bh_global_null": head["fdr"]["global_null"]["fdr_bh"]["rate"],
            "fwer_holm_global_null": head["fdr"]["global_null"]["fwer_holm_run_alpha"]["rate"],
            "declared_fdr": selection.bh_q,
            "declared_fwer": selection.holm_alpha,
            "null_p_ks_stat": head["null_calibration"]["pooled"]["ks_stat"],
            "null_p_ks_p": head["null_calibration"]["pooled"]["ks_p"],
            "null_size_at_0.05": head["null_calibration"]["pooled"]["size_at_0.05"]["rate"],
        },
        "statement": ("Synthetic outcomes except the pre-2007-11-01 non-Technology replication; nothing here is a "
                      "trading signal."),
    }
    checks = {}
    for name, sc in scored.items():
        g = sc["fdr"]["global_null"]
        allf = sc["fdr"]["all_simulations"]
        ks = sc["null_calibration"]["pooled"]
        checks[name] = {
            "fdr_bh_controlled": _within(allf["fdr_bh"], selection.bh_q),
            "fwer_holm_controlled": _within(g["fwer_holm_run_alpha"], selection.holm_alpha),
            "null_p_uniform_ks": bool(ks["ks_stat"] is not None and ks["ks_stat"] <= ks["critical_5pct"]),
        }
    card["checks"] = checks
    card["vs1_design_crosscheck"] = _crosscheck_section(config, cross) if crosscheck else {"status": "skipped"}
    if replicate and profile != "smoke":
        card["replication"] = replication.run_replication(config)
    else:
        card["replication"] = {"status": "skipped"}
    return _round(card)


def _dispatch(item: tuple) -> tuple:
    kind, args = item
    return kind, (_scenario_job(args) if kind == "scenario" else _crosscheck_job(args))


def _crosscheck_section(config: Mapping, cross: Mapping[str, dict]) -> dict:
    spec = config["vs1_design_crosscheck"]
    rows = dict(cross)
    repro = {}
    for name, registered in spec["reproduce_v1"].items():
        got = rows.get(name, {}).get("power")
        repro[name] = {"registered": registered, "e0v2_rerun": got,
                       "matches": None if got is None else abs(got - registered) < 1e-9}
    head = config["headline_scenario"]
    head_power = rows.get(head, {}).get("power")
    return {
        "status": spec["status"],
        "rule": spec["rule"],
        "geometry": spec["geometry"],
        "gate": spec["gate"],
        "design_target": spec["design_target"],
        "v8_registered_2011_10_01": spec["v8_registered_2011_10_01"],
        "v8_registered_2008_01_01": spec["v8_registered_2008_01_01"],
        "by_scenario": rows,
        "reproduction_of_v8_registered_v1_rows": repro,
        "headline_scenario": head,
        "headline_power": head_power,
        # On the 414-date v7 geometry: a conservative proxy for v8's selected 603-date 2008 design
        # (under every v1 model the 603-date geometry has the higher power).
        "headline_below_gate_on_414_date_proxy": None if head_power is None else head_power < spec["gate"],
        "source": spec["source"],
    }


def write_scorecard(card: Mapping, out_dir: Path) -> Path:
    return _write_scorecard(card, out_dir)
