"""Assemble the E0 scorecard: planted + null runners, v7 cross-check, replication."""

from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Mapping

from evals.e0 import VERSION, machinery, manifest, replication, runners, scorer
from evals.e0.structure import PACKAGE, load_structure


def load_config() -> dict:
    return json.loads((PACKAGE / "config.json").read_text(encoding="utf-8"))


def _profile(config: Mapping, name: str) -> dict:
    prof = dict(config["profiles"][name])
    prof.setdefault("ic_grid", config["planted"]["ic_grid"])
    prof.setdefault("null_sims", prof["sims"])
    return prof


def _round(value):
    """Floats rounded to 12 significant digits: stable JSON across re-runs."""
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return float(f"{value:.12g}")
    if isinstance(value, dict):
        return {k: _round(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_round(v) for v in value]
    return value


def crosscheck_v7(scored: Mapping[str, Mapping], config: Mapping, headline: str) -> dict:
    spec = config["v7_crosscheck"]
    ic = float(spec["target_ic"])
    registered = float(spec["registered_design_power"])
    reg_se = math.sqrt(registered * (1 - registered) / spec["registered_design_sims"])
    rows = {}
    for name, sc in scored.items():
        row = next((r for r in sc["power_curve"] if abs(r["target_ic"] - ic) < 1e-12), None)
        if row is None:
            continue
        raw = row["power_raw_threshold"]
        se = math.sqrt((raw["se"] or 0.0) ** 2 + reg_se ** 2)
        diff = raw["rate"] - registered
        rows[name] = {
            "e0_power_raw_threshold": raw["rate"],
            "e0_se": raw["se"],
            "e0_power_holm_run_alpha": row["power_holm_run_alpha"]["rate"],
            "e0_power_bh": row["power_bh"]["rate"],
            "sims": row["sims"],
            "difference_vs_registered": diff,
            "z_difference": diff / se if se > 0 else None,
            "disagrees_2se": bool(se > 0 and abs(diff) > 2 * se),
            "passes_v7_gate": raw["rate"] >= spec["gate"],
        }
    head = rows.get(headline)
    flag = None
    if head is not None:
        if head["disagrees_2se"] or head["passes_v7_gate"] != (registered >= spec["gate"]):
            flag = ("DISAGREES: under the headline (realistic) outcome model E0 power at IC "
                    f"{ic} is {head['e0_power_raw_threshold']:.3f} vs the registered design estimate "
                    f"{registered:.3f}")
        else:
            flag = "AGREES within 2 combined standard errors and on the gate side"
    return {
        "registered_design_power": registered,
        "registered_design_se": reg_se,
        "target_ic": ic,
        "threshold": spec["threshold"],
        "gate": spec["gate"],
        "design": spec["source"],
        "by_scenario": rows,
        "headline_scenario": headline,
        "flag": flag,
        "synthetic_outcomes_only": True,
    }


def _scenario_job(args: tuple[str, str]) -> tuple[str, dict, dict]:
    """One scenario end to end (a pure function of the pinned config: safe in a worker process)."""
    name, profile = args
    config = load_config()
    prof = _profile(config, profile)
    structure = load_structure(config)
    sel = config["selection"]
    selection = runners.Selection(ledger_q=sel["ledger_q"], run_k=sel["run_k"], bh_q=sel["bh_q"],
                                  direction=sel["direction"])
    threshold = runners.raw_threshold(selection, len(structure.trials))
    planted = config["planted"]["planted_trials"]
    targets = [ic for ic in prof["ic_grid"] if ic != 0]
    scenario = config["scenarios"][name]
    scales = runners.calibrate_scales(
        structure, name, scenario, targets, planted,
        pilot_sims=prof.get("pilot_sims", config["planted"]["pilot_sims"]), pilot_scale=config["planted"]["pilot_scale"],
        pilot_seed=config["seeds"]["pilot"],
    )
    rows = runners.run_scenario(
        structure, name, scenario, ic_grid=prof["ic_grid"], sims=prof["sims"], null_sims=prof["null_sims"],
        perms=prof["perms"], selection=selection, planted_trials=planted, scales=scales,
        base_seed=config["seeds"]["base"],
    )
    scored = scorer.score_scenario(rows, primary=structure.primary_trial, planted=planted,
                                   threshold=threshold, scales=scales, direction=selection.direction)
    scored["note"] = scenario.get("note")
    return name, scored, scales


def run(profile: str = "full", *, replicate: bool = True, jobs: int = 1, log=print) -> dict:
    verified = manifest.verify()
    config = load_config()
    if config["version"] != VERSION or verified["version"] != VERSION:
        raise manifest.ManifestError("config, manifest and package versions differ")
    prof = _profile(config, profile)
    structure = load_structure(config)
    sel = config["selection"]
    selection = runners.Selection(ledger_q=sel["ledger_q"], run_k=sel["run_k"], bh_q=sel["bh_q"],
                                  direction=sel["direction"])
    threshold = runners.raw_threshold(selection, len(structure.trials))
    jobs_args = [(name, profile) for name in prof["scenarios"]]
    started = time.time()
    if jobs > 1 and len(jobs_args) > 1:
        from concurrent.futures import ProcessPoolExecutor

        with ProcessPoolExecutor(max_workers=min(jobs, len(jobs_args))) as pool:
            results = list(pool.map(_scenario_job, jobs_args))
    else:
        results = [_scenario_job(a) for a in jobs_args]
    scored = {name: sc for name, sc, _ in results}
    calibration = {name: cal for name, _, cal in results}
    log(f"[e0] {len(results)} scenario(s) done ({time.time() - started:.0f}s)")

    headline_name = config["headline_scenario"] if config["headline_scenario"] in scored else next(iter(scored))
    head = scored[headline_name]
    card = {
        "benchmark": config["benchmark"],
        "version": VERSION,
        "manifest": verified,
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
        "planted_scale_calibration": calibration,
        "scenarios": scored,
        "headline": {
            "scenario": headline_name,
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
        "v7_crosscheck": crosscheck_v7(scored, config, headline_name),
        "statement": "Synthetic outcomes except the pre-2011 non-Technology replication; nothing here is a trading signal.",
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
    if replicate and profile != "smoke":
        try:
            card["replication"] = replication.run_replication(config)
        except FileNotFoundError:
            card["replication"] = {"status": "not_run", "reason": "cached pre-2011 extract missing"}
    else:
        card["replication"] = {"status": "skipped"}
    return _round(card)


def _within(rate: Mapping, nominal: float) -> bool:
    """Rate <= nominal, allowing two standard errors of Monte Carlo noise."""
    if rate["rate"] is None:
        return False
    return rate["rate"] <= nominal + 2 * (rate["se"] or 0.0)


def write_scorecard(card: Mapping, out_dir: Path) -> Path:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "scorecard.json"
    if path.exists():
        raise FileExistsError(f"{path} exists: scorecards are write-once")
    path.write_text(json.dumps(card, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8",
                    newline="\n")
    return path
