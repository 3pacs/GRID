"""E0 v2 (``evals/e0v2``): the calibrated-outcome-model benchmark, a sibling of the frozen e0-v1.

* v2 builds on the exact released e0-v1 (VS1 v8 verifies e0-v1 at run time; it must never change);
* the EVAL-E0C2 calibration receipt is the pinned one, used only pre-2007-11-01 data and the
  committed v1 replication panel; the calibrated scenarios are the receipt's fit (factor_ar1 = 0);
* v1's benchmark behaviour on the v2 config: byte-identical scorecard per seed, write-once, IC 0.05
  detection, FDR/FWER near nominal;
* IC 0.01 recovery on fresh worlds and a deterministic power pin (the E0-C1 pattern, v2 pins);
* the v2 replication never reads a date on or after 2007-11-01.

Synthetic outcomes only, except the pre-2007-11-01 non-Technology replication; no VS1 price,
return or IC; no DB.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import date
from functools import cache

import numpy as np
import pytest

from evals.e0 import manifest as v1_manifest
from evals.e0 import runners, scorer
from evals.e0.structure import load_structure
from evals.e0v2 import VERSION, benchmark, manifest, replication
from evals.e0v2.manifest import PACKAGE

E0_V1_MANIFEST = "75489d5091d82af64312f4523c41bdb52b0951a11e017644a022baa08a172822"
RECEIPT_SHA256 = "3b2b30cafc91c336e7707a51e333b2d416afefb1be02bafa88f5408ee5292914"
V1_PANEL_SHA256 = "d0b6968c46aa69c75c8c72be90fda53ff1bd9677009371875712a4ed5aa1a399"
CUTOFF = date(2007, 11, 1)
TARGET_IC = 0.01
MACHINERY_CHANGE = "a machinery change: re-run E0 v2 and get owner sign-off; do not re-pin casually"

#: ``ic01`` profile (60 sims, 199 flips, base seed 20260930), measured 2026-10-02 (numpy 2.5.2,
#: Python 3.13; checked on CI's Python 3.11). holm/bh: the planted primary trial selected with
#: the planted sign at IC 0.01; null_holm_primary: the primary selected by Holm on IC 0 worlds;
#: p_fingerprint: sum over both IC rows, every world and trial of p * (perms + 1).
PINNED_HITS = {
    "factor_t_garch_calibrated_exposed_030": {"holm": 25, "bh": 31, "null_holm_primary": 1, "p_fingerprint": 43445},
    "factor_t_garch_calibrated": {"holm": 28, "bh": 35, "null_holm_primary": 0, "p_fingerprint": 41886},
}
#: The IC 0.01 plant scale on the config's pilot worlds (24 pilot sims, pilot seed 20260931).
PINNED_SCALE = {"factor_t_garch_calibrated_exposed_030": 0.009526655551368822,
                "factor_t_garch_calibrated": 0.009562017582505833}


@cache
def _config() -> dict:
    return benchmark.load_config()


@cache
def _ci_card() -> dict:
    return benchmark.run("ci", replicate=False, log=lambda _m: None)


def _sha(path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ------------------------------------------------------------------ versioning and provenance

def test_v1_is_untouched_and_v2_builds_on_it():
    assert v1_manifest.verify()["manifest_sha256"] == E0_V1_MANIFEST
    assert manifest.verify_builds_on()["version"] == "e0-v1"
    assert _config()["builds_on"]["manifest_sha256"] == E0_V1_MANIFEST


def test_v2_manifest_verifies():
    out = manifest.verify()
    assert out["version"] == VERSION == _config()["version"] == "e0-v2"


def test_calibration_receipt_is_pinned_and_pre_cutoff():
    cfg = _config()["calibration"]
    path = PACKAGE / cfg["receipt"]
    assert _sha(path) == cfg["receipt_sha256"] == RECEIPT_SHA256
    receipt = json.loads(path.read_text(encoding="utf-8"))
    assert receipt["input"]["cutoff_exclusive"] == cfg["cutoff_exclusive"] == "2007-11-01"
    assert receipt["input"]["last_date"] < "2007-11-01"
    # the receipt's input is the committed v1 replication panel
    assert receipt["input"]["sha256"] == V1_PANEL_SHA256 == _config()["replication"]["file_sha256"]
    v1_panel = v1_manifest.PACKAGE / "data" / "replication_pre2011_nontech_grid5.npz"
    assert _sha(v1_panel) == V1_PANEL_SHA256


def test_calibrated_scenarios_are_the_receipt_fit():
    cfg = _config()
    fit = json.loads((PACKAGE / cfg["calibration"]["receipt"]).read_text(encoding="utf-8"))["fit"]["params"]
    v1 = json.loads((v1_manifest.PACKAGE / "config.json").read_text(encoding="utf-8"))["scenarios"]
    for name in ("gaussian_idio", "factor_t_garch", "factor_t_garch_exposed"):
        assert cfg["scenarios"][name] == v1[name]  # v1's scenarios carried unchanged
    exposures = {}
    for name in cfg["calibrated_scenarios"]:
        sc = cfg["scenarios"][name]
        exposures[name] = sc["exposure"]
        for key in ("market_vol", "beta_sd", "factor_vol", "idio_vol", "idio_vol_dispersion"):
            assert abs(sc[key] / fit[key] - 1) <= 0.02, (name, key)
        assert abs(sc["garch"]["alpha"] / fit["garch_alpha"] - 1) <= 0.02
        assert abs(sc["garch"]["beta"] / fit["garch_beta"] - 1) <= 0.02
        assert sc["tail_df"] == fit["tail_df"] == 3
        assert sc["factor_ar1"] == 0.0  # not identified (owner decision 2026-10-01)
        assert sc["label_missing_rate"] == 0.002 and sc["n_factors"] == 3 and sc["market"] is True
    assert sorted(exposures.values()) == [0.0, 0.15, 0.30, 0.45]
    assert cfg["headline_scenario"] in cfg["calibrated_scenarios"]


# ------------------------------------------------------------------ v1 benchmark behaviour on v2

def test_same_seed_gives_a_byte_identical_scorecard():
    first = benchmark.run("smoke", replicate=False, log=lambda _m: None)
    second = benchmark.run("smoke", replicate=False, log=lambda _m: None)
    dump = lambda card: json.dumps(card, indent=2, sort_keys=True, allow_nan=False)
    assert dump(first) == dump(second)
    assert first["version"] == "e0-v2" and first["builds_on"]["manifest_sha256"] == E0_V1_MANIFEST


def test_write_once(tmp_path):
    card = _ci_card()
    path = benchmark.write_scorecard(card, tmp_path)
    assert json.loads(path.read_text(encoding="utf-8"))["version"] == "e0-v2"
    with pytest.raises(FileExistsError):
        benchmark.write_scorecard(card, tmp_path)


def test_planted_ic_005_is_detected_with_high_power():
    card = _ci_card()
    scored = card["scenarios"][card["headline"]["scenario"]]
    row = next(r for r in scored["power_curve"] if r["target_ic"] == 0.05)
    assert row["power_holm_run_alpha"]["rate"] >= 0.9
    assert row["power_bh"]["rate"] >= 0.9
    assert abs(row["realized_mean_ic"] - 0.05) < 0.01


def test_null_fdr_and_fwer_near_nominal():
    card = _ci_card()
    scored = card["scenarios"][card["headline"]["scenario"]]
    null = scored["fdr"]["global_null"]
    assert null["fdr_bh"]["rate"] <= 0.10 + 0.11  # 48 null families: ~2.5 SE of slack
    assert null["fwer_holm_run_alpha"]["rate"] <= 0.05 + 0.08
    pooled = scored["null_calibration"]["pooled"]
    assert pooled["ks_stat"] <= 1.63 / np.sqrt(pooled["n"])  # 1% KS critical value
    assert card["checks"][card["headline"]["scenario"]]["fdr_bh_controlled"]


# ------------------------------------------------------------------ IC 0.01 (the E0-C1 pattern)

@cache
def _ic01(name: str) -> dict:
    config = _config()
    prof = benchmark._profile(config, "ic01")
    structure = load_structure(config)
    selection = benchmark._selection(config)
    planted = config["planted"]["planted_trials"]
    scenario = config["scenarios"][name]
    scales = benchmark.scale_for(structure, config, name, scenario, [TARGET_IC], config["planted"]["pilot_sims"])
    rows = runners.run_scenario(
        structure, name, scenario, ic_grid=prof["ic_grid"], sims=prof["sims"], null_sims=prof["null_sims"],
        perms=prof["perms"], selection=selection, planted_trials=planted, scales=scales,
        base_seed=config["seeds"]["base"])
    scored = scorer.score_scenario(rows, primary=structure.primary_trial, planted=planted,
                                   threshold=runners.raw_threshold(selection, len(structure.trials)),
                                   scales=scales, direction=selection.direction)
    primary = structure.primary_trial
    planted_recs = [s["trials"][primary] for s in rows[f"{TARGET_IC:g}"]]
    null_recs = [s["trials"][primary] for s in rows["0"]]

    def right_sign(rec):  # the scorer's rule: a detection must carry the planted sign
        return rec["mean_ic"] is not None and np.sign(rec["mean_ic"]) == selection.direction

    hits = {
        "holm": sum(bool(r["holm"] and right_sign(r)) for r in planted_recs),
        "bh": sum(bool(r["bh"] and right_sign(r)) for r in planted_recs),
        "null_holm_primary": sum(bool(r["holm"]) for r in null_recs),
        "p_fingerprint": sum(round(rec["p"] * (prof["perms"] + 1)) for key in ("0", f"{TARGET_IC:g}")
                             for s in rows[key] for rec in s["trials"].values()),
    }
    return {"prof": prof, "scales": scales, "scored": scored, "hits": hits, "primary": primary,
            "n_planted": len(planted_recs), "n_null": len(null_recs)}


@pytest.mark.parametrize("name", sorted(PINNED_HITS))
def test_ic01_realised_ic_recovers_target_on_fresh_worlds(name):
    config = _config()
    assert config["seeds"]["base"] != config["seeds"]["pilot"]  # scored worlds are not the pilot worlds
    run = _ic01(name)
    scale = run["scales"][run["primary"]]["scales"][f"{TARGET_IC:g}"]
    assert scale == pytest.approx(PINNED_SCALE[name], rel=1e-9), MACHINERY_CHANGE
    row = next(r for r in run["scored"]["power_curve"] if r["target_ic"] == TARGET_IC)
    assert row["sims"] == run["prof"]["sims"] == 60
    assert abs(row["realized_mean_ic"] - TARGET_IC) <= 0.0015, (
        f"{name}: realised mean rank IC {row['realized_mean_ic']:.5f} is not {TARGET_IC} +/- 0.0015")


@pytest.mark.parametrize("name", sorted(PINNED_HITS))
def test_ic01_power_is_pinned(name):
    run = _ic01(name)
    assert run["n_planted"] == run["n_null"] == 60
    row = next(r for r in run["scored"]["power_curve"] if r["target_ic"] == TARGET_IC)
    assert run["hits"]["holm"] == round(row["power_holm_run_alpha"]["rate"] * 60)
    assert run["hits"]["bh"] == round(row["power_bh"]["rate"] * 60)
    assert run["hits"] == PINNED_HITS[name], (
        f"{name}: E0 v2 IC {TARGET_IC} counts moved from {PINNED_HITS[name]} to {run['hits']}. This is "
        f"{MACHINERY_CHANGE}. The generator, the VS1 statistic, the Holm/BH adapters or numpy RNG changed.")


@pytest.mark.parametrize("name", sorted(PINNED_HITS))
def test_ic01_null_rejection_rate(name):
    rate = _ic01(name)["hits"]["null_holm_primary"] / 60
    assert rate <= 0.05 + 3 * math.sqrt(0.05 * 0.95 / 60)


# ------------------------------------------------------------------ replication window

def test_replication_panel_is_cut_before_2007_11_01_and_outside_vs1():
    panel = replication.load_prices(_config())
    assert panel.dates[-1] < CUTOFF and panel.dates[-1] == date(2007, 10, 26)
    assert len(panel.dates) == 727 and panel.closes.shape == (727, len(panel.tickers))
    from evals.e0 import replication as v1_replication

    assert not set(panel.tickers) & v1_replication.load_denylist(_config())


def test_replication_refuses_a_changed_panel():
    cfg = json.loads(json.dumps(_config()))
    cfg["replication"]["file_sha256"] = "0" * 64
    with pytest.raises(PermissionError, match="changed"):
        replication.load_prices(cfg)


def test_replication_refuses_a_late_cutoff_before_opening_the_panel(monkeypatch):
    def refuse(*_a, **_k):
        raise AssertionError("the replication panel was opened")

    monkeypatch.setattr(replication.np, "load", refuse)
    cfg = json.loads(json.dumps(_config()))
    cfg["replication"]["cutoff_exclusive"] = "2007-11-02"
    with pytest.raises(PermissionError, match="after 2007-11-01"):
        replication.load_prices(cfg)
    assert replication.MAX_CUTOFF == CUTOFF and _config()["replication"]["cutoff_exclusive"] == "2007-11-01"


def test_v8_crosscheck_section_is_information_only():
    section = benchmark._crosscheck_section(_config(), {
        "factor_t_garch_exposed": {"power": 0.73}, "factor_t_garch": {"power": 0.78},
        "factor_t_garch_calibrated_exposed_030": {"power": 0.49}})
    assert section["status"].startswith("information, not a gate")
    assert section["reproduction_of_v8_registered_v1_rows"]["factor_t_garch_exposed"]["matches"] is True
    assert section["headline_below_gate_on_414_date_proxy"] is True


def test_v8_rule_power_is_v8s_e0_power():
    """The cross-check loop is bit-identical to VS1 v8's own e0_power on the committed geometry."""
    from analysis import panel_insider_density_v8 as v8

    config = _config()
    structure = load_structure(config)
    name = "factor_t_garch_exposed"
    features = {t: ts.feature for t, ts in structure.trials.items()}
    theirs = v8.e0_power(features, structure.n_sessions, name, sims=6, perms=99)
    scale = benchmark.scale_for(structure, config, name, config["scenarios"][name], [TARGET_IC],
                                config["planted"]["pilot_sims"])[structure.primary_trial]["scales"]["0.01"]
    ours = benchmark.v8_rule_power(structure, name, config["scenarios"][name], scale=scale, sims=6, perms=99,
                                   base_seed=config["seeds"]["base"], alpha=v8.CONFIRMATORY_ALPHA_ONE_SIDED)
    assert ours["plant_scale"] == theirs["plant_scale"]
    assert ours["power"] == theirs["power"] and ours["realized_mean_ic"] == theirs["realized_mean_ic"]
    assert ours["usable_dates"] == theirs["usable_dates"] == 414
