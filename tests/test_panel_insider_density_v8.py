"""Synthetic VS1 v8 design, window, selection, Stage-0 and custody gates; no provider, price or DB access."""

from __future__ import annotations

import json
import shutil
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
import pytest

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v6 as v6
from analysis import panel_insider_density_v7 as v7
from analysis import panel_insider_density_v8 as v8
from analysis import price_admission_fetch as fetch
from analysis import price_admission_probe as gd4
from scripts import run_vs1_v2_insider_density as runner
from scripts.run_vs1_v8_insider_density import check_early_probe

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)


def test_v8_prereg_is_pinned_and_states_the_design():
    assert v8.check_prereg() == v8.PREREG_BODY_SHA256
    body = v1.prereg_body((v2.REPO / v8.PREREG_PATH).read_text(encoding="utf-8"))
    for phrase in ("[2008-01-01, 2020-01-01)", "2007-11-02 through 2019-12-31", "sole confirmatory trial",
                   "one-sided in the pre-registered positive direction, at alpha 0.05",
                   "**Both must be at least 0.50.**", "factor_t_garch_exposed", v7.E0_SCORECARD_SHA256):
        assert phrase in body
    assert v8.E0_MANIFEST_SHA256 == v7.E0_MANIFEST_SHA256
    assert v8.SUPERSEDED_BY is None and v7.SUPERSEDED_BY == {"version": "vs1-v8"}


def test_v8_window_is_scoped_and_holdout_fixed():
    original = v1.window_bounds("discovery")
    with v1.discovery_window(v8.DISCOVERY_START):
        assert v1.window_bounds("discovery") == (pd.Timestamp("2008-01-01T00:00:00Z"), pd.Timestamp(v1.SPLIT))
        assert v1.window_bounds("holdout") == (pd.Timestamp(v1.SPLIT), pd.Timestamp(v1.END))
    assert v1.window_bounds("discovery") == original


def test_v8_vendor_window_probe_identity_and_old_receipts_refuse(tmp_path):
    with fetch.discovery_vendor_window(v8.PROBE_START), \
            gd4.preregistered_window(v8.PROBE_START, v8.VERSION, v8.PREREG_BODY_SHA256):
        assert fetch.td_params("XLK", "all")["start_date"] == "2007-11-02"
        assert gd4.probe_rule().discovery_window == ("2007-11-02", "2019-12-31")
        assert gd4.probe_identity() == (v8.VERSION, v8.PREREG_BODY_SHA256)
    window = {"start": "2011-08-02", "end": "2019-12-31"}
    probe = {"prereg": {"study": v7.VERSION, "body_sha256": v7.PREREG_BODY_SHA256}, "read_window": window}
    cross = {"prereg": probe["prereg"], "read_window": window}
    (tmp_path / "p.json").write_text(json.dumps(probe))
    (tmp_path / "c.json").write_text(json.dumps(cross))
    with pytest.raises(PermissionError, match="not bound to the v8"):
        check_early_probe(tmp_path / "p.json", tmp_path / "c.json")


def test_v8_freeze_digest_covers_v8_and_e0_code(tmp_path, monkeypatch):
    assert set(runner._code_files(v7)) == set(runner.V7_CODE_FILES)
    original = runner._code_files(v8)
    assert set(original) == set(runner.V8_CODE_FILES)
    assert {"analysis/panel_insider_density_v8.py", "evals/e0/generator.py",
            "evals/e0/MANIFEST.sha256"} <= set(original)
    for name in runner.V8_CODE_FILES:
        copy = tmp_path / name
        copy.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(runner.REPO / name, copy)
    monkeypatch.setattr(runner, "REPO", tmp_path)
    (tmp_path / "evals/e0/generator.py").write_bytes((tmp_path / "evals/e0/generator.py").read_bytes() + b"\n#x\n")
    changed = runner._code_files(v8)
    assert changed["evals/e0/generator.py"] != original["evals/e0/generator.py"]
    frozen = {key: None for key in v2.OBSERVED_INPUT_KEYS}
    frozen["code_file_sha256"] = original
    with pytest.raises(PermissionError, match="code_file_sha256"):
        v2._check_observed(frozen, {**frozen, "code_file_sha256": changed})


def test_v8_is_fail_closed_until_the_v7_stop_head_is_bound(tmp_path):
    assert v8.V7_STOP_HEAD_SHA256 is None and v8.REGISTERED_RECORD_SHA256 is None
    with pytest.raises(PermissionError, match="not bound"):
        v8.registration_records(NOW, "a" * 40)
    with pytest.raises(PermissionError, match="not bound"):
        v8.check_census({"tip": "t"}, stop_head_sha256="b" * 64, witness_repo=tmp_path)
    with pytest.raises(PermissionError, match="not registered"):
        v8.append_stop_status(tmp_path, NOW, power_path=tmp_path / "p.json", decision_ref="x",
                              expected_prev_sha256="c" * 64, witness=None)


def test_v8_census_needs_both_terminal_stops(monkeypatch, tmp_path):
    head = "e" * 64
    monkeypatch.setattr(v8, "V7_STOP_HEAD_SHA256", head)
    seen = []
    monkeypatch.setattr(v7, "_v6_anchor_at_tip", lambda repo, tip, h: seen.append(("v6", h)))
    monkeypatch.setattr(v8, "_v7_anchor_at_tip", lambda repo, tip, h: seen.append(("v7", h)))
    counts = {key: 2 for key in v8.FROZEN_REGISTRIES} | {v6.VERSION: 3, v7.VERSION: 3}
    census = {"tip": "t", "files": {k: v1.canonical_witness_path(k) for k in counts}, "records": counts,
              "unknown": []}
    v8.check_census(census, stop_head_sha256=head, witness_repo=tmp_path)
    assert seen == [("v6", v7.V6_STOP_HEAD_SHA256), ("v7", head)]
    assert not v8.contamination([census])["contaminated"]
    opened = {**census, "files": {**census["files"], "vs1-v8": v1.canonical_witness_path("vs1-v8")},
              "records": {**counts, "vs1-v8": 2}}
    v8.check_census(opened, stop_head_sha256=head, witness_repo=tmp_path, opening=True)
    for key, value in ((v7.VERSION, 2), (v6.VERSION, 4), ("sectors-v4", 3)):
        bad = {**census, "records": {**counts, key: value}}
        with pytest.raises(PermissionError):
            v8.check_census(bad, stop_head_sha256=head, witness_repo=tmp_path)
        assert v8.contamination([bad])["contaminated"]
    s5 = {**census, "files": {**census["files"], "sectors-v5": v1.canonical_witness_path("sectors-v5")},
          "records": {**counts, "sectors-v5": 2}}
    with pytest.raises(PermissionError, match="missing or extra"):
        v8.check_census(s5, stop_head_sha256=head, witness_repo=tmp_path)
    with pytest.raises(PermissionError, match="verified v7 STOP head"):
        v8.check_census(census, stop_head_sha256="f" * 64, witness_repo=tmp_path)


def _row(trial, mean_ic, p_one, status="tested"):
    return {"trial": trial, "status": status, "mean_ic": mean_ic, "p_one_sided_positive": p_one}


def test_only_the_primary_can_be_selected_one_sided():
    ledger = [_row("A90|fwd5", 0.01, 0.049), _row("A30|fwd5", 0.05, 0.0001), _row("A90|fwd20", 0.02, 0.001),
              _row("A30|fwd20", 0.02, 0.001)]
    v8.confirmatory_selection(ledger)
    assert [t["selected"] for t in ledger] == [True, False, False, False]
    assert [t["confirmatory"] for t in ledger] == [True, False, False, False]
    for primary in (_row("A90|fwd5", 0.01, 0.051), _row("A90|fwd5", -0.01, 0.01),
                    _row("A90|fwd5", None, 1.0, status="insufficient_data")):
        assert v8.confirmatory_selection([primary])[0]["selected"] is False


def _structure_features(n_sessions=400, entities=40, seed=3):
    rng = np.random.default_rng(seed)
    out = {}
    for trial in v8.E0_TRIAL_ORDER:
        h = int(trial.split("|fwd")[1])
        rows = len(range(0, n_sessions, h))
        f = rng.poisson(0.3, size=(rows, entities)).astype(float)
        f[rng.random(f.shape) < 0.05] = np.nan
        out[trial] = f
    return out


def test_stage0_models_run_on_synthetic_geometry_and_gate_needs_both():
    features = _structure_features()
    g = v8.planted_power_one_sided(features["A90|fwd5"], 0.05, sims=8, perms=99)
    assert g["model"] == "gaussian_v1" and 0.0 <= g["power"] <= 1.0 and g["alpha_one_sided"] == 0.05
    e = v8.e0_power(features, 400, v8.E0_GATED_SCENARIO, target_ic=0.05, sims=4, perms=99)
    assert e["model"] == "e0:factor_t_garch_exposed" and e["e0_manifest_sha256"] == v8.E0_MANIFEST_SHA256
    assert 0.0 <= e["power"] <= 1.0
    gate = {"gaussian_v1": {"power": 0.9}, "e0:factor_t_garch_exposed": {"power": 0.49}}
    assert v8._gate_passed(gate) is False
    gate["e0:factor_t_garch_exposed"]["power"] = 0.5
    assert v8._gate_passed(gate) is True
    gate["gaussian_v1"]["power"] = 0.4999
    assert v8._gate_passed(gate) is False


def test_v8_power_verifier_refuses_inconsistent_gate():
    table = {t: [{"target_ic": ic, "power": 0.5, "sims": v1.POWER_SIMS} for ic in v1.POWER_TARGET_ICS]
             for t in v1.trial_names()}
    e0row = {"sims": v8.E0_SIMS, "perms": v1.POWER_PERMS, "alpha_one_sided": 0.05, "target_ic": 0.01,
             "e0_manifest_sha256": v8.E0_MANIFEST_SHA256}
    models = {"gaussian_v1": {"power": 0.7, "sims": v8.GAUSSIAN_SIMS, "perms": v1.POWER_PERMS, "seed": v1.SEED,
                              "alpha_one_sided": 0.05, "target_ic": 0.01},
              "e0:factor_t_garch_exposed": {**e0row, "power": 0.45},
              "e0:factor_t_garch": {**e0row, "power": 0.6}}
    power = {"version": v8.VERSION, "primary_trial": v8.PRIMARY_TRIAL, "settings": v1.power_settings(),
             "table": table, "gate_passed": False,
             "v8_stage0": {"settings": json.loads(json.dumps(v8.STAGE0_SETTINGS)), "models": models}}
    v8.verify_power(power)
    with pytest.raises(ValueError, match="disagrees"):
        v8.verify_power({**power, "gate_passed": True})
    bad = json.loads(json.dumps(power))
    bad["v8_stage0"]["models"]["e0:factor_t_garch_exposed"]["sims"] = 50
    with pytest.raises(ValueError, match="settings"):
        v8.verify_power(bad)
    with pytest.raises(ValueError, match="vs1-v8"):
        v8.verify_power({**power, "version": v7.VERSION})


def test_v8_records_link_the_v7_stop(monkeypatch):
    monkeypatch.setattr(v8, "V7_STOP_HEAD_SHA256", "e" * 64)
    header, prereg = v8.registration_records(NOW, "a" * 40)
    assert header["kind"] == "header" and prereg["kind"] == "preregistration"
    assert prereg["parent_registry_id"] == "vs1-v7" and prereg["parent_terminal_head_sha256"] == "e" * 64
    assert prereg["parent_terminal_records"] == 3 and prereg["parent_terminal_status"] == v7.STOP_STATUS
    assert prereg["discovery"]["start"] == v8.DISCOVERY_START
    assert prereg["confirmatory"]["alpha_one_sided"] == 0.05 and prereg["promotion_allowed"] is False
    assert [p["version"] for p in prereg["prior_registrations"]] == [f"vs1-v{n}" for n in range(1, 7)]
    assert date.fromisoformat(prereg["probe_window"][0]) == date(2007, 11, 2)
