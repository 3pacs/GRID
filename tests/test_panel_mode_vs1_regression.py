"""Old path vs new path: panel mode reproduces the VS1 statistic byte for byte (synthetic fixtures).

VS1 v6/v7/v8 all measure through ``analysis.panel_insider_density`` (v1
``measure_trial`` / ``discover_panel`` / ``evaluate_panel_holdout``, with
``relative_labels`` for labels). Panel mode imports those primitives without
editing them; these tests run the VS1 functions and the panel-mode functions on
the same seeded fixtures and require identical records (every VS1 key, JSON
bytes equal). The E0 machinery fingerprint test in
``test_panel_mode_e0_parity.py`` proves the VS1 files themselves are untouched.
No real price, label or IC is read.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analysis import panel_insider_density as v1
from analysis import panel_mode as pm
from analysis import panel_prices as pp
from tests.panel_mode_support import construct, dumps, run_spec, synthetic_panel, v8_terminal_witness

VS1_DISCOVERY = {"A90|fwd5": (70, 1), "A90|fwd20": (40, 2), "A30|fwd5": (70, 3), "A30|fwd20": (40, 4)}


def _vs1_panels(window: str, start: str, plant: dict | None = None) -> dict[str, pm.TrialPanel]:
    plant = plant or {}
    return {t: synthetic_panel(t, window, start, n, 30, seed, plant=plant.get(t, 0.0))
            for t, (n, seed) in VS1_DISCOVERY.items()}


def _same_vs1_keys(old: dict, new: dict) -> None:
    for k in old:
        assert dumps(old[k]) == dumps(new[k]), k


@pytest.mark.parametrize("sensitivity", [True, False])
@pytest.mark.parametrize("frozen_block", [None, 3])
def test_measure_trial_byte_identical(sensitivity, frozen_block):
    for trial, panel in _vs1_panels("discovery", "2012-01-03", {"A90|fwd5": 0.4}).items():
        old = v1.measure_trial(panel, block=frozen_block, perms=2000, sensitivity=sensitivity)
        new = pm.measure_panel_trial(panel, direction=1, block=frozen_block, perms=2000, sensitivity=sensitivity,
                                     magnitude="positive_vs_zero")
        _same_vs1_keys(old, new)
        assert new["p_one_sided"] == old["p_one_sided_positive"]


def test_insufficient_data_byte_identical():
    panel = synthetic_panel("A90|fwd20", "discovery", "2012-01-03", 12, 30, 9)
    old = v1.measure_trial(panel, perms=999)
    new = pm.measure_panel_trial(panel, direction=1, perms=999)
    assert old["status"] == "insufficient_data"
    _same_vs1_keys(old, new)


def test_negative_direction_uses_its_own_one_sided_p():
    """Pre-registered negative: the one-sided p is signflip_pvalues(..., direction=-1), never a flip."""
    panel = synthetic_panel("A90|fwd5", "discovery", "2012-01-03", 70, 30, 5, plant=-0.5)
    new = pm.measure_panel_trial(panel, direction=-1, perms=2000, sensitivity=False)
    ic, _ = v1.rank_ic_series(panel.feature, panel.label)
    series = ic[np.isfinite(ic)]
    _, two, neg = v1.signflip_pvalues(series, new["block"], 2000, v1.SEED, -1)
    _, two_pos, pos = v1.signflip_pvalues(series, new["block"], 2000, v1.SEED, 1)
    assert new["mean_ic"] < 0
    assert new["p_one_sided"] == neg and new["p_one_sided_positive"] == pos and new["p"] == two == two_pos
    assert new["p_one_sided"] < 0.05 < new["p_one_sided_positive"]


def _vs1_constructs():
    return (construct("A90", 90, confirmatory=(20,)), construct("A30", 30, confirmatory=()))


def test_discover_and_holdout_byte_identical_to_vs1_at_the_same_alpha(monkeypatch, tmp_path):
    """Full ledger + holdout equality with VS1's run alpha (k=1, q=0.10 = the separate ledger's k=1)."""
    panels = _vs1_panels("discovery", "2012-01-03", {"A90|fwd5": 0.5, "A30|fwd20": 0.3})
    spec = v1.RunSpec(run_id="vs1-regression", sector=v1.VS1_SECTOR, trials=v1.trial_names())
    old = v1.discover_panel(spec, panels, inputs={"fixture": "synthetic"})
    run = run_spec(run_id="vs1-regression", option="separate", k=1)
    assert run.alpha == spec.alpha
    guard = pm.OutcomeWindowGuard(v8_terminal=v8_terminal_witness(monkeypatch, tmp_path / "v8"))
    new = pm.discover_panel(_vs1_constructs(), run, {"Technology": panels}, inputs={"fixture": "synthetic"},
                            guard=guard)
    by_new = {t["trial"]: t for t in new["payload"]["ledger"]}
    for entry in old["payload"]["ledger"]:
        _same_vs1_keys(entry, by_new[entry["trial"]])
    hold = _vs1_panels("holdout", "2020-01-02", {"A90|fwd5": 0.5})
    old_key = v1.HoldoutKey(v1._HOLDOUT_TOKEN, old["sha256"], {"as_of_ts": "2026-09-30T00:00:00+00:00"},
                            log_dir=tmp_path / "vs1", witness_tip="0" * 40)
    old_h = v1.evaluate_panel_holdout(old, hold, old_key)
    reg = pm.PanelRegistry(tmp_path / "reg", run.registry_id, run.prereg_sha256)
    new_key = pm.PanelHoldoutKey(pm._HOLDOUT_TOKEN, registry=reg, frozen_sha256=new["sha256"],
                                 inputs={"as_of_ts": "2026-09-30T00:00:00+00:00"}, witness_tip="0" * 40)
    new_h = pm.evaluate_panel_holdout(new, {"Technology": hold}, new_key, guard=guard)
    old_checks = {c["trial"]: c for c in old_h["holdout_checks"]}
    new_checks = {c["trial"]: c for c in new_h["holdout_checks"]}
    assert set(old_checks) == set(new_checks)
    for trial, c in old_checks.items():
        for k in c:
            if k == "primary_one_sided_p":
                expected = new_checks[trial]["confirmatory_one_sided_p"]
                assert dumps(c[k]) == dumps(expected)
            else:
                assert dumps(c[k]) == dumps(new_checks[trial][k]), (trial, k)
    assert new_h["promotion_allowed"] is False and new_h["verdict"]["promotion_allowed"] is False


def test_relative_labels_byte_identical_to_vs1():
    rng = np.random.default_rng(7)
    days = pd.bdate_range("2011-06-01", "2020-03-31")
    tickers = [f"T{j}" for j in range(12)]
    closes = pd.DataFrame(np.exp(np.cumsum(0.01 * rng.standard_normal((len(days), 13)), axis=0)) * 50,
                          index=days, columns=tickers + ["XLK"])
    closes.iloc[rng.random(closes.shape) < 0.02] = np.nan
    lo, hi = v1.window_bounds("discovery")
    for h in (5, 20):
        old = v1.relative_labels(closes, "XLK", tickers, h, "discovery")
        new = pp.relative_labels(closes, "XLK", tickers, h, lo, hi)
        assert old[0] == new[0]
        assert old[1].tobytes() == new[1].tobytes() and old[2].tobytes() == new[2].tobytes()
