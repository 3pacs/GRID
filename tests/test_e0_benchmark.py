"""E0 benchmark behaviour at small N (the full run is ``python -m evals.e0 run --profile full``)."""

from __future__ import annotations

import json
from datetime import date, timedelta

import numpy as np
import pytest

from evals.e0 import benchmark, generator, machinery, replication, scorer
from evals.e0.structure import load_structure


@pytest.fixture(scope="module")
def config():
    return benchmark.load_config()


@pytest.fixture(scope="module")
def ci_card():
    return benchmark.run("ci", replicate=False, log=lambda _msg: None)


def test_same_seed_gives_a_byte_identical_scorecard():
    first = benchmark.run("smoke", replicate=False, log=lambda _msg: None)
    second = benchmark.run("smoke", replicate=False, log=lambda _msg: None)
    dump = lambda card: json.dumps(card, indent=2, sort_keys=True, allow_nan=False)  # noqa: E731
    assert dump(first) == dump(second)


def test_planted_ic_005_is_detected_with_high_power(ci_card):
    scored = ci_card["scenarios"][ci_card["headline"]["scenario"]]
    row = next(r for r in scored["power_curve"] if r["target_ic"] == 0.05)
    assert row["power_holm_run_alpha"]["rate"] >= 0.9
    assert row["power_bh"]["rate"] >= 0.9
    assert abs(row["realized_mean_ic"] - 0.05) < 0.01


def test_null_fdr_and_fwer_near_nominal(ci_card):
    scored = ci_card["scenarios"][ci_card["headline"]["scenario"]]
    null = scored["fdr"]["global_null"]
    # 48 null families: allow Monte Carlo slack of about 2.5 standard errors.
    assert null["fdr_bh"]["rate"] <= 0.10 + 0.11
    assert null["fwer_holm_run_alpha"]["rate"] <= 0.05 + 0.08
    pooled = scored["null_calibration"]["pooled"]
    assert pooled["ks_stat"] <= 1.63 / np.sqrt(pooled["n"])  # 1% KS critical value
    assert ci_card["checks"][ci_card["headline"]["scenario"]]["fdr_bh_controlled"]


def test_scorecard_is_attributable(ci_card):
    assert ci_card["version"] == "e0-v1"
    assert set(ci_card["machinery_sha256"]) == set(machinery.MACHINERY_FILES)
    assert len(ci_card["manifest"]["manifest_sha256"]) == 64


def test_write_once(tmp_path, ci_card):
    path = benchmark.write_scorecard(ci_card, tmp_path)
    assert json.loads(path.read_text(encoding="utf-8"))["version"] == "e0-v1"
    with pytest.raises(FileExistsError):
        benchmark.write_scorecard(ci_card, tmp_path)


def test_structure_is_the_v7_geometry(config):
    st = load_structure(config)
    assert st.n_entities == 202 and st.n_sessions == 2068
    assert st.trials["A90|fwd5"].feature.shape == (414, 202)
    assert st.trials["A90|fwd20"].feature.shape == (104, 202)


def test_generator_is_seeded_and_plant_zero_is_identity(config):
    st = load_structure(config)
    scenario = config["scenarios"]["factor_t_garch_exposed"]
    a = generator.simulate_world(st, scenario, np.random.default_rng([1, 2]))
    b = generator.simulate_world(st, scenario, np.random.default_rng([1, 2]))
    assert np.array_equal(a.cum_returns, b.cum_returns)
    label, sd = a.forward(st.trials["A90|fwd5"])
    assert label.shape == (414, 202) and np.all(sd > 0)
    assert generator.plant(label, sd, np.ones_like(label), 0.0) is label


def test_machinery_adapter_is_the_vs1_statistic():
    from analysis import panel_insider_density as panel

    rng = np.random.default_rng(7)
    feature = rng.standard_normal((60, 40))
    label = 0.1 * feature + rng.standard_normal((60, 40))
    rec = machinery.measure(feature, label, 5, "t", perms=199)
    ic, _ = panel.rank_ic_series(feature, label)
    direct = machinery.measure_ic_series(ic, perms=199, direction=1)
    assert rec["status"] == "tested"
    assert rec["p"] == direct["p"] and rec["mean_ic"] == pytest.approx(direct["mean_ic"])


def test_ks_uniform():
    grid = (np.arange(1, 1001) - 0.5) / 1000
    assert scorer.ks_uniform(grid)["ks_stat"] < 0.001
    assert scorer.ks_uniform(np.zeros(100))["ks_stat"] == pytest.approx(1.0)


def _fake_prices(tmp_path, tickers, start=date(1994, 1, 3), n=420, last=None):
    rng = np.random.default_rng(3)
    dates = [start + timedelta(days=7 * i) for i in range(n)]
    if last is not None:
        dates[-1] = last
    closes = 50 * np.exp(np.cumsum(0.03 * rng.standard_normal((n, len(tickers))), axis=0))
    path = tmp_path / "prices.npz"
    np.savez_compressed(path, dates=np.array([d.isoformat() for d in dates]), tickers=np.array(tickers),
                        closes=closes)
    return path


def test_replication_runs_on_a_clean_pre2011_panel(tmp_path, config):
    path = _fake_prices(tmp_path, [f"ZZ{i:03d}" for i in range(60)])
    out = replication.run_replication(config, perms=199, path=path)
    assert out["declared"] == 3 and set(out["trials"]) == set(config["replication"]["trials"])
    assert out["guards"]["vs1_windows_touched"] is False


def test_replication_refuses_vs1_windows(tmp_path, config):
    path = _fake_prices(tmp_path, [f"ZZ{i:03d}" for i in range(30)], last=date(2011, 1, 3))
    with pytest.raises(replication.ReplicationGuardError, match="cutoff"):
        replication.load_prices(config, path)


def test_replication_refuses_vs1_technology_tickers(tmp_path, config):
    assert "AAPL" in replication.load_denylist(config)
    path = _fake_prices(tmp_path, ["ZZ001", "AAPL"])
    with pytest.raises(replication.ReplicationGuardError, match="Technology"):
        replication.load_prices(config, path)


def test_v7_crosscheck_flags_disagreement(config):
    def card(rate):
        return {"power_curve": [{"target_ic": 0.01, "power_raw_threshold": {"rate": rate, "se": 0.01},
                                 "power_holm_run_alpha": {"rate": rate}, "power_bh": {"rate": rate},
                                 "sims": 500}]}

    agree = benchmark.crosscheck_v7({"s": card(0.53)}, config, "s")
    assert agree["flag"].startswith("AGREES")
    disagree = benchmark.crosscheck_v7({"s": card(0.35)}, config, "s")
    assert disagree["flag"].startswith("DISAGREES")
    assert disagree["by_scenario"]["s"]["passes_v7_gate"] is False
