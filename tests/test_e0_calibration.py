"""EVAL-E0C2: the E0 outcome-model calibration (scripts/e0_calibrate_outcome_model.py).

* the 2007-11-01 cutoff and the VS1 Technology deny-list are enforced by the loader;
* the fast simulation path is exactly the frozen e0-v1 generator (``simulate_world``);
* indirect inference recovers known e0-v1 parameters from a synthetic panel;
* the receipt is deterministic and write-once;
* the calibration on the cached panel needs no database.

Synthetic outcomes only, except the cached pre-2007-11 non-Technology replication panel; no VS1
price, return or IC.
"""

from __future__ import annotations

import builtins
import importlib
import json
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pytest

from evals.e0 import generator, replication

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts" / "e0_calibrate_outcome_model.py"


def _load_script():
    # A plain import from scripts/ (on sys.path), so --jobs process-pool workers (spawned with the
    # parent's sys.path) can unpickle the script's job functions.
    if str(SCRIPT.parent) not in sys.path:
        sys.path.insert(0, str(SCRIPT.parent))
    return importlib.import_module(SCRIPT.stem)


cal = _load_script()

TRUE = {  # the e0-v1 factor_t_garch values
    "market_vol": 0.010, "beta_sd": 0.3, "factor_vol": 0.006, "factor_ar1": 0.05, "tail_df": 4.0,
    "garch_alpha": 0.08, "garch_beta": 0.90, "idio_vol": 0.02, "idio_vol_dispersion": 0.4,
}


def _write_panel(path: Path, tickers, start=date(1995, 1, 3), n=120, last=None, seed=5) -> Path:
    rng = np.random.default_rng(seed)
    dates = [start + timedelta(days=7 * i) for i in range(n)]
    if last is not None:
        dates[-1] = last
    closes = 40 * np.exp(np.cumsum(0.03 * rng.standard_normal((n, len(tickers))), axis=0))
    np.savez_compressed(path, dates=np.array([d.isoformat() for d in dates]), tickers=np.array(tickers),
                        closes=closes)
    return path


def _tiny_settings(**kw):
    base = {"seed": 11, "bootstrap": 4, "reps": 1, "maxfev": 20, "df_grid": (4.0,), "jobs": 1, "sectors": False,
            "power_sims": 0, "ridge": False}
    base.update(kw)
    return cal.RunSettings(**base)


def test_e0_v1_params_match_the_frozen_config():
    v1 = cal.e0_v1_params()
    assert {k: v1[k] for k in TRUE} == TRUE
    assert cal.CUTOFF == date(2007, 11, 1)


def test_fast_path_is_simulate_world():
    """The calibration's cached simulation is the e0-v1 generator, draw for draw."""
    N, T = 23, 31
    params = dict(TRUE, factor_ar1=0.2)
    sim = cal.SimPanel(np.ones((T, N), dtype=bool), seed=7, reps=2)
    for rep in range(2):
        world = generator.simulate_world(cal.synthetic_structure(N, T), cal.scenario_from(params),
                                         np.random.default_rng([7, rep]))
        np.testing.assert_allclose(sim.returns(params, rep), cal.world_returns(world, T), rtol=1e-9, atol=1e-13)
    # re-use of the caches after a parameter change is still exact
    p2 = dict(params, garch_alpha=0.05, garch_beta=0.93, factor_ar1=0.0, market_vol=0.02, idio_vol_dispersion=0.1)
    world = generator.simulate_world(cal.synthetic_structure(N, T), cal.scenario_from(p2), np.random.default_rng([7, 1]))
    np.testing.assert_allclose(sim.returns(p2, 1), cal.world_returns(world, T), rtol=1e-9, atol=1e-13)


def test_refuses_post_cutoff(tmp_path):
    path = _write_panel(tmp_path / "late.npz", [f"ZZ{i:02d}" for i in range(25)], last=date(2007, 11, 1))
    with pytest.raises(replication.ReplicationGuardError, match="2007-11-01"):
        cal.load_panel(path)
    with pytest.raises(cal.CalibrationGuardError):
        cal.guard([date(2007, 10, 31), date(2008, 1, 2)], ["ZZ01"])
    assert issubclass(cal.CalibrationGuardError, replication.ReplicationGuardError)
    # the day before the cutoff is fine
    ok = _write_panel(tmp_path / "ok.npz", [f"ZZ{i:02d}" for i in range(25)], last=date(2007, 10, 31))
    assert cal.load_panel(ok).dates[-1] == date(2007, 10, 31)


def test_refuses_denylisted_ticker(tmp_path):
    deny = cal.load_denylist()
    assert "AAPL" in deny and len(deny) == 908
    path = _write_panel(tmp_path / "tech.npz", ["ZZ01", "AAPL", "ZZ02"])
    with pytest.raises(replication.ReplicationGuardError, match="Technology"):
        cal.load_panel(path)


def test_pinned_panel_is_cut_before_the_cutoff_and_outside_vs1():
    panel = cal.load_panel()
    assert panel.source["sha256"] == cal.PINNED_PANEL_SHA256
    assert panel.dates[-1] < cal.CUTOFF and panel.dates[-1] == date(2007, 10, 26)
    assert panel.source["rows_dropped_at_or_after_cutoff"] == 887 - len(panel.dates) > 0
    assert panel.closes.shape == (len(panel.dates), len(panel.tickers))
    assert not set(panel.tickers) & cal.load_denylist()


@pytest.fixture(scope="module")
def recovery():
    """A synthetic panel with KNOWN e0-v1 parameters (N=300, T=700 grid steps) and its fit."""
    N, T = 300, 700
    world = generator.simulate_world(cal.synthetic_structure(N, T), cal.scenario_from(TRUE),
                                     np.random.default_rng([424242, 1]))
    R = cal.world_returns(world, T)
    fitted = cal.fit_panel(R, seed=99, bootstrap=30, reps=2, maxfev=260, df_grid=[4.0], log=lambda _m: None)
    return R, fitted


@pytest.mark.timeout(900)  # a full indirect-inference fit: ~4 min on ubuntu-latest (setup included)
def test_parameter_recovery(recovery):
    R, fitted = recovery
    got = fitted["best"]["params"]
    sim = cal.SimPanel(np.isfinite(R), seed=99, reps=2)
    # market_vol scales ONE time series (the market shock). Under t(4) + GARCH(0.08, 0.90) its
    # in-sample scale over 700 grid steps varies a lot across seeds (measured: sd 0.15 over 40 seeds,
    # range 0.77-1.68; an independent check found sd 0.21, 5-95% 0.78-1.25 over 400 paths; this
    # seed's truth path is 0.83), so no estimator can promise +/-15% of the population value from one
    # panel. The +/-15% target is the truth panel's realised market scale (plus a loose +/-35% on the
    # population value); every cross-sectional parameter is checked against its known population
    # value. tail_df is held at its true value (df_grid=[4.0]): this recovers the other parameters
    # given df; df itself is profiled on a grid in the real calibration.
    truth = cal.SimPanel(np.isfinite(R), seed=424242, reps=2)  # rep 1 == the truth panel's draws
    realised_market = TRUE["market_vol"] * truth.market_scale(
        TRUE["tail_df"], {"alpha": TRUE["garch_alpha"], "beta": TRUE["garch_beta"]}, 1)
    targets = {"market_vol": realised_market, "idio_vol": TRUE["idio_vol"],
               "idio_vol_dispersion": TRUE["idio_vol_dispersion"], "beta_sd": TRUE["beta_sd"]}
    problems = [f"{k}: got {got[k]:.5g}, want {v:.5g} +/-15%" for k, v in targets.items()
                if abs(got[k] / v - 1) > 0.15]
    if abs(got["market_vol"] / TRUE["market_vol"] - 1) > 0.35:
        problems.append(f"market_vol {got['market_vol']:.5g} vs population 0.010 +/-35%")
    persistence = got["garch_alpha"] + got["garch_beta"]
    if abs(persistence - (TRUE["garch_alpha"] + TRUE["garch_beta"])) > 0.05:
        problems.append(f"persistence {persistence:.4f} vs 0.98 +/-0.05")
    # top-3 residual factor variance share: fitted model vs the true model (common random numbers)
    share = slice(1, 4)
    fitted_share, true_share = sim.moments(got)[share].sum(), sim.moments(TRUE)[share].sum()
    if abs(fitted_share - true_share) > 0.03:
        problems.append(f"top-3 factor share {fitted_share:.4f} vs {true_share:.4f} +/-0.03")
    assert not problems, f"{problems}; fitted {got}"


@pytest.mark.timeout(600)  # two tiny calibrations, one through a spawned process pool
def test_deterministic_and_write_once(tmp_path):
    path = _write_panel(tmp_path / "p.npz", [f"ZZ{i:02d}" for i in range(25)])
    two_df = {"df_grid": (4.0, 6.0)}
    a = cal.write_receipt(cal.calibrate(_tiny_settings(**two_df), panel_path=path, log=lambda _m: None),
                          tmp_path / "a")
    # the same seed through the process pool (jobs=2) gives the same bytes
    b = cal.write_receipt(cal.calibrate(_tiny_settings(**two_df, jobs=2), panel_path=path, log=lambda _m: None),
                          tmp_path / "b")
    assert a.read_bytes() == b.read_bytes()
    receipt = json.loads(a.read_text(encoding="utf-8"))
    assert receipt["input"]["cutoff_exclusive"] == "2007-11-01"
    assert set(receipt["fit"]["params"]) >= set(TRUE) | {"label_missing_rate"}
    with pytest.raises(FileExistsError):
        cal.write_receipt(receipt, tmp_path / "a")


@pytest.mark.timeout(900)  # the full pipeline (fit, ridge, sectors, corners, E0 power) at a tiny budget
def test_no_db(monkeypatch, tmp_path):
    """The FULL calibration on the cached panel runs with every DB import and connection refused.

    Every stage runs (df fit, bootstrap, GARCH ridge, sector fits, corner refits, synthetic E0 power
    and the conservative pick), at a tiny optimisation budget.
    """
    blocked = ("psycopg2", "psycopg", "sqlalchemy", "asyncpg")

    def refuse(*_a, **_k):
        raise AssertionError("the calibration touched a database")

    for name in list(sys.modules):
        root = name.split(".")[0]
        if root in blocked:
            for attr in ("create_engine", "connect", "create_async_engine"):
                if hasattr(sys.modules[name], attr):
                    monkeypatch.setattr(sys.modules[name], attr, refuse, raising=False)
            monkeypatch.delitem(sys.modules, name)
    real_import = builtins.__import__

    def guarded(name, *args, **kwargs):
        if name.split(".")[0] in blocked:
            raise ImportError(f"DB import blocked in test_no_db: {name}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded)
    settings = _tiny_settings(bootstrap=3, maxfev=4, aux_maxfev=2, sectors=True, ridge=True, power_sims=1,
                              power_pilot_sims=2)
    receipt = cal.calibrate(settings, log=lambda _m: None)
    assert receipt["input"]["pinned"] is True and receipt["input"]["last_date"] < "2007-11-01"
    assert receipt["input"]["tickers_used"] >= 300
    assert receipt["ridge_profile"] and receipt["sectors"]["fits"]
    assert receipt["conservative_pick"]["picked"] and receipt["power"]["e0_v1@exposure0.3"]["sims"] == 1
