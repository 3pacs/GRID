"""Temporal, economic and multiple-testing contracts for mystical research."""
import hashlib
import json
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pytest

from evaluation import astrogrid_celestial as study


def test_ridge_uses_training_only_scale_and_mean_with_constant_features():
    x = np.column_stack((np.arange(20), np.ones(20)))
    y = np.ones(20) * .01
    assert study.ridge_predict(x, y, np.asarray([[10000, 1]])).item() == pytest.approx(.01)
    assert study.ridge_predict(x, y + .03, np.asarray([[10000, 1]])).item() == pytest.approx(.04)


def test_training_outlier_does_not_change_unpenalized_intercept_contract():
    x = np.arange(1000, dtype=float)[:, None]
    x[-1] = 1e8
    y = x[:, 0] * .0001 + .2
    mean, scale = x.mean(axis=0), x.std(axis=0)
    standardized = (x - mean) / scale
    augmented = np.vstack((np.column_stack((np.ones(len(x)), standardized)), [[0, np.sqrt(study.LAMBDA)]]))
    coefficients = np.linalg.lstsq(augmented, np.concatenate((y, [0])), rcond=None)[0]
    future = np.asarray([[100]])
    expected = np.column_stack((np.ones(1), np.clip((future-mean)/scale, -8, 8))) @ coefficients
    assert study.ridge_predict(x, y, future) == pytest.approx(expected, abs=1e-10)


def test_economic_costs_both_legs_cash_and_short_direction():
    raw = np.asarray([.01, -.01, .02])
    side = np.asarray([1, -1, 0])
    actual = study.net_returns(side, raw)
    assert actual == pytest.approx([.01-.00025*2.01, .01-.00025*1.99, 0])
    assert study.net_returns(side, raw, 0) == pytest.approx([.01, .01, 0])


def synthetic_frame():
    n = 365 * 7
    days = [(date(2017, 1, 1) + timedelta(days=i)).isoformat() for i in range(n)]
    rng = np.random.default_rng(42)
    return {"dates": days, "prices": np.exp(np.arange(n) * .0001) * 100,
            "market": rng.normal(size=(n, 2)), "engines": rng.normal(size=(n, 12)),
            "sky": rng.normal(size=(n, 3)), "trend": np.ones(n), "high_vol": np.zeros(n),
            "engine_names": [str(i) for i in range(12)]}


def test_annual_training_purges_labels_that_reach_the_forecast_year(monkeypatch):
    frame = synthetic_frame()
    monkeypatch.setattr(study, "ridge_predict", lambda x, y, future: np.full(len(future), y.mean()))
    _, predictions, audit = study.walk_forward(frame, 20)
    assert audit
    assert all(a["last_label_end"] < f"{a['year']}-01-01" for a in audit)
    modified = {**frame, "prices": frame["prices"].copy()}
    boundary = frame["dates"].index("2023-01-01")
    modified["prices"][boundary:] *= np.exp(np.arange(len(frame["dates"])-boundary)*.01)
    _, changed, _ = study.walk_forward(modified, 20)
    assert np.array_equal(predictions["all_sky"][:boundary], changed["all_sky"][:boundary], equal_nan=True)
    assert predictions["all_sky"][boundary] == changed["all_sky"][boundary]


def test_block_resampling_is_deterministic_and_preserves_constant_paired_difference():
    positions = np.arange(0, 200, 5)
    values = np.column_stack((positions*.0001, positions*.0001+.001, np.ones(len(positions))*(-.001)))
    boot = study.bootstrap_means(values, positions, 200, n_boot=50)
    assert np.array_equal(boot, study.bootstrap_means(values, positions, 200, n_boot=50))
    assert np.allclose(boot[:, 2], -.001)


def test_every_declared_trial_remains_in_multiple_testing_family():
    from analysis.offline_research_proof import bh_adjusted

    small_family = bh_adjusted([.01])
    full_family = bh_adjusted([.01] + [1.0]*125)
    assert small_family[0] == .01
    assert full_family[0] > .05


def test_sky_inputs_include_regime_interactions_without_replacing_controls():
    frame = {"trend": np.asarray([1, -1]), "high_vol": np.asarray([0, 1])}
    raw = np.asarray([[2], [3]])
    assert np.array_equal(study.interaction_columns(raw, frame), [[2, 2, 0], [3, -3, 3]])


def test_published_results_match_the_evaluator_protocol_and_full_hypothesis_family():
    root = Path(__file__).resolve().parents[1]
    r = json.loads((root/'astrogrid_web/research/celestial-equity-v1.json').read_text())
    assert r['evaluator_sha256'] == hashlib.sha256((root/'evaluation/astrogrid_celestial.py').read_bytes()).hexdigest()
    assert r['protocol_sha256'] == hashlib.sha256((root/r['protocol_path']).read_bytes()).hexdigest()
    assert len(r['cases']) == r['declared_tests'] == 126
    assert r['eligible_tests'] == 84
    assert r['sky_feature_count'] == 245
    assert r['historical_candidates'] == r['corrected_incremental_tests'] == 0
    assert all(c['events'] == 709 for c in r['cases'] if c['horizon_sessions'] == 1)
    assert all(not c['eligible'] for c in r['cases'] if c['horizon_sessions'] == 20)
    assert 'not historical LLM answers' in r['forecast_model']
    assert r['primary_cost']['commission_bps_per_side'] == 1
