"""Tests for trading/high_sharpe_ensemble.py — selection, costs, no-lookahead, evaluation."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import trading.high_sharpe_ensemble as hse

REPO_ROOT = Path(__file__).resolve().parent.parent


# ── fixtures ────────────────────────────────────────────────────────────────


def _panel(seed: int = 7, start: str = "2018-01-01", end: str = "2021-12-31") -> tuple[pd.DataFrame, pd.Series]:
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range(start, end)
    cols = list(hse.UNIVERSE)
    rets = rng.normal(0.0003, 0.012, size=(len(idx), len(cols)))
    prices = pd.DataFrame(100 * np.exp(np.cumsum(rets, axis=0)), index=idx, columns=cols)
    vix = pd.Series(np.clip(20 + np.cumsum(rng.normal(0, 0.5, len(idx))) * 0.1, 10, 60), index=idx)
    return prices, vix


@pytest.fixture(scope="module")
def panel() -> tuple[pd.DataFrame, pd.Series]:
    return _panel()


@pytest.fixture(scope="module")
def small_config() -> hse.EnsembleConfig:
    return hse.EnsembleConfig(trailing=26, selection_every=4, bootstrap_reps=500, k=3)


# ── registration pin ────────────────────────────────────────────────────────


def test_registration_file_exists_and_sha_is_pinned():
    path = REPO_ROOT / hse.PREREG_PATH
    assert path.exists(), "the pre-registration must be committed with the module"
    assert hse.PREREG_SHA256 == hse.registration_sha256(REPO_ROOT), (
        "PREREG_SHA256 does not match the registration file; a changed registration is a new version"
    )


def test_registration_names_every_candidate_and_knob():
    text = (REPO_ROOT / hse.PREREG_PATH).read_text(encoding="utf-8")
    for name in hse.CANDIDATES:
        assert f"`{name}`" in text, name
    cfg = hse.EnsembleConfig()
    for token in (f"k = {cfg.k}", f"trailing {cfg.trailing} segments", f"every {cfg.selection_every} segments",
                  f"n / (n + {cfg.shrink_n0})", f"{cfg.corr_penalty} × max correlation",
                  f"{int(cfg.cost_bps_per_side)} bp", f"n_trials = {cfg.n_trials_declared}", f"seed {cfg.seed}",
                  f"{cfg.bootstrap_reps:,} resamples", f"exceeds {cfg.dsr_min}"):
        assert token in text, token


# ── pure helpers ────────────────────────────────────────────────────────────


def test_rebalance_dates_are_last_trading_day_of_week():
    idx = pd.bdate_range("2020-01-01", "2020-02-28")
    dates = hse.rebalance_dates(idx, date(2020, 1, 1), date(2020, 2, 28))
    assert all(d.weekday() == 4 for d in dates)  # Fridays in a holiday-free bdate_range
    assert dates == sorted(dates) and len(dates) == 9


def test_segment_returns_nan_beyond_cap_not_clipped():
    idx = pd.bdate_range("2020-01-06", "2020-01-17")
    p = pd.DataFrame({"A": np.linspace(100, 110, len(idx)), "B": 100.0}, index=idx)
    p.loc[idx[-1], "B"] = 300.0  # a 200 % week: data fault
    dates = hse.rebalance_dates(idx, idx[0].date(), idx[-1].date())
    seg = hse.segment_returns(p, dates, cap=0.5)
    assert seg.shape[0] == len(dates) - 1
    assert np.isnan(seg["B"].iloc[-1])
    assert seg["A"].iloc[0] > 0


def test_path_returns_costs_turnover_once_per_side():
    idx = pd.DatetimeIndex(["2020-01-03", "2020-01-10", "2020-01-17"])
    seg = pd.DataFrame({"A": [0.01, 0.02, 0.0], "B": [0.0, -0.01, 0.0]}, index=idx)
    w = pd.DataFrame({"A": [1.0, 0.0, 0.0], "B": [0.0, 1.0, 1.0]}, index=idx)
    gross, net, turn = hse.path_returns(w, seg, cost_bps_per_side=10.0)
    assert gross.tolist() == pytest.approx([0.01, -0.01, 0.0])
    assert turn.tolist() == pytest.approx([1.0, 2.0, 0.0])  # enter A; swap A→B (two sides); hold
    assert net.tolist() == pytest.approx([0.01 - 0.001, -0.01 - 0.002, 0.0])


def test_shrunk_sharpe_halves_at_n0():
    r = pd.Series([0.01, 0.02, -0.005, 0.015] * 13)  # n = 52
    raw = hse.annualised_sharpe(r, 52)
    assert hse.shrunk_sharpe(r, n0=52, periods_per_year=52) == pytest.approx(raw / 2)
    assert hse.shrunk_sharpe(pd.Series(dtype=float), 52, 52) == 0.0


def test_select_top_k_prefers_decorrelated_and_skips_negative():
    rng = np.random.default_rng(3)
    base = rng.normal(0.004, 0.01, 104)
    trailing = pd.DataFrame(
        {
            "a": base,
            "a_clone": base + rng.normal(0, 0.0005, 104),  # near-perfect correlation with a
            "b": rng.normal(0.003, 0.01, 104),
            "loser": rng.normal(-0.01, 0.01, 104),
        }
    )
    picked = hse.select_top_k(trailing, k=2, n0=52, corr_penalty=0.5, min_shrunk=0.0, periods_per_year=52)
    assert len(picked) == 2 and "loser" not in picked
    assert "a" in picked or "a_clone" in picked
    assert not ({"a", "a_clone"} <= set(picked)), "the correlation penalty should keep the clone out"
    assert hse.select_top_k(trailing[["loser"]], 2, 52, 0.5, 0.0, 52) == []


def test_inverse_vol_weights_sum_to_one_and_favour_low_vol():
    trailing = pd.DataFrame({"lo": np.tile([0.001, -0.001], 26), "hi": np.tile([0.01, -0.01], 26)})
    w = hse.inverse_vol_weights(trailing, ["lo", "hi"])
    assert sum(w.values()) == pytest.approx(1.0)
    assert w["lo"] > w["hi"]
    assert hse.inverse_vol_weights(trailing, []) == {}


def test_clean_weights_caps_gross_at_one_and_drops_unknown():
    w = hse._clean_weights({"SPY": 1.5, "QQQ": -1.5, "ZZZ": 1.0, "GLD": float("nan")}, ["SPY", "QQQ", "GLD"])
    assert set(w) == {"SPY", "QQQ"}
    assert sum(abs(v) for v in w.values()) == pytest.approx(1.0)


def test_long_short_equal_leaves_cash_when_a_leg_is_missing():
    assert hse._long_short_equal(["A", "B"], []) == {"A": 0.25, "B": 0.25}
    w = hse._long_short_equal(["A"], ["B", "C"])
    assert w == {"A": 0.5, "B": -0.25, "C": -0.25}


# ── candidates and the point-in-time store ──────────────────────────────────


def test_candidates_return_bounded_weights_in_universe(panel):
    prices, vix = panel
    pit = hse.PanelPITStore(prices, vix, data_as_of=date(2022, 1, 3))
    d = date(2021, 6, 30)
    pit.position(d)
    ctx = hse.DecisionContext(as_of=d, prices=prices.loc[:str(d)], vix=vix.loc[:str(d)], engine=None, pit=pit)
    for name, fn in hse.CANDIDATES.items():
        w = hse._clean_weights(fn(ctx), list(prices.columns))
        assert all(t in hse.UNIVERSE for t in w), name
        assert sum(abs(v) for v in w.values()) <= 1.0 + 1e-9, name


def test_panel_pit_store_refuses_forward_reads(panel):
    prices, vix = panel
    pit = hse.PanelPITStore(prices, vix, data_as_of=date(2022, 1, 3))
    pit.position(date(2020, 6, 30))
    ids = pit.feature_ids_by_name(["spy_full", "vix_spot", "not_a_feature"])
    assert set(ids) == {"spy_full", "vix_spot"}
    with pytest.raises(ValueError, match="LOOKAHEAD"):
        pit.get_feature_matrix([ids["spy_full"]], date(2020, 1, 1), date(2020, 7, 1), date(2022, 1, 3))
    frame = pit.get_feature_matrix([ids["spy_full"]], date(2020, 1, 1), date(2020, 6, 30), date(2022, 1, 3))
    assert frame.index.max().date() <= date(2020, 6, 30)
    with pytest.raises(ValueError, match="LOOKAHEAD"):
        pit.get_pit([ids["vix_spot"]], date(2022, 1, 3), start_date=date(2020, 1, 1), end_date=date(2020, 7, 2))
    v = pit.get_pit([ids["vix_spot"]], date(2022, 1, 3), start_date=date(2020, 1, 1), end_date=date(2020, 6, 30))
    assert list(v.columns) == ["feature_id", "obs_date", "value", "release_date", "vintage_date"]
    assert max(v["obs_date"]) <= date(2020, 6, 30)


def test_weights_do_not_change_when_future_prices_change(panel):
    """Every candidate's decision at d is a function of data ≤ d only."""
    prices, vix = panel
    d = date(2021, 3, 31)
    dates = [d]
    pit_a = hse.PanelPITStore(prices, vix, data_as_of=date(2022, 1, 3))
    w_a, _ = hse.compute_candidate_paths(prices, vix, dates, hse.CANDIDATES, pit=pit_a)
    future = prices.copy()
    future.loc[str(date(2021, 4, 1)):] *= np.random.default_rng(9).uniform(0.5, 1.5, future.shape[1])
    vix_b = vix.copy()
    vix_b.loc[str(date(2021, 4, 1)):] = 80.0
    pit_b = hse.PanelPITStore(future, vix_b, data_as_of=date(2022, 1, 3))
    w_b, _ = hse.compute_candidate_paths(future, vix_b, dates, hse.CANDIDATES, pit=pit_b)
    for name in hse.CANDIDATES:
        pd.testing.assert_frame_equal(w_a[name], w_b[name], check_exact=False, atol=1e-12)


# ── walk-forward and evaluation ─────────────────────────────────────────────


@pytest.fixture(scope="module")
def run(panel, small_config) -> hse.EnsembleRun:
    prices, vix = panel
    pit = hse.PanelPITStore(prices, vix, data_as_of=date(2022, 1, 3))
    return hse.walk_forward(prices, vix, date(2019, 1, 2), date(2021, 12, 31), small_config, pit=pit)


def test_walk_forward_shapes_and_warmup(run, small_config):
    n_seg = len(run.seg_ret.index)
    assert run.candidate_net.shape == (n_seg, len(hse.CANDIDATES))
    assert run.ensemble_weights.iloc[: small_config.trailing].abs().sum().sum() == 0.0
    assert len(run.oos_index) == n_seg - small_config.trailing
    assert run.selections and run.selections[0]["date"] == run.oos_index[0].date().isoformat()
    # allocations sum to one on every segment with a selection, never above
    sums = run.ensemble_weights.abs().sum(axis=1)
    assert ((sums < 1e-9) | (abs(sums - 1.0) < 1e-9)).all()
    # the underlying book never exceeds unit gross exposure
    assert (run.ensemble_underlying.abs().sum(axis=1) <= 1.0 + 1e-9).all()
    assert (run.benchmark_net.loc[run.oos_index].index == run.oos_index).all()


def test_selection_uses_only_prior_segments(panel, small_config):
    """Changing candidate returns after a selection date cannot change that selection."""
    prices, vix = panel
    pit = hse.PanelPITStore(prices, vix, data_as_of=date(2022, 1, 3))
    a = hse.walk_forward(prices, vix, date(2019, 1, 2), date(2021, 12, 31), small_config, pit=pit)
    cut = date(2021, 1, 4)
    prices_b = prices.copy()
    prices_b.loc[str(cut):] *= 1.0 + np.random.default_rng(5).uniform(-0.3, 0.3, prices.shape[1])
    pit_b = hse.PanelPITStore(prices_b, vix, data_as_of=date(2022, 1, 3))
    b = hse.walk_forward(prices_b, vix, date(2019, 1, 2), date(2021, 12, 31), small_config, pit=pit_b)
    before = [s for s in a.selections if s["date"] < cut.isoformat()]
    before_b = [s for s in b.selections if s["date"] < cut.isoformat()]
    assert before and before == before_b


def test_evaluate_report_structure_and_verdict_logic(run):
    rep = hse.evaluate(run)
    for key in ("ensemble", "spy_buy_and_hold", "equal_weight_all_candidates", "best_single_in_hindsight",
                "excess_vs_spy_bootstrap", "deflated_sharpe", "pbo_candidates", "pass_criterion", "candidates"):
        assert key in rep
    pc = rep["pass_criterion"]
    assert pc["verdict"] == ("PASS" if pc["excess_lower_bound_positive"] and pc["dsr_passed"] else "FAIL")
    assert rep["deflated_sharpe"].get("n_trials") == run.config.n_trials_declared
    assert set(rep["selection_counts"]) <= set(hse.CANDIDATES)
    md = hse.report_markdown(rep, title="t", data_basis="synthetic")
    assert "Verdict" in md and "SPY buy-and-hold" in md


def test_block_bootstrap_lower_bound_tracks_the_mean():
    rng = np.random.default_rng(11)
    pos = rng.normal(0.01, 0.01, 300)
    neg = rng.normal(-0.01, 0.01, 300)
    bp = hse.block_bootstrap_mean(pos, reps=2000, seed=1, alpha=0.05)
    bn = hse.block_bootstrap_mean(neg, reps=2000, seed=1, alpha=0.05)
    assert bp["lower_bound"] > 0 and bp["lower_bound"] < bp["mean"]
    assert bn["lower_bound"] < 0 and bn["p_mean_le_zero"] > 0.99
    assert hse.block_bootstrap_mean(np.array([0.1, 0.2]), reps=10, seed=1, alpha=0.05)["lower_bound"] is None


def test_deflated_sharpe_is_near_one_for_a_strong_series_and_low_for_noise():
    rng = np.random.default_rng(2)
    idx = pd.bdate_range("2019-01-04", periods=400, freq="W-FRI")
    cands = pd.DataFrame(rng.normal(0, 0.01, (400, 5)), index=idx, columns=list("abcde"))
    strong = pd.Series(rng.normal(0.006, 0.01, 400), index=idx)
    noise = pd.Series(rng.normal(0.0, 0.01, 400), index=idx)
    assert hse.deflated_sharpe(strong, cands, 248, 0.95)["dsr"] > 0.95
    assert hse.deflated_sharpe(noise, cands, 248, 0.95)["dsr"] < 0.5
    assert "error" in hse.deflated_sharpe(strong.iloc[:10], cands, 248, 0.95)


def test_pbo_over_candidates_handles_small_and_large_matrices():
    rng = np.random.default_rng(4)
    idx = pd.bdate_range("2019-01-04", periods=200, freq="W-FRI")
    big = pd.DataFrame(rng.normal(0, 0.01, (200, 6)), index=idx, columns=list("abcdef"))
    out = hse.pbo_over_candidates(big)
    assert 0.0 <= out["pbo"] <= 1.0 and out["n_trials"] == 6
    tiny = big.iloc[:20]
    assert "error" in hse.pbo_over_candidates(tiny)


def test_frame_to_records_is_json_safe(run):
    recs = hse.frame_to_records(run.ensemble_weights.head(3))
    assert len(recs) == 3 and all(isinstance(r["date"], str) for r in recs)


# ── v2: trend and dual momentum ─────────────────────────────────────────────


def test_every_registered_set_is_pinned_and_counts_trials():
    for name, (path, sha) in hse.REGISTRATIONS.items():
        assert (REPO_ROOT / path).exists(), path
        assert sha == hse.registration_sha256(REPO_ROOT, name), f"{name} registration changed after pinning"
    v2 = hse.candidate_set("v2")
    assert v2.n_trials_cumulative == 248 + 12 + 1 == 261
    assert hse.candidate_set("v1").n_trials_cumulative == 248
    text = (REPO_ROOT / hse.REGISTRATIONS["v2"][0]).read_text(encoding="utf-8")
    for arm in v2.candidates():
        assert f"`{arm.rsplit('_', 1)[0]}`" in text, arm
    assert "261" in text


def _trend_panel(up: bool) -> pd.DataFrame:
    idx = pd.bdate_range("2019-01-01", periods=320)
    base = np.linspace(100, 150 if up else 60, len(idx))
    cols = list(hse.UNIVERSE_V2)
    return pd.DataFrame({c: base * (1 + 0.001 * i) for i, c in enumerate(cols)}, index=idx)


def _ctx(prices: pd.DataFrame) -> hse.DecisionContext:
    d = prices.index[-1].date()
    return hse.DecisionContext(as_of=d, prices=prices, vix=pd.Series(20.0, index=prices.index))


def test_tsmom_and_ma_filter_follow_the_trend():
    up, down = _trend_panel(True), _trend_panel(False)
    w_up = hse.tsmom_assets_weights(_ctx(up))
    assert set(w_up) == set(hse.V2_ASSETS) and sum(w_up.values()) == pytest.approx(1.0)
    assert hse.tsmom_assets_weights(_ctx(down)) == {}
    assert hse.tsmom_assets_weights(_ctx(up), ma_filter=True) == w_up
    assert hse.ma10_spy_weights(_ctx(up)) == {"SPY": 1.0}
    assert hse.ma10_spy_weights(_ctx(down)) == {}
    top3 = hse.tsmom_sectors_top3_weights(_ctx(up))
    assert len(top3) == 3 and all(v == pytest.approx(1 / 3) for v in top3.values())
    assert hse.tsmom_sectors_top3_weights(_ctx(down)) == {}


def test_gem_dual_momentum_picks_winner_or_bonds():
    up = _trend_panel(True)
    # make EFA the strongest equity, SHY flat
    up["EFA"] = np.linspace(100, 200, len(up))
    up["SHY"] = 100.0
    assert hse.gem_dual_momentum_weights(_ctx(up)) == {"EFA": 1.0}
    down = _trend_panel(False)
    down["SHY"] = 100.0
    assert hse.gem_dual_momentum_weights(_ctx(down)) == {hse.V2_BOND: 1.0}
    short = up.iloc[-100:]
    assert hse.gem_dual_momentum_weights(_ctx(short)) == {}


def test_cadenced_wrapper_holds_within_a_month_and_resets_per_instance():
    calls = []

    def fn(ctx: hse.DecisionContext) -> dict[str, float]:
        calls.append(ctx.as_of)
        return {"SPY": float(len(calls))}

    arm = hse.Cadenced(fn, "M")
    prices = _trend_panel(True)
    for d in (date(2020, 3, 6), date(2020, 3, 13), date(2020, 3, 27), date(2020, 4, 3), date(2020, 4, 10)):
        ctx = hse.DecisionContext(as_of=d, prices=prices, vix=pd.Series(dtype=float))
        w = arm(ctx)
        assert w == {"SPY": 1.0} if d.month == 3 else {"SPY": 2.0}
    assert calls == [date(2020, 3, 6), date(2020, 4, 3)]
    fresh = hse.candidate_set("v2").candidates()
    assert isinstance(fresh["gem_dual_momentum_M"], hse.Cadenced) and fresh["gem_dual_momentum_M"]._last_period is None
    assert len(fresh) == 12 and all(k.endswith(("_M", "_W")) for k in fresh)


def test_watch_criterion_is_reported_and_routes_the_verdict(run):
    rep = hse.evaluate(run)
    watch = rep["watch_criterion"]
    for key in ("sharpe_beats_spy", "excess_lower_bound_positive", "dsr_above_half", "drawdown_no_worse_than_spy"):
        assert isinstance(watch[key], bool)
    expected = "PASS" if all(watch[k] for k in ("sharpe_beats_spy", "excess_lower_bound_positive", "dsr_above_half", "drawdown_no_worse_than_spy")) else "FAIL"
    assert watch["verdict"] == expected
    assert rep["pass_criterion"]["criterion"] == "v1_pass"
    assert rep["pass_criterion"]["verdict"] == rep["pass_criterion"]["v1_pass_verdict"]


# ── v3: volatility-managed and risk parity ──────────────────────────────────


def _v3_panel(vol_daily: float, seed: int = 21) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("2019-01-01", periods=120)
    cols = list(hse.UNIVERSE_V3)
    rets = rng.normal(0.0, vol_daily, (len(idx), len(cols)))
    return pd.DataFrame(100 * np.exp(np.cumsum(rets, axis=0)), index=idx, columns=cols)


def test_v3_set_counts_trials_and_names_rules():
    v3 = hse.candidate_set("v3")
    assert v3.n_trials_cumulative == 261 + 8 + 1 == 270
    text = (REPO_ROOT / hse.REGISTRATIONS["v3"][0]).read_text(encoding="utf-8")
    for arm in v3.candidates():
        assert f"`{arm.rsplit('_', 1)[0]}`" in text, arm
    assert "270" in text and len(v3.candidates()) == 8


def test_vol_target_scales_down_in_high_vol_and_caps_at_one():
    calm = hse.vol_target_weights(_ctx(_v3_panel(0.002)), "SPY")   # ~3 % annual vol → capped at 1
    wild = hse.vol_target_weights(_ctx(_v3_panel(0.03)), "SPY")    # ~48 % annual vol → ~0.21
    assert calm == {"SPY": 1.0}
    assert 0.1 < wild["SPY"] < 0.35
    assert hse.vol_target_weights(_ctx(_v3_panel(0.01).iloc[:5]), "SPY") == {}
    sf = hse.sixty_forty_vol_scaled_weights(_ctx(_v3_panel(0.03)))
    assert sf["SPY"] / sf["IEF"] == pytest.approx(1.5) and sf["SPY"] + sf["IEF"] < 1.0
    assert hse.sixty_forty_vol_scaled_weights(_ctx(_v3_panel(0.002))) == {"SPY": pytest.approx(0.6), "IEF": pytest.approx(0.4)}


def test_risk_parity_weights_sum_to_one_and_favour_low_vol():
    panel = _v3_panel(0.01)
    panel["TLT"] = 100.0 + np.arange(len(panel)) * 0.01  # nearly riskless
    w = hse.risk_parity_weights(_ctx(panel))
    assert set(w) == set(hse.V3_RISK_PARITY)
    assert sum(w.values()) == pytest.approx(1.0)
    assert w["TLT"] == max(w.values())


# ── v4: daily mean reversion ────────────────────────────────────────────────


def test_v4_set_is_daily_and_counts_two_ensembles():
    v4 = hse.candidate_set("v4")
    assert v4.n_trials_cumulative == 270 + 6 + 2 == 278
    cfg = v4.config()
    assert (cfg.rebalance, cfg.periods_per_year, cfg.trailing, cfg.selection_every, cfg.cost_bps_per_side) == ("B", 252, 252, 21, 20.0)
    assert v4.config(cost_bps_per_side=10.0).cost_bps_per_side == 10.0
    text = (REPO_ROOT / hse.REGISTRATIONS["v4"][0]).read_text(encoding="utf-8")
    for arm in v4.candidates():
        assert f"`{arm}`" in text, arm
    assert "278" in text


def test_daily_rebalance_dates_are_every_session():
    idx = pd.bdate_range("2020-01-01", "2020-03-31")
    dates = hse.rebalance_dates(idx, date(2020, 1, 1), date(2020, 3, 31), "B")
    assert len(dates) == len(idx) and dates[0] == idx[0].date()


def _mr_panel(tail: list[float]) -> pd.DataFrame:
    """A gently rising SPY/QQQ history ending with the given closes (above the 200-day mean)."""
    idx = pd.bdate_range("2019-01-01", periods=260 + len(tail))
    base = list(np.linspace(100, 110, 260)) + tail
    return pd.DataFrame({"SPY": base, "QQQ": base}, index=idx)


def test_rsi2_enters_on_sharp_pullback_and_exits_after_rebound():
    pulled = _mr_panel([112, 110, 109])          # two straight down closes → RSI(2) = 0, still above the 200-day mean
    assert hse.rsi2_weights(_ctx(pulled), "SPY") == {"SPY": 1.0}
    rebound = _mr_panel([112, 110, 109, 111, 114])  # RSI(2) now 100 > 60 → exited
    assert hse.rsi2_weights(_ctx(rebound), "SPY") == {}
    calm = _mr_panel([110.1, 110.2, 110.3])
    assert hse.rsi2_weights(_ctx(calm), "SPY") == {}
    assert hse.rsi(pd.Series([1.0, 2.0, 3.0])) == 100.0 and hse.rsi(pd.Series([3.0, 2.0, 1.0])) == 0.0


def test_three_down_enters_after_three_lower_closes_and_flattens_on_up_close():
    down3 = _mr_panel([112, 111, 110.5, 110.2])
    assert hse.three_down_weights(_ctx(down3), "QQQ") == {"QQQ": 1.0}
    up_after = _mr_panel([112, 111, 110.5, 110.2, 111.0])
    assert hse.three_down_weights(_ctx(up_after), "QQQ") == {}
    flat_after = _mr_panel([112, 111, 110.5, 110.2, 110.2])
    assert hse.three_down_weights(_ctx(flat_after), "QQQ") == {"QQQ": 1.0}
    assert hse.three_down_weights(_ctx(_mr_panel([112, 113, 114, 115])), "QQQ") == {}


def test_turn_of_month_is_long_at_month_edges_only():
    idx = pd.bdate_range("2019-06-01", "2020-03-31")
    prices = pd.DataFrame({"SPY": 100.0, "QQQ": 100.0}, index=idx)

    def w(d: str) -> dict:
        ts = pd.Timestamp(d)
        return hse.turn_of_month_weights(hse.DecisionContext(as_of=ts.date(), prices=prices.loc[:ts], vix=pd.Series(dtype=float)), "SPY")

    assert w("2020-02-03") == {"SPY": 1.0}   # 1st session of February
    assert w("2020-02-05") == {"SPY": 1.0}   # 3rd session
    assert w("2020-02-06") == {}             # 4th session
    assert w("2020-02-14") == {}             # mid-month
    assert w("2020-02-25") == {"SPY": 1.0}   # 3 business days remain after it
    assert w("2020-02-24") == {}             # 4 remain


# ── v5: combinations ────────────────────────────────────────────────────────


def _own_look_sharpes(set_name: str) -> tuple[dict[str, float], float]:
    import glob
    import json

    files = sorted(glob.glob(str(REPO_ROOT / "docs" / "reports" / f"high-sharpe-ensemble-{set_name}-look-*.json")))
    assert files, f"no committed look record for {set_name}"
    rec = json.loads(Path(files[-1]).read_text(encoding="utf-8"))
    return {c: v["sharpe_annualised"] for c, v in rec["candidates"].items()}, rec["spy_buy_and_hold"]["sharpe_annualised"]


def test_v5_qualified_arms_follow_the_rule_from_the_committed_records():
    derived = {}
    for set_name in ("v1", "v2", "v3", "v4"):
        sharpes, spy = _own_look_sharpes(set_name)
        for arm, sr in sharpes.items():
            if sr > spy:
                derived[arm] = set_name
    assert derived == hse.V5_QUALIFIED
    assert not any(v == "v4" for v in derived.values())


def test_v5_set_counts_only_its_two_ensembles_and_primary_holds_every_arm():
    v5 = hse.candidate_set("v5")
    assert v5.n_trials_cumulative == 278 + 0 + 2 == 280
    arms = v5.candidates()
    assert set(arms) == set(hse.V5_QUALIFIED) and len(arms) == 13
    assert isinstance(arms["vol_target_spy_M"], hse.Cadenced) and not isinstance(arms["vol_target_spy_W"], hse.Cadenced)
    cfg = v5.config()
    assert (cfg.k, cfg.corr_penalty) == (13, 0.0) and cfg.min_shrunk_sharpe < -1e6
    rng = np.random.default_rng(8)
    trailing = pd.DataFrame(rng.normal(-0.001, 0.01, (52, 13)), columns=list(arms))
    picked = hse.select_top_k(trailing, cfg.k, cfg.shrink_n0, cfg.corr_penalty, cfg.min_shrunk_sharpe, cfg.periods_per_year)
    assert set(picked) == set(arms), "the primary combination holds every arm, losers included"
    assert v5.secondary_overrides == {} and hse.EnsembleConfig(**v5.secondary_overrides).k == 3
    text = (REPO_ROOT / hse.REGISTRATIONS["v5"][0]).read_text(encoding="utf-8")
    assert "280" in text and all(f"`{a}`" in text or f"`{a.rsplit('_', 1)[0]}_M` / `_W`" in text for a in arms)
    assert set(hse.UNIVERSE_V5) >= set(hse.UNIVERSE) | set(hse.V2_ASSETS) | set(hse.UNIVERSE_V3)


# ── v6: regime-conditional selection, long sample ───────────────────────────

from trading import regime_ensemble as rg  # noqa: E402


def _macro(n: int = 200, seed: int = 5) -> rg.RegimeInputs:
    rng = np.random.default_rng(seed)
    idx = pd.date_range("1990-01-01", periods=n, freq="MS")
    cpi = pd.Series(100 * np.exp(np.cumsum(rng.normal(0.002, 0.002, n))), index=idx)
    indpro = pd.Series(100 * np.exp(np.cumsum(rng.normal(0.001, 0.005, n))), index=idx)
    aaa = pd.Series(5.0 + rng.normal(0, 0.1, n), index=idx)
    baa = aaa + 0.9 + rng.normal(0, 0.05, n)
    wti = pd.Series(30 * np.exp(np.cumsum(rng.normal(0, 0.03, n))), index=idx)
    return rg.RegimeInputs(cpi=cpi, indpro=indpro, baa=baa, aaa=aaa, wti=wti)


def test_label_cells_quadrants_and_override_precedence():
    inp = _macro()
    f = rg.regime_factors(inp)
    cells = rg.label_cells(f)
    assert set(cells.dropna().unique()) <= set(rg.CELLS)
    assert cells.iloc[:24].isna().all()  # needs 12 m of y/y plus 12 m of its lag
    # force a stress month and an oil-shock month and check precedence
    f2 = f.copy()
    t = f2.dropna().index[-1]
    f2.loc[t, "baa_aaa"] = f2.loc[t, "stress_threshold"] + 1.0
    f2.loc[t, "oil_6m_log"] = 0.9
    assert rg.label_cells(f2.loc[[t]]).iloc[0] == "stress"
    f2.loc[t, "baa_aaa"] = f2.loc[t, "stress_threshold"] - 0.1
    assert rg.label_cells(f2.loc[[t]]).iloc[0] == "oil_shock"
    f2.loc[t, "oil_6m_log"] = 0.0
    f2.loc[t, "indpro_yoy"], f2.loc[t, "cpi_yoy"], f2.loc[t, "cpi_yoy_12m_ago"] = 0.02, 0.04, 0.02
    assert rg.label_cells(f2.loc[[t]]).iloc[0] == "expand_reflate"
    f2.loc[t, "indpro_yoy"], f2.loc[t, "cpi_yoy"] = -0.02, 0.01
    assert rg.label_cells(f2.loc[[t]]).iloc[0] == "contract_disinfl"


def test_cells_known_at_applies_the_slowest_publication_lag_and_current_regime_reads_it():
    inp = _macro()
    f = rg.regime_factors(inp)
    cells = rg.label_cells(f)
    known = rg.cells_known_at(cells)
    first = cells.dropna().index[0]
    assert known.index[0] == first + pd.Timedelta(days=50)  # INDPRO 50 > CPI 45
    cur = rg.current_regime(f, cells, (first + pd.Timedelta(days=55)).date())
    assert cur["cell"] == cells.loc[first] and cur["observation_month"] == first.date().isoformat()
    too_early = rg.current_regime(f, cells, (first + pd.Timedelta(days=10)).date())
    assert too_early["cell"] is None


def test_bond_index_from_yields_rises_when_yields_fall():
    y = pd.Series([5.0, 4.5, 4.0, 3.5], index=pd.date_range("2000-01-01", periods=4, freq="MS"))
    idx = rg.bond_index_from_yields(y, duration=7.0)
    assert (idx.diff().dropna() > 0).all()
    y_up = pd.Series([3.0, 3.5, 4.0], index=pd.date_range("2000-01-01", periods=3, freq="MS"))
    assert rg.bond_index_from_yields(y_up, 7.0).iloc[-1] < 1.0


def test_long_sample_arms_hold_cash_in_bills_and_respect_missing_bonds():
    idx = pd.date_range("1930-01-31", periods=30, freq="ME")
    down = pd.DataFrame({"MKT": np.linspace(100, 60, 30), "RF": np.linspace(1, 1.02, 30), "MOM": 1.0, "HML": 1.0,
                         **{c: np.linspace(100, 60, 30) for c in rg.LS_INDUSTRIES}}, index=idx)
    down["Enrgy"] = np.linspace(100, 120, 30)
    ctx = hse.DecisionContext(as_of=idx[-1].date(), prices=down, vix=pd.Series(dtype=float))
    assert rg.ls_mkt_tsmom(ctx) == {"RF": 1.0} and rg.ls_mkt_ma10(ctx) == {"RF": 1.0}
    top = rg.ls_industry_mom_top3(ctx)
    assert "Enrgy" in top and len(top) == 3
    assert rg.ls_bonds(ctx) == {} and rg.ls_6040(ctx) == {}
    down["BOND"] = 1.0
    assert rg.ls_6040(hse.DecisionContext(as_of=idx[-1].date(), prices=down, vix=pd.Series(dtype=float))) == {"MKT": 0.6, "BOND": 0.4}
    assert len(rg.long_sample_candidates()) == 8


def test_engine_regime_conditional_selection_uses_in_cell_history_and_falls_back(panel):
    prices, vix = panel
    seg_dates = hse.rebalance_dates(prices.index, date(2019, 1, 2), date(2021, 12, 31))
    # alternate two cells by calendar half-year, known immediately
    labels = pd.Series(["A" if d.month <= 6 else "B" for d in seg_dates], index=pd.DatetimeIndex([pd.Timestamp(d) for d in seg_dates]))
    cfg = hse.EnsembleConfig(trailing=26, selection_every=4, bootstrap_reps=200, regime_min_obs=20)
    pit = hse.PanelPITStore(prices, vix, data_as_of=date(2022, 1, 3))
    run = hse.walk_forward(prices, vix, date(2019, 1, 2), date(2021, 12, 31), cfg, pit=pit, regime=labels)
    assert run.regime_path is not None and set(run.regime_path.dropna().unique()) == {"A", "B"}
    sels = run.selections
    assert all(s["cell"] in ("A", "B") for s in sels)
    assert any(s["fallback"] for s in sels[:2]) and any(not s["fallback"] for s in sels[-4:])
    rep = hse.evaluate(run)
    cells = rep["by_regime_cell"]
    assert set(cells) >= {"A", "B", "_selection_fallback_share"}
    assert cells["A"]["n_segments"] + cells["B"]["n_segments"] == len(run.oos_index)
    assert 0.0 <= cells["_selection_fallback_share"] <= 1.0


def test_v6_set_counts_arm_times_cell_trials():
    v6 = hse.candidate_set("v6")
    assert len(v6.candidates()) == 27
    assert hse.v6_trials_added() == 27 * 6 + 8 * 6 + 2 == 212
    assert v6.n_trials_cumulative == 280 + 212 == 492
    assert v6.config().regime_min_obs == 26 and v6.config().criterion == "watch"
    text = (REPO_ROOT / hse.REGISTRATIONS["v6"][0]).read_text(encoding="utf-8")
    for cell in rg.CELLS:
        assert f"`{cell}`" in text, cell
    for arm in rg.long_sample_candidates():
        assert f"`{arm}`" in text, arm
    assert "492" in text and "1946" in text and "1973" in text
