"""Composite regime estimators (held library code, 3pacs/GRID#1301): behaviour and the hold itself."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from intelligence.regime.composites import (
    causal_one_factor_kalman,
    compute_gold_fiscal_residual,
    evaluate_acpb,
    evaluate_cdds,
    evaluate_fdtp,
    evaluate_hcdw,
    evaluate_odfc,
    evaluate_pbwg,
    evaluate_rdfm,
    evaluate_rspi,
)

ROOT = Path(__file__).resolve().parents[1]


def test_no_live_path_imports_the_held_composites():
    """Held until state 10 (#1311) routes a composite into the market-state record."""
    users = []
    for path in ROOT.rglob("*.py"):
        rel = path.relative_to(ROOT).as_posix()
        if rel.startswith(("tests/", ".claude/", "intelligence/regime/composites.py")) or "/." in "/" + rel:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        if "regime.composites" in text or "from intelligence.regime import composites" in text:
            users.append(rel)
    assert users == []


def _make_daily_series(
    values: list[float],
    end_date: date = date(2026, 11, 20),
) -> pd.Series:
    idx = pd.bdate_range(end=end_date, periods=len(values))
    return pd.Series(values, index=[d.date() for d in idx], dtype=float)


def _make_weekly_series(
    values: list[float],
    end_date: date = date(2026, 11, 18),
) -> pd.Series:
    idx = [end_date - timedelta(days=7 * (len(values) - 1 - i)) for i in range(len(values))]
    return pd.Series(values, index=idx, dtype=float)


class TestCompositeRegimeEstimators:
    def test_rspi_gate_and_live_alert(self):
        as_of = date(2026, 11, 20)
        rng = np.random.default_rng(7)
        # Abundant RRP -> inactive_gated
        rrp_high = _make_daily_series([250.0] * 150, as_of)
        sofr_spr = _make_daily_series(list(rng.normal(0.0, 0.01, 149)) + [0.06], as_of)
        gated = evaluate_rspi(as_of, rrp_usd_b=rrp_high, sofr_minus_iorb=sofr_spr)
        assert gated.status == "inactive_gated"
        assert gated.value == 0.0

        # Empty RRP ($0.33B) + tight repo + heavy UST short crowding -> reserve_scarcity_alert
        rrp_empty = _make_daily_series([10.0] * 149 + [0.33], as_of)
        ust_lev = _make_weekly_series(list(rng.normal(-500_000, 100_000, 120)) + [-3_320_000.0], date(2026, 11, 17))
        alert = evaluate_rspi(as_of, rrp_usd_b=rrp_empty, sofr_minus_iorb=sofr_spr, ust_lev_net=ust_lev)
        assert alert.status == "ok"
        assert alert.regime_state == "reserve_scarcity_alert"
        assert alert.vehicle_convexity is not None
        assert any("TLT" in b for b in alert.vehicle_convexity["beneficiaries"])

    def test_hcdw_detects_hybrid_vol_and_ccc_fracture(self):
        as_of = date(2026, 11, 20)
        rng = np.random.default_rng(11)
        vix = _make_daily_series(list(rng.normal(17.0, 2.0, 149)) + [35.89], as_of)
        hy = _make_daily_series(list(rng.normal(3.3, 0.25, 149)) + [3.52], as_of)
        ccc = _make_daily_series(list(rng.normal(8.5, 0.5, 149)) + [13.61], as_of)
        bb = _make_daily_series(list(rng.normal(2.1, 0.15, 149)) + [2.10], as_of)

        res = evaluate_hcdw(as_of, vix=vix, hy_oas=hy, ccc_oas=ccc, bb_oas=bb)
        assert res.status == "ok"
        assert res.regime_state == "hybrid_vol_and_credit_fracture"
        assert res.components["ccc_minus_bb_pct"] == pytest.approx(11.51)

    def test_rdfm_detects_short_gamma_waterfall(self):
        as_of = date(2026, 11, 20)
        rng = np.random.default_rng(13)
        v9 = _make_daily_series([16.0] * 149 + [40.72], as_of)
        v3 = _make_daily_series([20.0] * 149 + [30.94], as_of)
        gex = _make_daily_series([-2785.0, -9171.0, -11547.0, -10292.0], as_of)
        flip = _make_daily_series([-0.56, -2.17, -2.50, -1.93], as_of)
        dix = _make_daily_series(list(rng.normal(0.45, 0.02, 149)) + [0.3884], as_of)

        res = evaluate_rdfm(as_of, vix9d=v9, vix3m=v3, net_gex_usd_m=gex, spot_to_flip_pct=flip, dix=dix)
        assert res.status == "ok"
        assert res.regime_state == "short_gamma_waterfall"
        assert res.value is not None and res.value > 0.5

    def test_cdds_fdtp_pbwg_odfc_acpb_and_strict_no_lookahead(self):
        as_of = date(2026, 11, 20)
        rng = np.random.default_rng(19)

        # CDDS factor unwind
        cor = _make_daily_series(list(rng.normal(14.0, 2.0, 149)) + [29.71], as_of)
        iqr = _make_daily_series(list(rng.normal(0.02, 0.005, 149)) + [0.09], as_of)
        cdds = evaluate_cdds(as_of, cor3m=cor, sector_iqr_5d=iqr)
        assert cdds.regime_state == "factor_podshop_unwind"

        # FDTP causal Gold residual — check zero lookahead
        n = 300
        real10 = _make_daily_series(list(rng.normal(1.8, 0.3, n - 1)) + [2.88], as_of)
        dxy = _make_daily_series(list(rng.normal(103.0, 2.0, n - 1)) + [107.0], as_of)
        gold_base = np.exp(7.7 - 0.15 * real10.to_numpy() - 0.3 * np.log(dxy.to_numpy()) + rng.normal(0, 0.01, n))
        gold_base[-1] = 3539.0  # Huge positive fiscal residual
        gold = pd.Series(gold_base, index=real10.index)

        resid_a = compute_gold_fiscal_residual(gold, real10, dxy)
        gold_perturbed = gold.copy()
        gold_perturbed.iloc[-1] = 99999.0
        resid_b = compute_gold_fiscal_residual(gold_perturbed, real10, dxy)
        # Earlier residuals must be bit-for-bit identical (zero lookahead!)
        assert np.allclose(resid_a.iloc[:-1].to_numpy(), resid_b.iloc[:-1].to_numpy())

        tp = _make_daily_series(list(rng.normal(0.2, 0.15, n - 1)) + [0.99], as_of)
        fdtp = evaluate_fdtp(as_of, gold=gold, real_10y=real10, dxy=dxy, term_premium_10y=tp)
        assert fdtp.regime_state == "fiscal_dominance_regime"

        # PBWG causal 1-factor Kalman filter — check zero lookahead
        ch1 = _make_daily_series(list(rng.normal(2.0, 0.3, 159)) + [3.5], as_of)
        ch2 = _make_daily_series(list(rng.normal(20.0, 3.0, 159)) + [35.0], as_of)
        k_a = causal_one_factor_kalman({"gdp": ch1, "crack": ch2})
        ch1_mod = ch1.copy()
        ch1_mod.iloc[-1] = -999.0
        k_b = causal_one_factor_kalman({"gdp": ch1_mod, "crack": ch2})
        assert np.allclose(k_a.iloc[:-1].to_numpy(), k_b.iloc[:-1].to_numpy())

        sw = _make_daily_series(list(rng.normal(100.0, 3.0, 159)) + [81.71], as_of)
        pbwg = evaluate_pbwg(as_of, physical_channels={"gdp": ch1, "crack": ch2}, indeed_software=sw)
        assert pbwg.regime_state == "physical_vs_software_wedge"

        # ODFC offshore USD squeeze
        dxy_sq = _make_daily_series(list(rng.normal(102.0, 0.5, 169)) + [109.0], as_of)
        brl_sq = _make_daily_series(list(rng.normal(5.1, 0.1, 169)) + [5.89], as_of)
        odfc = evaluate_odfc(as_of, dxy=dxy_sq, em_fx_stress=brl_sq)
        assert odfc.regime_state == "offshore_usd_squeeze"

        # ACPB AI power bottleneck rotation
        grid = _make_daily_series(list(rng.normal(0.02, 0.01, 149)) + [0.09], as_of)
        skew = _make_daily_series(list(rng.normal(0.05, 0.01, 149)) + [0.15], as_of)
        acpb = evaluate_acpb(as_of, grid_load_yoy=grid, semi_put_skew=skew)
        assert acpb.regime_state == "ai_power_bottleneck_rotation"

        # Fail-closed check on missing data
        unavail = evaluate_acpb(as_of)
        assert unavail.status == "unavailable"
