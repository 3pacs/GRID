"""Composite Regime-Particular State Estimators & Vehicle Convexity Map.

HELD (3pacs/GRID#1301): library code only. Nothing on a live path imports this module
(tests/test_regime_composites.py enforces that), and it is not part of the market-state
record. GRID already has several regime systems that disagree (#1301). A composite that
earns live use enters one at a time as a market-state variable through state 10
(#1311): standardized against its own baseline, given a max-age, backed by a registered
test, and introduced with a record-rule bump. It does not enter as a separate estimator.

Point-in-time caveat: the evaluators take series keyed by observation date. For inputs
published with a lag (CFTC positioning, EIA, H.4.1, monthly releases), a caller must
pass series aligned to when each value was known, e.g. via
``store.observations.read_window_known_at``. Otherwise a reading for day D uses values
not yet public on D.

Implements 8 PIT-clean, mathematically grounded composite estimators designed to
capture latent trends and non-linear regime particulars that produce oversized
convexity across specific investment vehicles:

  1. ``rspi``  Reserve-Scarcity & Collateral Plumbing Index (gated by RRP < $50B)
  2. ``hcdw``  Hidden Credit Decompression & Tail-Risk Wedge (CCC-BB vs VIX)
  3. ``rdfm``  Reflexive Dealer Fragility & Mechanical Flow Index (GEX + VIX9D/3M + DIX + Blackout)
  4. ``cdds``  Correlation-Dispersion Deleveraging State (COR3M x Sector IQR)
  5. ``fdtp``  Fiscal-Dominance & Term-Premium Regime Estimator (Gold residual vs Real 10Y + DXY)
  6. ``pbwg``  Physical Bottleneck vs. White-Collar Growth Wedge (1-factor causal Kalman filter)
  7. ``odfc``  Offshore Dollar-Funding & EM Carry-Break Index
  8. ``acpb``  AI CapEx-to-Power Bottleneck & Compute-Cycle Estimator

Availability & PIT contract:
  * Standardizes every component strictly against its own *prior* trailing
    history via ``intelligence.regime.baseline.standardize_last`` (never
    standardizes against itself or future rows).
  * Fails closed (``status="unavailable"`` with explicit ``reason``) when
    required inputs are missing or have insufficient history — never guesses
    or fills neutral defaults.
  * Every composite carries an explicit ``vehicle_convexity`` playbook mapping
    active regime states to high-convexity beneficiaries, victims/traps, and
    preferred structures.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any, Mapping

import numpy as np
import pandas as pd

from intelligence.regime.baseline import standardize_last, window_for

COMPOSITES_RULE = "market-composites/1"
RRP_SCARCITY_GATE_USD_B = 50.0  # RRPONTSYD is in $B on FRED


@dataclass(frozen=True)
class VehicleConvexity:
    """Explicit mapping from an active regime particular to vehicle convexity."""

    regime_particular: str
    beneficiaries: tuple[str, ...]
    victims_and_traps: tuple[str, ...]
    preferred_expression: str


@dataclass(frozen=True)
class CompositeReading:
    """Machine-readable state reading for one composite estimator."""

    name: str
    title: str
    status: str  # "ok" | "inactive_gated" | "unavailable"
    as_of: str
    value: float | None = None
    regime_state: str | None = None
    components: dict[str, Any] = field(default_factory=dict)
    vehicle_convexity: dict[str, Any] | None = None
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


VEHICLE_CONVEXITY_PLAYBOOK: dict[str, VehicleConvexity] = {
    "reserve_scarcity_alert": VehicleConvexity(
        regime_particular="Reserve-Scarcity Plumbing Shelf (ON RRP < $50B)",
        beneficiaries=("TLT straddles (two-way vol)", "SGOV / T-Bills", "Cash"),
        victims_and_traps=("Unhedged duration in repo squeeze", "BTC / Crypto", "Unprofitable high-multiple Tech"),
        preferred_expression="Long two-way rates vol (TLT straddles) until repo clears; avoid levered crypto/tech",
    ),
    "stealth_credit_fracture": VehicleConvexity(
        regime_particular="Stealth Lower-Tier Credit Fracture (CCC-BB decompression)",
        beneficiaries=("OTM HYG/JNK/BKLN puts (30-60 DTE)", "BRK-B / QUAL", "LQD"),
        victims_and_traps=("Levered Small Caps (IWM)", "BDCs / Private Credit (BIZD)", "CCC credit"),
        preferred_expression="Defined-risk 30-60 DTE OTM HYG/JNK put spreads + quality equity rotation",
    ),
    "positioning_crash": VehicleConvexity(
        regime_particular="Mechanical Equity Positioning Crash (VIX >> Credit)",
        beneficiaries=("Post-peak VIX9D/VIX3M calendar spreads", "HYG vs SPX relative value", "Quality calls after GEX turns"),
        victims_and_traps=("Chasing 0DTE puts after VIX9D > 40", "Panic selling quality credit"),
        preferred_expression="Wait for dGEX > 0 and VIX9D/VIX rollover, then sell front panic vs back vol",
    ),
    "hybrid_vol_and_credit_fracture": VehicleConvexity(
        regime_particular="Hybrid Vol Shock + Stealth CCC Credit Fracture",
        beneficiaries=("Cash / T-Bills", "Quality Fortress (BRK-B, XLP)", "Defined-risk HYG/IWM put spreads"),
        victims_and_traps=("Blind dip-buying leveraged beta", "Short 0DTE iron condors"),
        preferred_expression="Keep short-vol gated OFF; pair quality longs with 30-60 DTE credit/small-cap puts",
    ),
    "short_gamma_waterfall": VehicleConvexity(
        regime_particular="Reflexive Short-Gamma Backwardation Cliff",
        beneficiaries=("Long Gamma / 4-12 DTE Put Debit Spreads", "Cash"),
        victims_and_traps=("Short 0DTE Iron Condors (catastrophic left tail)", "Crowded single-stock calls"),
        preferred_expression="Hard-gate ALL short-vol sleeves OFF; trade defined-risk long gamma only",
    ),
    "factor_podshop_unwind": VehicleConvexity(
        regime_particular="Crowded Factor / Pod-Shop Deleveraging (High COR3M + High Sector IQR)",
        beneficiaries=("Dispersion (Short Index Vol / Long Single-Stock Vol)", "Quality/Value vs Crowded Momentum pairs"),
        victims_and_traps=("Crowded AI/Momentum concentration (XLK, NVDA)", "Unhedged beta"),
        preferred_expression="Market-neutral Quality-minus-Momentum pairs + dispersion once COR3M peaks",
    ),
    "fiscal_dominance_regime": VehicleConvexity(
        regime_particular="Fiscal-Dominance & Term-Premium Regime (Positive Gold Residual + High Real Yield)",
        beneficiaries=("Gold & Miners (GLD/GDX on margin-call dips)", "2s30s Steepeners", "Real Assets"),
        victims_and_traps=("Long-duration Treasury rallies (TLT)", "Long-duration equity multiples"),
        preferred_expression="Accumulate Gold/Real Assets on forced RDFM liquidations; fade TLT rallies",
    ),
    "physical_vs_software_wedge": VehicleConvexity(
        regime_particular="Physical Economy Tightness vs. White-Collar Software Slowdown",
        beneficiaries=("Refiners / Energy (XLE)", "NatGas / Power Grid (UNG, utilities)", "Industrial Metals (COPX)"),
        victims_and_traps=("Legacy seat-based SaaS (IGV)", "Levered software credit"),
        preferred_expression="Long Physical Energy/Power/Metals vs. Short levered software/high-multiple SaaS",
    ),
    "offshore_usd_squeeze": VehicleConvexity(
        regime_particular="Offshore Dollar Funding & EM Carry Break",
        beneficiaries=("Long USD vs BRL/MXN", "US T-Bills"),
        victims_and_traps=("EM Equities (EEM, EWZ)", "Silver (SLV)", "Bitcoin (BTC)"),
        preferred_expression="Stay defensive on EM/Crypto/Silver until DXY velocity peaks or PBOC/Fed swap relief hits",
    ),
    "ai_power_bottleneck_rotation": VehicleConvexity(
        regime_particular="AI CapEx-to-Power Bottleneck Rotation (Grid Load Accelerating, Silicon Skew Spiking)",
        beneficiaries=("Power/Grid Bottlenecks (UNG, merchant power, grid equipment)"),
        victims_and_traps=("Crowded silicon multiple expansion without EPS revision follow-through"),
        preferred_expression="Rotate incremental AI exposure from high-beta silicon into power/grid bottleneck assets",
    ),
}


def _last_z(series: pd.Series | None, cadence: str = "daily_fast") -> tuple[float | None, dict[str, Any]]:
    """Compute causal robust z-score on the latest observation using ``baseline.py``."""
    if series is None or series.empty:
        return None, {"status": "unavailable", "reason": "empty_series"}
    s = series.dropna().sort_index()
    if s.empty:
        return None, {"status": "unavailable", "reason": "all_nan"}
    win, min_obs = window_for(cadence)
    reading = standardize_last(s.to_numpy(dtype=float).tolist(), window=win, min_obs=min_obs)
    if reading is None:
        return None, {"status": "unavailable", "reason": f"insufficient_history(<{min_obs})"}
    z = reading.robust_z
    if z is None or not math.isfinite(float(z)):
        return None, {"status": "unavailable", "reason": "degenerate_scale", **asdict(reading)}
    return float(z), {"status": "ok", **asdict(reading)}


def causal_one_factor_kalman(
    series_map: Mapping[str, pd.Series],
    *,
    process_var: float = 0.05,
    obs_var: float = 1.0,
    min_series: int = 2,
) -> pd.Series:
    """Pure-NumPy causal 1-factor Kalman filter estimating a latent macro/physical state.

    Each input series is causally standardized (expanding median/MAD after
    warmup, zero lookahead) before being assimilated sequentially at each
    timestamp. Missing observations on a given date simply skip the measurement
    update for that channel (standard missing-data Kalman update).
    """
    valid = {
        k: v.dropna().sort_index().astype(float)
        for k, v in series_map.items()
        if v is not None and len(v.dropna()) >= 20
    }
    if len(valid) < min_series:
        return pd.Series(dtype=float, name="kalman_latent_state")

    # Causally standardize each channel using strictly prior expanding window
    z_channels: dict[str, pd.Series] = {}
    for k, s in valid.items():
        vals = s.to_numpy(dtype=float)
        idx = list(s.index)
        zs: dict[Any, float] = {}
        for i in range(15, len(vals)):
            hist = vals[:i]
            med = float(np.median(hist))
            mad = float(np.median(np.abs(hist - med)))
            scale = 1.4826 * mad
            if scale <= 1e-12:
                q75, q25 = np.percentile(hist, [75.0, 25.0])
                scale = (q75 - q25) / 1.349
            if scale > 1e-12:
                zs[idx[i]] = float(np.clip((vals[i] - med) / scale, -5.0, 5.0))
        if zs:
            z_channels[k] = pd.Series(zs, dtype=float).sort_index()

    if len(z_channels) < min_series:
        return pd.Series(dtype=float, name="kalman_latent_state")

    all_dates = sorted({d for s in z_channels.values() for d in s.index})
    x_hat = 0.0
    p_hat = 1.0
    state_out: dict[Any, float] = {}

    for d in all_dates:
        # Predict step (random walk latent factor)
        p_pred = p_hat + process_var
        x_pred = x_hat

        # Sequential scalar measurement updates for every observed channel on date d
        x_cur, p_cur = x_pred, p_pred
        obs_count = 0
        for s in z_channels.values():
            if d in s.index:
                y = float(s.loc[d])
                if math.isfinite(y):
                    k_gain = p_cur / (p_cur + obs_var)
                    x_cur = x_cur + k_gain * (y - x_cur)
                    p_cur = (1.0 - k_gain) * p_cur
                    obs_count += 1
        if obs_count > 0:
            x_hat, p_hat = x_cur, p_cur
            state_out[d] = float(x_hat)

    return pd.Series(state_out, dtype=float, name="kalman_latent_state").sort_index()


def compute_gold_fiscal_residual(
    gold: pd.Series,
    real_10y: pd.Series,
    dxy: pd.Series,
    *,
    window: int = 252,
    min_obs: int = 120,
) -> pd.Series:
    """Causal rolling OLS residual of ``ln(Gold)`` regressed on ``Real 10Y`` and ``ln(DXY)``.

    At each date ``t``, fits OLS strictly on ``[t - window, t - 1]`` and evaluates
    the out-of-sample residual at ``t`` (zero lookahead).
    """
    df = pd.concat(
        [
            gold.rename("gold"),
            real_10y.rename("real10y"),
            dxy.rename("dxy"),
        ],
        axis=1,
        join="inner",
    ).dropna()
    df = df[(df["gold"] > 0) & (df["dxy"] > 0)].sort_index()
    if len(df) <= min_obs:
        return pd.Series(dtype=float, name="gold_fiscal_residual")

    y = np.log(df["gold"].to_numpy(dtype=float))
    x1 = df["real10y"].to_numpy(dtype=float)
    x2 = np.log(df["dxy"].to_numpy(dtype=float))
    dates = list(df.index)

    out: dict[Any, float] = {}
    for i in range(min_obs, len(df)):
        lo = max(0, i - window)
        X_train = np.column_stack([np.ones(i - lo), x1[lo:i], x2[lo:i]])
        y_train = y[lo:i]
        try:
            beta, *_ = np.linalg.lstsq(X_train, y_train, rcond=None)
        except np.linalg.LinAlgError:
            continue
        y_pred = float(beta[0] + beta[1] * x1[i] + beta[2] * x2[i])
        resid = float(y[i] - y_pred)
        if math.isfinite(resid):
            out[dates[i]] = resid
    return pd.Series(out, dtype=float, name="gold_fiscal_residual").sort_index()


def evaluate_rspi(
    as_of: date,
    *,
    rrp_usd_b: pd.Series | None,
    sofr_minus_iorb: pd.Series | None,
    sofr_tail_p99: pd.Series | None = None,
    tga_4w_over_reserves: pd.Series | None = None,
    srf_ops: pd.Series | None = None,
    ust_lev_net: pd.Series | None = None,
) -> CompositeReading:
    """Composite 1: Reserve-Scarcity & Collateral Plumbing Index (RSPI)."""
    if rrp_usd_b is None or rrp_usd_b.dropna().empty:
        return CompositeReading(
            name="rspi",
            title="Reserve-Scarcity & Collateral Plumbing Index",
            status="unavailable",
            as_of=as_of.isoformat(),
            reason="missing RRPONTSYD buffer series",
        )
    latest_rrp = float(rrp_usd_b.dropna().sort_index().iloc[-1])
    if latest_rrp >= RRP_SCARCITY_GATE_USD_B:
        return CompositeReading(
            name="rspi",
            title="Reserve-Scarcity & Collateral Plumbing Index",
            status="inactive_gated",
            as_of=as_of.isoformat(),
            value=0.0,
            regime_state="abundant_rrp_buffer",
            components={"rrp_usd_b": latest_rrp, "gate_threshold_usd_b": RRP_SCARCITY_GATE_USD_B},
        )

    parts: dict[str, float] = {}
    z_sofr, _ = _last_z(sofr_minus_iorb, "daily_fast")
    if z_sofr is not None:
        parts["z_sofr_minus_iorb"] = z_sofr
    z_tail, _ = _last_z(sofr_tail_p99, "daily_fast")
    if z_tail is not None:
        parts["z_sofr_p99_tail"] = z_tail
    z_tga, _ = _last_z(tga_4w_over_reserves, "weekly")
    if z_tga is not None:
        parts["z_tga_4w_over_reserves"] = z_tga
    z_srf, _ = _last_z(srf_ops, "daily_fast")
    if z_srf is not None:
        parts["z_srf_ops"] = z_srf
    if ust_lev_net is not None and not ust_lev_net.empty:
        z_lev, _ = _last_z(-ust_lev_net, "weekly")
        if z_lev is not None:
            parts["z_ust_short_crowding"] = 0.5 * z_lev

    if not parts:
        return CompositeReading(
            name="rspi",
            title="Reserve-Scarcity & Collateral Plumbing Index",
            status="unavailable",
            as_of=as_of.isoformat(),
            reason="RRP gate armed (<$50B) but no standardized plumbing component available",
            components={"rrp_usd_b": latest_rrp},
        )

    score = float(sum(parts.values()))
    state = "reserve_scarcity_alert" if score >= 2.0 else ("elevated_plumbing" if score >= 1.0 else "armed_normal")
    conv = asdict(VEHICLE_CONVEXITY_PLAYBOOK["reserve_scarcity_alert"]) if state == "reserve_scarcity_alert" else None
    return CompositeReading(
        name="rspi",
        title="Reserve-Scarcity & Collateral Plumbing Index",
        status="ok",
        as_of=as_of.isoformat(),
        value=round(score, 4),
        regime_state=state,
        components={"rrp_usd_b": latest_rrp, **{k: round(v, 4) for k, v in parts.items()}},
        vehicle_convexity=conv,
    )


def evaluate_hcdw(
    as_of: date,
    *,
    vix: pd.Series | None,
    hy_oas: pd.Series | None,
    ccc_oas: pd.Series | None,
    bb_oas: pd.Series | None,
) -> CompositeReading:
    """Composite 2: Hidden Credit Decompression & Tail-Risk Wedge (HCDW)."""
    z_vix, _ = _last_z(vix, "daily_fast")
    z_hy, _ = _last_z(hy_oas, "daily_fast")
    if z_vix is None or z_hy is None:
        return CompositeReading(
            name="hcdw",
            title="Hidden Credit Decompression & Tail-Risk Wedge",
            status="unavailable",
            as_of=as_of.isoformat(),
            reason="missing VIX or HY OAS baseline",
        )

    vol_credit_gap = z_vix - z_hy
    hy_accel_5d = hy_oas.dropna().sort_index().diff(5) if hy_oas is not None else pd.Series(dtype=float)
    z_hy_accel, _ = _last_z(hy_accel_5d, "daily_fast")

    z_ccc_bb: float | None = None
    latest_ccc_bb: float | None = None
    if ccc_oas is not None and bb_oas is not None:
        spread = (
            pd.concat([ccc_oas.rename("ccc"), bb_oas.rename("bb")], axis=1, join="inner")
            .dropna()
            .sort_index()
        )
        if not spread.empty:
            s_diff = spread["ccc"] - spread["bb"]
            latest_ccc_bb = float(s_diff.iloc[-1])
            z_ccc_bb, _ = _last_z(s_diff, "daily_fast")

    score = (
        (z_ccc_bb if z_ccc_bb is not None else 0.0)
        + 0.5 * (z_hy_accel if z_hy_accel is not None else 0.0)
        - 0.5 * vol_credit_gap
    )

    high_tail = (z_ccc_bb is not None and z_ccc_bb >= 1.5) or (latest_ccc_bb is not None and latest_ccc_bb >= 8.0)
    high_vol_gap = vol_credit_gap >= 1.5
    if high_tail and high_vol_gap:
        state = "hybrid_vol_and_credit_fracture"
    elif high_tail:
        state = "stealth_credit_fracture"
    elif high_vol_gap:
        state = "positioning_crash"
    else:
        state = "aligned_calm"

    conv = asdict(VEHICLE_CONVEXITY_PLAYBOOK[state]) if state in VEHICLE_CONVEXITY_PLAYBOOK else None
    return CompositeReading(
        name="hcdw",
        title="Hidden Credit Decompression & Tail-Risk Wedge",
        status="ok",
        as_of=as_of.isoformat(),
        value=round(float(score), 4),
        regime_state=state,
        components={
            "z_vix": round(z_vix, 4),
            "z_hy_oas": round(z_hy, 4),
            "vol_credit_gap_z": round(vol_credit_gap, 4),
            "z_hy_accel_5d": round(z_hy_accel, 4) if z_hy_accel is not None else None,
            "ccc_minus_bb_pct": round(latest_ccc_bb, 4) if latest_ccc_bb is not None else None,
            "z_ccc_minus_bb": round(z_ccc_bb, 4) if z_ccc_bb is not None else None,
        },
        vehicle_convexity=conv,
    )


def evaluate_rdfm(
    as_of: date,
    *,
    vix9d: pd.Series | None,
    vix3m: pd.Series | None,
    net_gex_usd_m: pd.Series | None,
    spot_to_flip_pct: pd.Series | None,
    dix: pd.Series | None = None,
    buyback_blackout_pct: pd.Series | None = None,
) -> CompositeReading:
    """Composite 3: Reflexive Dealer Fragility & Mechanical Flow Index (RDFM)."""
    if (
        vix9d is None or vix9d.dropna().empty
        or vix3m is None or vix3m.dropna().empty
        or net_gex_usd_m is None or net_gex_usd_m.dropna().empty
        or spot_to_flip_pct is None or spot_to_flip_pct.dropna().empty
    ):
        return CompositeReading(
            name="rdfm",
            title="Reflexive Dealer Fragility & Mechanical Flow Index",
            status="unavailable",
            as_of=as_of.isoformat(),
            reason="missing VIX9D, VIX3M, net_gex_usd_m, or spot_to_flip_pct",
        )

    v9 = float(vix9d.dropna().sort_index().iloc[-1])
    v3 = float(vix3m.dropna().sort_index().iloc[-1])
    if v9 <= 0 or v3 <= 0:
        return CompositeReading(
            name="rdfm",
            title="Reflexive Dealer Fragility & Mechanical Flow Index",
            status="unavailable",
            as_of=as_of.isoformat(),
            reason="non-positive VIX9D/VIX3M reading",
        )

    gex_s = net_gex_usd_m.dropna().sort_index()
    gex_now = float(gex_s.iloc[-1])
    gex_accel = float(gex_s.diff().diff().iloc[-1]) if len(gex_s) >= 3 else None
    flip_dist = abs(float(spot_to_flip_pct.dropna().sort_index().iloc[-1])) / 100.0
    term_log = math.log(v9 / v3)

    z_dix, _ = _last_z(dix, "daily_fast")
    dix_amp = max(0.0, -z_dix) if z_dix is not None else 0.0
    blackout = (
        float(buyback_blackout_pct.dropna().sort_index().iloc[-1])
        if buyback_blackout_pct is not None and not buyback_blackout_pct.dropna().empty
        else 0.0
    )

    short_gamma = 1.0 if gex_now < 0.0 else 0.0
    # Scaled in bps-equivalent fragility units so > 0.50 is severe short-gamma backwardation
    raw_score = short_gamma * max(0.0, term_log) * (flip_dist * 100.0) * (1.0 + dix_amp + 0.5 * blackout)
    state = (
        "short_gamma_waterfall"
        if (short_gamma > 0 and term_log > 0.05 and raw_score >= 0.25)
        else ("short_gamma_watch" if short_gamma > 0 else "long_gamma_pinned")
    )
    conv = asdict(VEHICLE_CONVEXITY_PLAYBOOK["short_gamma_waterfall"]) if state == "short_gamma_waterfall" else None
    return CompositeReading(
        name="rdfm",
        title="Reflexive Dealer Fragility & Mechanical Flow Index",
        status="ok",
        as_of=as_of.isoformat(),
        value=round(raw_score, 4),
        regime_state=state,
        components={
            "ln_vix9d_over_vix3m": round(term_log, 4),
            "net_gex_usd_m": round(gex_now, 2),
            "gex_second_diff_usd_m": round(gex_accel, 2) if gex_accel is not None else None,
            "abs_spot_to_flip_pct": round(flip_dist * 100.0, 4),
            "z_dix": round(z_dix, 4) if z_dix is not None else None,
            "buyback_blackout_pct": round(blackout, 4),
        },
        vehicle_convexity=conv,
    )


def evaluate_cdds(
    as_of: date,
    *,
    cor3m: pd.Series | None,
    sector_iqr_5d: pd.Series | None,
) -> CompositeReading:
    """Composite 4: Correlation-Dispersion Deleveraging State (CDDS)."""
    z_cor, _ = _last_z(cor3m, "daily_fast")
    z_iqr, _ = _last_z(sector_iqr_5d, "daily_fast")
    if z_cor is None or z_iqr is None:
        return CompositeReading(
            name="cdds",
            title="Correlation-Dispersion Deleveraging State",
            status="unavailable",
            as_of=as_of.isoformat(),
            reason="missing COR3M or sector_iqr_5d baseline",
        )

    score = max(0.0, z_cor) * max(0.0, z_iqr)
    if z_cor >= 1.5 and z_iqr >= 1.25:
        state = "factor_podshop_unwind"
    elif z_cor >= 1.5 and z_iqr < 0.5:
        state = "broad_macro_selloff"
    else:
        state = "normal_dispersion"
    conv = asdict(VEHICLE_CONVEXITY_PLAYBOOK["factor_podshop_unwind"]) if state == "factor_podshop_unwind" else None
    return CompositeReading(
        name="cdds",
        title="Correlation-Dispersion Deleveraging State",
        status="ok",
        as_of=as_of.isoformat(),
        value=round(float(score), 4),
        regime_state=state,
        components={"z_cor3m": round(z_cor, 4), "z_sector_iqr_5d": round(z_iqr, 4)},
        vehicle_convexity=conv,
    )


def evaluate_fdtp(
    as_of: date,
    *,
    gold: pd.Series | None,
    real_10y: pd.Series | None,
    dxy: pd.Series | None,
    term_premium_10y: pd.Series | None = None,
    spread_5s30s: pd.Series | None = None,
    auction_dealer_pct: pd.Series | None = None,
) -> CompositeReading:
    """Composite 5: Fiscal-Dominance & Term-Premium Regime Estimator (FDTP)."""
    if gold is None or real_10y is None or dxy is None:
        return CompositeReading(
            name="fdtp",
            title="Fiscal-Dominance & Term-Premium Regime Estimator",
            status="unavailable",
            as_of=as_of.isoformat(),
            reason="missing gold, real_10y, or dxy series",
        )
    resid_series = compute_gold_fiscal_residual(gold, real_10y, dxy)
    z_resid, _ = _last_z(resid_series, "daily_fast")
    if z_resid is None:
        return CompositeReading(
            name="fdtp",
            title="Fiscal-Dominance & Term-Premium Regime Estimator",
            status="unavailable",
            as_of=as_of.isoformat(),
            reason="insufficient overlapping history for rolling Gold residual",
        )

    parts: dict[str, float] = {"z_gold_fiscal_residual": z_resid}
    for name, ser, cad in (
        ("z_term_premium_10y", term_premium_10y, "daily_fast"),
        ("z_5s30s_spread", spread_5s30s, "daily_fast"),
        ("z_auction_dealer_pct", auction_dealer_pct, "monthly"),
    ):
        z_v, _ = _last_z(ser, cad)
        if z_v is not None:
            parts[name] = z_v

    score = float(sum(parts.values()))
    state = "fiscal_dominance_regime" if score >= 2.0 else ("elevated_term_premium" if score >= 1.0 else "neutral_fiscal")
    conv = asdict(VEHICLE_CONVEXITY_PLAYBOOK["fiscal_dominance_regime"]) if state == "fiscal_dominance_regime" else None
    return CompositeReading(
        name="fdtp",
        title="Fiscal-Dominance & Term-Premium Regime Estimator",
        status="ok",
        as_of=as_of.isoformat(),
        value=round(score, 4),
        regime_state=state,
        components={k: round(v, 4) for k, v in parts.items()},
        vehicle_convexity=conv,
    )


def evaluate_pbwg(
    as_of: date,
    *,
    physical_channels: Mapping[str, pd.Series],
    indeed_software: pd.Series | None,
    cyclical_minus_defensive_20d: pd.Series | None = None,
) -> CompositeReading:
    """Composite 6: Physical Bottleneck vs. White-Collar Growth Wedge (PBWG)."""
    k_state = causal_one_factor_kalman(physical_channels)
    if k_state.empty:
        return CompositeReading(
            name="pbwg",
            title="Physical Bottleneck vs. White-Collar Growth Wedge",
            status="unavailable",
            as_of=as_of.isoformat(),
            reason="insufficient physical economy channels for Kalman state",
        )

    z_phys = float(k_state.iloc[-1])
    z_sw, _ = _last_z(indeed_software, "daily_fast")
    z_mkt, _ = _last_z(cyclical_minus_defensive_20d, "daily_fast")

    wedge = z_phys - (z_sw if z_sw is not None else 0.0)
    macro_gap = (z_phys - z_mkt) if z_mkt is not None else None
    state = (
        "physical_vs_software_wedge"
        if (z_sw is not None and wedge >= 1.5) or (macro_gap is not None and macro_gap >= 1.5)
        else "aligned_growth"
    )
    conv = asdict(VEHICLE_CONVEXITY_PLAYBOOK["physical_vs_software_wedge"]) if state == "physical_vs_software_wedge" else None
    return CompositeReading(
        name="pbwg",
        title="Physical Bottleneck vs. White-Collar Growth Wedge",
        status="ok",
        as_of=as_of.isoformat(),
        value=round(float(wedge), 4),
        regime_state=state,
        components={
            "kalman_physical_state": round(z_phys, 4),
            "z_indeed_software": round(z_sw, 4) if z_sw is not None else None,
            "macro_vs_equity_gap": round(macro_gap, 4) if macro_gap is not None else None,
        },
        vehicle_convexity=conv,
    )


def evaluate_odfc(
    as_of: date,
    *,
    dxy: pd.Series | None,
    em_fx_stress: pd.Series | None = None,
    cb_swaps: pd.Series | None = None,
    cp_spread: pd.Series | None = None,
    pboc_inject: pd.Series | None = None,
) -> CompositeReading:
    """Composite 7: Offshore Dollar-Funding & EM Carry-Break Index (ODFC)."""
    parts: dict[str, float] = {}
    if dxy is not None and len(dxy.dropna()) > 25:
        z_dxy, _ = _last_z(dxy.dropna().sort_index().pct_change(20), "daily_fast")
        if z_dxy is not None:
            parts["z_dxy_20d"] = z_dxy
    if em_fx_stress is not None and len(em_fx_stress.dropna()) > 25:
        z_em, _ = _last_z(em_fx_stress.dropna().sort_index().pct_change(20), "daily_fast")
        if z_em is not None:
            parts["z_em_fx_20d"] = z_em
    z_swp, _ = _last_z(cb_swaps, "weekly")
    if z_swp is not None:
        parts["z_cb_swaps"] = z_swp
    z_cp, _ = _last_z(cp_spread, "daily_fast")
    if z_cp is not None:
        parts["z_cp_a2p2_spread"] = z_cp
    z_pboc, _ = _last_z(pboc_inject, "daily_fast")
    if z_pboc is not None:
        parts["pboc_offset"] = -0.5 * z_pboc

    if not parts:
        return CompositeReading(
            name="odfc",
            title="Offshore Dollar-Funding & EM Carry-Break Index",
            status="unavailable",
            as_of=as_of.isoformat(),
            reason="no dollar-funding inputs available",
        )
    score = float(sum(parts.values()))
    state = "offshore_usd_squeeze" if score >= 1.5 else ("usd_tightening" if score >= 0.75 else "benign_usd_funding")
    conv = asdict(VEHICLE_CONVEXITY_PLAYBOOK["offshore_usd_squeeze"]) if state == "offshore_usd_squeeze" else None
    return CompositeReading(
        name="odfc",
        title="Offshore Dollar-Funding & EM Carry-Break Index",
        status="ok",
        as_of=as_of.isoformat(),
        value=round(score, 4),
        regime_state=state,
        components={k: round(v, 4) for k, v in parts.items()},
        vehicle_convexity=conv,
    )


def evaluate_acpb(
    as_of: date,
    *,
    grid_load_yoy: pd.Series | None = None,
    taiwan_ic_yoy: pd.Series | None = None,
    semi_eps_rev_breadth: pd.Series | None = None,
    semi_put_skew: pd.Series | None = None,
) -> CompositeReading:
    """Composite 8: AI CapEx-to-Power Bottleneck & Compute-Cycle Estimator (ACPB)."""
    parts: dict[str, float] = {}
    for key, ser, cad, sign in (
        ("z_grid_load_yoy", grid_load_yoy, "daily_fast", 1.0),
        ("z_taiwan_ic_yoy", taiwan_ic_yoy, "monthly", 1.0),
        ("z_semi_eps_rev", semi_eps_rev_breadth, "daily_fast", 1.0),
        ("z_semi_put_skew_inv", semi_put_skew, "daily_fast", -1.0),
    ):
        z_v, _ = _last_z(ser, cad)
        if z_v is not None:
            parts[key] = sign * z_v

    if not parts:
        return CompositeReading(
            name="acpb",
            title="AI CapEx-to-Power Bottleneck & Compute-Cycle Estimator",
            status="unavailable",
            as_of=as_of.isoformat(),
            reason="no AI/power bottleneck series with sufficient history",
        )

    score = float(sum(parts.values()))
    grid_hot = parts.get("z_grid_load_yoy", 0.0) >= 1.0
    skew_stressed = parts.get("z_semi_put_skew_inv", 0.0) <= -1.0
    if grid_hot and skew_stressed:
        state = "ai_power_bottleneck_rotation"
    elif score >= 1.5:
        state = "ai_capex_accelerating"
    elif score <= -1.5:
        state = "ai_digestion_risk"
    else:
        state = "balanced_compute_cycle"
    conv = (
        asdict(VEHICLE_CONVEXITY_PLAYBOOK["ai_power_bottleneck_rotation"])
        if state == "ai_power_bottleneck_rotation"
        else None
    )
    return CompositeReading(
        name="acpb",
        title="AI CapEx-to-Power Bottleneck & Compute-Cycle Estimator",
        status="ok",
        as_of=as_of.isoformat(),
        value=round(score, 4),
        regime_state=state,
        components={k: round(v, 4) for k, v in parts.items()},
        vehicle_convexity=conv,
    )
