"""Regime-conditioned ensemble (set v6): external macro regime cells, a long-sample validation
arm on Kenneth French portfolios, the analog windows, and the current-regime classification.

Registration: ``docs/paper_log/high-sharpe-ensemble-v6-preregistration.md``. Paper only.
Builds on :mod:`trading.high_sharpe_ensemble` (the walk-forward engine, with its
``regime=`` hook) and :mod:`alpha_research.data.long_sample` (the documented downloads).

Regime cells (fixed): a growth × inflation quadrant from industrial production and CPI, with
two binary overrides — a financial-stress flag from the Baa–Aaa spread and an energy
supply-shock flag from WTI — applied in that order of precedence:

    stress            Baa−Aaa spread above its trailing 120-month 80th percentile
    oil_shock         WTI 6-month log change above +0.35
    expand_reflate    INDPRO YoY > 0 and CPI YoY above its level 12 months earlier
    expand_disinflate INDPRO YoY > 0 and CPI YoY at or below 12 months earlier
    contract_reflate  INDPRO YoY ≤ 0 and CPI YoY rising
    contract_disinfl  INDPRO YoY ≤ 0 and CPI YoY falling

Every label is stamped with the date it became *known* (observation month start + the declared
publication lag of its slowest input), so the walk-forward uses the label in force at each
decision date.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from alpha_research.data.long_sample import PUBLICATION_LAG_DAYS, known_at
from trading.high_sharpe_ensemble import CandidateFn, DecisionContext

# ── Regime cells ────────────────────────────────────────────────────────────

CELLS: tuple[str, ...] = (
    "stress", "oil_shock", "expand_reflate", "expand_disinflate", "contract_reflate", "contract_disinfl",
)
STRESS_WINDOW = 120
STRESS_QUANTILE = 0.80
STRESS_MIN_HISTORY = 36
OIL_SHOCK_MONTHS = 6
OIL_SHOCK_LOG_CHANGE = 0.35
GPR_WINDOW = 120
GPR_QUANTILE = 0.90


@dataclass(frozen=True)
class RegimeInputs:
    """Monthly macro inputs (FRED ids), indexed by observation month start."""

    cpi: pd.Series       # CPIAUCNS level
    indpro: pd.Series    # INDPRO level
    baa: pd.Series       # Moody's Baa yield
    aaa: pd.Series       # Moody's Aaa yield
    wti: pd.Series       # WTISPLC monthly spot
    gpr: pd.Series | None = None  # GPRH / GPR, descriptive only


def regime_factors(inp: RegimeInputs) -> pd.DataFrame:
    """The derived monthly factors behind the cells (observation-month indexed)."""
    f = pd.DataFrame(index=inp.cpi.index.union(inp.indpro.index).union(inp.baa.index).union(inp.wti.index))
    f["cpi_yoy"] = np.log(inp.cpi).diff(12).reindex(f.index)
    f["cpi_yoy_12m_ago"] = f["cpi_yoy"].shift(12)
    f["indpro_yoy"] = np.log(inp.indpro).diff(12).reindex(f.index)
    spread = (inp.baa - inp.aaa).reindex(f.index)
    f["baa_aaa"] = spread
    f["stress_threshold"] = spread.rolling(STRESS_WINDOW, min_periods=STRESS_MIN_HISTORY).quantile(STRESS_QUANTILE).shift(1)
    f["oil_6m_log"] = np.log(inp.wti).diff(OIL_SHOCK_MONTHS).reindex(f.index)
    if inp.gpr is not None:
        g = inp.gpr.reindex(f.index)
        f["gpr"] = g
        f["gpr_threshold"] = g.rolling(GPR_WINDOW, min_periods=STRESS_MIN_HISTORY).quantile(GPR_QUANTILE).shift(1)
    return f


def label_cells(factors: pd.DataFrame) -> pd.Series:
    """Cell per observation month; NaN where an input is missing."""
    out = pd.Series(index=factors.index, dtype=object)
    for ts, row in factors.iterrows():
        need = ("cpi_yoy", "cpi_yoy_12m_ago", "indpro_yoy", "baa_aaa", "stress_threshold", "oil_6m_log")
        if any(pd.isna(row.get(k)) for k in need):
            out.loc[ts] = None
            continue
        if row["baa_aaa"] > row["stress_threshold"]:
            out.loc[ts] = "stress"
        elif row["oil_6m_log"] > OIL_SHOCK_LOG_CHANGE:
            out.loc[ts] = "oil_shock"
        else:
            expand = row["indpro_yoy"] > 0
            reflate = row["cpi_yoy"] > row["cpi_yoy_12m_ago"]
            out.loc[ts] = ("expand_" if expand else "contract_") + ("reflate" if reflate else ("disinflate" if expand else "disinfl"))
    return out


def cells_known_at(cells: pd.Series) -> pd.Series:
    """Re-index cell labels by the date they became public (slowest input: CPI/INDPRO, 45 days)."""
    lag = max(PUBLICATION_LAG_DAYS["CPIAUCNS"], PUBLICATION_LAG_DAYS["INDPRO"], PUBLICATION_LAG_DAYS["BAA"], PUBLICATION_LAG_DAYS["WTISPLC"])
    return known_at(cells, lag)


def current_regime(factors: pd.DataFrame, cells: pd.Series, as_of: date) -> dict[str, Any]:
    """The cell in force on ``as_of`` and the factor values behind it (as known then)."""
    known = cells_known_at(cells)
    in_force = known.loc[: pd.Timestamp(as_of)]
    if in_force.empty:
        return {"as_of": as_of.isoformat(), "cell": None}
    label = in_force.iloc[-1]
    obs_month = cells.dropna().index[cells.dropna().to_numpy() == label]
    # the observation month whose label is in force is the latest one known by as_of
    months_known = known_at(cells.dropna(), max(PUBLICATION_LAG_DAYS["CPIAUCNS"], PUBLICATION_LAG_DAYS["INDPRO"]))
    latest_known_month = cells.dropna().index[len(months_known.loc[: pd.Timestamp(as_of)]) - 1]
    row = factors.loc[latest_known_month]
    return {
        "as_of": as_of.isoformat(),
        "cell": label,
        "observation_month": latest_known_month.date().isoformat(),
        "cpi_yoy": _f(row.get("cpi_yoy")),
        "cpi_yoy_12m_ago": _f(row.get("cpi_yoy_12m_ago")),
        "indpro_yoy": _f(row.get("indpro_yoy")),
        "baa_aaa": _f(row.get("baa_aaa")),
        "stress_threshold": _f(row.get("stress_threshold")),
        "oil_6m_log": _f(row.get("oil_6m_log")),
        "gpr": _f(row.get("gpr")),
        "gpr_threshold": _f(row.get("gpr_threshold")),
        "n_months_with_label": int(len(obs_month)),
    }


def _f(v: Any) -> float | None:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


# ── Long-sample arms (monthly French panel) ─────────────────────────────────

#: Columns of :func:`alpha_research.data.long_sample.french_price_panel` plus ``BOND`` (built
#: from yields by :func:`bond_index_from_yields`) and ``OIL`` (WTISPLC level, 1946+).
LS_INDUSTRIES: tuple[str, ...] = ("NoDur", "Durbl", "Manuf", "Enrgy", "HiTec", "Telcm", "Shops", "Hlth", "Utils", "Other")
LS_CASH = "RF"
LS_BENCHMARK = "MKT"
LS_MOM_FORMATION = 12
LS_MOM_SKIP = 1
LS_MA_MONTHS = 10


def bond_index_from_yields(yields_pct: pd.Series, duration: float) -> pd.Series:
    """Approximate total-return index of a constant-maturity bond from its yield series (percent).

    Monthly return ≈ y_{t−1}/12 − D × (y_t − y_{t−1}) with ``y`` in decimals; a declared
    approximation (no convexity, constant duration). Index base 1.0 at the first observation.
    """
    y = yields_pct.dropna() / 100.0
    r = y.shift(1) / 12.0 - duration * y.diff()
    idx = (1.0 + r.fillna(0.0)).cumprod()
    return idx


def _mom_12_1_monthly(prices: pd.DataFrame) -> pd.Series:
    if len(prices) < LS_MOM_FORMATION + 1:
        return pd.Series(dtype=float)
    return (prices.iloc[-1 - LS_MOM_SKIP] / prices.iloc[-1 - LS_MOM_FORMATION] - 1.0).dropna()


def _above_ma_monthly(prices: pd.DataFrame) -> pd.Series:
    if len(prices) < LS_MA_MONTHS:
        return pd.Series(dtype=bool)
    return (prices.iloc[-1] > prices.iloc[-LS_MA_MONTHS:].mean()).reindex(prices.columns).fillna(False)


def ls_mkt_tsmom(ctx: DecisionContext) -> dict[str, float]:
    mom = _mom_12_1_monthly(ctx.prices[[LS_BENCHMARK]])
    if mom.empty:
        return {}
    return {LS_BENCHMARK: 1.0} if float(mom[LS_BENCHMARK]) > 0 else {LS_CASH: 1.0}


def ls_mkt_ma10(ctx: DecisionContext) -> dict[str, float]:
    ok = _above_ma_monthly(ctx.prices[[LS_BENCHMARK]])
    if ok.empty:
        return {}
    return {LS_BENCHMARK: 1.0} if bool(ok[LS_BENCHMARK]) else {LS_CASH: 1.0}


def ls_industry_mom_top3(ctx: DecisionContext) -> dict[str, float]:
    cols = [c for c in LS_INDUSTRIES if c in ctx.prices.columns]
    mom = _mom_12_1_monthly(ctx.prices[cols])
    if mom.empty:
        return {}
    top = mom.sort_values(ascending=False).head(3)
    return {t: 1.0 / 3.0 for t in top.index}


def ls_mom_factor(ctx: DecisionContext) -> dict[str, float]:
    return {"MOM": 1.0} if "MOM" in ctx.prices.columns else {}


def ls_hml_factor(ctx: DecisionContext) -> dict[str, float]:
    return {"HML": 1.0} if "HML" in ctx.prices.columns else {}


def ls_energy_tilt(ctx: DecisionContext) -> dict[str, float]:
    return {LS_BENCHMARK: 0.5, "Enrgy": 0.5} if "Enrgy" in ctx.prices.columns else {}


def ls_bonds(ctx: DecisionContext) -> dict[str, float]:
    return {"BOND": 1.0} if "BOND" in ctx.prices.columns and not pd.isna(ctx.prices["BOND"].iloc[-1]) else {}


def ls_6040(ctx: DecisionContext) -> dict[str, float]:
    if "BOND" not in ctx.prices.columns or pd.isna(ctx.prices["BOND"].iloc[-1]):
        return {}
    return {LS_BENCHMARK: 0.6, "BOND": 0.4}


def long_sample_candidates() -> dict[str, CandidateFn]:
    return {
        "ls_mkt_tsmom_12_1": ls_mkt_tsmom,
        "ls_mkt_ma10": ls_mkt_ma10,
        "ls_industry_mom_top3": ls_industry_mom_top3,
        "ls_mom_factor": ls_mom_factor,
        "ls_hml_factor": ls_hml_factor,
        "ls_energy_tilt": ls_energy_tilt,
        "ls_bonds": ls_bonds,
        "ls_6040": ls_6040,
    }


#: Monthly engine settings for the long sample: 12 periods a year, trailing 60 months, re-select
#: every 3, 20 bp per side, cash = bills (the ``RF`` column is held when an arm is "in cash").
LS_CONFIG: dict[str, Any] = {
    "rebalance": "ME", "periods_per_year": 12, "trailing": 60, "selection_every": 3,
    "cost_bps_per_side": 20.0, "shrink_n0": 24, "regime_min_obs": 24,
}

# ── Analog windows ──────────────────────────────────────────────────────────

ANALOG_WINDOWS: dict[str, tuple[str, str, str]] = {
    "1946-48 post-war disinflation under heavy debt": ("1946-01-31", "1948-12-31", "lead analog for today"),
    "1973-82 great inflation (failure mode)": ("1973-01-31", "1982-12-31", "the path to rule out"),
    "1973-74 embargo, Fed accommodated": ("1973-10-31", "1974-12-31", "oil shock cell"),
    "1979-81 Iranian revolution, Volcker tightened": ("1979-01-31", "1981-12-31", "oil shock cell"),
    "1990-91 Gulf War spike": ("1990-08-31", "1991-03-31", "oil shock cell"),
    "2021-23 post-shock inflation": ("2021-03-31", "2023-12-31", "current-era inflation"),
    "2022 Russia": ("2022-02-28", "2022-12-31", "oil shock cell"),
    "2025-06 Israel–Iran spike": ("2025-06-30", "2025-07-31", "current-era event"),
}


def window_report(
    prices: pd.DataFrame, cells: pd.Series, start: str, end: str, extra: dict[str, pd.Series] | None = None,
) -> dict[str, Any]:
    """Annualised return of each column over ``[start, end]`` (monthly panel), plus the cells in force."""
    seg = prices.loc[start:end]
    if len(seg) < 2:
        return {"n_months": int(len(seg)), "error": "too few months"}
    rets = seg.pct_change().dropna(how="all")
    n = len(rets)
    out: dict[str, Any] = {"n_months": int(n), "annualised_return": {}, "sharpe": {}}
    for c in rets.columns:
        r = rets[c].dropna()
        if len(r) < 2:
            continue
        out["annualised_return"][c] = float((1 + r).prod() ** (12 / len(r)) - 1)
        out["sharpe"][c] = float(r.mean() / r.std(ddof=1) * math.sqrt(12)) if r.std(ddof=1) > 0 else None
    if "Enrgy" in rets.columns and LS_BENCHMARK in rets.columns:
        out["energy_minus_market_annualised"] = out["annualised_return"]["Enrgy"] - out["annualised_return"][LS_BENCHMARK]
    cell_slice = cells.loc[start:end].dropna()
    out["cells_in_force"] = {str(k): int(v) for k, v in cell_slice.value_counts().items()}
    for name, s in (extra or {}).items():
        x = s.loc[start:end].dropna()
        if len(x) >= 2:
            out.setdefault("levels_start_end", {})[name] = [float(x.iloc[0]), float(x.iloc[-1])]
    return out


__all__ = [
    "ANALOG_WINDOWS", "CELLS", "LS_CONFIG", "RegimeInputs", "bond_index_from_yields", "cells_known_at",
    "current_regime", "label_cells", "long_sample_candidates", "regime_factors", "window_report",
]
