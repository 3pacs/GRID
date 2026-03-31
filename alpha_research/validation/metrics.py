"""
Portfolio metrics for alpha research validation.

Key finding from QuantaAlpha v2:
  - Validation RankIC does NOT predict OOS RankIC (R² = 0.00)
  - Validation net Sharpe DOES predict OOS net Sharpe (R² = 0.42)
  - Minimum threshold: val net Sharpe > 1.4 to expect positive OOS
  - Budget for 64% RankIC shrinkage from validation to OOS
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats


def rank_ic(
    signal: pd.DataFrame,
    forward_returns: pd.DataFrame,
    horizon: int = 1,
) -> pd.Series:
    """
    Compute daily Rank Information Coefficient.

    RankIC = Spearman correlation between signal ranks and forward returns
    across all tickers, computed per date.
    """
    fwd = forward_returns.shift(-horizon)

    common_dates = signal.index.intersection(fwd.dropna(how="all").index)
    ics = []

    for dt in common_dates:
        sig_row = signal.loc[dt].dropna()
        ret_row = fwd.loc[dt].dropna()
        common = sig_row.index.intersection(ret_row.index)

        if len(common) < 5:
            continue

        corr, _ = stats.spearmanr(sig_row[common], ret_row[common])
        if not np.isnan(corr):
            ics.append({"date": dt, "ic": corr})

    if not ics:
        return pd.Series(dtype=float, name="rank_ic")

    return pd.DataFrame(ics).set_index("date")["ic"]


def rank_icir(ic_series: pd.Series) -> float:
    """Information Ratio of RankIC: mean(IC) / std(IC)."""
    if len(ic_series) < 10 or ic_series.std() == 0:
        return 0.0
    return float(ic_series.mean() / ic_series.std())


def long_short_returns(
    signal: pd.DataFrame,
    forward_returns: pd.DataFrame,
    top_n: int = 5,
    cost_bps: float = 10.0,
) -> pd.Series:
    """
    Compute daily long-short portfolio returns.

    Top N tickers by signal → long. Bottom N → short.
    Average long return minus average short return, net of costs.
    """
    fwd = forward_returns.shift(-1)
    common_dates = signal.index.intersection(fwd.dropna(how="all").index)

    ls_returns = []
    prev_longs = set()
    prev_shorts = set()

    for dt in common_dates:
        sig_row = signal.loc[dt].dropna()
        ret_row = fwd.loc[dt].dropna()
        common = sig_row.index.intersection(ret_row.index)

        if len(common) < 2 * top_n:
            continue

        ranked = sig_row[common].sort_values()
        shorts = set(ranked.index[:top_n])
        longs = set(ranked.index[-top_n:])

        long_ret = ret_row[list(longs)].mean()
        short_ret = ret_row[list(shorts)].mean()
        gross_ret = long_ret - short_ret

        # Transaction costs: turnover-based
        long_turnover = len(longs - prev_longs) / max(top_n, 1)
        short_turnover = len(shorts - prev_shorts) / max(top_n, 1)
        avg_turnover = (long_turnover + short_turnover) / 2
        cost = avg_turnover * cost_bps * 2 / 10_000  # round-trip

        net_ret = gross_ret - cost
        ls_returns.append({"date": dt, "return": net_ret})

        prev_longs = longs
        prev_shorts = shorts

    if not ls_returns:
        return pd.Series(dtype=float, name="ls_return")

    return pd.DataFrame(ls_returns).set_index("date")["return"]


def sharpe_ratio(returns: pd.Series, annualize: int = 252) -> float:
    """Annualized Sharpe ratio."""
    if len(returns) < 20 or returns.std() == 0:
        return 0.0
    return float(returns.mean() / returns.std() * np.sqrt(annualize))


def annualized_return(returns: pd.Series, periods_per_year: int = 252) -> float:
    """Annualized return from daily returns series."""
    if len(returns) < 2:
        return 0.0
    total = (1 + returns).prod()
    years = len(returns) / periods_per_year
    if years <= 0 or total <= 0:
        return 0.0
    return float(total ** (1 / years) - 1)


def max_drawdown(returns: pd.Series) -> float:
    """Maximum drawdown (negative number)."""
    cum = (1 + returns).cumprod()
    peak = cum.cummax()
    dd = (cum - peak) / peak
    return float(dd.min()) if len(dd) > 0 else 0.0


def calmar_ratio(returns: pd.Series) -> float:
    """Annualized return / |max drawdown|."""
    mdd = max_drawdown(returns)
    if mdd == 0:
        return 0.0
    ar = annualized_return(returns)
    return float(ar / abs(mdd))


def turnover(signal: pd.DataFrame, top_n: int = 5) -> float:
    """Average daily turnover of long-short portfolio."""
    turnovers = []
    prev_longs = set()
    prev_shorts = set()

    for dt in signal.index:
        row = signal.loc[dt].dropna()
        if len(row) < 2 * top_n:
            continue
        ranked = row.sort_values()
        shorts = set(ranked.index[:top_n])
        longs = set(ranked.index[-top_n:])

        if prev_longs:
            long_to = len(longs - prev_longs) / top_n
            short_to = len(shorts - prev_shorts) / top_n
            turnovers.append((long_to + short_to) / 2)

        prev_longs = longs
        prev_shorts = shorts

    return float(np.mean(turnovers)) if turnovers else 0.0


def compute_signal_metrics(
    signal: pd.DataFrame,
    forward_returns: pd.DataFrame,
    top_n: int = 5,
    cost_bps: float = 10.0,
) -> dict:
    """Compute full metrics suite for a signal."""
    ic = rank_ic(signal, forward_returns)
    ls_ret = long_short_returns(signal, forward_returns, top_n, cost_bps)

    return {
        "mean_rank_ic": float(ic.mean()) if len(ic) > 0 else 0.0,
        "rank_icir": rank_icir(ic),
        "sharpe_gross": sharpe_ratio(
            long_short_returns(signal, forward_returns, top_n, 0.0)
        ),
        "sharpe_net": sharpe_ratio(ls_ret),
        "annualized_return": annualized_return(ls_ret),
        "max_drawdown": max_drawdown(ls_ret),
        "calmar": calmar_ratio(ls_ret),
        "turnover": turnover(signal, top_n),
        "n_days": len(ls_ret),
        "passes_threshold": sharpe_ratio(ls_ret) > 1.4,
    }
