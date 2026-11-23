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

    Contract: ``forward_returns`` must already be aligned to the decision row
    (i.e. ``forward_returns.loc[t]`` is the forward return realized after the
    signal at date ``t`` is known, such as ``prices.pct_change(h).shift(-h)``).
    At default ``horizon=1`` no extra shift is applied; ``horizon > 1`` shifts
    by ``-(horizon - 1)`` to measure decay at further lead steps.
    """
    if int(horizon) != horizon or horizon < 1:
        raise ValueError("horizon must be a positive integer")
    fwd = forward_returns if horizon == 1 else forward_returns.shift(-(horizon - 1))

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


def long_short_book(
    signal: pd.DataFrame,
    forward_returns: pd.DataFrame,
    top_n: int = 5,
    cost_bps: float = 10.0,
) -> pd.DataFrame:
    """Per decision date: the frozen long-short book, its net return and outcome coverage.

    The book (top ``top_n`` long, bottom ``top_n`` short) is picked from
    ``signal.loc[t]`` alone, the universe known at the decision, before any
    forward return is read. A book member whose outcome goes missing
    (delisted, halted, data gap) stays in the book and is never replaced by
    the next-ranked name: its outcome is counted as missing, not imputed.
    Each leg returns the equal-weight mean over its members with an observed
    outcome; a leg with none leaves the date unscored (``return`` NaN).
    ``coverage`` is observed book slots / book slots and ``missing`` names
    the members without one. Turnover cost is charged on the frozen book.
    Rows with no realized forward return at all (not yet scorable) are skipped.

    Contract: ``forward_returns`` must already be aligned to the decision row
    (i.e. ``forward_returns.loc[t]`` is the forward return realized after the
    signal at date ``t`` is known, such as ``prices.pct_change(h).shift(-h)``).
    This function does NOT shift ``forward_returns``.
    """
    fwd = forward_returns
    common_dates = signal.index.intersection(fwd.dropna(how="all").index)

    rows = []
    prev_longs = set()
    prev_shorts = set()

    for dt in common_dates:
        sig_row = signal.loc[dt].dropna()
        if len(sig_row) < 2 * top_n:
            continue

        ranked = sig_row.sort_values()
        shorts = list(ranked.index[:top_n])
        longs = list(ranked.index[-top_n:])
        out_row = fwd.loc[dt]  # outcomes are read only for the frozen book
        long_out, short_out = out_row.reindex(longs), out_row.reindex(shorts)

        net_ret = float("nan")
        if long_out.notna().any() and short_out.notna().any():
            gross_ret = long_out.mean() - short_out.mean()  # mean skips missing outcomes

            # Transaction costs: turnover-based
            long_turnover = len(set(longs) - prev_longs) / max(top_n, 1)
            short_turnover = len(set(shorts) - prev_shorts) / max(top_n, 1)
            avg_turnover = (long_turnover + short_turnover) / 2
            cost = avg_turnover * cost_bps * 2 / 10_000  # round-trip
            net_ret = float(gross_ret - cost)

        n_observed = int(long_out.notna().sum() + short_out.notna().sum())
        missing = tuple(n for n, v in [*long_out.items(), *short_out.items()] if pd.isna(v))
        rows.append((dt, net_ret, 2 * top_n, n_observed, n_observed / (2 * top_n), missing))

        prev_longs = set(longs)
        prev_shorts = set(shorts)

    cols = ["date", "return", "n_book", "n_observed", "coverage", "missing"]
    return pd.DataFrame(rows, columns=cols).set_index("date")


def outcome_coverage(book: pd.DataFrame) -> dict[str, float | int]:
    """Coverage of a :func:`long_short_book` (0.0 when no book was formed)."""
    n_slots = int(book["n_book"].sum())
    n_observed = int(book["n_observed"].sum())
    return {
        "outcome_coverage": n_observed / n_slots if n_slots else 0.0,
        "n_missing_outcomes": n_slots - n_observed,
        "n_unscored_days": int(book["return"].isna().sum()),
    }


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
    The book is frozen at the decision and a missing outcome is never
    reselected (rule and alignment contract: :func:`long_short_book`);
    unscored dates are dropped.
    """
    ls = long_short_book(signal, forward_returns, top_n, cost_bps)["return"].dropna()
    return ls.astype(float) if len(ls) else pd.Series(dtype=float, name="ls_return")


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
    book = long_short_book(signal, forward_returns, top_n, cost_bps)
    ls_ret = book["return"].dropna().astype(float)

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
        **outcome_coverage(book),
    }
