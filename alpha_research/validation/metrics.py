"""
Portfolio metrics for alpha research validation.

Key finding from QuantaAlpha v2:
  - Validation RankIC does NOT predict OOS RankIC (R² = 0.00)
  - Validation net Sharpe DOES predict OOS net Sharpe (R² = 0.42)
  - Minimum threshold: val net Sharpe > 1.4 to expect positive OOS
  - Budget for 64% RankIC shrinkage from validation to OOS
"""

from __future__ import annotations

from typing import Any

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


COVERAGE_COMPLETE = "complete"
COVERAGE_PARTIAL = "partial"
COVERAGE_NONE = "none"
ESTIMATE_FULL_BOOK = "full_book"
ESTIMATE_CONDITIONAL = "conditional_on_observed_outcomes"
# The net series is a cost-ledger accounting, not daily executable P&L: an
# unscored date books no P&L and its turnover cost lands on the next scored
# date (see long_short_book).
NET_BASIS_CARRIED_COST = "carried_cost_accounting"


def mature_through(calendar: pd.Index, outcome_horizon: int = 0, as_of: Any = None) -> Any:
    """Last decision date whose forward outcome is due by ``as_of``, or ``None``.

    Read from the outcome calendar (``forward_returns.index``, the row
    dates), never from outcome values. A decision row is mature once
    ``outcome_horizon`` further calendar rows have elapsed by ``as_of``
    (default: the calendar's last row). ``outcome_horizon=0`` takes every
    calendar row up to ``as_of`` as mature: the caller declares the panel
    holds mature rows only. A horizon cannot be dated past the calendar's
    last row, so ``as_of`` after it raises when ``outcome_horizon > 0``.
    """
    if int(outcome_horizon) != outcome_horizon or outcome_horizon < 0:
        raise ValueError("outcome_horizon must be a non-negative integer")
    cal = calendar.unique().sort_values()
    if as_of is not None:
        if isinstance(cal, pd.DatetimeIndex):
            as_of = pd.Timestamp(as_of)
        if outcome_horizon and len(cal) and as_of > cal[-1]:
            raise ValueError(
                "as_of is after the outcome calendar's last row: the calendar "
                "cannot date an outcome horizon past its end"
            )
        cal = cal[cal <= as_of]
    n_mature = len(cal) - int(outcome_horizon)
    return cal[n_mature - 1] if n_mature > 0 else None


def eligible_panels(
    signal: pd.DataFrame, forward_returns: pd.DataFrame, through: Any
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """The signal and outcome rows dated on or before ``through`` (none if ``None``)."""
    if through is None:
        return signal.iloc[:0], forward_returns.iloc[:0]
    return (
        signal.loc[signal.index <= through],
        forward_returns.loc[forward_returns.index <= through],
    )


def long_short_book(
    signal: pd.DataFrame,
    forward_returns: pd.DataFrame,
    top_n: int = 5,
    cost_bps: float = 10.0,
    *,
    outcome_horizon: int = 0,
    as_of: Any = None,
) -> pd.DataFrame:
    """Per decision date: the frozen long-short book, its cost ledger, net return and coverage.

    The book (top ``top_n`` long, bottom ``top_n`` short) is picked from
    ``signal.loc[t]`` alone, the universe known at the decision, before any
    forward return is read. A book member whose outcome goes missing
    (delisted, halted, data gap) stays in the book and is never replaced by
    the next-ranked name: its outcome is counted as missing, not imputed.
    Each leg returns the equal-weight mean over its members with an observed
    outcome (a conditional estimate when some are missing, see
    :func:`outcome_coverage`); a leg with none leaves the date unscored
    (``gross`` and ``return`` NaN). ``coverage`` is observed book slots /
    book slots and ``missing`` names the members without one.

    Cost ledger: every formed book pays its round-trip turnover cost against
    the previous formed book on its own date (``cost``), scored or not, so
    the previous membership never advances without its rebalance being
    charged. Rule for an unscored date: its cost is carried, never dropped.
    It is netted from the next scored date's return, so ``cost_charged`` is
    that date's own cost plus every cost carried since the last scored date
    (0.0 on an unscored date) and ``return = gross - cost_charged``. A cost
    after the last scored date has no net return to land on; it stays
    pending and :func:`outcome_coverage` reports it as
    ``turnover_cost_uncharged`` with ``net_coverage`` partial.

    Eligibility window: chronological and declared, never read from which
    outcomes are observed. The decision dates are the signal dates on or
    before :func:`mature_through` of the outcome calendar, ``outcome_horizon``
    and ``as_of``. Each one forms a book and pays its cost, scored or not: a
    date with no observed outcome at all (first, interior or last) is
    unscored, never dropped. Only the later signal dates (an immature tail,
    or after ``as_of``) form no book and none of their outcomes is read;
    ``book.attrs`` records ``outcome_horizon``, ``mature_through`` and
    ``n_immature_excluded``. With the default ``outcome_horizon=0`` an
    undeclared immature tail counts as missing outcomes (partial coverage):
    a panel built as ``shift(-h)`` declares ``outcome_horizon=h``.

    The net ``return`` is carried-cost accounting
    (:data:`NET_BASIS_CARRIED_COST`), not daily executable P&L.

    Contract: ``forward_returns`` must already be aligned to the decision row
    (i.e. ``forward_returns.loc[t]`` is the forward return realized after the
    signal at date ``t`` is known, such as ``prices.pct_change(h).shift(-h)``).
    This function does NOT shift ``forward_returns``.
    """
    through = mature_through(forward_returns.index, outcome_horizon, as_of)
    if through is None:
        eligible = np.zeros(len(signal.index), dtype=bool)
    else:
        eligible = np.asarray(signal.index <= through, dtype=bool)
    dates = signal.index[eligible]
    fwd = forward_returns.reindex(index=dates)

    rows = []
    prev_longs: set = set()
    prev_shorts: set = set()
    pending_cost = 0.0  # ledger cost of unscored dates not yet netted from a return

    for dt in dates:
        sig_row = signal.loc[dt].dropna()
        if len(sig_row) < 2 * top_n:
            continue

        ranked = sig_row.sort_values()
        shorts = list(ranked.index[:top_n])
        longs = list(ranked.index[-top_n:])

        # Cost ledger: this rebalance's turnover, charged on its own date.
        long_turnover = len(set(longs) - prev_longs) / max(top_n, 1)
        short_turnover = len(set(shorts) - prev_shorts) / max(top_n, 1)
        avg_turnover = (long_turnover + short_turnover) / 2
        cost = avg_turnover * cost_bps * 2 / 10_000  # round-trip

        out_row = fwd.loc[dt]  # outcomes are read only for the frozen book
        long_out, short_out = out_row.reindex(longs), out_row.reindex(shorts)

        gross_ret = net_ret = float("nan")
        charged = 0.0
        if long_out.notna().any() and short_out.notna().any():
            gross_ret = float(long_out.mean() - short_out.mean())  # mean skips missing
            charged = cost + pending_cost
            pending_cost = 0.0
            net_ret = gross_ret - charged
        else:
            pending_cost += cost

        n_observed = int(long_out.notna().sum() + short_out.notna().sum())
        missing = tuple(n for n, v in [*long_out.items(), *short_out.items()] if pd.isna(v))
        rows.append((
            dt, net_ret, gross_ret, cost, charged,
            2 * top_n, n_observed, n_observed / (2 * top_n), missing,
        ))

        prev_longs = set(longs)
        prev_shorts = set(shorts)

    cols = [
        "date", "return", "gross", "cost", "cost_charged",
        "n_book", "n_observed", "coverage", "missing",
    ]
    book = pd.DataFrame(rows, columns=cols).set_index("date")
    book.attrs.update(
        outcome_horizon=int(outcome_horizon),
        mature_through=through,
        n_immature_excluded=int((~eligible).sum()),
    )
    return book


def outcome_coverage(book: pd.DataFrame) -> dict[str, float | int | str | None]:
    """Coverage, labels and cost-ledger totals of a :func:`long_short_book`.

    ``gross_coverage`` is ``"complete"`` when every book slot on every date
    has an observed outcome, ``"partial"`` otherwise and ``"none"`` when no
    book was formed. ``net_coverage`` is the same, and also ``"partial"``
    while any ledger cost is uncharged. ``estimate`` is ``"full_book"`` only
    for a complete book; otherwise ``"conditional_on_observed_outcomes"``:
    leg means are renormalized over the observed members (``n_partial_days``
    scored dates) and unscored dates are absent, so the performance is
    conditional on the outcomes that were observed. ``turnover_cost_total``
    is the ledger sum, ``turnover_cost_carried`` the unscored-date costs
    netted from a later scored return and ``turnover_cost_uncharged`` the
    unscored-date costs after the last scored date.

    ``net_basis`` is ``"carried_cost_accounting"``: a net Sharpe on this
    series is not daily executable P&L. ``net_pass_blocked_reason`` is
    ``None`` only for a full-book net series (``net_coverage`` complete, no
    uncharged cost); otherwise it says why no ROBUST verdict or net-threshold
    pass may rest on it. ``outcome_horizon``, ``mature_through`` and
    ``n_immature_excluded`` label the eligibility window (the immature tail
    left out by declaration).
    """
    n_slots = int(book["n_book"].sum())
    n_observed = int(book["n_observed"].sum())
    scored = book["return"].notna().to_numpy()
    full = (book["n_observed"] == book["n_book"]).to_numpy()
    unscored_cost = np.where(scored, 0.0, book["cost"].to_numpy(dtype=float))
    scored_pos = np.flatnonzero(scored)
    after_last = np.arange(len(book)) > (scored_pos[-1] if len(scored_pos) else -1)
    uncharged = float(unscored_cost[after_last].sum())

    if n_slots == 0:
        gross_label = COVERAGE_NONE
    elif n_observed == n_slots:
        gross_label = COVERAGE_COMPLETE
    else:
        gross_label = COVERAGE_PARTIAL
    net_label = COVERAGE_PARTIAL if uncharged > 0.0 else gross_label
    estimate = ESTIMATE_FULL_BOOK if gross_label == COVERAGE_COMPLETE else ESTIMATE_CONDITIONAL
    n_unscored = int((~scored).sum())
    n_partial = int((scored & ~full).sum())
    blocked = None
    if net_label != COVERAGE_COMPLETE or uncharged > 0.0:
        blocked = (
            f"net coverage {net_label} ({n_unscored} unscored and {n_partial} partial "
            f"book dates, turnover cost uncharged {uncharged:.6g}): the net Sharpe "
            f"is {estimate}, not a full-book net figure"
        )
    through = book.attrs.get("mature_through")
    return {
        "outcome_coverage": n_observed / n_slots if n_slots else 0.0,
        "n_missing_outcomes": n_slots - n_observed,
        "n_unscored_days": n_unscored,
        "n_partial_days": n_partial,
        "gross_coverage": gross_label,
        "net_coverage": net_label,
        "estimate": estimate,
        "net_basis": NET_BASIS_CARRIED_COST,
        "net_pass_blocked_reason": blocked,
        "turnover_cost_total": float(book["cost"].to_numpy(dtype=float).sum()),
        "turnover_cost_carried": float(unscored_cost[~after_last].sum()),
        "turnover_cost_uncharged": uncharged,
        "outcome_horizon": book.attrs.get("outcome_horizon"),
        "mature_through": None if through is None else str(through),
        "n_immature_excluded": int(book.attrs.get("n_immature_excluded", 0)),
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
    unscored dates are dropped and their turnover cost is netted from the
    next scored return (carried-cost accounting, not daily executable P&L).
    Eligibility window: :func:`long_short_book` with its default
    ``outcome_horizon=0``. Coverage labels: :func:`outcome_coverage`.
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
    *,
    outcome_horizon: int = 0,
    as_of: Any = None,
) -> dict:
    """Compute full metrics suite for a signal.

    Gross and net figures come from one frozen book over the eligibility
    window (``outcome_horizon``, ``as_of``: :func:`long_short_book`) and
    carry its coverage labels (see :func:`outcome_coverage`); rank IC and
    turnover read only the eligible dates. ``sharpe_net`` is carried-cost
    accounting (``net_basis``), not daily executable P&L. ``passes_threshold``
    needs a full-book net series: a partial or uncharged book never passes,
    and ``net_pass_blocked_reason`` says why.
    """
    book = long_short_book(
        signal, forward_returns, top_n, cost_bps, outcome_horizon=outcome_horizon, as_of=as_of
    )
    signal, forward_returns = eligible_panels(
        signal, forward_returns, book.attrs["mature_through"]
    )
    ic = rank_ic(signal, forward_returns)
    ls_ret = book["return"].dropna().astype(float)
    coverage = outcome_coverage(book)
    sharpe_net = sharpe_ratio(ls_ret)

    return {
        "mean_rank_ic": float(ic.mean()) if len(ic) > 0 else 0.0,
        "rank_icir": rank_icir(ic),
        "sharpe_gross": sharpe_ratio(book["gross"].dropna().astype(float)),
        "sharpe_net": sharpe_net,
        "annualized_return": annualized_return(ls_ret),
        "max_drawdown": max_drawdown(ls_ret),
        "calmar": calmar_ratio(ls_ret),
        "turnover": turnover(signal, top_n),
        "n_days": len(ls_ret),
        "passes_threshold": sharpe_net > 1.4 and coverage["net_pass_blocked_reason"] is None,
        **coverage,
    }
