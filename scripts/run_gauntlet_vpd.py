#!/usr/bin/env python3
"""
Run the full 5-test False Discovery Gauntlet on vol_price_divergence.

Tests: permutation, deflated Sharpe, subsample stability, decay, CV consistency.

The evaluator gets an explicit session calendar (the pinned XNYS manifests,
independent of the price rows) and every decision date on it: no date is
dropped because its outcomes are missing (Codex HOLD 6032280271).
"""

from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from loguru import logger as log

_GRID_DIR = str(Path(__file__).resolve().parent.parent)
os.chdir(_GRID_DIR)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

from db import get_engine
from alpha_research.data.panel_builder import build_price_panel
from alpha_research.signals.quanta_alpha import vol_price_divergence
from alpha_research.validation.gauntlet import run_gauntlet
from alpha_research.validation.metrics import compute_signal_metrics
from intelligence.regime.market_state import xnys_sessions

# forward_returns[t] is the next session's return: the last session's outcome
# is not yet due on the decision clock.
OUTCOME_HORIZON = 1


def session_calendar(start: date, today: date) -> pd.DatetimeIndex:
    """Pinned XNYS sessions from ``start`` through the last session before ``today``.

    The evaluator's explicit calendar comes from the hash-pinned manifests
    (:func:`intelligence.regime.market_state.xnys_sessions`), never from the
    price panel's rows, so a session the panel lacks is a missing outcome and
    not a shorter calendar. Its last session, whose close is known by
    ``today``, is the decision clock. Raises outside the pinned coverage.
    """
    first, last, sessions, _ = xnys_sessions()
    if start < first or today > last + timedelta(days=1):
        raise ValueError(f"{start}..{today} is outside the pinned XNYS calendar ({first}..{last})")
    days = [d for d in sessions if start <= d < today]
    if not days:
        raise ValueError(f"no XNYS session in {start}..{today}")
    return pd.DatetimeIndex(days)


def evaluation_panels(
    prices: pd.DataFrame, calendar: pd.DatetimeIndex
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(signal, forward_returns) on ``calendar``'s sessions; no date is dropped for its outcomes.

    The signal is computed on the panel's own rows and kept on the sessions
    where it has a value (decision-time data only). The one-session forward
    return is read on the calendar: a session the panel lacks is a NaN price,
    so the decision before it has a missing outcome, counted by the
    evaluator, never a two-session return.
    """
    signal = vol_price_divergence(prices).reindex(calendar)
    signal = signal.loc[signal.notna().any(axis=1)]
    forward_returns = prices.reindex(calendar).pct_change(fill_method=None).shift(-1)
    return signal, forward_returns.reindex(index=signal.index)


def main() -> None:
    engine = get_engine()
    end_date = date.today()
    start_date = end_date - timedelta(days=365 * 3)
    calendar = session_calendar(start_date, end_date)

    log.info("Loading price panel {s} → {e}...", s=start_date, e=end_date)
    prices = build_price_panel(engine, start_date=start_date, end_date=end_date)
    if prices.empty:
        log.error("No price data")
        return

    prices = prices.ffill(limit=5)
    log.info("Panel: {r} rows × {c} tickers", r=len(prices), c=len(prices.columns))

    # Signal and 1-session forward returns on the calendar's sessions
    log.info("Computing vol_price_divergence signal...")
    signal, forward_returns = evaluation_panels(prices, calendar)

    log.info("Signal computed: {n} dates, {t} tickers", n=len(signal), t=len(signal.columns))

    # Quick metrics first
    log.info("")
    log.info("QUICK METRICS:")
    metrics = compute_signal_metrics(
        signal, forward_returns, outcome_horizon=OUTCOME_HORIZON, calendar=calendar
    )
    log.info("  Mean RankIC:     {ic:.4f}", ic=metrics["mean_rank_ic"])
    log.info("  ICIR:            {ir:.4f}", ir=metrics["rank_icir"])
    log.info("  Net Sharpe:      {s:.4f}", s=metrics["sharpe_net"])
    log.info("  Ann. Return:     {r:.4f}", r=metrics["annualized_return"])
    log.info("  Avg Turnover:    {t:.4f}", t=metrics["turnover"])
    log.info(
        "  Net coverage:    {c} ({u} unscored dates, calendar {b})",
        c=metrics["net_coverage"], u=metrics["n_unscored_days"], b=metrics["calendar_basis"],
    )
    log.info("")

    # Run gauntlet (this takes a while — 1000 permutations)
    log.info("Running 5-test False Discovery Gauntlet (1000 permutations)...")
    log.info("This will take 2-5 minutes...")
    result = run_gauntlet(
        signal,
        forward_returns,
        n_models_tested=4,  # we tested 4 signals
        top_n=5,
        cost_bps=10.0,
        n_permutations=1000,
        n_subsample_splits=20,
        outcome_horizon=OUTCOME_HORIZON,
        calendar=calendar,
    )

    log.info("")
    log.info("=" * 60)
    log.info("GAUNTLET RESULT: {v}", v=result.verdict)
    log.info("=" * 60)
    log.info("")
    log.info("1. PERMUTATION TEST:")
    log.info("   p-value: {p:.4f} (threshold: < 0.05)", p=result.permutation_p)
    log.info("   Passed:  {ok}", ok=result.permutation_passed)
    log.info("")
    log.info("2. DEFLATED SHARPE RATIO:")
    log.info("   Observed Sharpe:  {s:.4f}", s=result.observed_sharpe)
    log.info("   Threshold:        {t:.4f}", t=result.deflated_threshold)
    log.info("   Passed:           {ok}", ok=result.deflated_sharpe_passed)
    log.info("")
    log.info("3. SUBSAMPLE STABILITY:")
    log.info("   Stability: {s:.1f}% (threshold: > 50%)", s=result.subsample_stability * 100)
    log.info("   Passed:    {ok}", ok=result.subsample_passed)
    log.info("")
    log.info("4. DECAY ANALYSIS:")
    log.info("   Monotonic: {ok}", ok=result.decay_monotonic)
    if "decay_ics" in result.details:
        for h, ic in result.details["decay_ics"].items():
            log.info("   Horizon {h}d: IC={ic:.4f}", h=h, ic=ic)
    log.info("")
    log.info("5. CV CONSISTENCY:")
    log.info("   Consistency: {c:.1f}% (threshold: >= 75%)", c=result.cv_consistency * 100)
    log.info("   Passed:      {ok}", ok=result.cv_passed)
    log.info("")
    log.info("Net Sharpe: {s:.4f} (threshold > 1.4 for production: {ok})",
             s=result.details.get("net_sharpe", 0),
             ok=result.details.get("passes_net_sharpe_threshold", False))
    if result.details.get("net_pass_blocked_reason"):
        log.info("Net pass blocked: {r}", r=result.details["net_pass_blocked_reason"])
    log.info("")
    log.info("VERDICT: {v}", v=result.verdict)


if __name__ == "__main__":
    main()
