#!/usr/bin/env python3
"""Backfill decision_journal verdicts for unscored regime decisions.

Scoring logic:
- GROWTH/AGGRESSIVE decisions: HELPED if SPY went up in the scoring window,
  HARMED if down, NEUTRAL if flat (< 0.1% move).
- FRAGILE/DEFENSIVE decisions: HELPED if SPY fell (correctly defensive),
  HARMED if SPY rallied (missed upside), NEUTRAL if flat.
- CRISIS/CAPITAL_PRESERVATION: same as FRAGILE but with higher threshold.
- NEUTRAL/BALANCED: NEUTRAL unless SPY moved > 1% (then score directionally).
- FLYWHEEL/individual picks: marked INSUFFICIENT_DATA (no per-ticker scoring yet).

Uses the immutability-safe record_outcome() method from DecisionJournal.
Only scores decisions with at least 1 trading day of forward SPY data.
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger as log
from sqlalchemy import text

from db import get_engine
from journal.log import DecisionJournal

# Thresholds for scoring
FLAT_THRESHOLD = 0.001       # < 0.1% = flat
STRONG_MOVE_THRESHOLD = 0.01 # > 1% = strong move (for NEUTRAL scoring)
SCORING_WINDOW_DAYS = 3      # look 1-3 trading days ahead

# Regimes that imply bullish positioning
BULLISH_POSTURES = {"AGGRESSIVE"}
# Regimes that imply defensive positioning
DEFENSIVE_POSTURES = {"DEFENSIVE", "CAPITAL_PRESERVATION"}
# Regimes that are neutral
NEUTRAL_POSTURES = {"BALANCED", "HOLD", "CAUTIOUS"}
# Non-regime entries (individual picks)
INDIVIDUAL_STATES = {
    "EQUITY_VALUE", "BUYOUT_ARBITRAGE", "DISTRESSED_TURNAROUND",
    "CRYPTO_CORE", "CRYPTO_AI",
}


def _load_sp500_prices(engine) -> dict[date, float]:
    """Load SP500 daily prices keyed by date."""
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT rs.obs_date, rs.value "
            "FROM resolved_series rs "
            "JOIN feature_registry fr ON rs.feature_id = fr.id "
            "WHERE fr.name = 'sp500' "
            "ORDER BY rs.obs_date"
        )).fetchall()
    return {row[0]: float(row[1]) for row in rows}


def _load_vix_prices(engine) -> dict[date, float]:
    """Load VIX daily values keyed by date."""
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT rs.obs_date, rs.value "
            "FROM resolved_series rs "
            "JOIN feature_registry fr ON rs.feature_id = fr.id "
            "WHERE fr.name = 'vix' "
            "ORDER BY rs.obs_date"
        )).fetchall()
    return {row[0]: float(row[1]) for row in rows}


def _get_forward_return(
    prices: dict[date, float],
    decision_date: date,
    window_days: int = SCORING_WINDOW_DAYS,
) -> float | None:
    """Get the forward return from decision_date over the next window_days trading days.

    Returns the return as a decimal (e.g. 0.01 = +1%), or None if insufficient data.
    """
    # Find the closest price on or before decision_date
    sorted_dates = sorted(prices.keys())
    entry_price = None
    for d in sorted_dates:
        if d <= decision_date:
            entry_price = prices[d]
            entry_date = d
        else:
            break

    if entry_price is None:
        return None

    # Find the price window_days trading days later
    future_dates = [d for d in sorted_dates if d > entry_date]
    if not future_dates:
        return None

    # Use the closest available date up to window_days ahead
    exit_date = None
    for d in future_dates[:window_days]:
        exit_date = d

    if exit_date is None:
        return None

    exit_price = prices[exit_date]
    return (exit_price - entry_price) / entry_price


def _score_decision(
    inferred_state: str,
    grid_recommendation: str,
    action_taken: str,
    spy_return: float | None,
) -> tuple[str, float, str]:
    """Score a decision based on regime and forward SPY return.

    Returns (verdict, outcome_value, annotation).
    """
    # Individual stock picks -- can't score without per-ticker data
    if inferred_state in INDIVIDUAL_STATES:
        return (
            "INSUFFICIENT_DATA",
            0.0,
            f"Individual pick ({inferred_state}) -- per-ticker scoring not implemented",
        )

    if spy_return is None:
        return (
            "INSUFFICIENT_DATA",
            0.0,
            "No forward SPY data available for scoring window",
        )

    pct = spy_return * 100  # for annotation

    # Determine posture from grid_recommendation or action_taken
    posture = grid_recommendation
    if action_taken.startswith("AUTO_"):
        posture = action_taken.replace("AUTO_", "")

    # Bullish positioning (AGGRESSIVE)
    if posture in BULLISH_POSTURES:
        if spy_return > FLAT_THRESHOLD:
            return "HELPED", round(spy_return, 6), f"AGGRESSIVE correct: SPY +{pct:.2f}%"
        elif spy_return < -FLAT_THRESHOLD:
            return "HARMED", round(spy_return, 6), f"AGGRESSIVE wrong: SPY {pct:.2f}%"
        else:
            return "NEUTRAL", round(spy_return, 6), f"AGGRESSIVE flat: SPY {pct:+.2f}%"

    # Defensive positioning (DEFENSIVE, CAPITAL_PRESERVATION)
    if posture in DEFENSIVE_POSTURES:
        if spy_return < -FLAT_THRESHOLD:
            return "HELPED", round(-spy_return, 6), f"Defensive correct: SPY {pct:.2f}% (avoided loss)"
        elif spy_return > FLAT_THRESHOLD:
            return "HARMED", round(-spy_return, 6), f"Defensive wrong: SPY +{pct:.2f}% (missed upside)"
        else:
            return "NEUTRAL", 0.0, f"Defensive flat: SPY {pct:+.2f}%"

    # Neutral positioning (BALANCED, HOLD, CAUTIOUS)
    if posture in NEUTRAL_POSTURES:
        if abs(spy_return) > STRONG_MOVE_THRESHOLD:
            # Strong move means BALANCED missed an opportunity
            if spy_return > STRONG_MOVE_THRESHOLD:
                return "NEUTRAL", round(spy_return, 6), f"Balanced missed rally: SPY +{pct:.2f}%"
            else:
                return "HELPED", round(-spy_return, 6), f"Balanced avoided crash: SPY {pct:.2f}%"
        else:
            return "NEUTRAL", 0.0, f"Balanced correct: SPY {pct:+.2f}% (flat market)"

    # Unknown posture
    return "INSUFFICIENT_DATA", 0.0, f"Unknown posture '{posture}' -- cannot score"


def run(dry_run: bool = False) -> dict:
    """Backfill verdicts for all unscored decision_journal entries.

    Args:
        dry_run: If True, print what would be done without writing.

    Returns:
        Dict with counts of scored entries by verdict.
    """
    engine = get_engine()
    journal = DecisionJournal(db_engine=engine)

    # Load market data for scoring
    spy_prices = _load_sp500_prices(engine)
    vix_prices = _load_vix_prices(engine)
    log.info("Loaded {n} SPY prices, {m} VIX prices", n=len(spy_prices), m=len(vix_prices))

    if not spy_prices:
        log.error("No SPY price data available -- cannot score decisions")
        return {"error": "no SPY data"}

    # Get all unscored entries
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT id, decision_timestamp, inferred_state, grid_recommendation, "
            "action_taken "
            "FROM decision_journal "
            "WHERE outcome_recorded_at IS NULL "
            "ORDER BY decision_timestamp"
        )).fetchall()

    log.info("Found {n} unscored entries to evaluate", n=len(rows))

    counts = {"HELPED": 0, "HARMED": 0, "NEUTRAL": 0, "INSUFFICIENT_DATA": 0, "skipped": 0}

    for row in rows:
        entry_id = row[0]
        decision_ts = row[1]
        inferred_state = row[2]
        grid_rec = row[3]
        action_taken = row[4]

        decision_date = decision_ts.date() if hasattr(decision_ts, 'date') else decision_ts

        # Get forward return
        spy_return = _get_forward_return(spy_prices, decision_date)

        verdict, outcome_value, annotation = _score_decision(
            inferred_state, grid_rec, action_taken, spy_return,
        )

        if dry_run:
            log.info(
                "[DRY RUN] id={id} date={d} state={s} rec={r} -> {v} ({a})",
                id=entry_id, d=decision_date, s=inferred_state,
                r=grid_rec, v=verdict, a=annotation,
            )
            counts[verdict] += 1
            continue

        try:
            journal.record_outcome(
                decision_id=entry_id,
                outcome_value=outcome_value,
                verdict=verdict,
                annotation=annotation,
            )
            counts[verdict] += 1
            log.debug(
                "Scored id={id}: {v} (outcome={ov:.4f})",
                id=entry_id, v=verdict, ov=outcome_value,
            )
        except ValueError as exc:
            log.warning("Could not score id={id}: {e}", id=entry_id, e=str(exc))
            counts["skipped"] += 1

    log.info("=== BACKFILL COMPLETE ===")
    log.info("Results: {c}", c=counts)
    return counts


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Backfill decision_journal verdicts")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = parser.parse_args()

    result = run(dry_run=args.dry_run)
    print(f"\nResults: {result}")
