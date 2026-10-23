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

Price source
------------
Benchmarks are read from ``raw_series`` through ``store/observations.py``
(``pull_status = 'SUCCESS'`` only, latest vintage per ``obs_date``, one
explicit ``source_catalog`` name per series). They used to come from
``resolved_series`` joined on ``feature_registry.name``, whose ``sp500`` row
stopped at 2026-03-24 while ``YF:^GSPC:close`` kept updating; every decision
after that date was then scored ``INSUFFICIENT_DATA`` -- permanently, because
the journal is immutable. Measured on griddb 2026-10-23: 170 of the 174
decisions in the trailing 45 days were frozen that way.

Pending windows
---------------
A decision whose forward window has not elapsed yet is *not* written. It is
counted as ``pending`` and rescored on a later run, once the closes it needs
exist. ``INSUFFICIENT_DATA`` is only recorded when the window has fully
elapsed in wall-clock terms and the price feed is not merely lagging behind
the decision (see :func:`_window_pending`).
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger as log
from sqlalchemy import text

from db import get_engine
from ingestion.market_calendar import trading_days_between
from journal.log import DecisionJournal
from store.observations import read_window

# Thresholds for scoring
FLAT_THRESHOLD = 0.001       # < 0.1% = flat
STRONG_MOVE_THRESHOLD = 0.01 # > 1% = strong move (for NEUTRAL scoring)
SCORING_WINDOW_DAYS = 3      # look 1-3 trading days ahead

# Calendar days of price history read before the earliest unscored decision,
# so _get_forward_return can find an entry close on or before it.
PRICE_HISTORY_BUFFER_DAYS = 14

# Regimes that imply bullish positioning
BULLISH_POSTURES = {"AGGRESSIVE"}
# Regimes that imply defensive positioning
DEFENSIVE_POSTURES = {"DEFENSIVE", "CAPITAL_PRESERVATION"}
# Regimes that are neutral
NEUTRAL_POSTURES = {"BALANCED", "HOLD", "CAUTIOUS"}
# Non-regime entries — map to benchmark series for scoring
INDIVIDUAL_STATE_BENCHMARKS = {
    "EQUITY_VALUE": "iwm",         # Small-cap value → Russell 2000
    "BUYOUT_ARBITRAGE": "sp500",   # M&A arb → broad market
    "DISTRESSED_TURNAROUND": "hyg",  # Distressed → high yield bonds
    "CRYPTO_CORE": "btc",         # Crypto core → Bitcoin
    "CRYPTO_AI": "btc",           # Crypto AI → Bitcoin (no direct ETF)
}
INDIVIDUAL_STATES = set(INDIVIDUAL_STATE_BENCHMARKS.keys())

# Benchmark name -> (raw_series.series_id, source_catalog.name). Every read
# names its source explicitly because ``YF:*`` ids are written by more than
# one puller (store.observations.MixedSourceError). A benchmark absent from
# this table has no live series and is reported as unavailable (an empty
# price dict) -- it is never read from another table.
BENCHMARK_RAW_SERIES: dict[str, tuple[str, str]] = {
    "sp500": ("YF:^GSPC:close", "yfinance"),
    "vix": ("YF:^VIX:close", "yfinance"),
    "iwm": ("YF:IWM:close", "yfinance"),
    "hyg": ("YF:HYG:close", "yfinance"),
    "btc": ("YF:BTC-USD:close", "yfinance"),
}


def _load_benchmark_prices(
    engine,
    series_name: str,
    start: date | None = None,
    as_of: date | None = None,
) -> dict[date, float]:
    """Load a benchmark's daily closes keyed by date from ``raw_series``.

    Reads through :func:`store.observations.read_window`: SUCCESS rows only,
    one value per ``obs_date`` (latest vintage), bounded to ``[start, as_of]``.
    An unmapped benchmark name returns ``{}`` without touching the database.
    A database error propagates -- a failed read must abort the run rather
    than silently score decisions as INSUFFICIENT_DATA.
    """
    mapping = BENCHMARK_RAW_SERIES.get(series_name)
    if mapping is None:
        log.warning("No raw series mapped for benchmark {b}; unavailable", b=series_name)
        return {}
    series_id, source = mapping
    with engine.connect() as conn:
        observations = read_window(conn, series_id, source=source, start=start, as_of=as_of)
    return {o.obs_date: o.value for o in observations}


def _load_sp500_prices(
    engine, start: date | None = None, as_of: date | None = None,
) -> dict[date, float]:
    """Load SP500 daily prices keyed by date."""
    return _load_benchmark_prices(engine, "sp500", start=start, as_of=as_of)


def _load_vix_prices(
    engine, start: date | None = None, as_of: date | None = None,
) -> dict[date, float]:
    """Load VIX daily values keyed by date."""
    return _load_benchmark_prices(engine, "vix", start=start, as_of=as_of)


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


def _trading_days_after(start: date, end: date) -> int:
    """Number of US trading days in ``(start, end]``; 0 when ``end <= start``."""
    if end <= start:
        return 0
    return len(trading_days_between(start + timedelta(days=1), end))


def _window_pending(
    prices: dict[date, float],
    decision_date: date,
    today: date,
    window_days: int = SCORING_WINDOW_DAYS,
) -> bool:
    """True when forward closes for ``decision_date`` may still arrive.

    Called only after :func:`_get_forward_return` found no forward close.
    The window is pending -- so the decision is skipped, not written --
    while either holds:

    * fewer than or exactly ``window_days`` trading days have elapsed after
      ``decision_date`` as of ``today`` (the window has not elapsed yet; the
      last close it needs is pulled the evening of the ``window_days``-th
      trading day and seen by the next morning's run);
    * the latest available close is on or before ``decision_date`` and
      within ``window_days`` trading days of it (the feed has not caught up
      to the decision yet).

    Only when neither holds -- the window has elapsed and the feed was
    already more than a window behind when the decision was made -- is the
    data treated as genuinely missing and INSUFFICIENT_DATA recorded.
    """
    if _trading_days_after(decision_date, today) <= window_days:
        return True
    latest_close = max(prices) if prices else None
    if latest_close is not None and latest_close <= decision_date:
        return _trading_days_after(latest_close, decision_date) <= window_days
    return False


# Module-level cache for benchmark prices
_benchmark_cache: dict[str, dict[date, float]] = {}


def _score_decision(
    inferred_state: str,
    grid_recommendation: str,
    action_taken: str,
    spy_return: float | None,
    engine=None,
    decision_date: date | None = None,
) -> tuple[str, float, str]:
    """Score a decision based on regime and forward return.

    Returns (verdict, outcome_value, annotation).
    """
    # Individual strategy picks — score against strategy-specific benchmark
    if inferred_state in INDIVIDUAL_STATES and engine is not None and decision_date is not None:
        benchmark_name = INDIVIDUAL_STATE_BENCHMARKS[inferred_state]
        if benchmark_name not in _benchmark_cache:
            _benchmark_cache[benchmark_name] = _load_benchmark_prices(engine, benchmark_name)
        bench_prices = _benchmark_cache[benchmark_name]
        bench_return = _get_forward_return(bench_prices, decision_date)
        if bench_return is not None:
            pct = bench_return * 100
            posture = grid_recommendation
            if action_taken.startswith("AUTO_"):
                posture = action_taken.replace("AUTO_", "")
            if posture in BULLISH_POSTURES:
                if bench_return > FLAT_THRESHOLD:
                    return "HELPED", round(bench_return, 6), f"{inferred_state} AGGRESSIVE correct: {benchmark_name} +{pct:.2f}%"
                elif bench_return < -FLAT_THRESHOLD:
                    return "HARMED", round(bench_return, 6), f"{inferred_state} AGGRESSIVE wrong: {benchmark_name} {pct:.2f}%"
                return "NEUTRAL", round(bench_return, 6), f"{inferred_state} AGGRESSIVE flat: {benchmark_name} {pct:+.2f}%"
            elif posture in DEFENSIVE_POSTURES:
                if bench_return < -FLAT_THRESHOLD:
                    return "HELPED", round(-bench_return, 6), f"{inferred_state} defensive correct: {benchmark_name} {pct:.2f}%"
                elif bench_return > FLAT_THRESHOLD:
                    return "HARMED", round(-bench_return, 6), f"{inferred_state} defensive wrong: {benchmark_name} +{pct:.2f}%"
                return "NEUTRAL", 0.0, f"{inferred_state} defensive flat: {benchmark_name} {pct:+.2f}%"
            return "NEUTRAL", round(bench_return, 6), f"{inferred_state} balanced: {benchmark_name} {pct:+.2f}%"
        return ("INSUFFICIENT_DATA", 0.0, f"No benchmark data for {benchmark_name}")

    if inferred_state in INDIVIDUAL_STATES:
        return ("INSUFFICIENT_DATA", 0.0, f"Individual pick ({inferred_state}) — no engine for benchmark lookup")

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


def _fetch_unscored(engine) -> list:
    """All decision_journal rows without a recorded outcome, oldest first."""
    with engine.connect() as conn:
        return conn.execute(text(
            "SELECT id, decision_timestamp, inferred_state, grid_recommendation, "
            "action_taken "
            "FROM decision_journal "
            "WHERE outcome_recorded_at IS NULL "
            "ORDER BY decision_timestamp"
        )).fetchall()


def _decision_date(decision_ts) -> date:
    """UTC calendar date of a ``decision_timestamp`` (datetime, date or ISO string)."""
    if isinstance(decision_ts, datetime):
        return decision_ts.date()
    if isinstance(decision_ts, date):
        return decision_ts
    return date.fromisoformat(str(decision_ts)[:10])


def run(dry_run: bool = False, today: date | None = None) -> dict:
    """Backfill verdicts for all unscored decision_journal entries.

    Args:
        dry_run: If True, print what would be done without writing.
        today: The date the run is evaluated at (defaults to ``date.today()``).
            Decisions whose scoring window has not elapsed by this date are
            left unscored (``pending``) instead of written as INSUFFICIENT_DATA.

    Returns:
        Dict with counts of entries by verdict, plus ``pending`` (left for a
        later run), ``skipped`` (journal refused the write) and ``scored``
        (rows written, or that would be written under ``dry_run``).
    """
    # griddb runs Etc/UTC and decision_timestamp is UTC; evaluate the run on the UTC date.
    today = today or datetime.now(timezone.utc).date()
    engine = get_engine()
    journal = DecisionJournal(db_engine=engine)

    # Get all unscored entries first: they bound how much price history is read.
    rows = _fetch_unscored(engine)
    log.info("Found {n} unscored entries to evaluate", n=len(rows))

    start = None
    if rows:
        start = min(_decision_date(r[1]) for r in rows) - timedelta(days=PRICE_HISTORY_BUFFER_DAYS)

    # Load market data for scoring. The cache is per run: in the scheduler
    # process this module stays imported for days, and yesterday's closes
    # must not be reused.
    _benchmark_cache.clear()
    spy_prices = _load_sp500_prices(engine, start=start, as_of=today)
    vix_prices = _load_vix_prices(engine, start=start, as_of=today)
    _benchmark_cache["sp500"] = spy_prices
    _benchmark_cache["vix"] = vix_prices
    log.info("Loaded {n} SPY prices, {m} VIX prices", n=len(spy_prices), m=len(vix_prices))

    if not spy_prices:
        log.error("No SPY price data available -- cannot score decisions")
        return {"error": "no SPY data"}

    for state in {r[2] for r in rows} & INDIVIDUAL_STATES:
        benchmark = INDIVIDUAL_STATE_BENCHMARKS[state]
        if benchmark not in _benchmark_cache:
            _benchmark_cache[benchmark] = _load_benchmark_prices(
                engine, benchmark, start=start, as_of=today,
            )

    counts = {
        "HELPED": 0, "HARMED": 0, "NEUTRAL": 0, "INSUFFICIENT_DATA": 0,
        "pending": 0, "skipped": 0,
    }

    for row in rows:
        entry_id = row[0]
        decision_ts = row[1]
        inferred_state = row[2]
        grid_rec = row[3]
        action_taken = row[4]

        decision_date = _decision_date(decision_ts)

        # Get forward return
        spy_return = _get_forward_return(spy_prices, decision_date)

        # Which series this row is scored against, and whether it has a
        # forward close yet. Without one, leave the row for a later run
        # while the window is still open -- an INSUFFICIENT_DATA verdict
        # cannot be revised once written.
        if inferred_state in INDIVIDUAL_STATES:
            benchmark = INDIVIDUAL_STATE_BENCHMARKS[inferred_state]
            scoring_prices = _benchmark_cache.get(benchmark, {})
            has_forward = _get_forward_return(scoring_prices, decision_date) is not None
            can_pend = benchmark in BENCHMARK_RAW_SERIES  # unmapped never arrives
        else:
            scoring_prices = spy_prices
            has_forward = spy_return is not None
            can_pend = True

        if not has_forward and can_pend and _window_pending(scoring_prices, decision_date, today):
            counts["pending"] += 1
            log.debug(
                "Pending id={id} date={d} state={s}: scoring window not elapsed",
                id=entry_id, d=decision_date, s=inferred_state,
            )
            continue

        verdict, outcome_value, annotation = _score_decision(
            inferred_state, grid_rec, action_taken, spy_return,
            engine=engine, decision_date=decision_date,
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

    counts["scored"] = sum(counts[v] for v in ("HELPED", "HARMED", "NEUTRAL", "INSUFFICIENT_DATA"))
    if counts["INSUFFICIENT_DATA"]:
        latest = max(spy_prices)
        log.warning(
            "{n} decisions recorded INSUFFICIENT_DATA with the SPY feed last at {d}; "
            "those verdicts are permanent",
            n=counts["INSUFFICIENT_DATA"], d=latest,
        )
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
