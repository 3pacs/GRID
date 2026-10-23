#!/usr/bin/env python3
"""Backfill decision_journal verdicts for unscored regime decisions.

Scoring logic:
- GROWTH/AGGRESSIVE decisions: HELPED if SPY went up in the scoring window,
  HARMED if down, NEUTRAL if flat (< 0.1% move).
- FRAGILE/DEFENSIVE decisions: HELPED if SPY fell (correctly defensive),
  HARMED if SPY rallied (missed upside), NEUTRAL if flat.
- CRISIS/CAPITAL_PRESERVATION: same as FRAGILE but with higher threshold.
- NEUTRAL/BALANCED: NEUTRAL unless SPY moved > 1% (then score directionally).
- FLYWHEEL/individual picks: scored against their own benchmark
  (INDIVIDUAL_STATE_BENCHMARKS), INSUFFICIENT_DATA when none is mapped.

Uses the immutability-safe record_outcome() method from DecisionJournal.

Price basis (declared; see price_close_contract.py)
---------------------------------------------------
Every close is a *completed-close capture* read through
``store.observations.read_daily_policy_n``: a ``raw_series`` ``SUCCESS`` row
whose payload carries the capture policy for its series (``spy_close_v1`` for
``YF:SPY:close``, ``yf_daily_capture_v1`` for the other benchmarks) and whose
pull happened after the label day ended. Provisional captures (``INCOMPLETE``,
``UNKNOWN``, unmarked rows, FAILED/PARTIAL/QUARANTINED pulls) are never an
entry or an exit. Values are yfinance raw daily closes (``YF:<ticker>:close``,
unadjusted); no corporate-action adjustment is applied.

* **Entry** -- the newest completed close whose pull timestamp is at or before
  the decision timestamp (point-in-time: the close GRID actually had when the
  decision was logged; a later re-pull of that date is ignored), at most
  ``ENTRY_MAX_AGE_DAYS`` calendar days before the decision.
* **Exit** -- the close labelled exactly ``SCORING_WINDOW_DAYS`` sessions
  after the decision's *anchor session*: the last session whose close had
  become capturable at the decision instant (the contract's own capture
  boundary -- UTC day end for SPY and BTC, the next New York midnight for
  other US-listed tickers). Sessions are NYSE sessions
  (``ingestion.market_calendar``) for US-listed benchmarks and UTC calendar
  days for BTC. Anchoring to the decision rather than to the entry label
  means the horizon always ends after the decision, even when the entry is
  stale (a feed four days behind must not score a window that closed before
  the decision was made). The exit value is the latest vintage known at
  evaluation time (``evaluated_at``, one instant per run).
* **Evidence** -- every scored annotation records the series, contract and
  source, the entry label/value/pull timestamp and the decision instant it
  was known at, the anchor session, the exit label/value/pull timestamp and
  the full-precision evaluation cutoff, so the exact vintages selected can
  be reconstructed from the journal alone.
* **Deferral** -- when the exit session's close is not captured yet the row
  is left unscored (``pending``) and rescored on a later run. A missing exit
  close is deferred while the puller can still catch it up
  (``GAP_GRACE_DAYS``); after that, if the feed has a later completed close,
  the session is a gap and the row is INSUFFICIENT_DATA; if the feed has
  nothing later either, the row stays pending (a stalled feed is not proof
  that the close never existed, and the verdict cannot be revised once
  written). Never shift to the next available label.

History: the scorer read SPY from ``resolved_series`` joined on
``feature_registry.name = 'sp500'``, whose last row is 2026-03-24, so every
later decision was written INSUFFICIENT_DATA ~16 h after it was made --
permanently (measured on griddb 2026-10-23: 170 of the 174 decisions in the
trailing 45 days). VIX was loaded and never used; it is not a completed-close
series under the contract and is no longer read.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger as log
from sqlalchemy import text

from db import get_engine
from ingestion.market_calendar import is_market_open
from journal.log import DecisionJournal
from price_close_contract import (
    DAILY_CAPTURE_CONTRACT,
    SPY_CAPTURE_MAX_LOOKBACK_DAYS,
    SPY_CLOSE_CONTRACT,
    SPY_CLOSE_SERIES,
    SPY_ENTRY_MAX_AGE_DAYS,
    daily_capture_boundary,
    observation_end_utc,
)
from store.observations import Observation, read_daily_policy_n

# Thresholds for scoring
FLAT_THRESHOLD = 0.001       # < 0.1% = flat
STRONG_MOVE_THRESHOLD = 0.01 # > 1% = strong move (for NEUTRAL scoring)
SCORING_WINDOW_DAYS = 3      # exit = the close 3 sessions after the entry session

# Entry must be a completed close at most this many calendar days before the
# decision (price_close_contract.SPY_ENTRY_RULE companion constant).
ENTRY_MAX_AGE_DAYS = SPY_ENTRY_MAX_AGE_DAYS
# A missing exit close is deferred for as long as the puller's operational
# catch-up can still land it; after that it is a gap.
GAP_GRACE_DAYS = SPY_CAPTURE_MAX_LOOKBACK_DAYS
# Calendar days of history searched for the entry close.
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
REGIME_BENCHMARK = "sp500"

US_LISTED_SESSIONS = "us_listed"   # NYSE sessions per ingestion.market_calendar
UTC_24H_SESSIONS = "utc_24h"       # every UTC calendar day (crypto)


@dataclass(frozen=True)
class Benchmark:
    """A benchmark's completed-close series and its session calendar."""

    series_id: str
    source: str
    contract: str   # price_contract_version its completed captures carry
    sessions: str   # US_LISTED_SESSIONS or UTC_24H_SESSIONS


# A benchmark absent from this table has no completed-close series and is
# scored INSUFFICIENT_DATA; it is never read from another table. The source
# is explicit because ``YF:*`` ids are written by more than one puller
# (store.observations.MixedSourceError). ``sp500`` is YF:SPY:close rather
# than YF:^GSPC:close: SPY carries the dedicated, year-unbounded
# ``spy_close_v1`` capture contract, while the generic ``yf_daily_capture_v1``
# boundary for US-listed tickers is audited for 2026 only
# (price_close_contract.daily_capture_boundary).
BENCHMARKS: dict[str, Benchmark] = {
    "sp500": Benchmark(SPY_CLOSE_SERIES, "yfinance", SPY_CLOSE_CONTRACT, US_LISTED_SESSIONS),
    "iwm": Benchmark("YF:IWM:close", "yfinance", DAILY_CAPTURE_CONTRACT, US_LISTED_SESSIONS),
    "hyg": Benchmark("YF:HYG:close", "yfinance", DAILY_CAPTURE_CONTRACT, US_LISTED_SESSIONS),
    "btc": Benchmark("YF:BTC-USD:close", "yfinance", DAILY_CAPTURE_CONTRACT, UTC_24H_SESSIONS),
}


def _as_utc(value: datetime | date | str) -> datetime:
    """A decision timestamp as an aware UTC datetime.

    Naive datetimes are taken as UTC (griddb runs Etc/UTC); a bare date is
    the start of that UTC day; ISO strings (SQLite) are parsed.
    """
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.combine(value, time.min, timezone.utc)


def _is_session(d: date, sessions: str) -> bool:
    return True if sessions == UTC_24H_SESSIONS else is_market_open(d)


def _sessions_after(start: date, n: int, sessions: str) -> list[date]:
    """The ``n`` session dates strictly after ``start`` in calendar order."""
    out: list[date] = []
    d = start
    while len(out) < n:
        d += timedelta(days=1)
        if _is_session(d, sessions):
            out.append(d)
    return out


def _capture_boundary(bench: Benchmark, session: date) -> datetime | None:
    """UTC instant after which a capture of ``session``'s close is a completed close.

    The contract's own boundary: UTC day end for ``YF:SPY:close``
    (``spy_close_v1``) and BTC, the following New York midnight for other
    US-listed tickers. ``None`` when ``session`` is not a session the
    contract knows (weekend, NYSE holiday, unsupported year).
    """
    if bench.series_id == SPY_CLOSE_SERIES:
        return observation_end_utc(session) if is_market_open(session) else None
    ticker = bench.series_id.split(":")[1]
    return daily_capture_boundary(ticker, session, "1d")


def _anchor_session(bench: Benchmark, decision_ts: datetime) -> date | None:
    """The last session whose close had become capturable by ``decision_ts``."""
    d = decision_ts.date()
    for _ in range(PRICE_HISTORY_BUFFER_DAYS):
        boundary = _capture_boundary(bench, d)
        if boundary is not None and boundary <= decision_ts:
            return d
        d -= timedelta(days=1)
    return None


def _entry_close(conn, bench: Benchmark, decision_ts: datetime) -> Observation | None:
    """Newest completed close known at ``decision_ts``, or None when too old / absent."""
    decision_date = decision_ts.date()
    known = read_daily_policy_n(
        conn, bench.series_id, 1,
        start=decision_date - timedelta(days=PRICE_HISTORY_BUFFER_DAYS),
        source=bench.source, as_of=decision_date, as_of_ts=decision_ts,
    )
    if not known:
        return None
    entry = known[0]
    if (decision_date - entry.obs_date).days > ENTRY_MAX_AGE_DAYS:
        return None
    return entry


def _closes_after(
    conn, bench: Benchmark, entry_date: date, evaluated_at: datetime,
) -> dict[date, Observation]:
    """Completed closes after ``entry_date`` as known at ``evaluated_at`` (latest vintage)."""
    rows = read_daily_policy_n(
        conn, bench.series_id, 10_000,
        start=entry_date + timedelta(days=1),
        source=bench.source, as_of=evaluated_at.date(), as_of_ts=evaluated_at,
    )
    return {o.obs_date: o for o in rows}


@dataclass(frozen=True)
class ForwardOutcome:
    """What the benchmark did after a decision, or why that is not known yet."""

    status: str                 # "scored" | "pending" | "missing"
    forward_return: float | None
    entry: Observation | None
    anchor: date | None
    exit_session: date | None
    exit: Observation | None
    reason: str


def _forward_outcome(
    conn, bench: Benchmark, decision_ts: datetime, evaluated_at: datetime,
    window_days: int = SCORING_WINDOW_DAYS,
) -> ForwardOutcome:
    """Apply the declared entry/exit/deferral rules (module docstring)."""
    anchor = _anchor_session(bench, decision_ts)
    if anchor is None:
        return ForwardOutcome(
            "missing", None, None, None, None, None,
            f"no capturable {bench.series_id} session before {decision_ts.isoformat()}",
        )
    entry = _entry_close(conn, bench, decision_ts)
    if entry is None:
        return ForwardOutcome(
            "missing", None, None, anchor, None, None,
            f"no completed {bench.series_id} close known at "
            f"{decision_ts.isoformat()} within {ENTRY_MAX_AGE_DAYS}d",
        )
    # A completed close known at the decision has a label at or before the
    # anchor, so the window [entry label, exit session] always ends after
    # the decision: the exit is forward of it by construction.
    exit_session = _sessions_after(anchor, window_days, bench.sessions)[-1]
    later = _closes_after(conn, bench, entry.obs_date, evaluated_at)
    exit_obs = later.get(exit_session)
    if exit_obs is not None:
        fwd = (exit_obs.value - entry.value) / entry.value
        return ForwardOutcome("scored", fwd, entry, anchor, exit_session, exit_obs, "")
    if evaluated_at.date() <= exit_session + timedelta(days=GAP_GRACE_DAYS):
        return ForwardOutcome(
            "pending", None, entry, anchor, exit_session, None,
            f"completed close for session {exit_session} not captured yet",
        )
    resumed = sorted(d for d in later if d > exit_session)
    if resumed:
        return ForwardOutcome(
            "missing", None, entry, anchor, exit_session, None,
            f"no completed {bench.series_id} close for session {exit_session}; "
            f"feed resumed at {resumed[0]}",
        )
    return ForwardOutcome(
        "pending", None, entry, anchor, exit_session, None,
        f"feed has no completed {bench.series_id} close after session {exit_session} (stalled)",
    )


def _iso(ts: datetime | None) -> str:
    return ts.isoformat() if ts is not None else "unknown"


def _basis(bench: Benchmark, outcome: ForwardOutcome, decision_ts: datetime, evaluated_at: datetime) -> str:
    """Provenance suffix recorded with every scored verdict.

    Full precision throughout: values as stored (``repr``), the selected
    entry and exit captures' pull timestamps, the decision instant the entry
    was known at, the anchor session and the exact evaluation cutoff -- so
    which vintage was selected can be reconstructed from the journal alone.
    """
    entry, exit_obs = outcome.entry, outcome.exit
    assert entry is not None and exit_obs is not None
    return (
        f"basis {bench.series_id} ({bench.contract}, source {bench.source}): "
        f"entry {entry.obs_date} close {entry.value!r} pulled {_iso(entry.pull_timestamp)} "
        f"known_at {_iso(decision_ts)}; anchor session {outcome.anchor}; "
        f"exit session {exit_obs.obs_date} close {exit_obs.value!r} pulled {_iso(exit_obs.pull_timestamp)}; "
        f"evaluated_at {_iso(evaluated_at)}"
    )


def _score_decision(
    inferred_state: str,
    grid_recommendation: str,
    action_taken: str,
    forward_return: float | None,
    benchmark_name: str = REGIME_BENCHMARK,
) -> tuple[str, float, str]:
    """Score a decision based on regime and forward return.

    ``forward_return`` is the benchmark's return over the scoring window
    (``None`` when unavailable); for individual picks it is the return of
    ``benchmark_name``. Returns (verdict, outcome_value, annotation).
    """
    # Individual strategy picks — score against strategy-specific benchmark
    if inferred_state in INDIVIDUAL_STATES:
        bench_return = forward_return
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

    spy_return = forward_return
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


def run(dry_run: bool = False, evaluated_at: datetime | None = None) -> dict:
    """Backfill verdicts for all unscored decision_journal entries.

    Args:
        dry_run: If True, print what would be done without writing.
        evaluated_at: The instant the run evaluates at (default: now, UTC).
            It is the vintage cutoff for exit closes and the clock for the
            pending/gap rules; one value is used for the whole run.

    Returns:
        Dict with counts of entries by verdict, plus ``pending`` (left for a
        later run), ``skipped`` (journal refused the write) and ``scored``
        (rows written, or that would be written under ``dry_run``).
    """
    evaluated_at = _as_utc(evaluated_at) if evaluated_at else datetime.now(timezone.utc)
    engine = get_engine()
    journal = DecisionJournal(db_engine=engine)

    rows = _fetch_unscored(engine)
    log.info("Found {n} unscored entries to evaluate (as of {t})", n=len(rows), t=evaluated_at.isoformat())

    counts = {
        "HELPED": 0, "HARMED": 0, "NEUTRAL": 0, "INSUFFICIENT_DATA": 0,
        "pending": 0, "skipped": 0,
    }
    stalled: set[str] = set()

    with engine.connect() as conn:
        for row in rows:
            entry_id = row[0]
            decision_ts = _as_utc(row[1])
            inferred_state = row[2]
            grid_rec = row[3]
            action_taken = row[4]

            benchmark_name = INDIVIDUAL_STATE_BENCHMARKS.get(inferred_state, REGIME_BENCHMARK)
            bench = BENCHMARKS.get(benchmark_name)
            if bench is None:
                outcome = ForwardOutcome(
                    "missing", None, None, None, None, None,
                    f"no completed-close series mapped for benchmark {benchmark_name}",
                )
            else:
                outcome = _forward_outcome(conn, bench, decision_ts, evaluated_at)

            # Leave the row for a later run while its outcome can still
            # arrive -- an INSUFFICIENT_DATA verdict cannot be revised.
            if outcome.status == "pending":
                counts["pending"] += 1
                if "stalled" in outcome.reason and bench is not None:
                    stalled.add(bench.series_id)
                log.debug(
                    "Pending id={id} at={t} state={s}: {r}",
                    id=entry_id, t=decision_ts.isoformat(), s=inferred_state, r=outcome.reason,
                )
                continue

            verdict, outcome_value, annotation = _score_decision(
                inferred_state, grid_rec, action_taken, outcome.forward_return,
                benchmark_name=benchmark_name,
            )
            if outcome.status == "scored" and bench is not None:
                annotation = f"{annotation} | {_basis(bench, outcome, decision_ts, evaluated_at)}"
            else:
                annotation = f"{annotation} ({outcome.reason})"

            if dry_run:
                log.info(
                    "[DRY RUN] id={id} at={t} state={s} rec={r} -> {v} ({a})",
                    id=entry_id, t=decision_ts.isoformat(), s=inferred_state,
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
    if stalled:
        log.warning(
            "Completed-close feed stalled for {s}: decisions stay pending until it resumes",
            s=sorted(stalled),
        )
    if counts["INSUFFICIENT_DATA"]:
        log.warning(
            "{n} decisions recorded INSUFFICIENT_DATA; those verdicts are permanent",
            n=counts["INSUFFICIENT_DATA"],
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
