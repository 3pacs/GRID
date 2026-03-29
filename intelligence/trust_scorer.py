"""
GRID Intelligence — Source Trust Scoring Framework.

Tracks every signal source (congressional traders, insiders, dark pool feeds,
social accounts, scanners) by comparing what they said vs what actually happened.
Sources that are consistently right get amplified; sources that are consistently
wrong get dampened (or flipped as contrarian indicators).

Pipeline:
  1. score_pending_signals  — evaluate unscored signals against actual price moves
  2. update_trust_scores    — recompute trust scores using Bayesian updating + recency
  3. detect_convergence     — find multi-source agreement on signal_type
  4. generate_trust_report  — ranked leaderboard + convergence events + narrative
  5. run_trust_cycle        — orchestrate the full loop for hermes_operator

The trust score uses a Bayesian beta-distribution prior:
    trust = (hits + 1) / (hits + misses + 2)
This shrinks toward 0.5 with few observations and converges to the true rate
as evidence accumulates. Recent signals are exponentially weighted (half-life 90 days).
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ──────────────────────────────────────────────────────────────

# How many days a signal must age before scoring
EVALUATION_WINDOWS: dict[str, int] = {
    "congressional": 30,
    "insider": 14,
    "darkpool": 5,
    "social": 5,
    "scanner": 7,
    "foreign_lobbying": 45,    # FARA lobbying → policy change lag is long
    "geopolitical": 7,         # GDELT tension spikes → rapid market impact
    "diplomatic_cable": 30,    # Declassified cables → contextual, slower
    "lobbying": 30,            # Domestic lobbying → policy change lag
    "campaign_finance": 60,    # PAC contributions → election cycle lag
    "offshore_leak": 14,       # Offshore leak exposure → reputation impact
}

# Minimum price move to count as a signal_typeal outcome
MOVE_THRESHOLD_PCT: float = 1.0

# Recency weighting half-life in days
RECENCY_HALF_LIFE_DAYS: int = 90

# Convergence: minimum independent sources pointing the same way
MIN_CONVERGENCE_SOURCES: int = 3

# Lookback windows for insider edge queries
CONGRESSIONAL_LOOKBACK_DAYS: int = 45
INSIDER_LOOKBACK_DAYS: int = 30
DARKPOOL_LOOKBACK_DAYS: int = 7


# ── Data Classes ───────────────────────────────────────────────────────────

@dataclass
class SourceScore:
    """Trust profile for a single signal source."""

    source_type: str        # 'congressional', 'insider', 'darkpool', 'social', 'scanner'
    source_id: str          # member name, insider name, account handle
    trust_score: float      # 0-1, starts at 0.5 (Bayesian prior)
    hit_count: int
    miss_count: int
    total_signals: int
    win_rate: float
    avg_lead_time_hours: float   # how early before the move
    avg_return_on_hits: float
    best_ticker: str             # what they are most accurate on
    last_signal_date: str
    rank: int


@dataclass
class ConvergenceEvent:
    """Multiple independent sources agreeing on signal_type for a ticker."""

    ticker: str
    signal_type: str          # 'BUY' or 'SELL'
    source_count: int
    sources: list[dict]     # [{source_type, source_id, trust_score, signal_date}]
    combined_confidence: float
    detected_at: str


# ── Table Setup ────────────────────────────────────────────────────────────

def _ensure_tables(engine: Engine) -> None:
    """Create signal_sources table if it does not exist."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS signal_sources (
                id              BIGSERIAL PRIMARY KEY,
                source_type     TEXT NOT NULL,
                source_id       TEXT NOT NULL,
                ticker          TEXT NOT NULL,
                signal_type       TEXT NOT NULL CHECK (signal_type IN ('BUY', 'SELL')),
                signal_date     TIMESTAMPTZ NOT NULL,
                signal_value DOUBLE PRECISION,
                metadata        JSONB,

                -- Scoring fields (filled after evaluation)
                outcome         TEXT DEFAULT 'PENDING'
                                    CHECK (outcome IN ('PENDING', 'CORRECT', 'WRONG', 'EXPIRED')),
                outcome_return  DOUBLE PRECISION,
                scored_at       TIMESTAMPTZ,

                -- Trust aggregates (updated by update_trust_scores)
                trust_score     DOUBLE PRECISION DEFAULT 0.5,
                hit_count       INTEGER DEFAULT 0,
                miss_count      INTEGER DEFAULT 0,
                avg_lead_time_hours DOUBLE PRECISION DEFAULT 0.0,

                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_signal_sources_pending
                ON signal_sources (outcome, signal_date)
                WHERE outcome = 'PENDING'
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_signal_sources_type_id
                ON signal_sources (source_type, source_id)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_signal_sources_ticker
                ON signal_sources (ticker, signal_date DESC)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_signal_sources_trust
                ON signal_sources (trust_score DESC)
        """))
    log.debug("signal_sources table ensured")


# ── Price Helpers ──────────────────────────────────────────────────────────

def _get_price_near_date(
    engine: Engine, ticker: str, target_date: date,
) -> float | None:
    """Get closing price at or near *target_date*.

    Checks (in order):
      1. options_daily_signals.spot_price
      2. raw_series YF close data
      3. yfinance live fetch (last resort)
    """
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT spot_price FROM options_daily_signals
            WHERE ticker = :t AND signal_date <= :d AND spot_price > 0
            ORDER BY signal_date DESC LIMIT 1
        """), {"t": ticker, "d": target_date}).fetchone()
        if row:
            return float(row[0])

        row = conn.execute(text("""
            SELECT value FROM raw_series
            WHERE series_id = :sid AND obs_date <= :d AND pull_status = 'SUCCESS'
            ORDER BY obs_date DESC LIMIT 1
        """), {"sid": f"YF:{ticker}:close", "d": target_date}).fetchone()
        if row:
            return float(row[0])

    # Last resort — live yfinance
    return _fetch_yfinance_price(ticker, target_date)


def _fetch_yfinance_price(ticker: str, target_date: date) -> float | None:
    """Fetch price from yfinance. Graceful degradation if unavailable."""
    try:
        import yfinance as yf

        start = target_date - timedelta(days=5)
        end = target_date + timedelta(days=5)
        df = yf.download(ticker, start=str(start), end=str(end), progress=False)
        if df.empty:
            return None
        valid = df[df.index.date <= target_date]  # type: ignore[union-attr]
        if valid.empty:
            valid = df
        close_col = "Close"
        if close_col not in valid.columns:
            return None
        return float(valid[close_col].iloc[-1])
    except Exception as exc:
        log.debug("yfinance fallback failed for {t}: {e}", t=ticker, e=str(exc))
        return None


# ── 1. Score Pending Signals ──────────────────────────────────────────────

def score_pending_signals(engine: Engine) -> dict[str, Any]:
    """Evaluate pending signals against actual price moves.

    For each pending signal whose evaluation window has elapsed:
      - Fetch actual price change from signal_date to now.
      - BUY + price up >1%  -> CORRECT
      - SELL + price down >1% -> CORRECT
      - Otherwise -> WRONG
      - Signals too old (>90 days pending) -> EXPIRED

    Returns summary dict with counts.
    """
    _ensure_tables(engine)
    now = datetime.now(timezone.utc)
    today = date.today()

    summary: dict[str, Any] = {
        "scored": 0, "correct": 0, "wrong": 0, "expired": 0,
        "skipped_no_price": 0,
    }

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT id, source_type, source_id, ticker, signal_type,
                   signal_date, signal_value
            FROM signal_sources
            WHERE outcome = 'PENDING'
            ORDER BY signal_date
        """)).fetchall()

        if not rows:
            log.info("No pending signals to score")
            return summary

        log.info("Evaluating {n} pending signals", n=len(rows))

        for r in rows:
            sig_id, src_type, src_id, ticker, signal_type, signal_date, signal_value = r

            # Determine evaluation window for this source type
            eval_days = EVALUATION_WINDOWS.get(src_type, 7)
            signal_dt = signal_date if isinstance(signal_date, date) else signal_date.date()

            # Not enough time has passed yet
            if (today - signal_dt).days < eval_days:
                continue

            # Expire very old signals (>90 days)
            if (today - signal_dt).days > 90:
                conn.execute(text("""
                    UPDATE signal_sources
                    SET outcome = 'EXPIRED', scored_at = :now
                    WHERE id = :id
                """), {"id": sig_id, "now": now})
                summary["expired"] += 1
                continue

            # Get price at signal time if not stored
            entry_price = float(signal_value) if signal_value else None
            if entry_price is None:
                entry_price = _get_price_near_date(engine, ticker, signal_dt)
            if entry_price is None or entry_price <= 0:
                summary["skipped_no_price"] += 1
                continue

            # Get current/evaluation price
            eval_date = signal_dt + timedelta(days=eval_days)
            if eval_date > today:
                eval_date = today
            current_price = _get_price_near_date(engine, ticker, eval_date)
            if current_price is None:
                summary["skipped_no_price"] += 1
                continue

            # Compute return
            pct_change = (current_price - entry_price) / entry_price * 100.0

            # Classify
            if signal_type == "BUY" and pct_change > MOVE_THRESHOLD_PCT:
                outcome = "CORRECT"
                summary["correct"] += 1
            elif signal_type == "SELL" and pct_change < -MOVE_THRESHOLD_PCT:
                outcome = "CORRECT"
                summary["correct"] += 1
            else:
                outcome = "WRONG"
                summary["wrong"] += 1

            conn.execute(text("""
                UPDATE signal_sources
                SET outcome = :outcome,
                    outcome_return = :ret,
                    scored_at = :now,
                    signal_value = COALESCE(signal_value, :entry)
                WHERE id = :id
            """), {
                "outcome": outcome,
                "ret": round(pct_change, 4),
                "now": now,
                "entry": entry_price,
                "id": sig_id,
            })
            summary["scored"] += 1

    log.info(
        "Signal scoring complete: {s} scored ({c} correct, {w} wrong, {e} expired), "
        "{sk} skipped (no price)",
        s=summary["scored"], c=summary["correct"], w=summary["wrong"],
        e=summary["expired"], sk=summary["skipped_no_price"],
    )
    return summary


# ── 2. Update Trust Scores ────────────────────────────────────────────────

def update_trust_scores(engine: Engine) -> dict[str, Any]:
    """Recompute trust scores for every unique (source_type, source_id).

    For each source:
      - Count hits/misses with recency-weighted exponential decay (half-life 90 days).
      - Bayesian trust: (weighted_hits + 1) / (weighted_hits + weighted_misses + 2)
      - Compute average lead time and average return on correct signals.
      - Find best ticker (highest hit rate per ticker).
      - Update aggregates in signal_sources rows.

    Returns ranked list of source dicts.
    """
    _ensure_tables(engine)
    now = datetime.now(timezone.utc)
    today = date.today()
    decay_lambda = math.log(2) / RECENCY_HALF_LIFE_DAYS

    sources_updated: list[dict[str, Any]] = []

    with engine.connect() as conn:
        # Get all unique sources that have scored signals
        source_keys = conn.execute(text("""
            SELECT DISTINCT source_type, source_id
            FROM signal_sources
            WHERE outcome IN ('CORRECT', 'WRONG')
        """)).fetchall()

    if not source_keys:
        log.info("No scored sources to update trust for")
        return {"sources": [], "total": 0}

    log.info("Updating trust scores for {n} sources", n=len(source_keys))

    with engine.begin() as conn:
        for src_type, src_id in source_keys:
            rows = conn.execute(text("""
                SELECT outcome, outcome_return, signal_date, ticker
                FROM signal_sources
                WHERE source_type = :st AND source_id = :si
                  AND outcome IN ('CORRECT', 'WRONG')
                ORDER BY signal_date DESC
            """), {"st": src_type, "si": src_id}).fetchall()

            if not rows:
                continue

            weighted_hits = 0.0
            weighted_misses = 0.0
            raw_hits = 0
            raw_misses = 0
            hit_returns: list[float] = []
            lead_times: list[float] = []
            ticker_hits: dict[str, int] = {}
            ticker_totals: dict[str, int] = {}
            last_signal_date = None

            for outcome, ret, sig_date, ticker in rows:
                sig_dt = sig_date if isinstance(sig_date, date) else sig_date.date()

                if last_signal_date is None:
                    last_signal_date = sig_dt

                # Recency weight: exponential decay from today
                days_ago = (today - sig_dt).days
                weight = math.exp(-decay_lambda * days_ago)

                ticker_totals[ticker] = ticker_totals.get(ticker, 0) + 1

                if outcome == "CORRECT":
                    weighted_hits += weight
                    raw_hits += 1
                    ticker_hits[ticker] = ticker_hits.get(ticker, 0) + 1
                    if ret is not None:
                        hit_returns.append(float(ret))
                else:
                    weighted_misses += weight
                    raw_misses += 1

            # Bayesian trust score with Laplace smoothing
            trust = (weighted_hits + 1.0) / (weighted_hits + weighted_misses + 2.0)

            total_signals = raw_hits + raw_misses
            win_rate = raw_hits / total_signals if total_signals > 0 else 0.0
            avg_return = sum(hit_returns) / len(hit_returns) if hit_returns else 0.0

            # Best ticker: highest hit rate with at least 2 signals
            best_ticker = ""
            best_ticker_rate = 0.0
            for t, total in ticker_totals.items():
                if total >= 2:
                    rate = ticker_hits.get(t, 0) / total
                    if rate > best_ticker_rate:
                        best_ticker_rate = rate
                        best_ticker = t

            # Average lead time: hours between signal and the move
            # (approximated as eval_window * 24 / 2 for now; refined when we
            #  have intraday timestamps)
            eval_window = EVALUATION_WINDOWS.get(src_type, 7)
            avg_lead = eval_window * 24.0 / 2.0

            sources_updated.append({
                "source_type": src_type,
                "source_id": src_id,
                "trust_score": round(trust, 4),
                "hit_count": raw_hits,
                "miss_count": raw_misses,
                "total_signals": total_signals,
                "win_rate": round(win_rate, 4),
                "avg_lead_time_hours": round(avg_lead, 1),
                "avg_return_on_hits": round(avg_return, 4),
                "best_ticker": best_ticker,
                "last_signal_date": str(last_signal_date) if last_signal_date else "",
            })

            # Propagate aggregates to all rows for this source
            conn.execute(text("""
                UPDATE signal_sources
                SET trust_score = :ts,
                    hit_count = :hc,
                    miss_count = :mc,
                    avg_lead_time_hours = :alt
                WHERE source_type = :st AND source_id = :si
            """), {
                "ts": round(trust, 4),
                "hc": raw_hits,
                "mc": raw_misses,
                "alt": round(avg_lead, 1),
                "st": src_type,
                "si": src_id,
            })

    # Rank by trust score descending
    sources_updated.sort(key=lambda s: -s["trust_score"])
    for rank, s in enumerate(sources_updated, 1):
        s["rank"] = rank

    log.info(
        "Trust scores updated for {n} sources. "
        "Top: {top_id} ({top_ts:.3f}), Bottom: {bot_id} ({bot_ts:.3f})",
        n=len(sources_updated),
        top_id=sources_updated[0]["source_id"] if sources_updated else "N/A",
        top_ts=sources_updated[0]["trust_score"] if sources_updated else 0,
        bot_id=sources_updated[-1]["source_id"] if sources_updated else "N/A",
        bot_ts=sources_updated[-1]["trust_score"] if sources_updated else 0,
    )

    return {"sources": sources_updated, "total": len(sources_updated)}


# ── 3. Get Trusted Sources ────────────────────────────────────────────────

def get_trusted_sources(
    engine: Engine,
    min_signals: int = 5,
    min_trust: float = 0.6,
) -> list[SourceScore]:
    """Return sources above the trust threshold with enough history.

    This is the primary query for the recommendation engine: only amplify
    signals from sources that have proven themselves.

    Args:
        engine: SQLAlchemy engine.
        min_signals: Minimum scored signals required.
        min_trust: Minimum trust_score to qualify.

    Returns:
        List of SourceScore sorted by trust_score descending.
    """
    _ensure_tables(engine)

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT source_type, source_id,
                   trust_score, hit_count, miss_count,
                   avg_lead_time_hours,
                   MAX(signal_date) AS last_signal,
                   COUNT(*) AS total_signals
            FROM signal_sources
            WHERE outcome IN ('CORRECT', 'WRONG')
            GROUP BY source_type, source_id, trust_score, hit_count,
                     miss_count, avg_lead_time_hours
            HAVING (hit_count + miss_count) >= :min_sig
               AND trust_score >= :min_trust
            ORDER BY trust_score DESC
        """), {"min_sig": min_signals, "min_trust": min_trust}).fetchall()

    results: list[SourceScore] = []
    for rank, r in enumerate(rows, 1):
        src_type, src_id, trust, hits, misses, lead_time, last_sig, total = r
        total_scored = (hits or 0) + (misses or 0)
        wr = (hits or 0) / total_scored if total_scored > 0 else 0.0

        results.append(SourceScore(
            source_type=src_type,
            source_id=src_id,
            trust_score=float(trust or 0.5),
            hit_count=int(hits or 0),
            miss_count=int(misses or 0),
            total_signals=int(total or 0),
            win_rate=round(wr, 4),
            avg_lead_time_hours=float(lead_time or 0.0),
            avg_return_on_hits=0.0,  # filled from update_trust_scores cache
            best_ticker="",          # filled from update_trust_scores cache
            last_signal_date=str(last_sig) if last_sig else "",
            rank=rank,
        ))

    log.info(
        "Trusted sources: {n} sources meet criteria (min_signals={ms}, min_trust={mt})",
        n=len(results), ms=min_signals, mt=min_trust,
    )
    return results


# ── 4. Get Insider Edge ───────────────────────────────────────────────────

def get_insider_edge(engine: Engine, ticker: str) -> dict[str, Any] | None:
    """Check for intelligence signals on a specific ticker.

    Queries recent congressional trades, insider filings, and dark pool
    activity for the given ticker, weighted by source trust scores.

    Returns structured dict or None if no intelligence is available.
    """
    _ensure_tables(engine)
    edge: dict[str, Any] = {
        "ticker": ticker,
        "congressional": [],
        "insider": [],
        "darkpool": [],
        "has_signal": False,
    }

    with engine.connect() as conn:
        # Congressional trades (last 45 days)
        cong_rows = conn.execute(text("""
            SELECT source_id, signal_type, signal_date, signal_value,
                   trust_score, outcome, outcome_return, metadata
            FROM signal_sources
            WHERE source_type = 'congressional'
              AND ticker = :t
              AND signal_date >= NOW() - INTERVAL ':days days'
            ORDER BY signal_date DESC
        """.replace(":days", str(CONGRESSIONAL_LOOKBACK_DAYS))), {"t": ticker}).fetchall()

        for r in cong_rows:
            edge["congressional"].append({
                "member": r[0],
                "signal_type": r[1],
                "date": str(r[2]),
                "price": float(r[3]) if r[3] else None,
                "trust_score": float(r[4]) if r[4] else 0.5,
                "outcome": r[5],
                "return": float(r[6]) if r[6] else None,
                "metadata": r[7],
            })

        # Insider filings (last 30 days)
        insider_rows = conn.execute(text("""
            SELECT source_id, signal_type, signal_date, signal_value,
                   trust_score, outcome, outcome_return, metadata
            FROM signal_sources
            WHERE source_type = 'insider'
              AND ticker = :t
              AND signal_date >= NOW() - INTERVAL ':days days'
            ORDER BY signal_date DESC
        """.replace(":days", str(INSIDER_LOOKBACK_DAYS))), {"t": ticker}).fetchall()

        for r in insider_rows:
            edge["insider"].append({
                "insider": r[0],
                "signal_type": r[1],
                "date": str(r[2]),
                "price": float(r[3]) if r[3] else None,
                "trust_score": float(r[4]) if r[4] else 0.5,
                "outcome": r[5],
                "return": float(r[6]) if r[6] else None,
                "metadata": r[7],
            })

        # Dark pool unusual volume (last 7 days)
        dp_rows = conn.execute(text("""
            SELECT source_id, signal_type, signal_date, signal_value,
                   trust_score, outcome, outcome_return, metadata
            FROM signal_sources
            WHERE source_type = 'darkpool'
              AND ticker = :t
              AND signal_date >= NOW() - INTERVAL ':days days'
            ORDER BY signal_date DESC
        """.replace(":days", str(DARKPOOL_LOOKBACK_DAYS))), {"t": ticker}).fetchall()

        for r in dp_rows:
            edge["darkpool"].append({
                "source": r[0],
                "signal_type": r[1],
                "date": str(r[2]),
                "price": float(r[3]) if r[3] else None,
                "trust_score": float(r[4]) if r[4] else 0.5,
                "outcome": r[5],
                "return": float(r[6]) if r[6] else None,
                "metadata": r[7],
            })

    has_signal = bool(
        edge["congressional"] or edge["insider"] or edge["darkpool"]
    )
    edge["has_signal"] = has_signal

    if not has_signal:
        return None

    # Compute aggregate signal_typeal signal weighted by trust
    buy_weight = 0.0
    sell_weight = 0.0
    for category in ("congressional", "insider", "darkpool"):
        for sig in edge[category]:
            ts = sig.get("trust_score", 0.5)
            if sig["signal_type"] == "BUY":
                buy_weight += ts
            else:
                sell_weight += ts

    edge["net_signal_type"] = "BUY" if buy_weight > sell_weight else "SELL"
    edge["signal_type_confidence"] = round(
        max(buy_weight, sell_weight) / (buy_weight + sell_weight)
        if (buy_weight + sell_weight) > 0 else 0.5,
        4,
    )

    log.info(
        "Insider edge for {t}: {c} congressional, {i} insider, {d} darkpool — "
        "net {dir} (confidence {conf:.1%})",
        t=ticker,
        c=len(edge["congressional"]),
        i=len(edge["insider"]),
        d=len(edge["darkpool"]),
        dir=edge["net_signal_type"],
        conf=edge["signal_type_confidence"],
    )
    return edge


# ── 5. Detect Convergence ─────────────────────────────────────────────────

def detect_convergence(
    engine: Engine,
    ticker: str | None = None,
) -> list[dict[str, Any]]:
    """Find tickers where 3+ independent source types agree on signal_type.

    A convergence event is the highest-conviction signal the system produces.
    E.g., congressional BUY + insider cluster buy + bullish dark pool = convergence.

    Args:
        engine: SQLAlchemy engine.
        ticker: Optional — limit search to a single ticker.

    Returns:
        List of convergence event dicts with combined confidence.
    """
    _ensure_tables(engine)
    events: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    # Look at recent signals (last 14 days) that are still relevant
    params: dict[str, Any] = {"lookback": date.today() - timedelta(days=14)}
    ticker_filter = ""
    if ticker:
        ticker_filter = "AND ticker = :ticker"
        params["ticker"] = ticker

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT ticker, source_type, source_id, signal_type,
                   signal_date, trust_score
            FROM signal_sources
            WHERE signal_date >= :lookback
              AND outcome IN ('PENDING', 'CORRECT')
              {ticker_filter}
            ORDER BY ticker, signal_date DESC
        """), params).fetchall()

    if not rows:
        return events

    # Group by ticker
    ticker_signals: dict[str, list[dict]] = {}
    for r in rows:
        t = r[0]
        ticker_signals.setdefault(t, []).append({
            "source_type": r[1],
            "source_id": r[2],
            "signal_type": r[3],
            "signal_date": str(r[4]),
            "trust_score": float(r[5]) if r[5] else 0.5,
        })

    for t, signals in ticker_signals.items():
        # Count distinct source types per signal_type
        buy_sources: dict[str, dict] = {}
        sell_sources: dict[str, dict] = {}

        for sig in signals:
            st = sig["source_type"]
            # Only count one signal per source_type (most recent)
            if sig["signal_type"] == "BUY" and st not in buy_sources:
                buy_sources[st] = sig
            elif sig["signal_type"] == "SELL" and st not in sell_sources:
                sell_sources[st] = sig

        # Check for convergence (3+ independent source types)
        for signal_type, sources_map in [("BUY", buy_sources), ("SELL", sell_sources)]:
            if len(sources_map) >= MIN_CONVERGENCE_SOURCES:
                # Combined confidence = weighted average of trust scores
                trust_sum = sum(s["trust_score"] for s in sources_map.values())
                combined = trust_sum / len(sources_map)

                events.append({
                    "ticker": t,
                    "signal_type": signal_type,
                    "source_count": len(sources_map),
                    "sources": [
                        {
                            "source_type": st,
                            "source_id": s["source_id"],
                            "trust_score": s["trust_score"],
                            "signal_date": s["signal_date"],
                        }
                        for st, s in sources_map.items()
                    ],
                    "combined_confidence": round(combined, 4),
                    "detected_at": now.isoformat(),
                })

    # Sort by combined confidence descending
    events.sort(key=lambda e: -e["combined_confidence"])

    log.info(
        "Convergence detection: {n} events found across {t} tickers",
        n=len(events), t=len(set(e["ticker"] for e in events)) if events else 0,
    )

    # Push convergence alerts to connected WebSocket clients
    try:
        from api.main import broadcast_event
        for ev in events:
            sources_desc = ", ".join(
                f"{s['source_type']}({s['source_id']})"
                for s in ev.get("sources", [])
            )
            broadcast_event("alert", {
                "severity": "high",
                "message": (
                    f"Convergence: {ev['source_count']} sources "
                    f"{ev['signal_type']} on {ev['ticker']} — {sources_desc}"
                ),
                "ticker": ev["ticker"],
                "signal_type": ev["signal_type"],
                "source_count": ev["source_count"],
                "combined_confidence": ev["combined_confidence"],
            })
    except Exception:
        pass  # graceful degradation if API module not loaded

    return events


# ── 6. Generate Trust Report ──────────────────────────────────────────────

def generate_trust_report(engine: Engine) -> str:
    """Generate a human-readable trust scoring report.

    Includes:
      - Top 10 most trusted sources across all types
      - Bottom 10 (consistently wrong — contrarian indicator candidates)
      - Convergence events in last 7 days
      - LLM narrative if available
    """
    _ensure_tables(engine)

    # Get all scored sources
    trust_result = update_trust_scores(engine)
    all_sources = trust_result.get("sources", [])

    top_10 = all_sources[:10]
    bottom_10 = list(reversed(all_sources[-10:])) if len(all_sources) >= 10 else list(reversed(all_sources))

    # Convergence events
    convergence = detect_convergence(engine)
    recent_convergence = [
        e for e in convergence
        if e.get("detected_at", "") >= (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    ]

    # Build report
    lines = [
        "===============================================================",
        "  GRID Intelligence — Source Trust Report",
        "===============================================================",
        "",
        f"Total tracked sources: {len(all_sources)}",
        f"Report generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
    ]

    # Top trusted
    lines.append("-- Top 10 Most Trusted Sources --")
    if top_10:
        for s in top_10:
            lines.append(
                f"  #{s['rank']:>2}  {s['source_type']:>14} | {s['source_id']:<25} "
                f"trust={s['trust_score']:.3f}  "
                f"win_rate={s['win_rate']:.1%}  "
                f"({s['hit_count']}H/{s['miss_count']}M)  "
                f"best={s['best_ticker'] or 'N/A'}"
            )
    else:
        lines.append("  No scored sources yet.")
    lines.append("")

    # Bottom (contrarian candidates)
    lines.append("-- Bottom 10 (Contrarian Indicator Candidates) --")
    if bottom_10:
        for s in bottom_10:
            lines.append(
                f"  #{s['rank']:>2}  {s['source_type']:>14} | {s['source_id']:<25} "
                f"trust={s['trust_score']:.3f}  "
                f"win_rate={s['win_rate']:.1%}  "
                f"({s['hit_count']}H/{s['miss_count']}M)"
            )
    else:
        lines.append("  No scored sources yet.")
    lines.append("")

    # Convergence events
    lines.append("-- Convergence Events (Last 7 Days) --")
    if recent_convergence:
        for e in recent_convergence:
            src_list = ", ".join(
                f"{s['source_type']}({s['trust_score']:.2f})"
                for s in e["sources"]
            )
            lines.append(
                f"  {e['ticker']} {e['signal_type']} — "
                f"{e['source_count']} sources, "
                f"confidence={e['combined_confidence']:.3f}  "
                f"[{src_list}]"
            )
    else:
        lines.append("  No convergence events in the last 7 days.")
    lines.append("")

    # Try LLM narrative
    llm_narrative = _get_llm_trust_narrative(all_sources, recent_convergence)
    if llm_narrative:
        lines.extend([
            "-- LLM Analysis --",
            llm_narrative,
            "",
        ])

    lines.append("===============================================================")

    report = "\n".join(lines)
    log.info("Trust report generated ({n} lines)", n=len(lines))
    return report


def _get_llm_trust_narrative(
    sources: list[dict],
    convergence_events: list[dict],
) -> str | None:
    """Ask local LLM for pattern analysis. Returns None if unavailable."""
    try:
        from llamacpp.client import get_client

        llm = get_client()
        if not llm.is_available:
            return None
    except Exception:
        return None

    top_5 = sources[:5] if sources else []
    bottom_5 = sources[-5:] if sources else []

    prompt = (
        "You are analyzing signal source trust scores for GRID.\n\n"
        f"Top 5 trusted sources:\n"
        + "\n".join(
            f"  {s['source_type']}/{s['source_id']}: trust={s['trust_score']:.3f}, "
            f"win_rate={s['win_rate']:.1%}, signals={s['total_signals']}"
            for s in top_5
        )
        + f"\n\nBottom 5:\n"
        + "\n".join(
            f"  {s['source_type']}/{s['source_id']}: trust={s['trust_score']:.3f}, "
            f"win_rate={s['win_rate']:.1%}, signals={s['total_signals']}"
            for s in bottom_5
        )
        + f"\n\nConvergence events: {len(convergence_events)}\n"
        + "\n".join(
            f"  {e['ticker']} {e['signal_type']} ({e['source_count']} sources, "
            f"confidence={e['combined_confidence']:.3f})"
            for e in convergence_events[:5]
        )
        + "\n\nWhat patterns stand out? Any sources worth investigating further? "
        "Any convergence events that look especially actionable? Be concise (3-5 bullets)."
    )

    try:
        response = llm.generate(
            prompt=prompt,
            system="You are a quantitative intelligence analyst. Be specific and data-driven.",
            temperature=0.3,
        )
        if response and isinstance(response, dict):
            return response.get("response", response.get("text", ""))
        if isinstance(response, str):
            return response
        return None
    except Exception as exc:
        log.debug("LLM trust narrative failed: {e}", e=str(exc))
        return None


# ── 7. Run Trust Cycle ────────────────────────────────────────────────────

def run_trust_cycle(engine: Engine) -> dict[str, Any]:
    """Orchestrate the full trust scoring cycle.

    Steps:
      1. Score pending signals against actual price outcomes
      2. Update trust scores with Bayesian recency weighting
      3. Detect convergence events
      4. Generate human-readable report

    Designed to be called by hermes_operator on a recurring schedule.

    Returns:
        dict with scoring, trust_update, convergence, and report.
    """
    log.info("=== Trust Scoring Cycle Starting ===")

    # 1. Score pending signals
    scoring = score_pending_signals(engine)

    # 2. Update trust scores
    trust_update = update_trust_scores(engine)

    # 3. Detect convergence
    convergence = detect_convergence(engine)

    # 4. Generate report
    report = generate_trust_report(engine)

    result = {
        "scoring": scoring,
        "trust_update": trust_update,
        "convergence": convergence,
        "convergence_count": len(convergence),
        "report": report,
    }

    log.info(
        "=== Trust Scoring Cycle Complete: "
        "{s} scored, {t} sources updated, {c} convergence events ===",
        s=scoring.get("scored", 0),
        t=trust_update.get("total", 0),
        c=len(convergence),
    )
    return result


# ── Convenience: Register a New Signal ────────────────────────────────────

def register_signal(
    engine: Engine,
    source_type: str,
    source_id: str,
    ticker: str,
    signal_type: str,
    signal_date: datetime | None = None,
    signal_value: float | None = None,
    metadata: dict | None = None,
) -> int | None:
    """Insert a new signal into the tracking system.

    This is called by ingestion modules when they discover a signal
    (congressional trade, insider filing, dark pool print, etc.).

    Args:
        engine: SQLAlchemy engine.
        source_type: One of 'congressional', 'insider', 'darkpool', 'social', 'scanner'.
        source_id: Identifier for the specific source (member name, etc.).
        ticker: Stock ticker.
        signal_type: 'BUY' or 'SELL'.
        signal_date: When the signal was generated (defaults to now).
        signal_value: Price at signal time (auto-fetched if None).
        metadata: Optional extra context (filing URL, trade size, etc.).

    Returns:
        Row ID of the inserted signal, or None on failure.
    """
    _ensure_tables(engine)

    if signal_type not in ("BUY", "SELL"):
        log.warning("Invalid signal_type '{d}' for signal registration", d=signal_type)
        return None

    now = datetime.now(timezone.utc)
    sig_date = signal_date or now

    # Auto-fetch price if not provided
    if signal_value is None:
        sig_dt = sig_date.date() if hasattr(sig_date, "date") else sig_date
        signal_value = _get_price_near_date(engine, ticker, sig_dt)

    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                INSERT INTO signal_sources
                    (source_type, source_id, ticker, signal_type, signal_date,
                     signal_value, metadata)
                VALUES
                    (:st, :si, :t, :d, :sd, :p, :m)
                RETURNING id
            """), {
                "st": source_type,
                "si": source_id,
                "t": ticker.upper(),
                "d": signal_type,
                "sd": sig_date,
                "p": signal_value,
                "m": json.dumps(metadata) if metadata else None,
            })
            row_id = result.fetchone()
            sig_id = row_id[0] if row_id else None

        log.info(
            "Signal registered: {st}/{si} {d} {t} @ ${p} (id={id})",
            st=source_type, si=source_id, d=signal_type, t=ticker,
            p=f"{signal_value:.2f}" if signal_value else "N/A",
            id=sig_id,
        )
        return sig_id

    except Exception as exc:
        log.error(
            "Failed to register signal {st}/{si} {d} {t}: {e}",
            st=source_type, si=source_id, d=signal_type, t=ticker, e=str(exc),
        )
        return None


# ── CLI Entry Point ───────────────────────────────────────────────────────

if __name__ == "__main__":
    from db import get_engine

    engine = get_engine()
    result = run_trust_cycle(engine)

    print("\n" + result["report"])
    print(f"\nConvergence events: {result['convergence_count']}")
    print(f"Signals scored: {result['scoring'].get('scored', 0)}")
    print(f"Sources tracked: {result['trust_update'].get('total', 0)}")
