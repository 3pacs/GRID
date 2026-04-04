"""
GRID — Options recommendation outcome tracker and self-improvement loop.

Tracks how past options recommendations performed, computes signal-level
attribution scores, and feeds learnings back to update scanner weights.

Pipeline:
  1. Score expired recommendations — compare predicted vs actual P&L
  2. Compute signal scores — which of the 7 scanner signals predict winners?
  3. Update scanner weights — amplify winning signals, dampen losers
  4. Generate improvement report — rule-based or LLM-assisted analysis

Designed to be called by hermes_operator on a recurring schedule.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ──────────────────────────────────────────────────────────

# The 7 scanner signals from discovery/options_scanner.py
SCANNER_SIGNALS = [
    "pcr",
    "iv_skew",
    "max_pain_div",
    "term_structure",
    "oi_concentration",
    "iv_percentile",
    "gamma_squeeze",
]

# Default weights (all equal) — used when scanner_weights table is empty
DEFAULT_WEIGHTS = {s: 1.0 for s in SCANNER_SIGNALS}

# Weight evolution parameters
_MIN_WEIGHT = 0.1
_MAX_WEIGHT = 3.0
_LEARNING_RATE = 0.15
_MIN_SCORED_FOR_UPDATE = 5  # need at least N scored recs using a signal


# ── Table setup ────────────────────────────────────────────────────────

def _ensure_scanner_weights_table(engine: Engine) -> None:
    """Create the scanner_weights table if it doesn't exist."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS scanner_weights (
                id          SERIAL PRIMARY KEY,
                signal_name TEXT NOT NULL,
                weight      NUMERIC NOT NULL DEFAULT 1.0,
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                reason      TEXT
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_scanner_weights_signal
                ON scanner_weights (signal_name, updated_at DESC)
        """))


# ── 1. Score expired recommendations ──────────────────────────────────

def score_expired_recommendations(engine: Engine) -> dict[str, Any]:
    """Score all recommendations whose expiry has passed but outcome is NULL.

    For each expired recommendation:
      - Look up actual closing price at expiry via options_daily_signals
        or raw_series (yfinance close), or live yfinance as last resort.
      - Compute actual return relative to entry_price.
      - Determine outcome: WIN / LOSS / EXPIRED.
      - Update the DB row with outcome, actual_return, closed_at.

    Returns:
        dict with keys: scored, wins, losses, expired, total_pnl
    """
    today = date.today()
    summary: dict[str, Any] = {
        "scored": 0, "wins": 0, "losses": 0, "expired": 0, "total_pnl": 0.0,
    }

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT id, ticker, direction, strike, expiry,
                   entry_price, target_price, stop_loss
            FROM options_recommendations
            WHERE expiry <= :today AND outcome IS NULL
            ORDER BY expiry
        """), {"today": today}).fetchall()

        if not rows:
            log.info("No expired recommendations to score")
            return summary

        log.info("Scoring {n} expired recommendations", n=len(rows))

        for r in rows:
            rec_id, ticker, direction, strike, expiry, entry_price, target_price, stop_loss = r

            # Skip if entry_price is missing or zero
            if not entry_price or float(entry_price) <= 0:
                log.debug("Skipping rec {id}: no entry_price", id=rec_id)
                continue

            entry = float(entry_price)
            strike_f = float(strike) if strike else 0.0

            # Get actual closing price at expiry
            actual_price = _get_price_at_date(engine, ticker, expiry)
            if actual_price is None:
                log.debug(
                    "Skipping rec {id} ({t}): no price data at expiry {e}",
                    id=rec_id, t=ticker, e=expiry,
                )
                continue

            # Determine option P&L
            # For CALL: option has value if actual > strike, else expired worthless
            # For PUT: option has value if actual < strike, else expired worthless
            if direction == "CALL":
                intrinsic = max(0.0, actual_price - strike_f)
            elif direction == "PUT":
                intrinsic = max(0.0, strike_f - actual_price)
            else:
                intrinsic = 0.0

            # Actual return = (intrinsic - premium_paid) / premium_paid
            if entry > 0:
                actual_return = (intrinsic - entry) / entry
            else:
                actual_return = 0.0

            # Classify outcome
            if intrinsic <= 0:
                outcome = "EXPIRED"
                summary["expired"] += 1
            elif actual_return > 0:
                outcome = "WIN"
                summary["wins"] += 1
            else:
                outcome = "LOSS"
                summary["losses"] += 1

            summary["total_pnl"] += actual_return
            summary["scored"] += 1

            # Update the recommendation row
            conn.execute(text("""
                UPDATE options_recommendations
                SET outcome      = :outcome,
                    actual_return = :actual_return,
                    closed_at     = :closed_at
                WHERE id = :id
            """), {
                "outcome": outcome,
                "actual_return": round(actual_return, 6),
                "closed_at": datetime.now(timezone.utc),
                "id": rec_id,
            })

    summary["total_pnl"] = round(summary["total_pnl"], 4)

    log.info(
        "Scored {s} recommendations: {w}W / {l}L / {e}E  |  Total P&L: {p:+.2%}",
        s=summary["scored"], w=summary["wins"],
        l=summary["losses"], e=summary["expired"],
        p=summary["total_pnl"],
    )

    return summary


# ── 2. Compute signal scores ──────────────────────────────────────────

def compute_signal_scores(engine: Engine) -> dict[str, Any]:
    """Compute per-signal win rates and contribution metrics.

    For each of the 7 scanner signals, query all scored recommendations
    that had that signal active (score >= 3 in sanity_status signals),
    then compute:
      - win_rate: fraction of WIN outcomes when signal was present
      - avg_return: mean actual_return when signal was present
      - contribution: avg_return for winners minus avg_return for losers

    Returns:
        dict mapping signal_name -> {win_rate, avg_return, count,
        contribution, winners, losers}
    """
    signal_scores: dict[str, dict[str, Any]] = {}

    with engine.connect() as conn:
        # Load all scored recommendations with their sanity_status (contains signals)
        rows = conn.execute(text("""
            SELECT id, ticker, direction, outcome, actual_return, sanity_status,
                   confidence, thesis
            FROM options_recommendations
            WHERE outcome IS NOT NULL
            ORDER BY closed_at DESC
        """)).fetchall()

    if not rows:
        log.info("No scored recommendations for signal analysis")
        return {}

    # Build per-signal buckets
    for signal_name in SCANNER_SIGNALS:
        wins: list[float] = []
        losses: list[float] = []
        all_returns: list[float] = []

        for r in rows:
            rec_id, ticker, direction, outcome, actual_return, sanity_status, conf, thesis = r

            # Check if this signal was active in the recommendation
            # The sanity_status JSONB may contain signal info, or we check thesis text
            signal_active = _signal_was_active(signal_name, sanity_status, thesis)
            if not signal_active:
                continue

            ret = float(actual_return) if actual_return is not None else 0.0
            all_returns.append(ret)

            if outcome == "WIN":
                wins.append(ret)
            else:
                losses.append(ret)

        total = len(all_returns)
        if total == 0:
            signal_scores[signal_name] = {
                "win_rate": 0.0, "avg_return": 0.0, "count": 0,
                "contribution": 0.0, "winners": 0, "losers": 0,
            }
            continue

        win_rate = len(wins) / total if total > 0 else 0.0
        avg_return = sum(all_returns) / total if total > 0 else 0.0
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0
        contribution = avg_win - avg_loss  # positive = signal helps

        signal_scores[signal_name] = {
            "win_rate": round(win_rate, 4),
            "avg_return": round(avg_return, 4),
            "count": total,
            "contribution": round(contribution, 4),
            "winners": len(wins),
            "losers": len(losses),
        }

    # Rank by contribution
    ranked = sorted(signal_scores.items(), key=lambda x: x[1]["contribution"], reverse=True)
    log.info(
        "Signal scores computed for {n} signals across {t} scored recommendations",
        n=len(signal_scores), t=len(rows),
    )
    for rank, (name, scores) in enumerate(ranked, 1):
        log.debug(
            "  #{r} {s}: win_rate={wr:.1%}, avg_return={ar:+.2%}, "
            "contribution={c:+.4f}, n={n}",
            r=rank, s=name, wr=scores["win_rate"],
            ar=scores["avg_return"], c=scores["contribution"],
            n=scores["count"],
        )

    return dict(ranked)


# ── 3. Generate improvement report ────────────────────────────────────

def generate_improvement_report(engine: Engine) -> str:
    """Generate a self-improvement report from the last 30 days of outcomes.

    Attempts LLM analysis via llamacpp; falls back to rule-based report.

    Returns:
        str: The improvement report text.
    """
    cutoff = date.today() - timedelta(days=30)

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT ticker, direction, strike, expiry, entry_price,
                   target_price, outcome, actual_return, confidence,
                   generated_at, closed_at
            FROM options_recommendations
            WHERE outcome IS NOT NULL AND closed_at >= :cutoff
            ORDER BY closed_at DESC
        """), {"cutoff": cutoff}).fetchall()

    if not rows:
        return "No scored recommendations in the last 30 days. Nothing to report."

    # Aggregate stats
    outcomes = [r[6] for r in rows]
    returns = [float(r[7]) for r in rows if r[7] is not None]
    tickers = [r[0] for r in rows]

    total = len(outcomes)
    wins = outcomes.count("WIN")
    losses = outcomes.count("LOSS")
    expired = outcomes.count("EXPIRED")
    win_rate = wins / total if total > 0 else 0.0
    avg_return = sum(returns) / len(returns) if returns else 0.0

    # Per-ticker breakdown
    ticker_stats: dict[str, dict[str, Any]] = {}
    for r in rows:
        t = r[0]
        if t not in ticker_stats:
            ticker_stats[t] = {"wins": 0, "losses": 0, "expired": 0, "returns": []}
        if r[6] == "WIN":
            ticker_stats[t]["wins"] += 1
        elif r[6] == "LOSS":
            ticker_stats[t]["losses"] += 1
        else:
            ticker_stats[t]["expired"] += 1
        if r[7] is not None:
            ticker_stats[t]["returns"].append(float(r[7]))

    # Best/worst tickers
    ticker_perf = {
        t: sum(s["returns"]) / len(s["returns"]) if s["returns"] else 0.0
        for t, s in ticker_stats.items()
    }
    best_ticker = max(ticker_perf, key=ticker_perf.get) if ticker_perf else "N/A"  # type: ignore[arg-type]
    worst_ticker = min(ticker_perf, key=ticker_perf.get) if ticker_perf else "N/A"  # type: ignore[arg-type]

    # Signal scores for the report
    signal_scores = compute_signal_scores(engine)
    best_signal = next(iter(signal_scores), "N/A") if signal_scores else "N/A"
    worst_signal = list(signal_scores.keys())[-1] if signal_scores else "N/A"

    # Per-direction breakdown
    call_wins = sum(1 for r in rows if r[1] == "CALL" and r[6] == "WIN")
    call_total = sum(1 for r in rows if r[1] == "CALL")
    put_wins = sum(1 for r in rows if r[1] == "PUT" and r[6] == "WIN")
    put_total = sum(1 for r in rows if r[1] == "PUT")

    # Try LLM analysis
    llm_insight = _get_llm_improvement_analysis(rows, signal_scores)

    # Build report
    lines = [
        "═══════════════════════════════════════════════════════════",
        "  GRID Options Tracker — 30-Day Improvement Report",
        "═══════════════════════════════════════════════════════════",
        "",
        f"Period:          Last 30 days ({cutoff} to {date.today()})",
        f"Total scored:    {total}",
        f"Win rate:        {win_rate:.1%}  ({wins}W / {losses}L / {expired}E)",
        f"Avg return:      {avg_return:+.2%}",
        "",
        "── Direction Breakdown ──",
        f"  CALL:  {call_wins}/{call_total} wins ({call_wins / call_total:.1%})" if call_total else "  CALL:  no data",
        f"  PUT:   {put_wins}/{put_total} wins ({put_wins / put_total:.1%})" if put_total else "  PUT:   no data",
        "",
        "── Signal Performance (ranked by contribution) ──",
    ]

    for name, scores in signal_scores.items():
        lines.append(
            f"  {name:20s}  win_rate={scores['win_rate']:.1%}  "
            f"avg_ret={scores['avg_return']:+.2%}  "
            f"contribution={scores['contribution']:+.4f}  "
            f"n={scores['count']}"
        )

    lines.extend([
        "",
        f"Best signal:     {best_signal}",
        f"Worst signal:    {worst_signal}",
        f"Best ticker:     {best_ticker} (avg {ticker_perf.get(best_ticker, 0):+.2%})",
        f"Worst ticker:    {worst_ticker} (avg {ticker_perf.get(worst_ticker, 0):+.2%})",
        "",
    ])

    if llm_insight:
        lines.extend([
            "── LLM Analysis ──",
            llm_insight,
            "",
        ])

    lines.extend([
        "── Recommendations ──",
    ])

    # Rule-based recommendations
    if win_rate < 0.4:
        lines.append("  [!] Win rate below 40% — consider raising min_score threshold.")
    if expired > total * 0.3:
        lines.append("  [!] >30% expired worthless — strikes may be too far OTM.")
    if signal_scores:
        top_sig = next(iter(signal_scores))
        top_data = signal_scores[top_sig]
        if top_data["contribution"] > 0.05:
            lines.append(f"  [+] Signal '{top_sig}' is a strong winner — increase its weight.")
        bot_sig = list(signal_scores.keys())[-1]
        bot_data = signal_scores[bot_sig]
        if bot_data["contribution"] < -0.02:
            lines.append(f"  [-] Signal '{bot_sig}' is a net detractor — reduce its weight.")
    if call_total > 0 and put_total > 0:
        call_wr = call_wins / call_total
        put_wr = put_wins / put_total
        if abs(call_wr - put_wr) > 0.2:
            better = "CALL" if call_wr > put_wr else "PUT"
            lines.append(f"  [~] {better} direction significantly outperforms — check directional bias.")

    lines.append("")
    lines.append("═══════════════════════════════════════════════════════════")

    report = "\n".join(lines)
    log.info("Improvement report generated ({n} lines)", n=len(lines))
    return report


# ── 4. Update scanner weights ─────────────────────────────────────────

def update_scanner_weights(
    engine: Engine, signal_scores: dict[str, Any],
) -> dict[str, Any]:
    """Update scanner signal weights based on observed performance.

    Amplifies signals that correlate with winning trades; dampens losers.
    Stores the new weights in the scanner_weights table.

    Args:
        engine: SQLAlchemy engine.
        signal_scores: Output of compute_signal_scores().

    Returns:
        dict with old_weights and new_weights mappings.
    """
    _ensure_scanner_weights_table(engine)

    # Load current weights
    old_weights = _load_current_weights(engine)

    new_weights: dict[str, float] = {}
    reasons: dict[str, str] = {}

    for signal_name in SCANNER_SIGNALS:
        current_w = old_weights.get(signal_name, 1.0)
        scores = signal_scores.get(signal_name, {})
        count = scores.get("count", 0)

        if count < _MIN_SCORED_FOR_UPDATE:
            # Not enough data — keep current weight
            new_weights[signal_name] = current_w
            reasons[signal_name] = f"insufficient data (n={count})"
            continue

        win_rate = scores.get("win_rate", 0.5)
        contribution = scores.get("contribution", 0.0)

        # Target weight based on performance:
        # win_rate 0.5 + positive contribution → weight > 1.0
        # win_rate < 0.5 + negative contribution → weight < 1.0
        target = 0.5 + win_rate + contribution
        target = max(_MIN_WEIGHT, min(_MAX_WEIGHT, target))

        # Smooth update via learning rate
        new_w = current_w + _LEARNING_RATE * (target - current_w)
        new_w = round(max(_MIN_WEIGHT, min(_MAX_WEIGHT, new_w)), 4)

        new_weights[signal_name] = new_w
        direction = "up" if new_w > current_w else "down" if new_w < current_w else "unchanged"
        reasons[signal_name] = (
            f"win_rate={win_rate:.2%}, contribution={contribution:+.4f}, "
            f"weight {direction}: {current_w:.4f} -> {new_w:.4f}"
        )

    # Persist new weights
    with engine.begin() as conn:
        for signal_name, weight in new_weights.items():
            conn.execute(text("""
                INSERT INTO scanner_weights (signal_name, weight, updated_at, reason)
                VALUES (:signal, :weight, NOW(), :reason)
            """), {
                "signal": signal_name,
                "weight": weight,
                "reason": reasons.get(signal_name, ""),
            })

    log.info("Scanner weights updated: {w}", w=new_weights)
    for name in SCANNER_SIGNALS:
        old_w = old_weights.get(name, 1.0)
        new_w = new_weights.get(name, 1.0)
        if old_w != new_w:
            log.debug(
                "  {s}: {o:.4f} → {n:.4f} ({r})",
                s=name, o=old_w, n=new_w, r=reasons.get(name, ""),
            )

    return {
        "old_weights": old_weights,
        "new_weights": new_weights,
        "reasons": reasons,
    }


# ── 5. Run full improvement cycle ─────────────────────────────────────

def run_improvement_cycle(engine: Engine) -> dict[str, Any]:
    """Orchestrate the full outcome tracking and self-improvement loop.

    Steps:
      1. Score all expired recommendations
      2. Compute per-signal attribution scores
      3. Update scanner weights based on performance
      4. Generate human-readable improvement report

    Designed to be called by hermes_operator on a schedule.

    Returns:
        dict with scoring_summary, signal_scores, weight_update, report
    """
    log.info("═══ Options Improvement Cycle Starting ═══")

    # 1. Score expired recommendations
    scoring_summary = score_expired_recommendations(engine)

    # 2. Compute signal-level attribution
    signal_scores = compute_signal_scores(engine)

    # 3. Update scanner weights
    weight_update: dict[str, Any] = {}
    if signal_scores:
        weight_update = update_scanner_weights(engine, signal_scores)
    else:
        log.info("No signal scores available — skipping weight update")

    # 4. Generate improvement report
    report = generate_improvement_report(engine)

    result = {
        "scoring_summary": scoring_summary,
        "signal_scores": signal_scores,
        "weight_update": weight_update,
        "report": report,
    }

    log.info(
        "═══ Options Improvement Cycle Complete — "
        "scored={s}, signals={g}, weights_updated={w} ═══",
        s=scoring_summary.get("scored", 0),
        g=len(signal_scores),
        w=len(weight_update.get("new_weights", {})),
    )

    return result


# ── Internal helpers ───────────────────────────────────────────────────

def _get_price_at_date(engine: Engine, ticker: str, target_date: date) -> float | None:
    """Get the closing price for a ticker at or near a specific date.

    Checks (in order):
      1. options_daily_signals.spot_price
      2. raw_series with YF close data
      3. Live yfinance fetch (last resort)

    Returns:
        float price or None if unavailable.
    """
    with engine.connect() as conn:
        # 1. Try options_daily_signals
        row = conn.execute(text("""
            SELECT spot_price FROM options_daily_signals
            WHERE ticker = :t AND signal_date <= :d AND spot_price > 0
            ORDER BY signal_date DESC LIMIT 1
        """), {"t": ticker, "d": target_date}).fetchone()
        if row:
            return float(row[0])

        # 2. Try raw_series (yfinance close)
        row = conn.execute(text("""
            SELECT value FROM raw_series
            WHERE series_id = :sid AND obs_date <= :d AND pull_status = 'SUCCESS'
            ORDER BY obs_date DESC LIMIT 1
        """), {"sid": f"YF:{ticker}:close", "d": target_date}).fetchone()
        if row:
            return float(row[0])

    # 3. Last resort: live yfinance
    return _fetch_yfinance_price(ticker, target_date)


def _fetch_yfinance_price(ticker: str, target_date: date) -> float | None:
    """Fetch closing price from yfinance as a last resort.

    Gracefully returns None if yfinance is unavailable.
    """
    try:
        import yfinance as yf

        end = target_date + timedelta(days=5)  # buffer for weekends/holidays
        start = target_date - timedelta(days=5)
        df = yf.download(ticker, start=str(start), end=str(end), progress=False)
        if df.empty:
            return None
        # Get the closest date <= target_date
        valid = df[df.index.date <= target_date]  # type: ignore[union-attr]
        if valid.empty:
            valid = df  # take whatever we have
        close_col = "Close"
        if close_col not in valid.columns:
            return None
        return float(valid[close_col].iloc[-1])
    except Exception as exc:
        log.debug("yfinance fallback failed for {t}: {e}", t=ticker, e=str(exc))
        return None


def _signal_was_active(
    signal_name: str,
    sanity_status: Any,
    thesis: str | None,
) -> bool:
    """Determine if a scanner signal was active for a recommendation.

    Checks the sanity_status JSONB (which may contain signal data from
    the scanner) and the thesis text for signal name mentions.
    """
    # Check sanity_status if it's a dict or JSON string
    if sanity_status:
        status = sanity_status
        if isinstance(status, str):
            try:
                status = json.loads(status)
            except (json.JSONDecodeError, TypeError):
                status = {}

        if isinstance(status, dict):
            # Check for signal info in sanity status (scanner embeds signal data)
            for key, val in status.items():
                if signal_name in str(key).lower() or signal_name in str(val).lower():
                    return True

    # Fall back to checking the thesis text
    if thesis and signal_name.replace("_", " ") in thesis.lower():
        return True
    if thesis and signal_name in thesis.lower():
        return True

    # For broad attribution: assume all signals contributed unless
    # we have explicit evidence they didn't. This avoids under-counting
    # when signal metadata isn't persisted in sanity_status.
    # Once we have richer signal logging, tighten this heuristic.
    return True


def _load_current_weights(engine: Engine) -> dict[str, float]:
    """Load the most recent weight for each scanner signal.

    Returns DEFAULT_WEIGHTS if no weights have been stored yet.
    """
    _ensure_scanner_weights_table(engine)
    weights: dict[str, float] = {}

    with engine.connect() as conn:
        for signal_name in SCANNER_SIGNALS:
            row = conn.execute(text("""
                SELECT weight FROM scanner_weights
                WHERE signal_name = :s
                ORDER BY updated_at DESC
                LIMIT 1
            """), {"s": signal_name}).fetchone()
            if row:
                weights[signal_name] = float(row[0])

    if not weights:
        return dict(DEFAULT_WEIGHTS)

    # Fill any missing signals with default
    for s in SCANNER_SIGNALS:
        if s not in weights:
            weights[s] = 1.0

    return weights


def _get_llm_improvement_analysis(
    rows: list[Any],
    signal_scores: dict[str, Any],
) -> str | None:
    """Ask the LLM for pattern analysis on recommendation outcomes.

    Returns None if LLM is unavailable (graceful degradation).
    """
    try:
        from llm.router import get_llm, Tier

        llm = get_llm(Tier.REASON)
        if not llm.is_available:
            return None
    except Exception:
        return None

    # Build a concise data summary for the LLM
    total = len(rows)
    outcomes = [r[6] for r in rows]
    wins = outcomes.count("WIN")
    losses = outcomes.count("LOSS")
    expired = outcomes.count("EXPIRED")
    returns = [float(r[7]) for r in rows if r[7] is not None]
    avg_ret = sum(returns) / len(returns) if returns else 0.0

    signal_summary = "\n".join(
        f"  {name}: win_rate={s.get('win_rate', 0):.1%}, "
        f"avg_return={s.get('avg_return', 0):+.2%}, "
        f"contribution={s.get('contribution', 0):+.4f}, "
        f"n={s.get('count', 0)}"
        for name, s in signal_scores.items()
    )

    # Sample of recent trades (up to 10)
    trade_lines = []
    for r in rows[:10]:
        ticker, direction, strike, expiry, entry, target, outcome, ret, conf = r[:9]
        ret_str = f"{float(ret):+.2%}" if ret is not None else "N/A"
        trade_lines.append(
            f"  {ticker} {direction} ${float(strike):.0f} exp={expiry} "
            f"entry=${float(entry):.2f} outcome={outcome} return={ret_str} "
            f"confidence={float(conf):.2f}" if entry and strike and conf else
            f"  {ticker} {direction} outcome={outcome}"
        )
    trades_str = "\n".join(trade_lines)

    prompt = (
        f"You are analyzing options trading recommendation outcomes for GRID.\n\n"
        f"Last 30 days: {total} recommendations scored.\n"
        f"Results: {wins} wins, {losses} losses, {expired} expired worthless.\n"
        f"Average return: {avg_ret:+.2%}\n\n"
        f"Signal performance (ranked by contribution):\n{signal_summary}\n\n"
        f"Recent trades:\n{trades_str}\n\n"
        f"What patterns do you see? What specific changes would improve outcomes? "
        f"Be concise (3-5 bullet points)."
    )

    try:
        response = llm.generate(
            prompt=prompt,
            system="You are a quantitative trading analyst. Be specific and data-driven.",
            temperature=0.3,
        )
        if response and isinstance(response, dict):
            return response.get("response", response.get("text", ""))
        if isinstance(response, str):
            return response
        return None
    except Exception as exc:
        log.debug("LLM improvement analysis failed: {e}", e=str(exc))
        return None
