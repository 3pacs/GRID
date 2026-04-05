#!/usr/bin/env python3
"""
trial_gem_hunter/tests/test.py

AutoAgent scoring harness for GRID Trial Gem Hunter.

Reads agent picks from griddb trial_signals table,
evaluates them against historical price data,
and writes a score (0.0–1.0) to /logs/reward.txt.

Score is mean 30-day forward return of top-5 picks,
normalized between -10% (score=0.0) and +15% (score=1.0).
Regime-aware: picks in FRAGILE/CRISIS regimes are scored
against a WATCHLIST benchmark (not penalized for not being BUYs).
"""

import os
import sys
import json
import math
import datetime
import traceback
import requests
import psycopg2
import psycopg2.extras

# ── Config ────────────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host": os.getenv("GRID_DB_HOST", "localhost"),
    "port": int(os.getenv("GRID_DB_PORT", 5432)),
    "dbname": "griddb",
    "user": "grid",
    "password": "grid2026",
}

ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY", "SPT9IOAEYVUT7X6J")
LOGS_DIR = "/logs"
REWARD_FILE = f"{LOGS_DIR}/reward.txt"
RESULTS_FILE = f"{LOGS_DIR}/results.json"

# Score normalization bounds
FLOOR_RETURN   = -0.10   # score = 0.0
CEILING_RETURN =  0.15   # score = 1.0
TOP_N_PICKS    = 5


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def fetch_latest_picks(conn, run_id=None, limit=TOP_N_PICKS):
    """
    Fetch the top-N picks written by the agent in this run.
    Ordered by trial_strength_score desc.
    If run_id provided, filter to that run only.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if run_id:
        cur.execute("""
            SELECT * FROM trial_signals
            WHERE run_id = %s
            ORDER BY trial_strength_score DESC
            LIMIT %s
        """, (run_id, limit))
    else:
        # Most recent batch: picks from last 2 hours
        cur.execute("""
            SELECT * FROM trial_signals
            WHERE created_at >= NOW() - INTERVAL '2 hours'
            ORDER BY trial_strength_score DESC
            LIMIT %s
        """, (limit,))
    rows = cur.fetchall()
    cur.close()
    return rows


def fetch_historical_return(conn, ticker, from_date, days=30):
    """
    Try griddb market_data first (fast), fall back to Alpha Vantage.
    Returns float forward return or None if unavailable.
    """
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT close_price FROM market_data
            WHERE ticker = %s AND date >= %s
            ORDER BY date ASC LIMIT 1
        """, (ticker, from_date))
        row_start = cur.fetchone()

        target_date = from_date + datetime.timedelta(days=days)
        cur.execute("""
            SELECT close_price FROM market_data
            WHERE ticker = %s AND date >= %s
            ORDER BY date ASC LIMIT 1
        """, (ticker, target_date))
        row_end = cur.fetchone()

        if row_start and row_end and row_start[0] and row_end[0]:
            cur.close()
            return (row_end[0] - row_start[0]) / row_start[0]
    except Exception:
        pass
    finally:
        cur.close()

    # Fallback: Alpha Vantage
    return _av_forward_return(ticker, from_date, days)


def _av_forward_return(ticker, from_date, days=30):
    """Alpha Vantage fallback for price data."""
    try:
        url = (
            f"https://www.alphavantage.co/query"
            f"?function=TIME_SERIES_DAILY_ADJUSTED"
            f"&symbol={ticker}"
            f"&outputsize=full"
            f"&apikey={ALPHA_VANTAGE_KEY}"
        )
        r = requests.get(url, timeout=15)
        data = r.json().get("Time Series (Daily)", {})
        if not data:
            return None

        dates = sorted(data.keys())
        start_price = end_price = None

        for d in dates:
            dt = datetime.date.fromisoformat(d)
            if dt >= from_date and start_price is None:
                start_price = float(data[d]["4. close"])
            if dt >= from_date + datetime.timedelta(days=days) and end_price is None:
                end_price = float(data[d]["4. close"])
                break

        if start_price and end_price:
            return (end_price - start_price) / start_price
        return None
    except Exception:
        return None


def get_current_regime(conn):
    """Fetch latest GRID regime label."""
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT regime FROM regime_states
            ORDER BY timestamp DESC LIMIT 1
        """)
        row = cur.fetchone()
        cur.close()
        return row[0] if row else "UNKNOWN"
    except Exception:
        cur.close()
        return "UNKNOWN"


# ── Scoring ───────────────────────────────────────────────────────────────────

def normalize_return(ret):
    """Map return value to [0.0, 1.0] score."""
    if ret is None:
        return 0.0
    clamped = max(FLOOR_RETURN, min(CEILING_RETURN, ret))
    return (clamped - FLOOR_RETURN) / (CEILING_RETURN - FLOOR_RETURN)


def regime_multiplier(regime):
    """
    Regime-aware scoring.
    Agent should only BUY in GROWTH/NEUTRAL — reward correct regime awareness.
    Watchlist-only picks in FRAGILE/CRISIS are evaluated differently.
    """
    return {
        "GROWTH":  1.00,
        "NEUTRAL": 0.90,
        "FRAGILE": 0.70,   # picks here are risky; penalize confidence
        "CRISIS":  0.50,
        "UNKNOWN": 0.80,
    }.get(regime, 0.80)


def score_picks(picks, conn, current_regime):
    """
    For each pick, attempt to compute 30d forward return from signal date.
    Aggregate into final score.
    """
    results = []
    returns = []

    for pick in picks:
        ticker     = pick.get("ticker")
        signal_dt  = pick.get("created_at")
        regime     = pick.get("regime_at_signal", current_regime)
        strength   = float(pick.get("trial_strength_score", 0.5))
        signal_type = pick.get("signal_type", "BUY")

        if not ticker or not signal_dt:
            continue

        # Handle datetime vs date
        if hasattr(signal_dt, "date"):
            from_date = signal_dt.date()
        else:
            from_date = datetime.date.today() - datetime.timedelta(days=35)

        fwd_return = fetch_historical_return(conn, ticker, from_date, days=30)

        # For WATCHLIST signals in crisis, we score differently:
        # we want agent to correctly avoid BUY signals, reward caution
        if signal_type == "WATCHLIST" and regime in ("FRAGILE", "CRISIS"):
            # Reward for not going long in bad regime
            # Score = 1.0 if return was negative (agent was right to hold off)
            if fwd_return is not None and fwd_return < 0:
                normalized = 0.8
            else:
                normalized = 0.5
        else:
            normalized = normalize_return(fwd_return)

        # Weight by trial_strength_score (agent's own confidence)
        weighted = normalized * strength * regime_multiplier(regime)

        results.append({
            "ticker": ticker,
            "signal_type": signal_type,
            "trial_strength_score": strength,
            "regime": regime,
            "30d_forward_return": round(fwd_return, 4) if fwd_return else None,
            "normalized_score": round(normalized, 4),
            "weighted_score": round(weighted, 4),
            "nct_id": pick.get("nct_id"),
            "indication": pick.get("primary_indication"),
            "days_to_completion": pick.get("days_to_completion"),
        })
        returns.append(weighted)

    if not returns:
        return 0.0, results

    # Penalize for fewer than TOP_N picks (agent should always find gems)
    coverage_penalty = len(returns) / TOP_N_PICKS
    raw_score = sum(returns) / len(returns)
    final_score = raw_score * coverage_penalty

    return round(min(1.0, max(0.0, final_score)), 4), results


# ── Validation: trial_signals schema check ───────────────────────────────────

REQUIRED_COLUMNS = {
    "nct_id", "ticker", "company_name", "trial_phase",
    "primary_indication", "trial_strength_score",
    "signal_type", "regime_at_signal", "confidence",
}

def validate_schema(conn):
    """Confirm trial_signals table exists with required columns."""
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'trial_signals'
    """)
    cols = {row[0] for row in cur.fetchall()}
    cur.close()
    missing = REQUIRED_COLUMNS - cols
    if missing:
        raise RuntimeError(f"trial_signals missing columns: {missing}")


# ── Bonus scoring: signal orthogonality check ─────────────────────────────────

def orthogonality_bonus(picks, conn):
    """
    Reward agent for picking tickers that are orthogonal to
    existing GRID features (low correlation to existing signals).
    Small bonus — max 0.05 additive.
    """
    if not picks:
        return 0.0
    try:
        cur = conn.cursor()
        tickers = [p["ticker"] for p in picks if p.get("ticker")]
        cur.execute("""
            SELECT COUNT(DISTINCT ticker) FROM features
            WHERE ticker = ANY(%s)
        """, (tickers,))
        overlap = cur.fetchone()[0]
        cur.close()
        # More novel tickers = higher bonus (up to 0.05)
        novelty = 1.0 - (overlap / max(len(tickers), 1))
        return round(novelty * 0.05, 4)
    except Exception:
        return 0.0


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(LOGS_DIR, exist_ok=True)
    score = 0.0
    results = []
    error_msg = None

    try:
        conn = get_conn()

        # Validate schema
        try:
            validate_schema(conn)
        except RuntimeError as e:
            # Schema not set up yet — score 0, let agent know
            error_msg = str(e)
            score = 0.0
            results = [{"error": error_msg}]
            conn.close()
            _write_outputs(score, results)
            return

        current_regime = get_current_regime(conn)
        picks = fetch_latest_picks(conn)

        if not picks:
            error_msg = "No picks found in trial_signals for this run window."
            score = 0.0
            results = [{"error": error_msg, "regime": current_regime}]
        else:
            base_score, results = score_picks(picks, conn, current_regime)
            bonus = orthogonality_bonus(results, conn)
            score = min(1.0, base_score + bonus)

            # Log summary
            print(f"\n{'='*60}")
            print(f"GRID Trial Gem Hunter — Scoring Report")
            print(f"{'='*60}")
            print(f"Current Regime : {current_regime}")
            print(f"Picks Evaluated: {len(results)}")
            print(f"Base Score     : {base_score}")
            print(f"Novelty Bonus  : {bonus}")
            print(f"Final Score    : {score}")
            print(f"{'='*60}")
            for r in results:
                ret_str = f"{r['30d_forward_return']*100:.1f}%" if r['30d_forward_return'] else "N/A"
                print(f"  {r['ticker']:8s} | {r['indication']:20s} | "
                      f"strength={r['trial_strength_score']:.2f} | "
                      f"30d_return={ret_str} | "
                      f"wscore={r['weighted_score']:.3f}")

        conn.close()

    except Exception as e:
        error_msg = traceback.format_exc()
        print(f"SCORING ERROR:\n{error_msg}", file=sys.stderr)
        score = 0.0
        results = [{"error": str(e)}]

    _write_outputs(score, results, error_msg)


def _write_outputs(score, results, error_msg=None):
    # reward.txt — AutoAgent reads this
    with open(REWARD_FILE, "w") as f:
        f.write(str(score))

    # results.json — human readable
    output = {
        "score": score,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "results": results,
    }
    if error_msg:
        output["error"] = error_msg

    with open(RESULTS_FILE, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\n→ Score written: {score}")
    print(f"→ Results: {RESULTS_FILE}")


if __name__ == "__main__":
    main()
