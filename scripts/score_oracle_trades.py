#!/usr/bin/env python3
"""
Score Oracle Trades — Backfill entry prices, fix directions, score expired predictions.

Steps:
1. Fetch historical prices via yfinance for all prediction tickers
2. Backfill entry_price where it's 0
3. Map BULLISH→CALL, NEUTRAL→no_data verdict
4. Score expired predictions (expiry <= today)
5. Print scorecard
"""

import sys
import os
sys.path.insert(0, "/data/grid_v4/grid_repo")

from datetime import date, datetime, timedelta
from collections import defaultdict

import yfinance as yf
from loguru import logger as log
import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql://grid:gridmaster2026@localhost:5432/griddb"

# Ticker → yfinance symbol mapping
YF_MAP = {
    "BTC": "BTC-USD",
    "ETH": "ETH-USD",
    "SOL": "SOL-USD",
    "DXY": "DX-Y.NYB",
    "BRK-B": "BRK-B",
}

def get_yf_symbol(ticker: str) -> str:
    return YF_MAP.get(ticker, ticker)


def fetch_prices(tickers: list[str], start: str, end: str) -> dict[str, pd.DataFrame]:
    """Fetch daily close prices for all tickers from yfinance."""
    yf_symbols = [get_yf_symbol(t) for t in tickers]
    symbol_to_ticker = {get_yf_symbol(t): t for t in tickers}

    log.info("Fetching prices for {} tickers from {} to {}...", len(tickers), start, end)

    # Batch download
    data = yf.download(yf_symbols, start=start, end=end, group_by="ticker", progress=False)

    prices = {}  # ticker -> {date -> close_price}

    if len(yf_symbols) == 1:
        # Single ticker returns flat DataFrame
        sym = yf_symbols[0]
        ticker = symbol_to_ticker[sym]
        if "Close" in data.columns:
            prices[ticker] = data["Close"].dropna().to_dict()
    else:
        for sym in yf_symbols:
            ticker = symbol_to_ticker[sym]
            try:
                if sym in data.columns.get_level_values(0):
                    close = data[sym]["Close"].dropna()
                    prices[ticker] = {d.date() if hasattr(d, 'date') else d: v for d, v in close.items()}
            except Exception as e:
                log.info("  Warning: failed to get {} ({}): {}", ticker, sym, e)

    for t, p in prices.items():
        log.info("  {}: {} days of prices", t, len(p))

    return prices


def get_price_for_date(prices: dict, ticker: str, target_date: date) -> float | None:
    """Get close price for a ticker on a date, with lookback for weekends/holidays."""
    if ticker not in prices:
        return None

    ticker_prices = prices[ticker]

    # Try exact date, then look back up to 5 days
    for offset in range(6):
        check_date = target_date - timedelta(days=offset)
        if check_date in ticker_prices:
            val = ticker_prices[check_date]
            if pd.notna(val) and val > 0:
                return float(val)

    return None


def main():
    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        # ── Step 0: Get all pending predictions ──
        rows = conn.execute(text("""
            SELECT id, ticker, direction, entry_price, created_at::date, expiry
            FROM oracle_predictions
            WHERE verdict = 'pending'
            ORDER BY created_at
        """)).fetchall()

        log.info("\n{}", '='*60)
        log.info("ORACLE TRADE SCORER")
        log.info("{}", '='*60)
        log.info("Total pending predictions: {}", len(rows))

        # Get unique tickers and date range
        tickers = sorted(set(r[1] for r in rows))
        min_date = min(r[4] for r in rows)
        max_expiry = max(r[5] for r in rows)
        today = date.today()

        log.info("Tickers: {}", len(tickers))
        log.info("Date range: {} to {}", min_date, max_expiry)
        log.info("Today: {}", today)

        # ── Step 1: Fetch prices ──
        log.info("\n--- STEP 1: Fetch Prices ---")
        # Need prices from prediction creation dates through today (for scoring)
        fetch_start = (min_date - timedelta(days=7)).strftime("%Y-%m-%d")
        fetch_end = (today + timedelta(days=1)).strftime("%Y-%m-%d")

        prices = fetch_prices(tickers, fetch_start, fetch_end)

        # ── Step 2: Backfill entry prices ──
        log.info("\n--- STEP 2: Backfill Entry Prices ---")
        backfilled = 0
        no_price = 0

        for r in rows:
            pred_id, ticker, direction, entry_price, created_date, expiry = r

            if entry_price and entry_price > 0:
                continue  # Already has a price

            price = get_price_for_date(prices, ticker, created_date)
            if price is None:
                no_price += 1
                continue

            conn.execute(text("""
                UPDATE oracle_predictions
                SET entry_price = :price
                WHERE id = :id
            """), {"price": price, "id": pred_id})
            backfilled += 1

        log.info("  Backfilled: {}", backfilled)
        log.info("  No price available: {}", no_price)

        # ── Step 3: Fix direction mapping ──
        log.info("\n--- STEP 3: Fix Direction Mapping ---")

        # BULLISH → CALL
        res = conn.execute(text("""
            UPDATE oracle_predictions
            SET direction = 'CALL'
            WHERE verdict = 'pending' AND direction = 'BULLISH'
        """))
        log.info("  BULLISH → CALL: {}", res.rowcount)

        # BEARISH → PUT (just in case)
        res = conn.execute(text("""
            UPDATE oracle_predictions
            SET direction = 'PUT'
            WHERE verdict = 'pending' AND direction = 'BEARISH'
        """))
        log.info("  BEARISH → PUT: {}", res.rowcount)

        # NEUTRAL → no_data (unscorable)
        res = conn.execute(text("""
            UPDATE oracle_predictions
            SET verdict = 'no_data',
                score_notes = 'NEUTRAL direction is unscorable',
                scored_at = NOW()
            WHERE verdict = 'pending' AND direction = 'NEUTRAL'
        """))
        log.info("  NEUTRAL → no_data: {}", res.rowcount)

        # entry_price still 0 → no_data
        res = conn.execute(text("""
            UPDATE oracle_predictions
            SET verdict = 'no_data',
                score_notes = 'No entry price available for scoring',
                scored_at = NOW()
            WHERE verdict = 'pending' AND entry_price = 0
        """))
        log.info("  entry_price=0 → no_data: {}", res.rowcount)

        # ── Step 4: Score expired predictions ──
        log.info("\n--- STEP 4: Score Expired Predictions ---")

        expired = conn.execute(text("""
            SELECT id, ticker, direction, target_price, entry_price, expiry,
                   confidence, expected_move_pct, model_name
            FROM oracle_predictions
            WHERE verdict = 'pending' AND expiry <= :today AND entry_price > 0
            ORDER BY expiry
        """), {"today": today}).fetchall()

        log.info("  Expired & scoreable: {}", len(expired))

        hits = misses = partials = skipped = 0

        for r in expired:
            pred_id, ticker, direction, target, entry, expiry, conf, expected, model = r

            if direction not in ("CALL", "PUT"):
                skipped += 1
                continue

            # Get actual price at expiry
            actual = get_price_for_date(prices, ticker, expiry)
            if actual is None:
                conn.execute(text("""
                    UPDATE oracle_predictions
                    SET verdict = 'no_data', scored_at = NOW(),
                        score_notes = 'No price data at expiry'
                    WHERE id = :id
                """), {"id": pred_id})
                skipped += 1
                continue

            actual_move = (actual - entry) / entry * 100

            if direction == "CALL":
                hit = actual > entry
                pnl = actual_move
            elif direction == "PUT":
                hit = actual < entry
                pnl = -actual_move
            else:
                skipped += 1
                continue

            # Determine verdict
            exp_move = expected if expected else 1.0
            if hit and abs(actual_move) >= abs(exp_move) * 0.5:
                verdict = "hit"
                hits += 1
            elif hit:
                verdict = "partial"
                partials += 1
            else:
                verdict = "miss"
                misses += 1

            # Update prediction
            conn.execute(text("""
                UPDATE oracle_predictions
                SET verdict = :v, actual_price = :ap, actual_move_pct = :am,
                    pnl_pct = :pnl, scored_at = NOW(),
                    score_notes = :notes
                WHERE id = :id
            """), {
                "v": verdict, "ap": actual, "am": round(actual_move, 2),
                "pnl": round(pnl, 2), "id": pred_id,
                "notes": f"Entry ${entry:.2f} → Actual ${actual:.2f} ({actual_move:+.1f}%)",
            })

            # Update model stats
            col_map = {"hit": "hits", "partial": "partials", "miss": "misses"}
            verdict_col = col_map.get(verdict)
            if verdict_col:
                # Security: verdict_col is derived from col_map whose values are
                # hardcoded literals, but verdict itself originates from DB data that
                # could be attacker-influenced.  Assert against a frozen set before
                # interpolating into the UPDATE statement so any unexpected value
                # raises immediately rather than executing arbitrary SQL.
                _ALLOWED_VERDICT_COLS: frozenset[str] = frozenset({"hits", "partials", "misses"})
                assert verdict_col in _ALLOWED_VERDICT_COLS, \
                    f"Blocked DDL: verdict_col '{verdict_col}' not in allowed set"
                conn.execute(text(f"""
                    UPDATE oracle_models
                    SET {verdict_col} = {verdict_col} + 1,
                        predictions_made = predictions_made + 1,
                        cumulative_pnl = cumulative_pnl + :pnl,
                        last_updated = NOW()
                    WHERE name = :model
                """), {"pnl": pnl, "model": model})

        total_scored = hits + misses + partials
        log.info("  Scored: {} (Hits: {}, Miss: {}, Partial: {}, Skipped: {})", total_scored, hits, misses, partials, skipped)

        # ── Step 5: Scorecard ──
        log.info("\n{}", '='*60)
        log.info("SCORECARD")
        log.info("{}", '='*60)

        # Overall stats
        stats = conn.execute(text("""
            SELECT verdict, COUNT(*) FROM oracle_predictions
            GROUP BY verdict ORDER BY count DESC
        """)).fetchall()

        log.info("\nOverall Verdict Distribution:")
        for v, c in stats:
            log.info("  {:12s}: {:>6,}", v, c)

        # By model
        log.info("\nBy Model:")
        model_stats = conn.execute(text("""
            SELECT model_name,
                   COUNT(*) as total,
                   SUM(CASE WHEN verdict='hit' THEN 1 ELSE 0 END) as hits,
                   SUM(CASE WHEN verdict='miss' THEN 1 ELSE 0 END) as misses,
                   SUM(CASE WHEN verdict='partial' THEN 1 ELSE 0 END) as partials,
                   SUM(CASE WHEN verdict='pending' THEN 1 ELSE 0 END) as pending,
                   AVG(CASE WHEN verdict IN ('hit','miss','partial') THEN pnl_pct END) as avg_pnl
            FROM oracle_predictions
            GROUP BY model_name
            ORDER BY model_name
        """)).fetchall()

        log.info("  {:<20s} {:>6s} {:>5s} {:>5s} {:>5s} {:>5s} {:>6s} {:>8s}", 'Model', 'Total', 'Hits', 'Miss', 'Part', 'Pend', 'Hit%', 'AvgPnL')
        log.info("  {} {} {} {} {} {} {} {}", '-'*20, '-'*6, '-'*5, '-'*5, '-'*5, '-'*5, '-'*6, '-'*8)
        for m in model_stats:
            name, total, h, mi, p, pend, avg_pnl = m
            scored = (h or 0) + (mi or 0) + (p or 0)
            hit_pct = f"{(h or 0)/scored*100:.1f}%" if scored > 0 else "N/A"
            pnl_str = f"{avg_pnl:+.2f}%" if avg_pnl is not None else "N/A"
            log.info("  {:<20s} {:>6,} {:>5} {:>5} {:>5} {:>5} {:>6s} {:>8s}", name, total, h or 0, mi or 0, p or 0, pend or 0, hit_pct, pnl_str)

        # By ticker (scored only)
        log.info("\nBy Ticker (scored only):")
        ticker_stats = conn.execute(text("""
            SELECT ticker,
                   COUNT(*) as total,
                   SUM(CASE WHEN verdict='hit' THEN 1 ELSE 0 END) as hits,
                   SUM(CASE WHEN verdict='miss' THEN 1 ELSE 0 END) as misses,
                   AVG(pnl_pct) as avg_pnl
            FROM oracle_predictions
            WHERE verdict IN ('hit', 'miss', 'partial')
            GROUP BY ticker
            ORDER BY avg_pnl DESC NULLS LAST
        """)).fetchall()

        if ticker_stats:
            log.info("  {:<8s} {:>6s} {:>5s} {:>5s} {:>6s} {:>8s}", 'Ticker', 'Scored', 'Hits', 'Miss', 'Hit%', 'AvgPnL')
            log.info("  {} {} {} {} {} {}", '-'*8, '-'*6, '-'*5, '-'*5, '-'*6, '-'*8)
            for t in ticker_stats:
                name, total, h, mi, avg_pnl = t
                hit_pct = f"{(h or 0)/total*100:.1f}%" if total > 0 else "N/A"
                pnl_str = f"{avg_pnl:+.2f}%" if avg_pnl is not None else "N/A"
                log.info("  {:<8s} {:>6,} {:>5} {:>5} {:>6s} {:>8s}", name, total, h or 0, mi or 0, hit_pct, pnl_str)
        else:
            log.info("  No scored predictions yet (none expired)")

        # Pending by expiry
        log.info("\nPending by Expiry:")
        pending_exp = conn.execute(text("""
            SELECT expiry, COUNT(*),
                   COUNT(CASE WHEN entry_price > 0 THEN 1 END) as with_price
            FROM oracle_predictions
            WHERE verdict = 'pending'
            GROUP BY expiry ORDER BY expiry
        """)).fetchall()

        for exp, cnt, wp in pending_exp:
            status = "← SCOREABLE" if exp <= today else ""
            log.info("  {}: {:>5,} predictions ({} with entry price) {}", exp, cnt, wp, status)

    log.info("\n{}", '='*60)
    log.info("DONE")
    log.info("{}", '='*60)


if __name__ == "__main__":
    main()
