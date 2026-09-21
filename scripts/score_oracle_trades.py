#!/usr/bin/env python3
"""
Score Oracle Trades — fix directions, score expired predictions.

Steps:
1. Fetch historical prices via yfinance for all prediction tickers
2. Count (never backfill) missing/invalid entry_price -- HISTORICAL-WRITE
   HOLD: a non-null 0/negative entry_price is a legacy row and is left
   exactly as it is; a NULL entry_price is a new-policy row and is never
   fabricated with a price discovered after the fact.
3. Map BULLISH→CALL, NEUTRAL→no_data verdict
4. Score expired predictions (expiry <= today)  ← CHUNKED since 2026-05-13
5. Print scorecard

Step 4 (the only one that touches large row counts) runs in chunks of
``--chunk-size`` rows, each chunk in its own short transaction. This
prevents the single-transaction lock-and-OOM failure mode that would
otherwise hit when scoring large expiry waves (e.g. the 2026-05-15
window where ~2.27M predictions expire at once).
"""

import argparse
import importlib.util
import json
import sys
from typing import Any

from datetime import date, timedelta

from loguru import logger as log
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Every first-party (repo-root) import this module needs -- directly or,
# like intelligence.postmortem below, lazily inside a function -- must be
# resolvable BEFORE the sys.path insertion further down ever has a chance
# to run. See that insertion's own comment for why, and
# tests/test_score_oracle_trades_stale_repo_shadow.py for the regression
# proof (it also covers db and intelligence.postmortem, which this module
# does not import itself but which must be equally unaffected by anything
# this import does to sys.path).
from config import settings
from oracle.entry_price_policy import (
    SCORE_NOTE_ENTRY_NULL,
    entry_price_score_note,
)

# Compute-node fallback for a genuinely standalone
# `python scripts/score_oracle_trades.py` invocation whose CWD/PYTHONPATH
# does not already make this repo importable. Two guards, both required:
#
# * `__name__ == "__main__"` -- this must NEVER run when the module is
#   merely imported (Hermes's oracle step does not import this module, but
#   anything that ever does -- directly or transitively -- must not have
#   sys.path mutated as a side effect). The imports above already
#   succeeded by this point however this module ended up loaded, so this
#   line can only ever add a path, never fix a failure that already
#   happened.
# * `importlib.util.find_spec("config") is None` -- even under direct
#   execution, only insert the fallback if this repo is not ALREADY
#   importable (e.g. via PYTHONPATH or CWD). Preferring whatever already
#   resolves correctly over a hardcoded, potentially stale path is strictly
#   safer, and `find_spec` never imports/executes `config` -- it only
#   locates it.
#
# /data/grid_v4/grid_repo is a STALE checkout confirmed on grid-svr
# (revision 5facbdf0, no oracle/entry_price_policy.py) and on the gridz4
# PostgreSQL-proof host. The previous, unconditional
# `sys.path.insert(0, "/data/grid_v4/grid_repo")` ran on every import of
# this module, on every host where that directory exists, for the rest of
# the process's lifetime -- shadowing this repo's real `oracle`, `config`
# and any other first-party package for every import that happened to run
# after it, not just the first one (reproduced: ModuleNotFoundError on
# oracle.entry_price_policy during pytest collection, and separately on
# scripts/score_oracle_trades.py's own real scorer-path PostgreSQL proof).
if __name__ == "__main__" and importlib.util.find_spec("config") is None:
    sys.path.insert(0, "/data/grid_v4/grid_repo")

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
    import yfinance as yf  # heavy + optional; defer import to call time

    yf_symbols = [get_yf_symbol(t) for t in tickers]
    symbol_to_ticker = {get_yf_symbol(t): t for t in tickers}

    log.info("Fetching prices for {} tickers from {} to {}...", len(tickers), start, end)

    # auto_adjust=False is load-bearing, not cosmetic. Most rows scored by this
    # script never touch Step 2's backfill below — their entry_price was set at
    # prediction time by oracle/engine.py's _get_spot_price(), which reads the
    # raw, unadjusted options_daily_signals.spot_price. yfinance flipped
    # yf.download()'s default to True in 0.2.x, which silently back-adjusts
    # Close for dividends/splits. Scoring that raw entry_price against a
    # back-adjusted `actual` close here would manufacture a return equal to
    # the cumulative adjustment factor between entry and expiry — the same
    # mixed-basis bug PR #503 fixed in intelligence/trust_scorer.py.
    #
    # Batch download
    data = yf.download(
        yf_symbols, start=start, end=end, group_by="ticker", progress=False,
        auto_adjust=False,
    )

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
    """Get close price for a ticker on a date, with lookback for weekends/holidays.

    For ``target_date >= today`` we REFUSE to fall back to prior closes — if the
    market hasn't closed on the expiry day, the prediction must remain pending
    rather than be locked in against the wrong (earlier) close. Without this
    guard, predictions expiring on day T get "scored" against day T-1 at hermes
    cycles that fire before T's close, then frozen with verdict='hit/miss' so
    the post-close rerun is skipped. (2026-05-15: caught after 2.27M predictions
    expiring on Friday got scored Thursday night with Thursday's close.)
    """
    if ticker not in prices:
        return None

    ticker_prices = prices[ticker]
    today = date.today()

    if target_date >= today:
        # Same-day-only: refuse to substitute an earlier close.
        if target_date in ticker_prices:
            val = ticker_prices[target_date]
            if pd.notna(val) and val > 0:
                return float(val)
        return None

    # Historical expiry — fall back up to 5 days for weekends/holidays.
    for offset in range(6):
        check_date = target_date - timedelta(days=offset)
        if check_date in ticker_prices:
            val = ticker_prices[check_date]
            if pd.notna(val) and val > 0:
                return float(val)

    return None


def _parse_signals_blob(blob: Any) -> dict[str, Any] | None:
    """Coerce the oracle_predictions.signals JSONB into a dict for the
    ReasoningBank fingerprint builder. Returns None on any failure so the
    caller can fall back to a thin fingerprint without crashing scoring.
    """
    if blob is None:
        return None
    if isinstance(blob, dict):
        return blob
    if isinstance(blob, str):
        try:
            parsed = json.loads(blob)
            return parsed if isinstance(parsed, dict) else None
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
    return None


def _record_success_lesson_safe(
    *,
    engine: Engine,
    prediction_id: str,
    ticker: str,
    direction: str,
    verdict: str,
    confidence: float | None,
    expected_move_pct: float | None,
    actual_move_pct: float,
    pnl_pct: float,
    signals_blob: Any,
    created_at: Any,
    expiry: Any,
    model: str | None,
) -> None:
    """Persist a success-side ReasoningLesson for a hit/partial prediction.

    Defensive at every step — failure to record a lesson must NEVER block
    scoring. Lazy-imports postmortem so this script is still runnable in
    environments where the intelligence subpackage isn't installed.
    """
    try:
        from intelligence.postmortem import record_success_lesson
    except Exception as exc:
        log.debug("score: skipped success lesson (import failed): {e}", e=str(exc))
        return

    try:
        data_at_decision = _parse_signals_blob(signals_blob)

        horizon_days: int | None = None
        try:
            if created_at is not None and expiry is not None:
                start = created_at.date() if hasattr(created_at, "date") else created_at
                horizon_days = max(0, (expiry - start).days)
        except Exception:
            horizon_days = None

        thesis = (
            f"Oracle {model or '?'} predicted {direction} on {ticker} "
            f"(confidence={confidence:.2f}, expected_move={expected_move_pct:.2f}%)"
            if confidence is not None and expected_move_pct is not None
            else f"Oracle {model or '?'} predicted {direction} on {ticker}"
        )
        what_worked = (
            f"realized {actual_move_pct:+.2f}% pnl={pnl_pct:+.2f}% over "
            f"{horizon_days}d horizon" if horizon_days is not None else
            f"realized {actual_move_pct:+.2f}% pnl={pnl_pct:+.2f}%"
        )
        takeaway = (
            f"{ticker} {direction} setup at this fingerprint paid out as "
            f"{verdict}; reuse weighting if same regime/horizon recurs."
        )

        record_success_lesson(
            engine,
            trade_id_or_prediction_id=prediction_id,
            ticker=ticker,
            direction=direction,
            outcome=verdict,
            data_at_decision=data_at_decision,
            thesis_at_decision=thesis,
            what_worked=what_worked,
            generalizable_takeaway=takeaway,
            horizon_days=horizon_days,
        )
    except Exception as exc:
        log.debug("score: success lesson capture failed for {p}: {e}",
                  p=prediction_id, e=str(exc))


_ALLOWED_VERDICT_COLS: frozenset[str] = frozenset({"hits", "partials", "misses"})


def score_one_chunk(
    conn,
    *,
    engine: Engine,
    prices: dict,
    today: date,
    chunk_size: int,
) -> dict[str, int]:
    """Score one chunk of expired-and-scoreable pending predictions.

    Returns counters {scored, hits, misses, partials, skipped, no_data}
    for the chunk. The caller accumulates across chunks.

    Each call must run inside its own ``engine.begin()`` block — that
    way the row-level locks released between chunks instead of being
    held for the entire 2.27M-row scoring run.
    """
    chunk = conn.execute(text("""
        SELECT id, ticker, direction, target_price, entry_price, expiry,
               confidence, expected_move_pct, model_name,
               signals, created_at
        FROM oracle_predictions
        WHERE verdict = 'pending'
          AND expiry <= :today
          AND entry_price IS NOT NULL
          AND entry_price > 0
        ORDER BY expiry
        LIMIT :chunk_size
    """), {"today": today, "chunk_size": int(chunk_size)}).fetchall()

    counters = {"scored": 0, "hits": 0, "misses": 0, "partials": 0,
                "skipped": 0, "no_data": 0, "fetched": len(chunk),
                # Rows the chunk WHERE should already have excluded. Counted
                # separately from `skipped` so a non-zero value is visible as
                # the defect it would be, rather than hiding in the noise.
                "unscorable_entry_price": 0}

    for r in chunk:
        (
            pred_id, ticker, direction, target, entry, expiry,
            conf, expected, model, signals_blob, created_at,
        ) = r

        if direction not in ("CALL", "PUT"):
            counters["skipped"] += 1
            continue

        # Belt-and-braces against a future edit to the chunk WHERE above.
        # Settled BEFORE the division: a NULL entry is not a zero entry, a
        # zero entry is not a 0% move, and neither is an infinite one. The
        # row is closed with the reason rather than skipped silently, so it
        # cannot sit 'pending' forever, and the two reasons stay distinct.
        entry_note = entry_price_score_note(entry)
        if entry_note is not None:
            conn.execute(text("""
                UPDATE oracle_predictions
                SET verdict = 'no_data', scored_at = NOW(),
                    score_notes = :notes
                WHERE id = :id
            """), {"id": pred_id, "notes": entry_note})
            counters["unscorable_entry_price"] += 1
            counters["no_data"] += 1
            continue

        actual = get_price_for_date(prices, ticker, expiry)
        if actual is None:
            conn.execute(text("""
                UPDATE oracle_predictions
                SET verdict = 'no_data', scored_at = NOW(),
                    score_notes = 'No price data at expiry'
                WHERE id = :id
            """), {"id": pred_id})
            counters["no_data"] += 1
            continue

        actual_move = (actual - entry) / entry * 100

        if direction == "CALL":
            hit = actual > entry
            pnl = actual_move
        elif direction == "PUT":
            hit = actual < entry
            pnl = -actual_move
        else:
            counters["skipped"] += 1
            continue

        exp_move = expected if expected else 1.0
        if hit and abs(actual_move) >= abs(exp_move) * 0.5:
            verdict = "hit"
            counters["hits"] += 1
        elif hit:
            verdict = "partial"
            counters["partials"] += 1
        else:
            verdict = "miss"
            counters["misses"] += 1

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
        counters["scored"] += 1

        if verdict in ("hit", "partial"):
            _record_success_lesson_safe(
                engine=engine,
                prediction_id=pred_id,
                ticker=ticker,
                direction=direction,
                verdict=verdict,
                confidence=conf,
                expected_move_pct=expected,
                actual_move_pct=actual_move,
                pnl_pct=pnl,
                signals_blob=signals_blob,
                created_at=created_at,
                expiry=expiry,
                model=model,
            )

        col_map = {"hit": "hits", "partial": "partials", "miss": "misses"}
        verdict_col = col_map.get(verdict)
        if verdict_col:
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

    return counters


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--chunk-size", type=int, default=5000,
        help="Rows processed per transaction (default 5000). Lower for less "
             "lock pressure on a busy DB; higher for fewer round-trips.",
    )
    ap.add_argument(
        "--max-rows", type=int, default=None,
        help="Stop after scoring this many rows (default: score every "
             "expired-and-scoreable pending). Useful for a probe run "
             "before the full sweep.",
    )
    ap.add_argument(
        "--progress-every", type=int, default=10,
        help="Log progress every N chunks (default 10).",
    )
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    engine = create_engine(settings.DB_URL)

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
        # HISTORICAL-WRITE HOLD: this step used to backfill a missing/zero
        # entry_price with a historical close looked up just now. That is
        # repair of an already-published row, which is on hold for both
        # cases below:
        #   * entry_price is a non-null 0/negative value -- only possible on
        #     a legacy row written before oracle_pred_nullable_0918 made the
        #     column nullable. Never updated, closed, rescored or
        #     re-labelled; left exactly as it is.
        #   * entry_price IS NULL -- a new-policy row: nothing was measured
        #     at publish time. Writing a price discovered after the fact
        #     would be exactly the fabrication the new policy exists to
        #     avoid, so it is never backfilled either.
        # This step therefore only counts what it is holding; it writes
        # nothing to oracle_predictions.
        log.info("\n--- STEP 2: Backfill Entry Prices (historical-write hold) ---")
        held_legacy = 0
        held_new_policy = 0

        for r in rows:
            pred_id, ticker, direction, entry_price, created_date, expiry = r

            if entry_price is not None and entry_price > 0:
                continue  # Already has a price

            if entry_price is None:
                held_new_policy += 1
            else:
                held_legacy += 1

        log.info("  Held (legacy, non-null invalid entry_price): {}", held_legacy)
        log.info("  Held (new-policy, entry_price IS NULL): {}", held_new_policy)

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

        # No entry price measured → no_data. Only entry_price IS NULL is
        # closed here: a new-policy row where nothing was measured at
        # publish time, so it would otherwise sit 'pending' forever, neither
        # scored nor accounted for. Not repaired — the row is closed and the
        # note says why.
        #
        # HISTORICAL-WRITE HOLD: a non-null 0/negative entry_price can only
        # be a legacy row written before oracle_pred_nullable_0918 made the
        # column nullable (the new publish path writes NULL, never 0/neg,
        # when nothing was measured). That row is deliberately excluded from
        # this WHERE — historical rescoring/repair is on hold, so it is
        # never updated, closed, rescored or re-labelled here. It is left
        # pending, exactly as it was before this branch.
        res = conn.execute(text("""
            UPDATE oracle_predictions
            SET verdict = 'no_data',
                score_notes = :note_null,
                scored_at = NOW()
            WHERE verdict = 'pending'
              AND entry_price IS NULL
        """), {
            "note_null": SCORE_NOTE_ENTRY_NULL,
        })
        log.info("  no usable entry_price → no_data: {}", res.rowcount)

    # ── Step 4: Score expired predictions (CHUNKED) ──
    log.info("\n--- STEP 4: Score Expired Predictions ---")

    # Pre-count so progress logs have a denominator.
    with engine.connect() as conn:
        total_scoreable = int(conn.execute(text("""
            SELECT COUNT(*) FROM oracle_predictions
            WHERE verdict = 'pending'
              AND expiry <= :today
              AND entry_price IS NOT NULL
              AND entry_price > 0
        """), {"today": today}).scalar() or 0)

    log.info("  Expired & scoreable: {:,}", total_scoreable)
    log.info(
        "  Chunked: chunk-size={:,}, max-rows={}",
        args.chunk_size,
        args.max_rows if args.max_rows is not None else "all",
    )

    totals = {"scored": 0, "hits": 0, "misses": 0, "partials": 0,
              "skipped": 0, "no_data": 0}
    processed = 0
    chunk_idx = 0
    while True:
        chunk_size = args.chunk_size
        if args.max_rows is not None:
            remaining = args.max_rows - processed
            if remaining <= 0:
                break
            chunk_size = min(chunk_size, remaining)

        with engine.begin() as chunk_conn:
            chunk_counters = score_one_chunk(
                chunk_conn,
                engine=engine,
                prices=prices,
                today=today,
                chunk_size=chunk_size,
            )

        chunk_idx += 1
        fetched = chunk_counters.pop("fetched", 0)
        for k, v in chunk_counters.items():
            totals[k] += v
        processed += fetched

        # No more pending rows — done.
        if fetched == 0:
            break

        if chunk_idx % args.progress_every == 0 or fetched < chunk_size:
            pct = (processed / total_scoreable * 100) if total_scoreable else 0.0
            log.info(
                "  chunk {} done: processed {:,}/{:,} ({:.1f}%) "
                "[hits={:,}, partials={:,}, misses={:,}, no_data={:,}, skipped={:,}]",
                chunk_idx, processed, total_scoreable, pct,
                totals["hits"], totals["partials"], totals["misses"],
                totals["no_data"], totals["skipped"],
            )

    total_scored = totals["hits"] + totals["misses"] + totals["partials"]
    log.info(
        "  Scored: {:,} (Hits: {:,}, Miss: {:,}, Partial: {:,}, "
        "No-data: {:,}, Skipped: {:,}) across {} chunks",
        total_scored, totals["hits"], totals["misses"], totals["partials"],
        totals["no_data"], totals["skipped"], chunk_idx,
    )

    # Re-open a connection for the scorecard reads.
    with engine.connect() as conn:

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
