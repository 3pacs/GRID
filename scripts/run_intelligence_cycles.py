"""
Run all GRID intelligence cycles that have never been executed.

Targets:
  1. Thesis scoring (43 unscored snapshots)
  2. Trust scorer (source_accuracy has 0 rows)
  3. Forensic reports for SPY, BTC, ETH, QQQ, AAPL
  4. Pattern detection (event_patterns has 0 rows)
"""

import sys
import os
import json
import traceback
from loguru import logger as log

# Ensure project root is on the path
sys.path.insert(0, "/data/grid_v4/grid_repo")

from sqlalchemy import create_engine

ENGINE_URL = "postgresql://grid:gridmaster2026@localhost:5432/griddb"
engine = create_engine(ENGINE_URL)

SEPARATOR = "=" * 70


def run_step(label, func):
    """Run a function, print results, and handle errors gracefully."""
    log.info("{}", SEPARATOR)
    log.info("  {}", label)
    log.info("{}", SEPARATOR)
    try:
        result = func()
        log.info("{}", json.dumps(result, indent=2, default=str))
        return result
    except Exception:
        traceback.print_exc()
        return None


# ── 1. Thesis Scoring ────────────────────────────────────────────────────

def step_thesis():
    from intelligence.thesis_tracker import run_thesis_cycle
    return run_thesis_cycle(engine)


# ── 2. Trust Scorer ──────────────────────────────────────────────────────

def step_trust():
    from intelligence.trust_scorer import run_trust_cycle
    return run_trust_cycle(engine)


# ── 3. Forensic Reports ─────────────────────────────────────────────────

def step_forensics():
    from intelligence.forensics import batch_forensics
    tickers = ["SPY", "BTC", "ETH", "QQQ", "AAPL"]
    all_results = {}
    for ticker in tickers:
        log.info("\n--- Forensics for {} ---", ticker)
        try:
            reports = batch_forensics(engine, ticker, days=90, threshold=0.03)
            summary = []
            for r in reports:
                summary.append({
                    "ticker": r.ticker,
                    "move_date": r.move_date,
                    "move_pct": r.move_pct,
                    "move_direction": r.move_direction,
                    "warning_signals": r.warning_signals,
                    "key_actors": r.key_actors[:5],
                    "confidence": r.confidence,
                    "narrative": r.narrative[:200] if r.narrative else "",
                })
            all_results[ticker] = {
                "reports_generated": len(reports),
                "details": summary,
            }
            log.info("  {}: {} forensic reports generated", ticker, len(reports))
        except Exception:
            traceback.print_exc()
            all_results[ticker] = {"error": traceback.format_exc()}
    return all_results


# ── 4. Pattern Detection ────────────────────────────────────────────────

def step_patterns():
    from intelligence.event_sequence import find_recurring_patterns
    patterns = find_recurring_patterns(engine, min_occurrences=3)
    return {
        "patterns_found": len(patterns),
        "patterns": patterns[:20],  # cap output
    }


# ── Main ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("GRID Intelligence Cycle Runner")
    log.info("Database: {}", ENGINE_URL.replace('gridmaster2026', '***'))

    run_step("1. THESIS SCORING — run_thesis_cycle()", step_thesis)
    run_step("2. TRUST SCORER — run_trust_cycle()", step_trust)
    run_step("3. FORENSIC REPORTS — batch_forensics() for 5 tickers", step_forensics)
    run_step("4. PATTERN DETECTION — find_recurring_patterns(min_occurrences=3)", step_patterns)

    log.info("{}", SEPARATOR)
    log.info("  ALL INTELLIGENCE CYCLES COMPLETE")
    log.info("{}", SEPARATOR)
