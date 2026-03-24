#!/usr/bin/env python3
"""GRID AI Analyst — generates trade theses using local LLM + GRID data.

Uses llama.cpp client (via llama-server on localhost:8080) for inference.
Saves output to outputs/analyst_reports/ — does NOT mutate the decision journal.

Usage:
    python scripts/ai_analyst.py              # generate daily analysis
    python scripts/ai_analyst.py --quiet      # suppress stdout (cron mode)
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger as log

from config import settings
from db import execute_sql
from llamacpp.client import get_client as get_llamacpp


ANALYST_PROMPT = """\
You are GRID's AI analyst. You have access to 447 features across 36 data sources.

CURRENT REGIME: {regime} (confidence: {confidence:.0%})
CURRENT POSTURE: {posture}

REGIME HISTORY (last 5):
{regime_hist}

KEY SIGNALS:
{signal_text}

WATCHLIST FUNDAMENTALS:
{fund_text}

Based on this data, provide:

1. REGIME ASSESSMENT — Is the current {regime} classification correct? \
What's the strongest evidence for and against?

2. TOP 3 TRADES — Specific, actionable positions aligned with the regime. \
Include entry, target, stop, and thesis for each.

3. RISK FACTORS — What could invalidate the current regime? \
What signals would trigger a regime change?

4. HYPOTHESIS UPDATE — Which watchlist stocks look most interesting right \
now and why?

Be specific. Use the actual numbers. No hedging."""


def _fetch_context() -> dict:
    """Pull regime, signals, and fundamentals from the database."""
    # Current regime from decision journal
    regime_rows = execute_sql(
        "SELECT inferred_state, state_confidence, action_taken "
        "FROM decision_journal ORDER BY decision_timestamp DESC LIMIT 1",
    )
    if regime_rows:
        r = regime_rows[0]
        regime = r["inferred_state"]
        confidence = r["state_confidence"]
        posture = r["action_taken"]
    else:
        regime, confidence, posture = "UNKNOWN", 0.0, "UNKNOWN"

    # Regime history
    regime_history = execute_sql(
        "SELECT inferred_state, state_confidence "
        "FROM decision_journal ORDER BY decision_timestamp DESC LIMIT 5",
    )

    # Latest signals
    signals = execute_sql(
        """
        SELECT f.name, f.family, r.value, r.obs_date
        FROM resolved_series r
        JOIN feature_registry f ON f.id = r.feature_id
        WHERE r.obs_date = (
            SELECT MAX(obs_date) FROM resolved_series
            WHERE feature_id = r.feature_id
        )
        AND f.family IN ('rates','credit','vol','macro','commodity','sentiment')
        AND f.name NOT LIKE 'wiki_%%'
        AND f.name NOT LIKE 'news_%%'
        AND f.name NOT LIKE 'weather_%%'
        ORDER BY f.family, f.name
        """,
    )

    # Watchlist fundamentals
    fundamentals = execute_sql(
        """
        SELECT f.name, r.value, r.obs_date
        FROM resolved_series r
        JOIN feature_registry f ON f.id = r.feature_id
        WHERE (f.name LIKE '%%_pe_ratio' OR f.name LIKE '%%_short_pct'
            OR f.name LIKE '%%_insider_buys' OR f.name LIKE '%%_insider_sells'
            OR f.name LIKE '%%_fcf' OR f.name LIKE '%%_debt_to_equity')
        AND r.obs_date = (
            SELECT MAX(obs_date) FROM resolved_series
            WHERE feature_id = r.feature_id
        )
        ORDER BY f.name
        """,
    )

    return {
        "regime": regime,
        "confidence": confidence if confidence else 0.0,
        "posture": posture,
        "regime_history": regime_history or [],
        "signals": signals or [],
        "fundamentals": fundamentals or [],
    }


def run(quiet: bool = False) -> str | None:
    """Generate a daily analyst report via llama.cpp."""
    client = get_llamacpp()
    if not client.is_available:
        log.warning("llama-server not available — skipping AI analyst run")
        return None

    ctx = _fetch_context()

    signal_text = "\n".join(
        f"  {s['name']} ({s['family']}): {s['value']} as of {s['obs_date']}"
        for s in ctx["signals"][:20]
    )
    fund_text = "\n".join(
        f"  {f['name']}: {f['value']}" for f in ctx["fundamentals"]
    )
    regime_hist = "\n".join(
        f"  {r['inferred_state']} ({r['state_confidence']:.0%})"
        for r in ctx["regime_history"]
    )

    prompt = ANALYST_PROMPT.format(
        regime=ctx["regime"],
        confidence=ctx["confidence"],
        posture=ctx["posture"],
        regime_hist=regime_hist or "  (no history)",
        signal_text=signal_text or "  (no signals available)",
        fund_text=fund_text or "  (no fundamentals available)",
    )

    if not quiet:
        print("=" * 60)
        print("GRID AI ANALYST — DAILY BRIEFING")
        print(f"Date: {date.today()}")
        print(f"Regime: {ctx['regime']} | Confidence: {ctx['confidence']:.0%}"
              f" | Posture: {ctx['posture']}")
        print("=" * 60)
        print("\nQuerying local LLM...\n")

    response = client.chat(
        messages=[{"role": "user", "content": prompt}],
        num_predict=2000,
    )

    if not response:
        log.error("LLM returned empty response")
        return None

    # Save to outputs/analyst_reports/
    out_dir = Path(__file__).resolve().parent.parent / "outputs" / "analyst_reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"analyst_{timestamp}.txt"

    header = (
        f"GRID AI ANALYST — {date.today()}\n"
        f"Regime: {ctx['regime']} | Confidence: {ctx['confidence']:.0%}"
        f" | Posture: {ctx['posture']}\n"
        f"{'=' * 60}\n\n"
    )
    out_file.write_text(header + response, encoding="utf-8")
    log.info("Analyst report saved to {}", out_file)

    if not quiet:
        print(response)
        print(f"\nReport saved to {out_file}")

    return response


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GRID AI Analyst")
    parser.add_argument("--quiet", action="store_true", help="Suppress stdout (cron mode)")
    args = parser.parse_args()
    run(quiet=args.quiet)
