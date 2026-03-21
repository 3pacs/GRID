#!/usr/bin/env python3
"""GRID AI Analyst — generates trade theses using local LLM + GRID data."""

import sys, json, requests, psycopg2
sys.path.insert(0, '/home/grid/grid_v4/grid_repo/grid')
from db import get_engine
from store.pit import PITStore
from datetime import date

OLLAMA = "http://localhost:11434/api/generate"

def ask(prompt, model="llama3.2"):
    r = requests.post(OLLAMA, json={"model": model, "prompt": prompt, "stream": False}, timeout=600)
    return r.json().get('response', '')

def run():
    pg = psycopg2.connect(dbname='griddb', user='grid', password='grid2026')
    cur = pg.cursor()

    # Get current regime
    cur.execute("SELECT inferred_state, state_confidence, action_taken FROM decision_journal ORDER BY decision_timestamp DESC LIMIT 1")
    regime_row = cur.fetchone()
    regime = regime_row[0] if regime_row else "UNKNOWN"
    confidence = regime_row[1] if regime_row else 0
    posture = regime_row[2] if regime_row else "UNKNOWN"

    # Get latest signals
    cur.execute("""
        SELECT f.name, f.family, r.value, r.obs_date
        FROM resolved_series r
        JOIN feature_registry f ON f.id = r.feature_id
        WHERE r.obs_date = (SELECT MAX(obs_date) FROM resolved_series WHERE feature_id = r.feature_id)
        AND f.family IN ('rates','credit','vol','macro','commodity','sentiment')
        AND f.name NOT LIKE 'wiki_%'
        AND f.name NOT LIKE 'news_%'
        AND f.name NOT LIKE 'weather_%'
        ORDER BY f.family, f.name
    """)
    signals = cur.fetchall()

    # Get hypothesis stocks with fundamentals
    cur.execute("""
        SELECT f.name, r.value, r.obs_date
        FROM resolved_series r
        JOIN feature_registry f ON f.id = r.feature_id
        WHERE f.name LIKE '%_pe_ratio' OR f.name LIKE '%_short_pct'
            OR f.name LIKE '%_insider_buys' OR f.name LIKE '%_insider_sells'
            OR f.name LIKE '%_fcf' OR f.name LIKE '%_debt_to_equity'
        AND r.obs_date = (SELECT MAX(obs_date) FROM resolved_series WHERE feature_id = r.feature_id)
        ORDER BY f.name
    """)
    fundamentals = cur.fetchall()

    # Get orthogonality results
    cur.execute("SELECT inferred_state, state_confidence FROM decision_journal ORDER BY decision_timestamp DESC LIMIT 5")
    regime_history = cur.fetchall()

    # Build the prompt
    signal_text = "\n".join([f"  {s[0]} ({s[1]}): {s[2]} as of {s[3]}" for s in signals[:20]])
    fund_text = "\n".join([f"  {f[0]}: {f[1]}" for f in fundamentals])
    regime_hist = "\n".join([f"  {r[0]} ({r[1]:.0%})" for r in regime_history])

    prompt = f"""You are GRID's AI analyst. You have access to 447 features across 36 data sources.

CURRENT REGIME: {regime} (confidence: {confidence:.0%})
CURRENT POSTURE: {posture}

REGIME HISTORY (last 5):
{regime_hist}

KEY SIGNALS:
{signal_text}

WATCHLIST FUNDAMENTALS:
{fund_text}

Based on this data, provide:

1. REGIME ASSESSMENT — Is the current {regime} classification correct? What's the strongest evidence for and against?

2. TOP 3 TRADES — Specific, actionable positions aligned with the regime. Include entry, target, stop, and thesis for each.

3. RISK FACTORS — What could invalidate the current regime? What signals would trigger a regime change?

4. HYPOTHESIS UPDATE — Which of these watchlist stocks (EOG, DVN, CMCSA, RTX, GD, CI, PYPL, INTC) look most interesting right now and why?

Be specific. Use the actual numbers. No hedging."""

    print("=" * 60)
    print("GRID AI ANALYST — DAILY BRIEFING")
    print(f"Date: {date.today()}")
    print(f"Regime: {regime} | Confidence: {confidence:.0%} | Posture: {posture}")
    print("=" * 60)
    print("\nQuerying local LLM (this takes 30-60 seconds on CPU)...\n")

    response = ask(prompt)
    print(response)

    # Save to journal
    pg.commit()
    pg.autocommit = True
    cur.execute("SELECT COALESCE(MAX(id),0)+1 FROM decision_journal")
    # Save analysis as annotation on latest journal entry
    cur.execute("UPDATE decision_journal SET annotation=%s WHERE id=(SELECT MAX(id) FROM decision_journal)", (response[:2000],))
    print("\n" + "=" * 60)
    print("Analysis saved to decision journal.")
    pg.close()

if __name__ == "__main__":
    run()
