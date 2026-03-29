#!/usr/bin/env python3
"""Queue 100K+ LLM tasks for Qwen to grind through."""

import os
import sys
import random

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_engine
from sqlalchemy import text

engine = get_engine()

SP500 = [
    "AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","BRK-B","JPM","V",
    "UNH","XOM","JNJ","PG","MA","HD","AVGO","MRK","PEP","KO","COST","ABBV",
    "WMT","LLY","BAC","CSCO","TMO","CRM","MCD","ORCL","ACN","AMD","NFLX",
    "ADBE","TXN","PM","UPS","NEE","RTX","LOW","QCOM","INTU","HON","AMGN",
    "AMAT","CAT","BA","GE","IBM","GS","MS","BLK","SCHW","AXP","DE","LMT",
    "NOC","GD","SLB","EOG","COP","CVX","MPC","VLO","PSX","OXY","HAL","DVN",
    "F","GM","PLTR","SNOW","DDOG","NET","CRWD","ZS","PANW","FTNT","SQ",
    "PYPL","COIN","DIS","CMCSA","T","VZ","TMUS","UBER","LYFT","DASH","ABNB",
    "BKNG","MAR","HLT","RCL","DAL","UAL","FDX","UNP","CSX","WM","RSG",
    "CI","ELV","HUM","CVS","MCK","ABT","SYK","MDT","DHR","ISRG","ZTS",
    "REGN","VRTX","GILD","BIIB","MRNA","BMY","PFE","NKE","SBUX","TGT",
    "DG","DLTR","ROST","TJX","LULU","CMG","YUM","DARDEN",
]

sectors = ["Technology","Healthcare","Energy","Financials","Industrials",
           "Consumer Discretionary","Consumer Staples","Utilities","Materials",
           "Real Estate","Communication Services"]

strategies = ["momentum","mean_reversion","volatility","carry","value",
              "statistical_arb","event_driven"]

banks = ["UBS","Credit Suisse","HSBC","Deutsche Bank","Barclays","BNP Paribas",
         "Goldman Sachs","Morgan Stanley","JPMorgan","Citigroup",
         "Standard Chartered","Societe Generale","Julius Baer","Pictet",
         "Lombard Odier","Bank of Singapore","DBS Private Banking",
         "Rothschild & Co","Safra Sarasin","EFG International"]

havens = ["BVI","BAH","PMA","SEY","KY","BM","MLT","JE","IM","GG",
          "COOK","SAM","NIUE","ANG","BRB","KNA","LI","LU","HK","SGP",
          "LABUA","MAURI","SC","AW","NEV","VANU","BLZ","TC","VG"]

# Get ICIJ officers
with engine.connect() as conn:
    officers = conn.execute(text(
        "SELECT name FROM actors WHERE category = 'icij_officer' "
        "AND name !~ '.*(Limited|Ltd|Corp|Bearer|Nominees|Trust|Bank|S\\.A\\.).*' "
        "AND LENGTH(name) BETWEEN 10 AND 50 "
        "ORDER BY RANDOM() LIMIT 2000"
    )).fetchall()
officer_names = [r[0] for r in officers]

# Create backlog table
with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS llm_task_backlog (
            id BIGSERIAL PRIMARY KEY,
            task_type TEXT NOT NULL,
            prompt TEXT NOT NULL,
            context JSONB DEFAULT '{}',
            priority INTEGER DEFAULT 3,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """))
    conn.execute(text(
        "CREATE INDEX IF NOT EXISTS idx_ltb_status ON llm_task_backlog (status, created_at)"
    ))

tasks = []

# 1. S&P deep profiles (120)
for t in SP500:
    tasks.append(("sp500_profile", f"DEEP PROFILE {t}: executives, board, insider trades, PAC, lobbying, offshore, tax, SEC, shorts. Label each: confirmed/derived/estimated/rumored/inferred.", f'{{"ticker":"{t}"}}'))

# 2. ICIJ officers (2000)
for name in officer_names:
    safe = name.replace('"', '\\"')
    tasks.append(("icij_officer", f"INVESTIGATE: {name}. Background, nationality, offshore entities, public companies, sanctions, net worth, red flags. Label each finding.", f'{{"person":"{safe}"}}'))

# 3. Jurisdiction deep dives (29)
for h in havens:
    tasks.append(("jurisdiction", f"TAX HAVEN: {h}. Legal framework, ownership transparency, FATF rating, law firms, estimated assets, recent changes, famous cases.", f'{{"haven":"{h}"}}'))

# 4. Cross-ref offshore ↔ public (120)
for t in SP500:
    tasks.append(("offshore_xref", f"OFFSHORE XREF {t}: 10-K subsidiaries, tax rate, transfer pricing, executives in leaks, tax lobbying, board offshore connections.", f'{{"ticker":"{t}"}}'))

# 5. Sector deep analysis (55)
for s in sectors:
    for a in ["flows","positioning","earnings","valuation","technicals"]:
        tasks.append(("sector", f"SECTOR {s} — {a}: state, trend, drivers, top 5 names, contrarian signals, historical analogs, trade ideas.", f'{{"sector":"{s}","angle":"{a}"}}'))

# 6. Strategy signals (840)
for cat in strategies:
    for t in SP500:
        tasks.append(("strategy", f"STRATEGY {cat} on {t}: signal firing? win rate? entry/size/hold? invalidation? expected return?", f'{{"ticker":"{t}","strategy":"{cat}"}}'))

# 7. Deep forensic (120)
for t in SP500:
    tasks.append(("forensic", f"FORENSIC {t}: decompose 30d price action, each >1% move, market vs specific, news/earnings/insider/options around each, implied expectation, biggest unpriced risk, 2mo call.", f'{{"ticker":"{t}"}}'))

# 8. Earnings (120)
for t in SP500:
    tasks.append(("earnings", f"EARNINGS {t}: date, consensus, whisper, historical surprise (8Q), implied move, insider 90d, analyst revisions, best options trade 60+ DTE.", f'{{"ticker":"{t}"}}'))

# 9. Bank enabler (20)
for b in banks:
    safe = b.replace('"', '\\"')
    tasks.append(("bank_enabler", f"ENABLER {b}: offshore entities facilitated, jurisdictions, client types, fines/sanctions, compliance posture, key personnel, money laundering cases.", f'{{"bank":"{safe}"}}'))

# 10. Alpha101 per ticker (120)
for t in SP500:
    tasks.append(("alpha101", f"ALPHA101 {t}: which quant factors strongest? short vs medium agree? cross-sectional rank? VWAP deviation? directional call 1-10.", f'{{"ticker":"{t}"}}'))

print(f"Generated {len(tasks)} tasks")

# Bulk insert
batch_size = 1000
inserted = 0
with engine.begin() as conn:
    for i in range(0, len(tasks), batch_size):
        batch = tasks[i:i+batch_size]
        for task_type, prompt, context in batch:
            conn.execute(text(
                "INSERT INTO llm_task_backlog (task_type, prompt, context) "
                "VALUES (:t, :p, CAST(:c AS jsonb))"
            ), {"t": task_type, "p": prompt, "c": context})
            inserted += 1
        print(f"  Inserted {inserted}/{len(tasks)}...")

print(f"\nDONE: {inserted} tasks queued")

# Summary
with engine.connect() as conn:
    rows = conn.execute(text(
        "SELECT task_type, COUNT(*) FROM llm_task_backlog "
        "WHERE status = 'pending' GROUP BY task_type ORDER BY COUNT(*) DESC"
    )).fetchall()
    print("\nBacklog by type:")
    for r in rows:
        print(f"  {r[0]}: {r[1]}")
    total = conn.execute(text(
        "SELECT COUNT(*) FROM llm_task_backlog WHERE status = 'pending'"
    )).fetchone()
    print(f"\nTOTAL PENDING: {total[0]}")
