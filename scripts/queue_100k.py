#!/usr/bin/env python3
"""Queue 100,000 research tasks for Qwen."""
import os, sys, random
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_engine
from sqlalchemy import text

engine = get_engine()

# Pull names from DB
with engine.connect() as conn:
    officers = [r[0] for r in conn.execute(text(
        "SELECT name FROM actors WHERE category = 'icij_officer' "
        "AND LENGTH(name) BETWEEN 10 AND 50 ORDER BY RANDOM() LIMIT 50000"
    )).fetchall()]
    entities = [r[0] for r in conn.execute(text(
        "SELECT name FROM actors WHERE category = 'icij_entity' "
        "ORDER BY RANDOM() LIMIT 20000"
    )).fetchall()]

print(f"Loaded {len(officers)} officers, {len(entities)} entities")

SP500 = ["AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","BRK-B","JPM","V",
"UNH","XOM","JNJ","PG","MA","HD","AVGO","MRK","PEP","KO","COST","ABBV","WMT",
"LLY","BAC","CSCO","TMO","CRM","MCD","ORCL","ACN","AMD","NFLX","ADBE","TXN",
"PM","UPS","NEE","RTX","LOW","QCOM","INTU","HON","AMGN","AMAT","CAT","BA","GE",
"IBM","GS","MS","BLK","SCHW","AXP","DE","LMT","NOC","GD","SLB","EOG","COP",
"CVX","MPC","VLO","PSX","OXY","HAL","DVN","F","GM","PLTR","SNOW","DDOG","NET",
"CRWD","ZS","PANW","FTNT","SQ","PYPL","COIN","DIS","CMCSA","T","VZ","TMUS",
"UBER","LYFT","DASH","ABNB","BKNG","MAR","HLT","RCL","DAL","UAL","FDX","UNP",
"CI","ELV","HUM","CVS","MCK","ABT","SYK","MDT","DHR","ISRG","ZTS","REGN",
"VRTX","GILD","BIIB","MRNA","BMY","PFE","NKE","SBUX","TGT","DG","TJX","CMG"]

crypto = ["BTC","ETH","SOL","AVAX","DOT","LINK","UNI","AAVE","MKR","DOGE",
"XRP","ADA","MATIC","ARB","OP","ATOM","NEAR","FTM","INJ","TIA","SUI","SEI",
"PEPE","WIF","BONK","JUP","PYTH","RENDER","FET","TAO","ONDO","ENA"]

strategies = ["momentum","mean_reversion","volatility","carry","value",
    "statistical_arb","event_driven","pairs_trading","trend_following","breakout"]

sectors = ["Technology","Healthcare","Energy","Financials","Industrials",
    "Consumer Discretionary","Consumer Staples","Utilities","Materials",
    "Real Estate","Communication Services"]

tasks = []

# 1. ICIJ officer investigations (50,000)
for name in officers:
    tasks.append(("icij_investigate",
        f"INVESTIGATE: {name}. Who is this person? Background, offshore entities, "
        f"public companies, sanctions, net worth, red flags. Label each finding.",
        f'{{"person":"{name[:60]}"}}'))

# 2. ICIJ entity investigations (20,000)
for name in entities:
    safe = name[:60].replace('"', "'")
    tasks.append(("icij_entity_investigate",
        f"ENTITY: {safe}. What is this offshore entity? Purpose, jurisdiction, "
        f"beneficial owners, connected entities, suspicious patterns. "
        f"Cross-reference with public companies and known actors.",
        f'{{"entity":"{safe}"}}'))

# 3. S&P 500 deep profiles x multiple angles (120 x 5 = 600)
for ticker in SP500:
    for angle in ["executive_compensation","board_interlocks","insider_trading_pattern",
                  "offshore_tax_structure","political_influence"]:
        tasks.append(("sp500_angle",
            f"{ticker} — {angle.replace('_',' ').title()}: Deep analysis. "
            f"Names, numbers, dates, connections. Label confidence.",
            f'{{"ticker":"{ticker}","angle":"{angle}"}}'))

# 4. Crypto x multiple angles (32 x 5 = 160)
for ticker in crypto:
    for angle in ["whale_analysis","protocol_risk","regulatory_exposure",
                  "narrative_momentum","technical_setup"]:
        tasks.append(("crypto_angle",
            f"CRYPTO {ticker} — {angle.replace('_',' ').title()}: "
            f"Current state, key metrics, outlook, trade idea.",
            f'{{"ticker":"{ticker}","angle":"{angle}"}}'))

# 5. Strategy x ticker matrix (10 x 120 = 1,200)
for strat in strategies:
    for ticker in SP500:
        tasks.append(("strategy_matrix",
            f"STRATEGY {strat} on {ticker}: Signal status, win rate, entry/exit, "
            f"invalidation, expected return.",
            f'{{"ticker":"{ticker}","strategy":"{strat}"}}'))

# 6. Cross-sector pair analysis (11 x 10 = 110)
for i, s1 in enumerate(sectors):
    for s2 in sectors[i+1:]:
        tasks.append(("sector_pair",
            f"SECTOR PAIR: {s1} vs {s2}. Rotation dynamics, relative value, "
            f"correlation regime, which to overweight and why.",
            f'{{"sector_a":"{s1}","sector_b":"{s2}"}}'))

# 7. Earnings deep dive all S&P (120)
for ticker in SP500:
    tasks.append(("earnings_deep",
        f"EARNINGS {ticker}: Next date, consensus, whisper, historical surprise, "
        f"implied move, insider 90d, analyst revisions, best options trade 60+ DTE.",
        f'{{"ticker":"{ticker}"}}'))

# 8. Macro indicator analysis (50)
indicators = ["Fed Funds Rate","CPI","PPI","NFP","GDP","ISM PMI","Retail Sales",
    "Housing Starts","Consumer Confidence","Industrial Production","Trade Balance",
    "Initial Claims","Continuing Claims","PCE","Core PCE","M2 Money Supply",
    "10Y Treasury","2Y Treasury","Yield Curve","VIX","Dollar Index","Gold",
    "Crude Oil WTI","Natural Gas","Copper","Baltic Dry Index","S&P Case-Shiller",
    "JOLTS","Beige Book","Fed Balance Sheet","Reverse Repo","TGA Balance",
    "Bank Lending Standards","Auto Sales","Credit Card Delinquencies",
    "Student Loan Defaults","Commercial Real Estate Vacancy","Office REIT NAV",
    "Shipping Container Rates","Semiconductor Billings","Taiwan Exports",
    "China PMI","Eurozone PMI","Japan Tankan","UK Gilt Yields",
    "German Bund Spread","Italy BTP Spread","Emerging Market Spreads",
    "MOVE Index","Skew Index"]
for ind in indicators:
    tasks.append(("macro_indicator",
        f"MACRO: {ind}. Current value, trend, historical context, what it predicts, "
        f"which assets react most, lead/lag relationship, trade expression.",
        f'{{"indicator":"{ind}"}}'))

random.shuffle(tasks)
print(f"Generated {len(tasks)} tasks")

# Bulk insert
batch_size = 5000
inserted = 0
with engine.begin() as conn:
    for i in range(0, len(tasks), batch_size):
        batch = tasks[i:i+batch_size]
        for task_type, prompt, context in batch:
            conn.execute(text(
                "INSERT INTO llm_task_backlog (task_type, prompt, context) "
                "VALUES (:t, :p, CAST(:c AS jsonb))"
            ), {"t": task_type, "p": prompt, "c": context})
        inserted += len(batch)
        print(f"  Inserted {inserted}/{len(tasks)}...")

print(f"\nDONE: {inserted} tasks queued")

with engine.connect() as conn:
    r = conn.execute(text(
        "SELECT status, COUNT(*) FROM llm_task_backlog GROUP BY status ORDER BY status"
    )).fetchall()
    total = sum(row[1] for row in r)
    print(f"TOTAL BACKLOG: {total}")
    for row in r:
        print(f"  {row[0]}: {row[1]}")
