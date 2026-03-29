#!/usr/bin/env python3
"""Queue 100,000 research tasks for Qwen."""
import os, sys, random, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_engine
from sqlalchemy import text

engine = get_engine()

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

indicators = ["Fed Funds Rate","CPI","PPI","NFP","GDP","ISM PMI","Retail Sales",
    "Housing Starts","Consumer Confidence","Industrial Production","Trade Balance",
    "Initial Claims","PCE","Core PCE","M2 Money Supply","10Y Treasury",
    "2Y Treasury","Yield Curve","VIX","Dollar Index","Gold","Crude Oil WTI",
    "Natural Gas","Copper","Baltic Dry Index","JOLTS","Beige Book",
    "Fed Balance Sheet","Reverse Repo","TGA Balance","Bank Lending Standards",
    "Shipping Container Rates","Semiconductor Billings","Taiwan Exports",
    "China PMI","Eurozone PMI","Japan Tankan","UK Gilt Yields",
    "MOVE Index","Skew Index","Credit Spreads","High Yield OAS",
    "Financial Conditions Index","Real Rates","Breakeven Inflation",
    "Money Market Fund Flows","Bank Reserves","Overnight RRP Usage",
    "Treasury Auction Demand","Central Bank Gold Purchases"]

tasks = []

# 1. ICIJ officers (50,000)
for name in officers:
    tasks.append(("icij_investigate",
        f"INVESTIGATE: {name}. Background, offshore entities, public companies, sanctions, net worth, red flags.",
        json.dumps({"person": name[:60]})))

# 2. ICIJ entities (20,000)
for name in entities:
    tasks.append(("icij_entity",
        f"ENTITY: {name[:60]}. Purpose, jurisdiction, beneficial owners, connected entities, suspicious patterns.",
        json.dumps({"entity": name[:60]})))

# 3. S&P x angles (600)
for t in SP500:
    for a in ["executive_comp","board_interlocks","insider_pattern","offshore_tax","political_influence"]:
        tasks.append(("sp500_angle", f"{t} {a}: Deep analysis. Names, numbers, dates. Label confidence.",
            json.dumps({"ticker": t, "angle": a})))

# 4. Crypto x angles (160)
for t in crypto:
    for a in ["whale_analysis","protocol_risk","regulatory","narrative","technical"]:
        tasks.append(("crypto_angle", f"CRYPTO {t} {a}: State, metrics, outlook, trade idea.",
            json.dumps({"ticker": t, "angle": a})))

# 5. Strategy x ticker (1,200)
for s in strategies:
    for t in SP500:
        tasks.append(("strategy_matrix", f"STRATEGY {s} on {t}: Signal, win rate, entry/exit, expected return.",
            json.dumps({"ticker": t, "strategy": s})))

# 6. Sector pairs (55)
for i, s1 in enumerate(sectors):
    for s2 in sectors[i+1:]:
        tasks.append(("sector_pair", f"PAIR: {s1} vs {s2}. Rotation, relative value, which to overweight.",
            json.dumps({"a": s1, "b": s2})))

# 7. Earnings (120)
for t in SP500:
    tasks.append(("earnings_deep", f"EARNINGS {t}: Date, consensus, whisper, surprise history, implied move, trade.",
        json.dumps({"ticker": t})))

# 8. Macro (50)
for ind in indicators:
    tasks.append(("macro", f"MACRO: {ind}. Current, trend, what it predicts, assets affected, trade expression.",
        json.dumps({"indicator": ind})))

random.shuffle(tasks)
print(f"Generated {len(tasks)} tasks")

batch_size = 5000
inserted = 0
with engine.begin() as conn:
    for i in range(0, len(tasks), batch_size):
        batch = tasks[i:i+batch_size]
        for task_type, prompt, context in batch:
            try:
                conn.execute(text(
                    "INSERT INTO llm_task_backlog (task_type, prompt, context) "
                    "VALUES (:t, :p, CAST(:c AS jsonb))"
                ), {"t": task_type, "p": prompt, "c": context})
                inserted += 1
            except Exception:
                pass  # skip bad names
        print(f"  Inserted {inserted}/{len(tasks)}...")

print(f"\nDONE: {inserted} tasks queued")
with engine.connect() as conn:
    r = conn.execute(text("SELECT status, COUNT(*) FROM llm_task_backlog GROUP BY status")).fetchall()
    total = sum(row[1] for row in r)
    print(f"TOTAL BACKLOG: {total}")
    for row in r:
        print(f"  {row[0]}: {row[1]}")
