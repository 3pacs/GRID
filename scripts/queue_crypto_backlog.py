#!/usr/bin/env python3
"""Queue crypto-specific research tasks for Qwen."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_engine
from sqlalchemy import text
from loguru import logger as log

engine = get_engine()
tasks = []

tickers = ["BTC","ETH","SOL","AVAX","DOT","LINK","UNI","AAVE","MKR","DOGE",
    "XRP","ADA","MATIC","ARB","OP","ATOM","NEAR","FTM","APE","LDO",
    "SNX","CRV","COMP","SUSHI","YFI","RUNE","INJ","TIA","SUI","SEI"]

exchanges = ["Binance","Coinbase","Kraken","OKX","Bybit","Bitfinex","Huobi",
    "KuCoin","Deribit","BitMEX","Gemini","Crypto.com","Upbit","Bitstamp"]

protocols = ["Uniswap","Aave","MakerDAO","Compound","Curve","Lido","Synthetix",
    "Yearn","SushiSwap","PancakeSwap","GMX","dYdX","Raydium","Jupiter",
    "Orca","Marinade","Jito","Drift","Tensor","Magic Eden"]

people = ["CZ Changpeng Zhao","Sam Bankman-Fried","Do Kwon","Vitalik Buterin",
    "Anatoly Yakovenko","Brian Armstrong","Paolo Ardoino","Giancarlo Devasini",
    "Barry Silbert","Mike Novogratz","Su Zhu","Kyle Davies","Arthur Hayes",
    "Andre Cronje","Hayden Adams","Rune Christensen","Robert Leshner",
    "Justin Sun","Charles Hoskinson","Gavin Wood","Raj Gokal","Chris Larsen",
    "Brad Garlinghouse","Jesse Powell","Cameron Winklevoss","Tyler Winklevoss",
    "Michael Saylor","Cathie Wood","Gary Gensler","Hester Peirce"]

chains = ["Ethereum","Solana","Arbitrum","Optimism","Base","Avalanche",
    "Polygon","BNB Chain","Cosmos","Polkadot"]

# 1. Token forensics (30)
for t in tickers:
    tasks.append(("crypto_forensic",
        f"CRYPTO FORENSIC: {t}\nDecompose last 30 days. Whale moves, DEX vs CEX volume, "
        f"funding rates, OI changes, social sentiment, on-chain metrics, correlation to BTC/ETH. "
        f"What drove each major move? What is NOT priced in? 60-day outlook.",
        f'{{"ticker":"{t}"}}'))

# 2. Exchange profiles (14)
for ex in exchanges:
    tasks.append(("crypto_exchange",
        f"EXCHANGE: {ex}\nJurisdiction, registration, proof of reserves, hack history, "
        f"regulatory actions, market share, wash trading estimates, insider trading allegations, "
        f"connection to market manipulation. Rate trustworthiness 1-10.",
        f'{{"exchange":"{ex}"}}'))

# 3. DeFi protocols (20)
for p in protocols:
    tasks.append(("defi_protocol",
        f"DEFI: {p}\nTVL trend, tokenomics, governance, audit history, exploit history, "
        f"team background, VC backers, unlock schedule, whale concentration, revenue model. "
        f"Overvalued or undervalued? Why?",
        f'{{"protocol":"{p}"}}'))

# 4. Person investigations (30)
for person in people:
    tasks.append(("crypto_person",
        f"CRYPTO PERSON: {person}\nBackground, net worth, known wallets (public), regulatory "
        f"history, offshore entities, political connections, track record, conflicts of interest, "
        f"current projects. Label each: confirmed/derived/estimated/rumored/inferred.",
        f'{{"person":"{person}"}}'))

# 5. Chain analysis (10)
for chain in chains:
    tasks.append(("chain_analysis",
        f"CHAIN: {chain}\nDaily active users, TVL, tx count, gas fees, DEX volume, bridge flows, "
        f"top protocols, dev activity, narrative momentum, institutional adoption. "
        f"Where is money flowing TO and FROM this chain?",
        f'{{"chain":"{chain}"}}'))

# 6. Stablecoins (5)
for s in ["USDT Tether","USDC Circle","DAI MakerDAO","FDUSD First Digital","USDe Ethena"]:
    tasks.append(("stablecoin",
        f"STABLECOIN: {s}\nMarket cap, reserves, audit status, peg stability history, "
        f"redemption mechanism, jurisdiction, regulatory risk, counterparty risk, "
        f"systemic importance. De-peg probability estimate.",
        f'{{"stablecoin":"{s}"}}'))

# 7. Crypto theses (5)
for t in ["Bitcoin digital gold vs risk asset","Ethereum deflationary post-merge",
          "Solana vs Ethereum L2s for DeFi","Crypto regulation impact on prices",
          "Stablecoin systemic risk to TradFi"]:
    tasks.append(("crypto_thesis",
        f"THESIS: {t}\nArguments for/against, historical evidence, current data, "
        f"key metrics to watch, timeline, trade expression, risk management.",
        f'{{"thesis":"{t}"}}'))

# 8. Market microstructure (5)
for t in ["MEV extraction on Ethereum","Solana validator centralization risk",
          "CEX wash trading estimates","Market makers Wintermute GSR DWF Labs",
          "Token launch manipulation low float high FDV"]:
    tasks.append(("crypto_microstructure",
        f"MICROSTRUCTURE: {t}\nMechanics, scale, key players, market impact, "
        f"regulatory implications, how retail gets hurt, how to detect, how to profit.",
        f'{{"topic":"{t}"}}'))

# 9. On-chain forensics (10)
for t in ["FTX collapse fund flows","Luna/UST death spiral mechanics",
          "Celsius insolvency timeline","3AC liquidation cascade",
          "Tether reserve composition changes over time",
          "Ethereum Foundation ETH sales pattern",
          "Jump Crypto pre-FTX movements","Alameda on-chain activity before collapse",
          "DWF Labs token accumulation patterns","Wintermute market making profitability"]:
    tasks.append(("onchain_forensic",
        f"ON-CHAIN FORENSIC: {t}\nReconstruct the timeline. What moved, when, between "
        f"which wallets, how much. Who knew what and when. Who profited from the chaos. "
        f"Label each finding by confidence level.",
        f'{{"topic":"{t}"}}'))

# 10. Crypto regulation landscape (5)
for t in ["SEC vs crypto 2023-2026 enforcement timeline",
          "EU MiCA regulation impact on exchanges",
          "Hong Kong crypto hub strategy vs Singapore retreat",
          "Crypto ETF approvals and market impact",
          "CBDC competition with stablecoins"]:
    tasks.append(("crypto_regulation",
        f"REGULATION: {t}\nCurrent state, key decisions, impact on prices, "
        f"which tokens/exchanges most affected, timeline for resolution, "
        f"investment implications.",
        f'{{"topic":"{t}"}}'))

log.info("Generated {} crypto tasks", len(tasks))

with engine.begin() as conn:
    for task_type, prompt, context in tasks:
        conn.execute(text(
            "INSERT INTO llm_task_backlog (task_type, prompt, context) "
            "VALUES (:t, :p, CAST(:c AS jsonb))"
        ), {"t": task_type, "p": prompt, "c": context})

log.info("QUEUED {} crypto tasks", len(tasks))

with engine.connect() as conn:
    r = conn.execute(text(
        "SELECT status, COUNT(*) FROM llm_task_backlog GROUP BY status ORDER BY status"
    )).fetchall()
    for row in r:
        log.info("  {}: {}", row[0], row[1])
