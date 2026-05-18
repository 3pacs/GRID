"""
Dataset generators for GRID Gemma fine-tuning.

Builds conversation-format training data for each micro model task:
  - signal_classifier: (signal text) → CATEGORY / URGENCY / REASON
  - anomaly_narrator: (anomaly data) → one-line narrative
  - edgar_extractor: (filing text + fields) → JSON extraction
  - hermes_operator: (operator/fleet request) → safe action plan

Data sources:
  - Synthetic examples hand-crafted from GRID domain knowledge
  - Optionally augmented from the GRID database (signal_registry, etc.)

Output format: HuggingFace Dataset with "conversations" column, compatible
with Unsloth's standardize_data_formats() and chat templates.
"""

from __future__ import annotations

import json
import random
from pathlib import Path
from typing import Any

from gemma.training.config import TASK_SYSTEM_PROMPTS, TaskType


def _conv(system: str, user: str, assistant: str) -> list[dict[str, str]]:
    """Build a single conversation in OpenAI format."""
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
        {"role": "assistant", "content": assistant},
    ]


# ---------------------------------------------------------------------------
# Signal Classifier training data
# ---------------------------------------------------------------------------

_SIGNAL_EXAMPLES: list[tuple[str, str]] = [
    # (input signal text, expected model output)
    (
        "Fed raised rates 25bp to 5.75%, dot plot shows 2 more hikes expected. "
        "Treasury yields spiked 12bp across the curve.",
        "CATEGORY: rates\nURGENCY: critical\nREASON: Fed rate hike directly impacts all rate-sensitive assets and reprices the yield curve immediately."
    ),
    (
        "Tether minted $1.5B USDT on Ethereum. On-chain data shows tokens moved "
        "to Binance and Coinbase within 2 hours.",
        "CATEGORY: crypto\nURGENCY: high\nREASON: Large USDT mint signals incoming crypto buying pressure as stablecoins flow to exchanges."
    ),
    (
        "Senator Pelosi purchased $1-5M in NVDA call options expiring in 6 months, "
        "disclosed 30 days after transaction.",
        "CATEGORY: insider\nURGENCY: medium\nREASON: Congressional trading disclosure on a mega-cap tech name may signal legislative awareness of upcoming AI policy."
    ),
    (
        "VIX spiked to 32 from 18 in a single session. VVIX also elevated at 145. "
        "Put/call ratio hit 1.8.",
        "CATEGORY: volatility\nURGENCY: critical\nREASON: Extreme vol spike with elevated VVIX suggests panic selling and potential forced liquidation cascades."
    ),
    (
        "Dark pool prints show $2.3B in SPY block trades at prices 0.5% below NBBO. "
        "FINRA ATS data confirms institutional distribution.",
        "CATEGORY: flows\nURGENCY: high\nREASON: Large dark pool distribution below market suggests institutional selling pressure ahead of public price discovery."
    ),
    (
        "US CPI came in at 4.1% vs 3.8% expected. Core CPI 3.9% vs 3.7% expected. "
        "Markets pricing in 85% chance of additional rate hike.",
        "CATEGORY: macro\nURGENCY: critical\nREASON: CPI beat raises inflation expectations and directly changes Fed rate path, repricing all risk assets."
    ),
    (
        "GDELT tension score between China-Taiwan spiked to 8.7 (99th percentile). "
        "Military exercises announced in Taiwan Strait.",
        "CATEGORY: geopolitical\nURGENCY: critical\nREASON: Extreme China-Taiwan tension score at 99th percentile signals potential military escalation affecting global supply chains."
    ),
    (
        "Unusual options activity: 50,000 AAPL $200 calls bought for next Friday at "
        "$0.15 each. Total premium $750K. Open interest was 2,000.",
        "CATEGORY: options\nURGENCY: medium\nREASON: Large OTM call sweep with 25x normal OI suggests speculative positioning ahead of potential catalyst."
    ),
    (
        "BTC whale address 1A1zP moved 15,000 BTC ($900M) to Binance hot wallet. "
        "This is the largest single transfer in 6 months.",
        "CATEGORY: crypto\nURGENCY: critical\nREASON: Massive BTC transfer to exchange signals potential sell-side pressure from a top-100 whale."
    ),
    (
        "Corporate credit spreads widened 45bp this week. HY CDX hit 450bp, "
        "highest since March 2023. IG spreads also moving.",
        "CATEGORY: credit\nURGENCY: high\nREASON: Rapid credit spread widening across IG and HY indicates deteriorating credit conditions and risk-off sentiment."
    ),
    (
        "Gold broke $2,500/oz, up 3.2% today. Silver following at $34. "
        "Dollar index weakened 0.8% to 101.2.",
        "CATEGORY: commodities\nURGENCY: medium\nREASON: Gold breakout with dollar weakness suggests flight to hard assets amid monetary uncertainty."
    ),
    (
        "EUR/USD broke above 1.15 for first time in 18 months. ECB hawkish hold "
        "while Fed signaled pause. Carry trade unwinding.",
        "CATEGORY: fx\nURGENCY: high\nREASON: Major EUR/USD breakout driven by divergent central bank policy creates significant FX risk for US multinationals."
    ),
    (
        "SPY saw $8.2B in ETF inflows today, largest single-day inflow in 3 months. "
        "QQQ added $3.1B. Small cap IWM saw $400M outflows.",
        "CATEGORY: flows\nURGENCY: medium\nREASON: Record ETF inflows into large-cap with small-cap outflows signals institutional rotation to quality."
    ),
    (
        "ISM Manufacturing PMI came in at 47.3, 4th consecutive month below 50. "
        "New orders sub-index at 44.1, employment contracting.",
        "CATEGORY: macro\nURGENCY: medium\nREASON: Sustained manufacturing contraction with weakening new orders suggests economic slowdown accelerating."
    ),
    (
        "SEC filed enforcement action against major crypto exchange for unregistered "
        "securities offering. Trading in 13 tokens halted.",
        "CATEGORY: equity\nURGENCY: high\nREASON: SEC enforcement action halting token trading creates immediate contagion risk across crypto-adjacent equities."
    ),
    (
        "Japan's FARA-registered lobbyist increased spending 300% QoQ on semiconductor "
        "export control lobbying. 14 new meetings with Commerce Dept.",
        "CATEGORY: geopolitical\nURGENCY: medium\nREASON: Surge in Japan semiconductor lobbying signals potential trade policy shift affecting chip supply chains."
    ),
    (
        "Insider cluster: 5 directors at XYZ Corp bought $12M in shares within 3 days. "
        "Stock down 40% from highs. No pending SEC filings.",
        "CATEGORY: insider\nURGENCY: high\nREASON: Coordinated insider buying cluster during significant drawdown signals strong conviction from company leadership."
    ),
    (
        "Baltic Dry Index dropped 15% this week to 1,200. Container shipping rates "
        "also falling. Port congestion easing globally.",
        "CATEGORY: commodities\nURGENCY: low\nREASON: Declining shipping indices suggest easing supply chain pressures and potential demand softening."
    ),
    (
        "Polymarket prediction: 73% probability of government shutdown in 2 weeks, "
        "up from 45% yesterday. Rapid shift in betting odds.",
        "CATEGORY: macro\nURGENCY: medium\nREASON: Sharp rise in shutdown probability on prediction markets signals increasing political risk for near-term fiscal policy."
    ),
    (
        "Reddit WSB mentions of GME spiked 800% in 24 hours. Sentiment shifted from "
        "bearish to extremely bullish. Short interest still at 25%.",
        "CATEGORY: equity\nURGENCY: medium\nREASON: Social media attention spike on heavily shorted stock suggests potential retail-driven squeeze momentum building."
    ),
    (
        "Fed reverse repo facility usage dropped to $200B from $500B last month. "
        "Bank reserves increasing. M2 money supply ticking up.",
        "CATEGORY: rates\nURGENCY: medium\nREASON: Declining RRP usage with rising reserves indicates improving liquidity conditions supporting risk assets."
    ),
    (
        "Whale alert: $450M in ETH moved from cold storage to multiple DEX contracts. "
        "Gas fees spiking 5x normal. MEV bot activity surging.",
        "CATEGORY: crypto\nURGENCY: high\nREASON: Massive ETH movement to DEXs with elevated gas and MEV suggests large DeFi position being established or liquidated."
    ),
    (
        "13F filing: Berkshire Hathaway disclosed new $2.1B position in undisclosed "
        "insurance company. Reduced Apple stake by 25%.",
        "CATEGORY: flows\nURGENCY: medium\nREASON: Major institutional portfolio rebalance by Berkshire signals sector rotation from tech to insurance/value."
    ),
    (
        "NOAA satellite data shows Mississippi River water levels at historic lows. "
        "Barge traffic restricted. Grain transport disrupted.",
        "CATEGORY: commodities\nURGENCY: medium\nREASON: River transport disruption threatens grain supply chain, potentially driving agricultural commodity prices higher."
    ),
]


# ---------------------------------------------------------------------------
# Anomaly Narrator training data
# ---------------------------------------------------------------------------

_ANOMALY_EXAMPLES: list[tuple[str, str]] = [
    (
        "Feature: US_CPI_YOY\nValue: 5.2%\nExpected: 3.8%\nZ-score: 4.1\n"
        "Period: 2026-03\nContext: Previous 3 readings: 3.7%, 3.8%, 3.9%",
        "US CPI surged to 5.2% in March (+4.1σ), a 140bp beat over consensus that breaks the disinflationary trend of the prior three months."
    ),
    (
        "Feature: VIX_CLOSE\nValue: 45.2\nExpected: 18.5\nZ-score: 5.8\n"
        "Period: 2026-03-15\nContext: Was 16.3 just 3 days ago. SPX down 4.2% today.",
        "VIX exploded to 45.2 (+5.8σ) from 16.3 three days ago, a 177% spike accompanying a 4.2% SPX crash — signaling extreme fear and potential forced liquidation."
    ),
    (
        "Feature: DARK_POOL_NET_FLOW_SPY\nValue: -$4.2B\nExpected: -$200M\n"
        "Z-score: -3.7\nPeriod: 2026-03-12\nContext: Average daily flow is +$100M. "
        "This is the largest outflow in 2 years.",
        "SPY dark pool net flow hit -$4.2B (-3.7σ), a 21x deviation from the +$100M daily average and the largest institutional distribution in two years."
    ),
    (
        "Feature: BTC_EXCHANGE_NETFLOW\nValue: +42,000 BTC\nExpected: +2,000 BTC\n"
        "Z-score: 4.5\nPeriod: 2026-03-10\nContext: Binance received 60% of inflow. "
        "Price dropped 8% in following 6 hours.",
        "Exchange net inflows surged to 42,000 BTC (+4.5σ), with Binance absorbing 60% — a 21x spike over normal that preceded an 8% price drop within hours."
    ),
    (
        "Feature: CREDIT_SPREAD_HY\nValue: 580bp\nExpected: 350bp\nZ-score: 3.9\n"
        "Period: 2026-03-08\nContext: Widened 230bp in one week. Investment grade "
        "spreads also widening. Last seen at this level in March 2020.",
        "High-yield credit spreads blew out to 580bp (+3.9σ), widening 230bp in a single week to levels last seen during the March 2020 liquidity crisis."
    ),
    (
        "Feature: FED_NET_LIQUIDITY\nValue: $3.1T\nExpected: $3.8T\nZ-score: -3.2\n"
        "Period: 2026-03-05\nContext: Dropped $700B in 30 days due to QT + TGA rebuild.",
        "Fed net liquidity plunged to $3.1T (-3.2σ), draining $700B in 30 days from combined QT acceleration and Treasury General Account rebuilding."
    ),
    (
        "Feature: CONGRESSIONAL_TRADE_VOLUME\nValue: $89M\nExpected: $12M\n"
        "Z-score: 3.4\nPeriod: 2026-03-W10\nContext: 7.4x normal weekly volume. "
        "Concentrated in defense and pharma sectors.",
        "Congressional trading volume hit $89M (+3.4σ), 7.4x the normal weekly rate and concentrated in defense and pharma — suggesting legislative awareness of pending policy."
    ),
    (
        "Feature: GDELT_TENSION_US_CN\nValue: 9.1\nExpected: 4.2\nZ-score: 4.8\n"
        "Period: 2026-03-14\nContext: Highest since 2022 Taiwan crisis. "
        "Military keyword frequency up 340%.",
        "US-China GDELT tension spiked to 9.1 (+4.8σ) with military keyword frequency up 340%, the highest reading since the 2022 Taiwan Strait crisis."
    ),
    (
        "Feature: ISM_PMI_MFG\nValue: 41.2\nExpected: 48.5\nZ-score: -3.6\n"
        "Period: 2026-03\nContext: 6th consecutive month of contraction. "
        "New orders at 38.1, worst since 2008.",
        "ISM Manufacturing PMI collapsed to 41.2 (-3.6σ) with new orders at 38.1 — the worst reading since 2008 and the 6th straight month of contraction."
    ),
    (
        "Feature: ETH_GAS_PRICE\nValue: 450 gwei\nExpected: 25 gwei\nZ-score: 6.2\n"
        "Period: 2026-03-13T14:30Z\nContext: MEV bot war on Uniswap V4 pool. "
        "$1.2B in DEX volume in 1 hour.",
        "Ethereum gas exploded to 450 gwei (+6.2σ), an 18x spike driven by a MEV bot war over $1.2B in Uniswap V4 volume within a single hour."
    ),
    (
        "Feature: MARGIN_DEBT_TOTAL\nValue: $920B\nExpected: $750B\nZ-score: 2.8\n"
        "Period: 2026-02\nContext: Increased $170B in 2 months. Approaching 2021 peak of $936B.",
        "Total margin debt surged to $920B (+2.8σ), adding $170B in two months and approaching the 2021 speculative peak of $936B."
    ),
    (
        "Feature: USD_JPY\nValue: 162.5\nExpected: 148.0\nZ-score: 3.1\n"
        "Period: 2026-03-11\nContext: BOJ intervention rumored. "
        "Last BOJ intervention was at 160.2 in 2024.",
        "USD/JPY rocketed to 162.5 (+3.1σ), blowing past the 160.2 level that triggered BOJ intervention in 2024 and sparking fresh intervention rumors."
    ),
]


# ---------------------------------------------------------------------------
# EDGAR Extractor training data
# ---------------------------------------------------------------------------

_EDGAR_EXAMPLES: list[tuple[str, str]] = [
    (
        "Extract these fields: company_name, filing_type, total_revenue, net_income, eps_diluted, filing_date\n\n"
        "Filing text:\n"
        "APPLE INC\nFORM 10-Q\nFor the quarterly period ended December 30, 2025\n"
        "Total net revenue: $124.3 billion\nNet income: $33.9 billion\n"
        "Earnings per share (diluted): $2.18\nFiled: January 31, 2026",
        '{"company_name": "Apple Inc", "filing_type": "10-Q", "total_revenue": 124300000000, "net_income": 33900000000, "eps_diluted": 2.18, "filing_date": "2026-01-31"}'
    ),
    (
        "Extract these fields: company_name, insider_name, transaction_type, shares, price_per_share, total_value, transaction_date\n\n"
        "Filing text:\n"
        "FORM 4 — STATEMENT OF CHANGES IN BENEFICIAL OWNERSHIP\n"
        "Issuer: NVIDIA CORPORATION\nReporting Person: Jensen Huang\n"
        "Transaction: Sale\nShares: 120,000\nPrice: $875.50\n"
        "Total value: $105,060,000\nDate of transaction: March 5, 2026",
        '{"company_name": "NVIDIA Corporation", "insider_name": "Jensen Huang", "transaction_type": "Sale", "shares": 120000, "price_per_share": 875.50, "total_value": 105060000, "transaction_date": "2026-03-05"}'
    ),
    (
        "Extract these fields: company_name, filing_type, total_assets, total_liabilities, stockholders_equity, cash_and_equivalents\n\n"
        "Filing text:\n"
        "MICROSOFT CORPORATION\nFORM 10-K\nFiscal year ended June 30, 2025\n\n"
        "BALANCE SHEET (in millions):\n"
        "Total assets: $484,275\nTotal liabilities: $225,618\n"
        "Total stockholders' equity: $258,657\n"
        "Cash and cash equivalents: $34,704\nShort-term investments: $76,558",
        '{"company_name": "Microsoft Corporation", "filing_type": "10-K", "total_assets": 484275000000, "total_liabilities": 225618000000, "stockholders_equity": 258657000000, "cash_and_equivalents": 34704000000}'
    ),
    (
        "Extract these fields: filer_name, subject_company, ownership_percent, shares_held, filing_date, purpose\n\n"
        "Filing text:\n"
        "SCHEDULE 13D\nFiled by: Elliott Investment Management L.P.\n"
        "Subject: Southwest Airlines Co.\n"
        "Shares of Common Stock beneficially owned: 54,200,000\n"
        "Percent of class: 9.7%\nDate of event: February 15, 2026\n"
        "Purpose: The Reporting Person intends to engage with the Board "
        "regarding operational improvements and capital allocation.",
        '{"filer_name": "Elliott Investment Management L.P.", "subject_company": "Southwest Airlines Co.", "ownership_percent": 9.7, "shares_held": 54200000, "filing_date": "2026-02-15", "purpose": "Engage with Board regarding operational improvements and capital allocation"}'
    ),
    (
        "Extract these fields: company_name, offering_type, shares_offered, price_per_share, total_proceeds, underwriter\n\n"
        "Filing text:\n"
        "PALANTIR TECHNOLOGIES INC.\nFORM S-3 PROSPECTUS SUPPLEMENT\n\n"
        "We are offering 15,000,000 shares of our Class A common stock at "
        "a public offering price of $42.00 per share.\n"
        "Total gross proceeds: $630,000,000\n"
        "Underwriter: Goldman Sachs & Co. LLC",
        '{"company_name": "Palantir Technologies Inc.", "offering_type": "Secondary Offering (S-3)", "shares_offered": 15000000, "price_per_share": 42.00, "total_proceeds": 630000000, "underwriter": "Goldman Sachs & Co. LLC"}'
    ),
    (
        "Extract these fields: company_name, segment_name, segment_revenue, segment_operating_income, yoy_growth\n\n"
        "Filing text:\n"
        "ALPHABET INC\nFORM 10-Q\nQuarter ended September 30, 2025\n\n"
        "Google Cloud segment:\nRevenue: $12.4 billion (up 28% year-over-year)\n"
        "Operating income: $1.95 billion\n"
        "Operating margin: 15.7%",
        '{"company_name": "Alphabet Inc", "segment_name": "Google Cloud", "segment_revenue": 12400000000, "segment_operating_income": 1950000000, "yoy_growth": 28.0}'
    ),
    (
        "Extract these fields: company_name, risk_factor, severity, description\n\n"
        "Filing text:\n"
        "COINBASE GLOBAL INC\nFORM 10-K\n\n"
        "RISK FACTORS:\nRegulatory Risk — Critical\n"
        "We are subject to an ongoing SEC enforcement action alleging that "
        "certain digital assets available on our platform are unregistered "
        "securities. An adverse outcome could require us to delist a "
        "significant portion of our trading pairs and materially reduce revenue.",
        '{"company_name": "Coinbase Global Inc", "risk_factor": "Regulatory Risk", "severity": "Critical", "description": "Ongoing SEC enforcement action alleging unregistered securities could require delisting significant trading pairs and materially reduce revenue"}'
    ),
    (
        "Extract these fields: company_name, executive_name, compensation_total, base_salary, stock_awards, option_awards\n\n"
        "Filing text:\n"
        "TESLA INC\nDEF 14A — PROXY STATEMENT\n\n"
        "Summary Compensation Table — CEO\n"
        "Name: Elon Musk\nBase salary: $0\nStock awards: $0\n"
        "Option awards: $0\nAll other compensation: $1,812,274\n"
        "Total compensation: $1,812,274",
        '{"company_name": "Tesla Inc", "executive_name": "Elon Musk", "compensation_total": 1812274, "base_salary": 0, "stock_awards": 0, "option_awards": 0}'
    ),
]


# ---------------------------------------------------------------------------
# Knowledge Mapper training data
# ---------------------------------------------------------------------------

_KNOWLEDGE_MAPPER_EXAMPLES: list[tuple[str, str]] = [
    (
        "Fed Chair Powell announced a 25bp rate hike to 5.75%, citing persistent "
        "inflation in services. Dot plot shows 2 more hikes expected in 2026. "
        "Treasury yields spiked 12bp across the curve. Mortgage rates hit 8.1%.",

        "## Federal Reserve Rate Hike (5.75%, April 2026)\n"
        "[[Federal Reserve|Fed]] Chair [[Jerome Powell]] raised the [[federal funds rate]] "
        "by 25bp to 5.75%, the highest since 2007, citing persistent [[services inflation]]. "
        "The [[dot plot]] signals 2 more hikes in 2026, pushing [[Treasury yields]] up 12bp "
        "and [[mortgage rates]] to 8.1%.\n\n"
        "### Connections\n"
        "- [[Jerome Powell]] → [[FOMC]] → [[dot plot]]: Powell's hawkish guidance directly "
        "sets market expectations for the [[terminal rate]]\n"
        "- [[Federal funds rate]] → [[mortgage rates]] → [[housing market]]: Rate hike "
        "transmission chain — 8.1% mortgages will suppress [[home sales]] and [[construction spending]]\n"
        "- [[Services inflation]] → [[wage growth]] → [[labor market]]: Persistent services "
        "inflation implies [[wage-price spiral]] risk, connecting to [[BLS employment data]]\n"
        "- [[Treasury yields]] → [[bank unrealized losses]] → [[regional banks]]: Higher "
        "yields deepen [[held-to-maturity]] losses at banks still holding 2020-2021 vintage bonds\n"
        "- [[Rate hike]] → [[USD strength]] → [[EM debt crisis]]: Dollar appreciation "
        "pressures [[emerging market]] dollar-denominated debt, especially [[Turkey]], [[Argentina]]\n\n"
        "### See Also\n"
        "[[FOMC Meeting]], [[Yield Curve]], [[Credit Spreads]], [[Bank Stress Tests]], "
        "[[Real Estate REITs]], [[Mortgage-Backed Securities]]"
    ),
    (
        "Tether minted $1.5B USDT on Ethereum and transferred to Binance and "
        "Coinbase within 2 hours. On-chain data shows this is the largest single "
        "mint in 3 months.",

        "## Tether $1.5B USDT Mint (Ethereum)\n"
        "[[Tether]] minted $1.5B [[USDT]] on [[Ethereum]], with tokens flowing to "
        "[[Binance]] and [[Coinbase]] within 2 hours — the largest single mint in 3 months, "
        "signaling imminent buying pressure in [[crypto markets]].\n\n"
        "### Connections\n"
        "- [[Tether]] → [[USDT]] → [[exchange inflows]]: Stablecoin mints are a leading "
        "indicator of [[crypto liquidity]] — historically precede BTC rallies by 24-72 hours\n"
        "- [[Binance]] + [[Coinbase]] split: Dual-exchange flow suggests both [[retail]] "
        "(Coinbase) and [[offshore/institutional]] (Binance) demand, not single-whale activity\n"
        "- [[Tether reserves]] → [[US Treasury bills]]: Tether holds ~$80B in T-bills; "
        "large mints imply fresh dollar deposits, connecting to [[money market fund flows]]\n"
        "- [[USDT on Ethereum]] → [[DeFi TVL]]: Some portion likely routes through "
        "[[Aave]], [[Compound]], [[Uniswap]] — watch for [[TVL spikes]] in DeFi protocols\n"
        "- [[Stablecoin supply]] → [[crypto market cap]]: Stablecoin supply ratio (SSR) "
        "is a measure of [[crypto buying power]] relative to market size\n\n"
        "### See Also\n"
        "[[Circle USDC]], [[Crypto Exchange Flows]], [[Bitcoin ETF Flows]], "
        "[[DeFi Liquidity]], [[Stablecoin Dominance]]"
    ),
    (
        "Senator Pelosi purchased $1-5M in NVDA call options expiring in 6 months. "
        "NVDA is a key beneficiary of the CHIPS Act. Her husband previously traded "
        "NVDA in 2021 before the semiconductor subsidies were announced.",

        "## Congressional Trade: Pelosi NVDA Calls ($1-5M)\n"
        "[[Nancy Pelosi]] purchased $1-5M in [[NVIDIA|NVDA]] call options with "
        "6-month expiry. [[NVDA]] is a primary beneficiary of the [[CHIPS Act]], and "
        "her husband [[Paul Pelosi]] made similar trades in 2021 before semiconductor "
        "subsidies were announced.\n\n"
        "### Connections\n"
        "- [[Nancy Pelosi]] → [[House committees]] → [[CHIPS Act]]: As former Speaker, "
        "Pelosi has advance knowledge of [[semiconductor policy]] and [[export controls]]\n"
        "- [[Paul Pelosi]] → [[NVDA 2021 trades]] → [[CHIPS Act announcement]]: Pattern "
        "of trading semiconductor stocks ahead of favorable legislation — connects to "
        "[[congressional insider trading]] debate\n"
        "- [[NVDA]] → [[AI compute demand]] → [[data center capex]]: NVDA options are "
        "a bet on continued [[AI infrastructure]] spending by [[hyperscalers]] "
        "([[Microsoft]], [[Google]], [[Amazon]])\n"
        "- [[CHIPS Act]] → [[semiconductor reshoring]] → [[TSMC Arizona]]: Policy chain "
        "connecting US chip subsidies to [[Taiwan]] supply chain diversification\n"
        "- [[Congressional trading]] → [[STOCK Act]] → [[insider trading reform]]: "
        "Trade disclosure connects to ongoing legislative efforts to ban "
        "[[congressional stock trading]]\n\n"
        "### See Also\n"
        "[[Congressional Trading Tracker]], [[CHIPS and Science Act]], "
        "[[Semiconductor Supply Chain]], [[Export Controls China]], [[AI Policy]]"
    ),
    (
        "GDELT tension score between US and China spiked to 9.1 (99th percentile). "
        "Military exercises announced in Taiwan Strait. Simultaneously, Japan's "
        "FARA-registered lobbyist increased semiconductor export control lobbying 300%.",

        "## US-China Tension Spike (GDELT 9.1, 99th Percentile)\n"
        "[[GDELT]] [[US-China tension]] score hit 9.1 (99th percentile) amid "
        "[[Taiwan Strait]] military exercises. Concurrently, Japan's [[FARA]]-registered "
        "lobbyists surged [[semiconductor export control]] lobbying 300% — revealing "
        "a coordinated allied response.\n\n"
        "### Connections\n"
        "- [[Taiwan Strait exercises]] → [[TSMC]] → [[global chip supply]]: Military "
        "escalation threatens [[TSMC]]'s fabrication, which produces ~90% of advanced "
        "chips for [[Apple]], [[NVIDIA]], [[AMD]], [[Qualcomm]]\n"
        "- [[GDELT tension]] → [[defense stocks]] → [[Lockheed Martin]], [[Raytheon]]: "
        "Historical pattern: GDELT >8.0 correlates with 3-5% defense sector outperformance "
        "within 2 weeks\n"
        "- Japan [[FARA]] lobbying → [[semiconductor export controls]] → [[Tokyo Electron]], "
        "[[ASML]]: Japan lobbying signals pending alignment with US [[chip export restrictions]], "
        "pressuring equipment makers\n"
        "- [[US-China tension]] → [[CNY depreciation]] → [[EM contagion]]: Geopolitical "
        "stress typically triggers [[capital flight]] from China, weakening [[yuan]] and "
        "spreading to [[Asian FX]]\n"
        "- [[Military exercises]] → [[shipping insurance rates]] → [[Baltic Dry Index]]: "
        "Strait disruption risk reprices maritime insurance, affecting [[container shipping]] "
        "and [[commodity transport]] costs\n\n"
        "### See Also\n"
        "[[Taiwan Contingency]], [[Semiconductor Supply Chain]], [[AUKUS]], "
        "[[South China Sea]], [[Japan Rearmament]], [[FARA Foreign Lobbying]]"
    ),
    (
        "Elliott Management took a 9.7% stake in Southwest Airlines ($54.2M shares), "
        "filing 13D with stated purpose to 'engage with the Board regarding operational "
        "improvements and capital allocation.'",

        "## Elliott Management 13D: Southwest Airlines (9.7% Stake)\n"
        "[[Elliott Management]] disclosed a 9.7% stake in [[Southwest Airlines|LUV]] "
        "via [[Schedule 13D]], signaling an activist campaign focused on "
        "operational improvements and [[capital allocation]].\n\n"
        "### Connections\n"
        "- [[Elliott Management]] → [[activist campaign]] → [[board seats]]: Elliott's "
        "13D filing historically precedes proxy fights — connects to [[corporate governance]] "
        "and [[shareholder activism]] trends\n"
        "- [[Southwest Airlines]] → [[Boeing 737 MAX]] → [[fleet issues]]: LUV's operational "
        "problems trace partly to [[Boeing]] delivery delays, connecting to broader "
        "[[aerospace supply chain]] disruption\n"
        "- [[Capital allocation]] → [[buybacks]] vs [[fleet renewal]]: Elliott likely pushes "
        "for [[share buybacks]] or sale-leasebacks, potentially at expense of [[fleet modernization]]\n"
        "- [[Elliott 13D]] → [[airline sector consolidation]]: Activist pressure may push "
        "LUV toward M&A — connects to [[JetBlue]], [[Spirit Airlines]] consolidation wave\n"
        "- [[Operational improvements]] → [[labor costs]] → [[pilot union negotiations]]: "
        "Cost-cutting pressure connects to ongoing [[SWAPA]] pilot contract talks\n\n"
        "### See Also\n"
        "[[Activist Investors]], [[Airline Industry]], [[Boeing Delays]], "
        "[[13D Filings]], [[Proxy Fights]], [[Carl Icahn]]"
    ),
    (
        "VIX spiked from 16 to 45 in 3 days. SPX down 4.2% in a single session. "
        "Dark pool SPY outflows hit -$4.2B. Credit spreads widened 230bp in a week. "
        "Fed reverse repo dropped $700B in 30 days.",

        "## Multi-Signal Stress Event (VIX 45, SPX -4.2%)\n"
        "A convergence of stress signals: [[VIX]] exploded from 16 to 45, [[SPX]] crashed "
        "4.2%, [[dark pool]] SPY outflows hit -$4.2B, [[credit spreads]] widened 230bp, "
        "and [[Fed net liquidity]] drained $700B — indicating a systemic [[liquidity crisis]].\n\n"
        "### Connections\n"
        "- [[VIX spike]] → [[dealer gamma]] → [[forced hedging]]: VIX >40 forces dealers "
        "into [[negative gamma]] territory, amplifying moves as they delta-hedge — connects "
        "to [[options market structure]]\n"
        "- [[Dark pool outflows]] → [[institutional selling]] → [[13F disclosures]]: $4.2B "
        "distribution signals large funds de-risking, will appear in next quarter's "
        "[[institutional flows]] data\n"
        "- [[Credit spread widening]] → [[corporate refinancing risk]] → [[high yield defaults]]: "
        "230bp widening in a week approaches [[March 2020]] levels — [[zombie companies]] "
        "face refinancing walls\n"
        "- [[Fed liquidity drain]] → [[reverse repo]] → [[bank reserves]]: $700B drain "
        "from combined [[QT]] and [[TGA rebuild]] is the root cause — connects to "
        "[[money market stress]] and [[repo rate spikes]]\n"
        "- [[VIX + credit + liquidity]] → [[regime transition]]: Multi-factor stress "
        "suggests [[GROWTH → CRISIS regime]] transition in the [[regime detection]] model\n\n"
        "### See Also\n"
        "[[Liquidity Crisis]], [[Regime Detection]], [[Dealer Gamma Exposure]], "
        "[[Margin Calls]], [[Flight to Quality]], [[Treasury Market Stress]]"
    ),
    (
        "ClinicalTrials.gov shows Phase 3 trial for Vertex Pharmaceuticals' pain drug "
        "VX-548 is 'Active, Not Recruiting' with expected readout in 60 days. "
        "Market cap $98B. FDA granted Breakthrough Therapy designation.",

        "## Vertex VX-548 Phase 3 Readout (60 Days)\n"
        "[[Vertex Pharmaceuticals|VRTX]] Phase 3 trial for [[VX-548]] (non-opioid pain) "
        "is Active/Not Recruiting with readout in 60 days. [[FDA]] granted "
        "[[Breakthrough Therapy]] designation — a high-conviction [[catalyst event]].\n\n"
        "### Connections\n"
        "- [[VX-548]] → [[non-opioid pain market]] → [[opioid crisis]]: Success disrupts "
        "the $20B+ pain market dominated by [[opioids]], connecting to [[DEA policy]] "
        "and [[addiction treatment]] spending\n"
        "- [[Breakthrough Therapy]] → [[accelerated approval]] → [[FDA priority review]]: "
        "BT designation means faster review — connects to [[FDA calendar]] and "
        "[[PDUFA dates]] tracking\n"
        "- [[Phase 3 readout]] → [[implied volatility]] → [[options pricing]]: Binary "
        "event creates [[IV crush]] opportunity — connects to [[options scanner]] "
        "mispricing detection\n"
        "- [[Vertex]] → [[cystic fibrosis franchise]] → [[Trikafta revenue]]: VX-548 "
        "represents pipeline diversification beyond CF — connects to [[biotech "
        "pipeline valuation]] models\n"
        "- [[Non-opioid pain]] → [[Congress]] → [[pharma lobbying]]: Opioid alternative "
        "development connects to [[pharmaceutical lobbying]] and [[campaign finance]] "
        "from drug manufacturers\n\n"
        "### See Also\n"
        "[[Trial Gem Hunter]], [[Catalyst Calendar]], [[Biotech Pipeline]], "
        "[[FDA Approvals]], [[Options Catalyst Plays]], [[Pain Therapeutics Market]]"
    ),
    (
        "Mississippi River water levels at historic lows per NOAA satellite data. "
        "Barge traffic restricted. Grain transport disrupted. Similar event in 2022 "
        "caused $20B in agricultural losses.",

        "## Mississippi River Low Water Crisis\n"
        "[[NOAA]] satellite data shows [[Mississippi River]] water levels at historic lows, "
        "restricting [[barge traffic]] and disrupting [[grain transport]]. A similar event "
        "in 2022 caused $20B in [[agricultural losses]].\n\n"
        "### Connections\n"
        "- [[Mississippi River]] → [[grain transport]] → [[corn]], [[soybean]], [[wheat]] "
        "futures: 60% of US grain exports transit the Mississippi — disruption directly "
        "reprices [[agricultural commodities]]\n"
        "- [[Barge restrictions]] → [[railroad substitution]] → [[Union Pacific]], "
        "[[BNSF]]: Transport mode switching increases rail volume and pricing power — "
        "connects to [[transportation sector]] earnings\n"
        "- [[Low water levels]] → [[La Nina]] → [[climate patterns]]: Connects to "
        "broader [[drought cycle]] and [[ENSO]] forecasting for agricultural planning\n"
        "- [[Agricultural losses]] → [[crop insurance]] → [[USDA disaster payments]]: "
        "$20B loss precedent connects to [[federal agricultural spending]] and "
        "[[farm bill politics]]\n"
        "- [[Grain disruption]] → [[global food prices]] → [[social instability]]: "
        "US grain supply shocks transmit to [[World Food Programme]] import nations, "
        "connecting to [[geopolitical risk]] in [[MENA]] and [[Sub-Saharan Africa]]\n\n"
        "### See Also\n"
        "[[Supply Chain Disruption]], [[Agricultural Commodities]], [[Climate Risk]], "
        "[[NOAA Data]], [[Baltic Dry Index]], [[Food Security]]"
    ),
]


# ---------------------------------------------------------------------------
# Hermes Operator training data
# ---------------------------------------------------------------------------

_HERMES_OPERATOR_EXAMPLES: list[tuple[str, str]] = [
    (
        "/fleet panda render lane",
        "I will run a read-only fleet audit scoped to panda, compare GPU/service state with queued render work, and write findings to the durable report path. I will not restart or rebind services without explicit approval.",
    ),
    (
        "/restart grid-hermes",
        "That is a risky command. I need an approval token from the same allowlisted sender before queueing the restart, and I will only target the allowlisted grid-hermes unit.",
    ),
    (
        "Several sessions crashed. What should Hermes do first?",
        "Start from durable state: Obsidian agent reports, the central Agent-TODO file, current git worktrees, and live service health. Mark completed items off the list before opening new work, then leave a synced handoff report.",
    ),
    (
        "/render Storymill Sia walk cycle",
        "Queue the render request with the approved reference set attached, keep generated assets on grid-svr or scratch storage, record provenance, and route candidates through the fail-closed QC gate before promotion.",
    ),
]


def build_signal_classifier_dataset() -> list[dict[str, Any]]:
    """Build training conversations for signal classification."""
    system = TASK_SYSTEM_PROMPTS[TaskType.SIGNAL_CLASSIFIER]
    return [
        {"conversations": _conv(system, inp, out)}
        for inp, out in _SIGNAL_EXAMPLES
    ]


def build_anomaly_narrator_dataset() -> list[dict[str, Any]]:
    """Build training conversations for anomaly narration."""
    system = TASK_SYSTEM_PROMPTS[TaskType.ANOMALY_NARRATOR]
    return [
        {"conversations": _conv(system, inp, out)}
        for inp, out in _ANOMALY_EXAMPLES
    ]


def build_edgar_extractor_dataset() -> list[dict[str, Any]]:
    """Build training conversations for EDGAR extraction."""
    system = TASK_SYSTEM_PROMPTS[TaskType.EDGAR_EXTRACTOR]
    return [
        {"conversations": _conv(system, inp, out)}
        for inp, out in _EDGAR_EXAMPLES
    ]


def build_knowledge_mapper_dataset() -> list[dict[str, Any]]:
    """Build training conversations for knowledge mapping with backlinks."""
    system = TASK_SYSTEM_PROMPTS[TaskType.KNOWLEDGE_MAPPER]
    return [
        {"conversations": _conv(system, inp, out)}
        for inp, out in _KNOWLEDGE_MAPPER_EXAMPLES
    ]


def build_hermes_operator_dataset() -> list[dict[str, Any]]:
    """Build seed training conversations for safe Hermes operator behavior."""
    system = TASK_SYSTEM_PROMPTS[TaskType.HERMES_OPERATOR]
    return [
        {"conversations": _conv(system, inp, out)}
        for inp, out in _HERMES_OPERATOR_EXAMPLES
    ]


_DATASET_BUILDERS = {
    TaskType.SIGNAL_CLASSIFIER: build_signal_classifier_dataset,
    TaskType.ANOMALY_NARRATOR: build_anomaly_narrator_dataset,
    TaskType.EDGAR_EXTRACTOR: build_edgar_extractor_dataset,
    TaskType.KNOWLEDGE_MAPPER: build_knowledge_mapper_dataset,
    TaskType.HERMES_OPERATOR: build_hermes_operator_dataset,
}


def build_dataset(task: TaskType, shuffle: bool = True, seed: int = 42) -> list[dict[str, Any]]:
    """Build training dataset for a given task.

    Parameters:
        task: Which micro model task to build data for.
        shuffle: Whether to shuffle the examples.
        seed: Random seed for shuffling.

    Returns:
        List of conversation dicts ready for HuggingFace Dataset.
    """
    builder = _DATASET_BUILDERS[task]
    data = builder()
    if shuffle:
        rng = random.Random(seed)
        rng.shuffle(data)
    return data


def save_dataset_jsonl(task: TaskType, output_path: str | Path) -> Path:
    """Build and save a dataset as JSONL.

    Parameters:
        task: Which micro model task.
        output_path: Where to save the .jsonl file.

    Returns:
        Path to the saved file.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    data = build_dataset(task)
    with open(output_path, "w") as f:
        for row in data:
            f.write(json.dumps(row) + "\n")

    return output_path


def load_dataset_for_training(task: TaskType, dataset_path: str | None = None):
    """Load dataset as a HuggingFace Dataset object.

    Parameters:
        task: Which task (used if dataset_path is None).
        dataset_path: Path to a .jsonl file. If None, generates synthetic data.

    Returns:
        HuggingFace Dataset with 'conversations' column.
    """
    from datasets import Dataset

    if dataset_path:
        return Dataset.from_json(dataset_path)

    data = build_dataset(task)
    return Dataset.from_list(data)
