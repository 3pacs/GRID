"""
GRID Intelligence — Crypto Exchange Network Report
Generated: 2026-03-28
Confidence labels: confirmed / derived / estimated / rumored / inferred
"""

import json

CRYPTO_EXCHANGE_NETWORK_INTEL = {
    "report_meta": {
        "generated": "2026-03-28",
        "classification": "GRID_INTERNAL",
        "version": "1.0",
        "confidence_scale": "confirmed > derived > estimated > rumored > inferred",
        "sources": [
            "SEC filings", "CFTC enforcement", "DOJ plea agreements",
            "NBER working papers", "BDO Italy attestations", "CoinGecko",
            "The Block", "WSJ investigations", "MFSA public statements",
            "FTX bankruptcy court filings", "OpenSecrets.org", "ICIJ leaks"
        ],
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 1. BINANCE
    # ═══════════════════════════════════════════════════════════════════════
    "binance": {
        "entity_type": "centralized_exchange",
        "jurisdiction": {
            "registered": "No formal HQ — claims 'decentralized' corporate structure",
            "operating_entities": [
                "Binance Holdings Ltd (Cayman Islands)",
                "Binance.US (BAM Trading Services Inc, San Francisco)",
                "Binance Markets Ltd (UK — FCA banned June 2021)",
            ],
            "jurisdiction_hopping_timeline": {
                "2017_Q3": {"location": "China (Shanghai)", "event": "Founded by CZ", "confidence": "confirmed"},
                "2017_Q4": {"location": "Japan", "event": "Fled China crypto ban Sep 2017", "confidence": "confirmed"},
                "2018_Q1": {"location": "Malta announced", "event": "Announced Malta move Mar 2018", "confidence": "confirmed"},
                "2019_Q4": {"location": "Malta abandoned", "event": "Never obtained Malta license; MFSA denied jurisdiction Feb 2020", "confidence": "confirmed"},
                "2020_ongoing": {"location": "No stated HQ", "event": "Operates from Cayman, Dubai, multiple shell entities", "confidence": "confirmed"},
                "2023_Q4": {"location": "US DOJ settlement", "event": "$4.3B fine, CZ steps down Nov 2023", "confidence": "confirmed"},
            },
            "regulatory_status": "Operating under DOJ/FinCEN compliance monitor through 2028",
            "confidence": "confirmed",
        },
        "key_people": [
            {
                "name": "Changpeng Zhao (CZ)",
                "role": "Founder, former CEO (stepped down Nov 2023)",
                "background": "Chinese-Canadian, previously at Bloomberg Tradebook and OKCoin",
                "net_worth_estimate": "$33B (Forbes 2024)",
                "legal_status": "Pled guilty Nov 2023 to BSA violations; sentenced 4 months prison Apr 2024; served sentence; pardoned by President Trump Oct 2025",
                "pardon_controversy": "Sen. Elizabeth Warren called it 'corruption'; CZ had boosted Trump crypto ventures and lobbied for pardon prior",
                "current_status": "Free, pardoned; no longer CEO but remains largest shareholder",
                "confidence": "confirmed",
            },
            {
                "name": "Richard Teng",
                "role": "CEO (Nov 2023 — present)",
                "background": "Former Abu Dhabi Global Market (ADGM) regulator; former head of regional markets at SGX",
                "significance": "Compliance-focused CEO installed to satisfy US regulators",
                "confidence": "confirmed",
            },
            {
                "name": "Yi He",
                "role": "Co-founder, Head of Marketing / Customer Service",
                "background": "Former OKCoin VP; CZ's partner",
                "significance": "Publicly defended DWF Labs against WSJ manipulation allegations",
                "confidence": "confirmed",
            },
        ],
        "financial_metrics": {
            "annual_trading_volume_2025": "$34 trillion (all products)",
            "spot_market_share_2025": "38-41% of global CEX spot volume",
            "user_base": "300+ million registered users",
            "proof_of_reserves": "$162.8B in user assets (Dec 2025)",
            "doj_fine": "$4.3 billion (Nov 2023 settlement)",
            "confidence": "confirmed",
        },
        "wash_trading_estimates": {
            "nber_study_finding": "Unregulated exchanges average 70-77.5% wash trading; Tier-1 unregulated at 61.8%",
            "binance_classification": "Tier-1 unregulated exchange in NBER study (pre-2023 settlement)",
            "implied_binance_estimate": "Estimated 40-60% of reported volume may be wash trades on non-US platform",
            "post_settlement_status": "Under compliance monitor; wash trading likely reduced but not eliminated",
            "confidence": "estimated",
            "source": "NBER Working Paper 30783 (Cong, Li, Tang)",
        },
        "controversies": [
            {
                "event": "DOJ/SEC/CFTC settlement Nov 2023",
                "details": "$4.3B fine for AML/BSA violations, sanctions evasion, unlicensed money transmission",
                "confidence": "confirmed",
            },
            {
                "event": "DWF Labs wash trading cover-up",
                "details": "Internal surveillance team found $300M+ wash trading by DWF Labs; Binance fired investigators, retained DWF as client",
                "confidence": "confirmed",
                "source": "Wall Street Journal May 2024",
            },
            {
                "event": "FCA UK ban",
                "details": "June 2021 — FCA ordered Binance to stop all regulated UK activity",
                "confidence": "confirmed",
            },
            {
                "event": "CZ presidential pardon",
                "details": "Trump pardoned CZ Oct 2025 after CZ promoted Trump crypto ventures; corruption allegations from Congress",
                "confidence": "confirmed",
            },
        ],
        "connections": {
            "tether": "Binance is largest venue for USDT trading pairs; deep liquidity dependency",
            "dwf_labs": "Retained DWF as market maker despite internal wash trading findings",
            "wintermute": "Major market maker on Binance; accused of dumping BTC during thin liquidity NYE 2025",
            "ftx": "Former rival; absorbed FTX market share post-collapse Nov 2022",
            "trump_crypto": "CZ boosted Trump-linked crypto ventures pre-pardon",
        },
        "market_impact_assessment": {
            "systemic_risk": "HIGH — single exchange with 40% spot share; any disruption cascades globally",
            "regulatory_trajectory": "Under 5-year compliance monitor; improving but still opaque on wash trading",
            "trading_signal": "Binance reserve flows and BNB burns are leading indicators for crypto sentiment",
            "confidence": "derived",
        },
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 2. TETHER / BITFINEX
    # ═══════════════════════════════════════════════════════════════════════
    "tether_bitfinex": {
        "entity_type": "stablecoin_issuer_and_exchange",
        "jurisdiction": {
            "tether_holdings_ltd": "British Virgin Islands (incorporated 2014)",
            "ifinex_inc": "BVI — parent company of both Tether and Bitfinex",
            "bitfinex": "Registered in BVI, operated from Hong Kong historically",
            "regulatory_status": "No US license; settled NYAG $18.5M (2021); not registered with SEC/CFTC",
            "bvi_structure_significance": "BVI corporate secrecy enables opaque ownership and reserve management",
            "confidence": "confirmed",
        },
        "key_people": [
            {
                "name": "Paolo Ardoino",
                "role": "CEO of Tether (promoted 2023), former CTO of Bitfinex/Tether",
                "background": "Italian software developer; joined Bitfinex ~2014 as developer",
                "significance": "Public face of Tether; manages reserve attestation narrative",
                "confidence": "confirmed",
            },
            {
                "name": "Giancarlo Devasini",
                "role": "CFO of Bitfinex, co-founder of Tether",
                "background": "Italian; former plastic surgeon; named in Paradise Papers (Nov 2017) for setting up Tether Holdings in BVI",
                "net_worth_estimate": "Multi-billionaire from Tether profits",
                "legal_history": "Paid $18.5M fine to NYAG (2021) for using Tether reserves to cover Bitfinex losses",
                "significance": "The real power behind Tether; controls financial decisions; dual role creates permanent conflict of interest",
                "confidence": "confirmed",
            },
            {
                "name": "Stuart Hoegner",
                "role": "General Counsel, Tether/Bitfinex",
                "background": "Canadian lawyer; previously worked for Excapsa (online poker)",
                "confidence": "confirmed",
            },
        ],
        "financial_metrics": {
            "usdt_supply_q4_2025": "$186.5 billion in circulation",
            "excess_reserves": "$6.3 billion above liabilities",
            "us_treasury_holdings": "$141 billion (one of largest sovereign debt holders globally)",
            "gold_holdings": "$17.4 billion",
            "bitcoin_holdings": "$8.4 billion",
            "secured_loans": "$14.6 billion",
            "annual_profit_2025": "$10+ billion",
            "q2_2025_profit": "$4.9 billion",
            "attestation_auditor": "BDO Italy — limited assurance attestation (NOT a full audit)",
            "confidence": "confirmed (attestation-level, not audit-level)",
        },
        "reserve_composition_timeline": {
            "2019_and_prior": {
                "claim": "Fully backed 1:1 by USD",
                "reality": "NYAG found this was false; reserves included loans to Bitfinex",
                "confidence": "confirmed",
            },
            "2021_nyag_settlement": {
                "composition": "~76% cash and equivalents, large commercial paper holdings, loans to affiliates",
                "fine": "$18.5M for misleading statements about reserves",
                "confidence": "confirmed",
            },
            "2023_pivot": {
                "composition": "Shifted away from commercial paper to US Treasuries",
                "significance": "Response to regulatory pressure and competitor Circle's transparency",
                "confidence": "confirmed",
            },
            "2025_q4": {
                "composition": "~74% US Treasuries, remainder in gold ($17.4B), BTC ($8.4B), secured loans ($14.6B), other",
                "attestation": "BDO Italy limited assurance — not full GAAP audit",
                "confidence": "confirmed",
            },
        },
        "academic_research_on_manipulation": [
            {
                "paper": "Griffin & Shams (2020) — 'Is Bitcoin Really Untethered?'",
                "journal": "Journal of Finance",
                "finding": "Bitcoin prices increased after Tether minting during downturns; single large player on Bitfinex drove prices",
                "implication": "USDT issuance may have been used to artificially inflate BTC prices in 2017 bull run",
                "confidence": "confirmed (peer-reviewed)",
            },
            {
                "paper": "Wei (2018) — Stablecoin issuance and crypto returns",
                "finding": "Abnormally high crypto returns in 24 hours before and after stablecoin issuances",
                "implication": "Suggests coordinated front-running of USDT mints",
                "confidence": "confirmed (peer-reviewed)",
            },
        ],
        "controversies": [
            {
                "event": "NYAG lawsuit and $18.5M settlement (2021)",
                "details": "Tether lied about full USD backing; used reserves to cover $850M Bitfinex loss via Crypto Capital Corp",
                "confidence": "confirmed",
            },
            {
                "event": "Bitfinex hack (2016)",
                "details": "119,756 BTC stolen (~$72M at time, ~$7B+ at 2025 prices); led to socialized losses via BFX tokens",
                "confidence": "confirmed",
            },
            {
                "event": "No full audit ever conducted",
                "details": "Only quarterly 'attestations' by BDO Italy — limited scope, not GAAP audit",
                "confidence": "confirmed",
            },
            {
                "event": "Devasini conflict of interest",
                "details": "Simultaneously CFO of exchange (Bitfinex) and controller of its primary stablecoin (Tether)",
                "confidence": "confirmed",
            },
        ],
        "connections": {
            "binance": "USDT is dominant trading pair on Binance; Tether-Binance volume dependency is mutual",
            "cantor_fitzgerald": "Howard Lutnick (Cantor CEO, Trump Commerce Secretary) manages Tether's US Treasury portfolio",
            "bitfinex": "Same parent company (iFinex); shared leadership; reserves commingled historically",
            "coinbase": "Competitive tension — Coinbase backs USDC (Circle) as alternative",
            "ftx": "FTX collapse temporarily shook USDT peg; Tether survived but faced redemption pressure",
        },
        "market_impact_assessment": {
            "systemic_risk": "CRITICAL — $186B stablecoin underpins majority of crypto trading globally",
            "de_peg_scenario": "USDT de-peg would trigger cascading liquidations across all CEX and DeFi",
            "regulatory_trajectory": "US stablecoin legislation may force Tether to register or exit US market",
            "trading_signal": "USDT premium/discount to $1.00 is leading indicator for crypto stress; USDT mint/burn rate signals institutional flows",
            "confidence": "derived",
        },
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 3. COINBASE
    # ═══════════════════════════════════════════════════════════════════════
    "coinbase": {
        "entity_type": "centralized_exchange_publicly_traded",
        "ticker": "COIN (NASDAQ)",
        "jurisdiction": {
            "incorporated": "Delaware, USA",
            "hq": "No formal HQ (remote-first since 2022); previously San Francisco",
            "regulatory_status": "SEC lawsuit DISMISSED Feb 2025; state money transmitter licenses; NY BitLicense",
            "confidence": "confirmed",
        },
        "key_people": [
            {
                "name": "Brian Armstrong",
                "role": "CEO, Co-founder",
                "background": "Former Airbnb engineer; founded Coinbase 2012",
                "net_worth_estimate": "$11B+ (Forbes 2025)",
                "political_activity": "Led crypto PAC spending; met privately with Trump post-election; donated $1M to Trump inauguration",
                "legal_status": "Named defendant in shareholder derivative lawsuit (Mar 2025, NJ District Court) over misleading compliance disclosures",
                "confidence": "confirmed",
            },
            {
                "name": "Emilie Choi",
                "role": "President and COO",
                "background": "Former LinkedIn VP; joined Coinbase 2018",
                "confidence": "confirmed",
            },
            {
                "name": "Alesia Haas",
                "role": "CFO",
                "background": "Former Merrill Lynch, Sculptor Capital",
                "confidence": "confirmed",
            },
        ],
        "financial_metrics": {
            "q3_2025_revenue": "$1.9 billion (up 25% QoQ)",
            "2024_subscription_services_revenue": "$2.3 billion (up 60% YoY)",
            "revenue_breakdown": {
                "transaction_revenue": "68.5% of total (mostly retail trading fees)",
                "subscription_and_services": "31.5% (stablecoin revenue via USDC, staking, custodial fees)",
                "confidence": "confirmed",
            },
            "assets_under_custody": "$300 billion (ATH, Q4 2025)",
            "etf_custody_dominance": "80%+ of all US Bitcoin and Ethereum ETF assets",
            "etf_peak_inflows": "$31 billion in peak ETF inflows during 2025",
            "confidence": "confirmed",
        },
        "sec_lawsuit": {
            "filed": "June 2023 — SEC alleged Coinbase operated as unregistered exchange, broker, clearing agency",
            "cost_to_fight": "$50 million (per Brian Armstrong)",
            "outcome": "Dismissed Feb 2025 after SEC chair Paul Atkins stated 'most crypto tokens are not themselves securities'",
            "political_context": "Dismissal followed Trump election, Gary Gensler departure, Coinbase $1M inauguration donation",
            "confidence": "confirmed",
        },
        "political_donations": {
            "coinbase_direct": "$52M+ in political donations (OpenSecrets)",
            "stand_with_crypto": {
                "founded_by": "Coinbase (2023)",
                "members": "2.6-2.7 million US members",
                "partners": ["Coinbase", "Kraken", "Gemini", "Anchorage Digital", "Paradigm"],
                "confidence": "confirmed",
            },
            "fairshake_pac": {
                "war_chest": "$190M+ raised for 2026 midterms",
                "coinbase_2025_commitment": "$25M to Fairshake affiliates",
                "total_crypto_pac_funds": "$271M+ for 2026 cycle",
                "confidence": "confirmed",
            },
            "2024_election_spend": "$130M+ across all crypto industry to elect pro-crypto candidates",
            "impact_assessment": "Crypto PACs are now among the largest political spending forces in US elections",
            "confidence": "confirmed",
        },
        "controversies": [
            {
                "event": "Shareholder derivative lawsuit (Mar 2025)",
                "details": "Allegations of misleading disclosures on custody safeguards, token vetting, AML programs (2021-2023 period)",
                "confidence": "confirmed",
            },
            {
                "event": "Insider trading lawsuit (2026)",
                "details": "Directors and CEO facing insider trading allegations",
                "confidence": "confirmed",
            },
            {
                "event": "ETF custody concentration risk",
                "details": "80%+ of US crypto ETF assets in single custodian creates systemic risk",
                "confidence": "derived",
            },
        ],
        "connections": {
            "circle_usdc": "Primary USDC distribution partner; earns revenue share on USDC interest",
            "blackrock": "Custodian for iShares Bitcoin Trust (IBIT) — largest Bitcoin ETF",
            "sec": "Former adversary; relationship now cooperative under Atkins",
            "binance": "Primary global competitor; Coinbase dominates US regulated market",
            "trump_admin": "Donated to inauguration; SEC case dismissed; deep regulatory alignment",
        },
        "market_impact_assessment": {
            "systemic_risk": "HIGH — 80% ETF custody concentration; single point of failure for institutional crypto exposure",
            "regulatory_trajectory": "Favorable under current admin; shareholder lawsuits remain risk",
            "trading_signal": "COIN stock price is proxy for US crypto regulatory sentiment; ETF inflows through Coinbase signal institutional conviction",
            "confidence": "derived",
        },
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 4. FTX AFTERMATH
    # ═══════════════════════════════════════════════════════════════════════
    "ftx_aftermath": {
        "entity_type": "bankrupt_exchange",
        "status": "Chapter 11 bankruptcy — distributions ongoing",
        "jurisdiction": {
            "incorporated": "Antigua and Barbuda (FTX Digital Markets); Bahamas (FTX Trading Ltd)",
            "bankruptcy_court": "US Bankruptcy Court, District of Delaware",
            "confidence": "confirmed",
        },
        "key_people": [
            {
                "name": "Sam Bankman-Fried (SBF)",
                "role": "Founder, former CEO",
                "status": "Sentenced to 25 years in federal prison (Mar 2024); incarcerated at FCI Terminal Island (transferred from MDC Brooklyn)",
                "confidence": "confirmed",
            },
            {
                "name": "John J. Ray III",
                "role": "CEO appointed for bankruptcy proceedings",
                "background": "Managed Enron bankruptcy liquidation",
                "confidence": "confirmed",
            },
            {
                "name": "Caroline Ellison",
                "role": "Former CEO of Alameda Research",
                "status": "Sentenced to 2 years; cooperating witness",
                "confidence": "confirmed",
            },
            {
                "name": "Gary Wang",
                "role": "Co-founder, CTO",
                "status": "Pled guilty; cooperating witness; sentenced to time served",
                "confidence": "confirmed",
            },
            {
                "name": "Nishad Singh",
                "role": "Former head of engineering",
                "status": "Pled guilty; cooperating witness",
                "confidence": "confirmed",
            },
        ],
        "where_the_money_went": {
            "total_customer_shortfall": "~$8-10 billion at collapse",
            "alameda_research_losses": "Trading losses, loans to FTX insiders, venture investments using customer funds",
            "key_investments_with_customer_funds": {
                "anthropic": {"invested": "$500M", "sold_for": "$1.3B", "surplus": "$800M+", "confidence": "confirmed"},
                "robinhood_shares": {"invested": "$546M (via Emergent Fidelity Technologies)", "sold_for": "~$600M", "confidence": "confirmed"},
                "sequoia_capital": {"invested": "$200M+", "confidence": "confirmed"},
                "real_estate_bahamas": {"spent": "$256M on luxury properties", "confidence": "confirmed"},
                "political_donations": {"total": "$73M+ in political contributions", "confidence": "confirmed"},
                "genesis_digital_assets": {"clawback_suit": "$1B lawsuit pending", "confidence": "confirmed"},
            },
            "confidence": "confirmed",
        },
        "customer_recovery": {
            "total_recovered_assets": "$15B+ (from liquidations of investments, clawbacks, asset sales)",
            "recovery_rate": "98% of creditors to receive 119% of petition-date claim values",
            "critical_caveat": "Based on Nov 2022 crypto prices — NOT current prices; BTC was ~$16K at petition vs $80K+ now",
            "real_terms_recovery": "Customers receive ~25-30% of current market value of their holdings",
            "distribution_timeline": {
                "round_1": {"date": "Feb 2025", "amount": "~$1.2B", "confidence": "confirmed"},
                "round_2": {"date": "May 2025", "amount": "~$5B", "confidence": "confirmed"},
                "round_3": {"date": "Sep 2025", "amount": "$1.6B", "confidence": "confirmed"},
                "round_4": {"date": "Mar 2026", "amount": "TBD — scheduled", "confidence": "confirmed"},
                "total_distributed": "$7.1B+ as of Jan 2026",
            },
            "confidence": "confirmed",
        },
        "clawback_lawsuits": {
            "political_donations": {
                "total_targeted": "$73M+",
                "status": "Confidential demands sent to hundreds of politicians and PACs",
                "notable_returns": "Multiple politicians returned donations voluntarily",
                "confidence": "confirmed",
            },
            "genesis_digital_assets": {"amount": "$1B", "status": "Active litigation, Genesis seeking dismissal", "confidence": "confirmed"},
            "celebrity_endorsements": {"status": "Lawsuits filed against Tom Brady, Steph Curry, Larry David, others", "confidence": "confirmed"},
            "confidence": "confirmed",
        },
        "connections": {
            "binance": "CZ's withdrawal tweet triggered FTX bank run Nov 2022; Binance briefly considered acquisition then backed out",
            "anthropic": "$500M investment recovered at $1.3B — largest single recovery",
            "alameda_research": "Sister company; commingled funds were core of fraud",
            "tether": "FTX collapse tested USDT peg stability",
            "effective_altruism": "SBF's EA donations being clawed back; reputational damage to EA movement",
        },
        "market_impact_assessment": {
            "systemic_risk": "LOW (now) — bankruptcy is orderly; distributions reducing overhang",
            "regulatory_impact": "Catalyzed global push for exchange proof-of-reserves and regulatory frameworks",
            "trading_signal": "FTX distribution dates create predictable sell pressure as creditors receive cash and may convert to crypto then sell",
            "confidence": "derived",
        },
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 5. CRYPTO MARKET MAKERS
    # ═══════════════════════════════════════════════════════════════════════
    "market_makers": {
        "wintermute": {
            "entity_type": "algorithmic_market_maker",
            "jurisdiction": {
                "incorporated": "UK (Wintermute Trading Ltd)",
                "offices": "London (HQ), Singapore",
                "regulatory_status": "UK registered; no major regulatory actions",
                "confidence": "confirmed",
            },
            "key_people": [
                {
                    "name": "Evgeny Gaevoy",
                    "role": "Founder, CEO",
                    "background": "Russian-born; 10 years at Optiver (built EU ETF desk — one of largest in EU); Executive MBA London Business School; MSc Finance; BSc Economics",
                    "significance": "Built Wintermute from zero to one of largest crypto market makers globally",
                    "confidence": "confirmed",
                },
            ],
            "financial_metrics": {
                "daily_trading_volume": "$5B+ daily",
                "venues": "50+ trading venues (CEX and DEX)",
                "notable_loss": "$160M Optimism bridge exploit (Sep 2022)",
                "confidence": "confirmed",
            },
            "controversies": [
                {
                    "event": "FDUSD depeg arbitrage (Apr 2025)",
                    "details": "Moved $75M instantly during FDUSD depeg to $0.87; extracted $3M in arbitrage",
                    "confidence": "confirmed",
                },
                {
                    "event": "Pre-crash selling (Sep 2025)",
                    "details": "Massive BTC/ETH/SOL inflows to Wintermute wallets before $1.7B liquidation event",
                    "confidence": "confirmed",
                },
                {
                    "event": "NYE 2025 BTC dump",
                    "details": "Dumped 1,213 BTC onto Binance during thin New Year's Eve liquidity",
                    "confidence": "confirmed",
                },
            ],
            "market_impact": "HIGH — one of top 3 crypto market makers; can move prices through liquidity withdrawal",
        },
        "jump_crypto": {
            "entity_type": "algorithmic_market_maker_and_prop_trader",
            "jurisdiction": {
                "parent": "Jump Trading LLC (Chicago)",
                "crypto_subsidiary": "Jump Crypto Holdings LLC",
                "tai_mo_shan": "Tai Mo Shan Limited (Hong Kong — wholly owned subsidiary)",
                "regulatory_status": "Under CFTC probe; SEC settlement Dec 2024",
                "confidence": "confirmed",
            },
            "key_people": [
                {
                    "name": "Kanav Kariya",
                    "role": "Former President, Jump Crypto",
                    "status": "Stepped down Jun 2024 amid CFTC probe",
                    "confidence": "confirmed",
                },
                {
                    "name": "Bill DiSomma",
                    "role": "Co-founder, Jump Trading",
                    "background": "Former CME floor trader",
                    "confidence": "confirmed",
                },
                {
                    "name": "Paul Gurinas",
                    "role": "Co-founder, Jump Trading",
                    "background": "Former CME floor trader",
                    "confidence": "confirmed",
                },
            ],
            "sec_settlement": {
                "entity": "Tai Mo Shan Limited",
                "date": "Dec 20, 2024",
                "amount": "$123 million ($86M disgorgement + $36M civil penalty)",
                "charges": "Misleading investors about TerraUSD (UST) stability; purchased $20M UST to maintain peg during depeg event; unregistered offer/sale of LUNA as securities",
                "period": "Jan 2021 — May 2022",
                "confidence": "confirmed",
            },
            "controversies": [
                {
                    "event": "Terra/Luna manipulation",
                    "details": "Propped up UST peg with $20M purchase, misleading investors about algorithmic stability",
                    "confidence": "confirmed",
                },
                {
                    "event": "CFTC probe",
                    "details": "Ongoing investigation into broader crypto trading practices",
                    "confidence": "confirmed",
                },
                {
                    "event": "$4B lawsuit",
                    "details": "Facing $4 billion lawsuit over role in Terra collapse",
                    "confidence": "confirmed",
                },
                {
                    "event": "Crypto exit and return",
                    "details": "Largely exited crypto in 2023-2024; attempting return in 2025-2026 with 'Let's Build' messaging",
                    "confidence": "confirmed",
                },
            ],
            "market_impact": "MEDIUM — reduced crypto presence post-Terra scandal; legacy TradFi credibility damaged",
        },
        "dwf_labs": {
            "entity_type": "market_maker_and_investor",
            "jurisdiction": {
                "hq": "Dubai, UAE",
                "parent": "Digital Wave Finance (high-frequency trading)",
                "regulatory_status": "Dubai-registered; no US registration; under scrutiny but no formal charges",
                "confidence": "confirmed",
            },
            "key_people": [
                {
                    "name": "Andrei Grachev",
                    "role": "Founding Partner",
                    "background": "Russian entrepreneur; founded DWF Labs 2022",
                    "significance": "Public face of DWF; active on social media promoting token investments",
                    "confidence": "confirmed",
                },
                {
                    "name": "Heng Yu Lee",
                    "role": "Founding Partner",
                    "significance": "Dismissed manipulation claims as 'competitor-driven FUD'",
                    "confidence": "confirmed",
                },
            ],
            "controversies": [
                {
                    "event": "Binance internal investigation (2023-2024)",
                    "details": "Binance surveillance team found $300M+ in wash trades across 7+ tokens including YGG; team recommended action",
                    "outcome": "Binance fired the investigators and retained DWF as client",
                    "source": "Wall Street Journal (May 2024)",
                    "confidence": "confirmed",
                },
                {
                    "event": "YGG token manipulation",
                    "details": "Grachev tweeted about YGG; DWF sold ~5M tokens at peak causing price collapse",
                    "confidence": "confirmed",
                },
                {
                    "event": "Trump crypto investment",
                    "details": "Invested in Trump-linked crypto ventures; raised corruption concerns",
                    "source": "The Nation",
                    "confidence": "confirmed",
                },
            ],
            "market_impact": "MEDIUM — significant liquidity provider but reputation severely damaged; operates in regulatory gray zone from Dubai",
        },
        "gsr_markets": {
            "entity_type": "crypto_market_maker",
            "jurisdiction": {
                "hq": "UK",
                "offices": "London, New York, Singapore, Tokyo",
                "founded": "2013",
                "confidence": "confirmed",
            },
            "key_people": [
                {
                    "name": "Jakob Palmstierna",
                    "role": "Co-founder",
                    "confidence": "confirmed",
                },
                {
                    "name": "Rich Rosenblum",
                    "role": "Co-founder",
                    "background": "Former Goldman Sachs trader",
                    "confidence": "confirmed",
                },
            ],
            "financial_metrics": {
                "platform_capabilities": "OTC trading across 200+ digital assets and 25 fiat currencies",
                "exchange_partners": ["Kraken", "Coinbase", "Gemini"],
                "confidence": "confirmed",
            },
            "significance": "One of the oldest crypto market makers; publicly criticized DWF Labs practices",
            "market_impact": "MEDIUM — established institutional player; lower profile than Wintermute/Jump",
        },
        "amber_group": {
            "entity_type": "crypto_market_maker_and_asset_manager",
            "jurisdiction": {
                "hq": "Singapore/Hong Kong",
                "public_listing": "NASDAQ: AMBR (Amber International, went public 2025)",
                "confidence": "confirmed",
            },
            "key_people": [
                {
                    "name": "Michael Wu",
                    "role": "CEO, Co-founder",
                    "background": "Ex-Morgan Stanley",
                    "confidence": "confirmed",
                },
            ],
            "financial_metrics": {
                "daily_trading_volume": "$5B+ daily",
                "institutional_clients": "2,000+ institutional investors",
                "tokens_supported": "200+",
                "confidence": "confirmed",
            },
            "significance": "First major crypto market maker to go public (NASDAQ); bridges TradFi-to-crypto institutional flow",
            "market_impact": "MEDIUM — growing institutional presence; NASDAQ listing adds transparency",
        },
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 6. DEX LANDSCAPE
    # ═══════════════════════════════════════════════════════════════════════
    "dex_landscape": {
        "uniswap": {
            "entity_type": "decentralized_exchange",
            "chain": "Ethereum (primary), deployed on 20+ chains",
            "jurisdiction": {
                "protocol": "Permissionless smart contracts — no jurisdiction",
                "uniswap_labs": "US-based (New York); developed frontend and protocol",
                "uniswap_foundation": "Separate entity managing governance and grants",
                "regulatory_status": "SEC Wells Notice issued Apr 2024; dropped 2025 under new SEC leadership",
                "confidence": "confirmed",
            },
            "key_people": [
                {
                    "name": "Hayden Adams",
                    "role": "Founder, CEO of Uniswap Labs",
                    "background": "Former mechanical engineer at Siemens; built Uniswap after being laid off",
                    "confidence": "confirmed",
                },
            ],
            "financial_metrics": {
                "market_share_jan_2025": "~21% of total DEX volume",
                "status_vs_solana": "Lost top spot to Raydium (27%) in Jan 2025 due to Solana memecoin activity",
                "uniswap_v4": "Launched late 2025; introduces hooks for custom pool logic and MEV protection",
                "protocol_fee_switch": "Activated — protocol now earns fees directly (controversial among LPs)",
                "confidence": "confirmed",
            },
            "mev_protection": {
                "v4_hooks": "Modular plugins allowing MEV protection directly in pools",
                "uniswap_x": "Intent-based trading system to reduce MEV exposure",
                "confidence": "confirmed",
            },
        },
        "jupiter": {
            "entity_type": "dex_aggregator",
            "chain": "Solana",
            "jurisdiction": {
                "entity": "Jupiter Exchange (Cayman Islands)",
                "regulatory_status": "Unregulated; Solana ecosystem",
                "confidence": "estimated",
            },
            "key_people": [
                {
                    "name": "Meow (pseudonymous)",
                    "role": "Founder",
                    "confidence": "confirmed",
                },
            ],
            "financial_metrics": {
                "solana_market_share": "95% of Solana DEX aggregation volume",
                "swap_volume_share": "Routes 50%+ of all Solana swaps",
                "confidence": "confirmed",
            },
            "mev_protection": {
                "shadowlane_engine": "Routes transactions privately to avoid sandwich attacks",
                "jito_integration": "Optional Jito-based MEV protection routes",
                "confidence": "confirmed",
            },
        },
        "raydium": {
            "entity_type": "decentralized_exchange",
            "chain": "Solana",
            "jurisdiction": {
                "regulatory_status": "Unregulated; Solana ecosystem",
                "confidence": "estimated",
            },
            "financial_metrics": {
                "market_share_jan_2025": "27% of total DEX volume — briefly #1 globally above Uniswap",
                "driver": "Solana memecoin / pump.fun activity drove volume explosion",
                "largest_dex_on_solana_by_tvl": True,
                "confidence": "confirmed",
            },
        },
        "mev_extraction_statistics": {
            "ethereum_2025": {
                "sandwich_attacks_value": "$289.76 million",
                "sandwich_share_of_mev": "51.56% of all MEV volume on Ethereum",
                "avg_profit_per_sandwich_oct_2025": "~$3 (down significantly from prior years)",
                "unprofitable_bots": "~30% of active sandwich bots recording net losses; ~33% at breakeven",
                "confidence": "confirmed",
            },
            "solana_2025": {
                "sandwich_bot_extraction": "$370M-$500M extracted from Solana users over 16-month window",
                "primary_vector": "Memecoin trading on Raydium and Jupiter routes",
                "confidence": "estimated",
            },
            "total_annual_mev_estimate": {
                "ethereum": "$500M-$700M annually across all MEV types (arbitrage, liquidations, sandwich)",
                "solana": "$300M-$500M annually (growing rapidly)",
                "confidence": "estimated",
            },
            "trend": "MEV per-attack profitability declining on Ethereum as competition increases; Solana MEV growing as activity migrates",
        },
    },

    # ═══════════════════════════════════════════════════════════════════════
    # CROSS-ENTITY CONNECTIONS MAP
    # ═══════════════════════════════════════════════════════════════════════
    "network_connections": [
        {"from": "tether", "to": "binance", "type": "liquidity_dependency", "strength": "critical", "details": "USDT is dominant quote currency on Binance; mutual volume dependency", "confidence": "confirmed"},
        {"from": "tether", "to": "cantor_fitzgerald", "type": "financial_service", "strength": "high", "details": "Cantor manages Tether's $141B US Treasury portfolio; CEO Howard Lutnick is Trump Commerce Secretary", "confidence": "confirmed"},
        {"from": "coinbase", "to": "blackrock", "type": "custody", "strength": "critical", "details": "Coinbase custodies IBIT (largest BTC ETF); 80%+ of all US crypto ETF assets", "confidence": "confirmed"},
        {"from": "coinbase", "to": "circle", "type": "partnership", "strength": "high", "details": "USDC distribution partner; revenue share on interest", "confidence": "confirmed"},
        {"from": "binance", "to": "dwf_labs", "type": "market_making", "strength": "medium", "details": "DWF retained despite $300M wash trading findings; investigators fired", "confidence": "confirmed"},
        {"from": "binance", "to": "wintermute", "type": "market_making", "strength": "high", "details": "Major liquidity provider; involved in thin-liquidity incidents", "confidence": "confirmed"},
        {"from": "ftx", "to": "anthropic", "type": "investment", "strength": "resolved", "details": "$500M invested; sold for $1.3B in bankruptcy", "confidence": "confirmed"},
        {"from": "ftx", "to": "binance", "type": "competitive_destruction", "strength": "resolved", "details": "CZ tweet triggered FTX bank run; Binance absorbed market share", "confidence": "confirmed"},
        {"from": "jump_crypto", "to": "terraform_labs", "type": "market_manipulation", "strength": "resolved", "details": "$123M SEC settlement for propping up UST peg", "confidence": "confirmed"},
        {"from": "coinbase", "to": "trump_admin", "type": "political_influence", "strength": "high", "details": "$1M inauguration donation; $52M+ political spending; SEC case dismissed", "confidence": "confirmed"},
        {"from": "binance", "to": "trump_admin", "type": "political_influence", "strength": "high", "details": "CZ pardoned after boosting Trump crypto ventures", "confidence": "confirmed"},
        {"from": "wintermute", "to": "gsr_markets", "type": "competitive", "strength": "medium", "details": "Both criticized DWF Labs publicly; compete for market making mandates", "confidence": "confirmed"},
        {"from": "uniswap", "to": "mev_bots", "type": "parasitic", "strength": "high", "details": "51.56% of Ethereum MEV is sandwich attacks, primarily on Uniswap pools", "confidence": "confirmed"},
        {"from": "jupiter", "to": "raydium", "type": "aggregation", "strength": "high", "details": "Jupiter routes through Raydium pools; 95% aggregator market share", "confidence": "confirmed"},
    ],

    # ═══════════════════════════════════════════════════════════════════════
    # SYSTEMIC RISK SUMMARY
    # ═══════════════════════════════════════════════════════════════════════
    "systemic_risk_assessment": {
        "single_points_of_failure": [
            {"entity": "Tether/USDT", "risk": "CRITICAL", "scenario": "USDT depeg cascades through all CEX and DeFi; $186B systemic exposure", "confidence": "derived"},
            {"entity": "Coinbase ETF custody", "risk": "HIGH", "scenario": "80% of US crypto ETF assets in single custodian; operational failure impacts all institutional crypto", "confidence": "derived"},
            {"entity": "Binance market share", "risk": "HIGH", "scenario": "40% spot share; regulatory shutdown or hack cascades globally", "confidence": "derived"},
        ],
        "political_capture_risk": {
            "assessment": "Crypto industry has achieved regulatory capture through $271M+ PAC spending, Trump admin alignment, CZ pardon, SEC case dismissals",
            "entities_involved": ["Coinbase", "Binance", "Tether/Cantor Fitzgerald", "DWF Labs"],
            "confidence": "derived",
        },
        "market_maker_opacity": {
            "assessment": "Top 5 market makers control majority of crypto liquidity; operate with minimal transparency; documented wash trading and front-running",
            "entities": ["Wintermute", "Jump Crypto", "DWF Labs", "GSR Markets", "Amber Group"],
            "confidence": "derived",
        },
    },
}


if __name__ == "__main__":
    print(json.dumps(CRYPTO_EXCHANGE_NETWORK_INTEL, indent=2, default=str))
