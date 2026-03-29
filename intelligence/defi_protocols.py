"""
GRID Intelligence Platform — DeFi Protocol Analysis
Generated: 2026-03-28
Data sourced from: DefiLlama, CoinGecko, CoinMarketCap, protocol docs, web research
Confidence labels: confirmed / derived / estimated / rumored / inferred

Sources:
- https://defillama.com/protocols
- https://www.coingecko.com
- https://coinmarketcap.com
- https://www.spotedcrypto.com/defi-tvl-95b-aave-1t-loans-staking-airdrop-guide-march-2026/
- https://theledgermind.com/best-defi-protocols-2026/
- https://www.theblock.co/post/379288/1-billion-2025-fees-uniswap-eyes-governance-shift-protocol-burns
- https://www.halborn.com/reports/top-100-defi-hacks-2025
- https://cryptoimpacthub.com/crypto-hacks-scams-2026-roundup/
- https://www.spotedcrypto.com/morpho-apollo-defi-bet-altcoin-surge-february-2026/
"""

DEFI_PROTOCOLS = [
    # ──────────────────────────────────────────────────────────────
    # 1. Uniswap (UNI)
    # ──────────────────────────────────────────────────────────────
    {
        "name": "Uniswap",
        "ticker": "UNI",
        "category": "DEX (AMM)",
        "chain": "Ethereum, 39+ chains",
        "tvl": {
            "value_usd": 5_200_000_000,
            "trend": "stable",
            "change_30d_pct": -3.0,
            "confidence": "estimated",
            "note": "Combined across v2/v3/v4 deployments on 39 chains",
        },
        "token": {
            "price_usd": 3.35,
            "market_cap_usd": 2_010_000_000,
            "fdv_usd": 3_350_000_000,
            "confidence": "confirmed",
            "source": "CoinMarketCap 2026-03-28",
        },
        "key_person": {
            "name": "Hayden Adams",
            "role": "Founder & CEO",
            "confidence": "confirmed",
        },
        "top_governance_holders": [
            {"entity": "Uniswap Foundation / Treasury", "pct_supply": "~18%", "confidence": "derived"},
            {"entity": "a16z (Andreessen Horowitz)", "pct_supply": "~8%", "confidence": "estimated"},
            {"entity": "Binance (exchange custody)", "pct_supply": "~3%", "confidence": "estimated"},
        ],
        "revenue": {
            "model": "LP fees (0.01-1% per swap); protocol fee switch activated late 2025 via UNIfication proposal",
            "annualized_protocol_revenue_usd": 26_000_000,
            "annualized_gross_fees_usd": 1_000_000_000,
            "confidence": "confirmed",
            "note": "Fee switch just activated; $1B gross fees in 2025; protocol captures fraction. UNI burn + 20M UNI/yr growth budget approved",
        },
        "audit_status": {
            "audited": True,
            "auditors": ["Trail of Bits", "ABDK", "OpenZeppelin", "Spearbit"],
            "confidence": "confirmed",
            "note": "V4 audited by multiple firms; continuous bug bounty via Immunefi",
        },
        "exploit_history": [
            {
                "date": "N/A",
                "description": "No direct protocol exploit. Phishing attacks on users; front-end DNS attack attempted 2023.",
                "loss_usd": 0,
                "confidence": "confirmed",
            }
        ],
        "competitive_position": {
            "summary": "Largest DEX by volume globally. Dominant on Ethereum mainnet, expanding aggressively to L2s and alt-L1s. Uniswap v4 hooks enable custom pool logic. UNIfication proposal marks shift to value accrual for token holders.",
            "moat": "Liquidity depth, brand, multi-chain presence, developer ecosystem",
            "threats": "Aggregator commoditization (1inch, Jupiter), intent-based trading, Solana DEX volume growth",
            "confidence": "derived",
        },
    },

    # ──────────────────────────────────────────────────────────────
    # 2. Aave (AAVE)
    # ──────────────────────────────────────────────────────────────
    {
        "name": "Aave",
        "ticker": "AAVE",
        "category": "Lending / Borrowing",
        "chain": "Ethereum, Polygon, Arbitrum, Optimism, Avalanche, Base, 10+ chains",
        "tvl": {
            "value_usd": 26_460_000_000,
            "trend": "up",
            "change_30d_pct": 8.5,
            "confidence": "confirmed",
            "note": "Largest DeFi protocol by TVL as of March 2026. Crossed $1T cumulative loans.",
        },
        "token": {
            "price_usd": 96.76,
            "market_cap_usd": 1_450_000_000,
            "fdv_usd": 1_550_000_000,
            "confidence": "confirmed",
            "source": "CoinGecko 2026-03-28",
        },
        "key_person": {
            "name": "Stani Kulechov",
            "role": "Founder & CEO of Aave Labs",
            "confidence": "confirmed",
        },
        "top_governance_holders": [
            {"entity": "Aave Treasury / Ecosystem Reserve", "pct_supply": "~25%", "confidence": "derived"},
            {"entity": "Aave Labs / Team", "pct_supply": "~10%", "confidence": "estimated"},
            {"entity": "Binance (exchange custody)", "pct_supply": "~5%", "confidence": "estimated"},
        ],
        "revenue": {
            "model": "Interest rate spread on borrows, flash loan fees, liquidation penalties, GHO stablecoin revenue",
            "annualized_protocol_revenue_usd": 141_000_000,
            "annualized_gross_fees_usd": 1_087_000_000,
            "confidence": "confirmed",
            "note": "GHO stablecoin adds $14M/yr. $50M/yr locked in for AAVE buybacks. V4 launching 2026.",
        },
        "audit_status": {
            "audited": True,
            "auditors": ["OpenZeppelin", "Trail of Bits", "SigmaPrime", "Certora", "Peckshield"],
            "confidence": "confirmed",
            "note": "Formally verified via Certora. Continuous bug bounty.",
        },
        "exploit_history": [
            {
                "date": "2022-11",
                "description": "CRV market manipulation attempt by Avi Eisenberg; resulted in ~$1.6M bad debt on Aave v2",
                "loss_usd": 1_600_000,
                "confidence": "confirmed",
            },
            {
                "date": "Various",
                "description": "Minor bad debt events from volatile collateral liquidations; protocol absorbed losses via Safety Module",
                "loss_usd": 5_000_000,
                "confidence": "estimated",
            },
        ],
        "competitive_position": {
            "summary": "#1 lending protocol by TVL and revenue. Multi-chain dominance. GHO stablecoin growing. V4 architecture will introduce modular risk management.",
            "moat": "Deepest lending liquidity, institutional trust, multi-chain, GHO stablecoin",
            "threats": "Morpho efficiency gains, Compound V3 simplicity, regulatory risk on overcollateralized lending",
            "confidence": "derived",
        },
    },

    # ──────────────────────────────────────────────────────────────
    # 3. MakerDAO / Sky (MKR / SKY)
    # ──────────────────────────────────────────────────────────────
    {
        "name": "MakerDAO / Sky",
        "ticker": "MKR / SKY",
        "category": "CDP Stablecoin (DAI / USDS)",
        "chain": "Ethereum",
        "tvl": {
            "value_usd": 6_900_000_000,
            "trend": "stable",
            "change_30d_pct": -2.0,
            "confidence": "confirmed",
            "note": "Rebranded to Sky protocol Sep 2024. DAI/USDS still top decentralized stablecoin.",
        },
        "token": {
            "price_usd_mkr": 1_760.00,
            "price_usd_sky": 0.071,
            "market_cap_usd": 1_360_000_000,
            "fdv_usd": 1_680_000_000,
            "confidence": "confirmed",
            "source": "CoinMarketCap 2026-03-28. 1 MKR = 24,000 SKY upgrade ratio.",
        },
        "key_person": {
            "name": "Rune Christensen",
            "role": "Co-founder",
            "confidence": "confirmed",
        },
        "top_governance_holders": [
            {"entity": "a16z", "pct_supply": "~6%", "confidence": "confirmed"},
            {"entity": "Rune Christensen (personal)", "pct_supply": "~3%", "confidence": "estimated"},
            {"entity": "Paradigm", "pct_supply": "~2%", "confidence": "estimated"},
        ],
        "revenue": {
            "model": "Stability fees on DAI/USDS vaults, liquidation penalties, RWA yield (T-bills), PSM fees",
            "annualized_protocol_revenue_usd": 150_000_000,
            "annualized_gross_fees_usd": 150_000_000,
            "confidence": "estimated",
            "note": "Majority of revenue now from RWA (US Treasuries). Protocol is net profitable. Endgame plan restructuring ongoing.",
        },
        "audit_status": {
            "audited": True,
            "auditors": ["Trail of Bits", "PeckShield", "Runtime Verification", "ChainSecurity"],
            "confidence": "confirmed",
        },
        "exploit_history": [
            {
                "date": "2020-03-12",
                "description": "Black Thursday: ETH crash caused $8.3M in undercollateralized liquidations; vaults liquidated at $0 due to keeper failures",
                "loss_usd": 8_300_000,
                "confidence": "confirmed",
            },
        ],
        "competitive_position": {
            "summary": "Original DeFi stablecoin. RWA integration makes it quasi-TradFi. Endgame plan creates SubDAOs. Sky rebrand controversial but ongoing.",
            "moat": "DAI brand recognition, deep DeFi integrations, RWA yield pipeline",
            "threats": "Ethena USDe growth, USDC dominance, regulatory classification risk, rebrand confusion",
            "confidence": "derived",
        },
    },

    # ──────────────────────────────────────────────────────────────
    # 4. Lido (LDO)
    # ──────────────────────────────────────────────────────────────
    {
        "name": "Lido",
        "ticker": "LDO",
        "category": "Liquid Staking",
        "chain": "Ethereum",
        "tvl": {
            "value_usd": 17_960_000_000,
            "trend": "up",
            "change_30d_pct": 4.0,
            "confidence": "confirmed",
            "note": "~30% of all staked ETH. stETH is the dominant liquid staking token.",
        },
        "token": {
            "price_usd": 0.29,
            "market_cap_usd": 247_000_000,
            "fdv_usd": 290_000_000,
            "confidence": "confirmed",
            "source": "CoinGecko 2026-03-24",
        },
        "key_person": {
            "name": "Konstantin Lomashuk",
            "role": "Co-founder (P2P Validator)",
            "confidence": "confirmed",
            "note": "Also: Vasiliy Shapovalov (CTO), Kasper Rasmussen (team lead)",
        },
        "top_governance_holders": [
            {"entity": "Lido Treasury", "pct_supply": "~20%", "confidence": "derived"},
            {"entity": "Paradigm", "pct_supply": "~5%", "confidence": "estimated"},
            {"entity": "Dragonfly Capital", "pct_supply": "~3%", "confidence": "estimated"},
        ],
        "revenue": {
            "model": "10% fee on staking rewards (5% to node operators, 5% to DAO treasury)",
            "annualized_protocol_revenue_usd": 80_000_000,
            "annualized_gross_fees_usd": 160_000_000,
            "confidence": "estimated",
            "note": "At ~3.5% ETH staking yield on $18B TVL = ~$630M rewards, 10% take = ~$63M. Actual may be higher with MEV.",
        },
        "audit_status": {
            "audited": True,
            "auditors": ["ChainSecurity", "Certora", "StateMind", "Hexens", "Oxorio", "MixBytes", "SigmaPrime", "Quantstamp"],
            "confidence": "confirmed",
            "note": "Extensive audit history. Immunefi bug bounty. Open-source.",
        },
        "exploit_history": [
            {
                "date": "N/A",
                "description": "No direct protocol exploit to date.",
                "loss_usd": 0,
                "confidence": "confirmed",
            },
        ],
        "competitive_position": {
            "summary": "#2 DeFi protocol by TVL. Dominates liquid staking with ~30% of staked ETH. stETH deeply integrated across DeFi as collateral.",
            "moat": "Network effects of stETH as collateral, node operator set, early mover",
            "threats": "Ethereum centralization concerns (single entity staking cap debates), EigenLayer restaking competition, Rocket Pool/Coinbase cbETH",
            "confidence": "derived",
        },
    },

    # ──────────────────────────────────────────────────────────────
    # 5. Compound (COMP)
    # ──────────────────────────────────────────────────────────────
    {
        "name": "Compound",
        "ticker": "COMP",
        "category": "Lending / Borrowing",
        "chain": "Ethereum, Arbitrum, Base, Polygon, Optimism",
        "tvl": {
            "value_usd": 4_000_000_000,
            "trend": "stable",
            "change_30d_pct": 1.0,
            "confidence": "estimated",
            "note": "V3 (Comet) simplified architecture. Steady but losing share to Aave and Morpho.",
        },
        "token": {
            "price_usd": 17.95,
            "market_cap_usd": 173_500_000,
            "fdv_usd": 179_500_000,
            "confidence": "confirmed",
            "source": "MetaMask/CoinGecko 2026-03-29",
        },
        "key_person": {
            "name": "Robert Leshner",
            "role": "Founder (now focused on Superstate, RWA tokenization)",
            "confidence": "confirmed",
            "note": "Leshner stepped back from day-to-day; Compound Labs continues development",
        },
        "top_governance_holders": [
            {"entity": "Compound Treasury / Reserves", "pct_supply": "~25%", "confidence": "derived"},
            {"entity": "a16z", "pct_supply": "~7%", "confidence": "estimated"},
            {"entity": "Polychain Capital", "pct_supply": "~4%", "confidence": "estimated"},
        ],
        "revenue": {
            "model": "Interest rate spread (reserve factor) on borrows",
            "annualized_protocol_revenue_usd": 25_000_000,
            "annualized_gross_fees_usd": 120_000_000,
            "confidence": "estimated",
            "note": "Revenue well below Aave. V3 architecture is simpler but less capital-efficient than Morpho.",
        },
        "audit_status": {
            "audited": True,
            "auditors": ["OpenZeppelin", "Trail of Bits", "ChainSecurity"],
            "confidence": "confirmed",
        },
        "exploit_history": [
            {
                "date": "2021-09",
                "description": "COMP token distribution bug: ~$80M in COMP mistakenly distributed to users due to governance proposal error",
                "loss_usd": 80_000_000,
                "confidence": "confirmed",
                "note": "Not a hack; governance/code bug. ~$50M voluntarily returned.",
            },
        ],
        "competitive_position": {
            "summary": "Pioneer DeFi lending protocol. Losing market share to Aave and Morpho but retains institutional trust. Founder pivoted to RWAs.",
            "moat": "Brand recognition, institutional familiarity, simple V3 architecture",
            "threats": "Morpho capital efficiency, Aave multi-chain dominance, founder distraction",
            "confidence": "derived",
        },
    },

    # ──────────────────────────────────────────────────────────────
    # 6. Curve Finance (CRV)
    # ──────────────────────────────────────────────────────────────
    {
        "name": "Curve Finance",
        "ticker": "CRV",
        "category": "Stablecoin DEX / AMM",
        "chain": "Ethereum, multi-chain",
        "tvl": {
            "value_usd": 2_200_000_000,
            "trend": "down",
            "change_30d_pct": -8.0,
            "confidence": "estimated",
            "note": "TVL significantly declined from 2022 peak of $24B. Egorov liquidation events damaged confidence.",
        },
        "token": {
            "price_usd": 0.211,
            "market_cap_usd": 315_000_000,
            "fdv_usd": 700_000_000,
            "confidence": "confirmed",
            "source": "CoinGecko 2026-03-29. Down 98.6% from ATH of $15.37.",
        },
        "key_person": {
            "name": "Michael Egorov",
            "role": "Founder & CTO",
            "confidence": "confirmed",
            "note": "Liquidated for $140M in CRV June 2024. Sold 72M CRV OTC at $0.40 to repay. Requested 17.45M CRV ($6.6M) grant Dec 2025 for 2026 dev.",
        },
        "top_governance_holders": [
            {"entity": "Michael Egorov (personal + veCRV)", "pct_supply": "~25%", "confidence": "estimated", "note": "Reduced post-liquidation but still dominant"},
            {"entity": "Convex Finance (veCRV)", "pct_supply": "~35%", "confidence": "derived"},
            {"entity": "Yearn Finance / StakeDAO", "pct_supply": "~8%", "confidence": "estimated"},
        ],
        "revenue": {
            "model": "Trading fees (0.04% on stablecoin swaps), crvUSD interest, liquidation revenue",
            "annualized_protocol_revenue_usd": 15_000_000,
            "annualized_gross_fees_usd": 40_000_000,
            "confidence": "estimated",
            "note": "crvUSD adds revenue stream but adoption limited. Revenue down significantly from peak.",
        },
        "audit_status": {
            "audited": True,
            "auditors": ["Trail of Bits", "Quantstamp", "MixBytes"],
            "confidence": "confirmed",
            "note": "Vyper compiler vulnerability (July 2023) caused $70M exploit despite audited contracts.",
        },
        "exploit_history": [
            {
                "date": "2023-07-30",
                "description": "Vyper compiler reentrancy bug exploited across multiple Curve pools (alETH, msETH, pETH). ~$70M drained.",
                "loss_usd": 70_000_000,
                "confidence": "confirmed",
            },
            {
                "date": "2024-06",
                "description": "Egorov's $140M CRV position liquidated across multiple protocols, causing cascading bad debt",
                "loss_usd": 10_000_000,
                "confidence": "estimated",
                "note": "Bad debt on lending protocols, not direct Curve exploit",
            },
        ],
        "competitive_position": {
            "summary": "Once the dominant stablecoin DEX. Severely weakened by Vyper exploit, Egorov liquidation saga, and declining TVL. crvUSD stablecoin is innovation attempt.",
            "moat": "veCRV tokenomics (copied widely), stablecoin swap efficiency, DeFi composability",
            "threats": "Uniswap v3/v4 concentrated liquidity for stables, founder risk, declining community trust, Convex governance capture",
            "confidence": "derived",
        },
    },

    # ──────────────────────────────────────────────────────────────
    # 7. Synthetix (SNX)
    # ──────────────────────────────────────────────────────────────
    {
        "name": "Synthetix",
        "ticker": "SNX",
        "category": "Synthetic Assets / Perp Infrastructure",
        "chain": "Ethereum, Optimism, Base",
        "tvl": {
            "value_usd": 350_000_000,
            "trend": "down",
            "change_30d_pct": -12.0,
            "confidence": "estimated",
            "note": "TVL down significantly from 2021 highs. V3 launched but struggling for traction.",
        },
        "token": {
            "price_usd": 0.30,
            "market_cap_usd": 100_000_000,
            "fdv_usd": 100_000_000,
            "confidence": "confirmed",
            "source": "CoinDesk/CoinGecko 2026-03-19",
        },
        "key_person": {
            "name": "Kain Warwick",
            "role": "Founder",
            "confidence": "confirmed",
            "note": "Also co-founded Infinex (intent-based frontend). Stepped back from daily operations.",
        },
        "top_governance_holders": [
            {"entity": "Synthetix Treasury", "pct_supply": "~15%", "confidence": "derived"},
            {"entity": "Kain Warwick (personal)", "pct_supply": "~5%", "confidence": "estimated"},
            {"entity": "Framework Ventures", "pct_supply": "~3%", "confidence": "estimated"},
        ],
        "revenue": {
            "model": "Trading fees on synthetic assets and perps, distributed to SNX stakers. 100% fee revenue committed to buybacks in 2026.",
            "annualized_protocol_revenue_usd": 8_000_000,
            "annualized_gross_fees_usd": 8_000_000,
            "confidence": "estimated",
            "note": "Revenue has collapsed from 2023 highs. sUSD peg issues ongoing; 50/50 buyback split (SNX + sUSD) targeting peg restoration by Q2 2026.",
        },
        "audit_status": {
            "audited": True,
            "auditors": ["Iosiro", "Sigma Prime", "Etherscan"],
            "confidence": "confirmed",
        },
        "exploit_history": [
            {
                "date": "2019-06",
                "description": "Oracle manipulation: sKRW synth mispricing allowed attacker to extract ~$1B (mostly recovered via negotiation)",
                "loss_usd": 1_000_000,
                "confidence": "confirmed",
                "note": "Net loss minimal due to recovery",
            },
        ],
        "competitive_position": {
            "summary": "Pioneer synthetic assets protocol. V3 is modular liquidity layer powering multiple front-ends (Infinex, Kwenta). Struggling against Hyperliquid and GMX in perps.",
            "moat": "Composable synth infrastructure, multi-front-end model",
            "threats": "Hyperliquid dominance in perps, sUSD depeg risk, low TVL/volume, community fatigue",
            "confidence": "derived",
        },
    },

    # ──────────────────────────────────────────────────────────────
    # 8. GMX
    # ──────────────────────────────────────────────────────────────
    {
        "name": "GMX",
        "ticker": "GMX",
        "category": "Perpetual DEX",
        "chain": "Arbitrum, Avalanche, Solana",
        "tvl": {
            "value_usd": 450_000_000,
            "trend": "down",
            "change_30d_pct": -5.0,
            "confidence": "estimated",
            "note": "Expanded to Solana in 2025. Losing share to Hyperliquid.",
        },
        "token": {
            "price_usd": 6.63,
            "market_cap_usd": 69_000_000,
            "fdv_usd": 69_000_000,
            "confidence": "confirmed",
            "source": "MetaMask/CoinGecko 2026-03-18. ATH was $91.07.",
        },
        "key_person": {
            "name": "X (pseudonymous)",
            "role": "Lead developer (anonymous)",
            "confidence": "confirmed",
            "note": "Team is pseudonymous. Known community handle @xdev_10. DAO buyback $111K in March 2026.",
        },
        "top_governance_holders": [
            {"entity": "GMX Treasury / Floor Price Fund", "pct_supply": "~15%", "confidence": "derived"},
            {"entity": "Early contributors (vested)", "pct_supply": "~10%", "confidence": "estimated"},
            {"entity": "Binance Labs", "pct_supply": "~3%", "confidence": "estimated"},
        ],
        "revenue": {
            "model": "Trading fees split: 30% to GMX stakers, 70% to GLP/GM liquidity providers. Real yield protocol.",
            "annualized_protocol_revenue_usd": 30_000_000,
            "annualized_gross_fees_usd": 100_000_000,
            "confidence": "estimated",
            "note": "~$300B cumulative volume since 2021. Revenue down from peak. Solana expansion underway.",
        },
        "audit_status": {
            "audited": True,
            "auditors": ["ABDK", "Guardrails"],
            "confidence": "confirmed",
        },
        "exploit_history": [
            {
                "date": "2022-09",
                "description": "Price manipulation via AVAX low liquidity exploit. ~$565K lost.",
                "loss_usd": 565_000,
                "confidence": "confirmed",
            },
        ],
        "competitive_position": {
            "summary": "Pioneer pool-based perp DEX. Real-yield model attracted institutional attention. Losing ground to Hyperliquid order book. Multi-chain expansion is survival strategy.",
            "moat": "Real yield model, GLP/GM liquidity pool design, Arbitrum ecosystem position",
            "threats": "Hyperliquid volume dominance, order book superiority for large traders, Solana expansion risk",
            "confidence": "derived",
        },
    },

    # ──────────────────────────────────────────────────────────────
    # 9. dYdX
    # ──────────────────────────────────────────────────────────────
    {
        "name": "dYdX",
        "ticker": "DYDX",
        "category": "Perpetual DEX (Order Book)",
        "chain": "dYdX Chain (Cosmos SDK appchain)",
        "tvl": {
            "value_usd": 1_000_000_000,
            "trend": "stable",
            "change_30d_pct": 2.0,
            "confidence": "estimated",
            "note": "Migrated to sovereign appchain. Daily volume ~$2.8B.",
        },
        "token": {
            "price_usd": 0.097,
            "market_cap_usd": 80_000_000,
            "fdv_usd": 97_000_000,
            "confidence": "confirmed",
            "source": "CoinMarketCap 2026-03-28. 830M circulating supply.",
        },
        "key_person": {
            "name": "Antonio Juliano",
            "role": "Founder & CEO",
            "confidence": "confirmed",
        },
        "top_governance_holders": [
            {"entity": "dYdX Foundation / Treasury", "pct_supply": "~27%", "confidence": "derived"},
            {"entity": "a16z", "pct_supply": "~7%", "confidence": "estimated"},
            {"entity": "Paradigm", "pct_supply": "~5%", "confidence": "estimated"},
        ],
        "revenue": {
            "model": "Trading fees (maker/taker). 25% of protocol fees allocated to token buybacks.",
            "annualized_protocol_revenue_usd": 40_000_000,
            "annualized_gross_fees_usd": 40_000_000,
            "confidence": "estimated",
            "note": "Cosmos appchain captures all fees (no ETH gas). Spot trading integration + Telegram trading in pipeline.",
        },
        "audit_status": {
            "audited": True,
            "auditors": ["Trail of Bits", "Peckshield", "Informal Systems (Cosmos)"],
            "confidence": "confirmed",
        },
        "exploit_history": [
            {
                "date": "2023-11",
                "description": "YFI market manipulation on dYdX v3: attacker pumped YFI to drain insurance fund. ~$9M lost.",
                "loss_usd": 9_000_000,
                "confidence": "confirmed",
            },
        ],
        "competitive_position": {
            "summary": "Order book perp DEX on own appchain. Clean UX but losing volume share to Hyperliquid. Cosmos migration isolated it from EVM DeFi composability.",
            "moat": "Sovereign chain, institutional-grade order book, regulatory positioning",
            "threats": "Hyperliquid outperforming on volume/UX, chain isolation from EVM, token price collapse (-97% from ATH)",
            "confidence": "derived",
        },
    },

    # ──────────────────────────────────────────────────────────────
    # 10. Pendle (PENDLE)
    # ──────────────────────────────────────────────────────────────
    {
        "name": "Pendle",
        "ticker": "PENDLE",
        "category": "Yield Trading / Tokenization",
        "chain": "Ethereum, Arbitrum, BSC, Optimism, Mantle",
        "tvl": {
            "value_usd": 5_800_000_000,
            "trend": "up",
            "change_30d_pct": 5.0,
            "confidence": "estimated",
            "note": "Surged from $3B to $11B in mid-2025. ~75% of funds are Ethena USDe related.",
        },
        "token": {
            "price_usd": 1.34,
            "market_cap_usd": 222_000_000,
            "fdv_usd": 370_000_000,
            "confidence": "confirmed",
            "source": "CoinGecko 2026-03-28",
        },
        "key_person": {
            "name": "TN Lee",
            "role": "Co-founder & CEO",
            "confidence": "confirmed",
            "note": "Former head of Kyber Network business. Also: Vu Nguyen (co-founder)",
        },
        "top_governance_holders": [
            {"entity": "Pendle Team / Ecosystem Fund", "pct_supply": "~20%", "confidence": "estimated"},
            {"entity": "Binance Labs", "pct_supply": "~5%", "confidence": "estimated"},
            {"entity": "Spartan Group", "pct_supply": "~3%", "confidence": "estimated"},
        ],
        "revenue": {
            "model": "3% fee on yield from expired PT tokens + swap fees on YT/PT AMM",
            "annualized_protocol_revenue_usd": 40_000_000,
            "annualized_gross_fees_usd": 40_000_000,
            "confidence": "confirmed",
            "note": "$47.8B trading volume in 2025. Boros (new product) launching for funding rate trading.",
        },
        "audit_status": {
            "audited": True,
            "auditors": ["Dedaub", "Dingbats", "Ackee Blockchain"],
            "confidence": "confirmed",
        },
        "exploit_history": [
            {
                "date": "N/A",
                "description": "No direct protocol exploit to date.",
                "loss_usd": 0,
                "confidence": "confirmed",
            },
        ],
        "competitive_position": {
            "summary": "Dominant yield trading protocol. Created new DeFi primitive (yield tokenization). Massive TVL growth but heavily dependent on Ethena/points meta.",
            "moat": "Novel yield tokenization primitive, first mover in yield trading, deep integrations",
            "threats": "Ethena concentration risk (~75% TVL), points meta dependency, yield compression in bear market",
            "confidence": "derived",
        },
    },

    # ──────────────────────────────────────────────────────────────
    # 11. EigenLayer (EIGEN)
    # ──────────────────────────────────────────────────────────────
    {
        "name": "EigenLayer",
        "ticker": "EIGEN",
        "category": "Restaking",
        "chain": "Ethereum",
        "tvl": {
            "value_usd": 13_000_000_000,
            "trend": "down",
            "change_30d_pct": -10.0,
            "confidence": "estimated",
            "note": "~68% of $26B restaking market. Rebranded to EigenCloud. TVL volatile as points farming ends.",
        },
        "token": {
            "price_usd": 0.18,
            "market_cap_usd": 116_000_000,
            "fdv_usd": 1_080_000_000,
            "confidence": "confirmed",
            "source": "MetaMask/CoinGecko 2026-03-27. ATH was $5.65. Down 97%.",
        },
        "key_person": {
            "name": "Sreeram Kannan",
            "role": "Founder & CEO (UW professor)",
            "confidence": "confirmed",
        },
        "top_governance_holders": [
            {"entity": "Eigen Foundation / Ecosystem", "pct_supply": "~45%", "confidence": "derived"},
            {"entity": "a16z", "pct_supply": "~10%", "confidence": "estimated"},
            {"entity": "Coinbase Ventures + other VCs", "pct_supply": "~8%", "confidence": "estimated"},
        ],
        "revenue": {
            "model": "AVS (Actively Validated Services) pay fees to restakers. EigenLayer takes protocol cut.",
            "annualized_protocol_revenue_usd": 5_000_000,
            "annualized_gross_fees_usd": 20_000_000,
            "confidence": "estimated",
            "note": "Revenue nascent. EigenDA is first major AVS. Business model unproven at scale. Most TVL is speculative/points-driven.",
        },
        "audit_status": {
            "audited": True,
            "auditors": ["Dedaub", "Sigma Prime", "ConsenSys Diligence"],
            "confidence": "confirmed",
            "note": "Code quality rated excellent by Dedaub. Multiple rounds of audits.",
        },
        "exploit_history": [
            {
                "date": "2024-10",
                "description": "Email compromise led to theft of 1.67M EIGEN tokens ($5.7M) from investor during token claim",
                "loss_usd": 5_700_000,
                "confidence": "confirmed",
                "note": "Not a smart contract exploit; social engineering / email hack",
            },
        ],
        "competitive_position": {
            "summary": "Created restaking category. Massive TVL but revenue model unproven. Token has crashed 97% from ATH. EigenDA is key product. Rebranded to EigenCloud.",
            "moat": "Restaking primitive, ETH security sharing concept, VC backing, EigenDA first-mover",
            "threats": "Symbiotic (Lido-backed competitor), revenue reality vs hype, token unlock pressure, AVS adoption uncertainty",
            "confidence": "derived",
        },
    },

    # ──────────────────────────────────────────────────────────────
    # 12. Ethena (ENA)
    # ──────────────────────────────────────────────────────────────
    {
        "name": "Ethena",
        "ticker": "ENA",
        "category": "Synthetic Dollar (USDe)",
        "chain": "Ethereum, multi-chain",
        "tvl": {
            "value_usd": 7_400_000_000,
            "trend": "down",
            "change_30d_pct": -15.0,
            "confidence": "estimated",
            "note": "Halved from $14.8B peak after Oct market crash. $8B in outflows over two months. USDe is 3rd largest stablecoin by market cap.",
        },
        "token": {
            "price_usd": 0.093,
            "market_cap_usd": 793_000_000,
            "fdv_usd": 1_400_000_000,
            "confidence": "confirmed",
            "source": "CoinGecko 2026-03-28",
        },
        "key_person": {
            "name": "Guy Young",
            "role": "Founder & CEO",
            "confidence": "confirmed",
        },
        "top_governance_holders": [
            {"entity": "Ethena Labs / Foundation", "pct_supply": "~30%", "confidence": "derived"},
            {"entity": "Dragonfly Capital", "pct_supply": "~5%", "confidence": "estimated"},
            {"entity": "Franklin Templeton / Fidelity (indirect via USDe)", "pct_supply": "~3%", "confidence": "rumored"},
        ],
        "revenue": {
            "model": "Funding rate arbitrage: long spot ETH/BTC + short perps. Revenue = positive funding rates. sUSDe yield to holders.",
            "annualized_protocol_revenue_usd": 100_000_000,
            "annualized_gross_fees_usd": 200_000_000,
            "confidence": "estimated",
            "note": "Revenue highly cyclical (depends on positive funding rates). Hyperliquid perp integration could add $59-351M. Kraken appointed institutional custodian Jan 2026.",
        },
        "audit_status": {
            "audited": True,
            "auditors": ["Quantstamp", "Pashov", "Code4rena", "Spearbit"],
            "confidence": "confirmed",
            "note": "No critical/high issues in any audit. Bug bounty via Immunefi.",
        },
        "exploit_history": [
            {
                "date": "N/A",
                "description": "No direct exploit. Key risk is negative funding rate scenario causing USDe depeg.",
                "loss_usd": 0,
                "confidence": "confirmed",
                "note": "Not yet tested in prolonged negative funding environment. Reserve fund exists as backstop.",
            },
        ],
        "competitive_position": {
            "summary": "Fastest-growing stablecoin in DeFi history. ~$7.4B USDe outstanding. Innovative delta-neutral model but cyclical revenue. Deep Pendle integration.",
            "moat": "Novel delta-neutral mechanism, high sUSDe yields, institutional custody (Kraken), multi-chain deployment",
            "threats": "Negative funding rate risk, regulatory classification, centralized custodian dependency, concentration of hedge positions",
            "confidence": "derived",
        },
    },

    # ──────────────────────────────────────────────────────────────
    # 13. Jupiter (JUP)
    # ──────────────────────────────────────────────────────────────
    {
        "name": "Jupiter",
        "ticker": "JUP",
        "category": "DEX Aggregator / DeFi Hub",
        "chain": "Solana",
        "tvl": {
            "value_usd": 2_500_000_000,
            "trend": "up",
            "change_30d_pct": 5.0,
            "confidence": "estimated",
            "note": "Evolved from pure aggregator to full DeFi platform. Jupiter Lend hit $1.65B TVL by Oct 2025. $700M daily swap volume.",
        },
        "token": {
            "price_usd": 0.193,
            "market_cap_usd": 675_000_000,
            "fdv_usd": 1_930_000_000,
            "confidence": "confirmed",
            "source": "CoinMarketCap 2026-03-04. ParaFi $35M investment absorbed unlock selling pressure.",
        },
        "key_person": {
            "name": "Meow (pseudonymous)",
            "role": "Co-founder",
            "confidence": "confirmed",
            "note": "Also: Ben Chow (co-founder). Meow is public-facing leader of Jupiter ecosystem.",
        },
        "top_governance_holders": [
            {"entity": "Jupiter Team / Treasury", "pct_supply": "~50%", "confidence": "derived"},
            {"entity": "Community airdrop recipients", "pct_supply": "~40%", "confidence": "derived"},
            {"entity": "ParaFi Capital", "pct_supply": "~2%", "confidence": "confirmed"},
        ],
        "revenue": {
            "model": "Aggregator routing fees, Jupiter Lend interest, perp trading fees, launchpad fees",
            "annualized_protocol_revenue_usd": 50_000_000,
            "annualized_gross_fees_usd": 80_000_000,
            "confidence": "estimated",
            "note": "Solana's dominant DeFi frontend. Revenue diversifying across lending, perps, and launchpad.",
        },
        "audit_status": {
            "audited": True,
            "auditors": ["OtterSec", "Neodyme"],
            "confidence": "confirmed",
        },
        "exploit_history": [
            {
                "date": "N/A",
                "description": "No direct protocol exploit to date.",
                "loss_usd": 0,
                "confidence": "confirmed",
            },
        ],
        "competitive_position": {
            "summary": "Solana's #1 DeFi app. Expanded from aggregation to lending, perps, and launchpad. Capturing the Solana DeFi super-app thesis.",
            "moat": "Solana routing dominance, user base, brand, full-stack DeFi expansion",
            "threats": "Raydium building own aggregator, Solana ecosystem concentration risk, regulatory uncertainty",
            "confidence": "derived",
        },
    },

    # ──────────────────────────────────────────────────────────────
    # 14. Raydium (RAY)
    # ──────────────────────────────────────────────────────────────
    {
        "name": "Raydium",
        "ticker": "RAY",
        "category": "AMM / DEX",
        "chain": "Solana",
        "tvl": {
            "value_usd": 2_300_000_000,
            "trend": "up",
            "change_30d_pct": 3.0,
            "confidence": "estimated",
            "note": "Grew 32.3% QoQ in Q3 2025. Dominant Solana AMM, especially for memecoin/new token launches.",
        },
        "token": {
            "price_usd": 0.58,
            "market_cap_usd": 156_000_000,
            "fdv_usd": 320_000_000,
            "confidence": "confirmed",
            "source": "CoinGecko 2026-03-28. Range $0.57-0.61 in March.",
        },
        "key_person": {
            "name": "AlphaRay (pseudonymous)",
            "role": "Lead developer",
            "confidence": "confirmed",
            "note": "Team is pseudonymous. Raydium team based in Asia.",
        },
        "top_governance_holders": [
            {"entity": "Raydium Treasury", "pct_supply": "~25%", "confidence": "derived"},
            {"entity": "Team (vested)", "pct_supply": "~15%", "confidence": "estimated"},
            {"entity": "Alameda/FTX estate (residual)", "pct_supply": "~3%", "confidence": "estimated"},
        ],
        "revenue": {
            "model": "Trading fees (0.25% standard, lower for concentrated liquidity). Memecoin launch fees.",
            "annualized_protocol_revenue_usd": 80_000_000,
            "annualized_gross_fees_usd": 200_000_000,
            "confidence": "estimated",
            "note": "Benefited enormously from Solana memecoin boom. Revenue correlated with speculative activity.",
        },
        "audit_status": {
            "audited": True,
            "auditors": ["Kudelski Security", "MadShield"],
            "confidence": "confirmed",
        },
        "exploit_history": [
            {
                "date": "2022-12",
                "description": "Private key compromise (linked to FTX/Alameda relationship). ~$4.4M drained from liquidity pools.",
                "loss_usd": 4_400_000,
                "confidence": "confirmed",
            },
        ],
        "competitive_position": {
            "summary": "Solana's primary AMM. Captures huge memecoin launch volume. Building own swap aggregator to compete with Jupiter routing.",
            "moat": "Deep Solana liquidity, memecoin launch ecosystem, Serum/OpenBook integration",
            "threats": "Jupiter aggregator dominance, pump.fun competition for launches, Orca concentrated liquidity",
            "confidence": "derived",
        },
    },

    # ──────────────────────────────────────────────────────────────
    # 15. Orca
    # ──────────────────────────────────────────────────────────────
    {
        "name": "Orca",
        "ticker": "ORCA",
        "category": "Concentrated Liquidity DEX",
        "chain": "Solana",
        "tvl": {
            "value_usd": 400_000_000,
            "trend": "stable",
            "change_30d_pct": 0.0,
            "confidence": "estimated",
            "note": "Focused on concentrated liquidity (Whirlpools). Clean UX but smaller than Raydium.",
        },
        "token": {
            "price_usd": 0.90,
            "market_cap_usd": 54_500_000,
            "fdv_usd": 90_000_000,
            "confidence": "confirmed",
            "source": "CoinGecko 2026-03-28",
        },
        "key_person": {
            "name": "Yutaro Mori (Ori)",
            "role": "Co-founder",
            "confidence": "confirmed",
            "note": "Also: Grace Kwan (co-founder, design lead)",
        },
        "top_governance_holders": [
            {"entity": "Orca Foundation / Treasury", "pct_supply": "~30%", "confidence": "estimated"},
            {"entity": "Team (vested)", "pct_supply": "~15%", "confidence": "estimated"},
            {"entity": "Polychain Capital", "pct_supply": "~5%", "confidence": "estimated"},
        ],
        "revenue": {
            "model": "LP swap fees (protocol takes small cut). Fee tiers vary by pool.",
            "annualized_protocol_revenue_usd": 10_000_000,
            "annualized_gross_fees_usd": 30_000_000,
            "confidence": "estimated",
            "note": "Revenue smaller than Raydium. Focused on capital efficiency rather than volume.",
        },
        "audit_status": {
            "audited": True,
            "auditors": ["Neodyme", "Kudelski Security"],
            "confidence": "confirmed",
        },
        "exploit_history": [
            {
                "date": "N/A",
                "description": "No direct protocol exploit to date.",
                "loss_usd": 0,
                "confidence": "confirmed",
            },
        ],
        "competitive_position": {
            "summary": "Solana's concentrated liquidity leader. Best UX among Solana DEXs. Smaller TVL but higher capital efficiency per dollar.",
            "moat": "UX quality, concentrated liquidity design (Whirlpools), developer experience",
            "threats": "Raydium CLMM competition, Jupiter routing bypasses, smaller liquidity depth",
            "confidence": "derived",
        },
    },

    # ──────────────────────────────────────────────────────────────
    # 16. Marinade (MNDE)
    # ──────────────────────────────────────────────────────────────
    {
        "name": "Marinade",
        "ticker": "MNDE",
        "category": "Liquid Staking",
        "chain": "Solana",
        "tvl": {
            "value_usd": 1_200_000_000,
            "trend": "stable",
            "change_30d_pct": -2.0,
            "confidence": "estimated",
            "note": "mSOL is Solana's original liquid staking token. Emphasizes validator diversification.",
        },
        "token": {
            "price_usd": 0.019,
            "market_cap_usd": 11_750_000,
            "fdv_usd": 19_000_000,
            "confidence": "confirmed",
            "source": "CoinGecko 2026-03-28. Down 97%+ from ATH.",
        },
        "key_person": {
            "name": "Michael Repetny",
            "role": "Co-founder",
            "confidence": "confirmed",
            "note": "Marinade team is relatively small and distributed.",
        },
        "top_governance_holders": [
            {"entity": "Marinade Treasury", "pct_supply": "~30%", "confidence": "estimated"},
            {"entity": "Team (vested)", "pct_supply": "~15%", "confidence": "estimated"},
            {"entity": "Multicoin Capital", "pct_supply": "~5%", "confidence": "estimated"},
        ],
        "revenue": {
            "model": "Commission on staking rewards (typically 2-6% of rewards). Native staking + liquid staking.",
            "annualized_protocol_revenue_usd": 8_000_000,
            "annualized_gross_fees_usd": 15_000_000,
            "confidence": "estimated",
            "note": "Revenue from SOL staking yield. Marinade Native (no liquid token) added as alternative.",
        },
        "audit_status": {
            "audited": True,
            "auditors": ["Neodyme", "Ackee Blockchain"],
            "confidence": "confirmed",
        },
        "exploit_history": [
            {
                "date": "N/A",
                "description": "No direct protocol exploit to date.",
                "loss_usd": 0,
                "confidence": "confirmed",
            },
        ],
        "competitive_position": {
            "summary": "Solana's first liquid staking protocol. mSOL widely used but losing share to Jito's JitoSOL which adds MEV yield.",
            "moat": "mSOL DeFi integrations, validator diversification focus, Solana native",
            "threats": "Jito (MEV-enhanced yield), Sanctum (liquid staking aggregator), tiny market cap",
            "confidence": "derived",
        },
    },

    # ──────────────────────────────────────────────────────────────
    # 17. Jito (JTO)
    # ──────────────────────────────────────────────────────────────
    {
        "name": "Jito",
        "ticker": "JTO",
        "category": "MEV + Liquid Staking",
        "chain": "Solana",
        "tvl": {
            "value_usd": 2_500_000_000,
            "trend": "up",
            "change_30d_pct": 8.0,
            "confidence": "estimated",
            "note": "JitoSOL captures MEV tips on top of staking yield. Driving Solana TVL growth alongside Kamino.",
        },
        "token": {
            "price_usd": 0.31,
            "market_cap_usd": 140_500_000,
            "fdv_usd": 310_000_000,
            "confidence": "confirmed",
            "source": "CoinGecko 2026-03-28. 450M circulating supply.",
        },
        "key_person": {
            "name": "Lucas Bruder",
            "role": "CEO, Jito Labs",
            "confidence": "confirmed",
        },
        "top_governance_holders": [
            {"entity": "Jito Foundation", "pct_supply": "~35%", "confidence": "derived"},
            {"entity": "Multicoin Capital", "pct_supply": "~8%", "confidence": "estimated"},
            {"entity": "Framework Ventures", "pct_supply": "~5%", "confidence": "estimated"},
        ],
        "revenue": {
            "model": "MEV tips distribution (Jito-Solana client captures MEV). Liquid staking commission. Jito Block Engine fees.",
            "annualized_protocol_revenue_usd": 50_000_000,
            "annualized_gross_fees_usd": 100_000_000,
            "confidence": "estimated",
            "note": "Jito-Solana client runs ~80% of Solana validators. MEV revenue is unique differentiator vs Marinade.",
        },
        "audit_status": {
            "audited": True,
            "auditors": ["Neodyme", "OtterSec"],
            "confidence": "confirmed",
        },
        "exploit_history": [
            {
                "date": "N/A",
                "description": "No direct protocol exploit. Jito block engine controversial for MEV extraction but not exploited.",
                "loss_usd": 0,
                "confidence": "confirmed",
            },
        ],
        "competitive_position": {
            "summary": "Solana's MEV infrastructure layer + liquid staking. JitoSOL offers higher yield than mSOL. Jito-Solana client dominates validator set.",
            "moat": "MEV infrastructure monopoly on Solana, ~80% validator client share, JitoSOL yield premium",
            "threats": "MEV centralization concerns, Solana Foundation pushback on MEV extraction, Marinade/Sanctum competition",
            "confidence": "derived",
        },
    },

    # ──────────────────────────────────────────────────────────────
    # 18. Drift Protocol
    # ──────────────────────────────────────────────────────────────
    {
        "name": "Drift",
        "ticker": "DRIFT",
        "category": "Perpetual DEX",
        "chain": "Solana",
        "tvl": {
            "value_usd": 300_000_000,
            "trend": "stable",
            "change_30d_pct": -3.0,
            "confidence": "estimated",
            "note": "Sub-400ms execution. Growing but small relative to Hyperliquid.",
        },
        "token": {
            "price_usd": 0.075,
            "market_cap_usd": 43_000_000,
            "fdv_usd": 75_000_000,
            "confidence": "confirmed",
            "source": "CoinGecko 2026-03-28. Down 97% from ATH.",
        },
        "key_person": {
            "name": "Cindy Leow",
            "role": "Co-founder",
            "confidence": "confirmed",
            "note": "Also: David Lu (co-founder, CTO)",
        },
        "top_governance_holders": [
            {"entity": "Drift Foundation", "pct_supply": "~40%", "confidence": "estimated"},
            {"entity": "Multicoin Capital", "pct_supply": "~8%", "confidence": "estimated"},
            {"entity": "Jump Crypto", "pct_supply": "~5%", "confidence": "estimated"},
        ],
        "revenue": {
            "model": "Perpetual trading fees (maker/taker), insurance fund revenue, spot fees",
            "annualized_protocol_revenue_usd": 10_000_000,
            "annualized_gross_fees_usd": 25_000_000,
            "confidence": "estimated",
            "note": "Expanding into spot trading and prediction markets. Revenue small but growing.",
        },
        "audit_status": {
            "audited": True,
            "auditors": ["OtterSec", "Neodyme"],
            "confidence": "confirmed",
        },
        "exploit_history": [
            {
                "date": "N/A",
                "description": "No major direct protocol exploit. Minor oracle issues in early versions.",
                "loss_usd": 0,
                "confidence": "confirmed",
            },
        ],
        "competitive_position": {
            "summary": "Solana's leading perp DEX. Fast execution leveraging Solana's speed. Expanding to spot + prediction markets. Tiny vs Hyperliquid.",
            "moat": "Solana-native speed, vAMM + DLOB hybrid model, Solana DeFi ecosystem integration",
            "threats": "Hyperliquid dominance, Jupiter perps competition, small liquidity depth",
            "confidence": "derived",
        },
    },

    # ──────────────────────────────────────────────────────────────
    # 19. Hyperliquid (HYPE)
    # ──────────────────────────────────────────────────────────────
    {
        "name": "Hyperliquid",
        "ticker": "HYPE",
        "category": "L1 Perpetual DEX",
        "chain": "Hyperliquid L1 (custom chain)",
        "tvl": {
            "value_usd": 4_500_000_000,
            "trend": "up",
            "change_30d_pct": 10.0,
            "confidence": "confirmed",
            "note": "$200B monthly trading volume. $5B open interest. Top-earning protocol by fees.",
        },
        "token": {
            "price_usd": 38.02,
            "market_cap_usd": 9_050_000_000,
            "fdv_usd": 38_020_000_000,
            "confidence": "confirmed",
            "source": "CoinGecko 2026-03-28",
        },
        "key_person": {
            "name": "Jeff Yan",
            "role": "Founder & CEO",
            "confidence": "confirmed",
            "note": "Ex-Hudson River Trading. Harvard math. No VC funding taken.",
        },
        "top_governance_holders": [
            {"entity": "Hyper Foundation / Team", "pct_supply": "~60%", "confidence": "derived", "note": "Massive insider allocation, no VC"},
            {"entity": "Community airdrop recipients", "pct_supply": "~31%", "confidence": "confirmed"},
            {"entity": "Assistance Fund (buyback/burn)", "pct_supply": "accumulating via 97% fee revenue", "confidence": "confirmed"},
        ],
        "revenue": {
            "model": "Trading fees (maker/taker). 97% of fees go to Assistance Fund for HYPE buybacks/burns.",
            "annualized_protocol_revenue_usd": 736_000_000,
            "annualized_gross_fees_usd": 830_000_000,
            "confidence": "confirmed",
            "source": "DefiLlama 2026-03-20",
            "note": "$54M fees in Feb-Mar 2026. WTI oil perps saw $5B volume in 72 hours during Middle East volatility.",
        },
        "audit_status": {
            "audited": True,
            "auditors": ["Zellic", "Quantstamp"],
            "confidence": "confirmed",
            "note": "Custom L1 introduces non-standard risk profile. Bridge security is key concern.",
        },
        "exploit_history": [
            {
                "date": "2025-03",
                "description": "JELLY memecoin market manipulation: attacker exploited low-cap perp listing to drain HLP vault. ~$10M loss.",
                "loss_usd": 10_000_000,
                "confidence": "confirmed",
            },
            {
                "date": "2025",
                "description": "Concerns over centralized validator set (only team validators). Not exploited but architectural risk.",
                "loss_usd": 0,
                "confidence": "confirmed",
            },
        ],
        "competitive_position": {
            "summary": "Dominant perp DEX. Top protocol by fee revenue. No VC funding. $200B monthly volume. Custom L1 for speed. Killed dYdX and GMX volume share.",
            "moat": "Sub-second finality, CEX-like UX, massive fee revenue, no VC overhang, HYPE burn mechanism",
            "threats": "Centralized validator set, regulatory risk (unregistered exchange), bridge security, single-product risk",
            "confidence": "derived",
        },
    },

    # ──────────────────────────────────────────────────────────────
    # 20. Morpho
    # ──────────────────────────────────────────────────────────────
    {
        "name": "Morpho",
        "ticker": "MORPHO",
        "category": "Lending Aggregator / Modular Lending",
        "chain": "Ethereum, Base",
        "tvl": {
            "value_usd": 5_800_000_000,
            "trend": "up",
            "change_30d_pct": 12.0,
            "confidence": "confirmed",
            "note": "#2 lending protocol behind Aave. 7x capital efficiency vs Aave. Apollo Global Management partnership.",
        },
        "token": {
            "price_usd": 1.52,
            "market_cap_usd": 450_000_000,
            "fdv_usd": 1_520_000_000,
            "confidence": "confirmed",
            "source": "CoinGecko 2026-03-28",
        },
        "key_person": {
            "name": "Paul Frambot",
            "role": "Co-founder & CEO",
            "confidence": "confirmed",
            "note": "French. Ex-Polytechnique. Also: Merlin Egalite, Mathis Gontier Delaunay (co-founders).",
        },
        "top_governance_holders": [
            {"entity": "Morpho Association / Treasury", "pct_supply": "~35%", "confidence": "derived"},
            {"entity": "Apollo Global Management (option for 90M tokens over 48 months)", "pct_supply": "potential ~9%", "confidence": "confirmed"},
            {"entity": "a16z + Variant + Pantera", "pct_supply": "~10%", "confidence": "estimated"},
        ],
        "revenue": {
            "model": "Protocol fee on interest spread in Morpho Blue markets. Modular vault architecture allows permissionless market creation.",
            "annualized_protocol_revenue_usd": 113_000_000,
            "annualized_gross_fees_usd": 113_000_000,
            "confidence": "estimated",
            "note": "$310K daily fees with $3.4B active loans (Feb 2026). 1.33% efficiency index = 7x Aave. V2 launching with fixed-rate/fixed-term loans.",
        },
        "audit_status": {
            "audited": True,
            "auditors": ["Spearbit", "Trail of Bits", "Cantina"],
            "confidence": "confirmed",
            "note": "Morpho Blue is formally verified. Immunefi bug bounty.",
        },
        "exploit_history": [
            {
                "date": "N/A",
                "description": "No direct protocol exploit to date. Morpho Blue's minimal design reduces attack surface.",
                "loss_usd": 0,
                "confidence": "confirmed",
            },
        ],
        "competitive_position": {
            "summary": "Fastest-growing lending protocol. 7x more capital efficient than Aave. Apollo partnership signals institutional adoption. Modular architecture enables permissionless markets.",
            "moat": "Capital efficiency, modular vault architecture, formal verification, institutional backing (Apollo)",
            "threats": "Aave V4 catching up, smart contract risk in permissionless markets, protocol fee not yet widely activated",
            "confidence": "derived",
        },
    },
]
