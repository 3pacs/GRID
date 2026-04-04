"""GRID Sector Map — hierarchical mapping from sectors to subsectors to key actors.

Each actor has a `weight` representing their relative market-moving influence
within that subsector. This captures the reality that some companies and
institutions press the scale harder than others.

Structure:
    Sector -> Subsector -> [Actors]
    Each actor: {name, ticker/id, weight, type, features, description}

The `features` list maps to feature_registry names so we can pull z-scores
and connect macro/market data directly to the actors that influence them.

Covers 20 sectors across equity sectors (GICS-aligned), cross-asset classes,
and thematic verticals to map the full global capital system.
"""

SECTOR_MAP: dict[str, dict] = {
    # -------------------------------------------------------------------------
    # EXISTING SECTORS (unchanged)
    # -------------------------------------------------------------------------
    "Technology": {
        "etf": "XLK",
        "subsectors": {
            "Semiconductors": {
                "weight": 0.30,  # Semis drive the cycle
                "actors": [
                    {"name": "NVIDIA", "ticker": "NVDA", "weight": 0.30, "type": "company",
                     "description": "AI chip monopoly — sets the capex cycle for hyperscalers",
                     "features": ["nvda", "nvda_full"]},
                    {"name": "Broadcom", "ticker": "AVGO", "weight": 0.15, "type": "company",
                     "description": "Custom AI silicon + networking — VMware integration",
                     "features": ["avgo", "avgo_full"]},
                    {"name": "TSMC", "ticker": "TSM", "weight": 0.15, "type": "company",
                     "description": "Fabrication bottleneck — all advanced chips flow through here",
                     "features": ["tsm", "tsm_full"]},
                    {"name": "Intel", "ticker": "INTC", "weight": 0.10, "type": "company",
                     "description": "US fab champion — CHIPS Act beneficiary",
                     "features": ["intc", "intc_full"]},
                    {"name": "AMD", "ticker": "AMD", "weight": 0.05, "type": "company",
                     "description": "GPU/CPU competitor — MI300 AI accelerator challenger",
                     "features": ["amd", "amd_full"]},
                    {"name": "US CHIPS Act", "ticker": None, "weight": 0.15, "type": "policy",
                     "description": "$52B subsidy reshaping global fab investment",
                     "features": ["wiki_ai", "smh"]},
                    {"name": "China Export Controls", "ticker": None, "weight": 0.10, "type": "policy",
                     "description": "Restricts advanced chip sales — splits the supply chain",
                     "features": ["china_pmi_mfg", "shanghai"]},
                ],
            },
            "Software & Cloud": {
                "weight": 0.30,
                "actors": [
                    {"name": "Microsoft", "ticker": "MSFT", "weight": 0.25, "type": "company",
                     "description": "Azure + OpenAI integration — enterprise AI gatekeeper",
                     "features": ["msft", "msft_full"]},
                    {"name": "Amazon", "ticker": "AMZN", "weight": 0.20, "type": "company",
                     "description": "AWS dominates cloud infra spend",
                     "features": ["amzn", "amzn_full"]},
                    {"name": "Alphabet", "ticker": "GOOGL", "weight": 0.20, "type": "company",
                     "description": "Google Cloud + Gemini AI — search/ads monopoly",
                     "features": ["googl", "googl_full"]},
                    {"name": "Meta", "ticker": "META", "weight": 0.15, "type": "company",
                     "description": "Largest open-source AI spender — sets capex tone",
                     "features": ["meta", "meta_full"]},
                    {"name": "Salesforce", "ticker": "CRM", "weight": 0.08, "type": "company",
                     "description": "Enterprise SaaS bellwether — AI agent platform",
                     "features": ["crm", "crm_full"]},
                    {"name": "PayPal", "ticker": "PYPL", "weight": 0.07, "type": "company",
                     "description": "Digital payments — fintech/consumer spending proxy",
                     "features": ["pypl", "pypl_full"]},
                    {"name": "Comcast", "ticker": "CMCSA", "weight": 0.05, "type": "company",
                     "description": "Cable/broadband + NBCUniversal — media/connectivity spend",
                     "features": ["cmcsa", "cmcsa_full"]},
                ],
            },
            "Consumer Electronics": {
                "weight": 0.15,
                "actors": [
                    {"name": "Apple", "ticker": "AAPL", "weight": 0.60, "type": "company",
                     "description": "Consumer hardware + services — China demand proxy",
                     "features": ["aapl", "aapl_full"]},
                    {"name": "Tesla", "ticker": "TSLA", "weight": 0.25, "type": "company",
                     "description": "EV + energy + retail sentiment bellwether",
                     "features": ["tsla", "tsla_full"]},
                ],
            },
            "Payments & Fintech": {
                "weight": 0.25,
                "actors": [
                    {"name": "Visa", "ticker": "V", "weight": 0.35, "type": "company",
                     "description": "Global payment network — consumer spending proxy",
                     "features": ["v", "v_full"]},
                    {"name": "Mastercard", "ticker": "MA", "weight": 0.30, "type": "company",
                     "description": "Cross-border transactions — international travel/trade gauge",
                     "features": ["ma", "ma_full"]},
                ],
            },
        },
    },
    "Energy": {
        "etf": "XLE",
        "subsectors": {
            "Crude Oil": {
                "weight": 0.40,
                "actors": [
                    {"name": "Saudi Arabia / OPEC+", "ticker": None, "weight": 0.35, "type": "sovereign",
                     "description": "Controls ~40% of global oil supply — production cuts move price",
                     "features": ["crude_oil", "wti_crude"]},
                    {"name": "ExxonMobil", "ticker": "XOM", "weight": 0.15, "type": "company",
                     "description": "Largest US oil major — upstream/downstream bellwether",
                     "features": ["xom", "xom_full"]},
                    {"name": "US Shale (EOG/DVN)", "ticker": "EOG", "weight": 0.15, "type": "company",
                     "description": "Marginal swing producer — SPR policy target",
                     "features": ["eog", "eog_full", "dvn", "dvn_full"]},
                    {"name": "US Strategic Reserve", "ticker": None, "weight": 0.10, "type": "policy",
                     "description": "SPR draws/fills move short-term supply balance",
                     "features": ["crude_oil", "wti_crude"]},
                    {"name": "China Demand", "ticker": None, "weight": 0.15, "type": "macro",
                     "description": "Largest incremental oil importer — PMI drives demand forecasts",
                     "features": ["china_pmi_mfg"]},
                    {"name": "EIA Inventories", "ticker": None, "weight": 0.10, "type": "data",
                     "description": "Weekly stockpile report — supply/demand reality check",
                     "features": ["eia_crude_price"]},
                ],
            },
            "Natural Gas": {
                "weight": 0.20,
                "actors": [
                    {"name": "LNG Export Capacity", "ticker": None, "weight": 0.40, "type": "infra",
                     "description": "US LNG terminals set floor on domestic natgas price",
                     "features": ["eia_natgas_henry_hub"]},
                    {"name": "Weather (HDD/CDD)", "ticker": None, "weight": 0.35, "type": "physical",
                     "description": "Heating/cooling demand drives seasonal swings",
                     "features": ["weather_nyc_temp", "weather_london_temp"]},
                ],
            },
            "Renewables": {
                "weight": 0.15,
                "actors": [
                    {"name": "IRA Subsidies", "ticker": None, "weight": 0.50, "type": "policy",
                     "description": "Inflation Reduction Act — $369B in clean energy tax credits",
                     "features": ["icln", "patent_velocity_cleanenergy"]},
                ],
            },
        },
    },
    "Financials": {
        "etf": "XLF",
        "subsectors": {
            "Banks": {
                "weight": 0.40,
                "actors": [
                    {"name": "Federal Reserve", "ticker": None, "weight": 0.30, "type": "central_bank",
                     "description": "Sets fed funds rate, runs QT, regulates capital requirements",
                     "features": ["fed_funds_rate", "treasury_10y", "treasury_2y", "yield_curve_10y2y"]},
                    {"name": "JPMorgan Chase", "ticker": "JPM", "weight": 0.20, "type": "company",
                     "description": "Largest US bank — loan growth proxy",
                     "features": ["jpm", "jpm_full", "loan_growth"]},
                    {"name": "Bank of America", "ticker": "BAC", "weight": 0.15, "type": "company",
                     "description": "Consumer banking bellwether — rate sensitivity proxy",
                     "features": ["bac", "bac_full"]},
                    {"name": "Goldman Sachs", "ticker": "GS", "weight": 0.10, "type": "company",
                     "description": "Investment banking + trading — capital markets activity gauge",
                     "features": ["gs", "gs_full"]},
                    {"name": "FDIC / OCC", "ticker": None, "weight": 0.10, "type": "regulator",
                     "description": "Bank failure resolution, capital rules",
                     "features": ["loan_growth", "ted_spread"]},
                    {"name": "Regional Banks (KRE)", "ticker": "KRE", "weight": 0.15, "type": "subsector",
                     "description": "CRE exposure, deposit flight canary",
                     "features": ["kre"]},
                ],
            },
            "Insurance & Asset Mgmt": {
                "weight": 0.25,
                "actors": [
                    {"name": "Bond Yields", "ticker": None, "weight": 0.35, "type": "macro",
                     "description": "Insurance float returns track long-end yields",
                     "features": ["treasury_30y", "breakeven_10y"]},
                    {"name": "UnitedHealth", "ticker": "UNH", "weight": 0.25, "type": "company",
                     "description": "Largest health insurer — managed care bellwether",
                     "features": ["unh", "unh_full"]},
                    {"name": "BlackRock (BLK)", "ticker": "BLK", "weight": 0.20, "type": "company",
                     "description": "$10T AUM — passive flow + ETF dominance",
                     "features": ["blk", "blk_full"]},
                    {"name": "Berkshire Hathaway", "ticker": "BRK-B", "weight": 0.20, "type": "company",
                     "description": "Insurance + conglomerate — Buffett indicator",
                     "features": ["brk_b", "brk_b_full"]},
                ],
            },
            "Credit Markets": {
                "weight": 0.35,
                "actors": [
                    {"name": "HY Spread", "ticker": None, "weight": 0.35, "type": "indicator",
                     "description": "High-yield spread — real-time credit stress gauge",
                     "features": ["hy_spread", "hyg", "lqd"]},
                    {"name": "Chicago Fed NFCI", "ticker": None, "weight": 0.25, "type": "indicator",
                     "description": "National Financial Conditions Index — tightening/easing",
                     "features": ["chicago_fed"]},
                    {"name": "IG/HY Issuance", "ticker": None, "weight": 0.20, "type": "flow",
                     "description": "New bond issuance pace — risk appetite proxy",
                     "features": ["hyg", "lqd", "hy_spread"]},
                ],
            },
        },
    },
    "Commodities": {
        "etf": "DBC",
        "subsectors": {
            "Precious Metals": {
                "weight": 0.30,
                "actors": [
                    {"name": "Central Bank Buying", "ticker": None, "weight": 0.30, "type": "sovereign",
                     "description": "China/India/Turkey CBs accumulating gold — structural bid",
                     "features": ["gold", "gold_full"]},
                    {"name": "Real Rates", "ticker": None, "weight": 0.35, "type": "macro",
                     "description": "Gold inversely tracks TIPS yields",
                     "features": ["breakeven_10y", "treasury_10y"]},
                    {"name": "Dollar", "ticker": None, "weight": 0.25, "type": "fx",
                     "description": "Strong dollar = gold headwind (inverse correlation)",
                     "features": ["dollar_index"]},
                ],
            },
            "Industrial Metals": {
                "weight": 0.30,
                "actors": [
                    {"name": "China Construction", "ticker": None, "weight": 0.40, "type": "macro",
                     "description": "China property sector drives >50% of copper demand",
                     "features": ["copper", "china_pmi_mfg"]},
                    {"name": "EV Transition", "ticker": None, "weight": 0.25, "type": "structural",
                     "description": "Each EV uses 4x the copper of an ICE vehicle",
                     "features": ["lit", "tsla", "copper"]},
                ],
            },
        },
    },
    "Healthcare": {
        "etf": "XLV",
        "subsectors": {
            "Pharma": {
                "weight": 0.40,
                "actors": [
                    {"name": "FDA Approvals", "ticker": None, "weight": 0.20, "type": "regulator",
                     "description": "Drug approvals and recalls — binary catalysts",
                     "features": ["fda_adverse_events"]},
                    {"name": "IRA Drug Pricing", "ticker": None, "weight": 0.15, "type": "policy",
                     "description": "Medicare negotiation compresses pharma margins",
                     "features": ["xlv", "xbi"]},
                    {"name": "Johnson & Johnson", "ticker": "JNJ", "weight": 0.15, "type": "company",
                     "description": "Pharma + MedTech diversified — defensive healthcare name",
                     "features": ["jnj", "jnj_full"]},
                    {"name": "Pfizer", "ticker": "PFE", "weight": 0.12, "type": "company",
                     "description": "Vaccine + oncology pipeline — post-COVID revenue normalization",
                     "features": ["pfe", "pfe_full"]},
                    {"name": "Merck", "ticker": "MRK", "weight": 0.12, "type": "company",
                     "description": "Keytruda franchise — oncology leadership",
                     "features": ["mrk", "mrk_full"]},
                    {"name": "AbbVie", "ticker": "ABBV", "weight": 0.10, "type": "company",
                     "description": "Humira LOE + Skyrizi/Rinvoq growth — immunology pivot",
                     "features": ["abbv", "abbv_full"]},
                    {"name": "Cigna (CI)", "ticker": "CI", "weight": 0.08, "type": "company",
                     "description": "Health services + PBM — managed care margins",
                     "features": ["ci", "ci_full"]},
                ],
            },
            "Biotech": {
                "weight": 0.30,
                "actors": [
                    {"name": "Eli Lilly", "ticker": "LLY", "weight": 0.40, "type": "company",
                     "description": "GLP-1 leader (Mounjaro/Zepbound) — obesity market catalyst",
                     "features": ["lly", "lly_full"]},
                    {"name": "Thermo Fisher", "ticker": "TMO", "weight": 0.25, "type": "company",
                     "description": "Life science tools — biotech R&D spend proxy",
                     "features": ["tmo", "tmo_full"]},
                    {"name": "Biotech Sentiment (XBI)", "ticker": "XBI", "weight": 0.20, "type": "indicator",
                     "description": "Equal-weight biotech ETF — small-cap pipeline bets",
                     "features": ["xbi"]},
                ],
            },
            "Medical Devices": {
                "weight": 0.30,
                "actors": [
                    {"name": "UnitedHealth", "ticker": "UNH", "weight": 0.45, "type": "company",
                     "description": "Largest health insurer — procedure volume drives device demand",
                     "features": ["unh", "unh_full"]},
                    {"name": "Hospital Spending", "ticker": None, "weight": 0.30, "type": "macro",
                     "description": "Elective procedure volumes track consumer confidence",
                     "features": ["consumer_confidence"]},
                ],
            },
        },
    },
    "Industrials": {
        "etf": "XLI",
        "subsectors": {
            "Defense": {
                "weight": 0.30,
                "actors": [
                    {"name": "US DoD Budget", "ticker": None, "weight": 0.30, "type": "policy",
                     "description": "Defense spending sets revenue ceiling for primes",
                     "features": ["ita", "gdelt_conflict_count"]},
                    {"name": "Raytheon (RTX)", "ticker": "RTX", "weight": 0.25, "type": "company",
                     "features": ["rtx", "rtx_full"]},
                    {"name": "General Dynamics", "ticker": "GD", "weight": 0.20, "type": "company",
                     "features": ["gd", "gd_full"]},
                    {"name": "Geopolitical Tension", "ticker": None, "weight": 0.25, "type": "sentiment",
                     "description": "Ukraine, Taiwan, Middle East drive order book expectations",
                     "features": ["crucix_vix", "gdelt_conflict_count"]},
                ],
            },
            "Manufacturing": {
                "weight": 0.35,
                "actors": [
                    {"name": "ISM PMI", "ticker": None, "weight": 0.30, "type": "indicator",
                     "description": "Leading indicator for factory output and capex",
                     "features": ["ism_manufacturing"]},
                    {"name": "Home Depot", "ticker": "HD", "weight": 0.15, "type": "company",
                     "description": "Housing/construction cycle — consumer durables proxy",
                     "features": ["hd", "hd_full"]},
                    {"name": "Costco", "ticker": "COST", "weight": 0.10, "type": "company",
                     "description": "Consumer staples/discretionary — membership data = confidence",
                     "features": ["cost", "cost_full"]},
                    {"name": "Tariff Policy", "ticker": None, "weight": 0.25, "type": "policy",
                     "description": "Reshoring incentives vs input cost inflation",
                     "features": ["wiki_tariff"]},
                ],
            },
            "Consumer Staples": {
                "weight": 0.20,
                "actors": [
                    {"name": "Procter & Gamble", "ticker": "PG", "weight": 0.35, "type": "company",
                     "description": "Consumer staples bellwether — pricing power gauge",
                     "features": ["pg", "pg_full"]},
                    {"name": "Coca-Cola", "ticker": "KO", "weight": 0.25, "type": "company",
                     "description": "Global consumer demand — FX exposure canary",
                     "features": ["ko", "ko_full"]},
                    {"name": "PepsiCo", "ticker": "PEP", "weight": 0.20, "type": "company",
                     "description": "Snack + beverage — consumer volume trends",
                     "features": ["pep", "pep_full"]},
                ],
            },
        },
    },
    "Crypto": {
        "etf": "BITO",
        "subsectors": {
            "Bitcoin": {
                "weight": 0.50,
                "actors": [
                    {"name": "BTC Spot ETFs", "ticker": None, "weight": 0.25, "type": "flow",
                     "description": "BlackRock IBIT etc. — institutional flow channel",
                     "features": ["btc", "btc_full"]},
                    {"name": "Halving Cycle", "ticker": None, "weight": 0.20, "type": "structural",
                     "description": "Supply reduction every 4 years — 2024 halving priced?",
                     "features": ["btc", "btc_full", "wiki_bitcoin"]},
                    {"name": "Crypto Fear/Greed", "ticker": None, "weight": 0.15, "type": "sentiment",
                     "description": "Retail sentiment oscillator",
                     "features": ["crypto_fear_greed"]},
                    {"name": "Fed Liquidity", "ticker": None, "weight": 0.25, "type": "macro",
                     "description": "BTC tracks global liquidity — QT headwind, QE tailwind",
                     "features": ["fed_funds_rate"]},
                    {"name": "Lightning Network", "ticker": None, "weight": 0.05, "type": "infra",
                     "features": ["lightning_capacity_btc"]},
                ],
            },
            "DeFi / Altcoins": {
                "weight": 0.30,
                "actors": [
                    {"name": "Ethereum", "ticker": "ETH", "weight": 0.40, "type": "asset",
                     "features": ["eth", "eth_full"]},
                    {"name": "Solana", "ticker": "SOL", "weight": 0.25, "type": "asset",
                     "features": ["sol"]},
                    {"name": "BTC Dominance", "ticker": None, "weight": 0.20, "type": "indicator",
                     "description": "Rising = risk-off in crypto, falling = alt season",
                     "features": ["btc_dominance"]},
                ],
            },
        },
    },

    # -------------------------------------------------------------------------
    # NEW EQUITY SECTORS (GICS-aligned)
    # -------------------------------------------------------------------------
    "Consumer Discretionary": {
        "etf": "XLY",
        "subsectors": {
            "Retail": {
                "weight": 0.30,
                "actors": [
                    {"name": "Amazon", "ticker": "AMZN", "weight": 0.25, "type": "company",
                     "description": "E-commerce dominance — consumer discretionary spending proxy",
                     "features": ["amzn", "amzn_full"]},
                    {"name": "Home Depot", "ticker": "HD", "weight": 0.15, "type": "company",
                     "description": "Housing cycle barometer — renovation and new build demand",
                     "features": ["hd", "hd_full"]},
                    {"name": "Costco", "ticker": "COST", "weight": 0.12, "type": "company",
                     "description": "Membership-driven retail — consumer confidence signal",
                     "features": ["cost", "cost_full"]},
                ],
            },
            "Automotive": {
                "weight": 0.25,
                "actors": [
                    {"name": "Tesla", "ticker": "TSLA", "weight": 0.20, "type": "company",
                     "description": "EV market leader — retail sentiment and auto demand bellwether",
                     "features": ["tsla", "tsla_full"]},
                    {"name": "General Motors", "ticker": "GM", "weight": 0.08, "type": "company",
                     "description": "Legacy auto — EV transition capex and ICE margin proxy",
                     "features": ["gm", "gm_full"]},
                    {"name": "Ford", "ticker": "F", "weight": 0.05, "type": "company",
                     "description": "Truck/fleet demand — blue-collar consumer gauge",
                     "features": ["f", "f_full"]},
                ],
            },
            "Travel & Leisure": {
                "weight": 0.15,
                "actors": [
                    {"name": "Booking Holdings", "ticker": "BKNG", "weight": 0.08, "type": "company",
                     "description": "Online travel aggregator — global travel demand proxy",
                     "features": ["bkng", "bkng_full"]},
                    {"name": "Marriott", "ticker": "MAR", "weight": 0.05, "type": "company",
                     "description": "Hotel occupancy and RevPAR — business/leisure travel gauge",
                     "features": ["mar", "mar_full"]},
                ],
            },
            "Consumer Confidence": {
                "weight": 0.30,
                "actors": [
                    {"name": "Consumer Confidence Index", "ticker": None, "weight": 0.15, "type": "macro",
                     "description": "Conference Board CCI — forward spending intentions",
                     "features": ["consumer_confidence"]},
                ],
            },
        },
    },
    "Consumer Staples": {
        "etf": "XLP",
        "subsectors": {
            "Food & Beverage": {
                "weight": 0.35,
                "actors": [
                    {"name": "Procter & Gamble", "ticker": "PG", "weight": 0.20, "type": "company",
                     "description": "Staples bellwether — pricing power and volume trends",
                     "features": ["pg", "pg_full"]},
                    {"name": "Coca-Cola", "ticker": "KO", "weight": 0.15, "type": "company",
                     "description": "Global beverage demand — FX and EM consumer proxy",
                     "features": ["ko", "ko_full"]},
                    {"name": "PepsiCo", "ticker": "PEP", "weight": 0.12, "type": "company",
                     "description": "Snack + beverage portfolio — consumer volume tracker",
                     "features": ["pep", "pep_full"]},
                    {"name": "Mondelez", "ticker": "MDLZ", "weight": 0.08, "type": "company",
                     "description": "Global snack giant — EM consumer exposure",
                     "features": ["mdlz", "mdlz_full"]},
                ],
            },
            "Household": {
                "weight": 0.15,
                "actors": [
                    {"name": "Colgate-Palmolive", "ticker": "CL", "weight": 0.08, "type": "company",
                     "description": "Oral care + household staples — defensive positioning",
                     "features": ["cl", "cl_full"]},
                    {"name": "Church & Dwight", "ticker": "CHD", "weight": 0.05, "type": "company",
                     "description": "Value-oriented consumer products — trade-down beneficiary",
                     "features": ["chd", "chd_full"]},
                ],
            },
            "Retail Staples": {
                "weight": 0.50,
                "actors": [
                    {"name": "Walmart", "ticker": "WMT", "weight": 0.20, "type": "company",
                     "description": "Largest retailer — low-income consumer spending proxy",
                     "features": ["wmt", "wmt_full"]},
                    {"name": "Philip Morris", "ticker": "PM", "weight": 0.12, "type": "company",
                     "description": "Tobacco + IQOS transition — dividend and defensive play",
                     "features": ["pm", "pm_full"]},
                ],
            },
        },
    },
    "Real Estate": {
        "etf": "XLRE",
        "subsectors": {
            "Commercial REITs": {
                "weight": 0.35,
                "actors": [
                    {"name": "Prologis", "ticker": "PLD", "weight": 0.15, "type": "company",
                     "description": "Industrial logistics REIT — e-commerce warehouse demand",
                     "features": ["pld", "pld_full"]},
                    {"name": "American Tower", "ticker": "AMT", "weight": 0.12, "type": "company",
                     "description": "Cell tower REIT — 5G and data infrastructure demand",
                     "features": ["amt", "amt_full"]},
                    {"name": "Crown Castle", "ticker": "CCI", "weight": 0.10, "type": "company",
                     "description": "Fiber + tower REIT — connectivity infrastructure proxy",
                     "features": ["cci", "cci_full"]},
                    {"name": "Equinix", "ticker": "EQIX", "weight": 0.10, "type": "company",
                     "description": "Data center REIT — AI/cloud capacity demand",
                     "features": ["eqix", "eqix_full"]},
                ],
            },
            "Residential": {
                "weight": 0.35,
                "actors": [
                    {"name": "Mortgage Rates", "ticker": None, "weight": 0.20, "type": "macro",
                     "description": "30yr mortgage rate drives housing affordability and turnover",
                     "features": ["treasury_10y", "mortgage_30y"]},
                    {"name": "Housing Starts", "ticker": None, "weight": 0.15, "type": "macro",
                     "description": "HOUST — new residential construction leading indicator",
                     "features": ["housing_starts"]},
                ],
            },
            "CRE Stress": {
                "weight": 0.30,
                "actors": [
                    {"name": "Regional Bank CRE Exposure", "ticker": "KRE", "weight": 0.10, "type": "indicator",
                     "description": "Regional banks hold concentrated CRE risk — stress canary",
                     "features": ["kre"]},
                    {"name": "CMBS Spreads", "ticker": None, "weight": 0.08, "type": "indicator",
                     "description": "Commercial mortgage-backed securities — CRE distress gauge",
                     "features": ["hy_spread", "lqd"]},
                ],
            },
        },
    },
    "Utilities": {
        "etf": "XLU",
        "subsectors": {
            "Regulated Utilities": {
                "weight": 0.40,
                "actors": [
                    {"name": "NextEra Energy", "ticker": "NEE", "weight": 0.25, "type": "company",
                     "description": "Largest US utility — renewables + regulated rate base",
                     "features": ["nee", "nee_full"]},
                    {"name": "Southern Company", "ticker": "SO", "weight": 0.15, "type": "company",
                     "description": "Southeast utility — nuclear + regulated earnings",
                     "features": ["so", "so_full"]},
                    {"name": "Duke Energy", "ticker": "DUK", "weight": 0.12, "type": "company",
                     "description": "Regulated utility — rate case outcomes drive earnings",
                     "features": ["duk", "duk_full"]},
                    {"name": "American Electric Power", "ticker": "AEP", "weight": 0.08, "type": "company",
                     "description": "Midwest/South utility — grid modernization capex",
                     "features": ["aep", "aep_full"]},
                ],
            },
            "Power Gen & Grid": {
                "weight": 0.30,
                "actors": [
                    {"name": "IRA Clean Energy Subsidies", "ticker": None, "weight": 0.15, "type": "policy",
                     "description": "IRA tax credits accelerate renewable + storage buildout",
                     "features": ["icln", "patent_velocity_cleanenergy"]},
                    {"name": "Nuclear Renaissance (SMR)", "ticker": None, "weight": 0.10, "type": "structural",
                     "description": "Small modular reactors — baseload solution for AI/data centers",
                     "features": ["uranium"]},
                ],
            },
            "Data Center Power": {
                "weight": 0.30,
                "actors": [
                    {"name": "AI Power Demand", "ticker": None, "weight": 0.15, "type": "structural",
                     "description": "AI training/inference driving unprecedented electricity demand",
                     "features": ["nvda", "eqix", "nee"]},
                ],
            },
        },
    },
    "Communication Services": {
        "etf": "XLC",
        "subsectors": {
            "Digital Advertising": {
                "weight": 0.35,
                "actors": [
                    {"name": "Alphabet", "ticker": "GOOGL", "weight": 0.25, "type": "company",
                     "description": "Search + YouTube ad duopoly — digital ad cycle proxy",
                     "features": ["googl", "googl_full"]},
                    {"name": "Meta", "ticker": "META", "weight": 0.20, "type": "company",
                     "description": "Social media ad monopoly — SMB ad spend bellwether",
                     "features": ["meta", "meta_full"]},
                ],
            },
            "Streaming & Media": {
                "weight": 0.25,
                "actors": [
                    {"name": "Netflix", "ticker": "NFLX", "weight": 0.12, "type": "company",
                     "description": "Streaming leader — subscriber growth and ARPU trends",
                     "features": ["nflx", "nflx_full"]},
                    {"name": "Disney", "ticker": "DIS", "weight": 0.10, "type": "company",
                     "description": "Parks + streaming + content — consumer entertainment spend",
                     "features": ["dis", "dis_full"]},
                    {"name": "Warner Bros Discovery", "ticker": "WBD", "weight": 0.05, "type": "company",
                     "description": "Legacy media restructuring — content library monetization",
                     "features": ["wbd", "wbd_full"]},
                ],
            },
            "Telecom": {
                "weight": 0.25,
                "actors": [
                    {"name": "AT&T", "ticker": "T", "weight": 0.08, "type": "company",
                     "description": "Largest US telecom — 5G capex and fiber buildout",
                     "features": ["t", "t_full"]},
                    {"name": "Verizon", "ticker": "VZ", "weight": 0.08, "type": "company",
                     "description": "Wireless + broadband — subscriber and churn metrics",
                     "features": ["vz", "vz_full"]},
                    {"name": "T-Mobile", "ticker": "TMUS", "weight": 0.07, "type": "company",
                     "description": "Wireless growth leader — market share gains",
                     "features": ["tmus", "tmus_full"]},
                ],
            },
            "AI Content": {
                "weight": 0.15,
                "actors": [
                    {"name": "AI Content Disruption", "ticker": None, "weight": 0.05, "type": "structural",
                     "description": "OpenAI/Anthropic disrupting search, media, and content creation",
                     "features": ["wiki_ai", "patent_velocity_ai"]},
                ],
            },
        },
    },
    "Materials": {
        "etf": "XLB",
        "subsectors": {
            "Mining": {
                "weight": 0.30,
                "actors": [
                    {"name": "BHP Group", "ticker": "BHP", "weight": 0.15, "type": "company",
                     "description": "Diversified miner — iron ore, copper, and coal bellwether",
                     "features": ["bhp", "bhp_full"]},
                    {"name": "Rio Tinto", "ticker": "RIO", "weight": 0.12, "type": "company",
                     "description": "Iron ore and aluminum — China construction demand proxy",
                     "features": ["rio", "rio_full"]},
                    {"name": "Freeport-McMoRan", "ticker": "FCX", "weight": 0.12, "type": "company",
                     "description": "Copper pure-play — electrification and EV demand",
                     "features": ["fcx", "fcx_full", "copper"]},
                ],
            },
            "Chemicals": {
                "weight": 0.30,
                "actors": [
                    {"name": "Linde", "ticker": "LIN", "weight": 0.15, "type": "company",
                     "description": "Industrial gases leader — manufacturing activity proxy",
                     "features": ["lin", "lin_full"]},
                    {"name": "Air Products", "ticker": "APD", "weight": 0.10, "type": "company",
                     "description": "Industrial gases + hydrogen — clean energy transition",
                     "features": ["apd", "apd_full"]},
                    {"name": "DuPont", "ticker": "DD", "weight": 0.08, "type": "company",
                     "description": "Specialty materials — electronics and construction cycle",
                     "features": ["dd", "dd_full"]},
                ],
            },
            "Construction Materials": {
                "weight": 0.20,
                "actors": [
                    {"name": "Vulcan Materials", "ticker": "VMC", "weight": 0.08, "type": "company",
                     "description": "Aggregates producer — infrastructure bill beneficiary",
                     "features": ["vmc", "vmc_full"]},
                    {"name": "Martin Marietta", "ticker": "MLM", "weight": 0.08, "type": "company",
                     "description": "Heavy-side materials — highway and commercial construction",
                     "features": ["mlm", "mlm_full"]},
                ],
            },
            "Lithium & Battery": {
                "weight": 0.20,
                "actors": [
                    {"name": "Albemarle", "ticker": "ALB", "weight": 0.07, "type": "company",
                     "description": "Largest lithium producer — EV battery supply chain",
                     "features": ["alb", "alb_full", "lit"]},
                    {"name": "Lithium Price", "ticker": None, "weight": 0.05, "type": "indicator",
                     "description": "Lithium carbonate spot — battery cost and EV margin driver",
                     "features": ["lit"]},
                ],
            },
        },
    },
    "Transportation & Logistics": {
        "etf": "IYT",
        "subsectors": {
            "Airlines": {
                "weight": 0.25,
                "actors": [
                    {"name": "Delta Air Lines", "ticker": "DAL", "weight": 0.12, "type": "company",
                     "description": "Premium airline — business/leisure travel demand gauge",
                     "features": ["dal", "dal_full"]},
                    {"name": "United Airlines", "ticker": "UAL", "weight": 0.10, "type": "company",
                     "description": "International routes — global travel recovery proxy",
                     "features": ["ual", "ual_full"]},
                    {"name": "Southwest Airlines", "ticker": "LUV", "weight": 0.08, "type": "company",
                     "description": "Domestic budget carrier — middle-class travel spending",
                     "features": ["luv", "luv_full"]},
                ],
            },
            "Shipping & Freight": {
                "weight": 0.30,
                "actors": [
                    {"name": "FedEx", "ticker": "FDX", "weight": 0.12, "type": "company",
                     "description": "Express delivery bellwether — global trade volume proxy",
                     "features": ["fdx", "fdx_full"]},
                    {"name": "UPS", "ticker": "UPS", "weight": 0.12, "type": "company",
                     "description": "Package delivery — e-commerce and B2B shipping demand",
                     "features": ["ups", "ups_full"]},
                    {"name": "Baltic Dry Index", "ticker": None, "weight": 0.10, "type": "macro",
                     "description": "Dry bulk shipping rates — global trade activity indicator",
                     "features": ["baltic_dry"]},
                ],
            },
            "Rail": {
                "weight": 0.25,
                "actors": [
                    {"name": "Union Pacific", "ticker": "UNP", "weight": 0.10, "type": "company",
                     "description": "Western US rail — intermodal and bulk freight volumes",
                     "features": ["unp", "unp_full"]},
                    {"name": "CSX", "ticker": "CSX", "weight": 0.08, "type": "company",
                     "description": "Eastern US rail — coal, auto, and merchandise carloads",
                     "features": ["csx", "csx_full"]},
                    {"name": "Norfolk Southern", "ticker": "NSC", "weight": 0.08, "type": "company",
                     "description": "Eastern rail — industrial and consumer goods transport",
                     "features": ["nsc", "nsc_full"]},
                ],
            },
            "Supply Chain Stress": {
                "weight": 0.20,
                "actors": [
                    {"name": "Global Supply Chain Pressure Index", "ticker": None, "weight": 0.10, "type": "macro",
                     "description": "NY Fed GSCPI — shipping costs, delivery times, backlogs",
                     "features": ["supply_chain_pressure"]},
                ],
            },
        },
    },

    # -------------------------------------------------------------------------
    # CROSS-ASSET CLASSES
    # -------------------------------------------------------------------------
    "Sovereign Debt & Fixed Income": {
        "etf": "TLT",
        "junction_points": ["treasury_issuance", "corporate_bond_issuance", "fed_balance_sheet"],
        "subsectors": {
            "US Treasuries": {
                "weight": 0.40,
                "actors": [
                    {"name": "10Y Treasury Yield", "ticker": None, "weight": 0.25, "type": "macro",
                     "description": "Risk-free rate benchmark — discounts all assets globally",
                     "features": ["treasury_10y"]},
                    {"name": "2Y Treasury Yield", "ticker": None, "weight": 0.15, "type": "macro",
                     "description": "Fed policy expectations — front-end rate sensitivity",
                     "features": ["treasury_2y"]},
                    {"name": "Yield Curve (10Y-2Y)", "ticker": None, "weight": 0.15, "type": "indicator",
                     "description": "Inversion signals recession — steepening signals recovery",
                     "features": ["yield_curve_10y2y"]},
                    {"name": "TGA Balance", "ticker": None, "weight": 0.10, "type": "macro",
                     "description": "Treasury General Account — drawdowns inject liquidity",
                     "features": ["tga_balance"]},
                ],
            },
            "Corporate Bonds": {
                "weight": 0.30,
                "actors": [
                    {"name": "High Yield (HYG)", "ticker": "HYG", "weight": 0.12, "type": "indicator",
                     "description": "Junk bond ETF — credit risk appetite in real time",
                     "features": ["hyg", "hy_spread"]},
                    {"name": "Investment Grade (LQD)", "ticker": "LQD", "weight": 0.12, "type": "indicator",
                     "description": "IG bond ETF — corporate funding conditions proxy",
                     "features": ["lqd"]},
                    {"name": "HY Spread", "ticker": None, "weight": 0.08, "type": "indicator",
                     "description": "OAS over treasuries — credit stress thermometer",
                     "features": ["hy_spread"]},
                ],
            },
            "EM Debt": {
                "weight": 0.15,
                "actors": [
                    {"name": "EM Bond ETF (EMB)", "ticker": "EMB", "weight": 0.05, "type": "indicator",
                     "description": "USD-denominated EM sovereign debt — dollar sensitivity",
                     "features": ["emb", "dollar_index"]},
                    {"name": "EM Sovereign Spread", "ticker": None, "weight": 0.05, "type": "indicator",
                     "description": "EM yield premium over UST — capital flight risk gauge",
                     "features": ["dollar_index"]},
                ],
            },
            "Munis": {
                "weight": 0.15,
                "actors": [
                    {"name": "Municipal Bond ETF (MUB)", "ticker": "MUB", "weight": 0.03, "type": "indicator",
                     "description": "Tax-exempt muni market — state/local fiscal health",
                     "features": ["mub"]},
                ],
            },
        },
    },
    "FX & Currency": {
        "etf": "UUP",
        "junction_points": ["fx_reserves", "carry_trade", "trade_balance"],
        "subsectors": {
            "Major Pairs": {
                "weight": 0.50,
                "actors": [
                    {"name": "US Dollar Index (DXY)", "ticker": "DX-Y.NYB", "weight": 0.30, "type": "macro",
                     "description": "Trade-weighted dollar — global risk-on/off toggle",
                     "features": ["dollar_index"]},
                    {"name": "EUR/USD", "ticker": None, "weight": 0.15, "type": "macro",
                     "description": "Most traded pair — ECB vs Fed policy divergence",
                     "features": ["eurusd"]},
                    {"name": "USD/JPY", "ticker": None, "weight": 0.15, "type": "macro",
                     "description": "Carry trade barometer — BOJ policy shifts move all assets",
                     "features": ["usdjpy"]},
                    {"name": "GBP/USD", "ticker": None, "weight": 0.10, "type": "macro",
                     "description": "Cable — UK fiscal/monetary policy proxy",
                     "features": ["gbpusd"]},
                ],
            },
            "EM Currencies": {
                "weight": 0.25,
                "actors": [
                    {"name": "China Yuan (USD/CNY)", "ticker": None, "weight": 0.10, "type": "macro",
                     "description": "PBOC managed float — trade war and capital flow signal",
                     "features": ["usdcny"]},
                    {"name": "Carry Trade Differential", "ticker": None, "weight": 0.10, "type": "indicator",
                     "description": "Rate differentials drive cross-border capital flows",
                     "features": ["fed_funds_rate", "usdjpy"]},
                ],
            },
            "FX Reserves": {
                "weight": 0.25,
                "actors": [
                    {"name": "IMF COFER Data", "ticker": None, "weight": 0.10, "type": "sovereign",
                     "description": "Global reserve composition — dollar hegemony tracker",
                     "features": ["dollar_index", "gold"]},
                ],
            },
        },
    },
    "Private Markets": {
        "etf": "PSP",
        "junction_points": ["private_credit", "ipo_pipeline"],
        "subsectors": {
            "Private Equity": {
                "weight": 0.35,
                "actors": [
                    {"name": "Blackstone", "ticker": "BX", "weight": 0.20, "type": "company",
                     "description": "Largest alt manager — PE, real estate, credit bellwether",
                     "features": ["bx", "bx_full"]},
                    {"name": "KKR", "ticker": "KKR", "weight": 0.15, "type": "company",
                     "description": "PE + infra + credit — deal flow and exit activity",
                     "features": ["kkr", "kkr_full"]},
                    {"name": "Apollo", "ticker": "APO", "weight": 0.15, "type": "company",
                     "description": "Credit-oriented alt manager — private credit pioneer",
                     "features": ["apo", "apo_full"]},
                ],
            },
            "Venture Capital": {
                "weight": 0.20,
                "actors": [
                    {"name": "ARK Innovation (proxy)", "ticker": "ARKK", "weight": 0.10, "type": "indicator",
                     "description": "Disruptive innovation ETF — VC sentiment proxy",
                     "features": ["arkk"]},
                    {"name": "IPO Pipeline", "ticker": None, "weight": 0.10, "type": "indicator",
                     "description": "IPO volume and pricing — VC exit window gauge",
                     "features": ["ipo_count"]},
                ],
            },
            "Private Credit": {
                "weight": 0.25,
                "actors": [
                    {"name": "Ares Capital", "ticker": "ARCC", "weight": 0.10, "type": "company",
                     "description": "Largest BDC — middle-market private credit health",
                     "features": ["arcc", "arcc_full"]},
                    {"name": "Blackstone Secured Lending", "ticker": "BXSL", "weight": 0.08, "type": "company",
                     "description": "Secured private credit — direct lending conditions",
                     "features": ["bxsl", "bxsl_full"]},
                ],
            },
            "Infrastructure": {
                "weight": 0.20,
                "actors": [
                    {"name": "Brookfield Infrastructure", "ticker": "BIP", "weight": 0.07, "type": "company",
                     "description": "Global infra owner — utilities, transport, data, midstream",
                     "features": ["bip", "bip_full"]},
                    {"name": "Antero Midstream", "ticker": "AM", "weight": 0.05, "type": "company",
                     "description": "Midstream gathering — natural gas infrastructure proxy",
                     "features": ["am", "am_full"]},
                ],
            },
        },
    },
    "Insurance & Pensions": {
        "etf": "KIE",
        "junction_points": ["pension_rebalancing", "insurance_float"],
        "subsectors": {
            "Life Insurance": {
                "weight": 0.30,
                "actors": [
                    {"name": "MetLife", "ticker": "MET", "weight": 0.15, "type": "company",
                     "description": "Largest US life insurer — rate sensitivity and EM exposure",
                     "features": ["met", "met_full"]},
                    {"name": "Prudential Financial", "ticker": "PRU", "weight": 0.12, "type": "company",
                     "description": "Life + retirement — long-duration liability management",
                     "features": ["pru", "pru_full"]},
                    {"name": "AIG", "ticker": "AIG", "weight": 0.10, "type": "company",
                     "description": "Insurance conglomerate — post-restructuring simplification",
                     "features": ["aig", "aig_full"]},
                ],
            },
            "P&C Insurance": {
                "weight": 0.25,
                "actors": [
                    {"name": "Allstate", "ticker": "ALL", "weight": 0.10, "type": "company",
                     "description": "Personal lines P&C — auto/home loss ratios",
                     "features": ["all_ins", "all_ins_full"]},
                    {"name": "Travelers", "ticker": "TRV", "weight": 0.10, "type": "company",
                     "description": "Commercial P&C — pricing cycle and reserve adequacy",
                     "features": ["trv", "trv_full"]},
                    {"name": "Chubb", "ticker": "CB", "weight": 0.08, "type": "company",
                     "description": "Global P&C leader — commercial and specialty lines",
                     "features": ["cb", "cb_full"]},
                ],
            },
            "Pension Flows": {
                "weight": 0.30,
                "actors": [
                    {"name": "Calendar Rebalancing", "ticker": None, "weight": 0.15, "type": "macro",
                     "description": "Quarter-end pension rebalancing drives equity/bond flows",
                     "features": ["treasury_30y", "spy"]},
                    {"name": "Long Bond Yield (30Y)", "ticker": None, "weight": 0.12, "type": "macro",
                     "description": "Pension funded status tracks 30Y yield — drives asset allocation",
                     "features": ["treasury_30y"]},
                ],
            },
            "Reinsurance": {
                "weight": 0.15,
                "actors": [
                    {"name": "Catastrophe Bonds", "ticker": None, "weight": 0.08, "type": "indicator",
                     "description": "Cat bond spreads reflect disaster risk pricing",
                     "features": ["cat_bond_spread"]},
                ],
            },
        },
    },
    "Defense & Aerospace": {
        "etf": "ITA",
        "junction_points": ["defense_spending", "geopolitical_risk"],
        "subsectors": {
            "Prime Contractors": {
                "weight": 0.60,
                "actors": [
                    {"name": "Lockheed Martin", "ticker": "LMT", "weight": 0.20, "type": "company",
                     "description": "F-35, missiles, space — largest US defense contractor",
                     "features": ["lmt", "lmt_full"]},
                    {"name": "Raytheon (RTX)", "ticker": "RTX", "weight": 0.18, "type": "company",
                     "description": "Missiles, radar, engines — NATO rearmament beneficiary",
                     "features": ["rtx", "rtx_full"]},
                    {"name": "Northrop Grumman", "ticker": "NOC", "weight": 0.15, "type": "company",
                     "description": "B-21 bomber, space, nuclear deterrence programs",
                     "features": ["noc", "noc_full"]},
                    {"name": "General Dynamics", "ticker": "GD", "weight": 0.12, "type": "company",
                     "description": "Submarines, combat vehicles, Gulfstream jets",
                     "features": ["gd", "gd_full"]},
                    {"name": "Boeing", "ticker": "BA", "weight": 0.10, "type": "company",
                     "description": "Defense + commercial aero — tankers, fighters, and satellites",
                     "features": ["ba", "ba_full"]},
                ],
            },
            "Space": {
                "weight": 0.15,
                "actors": [
                    {"name": "Rocket Lab", "ticker": "RKLB", "weight": 0.05, "type": "company",
                     "description": "Small-launch provider — space economy access layer",
                     "features": ["rklb", "rklb_full"]},
                    {"name": "Space Economy", "ticker": None, "weight": 0.05, "type": "structural",
                     "description": "Satellite, launch, and space infrastructure market growth",
                     "features": ["patent_velocity_space"]},
                ],
            },
            "Geopolitical Demand": {
                "weight": 0.25,
                "actors": [
                    {"name": "GDELT Conflict Count", "ticker": None, "weight": 0.08, "type": "indicator",
                     "description": "Global conflict events — defense procurement urgency signal",
                     "features": ["gdelt_conflict_count"]},
                    {"name": "DoD Budget Policy", "ticker": None, "weight": 0.07, "type": "policy",
                     "description": "Annual defense authorization — sets multi-year spending trajectory",
                     "features": ["ita", "gdelt_conflict_count"]},
                ],
            },
        },
    },
    "Agriculture & Food": {
        "etf": "DBA",
        "junction_points": ["commodity_supercycle", "trade_balance"],
        "subsectors": {
            "Farming & Grain": {
                "weight": 0.30,
                "actors": [
                    {"name": "Archer-Daniels-Midland", "ticker": "ADM", "weight": 0.15, "type": "company",
                     "description": "Grain processing giant — crop prices and trade flow proxy",
                     "features": ["adm", "adm_full"]},
                    {"name": "Bunge", "ticker": "BG", "weight": 0.12, "type": "company",
                     "description": "Global agribusiness — oilseed and grain origination",
                     "features": ["bg", "bg_full"]},
                    {"name": "Corn/Wheat/Soy Complex", "ticker": None, "weight": 0.15, "type": "macro",
                     "description": "Row crop commodity basket — food inflation leading indicator",
                     "features": ["corn", "wheat", "soybean"]},
                ],
            },
            "Fertilizers": {
                "weight": 0.25,
                "actors": [
                    {"name": "Nutrien", "ticker": "NTR", "weight": 0.12, "type": "company",
                     "description": "Largest potash/nitrogen producer — crop input cost driver",
                     "features": ["ntr", "ntr_full"]},
                    {"name": "Mosaic", "ticker": "MOS", "weight": 0.10, "type": "company",
                     "description": "Phosphate + potash — fertilizer pricing cycle",
                     "features": ["mos", "mos_full"]},
                    {"name": "CF Industries", "ticker": "CF", "weight": 0.08, "type": "company",
                     "description": "Nitrogen fertilizer — natural gas cost pass-through",
                     "features": ["cf", "cf_full"]},
                ],
            },
            "Food Processing": {
                "weight": 0.20,
                "actors": [
                    {"name": "General Mills", "ticker": "GIS", "weight": 0.08, "type": "company",
                     "description": "Packaged food — consumer trade-down and private label pressure",
                     "features": ["gis", "gis_full"]},
                    {"name": "Kellanova", "ticker": "K", "weight": 0.05, "type": "company",
                     "description": "Snack/cereal — input cost and volume elasticity",
                     "features": ["k", "k_full"]},
                    {"name": "J.M. Smucker", "ticker": "SJM", "weight": 0.05, "type": "company",
                     "description": "Coffee and pet food — commodity cost and shelf-stable demand",
                     "features": ["sjm", "sjm_full"]},
                ],
            },
            "Water": {
                "weight": 0.25,
                "actors": [
                    {"name": "American Water Works", "ticker": "AWK", "weight": 0.05, "type": "company",
                     "description": "Largest US water utility — regulated rate base growth",
                     "features": ["awk", "awk_full"]},
                    {"name": "Water Stress", "ticker": None, "weight": 0.05, "type": "structural",
                     "description": "Global water scarcity driving infra spend and crop yields",
                     "features": ["water_stress"]},
                ],
            },
        },
    },
}


# -----------------------------------------------------------------------------
# JUNCTION POINTS — cross-sector capital flow nodes
# -----------------------------------------------------------------------------

JUNCTION_POINTS: dict[str, dict] = {
    # --- Monetary ---
    "fed_balance_sheet": {
        "layer": "monetary",
        "label": "Fed Balance Sheet",
        "series_id": "WALCL",
        "unit": "USD_millions",
        "update_freq": "weekly",
        "magnitude_usd": 7_400_000_000_000,
        "description": "Federal Reserve total assets — QE/QT directly injects/drains liquidity",
    },
    "reverse_repo": {
        "layer": "monetary",
        "label": "Overnight Reverse Repo",
        "series_id": "RRPONTSYD",
        "unit": "USD_millions",
        "update_freq": "daily",
        "magnitude_usd": 500_000_000_000,
        "description": "ON RRP facility — money market parking lot, drawdown releases liquidity",
    },
    "tga_balance": {
        "layer": "monetary",
        "label": "Treasury General Account",
        "series_id": "WTREGEN",
        "unit": "USD_millions",
        "update_freq": "weekly",
        "magnitude_usd": 750_000_000_000,
        "description": "Treasury cash balance — drawdowns inject liquidity, rebuilds drain it",
    },
    "global_m2": {
        "layer": "monetary",
        "label": "Global M2 Money Supply",
        "series_id": "M2SL",
        "unit": "USD_billions",
        "update_freq": "monthly",
        "magnitude_usd": 21_000_000_000_000,
        "description": "US M2 + global estimates — broadest liquidity measure for risk assets",
    },
    "ecb_balance_sheet": {
        "layer": "monetary",
        "label": "ECB Balance Sheet",
        "series_id": "ecb_total_assets",
        "unit": "EUR_millions",
        "update_freq": "weekly",
        "magnitude_usd": 6_500_000_000_000,
        "description": "European Central Bank total assets — eurozone liquidity injection/drain",
    },

    # --- Credit ---
    "bank_credit": {
        "layer": "credit",
        "label": "Bank Credit (Total)",
        "series_id": "TOTBKCR",
        "unit": "USD_billions",
        "update_freq": "weekly",
        "magnitude_usd": 17_500_000_000_000,
        "description": "Total bank credit outstanding — real economy lending pulse",
    },
    "hy_spread": {
        "layer": "credit",
        "label": "High Yield Spread",
        "series_id": "BAMLH0A0HYM2",
        "unit": "bps",
        "update_freq": "daily",
        "magnitude_usd": None,
        "description": "ICE BofA HY OAS — credit stress and risk appetite thermometer",
    },
    "ig_spread": {
        "layer": "credit",
        "label": "Investment Grade Spread",
        "series_id": "BAMLC0A0CM",
        "unit": "bps",
        "update_freq": "daily",
        "magnitude_usd": None,
        "description": "ICE BofA IG OAS — corporate funding conditions gauge",
    },
    "money_market_funds": {
        "layer": "credit",
        "label": "Money Market Fund Assets",
        "series_id": "estimated",
        "unit": "USD_billions",
        "update_freq": "weekly",
        "magnitude_usd": 6_000_000_000_000,
        "description": "Total MMF AUM — sidelined cash waiting to deploy into risk assets",
    },

    # --- Market ---
    "etf_flows": {
        "layer": "market",
        "label": "ETF Fund Flows",
        "series_id": "proxy",
        "unit": "USD_millions",
        "update_freq": "daily",
        "magnitude_usd": None,
        "description": "Net ETF creation/redemption flows — real-time demand for asset classes",
    },
    "options_positioning": {
        "layer": "market",
        "label": "Options Positioning",
        "series_id": "options_daily_signals",
        "unit": "contracts",
        "update_freq": "daily",
        "magnitude_usd": None,
        "description": "Put/call ratios and gamma exposure — dealer hedging drives intraday",
    },
    "dark_pool_activity": {
        "layer": "market",
        "label": "Dark Pool Activity",
        "series_id": "signal_sources",
        "unit": "shares",
        "update_freq": "daily",
        "magnitude_usd": None,
        "description": "Off-exchange volume and block prints — institutional conviction signal",
    },
    "margin_debt": {
        "layer": "market",
        "label": "FINRA Margin Debt",
        "series_id": "FINRA",
        "unit": "USD_millions",
        "update_freq": "monthly",
        "magnitude_usd": 750_000_000_000,
        "description": "Margin balances — leverage proxy, peaks precede corrections",
    },

    # --- Corporate ---
    "buyback_activity": {
        "layer": "corporate",
        "label": "Corporate Buybacks",
        "series_id": "estimated",
        "unit": "USD_billions",
        "update_freq": "quarterly",
        "magnitude_usd": 800_000_000_000,
        "description": "Authorized and executed buybacks — largest net equity demand source",
    },
    "corporate_bond_issuance": {
        "layer": "corporate",
        "label": "Corporate Bond Issuance",
        "series_id": "estimated",
        "unit": "USD_billions",
        "update_freq": "monthly",
        "magnitude_usd": 1_500_000_000_000,
        "description": "IG + HY new issuance — corporate funding appetite and refinancing wall",
    },

    # --- Sovereign ---
    "fx_reserves": {
        "layer": "sovereign",
        "label": "Global FX Reserves",
        "series_id": "estimated",
        "unit": "USD_trillions",
        "update_freq": "quarterly",
        "magnitude_usd": 12_000_000_000_000,
        "description": "Central bank reserve holdings — dollar recycling and UST demand",
    },
    "trade_balance": {
        "layer": "sovereign",
        "label": "US Trade Balance",
        "series_id": "BOPGTB",
        "unit": "USD_millions",
        "update_freq": "monthly",
        "magnitude_usd": None,
        "description": "Goods and services trade deficit — dollar demand and capital recycling",
    },
    "foreign_treasury_holdings": {
        "layer": "sovereign",
        "label": "Foreign Treasury Holdings (TIC)",
        "series_id": "estimated",
        "unit": "USD_billions",
        "update_freq": "monthly",
        "magnitude_usd": 7_600_000_000_000,
        "description": "Foreign official + private UST holdings — de-dollarization tracker",
    },
    "treasury_issuance": {
        "layer": "sovereign",
        "label": "Treasury Issuance Schedule",
        "series_id": "estimated",
        "unit": "USD_billions",
        "update_freq": "quarterly",
        "magnitude_usd": 2_000_000_000_000,
        "description": "Quarterly refunding — supply shock potential for rates market",
    },

    # --- Retail ---
    "consumer_sentiment": {
        "layer": "retail",
        "label": "Consumer Sentiment",
        "series_id": "UMCSENT",
        "unit": "index",
        "update_freq": "monthly",
        "magnitude_usd": None,
        "description": "UMich consumer sentiment — forward spending intentions and inflation expectations",
    },
    "retail_fund_flows": {
        "layer": "retail",
        "label": "Retail Fund Flows",
        "series_id": "proxy",
        "unit": "USD_millions",
        "update_freq": "weekly",
        "magnitude_usd": None,
        "description": "Retail mutual fund and ETF net flows — dumb money indicator",
    },

    # --- Crypto ---
    "stablecoin_supply": {
        "layer": "crypto",
        "label": "Stablecoin Total Supply",
        "series_id": "estimated",
        "unit": "USD_billions",
        "update_freq": "daily",
        "magnitude_usd": 160_000_000_000,
        "description": "USDT + USDC + DAI supply — on-chain liquidity and crypto dry powder",
    },
    "btc_etf_flows": {
        "layer": "crypto",
        "label": "Bitcoin ETF Flows",
        "series_id": "proxy",
        "unit": "USD_millions",
        "update_freq": "daily",
        "magnitude_usd": None,
        "description": "BTC spot ETF net flows — institutional crypto demand in real time",
    },
}


# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------

def get_sector_features(sector: str) -> list[str]:
    """Return all feature names relevant to a sector."""
    s = SECTOR_MAP.get(sector, {})
    features = []
    for sub in s.get("subsectors", {}).values():
        for actor in sub.get("actors", []):
            features.extend(actor.get("features", []))
    return [f for f in features if f]


def get_actor_influence(sector: str) -> list[dict]:
    """Return all actors in a sector with their absolute influence weight."""
    s = SECTOR_MAP.get(sector, {})
    actors = []
    for sub_name, sub in s.get("subsectors", {}).items():
        sub_weight = sub.get("weight", 1.0)
        for actor in sub.get("actors", []):
            actors.append({
                "name": actor["name"],
                "subsector": sub_name,
                "type": actor["type"],
                "ticker": actor.get("ticker"),
                "influence": round(sub_weight * actor["weight"], 4),
                "description": actor.get("description", ""),
                "features": actor.get("features", []),
            })
    actors.sort(key=lambda a: a["influence"], reverse=True)
    return actors


def get_all_sectors() -> list[str]:
    return list(SECTOR_MAP.keys())


def get_junction_points_for_sector(sector: str) -> list[str]:
    """Return junction point IDs relevant to a sector."""
    s = SECTOR_MAP.get(sector, {})
    return list(s.get("junction_points", []))


def get_junction_point(junction_id: str) -> dict | None:
    """Return a junction point config by ID."""
    return JUNCTION_POINTS.get(junction_id)
