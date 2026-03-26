"""GRID Sector Map — hierarchical mapping from sectors to subsectors to key actors.

Each actor has a `weight` representing their relative market-moving influence
within that subsector. This captures the reality that some companies and
institutions press the scale harder than others.

Structure:
    Sector -> Subsector -> [Actors]
    Each actor: {name, ticker/id, weight, type, features, description}

The `features` list maps to feature_registry names so we can pull z-scores
and connect macro/market data directly to the actors that influence them.
"""

SECTOR_MAP: dict[str, dict] = {
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
}


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
